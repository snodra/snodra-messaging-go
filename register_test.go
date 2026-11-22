package snodramessaging

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
)

func place(_ context.Context, p PlaceOrder) (Receipt, error) {
	return Receipt{OrderID: p.OrderID, Total: 42}, nil
}

func get(_ context.Context, q GetOrder) (Receipt, error) { return Receipt{OrderID: q.OrderID}, nil }

// AddItem sorts before orders.PlaceOrder.
type AddItem struct{}

func (AddItem) MessageType() string { return "orders.AddItem" }

// Unnamed has an empty type name.
type Unnamed struct{}

func (Unnamed) MessageType() string { return "" }

// Broken panics when asked its type name.
type Broken struct{}

func (Broken) MessageType() string { panic("no name") }

func TestRegisterListsTheHandledTypes(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	is(t, HandleCommand(app, place), nil)
	is(t, HandleQuery(app, get), nil)
	is(t, HandleCommand(app, func(context.Context, AddItem) (Empty, error) { return Empty{}, nil }), nil)
	is(t, app.Start(context.Background()), nil)
	r := hub.Next(t).RecvRegister(t)
	is(t, strings.Join(r.GetCommandTypes(), ","), "orders.AddItem,orders.PlaceOrder") // sorted
	is(t, strings.Join(r.GetQueryTypes(), ","), "orders.GetOrder")
	is(t, len(r.GetSharedQueryTypes()), 0)
}

func TestRegistrationAfterStartIsRefused(t *testing.T) {
	hub := fakehub.Start(t)
	started := startApp(t, Hubs(hub.Addr()))
	is(t, errors.Is(HandleCommand(started, place), ErrRegistrationClosed), true)
	is(t, errors.Is(HandleQuery(started, get), ErrRegistrationClosed), true)
	is(t, hub.Next(t).RecvRegister(t).GetCommandTypes() == nil, true) // nothing registered late
}

func TestStartReturnsEveryRegistrationError(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	is(t, HandleCommand(app, place), nil)
	var errs []error
	keep := func(err error) {
		t.Helper()
		if err == nil {
			t.Fatal("a bad registration was accepted")
		}
		errs = append(errs, err)
	}
	keep(HandleCommand(app, place))                                                                  // the same type twice
	keep(HandleQuery(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil })) // command and query
	keep(HandleCommand(app, func(context.Context, Unnamed) (Empty, error) { return Empty{}, nil }))  // empty type name
	keep(HandleCommand(app, func(context.Context, Message) (Empty, error) { return Empty{}, nil }))  // an interface type
	keep(HandleCommand(app, func(context.Context, Broken) (Empty, error) { return Empty{}, nil }))   // MessageType panics
	keep(HandleQuery[GetOrder, Receipt](app, nil))                                                   // no handler
	for _, want := range []string{"already registered", "both", "empty type name", "interface", "no name", "nil handler"} {
		if !slices.ContainsFunc(errs, func(e error) bool { return strings.Contains(e.Error(), want) }) {
			t.Fatalf("no registration error mentions %q: %v", want, errs)
		}
	}
	err := app.Start(context.Background())
	if err == nil {
		t.Fatal("Start ignored the registration errors")
	}
	for _, e := range errs {
		if !strings.Contains(err.Error(), e.Error()) {
			t.Fatalf("Start's error %q misses %q", err, e)
		}
	}
	hub.NoStreamWithin(t, 200*ms) // not started
}
