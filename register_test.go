package snodra

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

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

// The Register declares the types served by subscription handlers in subscription_query_types,
// each also in query_types or shared_query_types; the hub serves them in mode SUBSCRIPTION only.
// Every Register carries the declaration, the empty one of a shutdown clears it with the lists.
func TestRegisterDeclaresTheSubscriptionTypes(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	is(t, HandleQuery(app, get), nil)
	is(t, HandleStream(app, listOrders), nil)
	_, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders)
	is(t, err, nil)
	_, err = HandleSubscription[GetStock, Stock, Stock](app, func(context.Context, GetStock) (Stock, error) { return Stock{}, nil }, Shared())
	is(t, err, nil)
	is(t, app.Start(context.Background()), nil)
	s := hub.Next(t)
	r := s.RecvRegister(t)
	is(t, strings.Join(r.GetQueryTypes(), ","), "orders.GetOrder,orders.ListOrders,orders.WatchOrders")
	is(t, strings.Join(r.GetSharedQueryTypes(), ","), "orders.GetStock")
	is(t, strings.Join(r.GetSubscriptionQueryTypes(), ","), "orders.GetStock,orders.WatchOrders") // sorted
	s.Recv(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	empty := s.RecvRegister(t)
	is(t, len(empty.GetQueryTypes())+len(empty.GetSharedQueryTypes())+len(empty.GetSubscriptionQueryTypes()), 0)
	s.Ack(t, fakehub.Ack("hub-1"))
	s.HalfClosed(t, 2*time.Second)
	s.Close(codes.OK, "")
	is(t, <-done, nil)

	// Without a subscription handler, nothing is declared.
	plain := newApp(t, Hubs(hub.Addr()))
	is(t, HandleQuery(plain, get), nil)
	is(t, plain.Start(context.Background()), nil)
	is(t, len(hub.Next(t).RecvRegister(t).GetSubscriptionQueryTypes()), 0)
}
