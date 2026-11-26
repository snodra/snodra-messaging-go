package snodra

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The processor names, subscriptions and stage-1 markers are the ones the specification gives.
func TestProcessorNamesAndMarkers(t *testing.T) {
	is(t, DefaultProcessor, "default")
	is(t, ProcessorSubscription("orders-service", "mail"), "orders-service.mail")
	is(t, ProcessorSubscription("orders-service", DefaultProcessor), "orders-service.default")
	is(t, ProcessorMarker("mail", "e-1"), "processor:mail:e-1")
	is(t, ProcessorMarker(DefaultProcessor, "e-1"), "processor:default:e-1")
}

// A processor name matches [a-z0-9][a-z0-9-]{0,47}; "default" is where handlers without
// InProcessor run.
func TestInProcessorRefusesBadNames(t *testing.T) {
	app := eventsApp(t)
	register := func(opts ...EventOption) error {
		return HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, opts...)
	}
	for _, name := range []string{"", " ", "default", "Mail", "-mail", "mail_", "x y", strings.Repeat("a", 49)} {
		err := register(InProcessor(name))
		if err == nil || !strings.Contains(err.Error(), "InProcessor") {
			t.Errorf("InProcessor(%q): got %v, want a refusal naming InProcessor", name, err)
		}
	}
	for _, name := range []string{"mail", "a", "0", "mail-2", strings.Repeat("a", 48)} {
		if err := register(InProcessor(name)); err != nil {
			t.Errorf("InProcessor(%q): %v", name, err)
		}
	}
	for _, name := range []string{"mail", "a", "0", "mail-2", strings.Repeat("a", 48)} {
		if _, ok := app.eventHandlers[name]; !ok {
			t.Errorf("the processor %q was not registered", name)
		}
	}
}

// A nil event handler, a nil app and a registration after Start are refused.
func TestHandleEventRegistrationErrors(t *testing.T) {
	if err := HandleEvent[OrderPlaced](nil, func(context.Context, OrderPlaced) error { return nil }); err == nil {
		t.Fatal("a nil App accepted an event handler")
	}
	app := eventsApp(t)
	if err := HandleEvent[OrderPlaced](app, nil); err == nil || !strings.Contains(err.Error(), "nil event handler") {
		t.Fatalf("a nil handler: %v", err)
	}
	other := eventsApp(t)
	if err := HandleEvent(other, func(context.Context, OrderPlaced) error { return nil }, InProcessor("bad name")); err == nil {
		t.Fatal("a bad processor name was accepted")
	}
	started := eventsApp(t)
	if err := started.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	err := HandleEvent(started, func(context.Context, OrderShipped) error { return nil })
	if !errors.Is(err, ErrRegistrationClosed) {
		t.Fatalf("registration after Start: got %v, want ErrRegistrationClosed", err)
	}
}

// Several handlers of one event type in one processor all run, in registration order, and a
// command of the same type name stays apart.
func TestSeveralHandlersOfOneEventType(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events), withEventSettings(consumerSettings()), OutboxEnabled(false))
	var order []string
	first := func(context.Context, OrderPlaced) error { order = append(order, "first"); return nil }
	second := func(context.Context, OrderPlaced) error { order = append(order, "second"); return nil }
	if err := HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, first); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, second); err != nil {
		t.Fatal(err)
	}
	// The command type and the event types are separate namespaces.
	is(t, len(app.handlers), 1)
	is(t, len(app.eventHandlers[DefaultProcessor]), 1)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	handlers := app.processors[0].handlers["orders.OrderPlaced"]
	is(t, len(handlers), 2)
	// Run them as the consumer does.
	scope := eventHandling("e-1", "orders.OrderPlaced", map[string]string{HeaderID: "e-1", HeaderType: "orders.OrderPlaced"})
	ctx := withHandling(context.Background(), scope)
	for _, handler := range handlers {
		if err := handler.call(ctx, JSON, []byte(`{"orderId":"o-1"}`)); err != nil {
			t.Fatalf("handler: %v", err)
		}
	}
	is(t, strings.Join(order, ","), "first,second")
}

// The default processor comes before the named ones, whatever their names sort like.
func TestTheDefaultProcessorComesFirst(t *testing.T) {
	app := eventsApp(t, WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), ConsumerEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor("audit")); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	is(t, len(app.processors), 2)
	is(t, app.processors[0].name, DefaultProcessor)
	is(t, app.processors[1].name, "audit")
}
