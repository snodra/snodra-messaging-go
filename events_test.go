package snodra

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"
)

// OrderPlaced is an event the tests publish.
type OrderPlaced struct {
	OrderID string `json:"orderId"`
	Cents   int64  `json:"cents"`
}

func (OrderPlaced) MessageType() string { return "orders.OrderPlaced" }

// OrderShipped is another event, published by handlers.
type OrderShipped struct {
	OrderID string `json:"orderId"`
}

func (OrderShipped) MessageType() string { return "orders.OrderShipped" }

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// testLogger is a logger that drops every record.
func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// eventsApp builds an app with the two in-memory stores, no connection to a hub and no
// background work started unless the test starts it.
func eventsApp(t *testing.T, opts ...Option) *App {
	t.Helper()
	all := append([]Option{Hubs("127.0.0.1:1"), Logger(testLogger())}, opts...)
	return newApp(t, all...)
}

// claimEntry claims the next entry of the store's outbox for the test.
func claimEntry(t *testing.T, outbox OutboxStore) OutboxEntry {
	t.Helper()
	entries, err := outbox.Claim(context.Background(), "the-test", time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no outbox entry")
	}
	if len(entries) > 1 {
		t.Fatalf("got %d outbox entries, want 1", len(entries))
	}
	return entries[0]
}

// L2-HEADERS: every published event carries the seven headers, with the content messages.md
// gives them.
func TestPublishFillsTheSevenHeaders(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events), OutboxEnabled(false))
	at := time.Date(2026, 11, 26, 14, 3, 22, 0, time.UTC)
	app.now = func() time.Time { return at }
	ctx := withHandling(context.Background(), &handling{
		info:          MessageInfo{ID: "cmd-1", Type: "orders.PlaceOrder", CorrelationID: "correlation-1"},
		caller:        &Caller{Subject: "user-7"},
		traceparent:   testTraceparent,
		authorization: "Bearer secret",
	})
	if err := Publish(ctx, app, OrderPlaced{OrderID: "o-1", Cents: 1250}, Key("order-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	entry := claimEntry(t, store.Outbox())
	if entry.Key == nil || *entry.Key != "order-1" {
		t.Fatalf("key: %v", entry.Key)
	}
	// The entry keeps what the relay cannot derive; type, id, timestamp and source come from the
	// entry when the relay builds the event.
	is(t, entry.Type, "orders.OrderPlaced")
	is(t, len(entry.ID), 36)
	is(t, entry.Headers[HeaderCorrelationID], "correlation-1")
	is(t, entry.Headers[HeaderUserID], "user-7")
	is(t, entry.Headers[HeaderTraceparent], testTraceparent)
	for _, derived := range []string{HeaderType, HeaderID, HeaderTimestamp, HeaderSource} {
		if _, ok := entry.Headers[derived]; ok {
			t.Errorf("the outbox entry carries the derived header %q", derived)
		}
	}
	if strings.Contains(string(entry.Payload), "Bearer") {
		t.Fatalf("the token crossed into the payload: %s", entry.Payload)
	}
	relay := newOutboxRelay(app)
	outgoing := relay.outgoing(entry)
	is(t, outgoing.Source, DefaultEventsSource)
	is(t, outgoing.ID, entry.ID)
	is(t, outgoing.Type, "orders.OrderPlaced")
	is(t, outgoing.Headers[HeaderType], "orders.OrderPlaced")
	is(t, outgoing.Headers[HeaderID], entry.ID)
	is(t, outgoing.Headers[HeaderCorrelationID], "correlation-1")
	is(t, outgoing.Headers[HeaderTimestamp], "2026-11-26T14:03:22.000Z")
	is(t, outgoing.Headers[HeaderSource], "orders-service")
	is(t, outgoing.Headers[HeaderUserID], "user-7")
	is(t, outgoing.Headers[HeaderTraceparent], testTraceparent)
	for _, name := range EventHeaders {
		if _, ok := outgoing.Headers[name]; !ok {
			t.Errorf("the event has no %q header", name)
		}
	}
	is(t, len(outgoing.Headers), len(EventHeaders))
}

// Outside any handling the event has a fresh correlation id, no userId and no traceparent.
func TestPublishOutsideAnyHandling(t *testing.T) {
	store := NewMemoryMessageStore()
	app := eventsApp(t, WithStore(store), WithEventStore(NewMemoryEventStore()), OutboxEnabled(false))
	if err := Publish(context.Background(), app, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	entry := claimEntry(t, store.Outbox())
	if len(entry.Headers[HeaderCorrelationID]) != 36 {
		t.Fatalf("correlation id %q", entry.Headers[HeaderCorrelationID])
	}
	is(t, entry.Headers[HeaderUserID], "")
	if _, ok := entry.Headers[HeaderTraceparent]; ok {
		t.Fatal("a traceparent outside any trace")
	}
	is(t, eventTimestamp(entry.CreatedAt), entry.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"))
	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`).MatchString(eventTimestamp(entry.CreatedAt)) {
		t.Fatalf("timestamp %q", eventTimestamp(entry.CreatedAt))
	}
}

// An event published while handling a command carries that command's correlation id, and the
// verified caller's subject as its userId; the token stays behind.
func TestPublishInsideACommandCarriesTheCommand(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events), OutboxEnabled(false))
	var command MessageInfo
	if err := HandleCommand(app, func(ctx context.Context, cmd PlaceOrder) (Empty, error) {
		command, _ = MessageFrom(ctx)
		return Empty{}, Publish(ctx, app, OrderPlaced{OrderID: cmd.OrderID, Cents: 1})
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx := withHandling(context.Background(), &handling{
		info:   MessageInfo{ID: "cmd-1", CorrelationID: "correlation-1"},
		caller: &Caller{Subject: "user-7"},
	})
	// Send answers in-process; the handling context above is the caller's.
	if _, err := Send[Empty](ctx, app, PlaceOrder{OrderID: "o-1"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	is(t, command.CorrelationID, "correlation-1")
	entry := claimEntry(t, store.Outbox())
	is(t, entry.Headers[HeaderCorrelationID], "correlation-1")
	is(t, entry.Headers[HeaderUserID], "user-7")
	if strings.Contains(string(entry.Payload), "Bearer") {
		t.Fatal("the token crossed into the event")
	}
}

// L2-OUTBOX: an event published in the caller's transaction is appended in it and reaches the
// outbox only when the transaction commits.
func TestPublishJoinsTheCallersTransaction(t *testing.T) {
	store := NewMemoryMessageStore()
	app := eventsApp(t, WithStore(store), WithEventStore(NewMemoryEventStore()), OutboxEnabled(false))
	ctx := context.Background()

	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(txctx, app, OrderPlaced{OrderID: "rolled-back"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 0 {
		t.Fatalf("pending before the commit: %d, %v", pending, err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 0 {
		t.Fatalf("pending after the rollback: %d, %v", pending, err)
	}

	txctx, tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(txctx, app, OrderPlaced{OrderID: "committed"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 1 {
		t.Fatalf("pending after the commit: %d, %v", pending, err)
	}
}

// A payload the outbox contract refuses is refused at publish, naming the event.
func TestPublishRefusesAnEventTheOutboxWouldRefuse(t *testing.T) {
	app := eventsApp(t, WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()))
	var missing *OrderPlaced
	err := Publish(context.Background(), app, missing)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "orders.OrderPlaced") || !strings.Contains(err.Error(), "JSON object") {
		t.Fatalf("got %q, want it to name the event and the JSON object rule", err)
	}
}
