package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

type sagaState struct {
	Orders int    `json:"orders"`
	Step   string `json:"step,omitempty"`
}

type resolveOrder struct {
	OrderID string `json:"orderId"`
}

type confirmed struct {
	OrderID string `json:"orderId"`
}

func (confirmed) MessageType() string { return "conformance.Confirmed" }

func (resolveOrder) MessageType() string { return "orders.ResolveOrder" }

func sagaTestApp(t *testing.T) (*App, *MemoryMessageStore, *MemoryEventStore) {
	t.Helper()
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), OutboxEnabled(false))
	return app, store, events
}

func sagaStateOf(t *testing.T, store *MemoryMessageStore, id string) (sagaState, *Saga) {
	t.Helper()
	doc, err := store.Sagas().Sagas.FindByID(context.Background(), id)
	if err != nil || doc == nil {
		t.Fatalf("saga %s: %+v, %v", id, doc, err)
	}
	var state sagaState
	if err := (jsonCodec{}).Unmarshal([]byte(doc.State), &state); err != nil {
		t.Fatal(err)
	}
	return state, doc
}

func TestASagaStartsOnceAndRecordsItsDispatch(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(ctx context.Context, saga *SagaContext, state *sagaState, event OrderPlaced) error {
				state.Orders++
				if err := saga.Send(resolveOrder{OrderID: event.OrderID}, "o-"+event.OrderID); err != nil {
					return err
				}
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	payload := []byte(`{"orderId":"o-1","cents":10}`)
	msg := sagaMessage{id: "e-1", typeName: "orders.OrderPlaced", correlationID: "c-1", headers: map[string]string{HeaderCorrelationID: "c-1"}}
	reached, err := app.sagaRuntime.process(context.Background(), msg, payload)
	if err != nil || !reached {
		t.Fatalf("process: %t, %v", reached, err)
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if err != nil || len(live) != 1 {
		t.Fatalf("live: %+v, %v", live, err)
	}
	state, doc := sagaStateOf(t, store, live[0].ID)
	if state.Orders != 1 || doc.Version != 0 || doc.Ended || doc.StartKey == nil {
		t.Fatalf("the saga: %+v, state %+v", doc, state)
	}
	entry, err := store.Sagas().Dispatch.NextPending(context.Background(), doc.ID)
	if err != nil || entry == nil {
		t.Fatalf("dispatch: %+v, %v", entry, err)
	}
	if entry.Seq != 0 || entry.Type != "orders.ResolveOrder" || entry.Headers[sagaCorrelationHeader] != "c-1" ||
		entry.RoutingKey == nil || *entry.RoutingKey != "o-o-1" {
		t.Fatalf("the dispatch entry: %+v", entry)
	}

	// The same event again is a duplicate: no second handling, no second dispatch.
	reached, err = app.sagaRuntime.process(context.Background(), msg, payload)
	if err != nil || !reached {
		t.Fatalf("the duplicate: %t, %v", reached, err)
	}
	state, doc = sagaStateOf(t, store, doc.ID)
	if state.Orders != 1 {
		t.Fatalf("the duplicate changed the state: %+v", state)
	}
	if next, err := store.Sagas().Dispatch.NextPending(context.Background(), doc.ID); err != nil || next == nil || next.ID != entry.ID {
		t.Fatalf("the duplicate changed the dispatch: %+v, %v", next, err)
	}
}

func TestASagaHandlesLaterEventsAtTheNextVersion(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
				state.Orders++
				return nil
			}))
		d.On(HandledOn(Associate("orderId", func(e confirmed) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ confirmed) error {
				state.Step = "confirmed"
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-2", typeName: "conformance.Confirmed"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if len(live) != 1 {
		t.Fatalf("live: %+v", live)
	}
	state, doc := sagaStateOf(t, store, live[0].ID)
	if state.Step != "confirmed" || doc.Version != 1 {
		t.Fatalf("the saga: %+v, state %+v", doc, state)
	}
}

func TestASagaEventThatFailsAllAttemptsIsParked(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error {
				return errors.New("the saga cannot handle it")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-1", headers: map[string]string{HeaderCorrelationID: "c-1"}}, []byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	failure, err := store.Sagas().Failures.Find(context.Background(), "orders:e-1")
	if err != nil || failure == nil {
		t.Fatalf("the parked failure: %+v, %v", failure, err)
	}
	if failure.SagaType != "orders" || failure.EventType != "orders.OrderPlaced" || !strings.Contains(failure.Error, "cannot handle") {
		t.Fatalf("the failure: %+v", failure)
	}
	if exists, _ := store.Sagas().Markers.Exists(context.Background(), "saga:orders:e-1"); !exists {
		t.Fatal("a parked event has no saga marker")
	}
}

func TestTheSagaRelaySendsACommandLocallyUnderItsLease(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var calls atomic.Int64
	var idempotency, sagaID, correlation string
	if err := HandleCommand(app, func(ctx context.Context, command resolveOrder) (Empty, error) {
		calls.Add(1)
		info, _ := MessageFrom(ctx)
		idempotency, sagaID, correlation = info.Headers["idempotency-key"], info.Headers["saga-id"], info.CorrelationID
		return Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-1", headers: map[string]string{HeaderCorrelationID: "c-1"}}, []byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("the command ran while the saga was handling")
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// poll claims the saga and sends; the lease is released at the end.
	if calls.Load() != 1 {
		t.Fatalf("the command ran %d times", calls.Load())
	}
	if idempotency == "" || sagaID == "" || correlation != "c-1" {
		t.Fatalf("the command headers: idempotency %q, saga %q, correlation %q", idempotency, sagaID, correlation)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	entry, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID)
	if err != nil || entry != nil {
		t.Fatalf("the entry after sending: %+v, %v", entry, err)
	}
}

func TestASagaCommandFailureComesBackLocally(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
		return Empty{}, PermissionDenied("no caller")
	}); err != nil {
		t.Fatal(err)
	}
	var failed SagaCommandFailed
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
		d.OnCommandFailed(CommandFailed(func(_ context.Context, _ *SagaContext, state *sagaState, event SagaCommandFailed) error {
			failed = event
			state.Step = string(event.Reason)
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-1", headers: map[string]string{HeaderCorrelationID: "c-1"}}, []byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	state, doc := sagaStateOf(t, store, live[0].ID)
	if failed.Reason != SagaCommandFailedDenied || state.Step != string(SagaCommandFailedDenied) || doc.Version != 1 {
		t.Fatalf("the notice: %+v, state %+v, doc %+v", failed, state, doc)
	}
}

func TestASagaDeadlineFiresOnce(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				_, err := saga.Schedule(0, "timeout", nil)
				return err
			}))
		d.OnDeadline(Deadline("timeout", func(_ context.Context, _ *SagaContext, state *sagaState, _ any) error {
			state.Step = "timed out"
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	scheduler := newDeadlineScheduler(app)
	scheduler.poll(context.Background())
	state, doc := sagaStateOf(t, store, live[0].ID)
	if state.Step != "timed out" || doc.Version != 1 {
		t.Fatalf("the fired deadline: %+v, %+v", doc, state)
	}
	// A second poll claims nothing: the deadline is FIRED.
	scheduler.poll(context.Background())
	state, _ = sagaStateOf(t, store, live[0].ID)
	if state.Step != "timed out" {
		t.Fatalf("the deadline fired twice: %+v", state)
	}
	_ = time.Now()
}

// The saga stage runs after stage 1 committed, and also when stage 1 failed.
func TestTheSagaStageRunsAfterAFailedStageOne(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	settings := consumerSettings()
	settings.consumerTries = 1
	app, _ := l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			return errors.New("stage 1 fails")
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
					state.Step = "saga ran"
					return nil
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings))
	publishTestEvent(t, events, "e-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the saga after a failed stage 1", func() bool {
		live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "e-1")
		return err == nil && len(live) == 1
	})
	_ = app
}

// A record no ordinary handler wants still reaches the sagas.
func TestASagaRunsForARecordWithoutAnEventHandler(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	_, _ = l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
					state.Step = "saga only"
					return nil
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "e-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the saga-only record", func() bool {
		live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "e-1")
		return err == nil && len(live) == 1
	})
}

// L4-SAGA-DISPATCH: an event entry reaches the outbox in the relay's transaction.
func TestASagaPublishedEventReachesTheOutbox(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Publish(OrderShipped{OrderID: event.OrderID}, "order-"+event.OrderID)
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-1", headers: map[string]string{HeaderCorrelationID: "c-1"}}, []byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	dispatchEntry, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID)
	if err != nil || dispatchEntry == nil {
		t.Fatalf("the dispatch entry: %+v, %v", dispatchEntry, err)
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Outbox().Claim(context.Background(), "test", time.Minute, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("the outbox: %+v, %v", entries, err)
	}
	if entries[0].Type != "orders.OrderShipped" || entries[0].Key == nil || *entries[0].Key != "order-o-1" ||
		entries[0].Headers[HeaderCorrelationID] != "c-1" {
		t.Fatalf("the outbox entry: %+v", entries[0])
	}
	if entries[0].ID == "" || entries[0].ID == dispatchEntry.ID {
		t.Fatalf("the outbox event id: %q, the dispatch entry %q", entries[0].ID, dispatchEntry.ID)
	}
	if next, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID); err != nil || next != nil {
		t.Fatalf("the dispatched event entry: %+v, %v", next, err)
	}
}

// L4-SAGA-DISPATCH: one saga's entries are sent in seq order.
func TestASagasEntriesAreSentInSeqOrder(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var mu sync.Mutex
	var order []string
	if err := HandleCommand(app, func(ctx context.Context, command resolveOrder) (Empty, error) {
		mu.Lock()
		order = append(order, command.OrderID)
		mu.Unlock()
		return Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: "first-" + event.OrderID}, "")
			}))
		d.On(HandledOn(Associate("orderId", func(e confirmed) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event confirmed) error {
				return saga.Send(resolveOrder{OrderID: "second-" + event.OrderID}, "")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-2", typeName: "conformance.Confirmed"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if live[0].Version != 1 {
		t.Fatalf("the version: %d", live[0].Version)
	}
	if err := newSagaRelay(app).poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "first-o-1" || order[1] != "second-o-1" {
		t.Fatalf("the send order: %v", order)
	}
}

// L4-COMMAND-FAILED: the retry taxonomy classifies every documented outcome.
func TestTheSagaCommandFailureTaxonomy(t *testing.T) {
	cases := []struct {
		err    error
		kind   sagaSendKind
		reason FailureReason
	}{
		{nil, sagaSendPermanent, ""},
		{ErrNeverSent, sagaSendTransient, FailureExhausted},
		{ErrOutcomeUnknown, sagaSendTransient, FailureExhausted},
		{&Error{Code: CodeTimeout}, sagaSendTransient, FailureExhausted},
		{&Error{Code: CodeHandlerDisconnected}, sagaSendTransient, FailureExhausted},
		{&Error{Code: CodeResourceExhausted}, sagaSendTransient, FailureExhausted},
		{&Error{Code: CodeNoHandler}, sagaSendTransient, FailureExhausted},
		{&Error{Code: CodeUnauthenticated}, sagaSendPermanent, FailureDenied},
		{&Error{Code: CodePermissionDenied}, sagaSendPermanent, FailureDenied},
		{&Error{Code: CodeInvalidRequest}, sagaSendPermanent, FailureInvalid},
		{&Error{Code: CodeHandlerError, Details: []byte(` {"snodra":"retryable"} `)}, sagaSendTransient, FailureExhausted},
		{&Error{Code: CodeHandlerError, Details: []byte(`{"snodra":"unknown-outcome"}`)}, sagaSendPermanent, FailureUnknownOutcome},
		{&Error{Code: CodeHandlerError}, sagaSendPermanent, FailureFailed},
		{&Error{Code: CodeCancelled}, sagaSendPermanent, FailureFailed},
		{errors.New("the sender's own storage failed"), sagaSendTransient, FailureExhausted},
		{&localRefusal{errors.New("the call is refused locally")}, sagaSendPermanent, FailureInvalid},
	}
	for _, c := range cases {
		kind, reason, _ := classifySagaSend(c.err)
		if kind != c.kind || reason != c.reason {
			t.Fatalf("%v: got %v/%v, want %v/%v", c.err, kind, reason, c.kind, c.reason)
		}
	}
}

// L4-COMMAND-FAILED: a permanent failure is recorded before its notice, and the later entries go on.
func TestASagaCommandFailureLetsLaterEntriesGoOn(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var calls atomic.Int64
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
		if calls.Add(1) == 1 {
			return Empty{}, PermissionDenied("no caller")
		}
		return Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	var failed SagaCommandFailed
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				if err := saga.Send(resolveOrder{OrderID: "one"}, ""); err != nil {
					return err
				}
				return saga.Send(resolveOrder{OrderID: "two"}, "")
			}))
		d.OnCommandFailed(CommandFailed(func(_ context.Context, _ *SagaContext, _ *sagaState, event SagaCommandFailed) error {
			failed = event
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-1", headers: map[string]string{HeaderCorrelationID: "c-1"}}, []byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("the later command did not run: %d calls", calls.Load())
	}
	if failed.Reason != SagaCommandFailedDenied || failed.CommandType != "orders.ResolveOrder" {
		t.Fatalf("the notice: %+v", failed)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if next, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID); err != nil || next != nil {
		t.Fatalf("pending after both: %+v, %v", next, err)
	}
}

// L4-DEADLINES: a cancelled deadline never fires.
func TestACancelledSagaDeadlineNeverFires(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				id, err := saga.Schedule(0, "timeout", nil)
				if err != nil {
					return err
				}
				saga.CancelDeadline(id)
				return nil
			}))
		d.OnDeadline(Deadline("timeout", func(_ context.Context, _ *SagaContext, state *sagaState, _ any) error {
			state.Step = "fired"
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	state, _ := sagaStateOf(t, store, live[0].ID)
	if state.Step == "fired" {
		t.Fatal("a cancelled deadline fired")
	}
}

// L4-DEADLINES: two schedulers on one store fire a due deadline once.
func TestTwoSchedulersFireOneDeadlineOnce(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	var fired atomic.Int64
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				_, err := saga.Schedule(0, "timeout", nil)
				return err
			}))
		d.OnDeadline(Deadline("timeout", func(_ context.Context, _ *SagaContext, _ *sagaState, _ any) error {
			fired.Add(1)
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			newDeadlineScheduler(app).poll(context.Background())
		}()
	}
	wg.Wait()
	if fired.Load() != 1 {
		t.Fatalf("the deadline fired %d times", fired.Load())
	}
}

// L4-SAGA-ONCE: a second instance continues a saga on the same store at its next version.
func TestASecondInstanceContinuesASaga(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app1 := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), OutboxEnabled(false))
	app2 := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), OutboxEnabled(false))
	define := func(app *App) {
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
					state.Orders++
					return nil
				}))
			d.On(HandledOn(Associate("orderId", func(e confirmed) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ confirmed) error {
					state.Step = "continued"
					return nil
				}))
		}); err != nil {
			t.Fatal(err)
		}
		app.sagaRuntime = newSagaRuntime(app)
	}
	define(app1)
	define(app2)
	if _, err := app1.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := app2.sagaRuntime.process(context.Background(), sagaMessage{id: "e-2", typeName: "conformance.Confirmed"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if len(live) != 1 {
		t.Fatalf("live: %+v", live)
	}
	state, doc := sagaStateOf(t, store, live[0].ID)
	if state.Step != "continued" || doc.Version != 1 || state.Orders != 1 {
		t.Fatalf("the continued saga: %+v, %+v", doc, state)
	}
}

// L4-COMMAND-FAILED: a notice that fails is retried on its own backoff.
func TestASagaCommandFailureNoticeIsRetried(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	settings := app.cfg.events
	settings.sagaDispatchTries = 2
	settings.sagaDispatchWait = time.Millisecond
	settings.sagaDispatchMax = 5 * time.Millisecond
	app.cfg.events = settings
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
		return Empty{}, errors.New("the command fails")
	}); err != nil {
		t.Fatal(err)
	}
	var noticeAttempts atomic.Int64
	if err := HandleEvent(app, func(context.Context, SagaCommandFailed) error {
		if noticeAttempts.Add(1) == 1 {
			return errors.New("the notice delivery fails once")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var failed SagaCommandFailed
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
		d.OnCommandFailed(CommandFailed(func(_ context.Context, _ *SagaContext, _ *sagaState, event SagaCommandFailed) error {
			failed = event
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	app.freezeProcessors()
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The first notice delivery failed; the next poll after its backoff retries it.
	time.Sleep(3 * time.Millisecond)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if noticeAttempts.Load() < 2 || failed.Reason != SagaCommandFailedFailed {
		t.Fatalf("the retried notice: attempts %d, %+v", noticeAttempts.Load(), failed)
	}
}

// L4-SAGA-DISPATCH: a command through the hub carries the entry's id and the saga's id, the
// handled event's correlation and trace, and no authorization or caller identity; a type the
// service handles itself still goes to the hub when every request does.
func TestASagaCommandThroughTheHub(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	settings := consumerSettings()
	settings.sagaPoll = time.Hour // the test polls by hand
	var localCalls atomic.Int64
	app, s := l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
			localCalls.Add(1)
			return Empty{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
					return saga.Send(resolveOrder{OrderID: event.OrderID}, "order-"+event.OrderID)
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings),
		AlwaysRouteThroughHub(true), ForwardJWT(true))
	live := func() *Saga {
		t.Helper()
		docs, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
		if err != nil || len(docs) != 1 {
			t.Fatalf("live: %+v, %v", docs, err)
		}
		return &docs[0]
	}
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-1", traceparent: testTraceparent,
		headers: map[string]string{HeaderCorrelationID: "c-1", HeaderTraceparent: testTraceparent}},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	doc := live()
	done := make(chan error, 1)
	go func() { done <- newSagaRelay(app).poll(context.Background()) }()
	req := s.RecvRequest(t)
	if req.GetKind() != hubpb.RequestKind_COMMAND || req.GetType() != "orders.ResolveOrder" {
		t.Fatalf("the request: %+v", req)
	}
	if string(req.GetPayload()) != `{"orderId":"o-1"}` {
		t.Fatalf("the payload: %s", req.GetPayload())
	}
	if got := req.GetRoutingKey(); got != "order-o-1" {
		t.Fatalf("the routing key: %q", got)
	}
	if pending, err := store.Sagas().Dispatch.NextPending(context.Background(), doc.ID); err != nil || pending == nil {
		t.Fatalf("the entry was marked dispatched before the reply: %+v, %v", pending, err)
	}
	if got := req.GetHeaders()["idempotency-key"]; got != doc.ID+":0" {
		t.Fatalf("the idempotency key: %q, want %q", got, doc.ID+":0")
	}
	if got := req.GetHeaders()["saga-id"]; got != doc.ID {
		t.Fatalf("the saga id header: %q, want %q", got, doc.ID)
	}
	if got := req.GetHeaders()["correlationId"]; got != "c-1" {
		t.Fatalf("the correlation id: %q", got)
	}
	if got := req.GetHeaders()["traceparent"]; got != testTraceparent {
		t.Fatalf("the traceparent: %q", got)
	}
	if _, ok := req.GetHeaders()["authorization"]; ok {
		t.Fatalf("a saga command carries an authorization header: %v", req.GetHeaders())
	}
	s.Send(t, replyTo(req.GetId(), "{}"))
	if err := receive(t, done); err != nil {
		t.Fatalf("the relay: %v", err)
	}
	if localCalls.Load() != 0 {
		t.Fatalf("the local handler ran although every request routes through the hub")
	}
	if next, err := store.Sagas().Dispatch.NextPending(context.Background(), doc.ID); err != nil || next != nil {
		t.Fatalf("the dispatched entry: %+v, %v", next, err)
	}
}

// conflictOnceStore injects one optimistic-version conflict into the saga repository, so the
// retry path is exercised without depending on two racing goroutines.
type conflictOnceStore struct {
	*MemoryMessageStore
	sagas SagaStores
	once  atomic.Bool
}

func (s *conflictOnceStore) Sagas() SagaStores { return s.sagas }

type conflictOnceRepo struct {
	SagaRepository
	once *atomic.Bool
}

func (r *conflictOnceRepo) Update(ctx context.Context, saga Saga, expected int64) (bool, error) {
	if r.once.CompareAndSwap(false, true) {
		return false, fmt.Errorf("%w: injected for the test", ErrConflict)
	}
	return r.SagaRepository.Update(ctx, saga, expected)
}

// L4-SAGA-ONCE: a conflicting state write retries in a new transaction, so no event is lost and
// no state write is applied twice.
func TestASagaConflictRetriesTheEvent(t *testing.T) {
	base := NewMemoryMessageStore()
	store := &conflictOnceStore{MemoryMessageStore: base}
	store.sagas = base.Sagas()
	store.sagas.Sagas = &conflictOnceRepo{SagaRepository: store.sagas.Sagas, once: &store.once}
	events := NewMemoryEventStore()
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), OutboxEnabled(false))
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
				state.Orders++
				return nil
			}))
		d.On(HandledOn(Associate("orderId", func(e confirmed) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ confirmed) error {
				state.Step = "continued"
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-2", typeName: "conformance.Confirmed"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatalf("the event after the injected conflict: %v", err)
	}
	if !store.once.Load() {
		t.Fatal("the injected conflict did not happen")
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if err != nil || len(live) != 1 {
		t.Fatalf("live: %+v, %v", live, err)
	}
	state, doc := sagaStateOf(t, base, live[0].ID)
	if state.Orders != 1 || state.Step != "continued" || doc.Version != 1 {
		t.Fatalf("the saga: %+v, %+v", doc, state)
	}
}

// L4-DEADLINES: a firing whose handler fails is retried, and a claim that lapses fires again.
func TestASagaDeadlineFiresAfterItsClaimLapses(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	settings := app.cfg.events
	settings.deadlineLease = 20 * time.Millisecond
	settings.sagaHandlerAttempts = 2
	settings.sagaHandlerPause = time.Millisecond
	app.cfg.events = settings
	var calls atomic.Int64
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				_, err := saga.Schedule(0, "timeout", nil)
				return err
			}))
		d.OnDeadline(Deadline("timeout", func(_ context.Context, _ *SagaContext, state *sagaState, _ any) error {
			if calls.Add(1) <= 2 {
				return errors.New("the deadline handler is not ready")
			}
			state.Step = "fired"
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	first := newDeadlineScheduler(app)
	first.poll(context.Background())
	if calls.Load() != 2 {
		t.Fatalf("the first firing made %d handler calls, want the two quick attempts", calls.Load())
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if state, _ := sagaStateOf(t, store, live[0].ID); state.Step != "" {
		t.Fatalf("the failed firing wrote the state: %+v", state)
	}
	time.Sleep(30 * time.Millisecond) // let the claim lapse
	newDeadlineScheduler(app).poll(context.Background())
	if calls.Load() != 3 {
		t.Fatalf("the handler ran %d times, want one more after the claim lapsed", calls.Load())
	}
	if state, _ := sagaStateOf(t, store, live[0].ID); state.Step != "fired" {
		t.Fatalf("the deadline did not fire: %+v", state)
	}
}

// A record SEQUENCED behind a parked letter still reaches the sagas of default.
func TestASagaSeesASequencedRecord(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	settings := consumerSettings()
	settings.consumerTries = 1
	events := NewMemoryEventStore()
	_, _ = l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			return errors.New("stage 1 always fails")
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
					state.Orders++
					return nil
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings))
	publish := func(id string) {
		t.Helper()
		key := "o-1"
		err := events.Publish(context.Background(), OutgoingEvent{
			Source: DefaultEventsSource, ID: id, Type: "orders.OrderPlaced", Key: &key,
			Headers: map[string]string{HeaderType: "orders.OrderPlaced", HeaderID: id,
				HeaderCorrelationID: "c-" + id, HeaderTimestamp: eventTimestamp(time.Now()),
				HeaderSource: "test-service"},
			Payload: []byte(`{"orderId":"o-1"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	publish("e-1")
	waitUntil(t, "the first saga", func() bool {
		live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
		return err == nil && len(live) == 1
	})
	publish("e-2")
	waitUntil(t, "the sequenced record's saga", func() bool {
		live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
		if err != nil || len(live) != 1 {
			return false
		}
		var state sagaState
		if err := (jsonCodec{}).Unmarshal([]byte(live[0].State), &state); err != nil {
			return false
		}
		return state.Orders == 2
	})
}

// Registration refuses an invalid saga before Start, and Start reports it like any other.
func TestSagaRegistrationRefusesBadDefinitions(t *testing.T) {
	newTestApp := func() *App {
		return newApp(t, Hubs("127.0.0.1:1"), WithStore(NewMemoryMessageStore()),
			WithEventStore(NewMemoryEventStore()), OutboxEnabled(false))
	}
	if err := HandleSaga[sagaState](newTestApp(), "", nil); err == nil || !strings.Contains(err.Error(), "needs a name") {
		t.Fatalf("the empty name: %v", err)
	}
	if err := HandleSaga[sagaState](newTestApp(), "orders", nil); err == nil || !strings.Contains(err.Error(), "nil definition") {
		t.Fatalf("the nil definition: %v", err)
	}
	if err := HandleSaga[sagaState](newTestApp(), "orders", func(*SagaDefinition[sagaState]) {}); err == nil ||
		!strings.Contains(err.Error(), "no start handler") {
		t.Fatalf("the saga without a start handler: %v", err)
	}
	if err := HandleSaga[[]int](newTestApp(), "orders", func(d *SagaDefinition[[]int]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *[]int, OrderPlaced) error { return nil }))
	}); err == nil || !strings.Contains(err.Error(), "not a JSON object") {
		t.Fatalf("the non-object state: %v", err)
	}
	app := newTestApp()
	define := func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
	}
	if err := HandleSaga[sagaState](app, "orders", define); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", define); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("the duplicate name: %v", err)
	}
	deadlineTwice := func(d *SagaDefinition[sagaState]) {
		define(d)
		handler := Deadline("timeout", func(context.Context, *SagaContext, *sagaState, any) error { return nil })
		d.OnDeadline(handler)
		d.OnDeadline(handler)
	}
	if err := HandleSaga[sagaState](newTestApp(), "timed", deadlineTwice); err == nil ||
		!strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("the duplicate deadline: %v", err)
	}
	onAStart := func(d *SagaDefinition[sagaState]) {
		d.On(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
	}
	if err := HandleSaga[sagaState](newTestApp(), "wrong", onAStart); err == nil ||
		!strings.Contains(err.Error(), "On needs a HandledOn handler") {
		t.Fatalf("On with a start handler: %v", err)
	}
	bad := newTestApp()
	if err := HandleSaga[sagaState](bad, "orders", func(*SagaDefinition[sagaState]) {}); err == nil {
		t.Fatal("the invalid saga passed registration")
	}
	if err := bad.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "no start handler") {
		t.Fatalf("Start did not return the registration error: %v", err)
	}
}

// A saga-only service needs the message and event stores, and Start names the saga.
func TestASagaOnlyServiceNeedsBothStores(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"), OutboxEnabled(false))
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
	}); err != nil {
		t.Fatal(err)
	}
	err := app.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "the saga needs a message store") {
		t.Fatalf("Start without stores: %v", err)
	}
}

// A saga handling that fails records neither its command nor its state: the transaction rolls
// back before the relay can see anything.
func TestASagaThatFailsRecordsNoDispatch(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				if err := saga.Send(resolveOrder{OrderID: event.OrderID}, ""); err != nil {
					return err
				}
				return errors.New("the handler fails after recording")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Sagas().Dispatch.Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("a rolled-back handling recorded %d dispatch entries (%v)", pending, err)
	}
	if failure, err := store.Sagas().Failures.Find(context.Background(), "orders:e-1"); err != nil || failure == nil {
		t.Fatalf("the parked failure: %+v, %v", failure, err)
	}
}

// The failure notice reaches the service's ordinary handlers and the saga, and is never published
// to the event store or the outbox.
func TestASagaCommandFailureNoticeIsLocalOnly(t *testing.T) {
	app, store, events := sagaTestApp(t)
	var ordinary, inSaga atomic.Bool
	if err := HandleEvent(app, func(context.Context, SagaCommandFailed) error {
		ordinary.Store(true)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
		return Empty{}, PermissionDenied("no caller")
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
		d.OnCommandFailed(CommandFailed(func(context.Context, *SagaContext, *sagaState, SagaCommandFailed) error {
			inSaga.Store(true)
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	app.freezeProcessors()
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := newSagaRelay(app).poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ordinary.Load() || !inSaga.Load() {
		t.Fatalf("the notice reached ordinary handlers %t, the saga %t", ordinary.Load(), inSaga.Load())
	}
	if published := events.Events(DefaultEventsSource); len(published) != 0 {
		t.Fatalf("the notice was published: %+v", published)
	}
	if entries, err := store.Outbox().Claim(context.Background(), "test", time.Minute, 10); err != nil || len(entries) != 0 {
		t.Fatalf("the notice reached the outbox: %+v, %v", entries, err)
	}
}

// A ForceNew start handler starts another instance with no start key, even when one matches.
func TestASagaForceNewStartsWithoutAStartKey(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
				state.Orders++
				return nil
			}).ForceNew())
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	for _, id := range []string{"e-1", "e-2"} {
		if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: id, typeName: "orders.OrderPlaced"},
			[]byte(`{"orderId":"o-1"}`)); err != nil {
			t.Fatal(err)
		}
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if err != nil || len(live) != 2 {
		t.Fatalf("the force-new instances: %+v, %v", live, err)
	}
	for _, doc := range live {
		if doc.StartKey != nil {
			t.Fatalf("a force-new instance kept a start key: %+v", doc)
		}
	}
}

// A deadline's handler receives its payload; the work it records carries the scheduling message's
// correlation id and no trace context.
func TestASagaDeadlineWorkKeepsTheSchedulingCorrelationAndNoTrace(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	type timeoutPayload struct {
		Reason string `json:"reason"`
	}
	var got timeoutPayload
	var deadlineInfo MessageInfo
	deadlineInfoOK := true
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				_, err := saga.ScheduleAt(time.Now(), "timeout", timeoutPayload{Reason: "slow"})
				return err
			}))
		d.OnDeadline(Deadline("timeout", func(ctx context.Context, saga *SagaContext, _ *sagaState, payload timeoutPayload) error {
			got = payload
			deadlineInfo, deadlineInfoOK = MessageFrom(ctx)
			return saga.Send(resolveOrder{OrderID: "from-deadline"}, "")
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced",
		correlationID: "c-schedule", traceparent: testTraceparent,
		headers: map[string]string{HeaderCorrelationID: "c-schedule", HeaderTraceparent: testTraceparent}},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	if got.Reason != "slow" {
		t.Fatalf("the deadline payload: %+v", got)
	}
	if !deadlineInfoOK || deadlineInfo.CorrelationID != "c-schedule" || len(deadlineInfo.Headers) != 0 {
		t.Fatalf("the deadline context: ok %t, info %+v", deadlineInfoOK, deadlineInfo)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	entry, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID)
	if err != nil || entry == nil {
		t.Fatalf("the deadline's command: %+v, %v", entry, err)
	}
	if entry.Headers[sagaCorrelationHeader] != "c-schedule" {
		t.Fatalf("the command's correlation: %q", entry.Headers[sagaCorrelationHeader])
	}
	if _, ok := entry.Headers[sagaTraceparentHeader]; ok {
		t.Fatalf("the deadline's command carries a trace: %v", entry.Headers)
	}
}

// L4-SAGA-DISPATCH: the relay owns the retries; the sender's policy, even a default that would
// retry NO_HANDLER, never stacks under them.
func TestASagaCommandIsNotRetriedByTheSender(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	settings := consumerSettings()
	settings.sagaPoll = time.Hour
	settings.sagaSendTimeout = time.Second
	settings.sagaDispatchWait = time.Second
	settings.sagaDispatchMax = time.Second
	app, s := l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
					return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(NewMemoryEventStore()), withEventSettings(settings),
		DefaultRetry(RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Millisecond,
			Multiplier: 1, MaxBackoff: time.Millisecond}))
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- newSagaRelay(app).poll(context.Background()) }()
	req := s.RecvRequest(t)
	s.Send(t, failWith(req.GetId(), hubpb.ErrorCode_NO_HANDLER, "", ""))
	if err := receive(t, done); err != nil {
		t.Fatalf("the relay: %v", err)
	}
	live, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	entry, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID)
	if err != nil || entry == nil {
		t.Fatalf("the retried entry: %+v, %v", entry, err)
	}
	if entry.Attempts != 1 || entry.NextAttemptAt.IsZero() || !entry.NextAttemptAt.After(time.Now()) {
		t.Fatalf("the relay's attempt: %+v", entry)
	}
	s.Nothing(t, 150*time.Millisecond, "request")
}

// L4-SAGA-DISPATCH: a transient failure holds back only its own saga; another saga's entries are
// sent by the same poll.
func TestASagaRetryDoesNotHoldAnotherSaga(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	settings := app.cfg.events
	settings.sagaDispatchWait = time.Millisecond
	settings.sagaDispatchMax = time.Millisecond
	app.cfg.events = settings
	var mu sync.Mutex
	calls := map[string]int{}
	if err := HandleCommand(app, func(_ context.Context, command resolveOrder) (Empty, error) {
		mu.Lock()
		calls[command.OrderID]++
		first := calls[command.OrderID] == 1
		mu.Unlock()
		if command.OrderID == "o-1" && first {
			return Empty{}, WithDetails(errors.New("the handler is busy"), map[string]string{"snodra": "retryable"})
		}
		return Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	for i, order := range []string{"o-1", "o-2"} {
		if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{
			id: fmt.Sprintf("e-%d", i+1), typeName: "orders.OrderPlaced"},
			[]byte(`{"orderId":"`+order+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if calls["o-1"] != 1 || calls["o-2"] != 1 {
		t.Fatalf("the first poll's calls: %v", calls)
	}
	mu.Unlock()
	first, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	second, _ := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-2")
	if second[0].Version != 0 {
		t.Fatalf("the other saga: %+v", second[0])
	}
	if pending, err := store.Sagas().Dispatch.NextPending(context.Background(), second[0].ID); err != nil || pending != nil {
		t.Fatalf("the other saga's entry: %+v, %v", pending, err)
	}
	waiting, err := store.Sagas().Dispatch.NextPending(context.Background(), first[0].ID)
	if err != nil || waiting == nil || waiting.Attempts != 1 {
		t.Fatalf("the retried saga's entry: %+v, %v", waiting, err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["o-1"] != 2 {
		t.Fatalf("the retried command's calls: %v", calls)
	}
	if pending, err := store.Sagas().Dispatch.NextPending(context.Background(), first[0].ID); err != nil || pending != nil {
		t.Fatalf("the retried saga after success: %+v, %v", pending, err)
	}
}

// listEvent encodes as a JSON array: a valid message payload, but never an outbox event.
type listEvent struct{}

func (listEvent) MessageType() string { return "orders.ListEvent" }

func (listEvent) MarshalJSON() ([]byte, error) { return []byte(`[]`), nil }

// L4-SAGA-DISPATCH: an event the outbox could never hold is refused when the saga records it.
func TestASagaRefusesAnEventTheOutboxCannotHold(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	saga := &SagaContext{app: app, sagaType: "orders", id: "s-1", correlationID: "c-1", now: time.Now()}
	err := saga.Publish(listEvent{}, "")
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("the refused event: %v", err)
	}
	if len(saga.entries) != 0 {
		t.Fatalf("the refused event was recorded: %+v", saga.entries)
	}
}

// L4-SAGA-DISPATCH: different sagas are dispatched at the same time; a slow command of one does
// not hold another back.
func TestDifferentSagasAreDispatchedAtTheSameTime(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	if err := HandleCommand(app, func(_ context.Context, command resolveOrder) (Empty, error) {
		started <- command.OrderID
		<-release
		return Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	for i, order := range []string{"o-1", "o-2"} {
		if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{
			id: fmt.Sprintf("e-%d", i+1), typeName: "orders.OrderPlaced"},
			[]byte(`{"orderId":"`+order+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- newSagaRelay(app).poll(context.Background()) }()
	first, second := receive(t, started), receive(t, started)
	if first == second {
		t.Fatalf("the same saga was dispatched twice: %q", first)
	}
	unblock()
	if err := receive(t, done); err != nil {
		t.Fatalf("the relay: %v", err)
	}
}

// A panicking saga handler is a failed handling, not a crashed process: the event is parked.
func TestASagaHandlerPanicIsParked(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error {
				panic("boom")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	failure, err := store.Sagas().Failures.Find(context.Background(), "orders:e-1")
	if err != nil || failure == nil || !strings.Contains(failure.Error, "panicked: boom") {
		t.Fatalf("the panic's parked failure: %+v, %v", failure, err)
	}
}

// A panicking association value fails the attempt like a handler panic: it is parked.
func TestASagaAssociationPanicIsParked(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(OrderPlaced) string { panic("association boom") }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	failure, err := store.Sagas().Failures.Find(context.Background(), "orders:e-1")
	if err != nil || failure == nil || !strings.Contains(failure.Error, "association boom") {
		t.Fatalf("the association panic's parked failure: %+v, %v", failure, err)
	}
}

// L4-SAGA-DISPATCH: two relays on one store send one saga's entries once; the lease is exclusive.
func TestTwoSagaRelaysSendOneSagaOnce(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	var calls atomic.Int64
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
		calls.Add(1)
		return Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := newSagaRelay(app).poll(context.Background()); err != nil {
				t.Errorf("a relay: %v", err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("the command ran %d times", calls.Load())
	}
}

// A saga name with ':' would make the marker id ambiguous.
func TestASagaNameCannotContainAColon(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(NewMemoryMessageStore()),
		WithEventStore(NewMemoryEventStore()), OutboxEnabled(false))
	err := HandleSaga[sagaState](app, "orders:eu", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
	})
	if err == nil || !strings.Contains(err.Error(), "cannot contain ':'") {
		t.Fatalf("the colon in a saga name: %v", err)
	}
}

// The design skips the saga stage for records inside a replay range, as Kotlin does.
func TestASagaSkipsAReplay(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
				state.Orders++
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	app.freezeTypes()
	p := app.processors[0]
	if p.name != DefaultProcessor {
		t.Fatalf("the first processor is %q", p.name)
	}
	p.run.setReplay([]ReplayRange{{Source: DefaultEventsSource, Segment: "0", From: "0", Until: "1"}})
	delivery := Delivery{Segment: "0", Position: "0",
		Headers: map[string]string{HeaderID: "e-1", HeaderType: "orders.OrderPlaced"},
		Payload: []byte(`{"orderId":"o-1"}`)}
	if err := app.runSagas(p, delivery, "e-1", "orders.OrderPlaced"); err != nil {
		t.Fatal(err)
	}
	if live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1"); err != nil || len(live) != 0 {
		t.Fatalf("the replayed event started a saga: %+v, %v", live, err)
	}
	// Outside the range the same stage starts it, so the range was the reason.
	delivery.Position = "1"
	delivery.Headers[HeaderID] = "e-2"
	if err := app.runSagas(p, delivery, "e-2", "orders.OrderPlaced"); err != nil {
		t.Fatal(err)
	}
	if live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1"); err != nil || len(live) != 1 {
		t.Fatalf("the event after the replay: %+v, %v", live, err)
	}
}

// A local command whose handler honours an ended context is an unknown outcome, not a
// permanent handler failure: the relay retries it with the same idempotency key.
func TestALocalSagaCommandAbandonedAtTheBoundIsTransient(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	if err := HandleCommand(app, func(ctx context.Context, _ resolveOrder) (Empty, error) {
		<-ctx.Done()
		return Empty{}, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := sendRawCommand(ctx, app, "orders.ResolveOrder", []byte(`{"orderId":"o-1"}`), "", nil, 0)
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("the abandoned local command: %v, want ErrOutcomeUnknown", err)
	}
	done, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	err = sendRawCommand(done, app, "orders.ResolveOrder", []byte(`{"orderId":"o-1"}`), "", nil, 0)
	if !errors.Is(err, ErrNeverSent) {
		t.Fatalf("the command with an ended context: %v, want ErrNeverSent", err)
	}
}

// ackCheckEvents checks the saga stage's commit before the subscription acknowledges the record.
type ackCheckEvents struct {
	EventStore
	check  func() bool
	failed atomic.Bool
	acks   atomic.Int64
}

func (e *ackCheckEvents) Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error) {
	sub, err := e.EventStore.Subscribe(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &ackCheckSubscription{EventSubscription: sub, events: e}, nil
}

type ackCheckSubscription struct {
	EventSubscription
	events *ackCheckEvents
}

func (s *ackCheckSubscription) Ack(segment, position string) error {
	if s.events.check != nil && !s.events.check() {
		s.events.failed.Store(true)
	}
	s.events.acks.Add(1)
	return s.EventSubscription.Ack(segment, position)
}

// The record is acknowledged only after both stages committed.
func TestAnEventIsAcknowledgedAfterBothStages(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := &ackCheckEvents{EventStore: NewMemoryEventStore()}
	var stage1 atomic.Bool
	_, _ = l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			stage1.Store(true)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
					state.Orders++
					return nil
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	events.check = func() bool {
		live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "e-1")
		return stage1.Load() && err == nil && len(live) == 1
	}
	publishTestEvent(t, events, "e-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the acknowledgement", func() bool { return events.acks.Load() > 0 })
	if events.failed.Load() {
		t.Fatal("the record was acknowledged before both stages committed")
	}
}

// A duplicate stage-1 marker still runs stage 2: the crash may have happened between the stages.
func TestADuplicateStageOneMarkerStillRunsStageTwo(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	processed, err := store.Processed().Record(context.Background(), ProcessedMarker{
		ID: ProcessorMarker(DefaultProcessor, "e-1"), Type: "orders.OrderPlaced",
		Processor: DefaultProcessor, ProcessedAt: time.Now()})
	if err != nil || !processed {
		t.Fatalf("the stage-1 marker: %t, %v", processed, err)
	}
	var stage1 atomic.Int64
	events := NewMemoryEventStore()
	_, _ = l3ServingAt(t, hub, time.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			stage1.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(_ context.Context, _ *SagaContext, state *sagaState, _ OrderPlaced) error {
					state.Orders++
					return nil
				}))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "e-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the saga after the duplicate stage-1 marker", func() bool {
		live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "e-1")
		return err == nil && len(live) == 1
	})
	if stage1.Load() != 0 {
		t.Fatalf("stage 1 ran despite its marker: %d", stage1.Load())
	}
}

// Two events for one instance in real concurrent transactions both apply once; the optimistic
// version makes the loser retry.
func TestTwoConcurrentEventsApplyToTheSameSagaOnceEach(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
		d.On(HandledOn(Associate("orderId", func(e confirmed) string { return e.OrderID }),
			func(_ context.Context, _ *SagaContext, state *sagaState, _ confirmed) error {
				state.Orders++
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("e-%d", i+2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := app.sagaRuntime.process(context.Background(),
				sagaMessage{id: id, typeName: "conformance.Confirmed"}, []byte(`{"orderId":"o-1"}`))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if err != nil || len(live) != 1 {
		t.Fatalf("live: %+v, %v", live, err)
	}
	state, doc := sagaStateOf(t, store, live[0].ID)
	if state.Orders != 2 || doc.Version != 2 {
		t.Fatalf("the concurrent writes: %+v, %+v", doc, state)
	}
}

// Ending() stops the later handlers of the same event and ends the instance.
func TestASagaEndingStopsTheLaterHandlers(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var sagaID string
	var later atomic.Bool
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, state *sagaState, _ OrderPlaced) error {
				sagaID = saga.SagaID()
				state.Orders++
				return nil
			}).Ending())
		d.On(HandledOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error {
				later.Store(true)
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	doc, err := store.Sagas().Sagas.FindByID(context.Background(), sagaID)
	if err != nil || doc == nil || !doc.Ended {
		t.Fatalf("the ended saga: %+v, %v", doc, err)
	}
	if later.Load() {
		t.Fatal("a handler after Ending ran")
	}
}

// extraConfirmed reaches a saga by an association that is not the one it started with.
type extraConfirmed struct {
	Extra string `json:"extra"`
}

func (extraConfirmed) MessageType() string { return "conformance.ExtraConfirmed" }

// Associations, ScheduleAt, CancelDeadlines, End, and the cancel-on-end rule.
func TestASagaContextAssociationsDeadlinesAndEnd(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var sagaID, cleanupA, cleanupB, laterID string
	var laterFired atomic.Bool
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				sagaID = saga.SagaID()
				saga.Associate("extra", "x")
				saga.Associate("temp", "y")
				saga.RemoveAssociation("temp", "y")
				saga.RemoveAssociation("orderId", event.OrderID)
				cleanupA, _ = saga.Schedule(0, "cleanup", nil)
				cleanupB, _ = saga.Schedule(0, "cleanup", nil)
				laterID, _ = saga.ScheduleAt(time.Now().Add(time.Hour), "later", resolveOrder{OrderID: "later"})
				saga.CancelDeadlines("cleanup")
				return nil
			}))
		d.On(HandledOn(Associate("extra", func(e extraConfirmed) string { return e.Extra }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ extraConfirmed) error {
				saga.End()
				return nil
			}))
		d.OnDeadline(Deadline("later", func(context.Context, *SagaContext, *sagaState, resolveOrder) error {
			laterFired.Store(true)
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	doc, err := store.Sagas().Sagas.FindByID(context.Background(), sagaID)
	if err != nil || doc == nil {
		t.Fatalf("the saga: %+v, %v", doc, err)
	}
	if doc.StartKey != nil {
		t.Fatalf("the removed start association kept a start key: %v", *doc.StartKey)
	}
	if len(doc.Associations) != 1 || doc.Associations[0] != (SagaAssociation{Key: "extra", Value: "x"}) {
		t.Fatalf("the associations: %+v", doc.Associations)
	}
	for _, id := range []string{cleanupA, cleanupB} {
		if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), id); err != nil || deadline != nil {
			t.Fatalf("the cancelled cleanup %s: %+v, %v", id, deadline, err)
		}
	}
	if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), laterID); err != nil || deadline == nil {
		t.Fatalf("the later deadline before the end: %+v, %v", deadline, err)
	}
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-2", typeName: "conformance.ExtraConfirmed"},
		[]byte(`{"extra":"x"}`)); err != nil {
		t.Fatal(err)
	}
	doc, err = store.Sagas().Sagas.FindByID(context.Background(), sagaID)
	if err != nil || doc == nil || !doc.Ended {
		t.Fatalf("the saga after End: %+v, %v", doc, err)
	}
	if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), laterID); err != nil || deadline != nil {
		t.Fatalf("the deadline after the end: %+v, %v", deadline, err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	if laterFired.Load() {
		t.Fatal("a deadline after the saga ended fired")
	}
}

// A command that fails transiently ten times becomes EXHAUSTED after exactly ten sends.
func TestASagaCommandExhaustsItsTenAttempts(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	settings := app.cfg.events
	settings.sagaDispatchWait = time.Millisecond
	settings.sagaDispatchMax = time.Millisecond
	app.cfg.events = settings
	var calls atomic.Int64
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) {
		calls.Add(1)
		return Empty{}, WithDetails(errors.New("still busy"), map[string]string{"snodra": "retryable"})
	}); err != nil {
		t.Fatal(err)
	}
	var failed SagaCommandFailed
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(resolveOrder{OrderID: event.OrderID}, "")
			}))
		d.OnCommandFailed(CommandFailed(func(_ context.Context, _ *SagaContext, _ *sagaState, event SagaCommandFailed) error {
			failed = event
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	relay := newSagaRelay(app)
	waitUntil(t, "the ten attempts", func() bool {
		if err := relay.poll(context.Background()); err != nil {
			t.Fatalf("the relay: %v", err)
		}
		time.Sleep(3 * time.Millisecond)
		return calls.Load() >= 10
	})
	if calls.Load() != 10 {
		t.Fatalf("the command ran %d times", calls.Load())
	}
	if failed.Reason != SagaCommandFailedExhausted {
		t.Fatalf("the exhausted notice: %+v", failed)
	}
}

// The relay's backoff starts at 1 s, doubles to the 5 min cap, and applies the ±20 % jitter.
func TestSagaDispatchBackoffIsJitteredAndCapped(t *testing.T) {
	settings := eventSettings{sagaDispatchWait: time.Second, sagaDispatchMax: 5 * time.Minute}
	for attempts := 1; attempts <= 10; attempts++ {
		nominal := settings.sagaDispatchWait
		for i := 1; i < attempts; i++ {
			nominal *= 2
		}
		if nominal > settings.sagaDispatchMax {
			nominal = settings.sagaDispatchMax
		}
		low := time.Duration(float64(nominal) * 0.8)
		high := time.Duration(float64(nominal) * 1.2)
		if high > settings.sagaDispatchMax {
			high = settings.sagaDispatchMax
		}
		for i := 0; i < 20; i++ {
			got := sagaDispatchWait(settings, attempts)
			if got < low || got > high {
				t.Fatalf("attempt %d: wait %v outside [%v, %v]", attempts, got, low, high)
			}
		}
	}
}

// The deadline scheduler does not fire before the due time, and a handler that calls End cancels
// the instance's scheduled deadlines.
func TestASagaDeadlineDoesNotFireEarlyAndIsCancelledAtTheEnd(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var earlyID, sagaID string
	var earlyFired atomic.Bool
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				sagaID = saga.SagaID()
				earlyID, _ = saga.Schedule(time.Hour, "early", nil)
				return nil
			}))
		d.On(HandledOn(Associate("orderId", func(e confirmed) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ confirmed) error {
				saga.End()
				return nil
			}))
		d.OnDeadline(Deadline("early", func(context.Context, *SagaContext, *sagaState, any) error {
			earlyFired.Store(true)
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	if earlyFired.Load() {
		t.Fatal("a deadline fired before it was due")
	}
	if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), earlyID); err != nil || deadline == nil {
		t.Fatalf("the early deadline: %+v, %v", deadline, err)
	}
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-2", typeName: "conformance.Confirmed"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), earlyID); err != nil || deadline != nil {
		t.Fatalf("the deadline after End: %+v, %v", deadline, err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	if earlyFired.Load() {
		t.Fatal("a deadline fired after the saga ended")
	}
	_ = sagaID
}

// A due deadline whose saga is already ended is cancelled, not fired.
func TestASagaDeadlineForAnEndedSagaIsCancelled(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var goneID, sagaID string
	var fired atomic.Bool
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				sagaID = saga.SagaID()
				goneID, _ = saga.Schedule(0, "gone", nil)
				return nil
			}))
		d.OnDeadline(Deadline("gone", func(context.Context, *SagaContext, *sagaState, any) error {
			fired.Store(true)
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	doc, err := store.Sagas().Sagas.FindByID(context.Background(), sagaID)
	if err != nil || doc == nil {
		t.Fatalf("the saga: %+v, %v", doc, err)
	}
	next := *doc
	next.Ended = true
	next.EndedAt = time.Now()
	next.UpdatedAt = time.Now()
	next.Version = doc.Version + 1
	if ok, err := store.Sagas().Sagas.Update(context.Background(), next, doc.Version); err != nil || !ok {
		t.Fatalf("ending the saga directly: %t, %v", ok, err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	if fired.Load() {
		t.Fatal("a deadline for an ended saga fired")
	}
	if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), goneID); err != nil || deadline != nil {
		t.Fatalf("the ended saga's deadline: %+v, %v", deadline, err)
	}
}

// A due deadline with no handler is cancelled and logged, not fired.
func TestASagaDeadlineWithoutAHandlerIsCancelled(t *testing.T) {
	app, store, _ := sagaTestApp(t)
	var ghostID, sagaID string
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, _ OrderPlaced) error {
				sagaID = saga.SagaID()
				ghostID, _ = saga.Schedule(0, "ghost", nil)
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	newDeadlineScheduler(app).poll(context.Background())
	if deadline, err := store.Sagas().Deadlines.FindScheduled(context.Background(), ghostID); err != nil || deadline != nil {
		t.Fatalf("the handlerless deadline: %+v, %v", deadline, err)
	}
	if state, _ := sagaStateOf(t, store, sagaID); state.Orders != 0 {
		t.Fatalf("the handlerless deadline changed the state: %+v", state)
	}
}

// The app starts and stops the saga relay and the deadline scheduler with the service.
type countingSagaStore struct {
	*MemoryMessageStore
	sagas SagaStores
}

func (s *countingSagaStore) Sagas() SagaStores { return s.sagas }

type countingDispatch struct {
	SagaDispatchStore
	due atomic.Int64
}

func (d *countingDispatch) DueSagas(ctx context.Context, now time.Time, except []string, limit int) ([]string, error) {
	d.due.Add(1)
	return d.SagaDispatchStore.DueSagas(ctx, now, except, limit)
}

type countingDeadlines struct {
	SagaDeadlineStore
	claims atomic.Int64
}

func (d *countingDeadlines) ClaimDue(ctx context.Context, owner string, now, until time.Time) (*SagaDeadline, error) {
	d.claims.Add(1)
	return d.SagaDeadlineStore.ClaimDue(ctx, owner, now, until)
}

func TestSagaRelayAndSchedulerStartAndStopWithTheApp(t *testing.T) {
	hub := fakehub.Start(t)
	base := NewMemoryMessageStore()
	store := &countingSagaStore{MemoryMessageStore: base}
	store.sagas = base.Sagas()
	dispatch := &countingDispatch{SagaDispatchStore: store.sagas.Dispatch}
	deadlines := &countingDeadlines{SagaDeadlineStore: store.sagas.Deadlines}
	store.sagas.Dispatch = dispatch
	store.sagas.Deadlines = deadlines
	settings := consumerSettings()
	settings.sagaPoll = 5 * time.Millisecond
	settings.deadlinePoll = 5 * time.Millisecond
	app, _ := serving(t, hub, func(app *App) {
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(NewMemoryEventStore()), withEventSettings(settings))
	waitUntil(t, "the relay and the scheduler", func() bool {
		return dispatch.due.Load() > 0 && deadlines.claims.Load() > 0
	})
	if err := app.Shutdown(ctxWithin(t, 5*time.Second)); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	due, claims := dispatch.due.Load(), deadlines.claims.Load()
	time.Sleep(30 * time.Millisecond)
	if dispatch.due.Load() != due || deadlines.claims.Load() != claims {
		t.Fatalf("the workers kept running after Shutdown: due %d->%d, claims %d->%d",
			due, dispatch.due.Load(), claims, deadlines.claims.Load())
	}
}

// The default processor's status lists the saga event types, so a saga-only service reports them.
func TestASagaOnlyStatusListsTheSagaEventTypes(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
		d.OnCommandFailed(CommandFailed(func(context.Context, *SagaContext, *sagaState, SagaCommandFailed) error {
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.freezeTypes()
	p := app.processors[0]
	status := app.processorStatus(p, time.Now())
	types := status.GetEventTypes()
	for _, want := range []string{"orders.OrderPlaced", commandFailedTypeName} {
		found := false
		for _, typ := range types {
			if typ == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("the default processor's event types %v miss %q", types, want)
		}
	}
}

// A store without the saga ports fails Start naming the port, not the first event.
type noSagaPortsStore struct{ *MemoryMessageStore }

func (s *noSagaPortsStore) Sagas() SagaStores { return SagaStores{} }

type unsupportedSagaMarkers struct{}

func (unsupportedSagaMarkers) Exists(context.Context, string) (bool, error) {
	return false, ErrUnsupported
}
func (unsupportedSagaMarkers) Insert(context.Context, string, string, time.Time) error {
	return ErrUnsupported
}

type unsupportedSagaStore struct {
	*MemoryMessageStore
	sagas SagaStores
}

func (s *unsupportedSagaStore) Sagas() SagaStores { return s.sagas }

func TestASagaStartupProbesTheStorePorts(t *testing.T) {
	newSagaApp := func(store MessageStore) *App {
		app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store),
			WithEventStore(NewMemoryEventStore()), OutboxEnabled(false))
		if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
			d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
				func(context.Context, *SagaContext, *sagaState, OrderPlaced) error { return nil }))
		}); err != nil {
			t.Fatal(err)
		}
		return app
	}
	err := newSagaApp(&noSagaPortsStore{MemoryMessageStore: NewMemoryMessageStore()}).Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "saga repository") {
		t.Fatalf("the store without saga ports: %v", err)
	}
	base := NewMemoryMessageStore()
	unmarked := &unsupportedSagaStore{MemoryMessageStore: base}
	unmarked.sagas = base.Sagas()
	unmarked.sagas.Markers = unsupportedSagaMarkers{}
	err = newSagaApp(unmarked).Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "saga markers") || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("the store without saga markers: %v", err)
	}
}

// The app-level Send and Publish are refused inside a saga handler; SagaContext is the path.
func TestASagaHandlerCannotUseTheAppLevelSendOrPublish(t *testing.T) {
	app, _, _ := sagaTestApp(t)
	if err := HandleCommand(app, func(context.Context, resolveOrder) (Empty, error) { return Empty{}, nil }); err != nil {
		t.Fatal(err)
	}
	var sendErr, publishErr error
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(ctx context.Context, _ *SagaContext, _ *sagaState, _ OrderPlaced) error {
				_, sendErr = Send[Empty](ctx, app, resolveOrder{OrderID: "x"})
				publishErr = Publish(ctx, app, OrderShipped{OrderID: "x"})
				return nil
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{"Send": sendErr, "Publish": publishErr} {
		if err == nil || !strings.Contains(err.Error(), "SagaContext") {
			t.Fatalf("%s inside a saga handler: %v", name, err)
		}
	}
}

// SG-52: without a hubless service, a hub's NO_HANDLER is the transient row; the permanent
// local NO_HANDLER reason is kept for records and notices from other libraries.
func TestAHubNoHandlerIsTransientWithoutAHublessService(t *testing.T) {
	kind, reason, _ := classifySagaSend(&Error{Code: CodeNoHandler, Message: "no handler"})
	if kind != sagaSendTransient || reason != FailureExhausted {
		t.Fatalf("the hub NO_HANDLER: %v/%v, want transient/EXHAUSTED", kind, reason)
	}
}
