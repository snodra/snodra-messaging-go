package snodra

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// neverWithin fails when cond holds within d (an assertion of absence).
func neverWithin(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("%s happened within %v", what, d)
		}
		time.Sleep(time.Millisecond)
	}
}

// members is how many subscriptions of spec read the memory event store now.
func members(t *testing.T, events *MemoryEventStore, spec SubscriptionSpec) int {
	t.Helper()
	admin, err := events.Admin(spec)
	if err != nil {
		t.Fatal(err)
	}
	list, err := admin.Members(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(list)
}

// L3-PAUSE: a pause stops delivery without leaving the subscription, and a resume delivers from
// where it stopped; the pause is persisted in the message store.
func TestPauseStopsDeliveryWithoutLeavingTheSubscriptionAndResumesInPlace(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	recorder := &subscriptionRecorder{EventStore: events}
	var handled atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(recorder),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the first record", func() bool { return handled.Load() == 1 })

	// Another instance (or a hub request this instance missed) persists the pause.
	if err := store.Control().SetPaused(context.Background(), DefaultProcessor, true, "operator", app.now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the processor PAUSED", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetState() == hubpb.ProcessorState_PROCESSOR_PAUSED
	})
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || !state.Paused {
		t.Fatalf("the pause was not persisted: %+v, %v", state, err)
	}

	spec := SubscriptionSpec{Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource}
	is(t, members(t, events, spec), 1)
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", nil, nil)
	neverWithin(t, 100*ms, "a delivery while paused", func() bool { return handled.Load() > 1 })
	is(t, recorder.closed(), 0)

	if err := store.Control().SetPaused(context.Background(), DefaultProcessor, false, "operator", app.now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the record held by the pause", func() bool { return handled.Load() == 2 })
	is(t, recorder.closed(), 0)
}

// L3-PAUSE: an instance that starts reads its pause from the store before subscribing; a segment
// assigned while paused is paused at once; the resume delivers its records from where it stopped.
func TestAStartedProcessorReadsItsPauseAtStart(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Control().SetPaused(context.Background(), DefaultProcessor, true, "operator", time.Now()); err != nil {
		t.Fatal(err)
	}
	var handled atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitUntil(t, "the persisted pause", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetState() == hubpb.ProcessorState_PROCESSOR_PAUSED
	})
	// One event per segment: both segments were assigned while paused.
	for segment := 0; segment < 2; segment++ {
		key := keyOfSegment(t, segment, 2)
		publishTestEvent(t, events, fmt.Sprintf("evt-%d", segment), "orders.OrderPlaced", &key, nil)
	}
	waitUntil(t, "the two segments assigned while paused", func() bool {
		return len(processorStatusOf(t, app, DefaultProcessor).GetSegments()) == 2
	})
	neverWithin(t, 100*ms, "a delivery of a segment assigned while paused", func() bool { return handled.Load() > 0 })
	is(t, members(t, events, SubscriptionSpec{Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource}), 1)

	if err := store.Control().SetPaused(context.Background(), DefaultProcessor, false, "operator", app.now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "both held records", func() bool { return handled.Load() == 2 })
}

// keyOfSegment is a key the memory event store puts on segment.
func keyOfSegment(t *testing.T, segment, segments int) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if memorySegment(key, segments) == segment {
			return key
		}
	}
	t.Fatalf("no key for segment %d", segment)
	return ""
}

// L3-PAUSE: a pause never touches the reset lock of the processor.
func TestAPauseDoesNotTouchTheResetLock(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	at := time.Now()
	if outcome, err := store.Control().Lock(context.Background(), DefaultProcessor, "op-1", at.Add(time.Minute), at); err != nil || !outcome.Taken {
		t.Fatalf("Lock: %+v, %v", outcome, err)
	}
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store))

	s.Send(t, pauseControl("pause-1", DefaultProcessor))
	answer := s.RecvControlReply(t)
	if answer.GetOk() == nil {
		t.Fatalf("pause: %v", answer)
	}
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Paused {
		t.Fatal("the pause was not persisted")
	}
	if state.Operation == nil || state.Operation.ID != "op-1" || !state.Operation.LeaseUntil.Equal(at.Add(time.Minute)) {
		t.Fatalf("the pause changed the reset lock: %+v", state.Operation)
	}

	s.Send(t, resumeControl("resume-1", DefaultProcessor))
	answer = s.RecvControlReply(t)
	if answer.GetOk() == nil {
		t.Fatalf("resume: %v", answer)
	}
	state, err = store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil {
		t.Fatal(err)
	}
	if state.Paused {
		t.Fatal("the resume was not persisted")
	}
	if state.Operation == nil || state.Operation.ID != "op-1" {
		t.Fatalf("the resume changed the reset lock: %+v", state.Operation)
	}
}
