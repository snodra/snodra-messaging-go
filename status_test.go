package snodra

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// mustInt reads a decimal position a test expects to be one.
func decimalOf(t testing.TB, text string) int64 {
	t.Helper()
	if text == "" {
		return 0
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		t.Fatalf("%q is not a decimal position: %v", text, err)
	}
	return value
}

// processorStatusOf is the status of one processor of app.
func processorStatusOf(t testing.TB, app *App, name string) *hubpb.ProcessorStatus {
	t.Helper()
	for _, status := range app.processorStatuses() {
		if status.GetName() == name {
			return status
		}
	}
	t.Fatalf("no status of processor %s", name)
	return nil
}

// L3-STATUS: every status tick carries one ProcessorStatus per started processor, with its
// subscription, source, event store, capabilities, event types and dead-letter mode.
func TestStatusCarriesOneProcessorPerStartedProcessor(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := newApp(t, Hubs(hub.Addr()), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false),
		StatusInterval(10*ms))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor("mail")); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Recv(t)
	s.Ack(t, fakehub.Ack("hub-1"))

	var status *hubpb.InstanceStatus
	waitUntil(t, "a status with both processors", func() bool {
		statuses := s.Statuses()
		if len(statuses) == 0 {
			return false
		}
		status = statuses[len(statuses)-1]
		return len(status.GetProcessors()) == 2
	})
	if len(status.GetProcessors()) != 2 {
		t.Fatalf("processors: %v", status.GetProcessors())
	}
	byName := map[string]*hubpb.ProcessorStatus{}
	for _, processor := range status.GetProcessors() {
		byName[processor.GetName()] = processor
	}
	for _, name := range []string{DefaultProcessor, "mail"} {
		processor := byName[name]
		if processor == nil {
			t.Fatalf("no status of %s in %v", name, status.GetProcessors())
		}
		is(t, processor.GetSubscription(), ProcessorSubscription("orders-service", name))
		is(t, processor.GetSource(), DefaultEventsSource)
		is(t, processor.GetEventStore(), "memory")
		types := processor.GetEventTypes()
		if len(types) != 1 || types[0] != "orders.OrderPlaced" {
			t.Fatalf("%s event types: %v", name, types)
		}
		is(t, processor.GetDeadLetterMode(), DeadLetterModeCollection)
		caps := processor.GetCapabilities()
		if len(caps) != 2 || caps[0] != "lag" || caps[1] != "replay" {
			t.Fatalf("%s capabilities: %v", name, caps)
		}
	}
	// Every tick carries the whole list, not a delta.
	waitUntil(t, "a later status with both processors", func() bool {
		statuses := s.Statuses()
		return len(statuses) > 1 && len(statuses[len(statuses)-1].GetProcessors()) == 2
	})
}

// startingSubscribeStore blocks Subscribe until gate closes, so a processor stays STARTING.
type startingSubscribeStore struct {
	EventStore
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (s *startingSubscribeStore) Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.EventStore.Subscribe(ctx, spec)
}

// L3-STATUS: a processor is STARTING before its first assignment, RUNNING once assigned (idle is
// running), FAILING for the failing window after an exhausted record, PAUSED while paused and
// STOPPED at shutdown; the counters and the sampled dead-letter count are there.
func TestStatusStatesCountersSegmentsAndDeadLetters(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(2)
	if err != nil {
		t.Fatal(err)
	}
	blocking := &startingSubscribeStore{EventStore: events, gate: make(chan struct{}), entered: make(chan struct{})}
	var mailFail atomicBool
	settings := consumerSettings()
	settings.consumerTries = 1
	app := eventsApp(t, WithStore(store), WithEventStore(blocking),
		withEventSettings(settings), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		if mailFail.Load() {
			return errText("mail cannot take it")
		}
		return nil
	}, InProcessor("mail")); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-blocking.entered
	// No assignment yet: STARTING (the paused state is not involved).
	is(t, processorStatusOf(t, app, DefaultProcessor).GetState(), hubpb.ProcessorState_PROCESSOR_STARTING)
	close(blocking.gate)
	waitUntil(t, "both processors RUNNING", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetState() == hubpb.ProcessorState_PROCESSOR_RUNNING &&
			processorStatusOf(t, app, "mail").GetState() == hubpb.ProcessorState_PROCESSOR_RUNNING
	})

	key := "k1"
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", &key, nil)
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", &key, nil)
	waitUntil(t, "two processed records in both processors", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetProcessedTotal() == 2 &&
			processorStatusOf(t, app, "mail").GetProcessedTotal() == 2
	})
	status := processorStatusOf(t, app, DefaultProcessor)
	if len(status.GetSegments()) != 2 {
		t.Fatalf("segments: %v", status.GetSegments())
	}
	for i, segment := range status.GetSegments() {
		is(t, segment.GetSegment(), []string{"0", "1"}[i])
		is(t, segment.GetLag(), int64(0))
	}
	var positions, committed, heads int64
	for _, segment := range status.GetSegments() {
		is(t, segment.GetLag(), int64(0))
		positions += decimalOf(t, segment.GetPosition())
		committed += decimalOf(t, segment.GetCommitted())
		heads += decimalOf(t, segment.GetHead())
	}
	is(t, positions, int64(2))
	is(t, committed, int64(2))
	is(t, heads, int64(2))

	mailFail.Store(true)
	publishTestEvent(t, events, "bad-1", "orders.OrderPlaced", &key, nil)
	waitUntil(t, "mail FAILING with a letter", func() bool {
		status := processorStatusOf(t, app, "mail")
		return status.GetState() == hubpb.ProcessorState_PROCESSOR_FAILING &&
			status.GetFailedTotal() == 1 && status.GetDeadLetters() == 1 &&
			status.GetLastError() != "" && status.GetLastErrorAtMs() > 0
	})
	is(t, processorStatusOf(t, app, DefaultProcessor).GetState(), hubpb.ProcessorState_PROCESSOR_RUNNING)
	is(t, processorStatusOf(t, app, DefaultProcessor).GetFailedTotal(), uint64(0))

	waitUntil(t, "the failing window to pass", func() bool {
		return processorStatusOf(t, app, "mail").GetState() == hubpb.ProcessorState_PROCESSOR_RUNNING
	})

	if err := store.Control().SetPaused(context.Background(), "mail", true, "test", app.now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "mail PAUSED", func() bool {
		return processorStatusOf(t, app, "mail").GetState() == hubpb.ProcessorState_PROCESSOR_PAUSED
	})
	if err := store.Control().SetPaused(context.Background(), "mail", false, "test", app.now()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "mail running again", func() bool {
		return processorStatusOf(t, app, "mail").GetState() == hubpb.ProcessorState_PROCESSOR_RUNNING
	})

	if err := app.Shutdown(ctxWithin(t, 5*s)); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitUntil(t, "STOPPED at shutdown", func() bool {
		return processorStatusOf(t, app, "mail").GetState() == hubpb.ProcessorState_PROCESSOR_STOPPED
	})
}

// atomicBool is a settable bool for a handler.
type atomicBool struct {
	mu  sync.Mutex
	set bool
}

func (b *atomicBool) Load() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.set
}

func (b *atomicBool) Store(value bool) {
	b.mu.Lock()
	b.set = value
	b.mu.Unlock()
}

// L3-STATUS: building the status reads memory only: a handler in flight never delays it.
func TestStatusBuildingNeverBlocks(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	handling := make(chan struct{})
	release := make(chan struct{})
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		close(handling)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "busy-1", "orders.OrderPlaced", nil, nil)
	<-handling
	for i := 0; i < 50; i++ {
		began := time.Now()
		status := processorStatusOf(t, app, DefaultProcessor)
		if elapsed := time.Since(began); elapsed > 50*ms {
			t.Fatalf("building the status took %v", elapsed)
		}
		is(t, status.GetState(), hubpb.ProcessorState_PROCESSOR_RUNNING)
		if len(status.GetSegments()) == 0 {
			t.Fatalf("a busy processor lost its segments")
		}
	}
	close(release)
	waitUntil(t, "the record's acknowledgement", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetProcessedTotal() == 1
	})
}

// L3-STATUS: outside mode collection the dead-letter count reads 0 (and nothing is sampled).
func TestStatusReportsNoDeadLetterCountOutsideCollection(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	settings := consumerSettings()
	settings.consumerTries = 1
	settings.deadLetterMode = DeadLetterModeLog
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(settings), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		return errText("never works")
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "bad-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the failure in log mode", func() bool {
		status := processorStatusOf(t, app, DefaultProcessor)
		return status.GetFailedTotal() == 1 && status.GetDeadLetterMode() == DeadLetterModeLog
	})
	app.sampleDeadLetterCounts(context.Background(), true)
	is(t, processorStatusOf(t, app, DefaultProcessor).GetDeadLetters(), uint64(0))
	if count, err := store.DeadLetters().Count(context.Background(), DefaultProcessor); err != nil || count != 0 {
		t.Fatalf("a letter in log mode: %d, %v", count, err)
	}
}
