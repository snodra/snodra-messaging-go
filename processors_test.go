package snodra

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// controlTimings are the scaled timings with the processor clocks shortened: control state every
// 5 ms, dead-letter counts every 5 ms, and a 500 ms failing window (long enough that a slow race
// run still observes FAILING, short enough to wait for it to pass).
func controlTimings() timings {
	t := scaled()
	t.controlPoll = 5 * ms
	t.deadLetterSample = 5 * ms
	t.failingWindow = 500 * ms
	return t
}

// subscriptionRecorder is an event store that records the subscriptions opened and closed.
type subscriptionRecorder struct {
	EventStore
	mu     sync.Mutex
	specs  []SubscriptionSpec
	closes int
}

func (r *subscriptionRecorder) Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	sub, err := r.EventStore.Subscribe(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &closeCountingSubscription{EventSubscription: sub, recorder: r}, nil
}

func (r *subscriptionRecorder) opened() []SubscriptionSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]SubscriptionSpec(nil), r.specs...)
}

func (r *subscriptionRecorder) closed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

type closeCountingSubscription struct {
	EventSubscription
	recorder *subscriptionRecorder
	once     sync.Once
}

func (s *closeCountingSubscription) Close(ctx context.Context) error {
	s.once.Do(func() {
		s.recorder.mu.Lock()
		s.recorder.closes++
		s.recorder.mu.Unlock()
	})
	return s.EventSubscription.Close(ctx)
}

func openedNames(specs []SubscriptionSpec) []string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		if len(names) == 0 || names[len(names)-1] != spec.Name {
			names = append(names, spec.Name)
		}
	}
	return names
}

// L3-PROCESSORS: each processor has its own subscription "<service>.<processor>" and its own
// marker family, and one event type handled in two processors reaches both.
func TestNamedProcessorsGetEveryEventUnderTheirOwnSubscriptionAndMarkers(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(2)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &subscriptionRecorder{EventStore: events}
	var plain, mail, billing atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(recorder),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		plain.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		mail.Add(1)
		return nil
	}, InProcessor("mail")); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(context.Context, OrderShipped) error {
		billing.Add(1)
		return nil
	}, InProcessor("billing")); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "both OrderPlaced handlers", func() bool { return plain.Load() == 1 && mail.Load() == 1 })
	waitUntil(t, "the markers of both processors", func() bool {
		a, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1"))
		if err != nil || !a {
			return false
		}
		b, err := store.Processed().Exists(context.Background(), ProcessorMarker("mail", "evt-1"))
		return err == nil && b
	})
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker("billing", "evt-1")); err != nil || exists {
		t.Fatalf("a billing marker for an event billing does not handle: %v, %v", exists, err)
	}

	publishTestEvent(t, events, "evt-2", "orders.OrderShipped", nil, nil)
	waitUntil(t, "the billing handler", func() bool { return billing.Load() == 1 })
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker("mail", "evt-2")); err != nil || exists {
		t.Fatalf("a mail marker for an event mail does not handle: %v, %v", exists, err)
	}

	waitUntil(t, "every subscription opened", func() bool { return len(openedNames(recorder.opened())) == 3 })
	names := openedNames(recorder.opened())
	sort.Strings(names)
	want := []string{
		ProcessorSubscription("orders-service", "billing"),
		ProcessorSubscription("orders-service", DefaultProcessor),
		ProcessorSubscription("orders-service", "mail"),
	}
	if len(names) != len(want) {
		t.Fatalf("subscriptions: %v", names)
	}
	for i := range want {
		is(t, names[i], want[i])
	}
	for _, spec := range recorder.opened() {
		is(t, spec.Source, DefaultEventsSource)
	}
}

// L3-PROCESSORS: a failure in one processor parks there only and does not hold the other.
func TestAFailureInOneProcessorDoesNotAffectTheOther(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	var plain, mail atomic.Int64
	settings := consumerSettings()
	settings.consumerTries = 1
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(settings), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		plain.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		mail.Add(1)
		if event.OrderID == "bad-1" {
			return errText("mail cannot take it")
		}
		return nil
	}, InProcessor("mail")); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	key := "k1"
	publishTestEvent(t, events, "bad-1", "orders.OrderPlaced", &key, nil)
	publishTestEvent(t, events, "ok-1", "orders.OrderPlaced", &key, nil)
	waitUntil(t, "default handling both events", func() bool { return plain.Load() == 2 })
	waitUntil(t, "mail's two letters", func() bool {
		count, err := store.DeadLetters().Count(context.Background(), "mail")
		return err == nil && count == 2
	})
	// Mail never ran for the sequenced follower, and default is unaffected by mail's parked key.
	is(t, mail.Load(), int64(1))
	for _, id := range []string{"bad-1", "ok-1"} {
		if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, id)); err != nil || !exists {
			t.Fatalf("default's marker for %s: %v, %v", id, exists, err)
		}
	}
	letters, err := store.DeadLetters().List(context.Background(), "mail", 0)
	if err != nil || len(letters) != 2 {
		t.Fatalf("mail's letters: %v, %v", letters, err)
	}
	reasons := map[DeadLetterReason]int{}
	for _, letter := range letters {
		reasons[letter.Reason]++
	}
	is(t, reasons[DeadLetterHandlerFailed], 1)
	is(t, reasons[DeadLetterSequenced], 1)
	statuses := app.processorStatuses()
	byName := map[string]int{}
	for i, status := range statuses {
		byName[status.GetName()] = i
	}
	mailStatus := statuses[byName["mail"]]
	is(t, mailStatus.GetFailedTotal(), uint64(1))
	plainStatus := statuses[byName[DefaultProcessor]]
	is(t, plainStatus.GetFailedTotal(), uint64(0))
	is(t, plainStatus.GetProcessedTotal(), uint64(2))
}

// L3-PROCESSORS: a processor's name matches [a-z0-9][a-z0-9-]{0,47} and is never "default".
func TestInProcessorNames(t *testing.T) {
	app, err := New(Service("orders-service"), Hubs("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "0", "mail", "a-b", "a1-b2", strings.Repeat("a", 48)} {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor(name)); err != nil {
			t.Errorf("InProcessor(%q): %v", name, err)
		}
	}
	for _, name := range []string{"", "default", "Mail", "-mail", "mail_1", "mail ", strings.Repeat("a", 49)} {
		err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor(name))
		if name == "mail " {
			if err != nil {
				t.Errorf("InProcessor(%q) is trimmed to a valid name: %v", name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("InProcessor(%q) was accepted", name)
		}
	}
	is(t, DefaultProcessor, "default")
	is(t, ProcessorSubscription("orders-service", "mail"), "orders-service.mail")
	is(t, ProcessorMarker("mail", "evt-1"), "processor:mail:evt-1")
}

// L3-PROCESSORS: only processors with handlers start, and a service without event handlers
// reports no processor.
func TestOnlyProcessorsWithHandlersStart(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	recorder := &subscriptionRecorder{EventStore: events}
	app := eventsApp(t, WithStore(store), WithEventStore(recorder),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor("mail")); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	poll(t, func() bool { return len(openedNames(recorder.opened())) == 1 })
	is(t, openedNames(recorder.opened())[0], "orders-service.mail")
	statuses := app.processorStatuses()
	is(t, len(statuses), 1)
	is(t, statuses[0].GetName(), "mail")

	noHandlers := eventsApp(t, WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), withTimings(controlTimings()))
	if err := noHandlers.Start(context.Background()); err != nil {
		t.Fatalf("Start without handlers: %v", err)
	}
	is(t, len(noHandlers.processorStatuses()), 0)
}

// L3-PROCESSORS: the default processor is first, then the named ones sorted.
func TestProcessorsOrderDefaultThenNamed(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false),
		Logger(slog.New(&logRecorder{})))
	for _, name := range []string{"zeta", "alpha"} {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor(name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	statuses := app.processorStatuses()
	names := make([]string, len(statuses))
	for i, status := range statuses {
		names[i] = status.GetName()
	}
	want := []string{DefaultProcessor, "alpha", "zeta"}
	if len(names) != len(want) {
		t.Fatalf("processors: %v", names)
	}
	for i := range want {
		is(t, names[i], want[i])
	}
}
