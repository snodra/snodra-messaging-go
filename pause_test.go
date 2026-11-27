package snodra

import (
	"context"
	"fmt"
	"sync"
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

// lifecycleSubscription is an EventSubscription whose Close marks it closed; a Pause, Resume or
// Segments call after that is a lifecycle violation the test counts.
type lifecycleSubscription struct {
	mu                             sync.Mutex
	closing                        bool
	pauses, resumes, segmentsCalls int
	afterClose                     int
}

func (s *lifecycleSubscription) Next(ctx context.Context) (SubscriptionEvent, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *lifecycleSubscription) Ack(string, string) error { return nil }
func (s *lifecycleSubscription) Redeliver(string, string) error {
	return nil
}
func (s *lifecycleSubscription) TakeError() string { return "" }

func (s *lifecycleSubscription) Pause() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		s.afterClose++
	}
	s.pauses++
}

func (s *lifecycleSubscription) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		s.afterClose++
	}
	s.resumes++
}

func (s *lifecycleSubscription) Segments() []SegmentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		s.afterClose++
	}
	s.segmentsCalls++
	return nil
}

func (s *lifecycleSubscription) Close(context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	return nil
}

func (s *lifecycleSubscription) violation() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.afterClose
}

// L3-PAUSE: a pause, a resume or a status read never runs beside the subscription's Close: it
// waits, then sees no subscription.
func TestPauseAndCloseOfASubscriptionNeverOverlap(t *testing.T) {
	run := newProcessorRuntime()
	sub := &lifecycleSubscription{}
	run.attach(sub)

	closeEntered := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		run.detachAndClose(sub, func() {
			_ = sub.Close(context.Background())
			close(closeEntered)
			<-release
		})
		close(closed)
	}()
	<-closeEntered

	paused := make(chan struct{})
	go func() {
		run.applyPaused(true)
		close(paused)
	}()
	select {
	case <-paused:
		t.Fatal("a pause finished while the subscription was closing")
	case <-time.After(20 * ms):
	}
	if n := sub.violation(); n != 0 {
		t.Fatalf("the subscription was paused while it was closing")
	}

	segmentsDone := make(chan struct{})
	go func() {
		run.segments()
		close(segmentsDone)
	}()
	select {
	case <-segmentsDone:
		t.Fatal("the status read the segments of a closing subscription")
	case <-time.After(20 * ms):
	}
	close(release)
	<-closed
	<-paused
	<-segmentsDone
	if n := sub.violation(); n != 0 {
		t.Fatalf("a closed subscription was used %d times", n)
	}
	sub.mu.Lock()
	pauses, resumes := sub.pauses, sub.resumes
	sub.mu.Unlock()
	if pauses != 0 || resumes != 0 {
		t.Fatalf("a detached subscription was paused %d and resumed %d times", pauses, resumes)
	}
}

// pauseOnDeliveryStore pauses its processor just before handing out the first delivery, so the
// record is in hand when the pause takes effect.
type pauseOnDeliveryStore struct {
	EventStore
	app           *App
	control       ProcessorControlStore
	once          sync.Once
	onceRedeliver sync.Once
	redelivered   chan struct{}
	acks          atomic.Int64
}

func (s *pauseOnDeliveryStore) Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error) {
	sub, err := s.EventStore.Subscribe(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &pauseOnDeliverySubscription{EventSubscription: sub, store: s}, nil
}

type pauseOnDeliverySubscription struct {
	EventSubscription
	store *pauseOnDeliveryStore
}

func (s *pauseOnDeliverySubscription) Next(ctx context.Context) (SubscriptionEvent, error) {
	event, err := s.EventSubscription.Next(ctx)
	if err != nil {
		return event, err
	}
	if _, delivered := event.(Delivery); delivered {
		s.store.once.Do(func() {
			_ = s.store.control.SetPaused(context.Background(), DefaultProcessor, true, "test", time.Now())
			s.store.app.processorByName(DefaultProcessor).run.applyPaused(true)
		})
	}
	return event, err
}

func (s *pauseOnDeliverySubscription) Ack(segment, position string) error {
	if err := s.EventSubscription.Ack(segment, position); err != nil {
		return err
	}
	s.store.acks.Add(1)
	return nil
}

func (s *pauseOnDeliverySubscription) Redeliver(segment, position string) error {
	if err := s.EventSubscription.Redeliver(segment, position); err != nil {
		return err
	}
	s.store.onceRedeliver.Do(func() { close(s.store.redelivered) })
	return nil
}

// L3-PAUSE: a record handed out after the pause took effect is redelivered, not handled and not
// acknowledged; the resume delivers it.
func TestARecordHandedOutWhilePausingIsRedeliveredNotHandled(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	wrapper := &pauseOnDeliveryStore{
		EventStore:  events,
		control:     store.Control(),
		redelivered: make(chan struct{}),
	}
	var handled atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(wrapper),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wrapper.app = app
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "held-1", "orders.OrderPlaced", nil, nil)
	<-wrapper.redelivered
	is(t, handled.Load(), int64(0))
	is(t, wrapper.acks.Load(), int64(0))
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || !state.Paused {
		t.Fatalf("the pause was not persisted: %+v, %v", state, err)
	}

	if err := store.Control().SetPaused(context.Background(), DefaultProcessor, false, "test", app.now()); err != nil {
		t.Fatal(err)
	}
	app.processorByName(DefaultProcessor).run.applyPaused(false)
	waitUntil(t, "the held record after the resume", func() bool {
		return handled.Load() == 1 && wrapper.acks.Load() == 1
	})
}
