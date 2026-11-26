package snodra

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// consumerSettings are short settings for a consumer test.
func consumerSettings() eventSettings {
	s := defaultEventSettings()
	s.poll = 5 * time.Millisecond
	s.batch = 10
	s.lease = 100 * time.Millisecond
	s.sendTimeout = 20 * time.Millisecond
	s.consumerTries = 3
	s.consumerBackoff = time.Millisecond
	s.resubscribeMin = 5 * time.Millisecond
	s.resubscribeMax = 20 * time.Millisecond
	return s
}

// waitUntil waits until cond holds, at most 10 s, and fails naming what it waited for.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// recordingProcessed is a processed-event store that records what the consumer records.
type recordingProcessed struct {
	ProcessedStore
	mu      sync.Mutex
	markers []ProcessedMarker
}

func (p *recordingProcessed) Record(ctx context.Context, marker ProcessedMarker) (bool, error) {
	p.mu.Lock()
	p.markers = append(p.markers, marker)
	p.mu.Unlock()
	return p.ProcessedStore.Record(ctx, marker)
}

func (p *recordingProcessed) recorded() []ProcessedMarker {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ProcessedMarker(nil), p.markers...)
}

// markerStore is a message store whose processed port is recordingProcessed.
type markerStore struct {
	MessageStore
	processed *recordingProcessed
}

func newMarkerStore(inner MessageStore) *markerStore {
	return &markerStore{MessageStore: inner, processed: &recordingProcessed{ProcessedStore: inner.Processed()}}
}

func (s *markerStore) Processed() ProcessedStore { return s.processed }

// ackObserver is an event store that counts acknowledgements and may check something about each.
type ackObserver struct {
	EventStore
	mu    sync.Mutex
	acks  int
	onAck func(segment, position string)
}

func (o *ackObserver) Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error) {
	sub, err := o.EventStore.Subscribe(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &observedSubscription{EventSubscription: sub, observer: o}, nil
}

func (o *ackObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.acks
}

func (o *ackObserver) acknowledged() {
	o.mu.Lock()
	o.acks++
	onAck := o.onAck
	o.mu.Unlock()
	if onAck != nil {
		onAck("", "")
	}
}

type observedSubscription struct {
	EventSubscription
	observer *ackObserver
}

func (s *observedSubscription) Ack(segment, position string) error {
	err := s.EventSubscription.Ack(segment, position)
	s.observer.acknowledged()
	return err
}

// silentStore is a message store whose outbox cannot wake the relay: the relay polls.
type silentStore struct{ MessageStore }

func (s silentStore) Outbox() OutboxStore { return silentOutbox{s.MessageStore.Outbox()} }

type silentOutbox struct{ OutboxStore }

func (silentOutbox) OnAppend(func()) func() { return func() {} }

// flakyCommitStore is a message store whose next commits fail, as a database's conflict would.
type flakyCommitStore struct {
	MessageStore
	failures atomic.Int64
}

func (s *flakyCommitStore) Begin(ctx context.Context) (context.Context, Tx, error) {
	txctx, tx, err := s.MessageStore.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	return txctx, &flakyTx{Tx: tx, store: s}, nil
}

type flakyTx struct {
	Tx
	store *flakyCommitStore
}

func (t *flakyTx) Commit(ctx context.Context) error {
	if t.store.failures.Add(-1) >= 0 {
		_ = t.Rollback(ctx)
		return errors.New("the commit failed for the test")
	}
	return t.Tx.Commit(ctx)
}

// publishTestEvent appends an event to the in-memory event store as a publisher would.
func publishTestEvent(t testing.TB, events EventStore, id, typeName string, key *string, extra map[string]string) {
	t.Helper()
	headers := map[string]string{
		HeaderType:          typeName,
		HeaderID:            id,
		HeaderCorrelationID: "c-" + id,
		HeaderTimestamp:     eventTimestamp(time.Now()),
		HeaderSource:        "test-service",
		HeaderUserID:        "user-1",
	}
	for name, value := range extra {
		headers[name] = value
	}
	err := events.Publish(context.Background(), OutgoingEvent{
		Source:  DefaultEventsSource,
		ID:      id,
		Type:    typeName,
		Key:     key,
		Headers: headers,
		Payload: []byte(`{"orderId":"` + id + `"}`),
	})
	if err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
}

// L2-CONSUME: the stage-1 marker and the handlers' writes commit in one transaction; a failed
// attempt leaves neither, and the retry records both.
func TestConsumerRecordsTheMarkerAndHandlersInOneTransaction(t *testing.T) {
	store := newMarkerStore(NewMemoryMessageStore())
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(consumerSettings()), OutboxEnabled(false))
	var calls atomic.Int64
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		if calls.Add(1) == 1 {
			// The failed attempt's writes, the marker included, roll back.
			if err := Publish(ctx, app, OrderShipped{OrderID: event.OrderID}); err != nil {
				return err
			}
			return errors.New("the first attempt fails")
		}
		return Publish(ctx, app, OrderShipped{OrderID: event.OrderID})
	}); err != nil {
		t.Fatal(err)
	}
	observer.onAck = func(string, string) {
		exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1"))
		if err != nil || !exists {
			t.Errorf("an event was acknowledged before its marker committed: exists=%v err=%v", exists, err)
		}
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the handler's second attempt", func() bool { return calls.Load() >= 2 })
	waitUntil(t, "the event's acknowledgement", func() bool { return observer.count() >= 1 })
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1")); err != nil || !exists {
		t.Fatalf("marker: %v, %v", exists, err)
	}
	// Exactly one event was published: the failed attempt's append rolled back with its marker.
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 1 {
		t.Fatalf("pending: %d, %v", pending, err)
	}
	entry := claimEntry(t, store.Outbox())
	is(t, entry.Type, "orders.OrderShipped")
	markers := store.processed.recorded()
	if len(markers) != 2 {
		t.Fatalf("markers recorded: %d", len(markers))
	}
	for _, marker := range markers {
		is(t, marker.ID, "processor:default:evt-1")
		is(t, marker.Type, "orders.OrderPlaced")
		is(t, marker.Processor, DefaultProcessor)
		is(t, marker.Source, DefaultEventsSource)
		is(t, marker.Segment, "0")
		is(t, marker.Position, "0")
	}
}

// L2-CONSUME: a duplicate marker skips the handlers and is acknowledged.
func TestConsumerSkipsADuplicate(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	app := eventsApp(t, WithStore(store), WithEventStore(observer), OutboxEnabled(true), withEventSettings(consumerSettings()))
	var calls atomic.Int64
	handled := make(chan struct{}, 2)
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		calls.Add(1)
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "dup", "orders.OrderPlaced", nil, nil)
	<-handled
	waitUntil(t, "the first acknowledgement", func() bool { return observer.count() >= 1 })
	// A redelivery of the same event id: the marker skips it, and it is acknowledged.
	publishTestEvent(t, events, "dup", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the duplicate's acknowledgement", func() bool { return observer.count() >= 2 })
	is(t, calls.Load(), int64(1))
}

// L2-CONSUME: an event type no handler handles is acknowledged and runs nothing.
func TestConsumerAcknowledgesAnUnknownTypeWithoutAMarker(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	app := eventsApp(t, WithStore(store), WithEventStore(observer), OutboxEnabled(true), withEventSettings(consumerSettings()))
	var calls atomic.Int64
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "unknown-1", "orders.Unknown", nil, nil)
	publishTestEvent(t, events, "known-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "both acknowledgements", func() bool { return observer.count() >= 2 })
	is(t, calls.Load(), int64(1))
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "unknown-1")); err != nil || exists {
		t.Fatalf("the unknown type has a marker: %v, %v", exists, err)
	}
}

// A record that fails every attempt is logged and skipped, and the next event goes on.
func TestConsumerLogsAndSkipsARecordThatFailsEveryAttempt(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	recorder := &logRecorder{}
	settings := consumerSettings()
	settings.consumerTries = 2
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(settings), Logger(slog.New(recorder)))
	var badCalls, goodCalls atomic.Int64
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		info, _ := MessageFrom(ctx)
		if info.ID == "bad-1" {
			badCalls.Add(1)
			return errors.New("this event never works")
		}
		goodCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "bad-1", "orders.OrderPlaced", nil, nil)
	publishTestEvent(t, events, "good-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the good event", func() bool { return goodCalls.Load() >= 1 })
	waitUntil(t, "both acknowledgements", func() bool { return observer.count() >= 2 })
	is(t, badCalls.Load(), int64(2))
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "bad-1")); err != nil || exists {
		t.Fatalf("the skipped event has a marker: %v, %v", exists, err)
	}
	if recorder.count(slog.LevelError, "an event handler failed on every attempt; skipping the event") == 0 {
		t.Fatal("the skip was not logged at error")
	}
}

// A panicking handler fails its attempt like an error and does not stop the processor.
func TestConsumerSurvivesAPanickingHandler(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	settings := consumerSettings()
	settings.consumerTries = 1
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(settings))
	var goodCalls atomic.Int64
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		info, _ := MessageFrom(ctx)
		if info.ID == "panic-1" {
			panic("boom")
		}
		goodCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "panic-1", "orders.OrderPlaced", nil, nil)
	publishTestEvent(t, events, "after-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the event after the panic", func() bool { return goodCalls.Load() >= 1 })
}

// L2-CONSUME: a record is acknowledged only after its transaction committed. A commit that fails
// leaves the marker uncommitted, the record is retried in place, and only the committed handling
// is acknowledged.
func TestConsumerAcknowledgesOnlyAfterTheCommit(t *testing.T) {
	inner := NewMemoryMessageStore()
	store := &flakyCommitStore{MessageStore: inner}
	store.failures.Store(1)
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	settings := consumerSettings()
	settings.consumerTries = 3
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(settings))
	var calls atomic.Int64
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	observer.onAck = func(string, string) {
		exists, err := inner.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "commit-1"))
		if err != nil || !exists {
			t.Errorf("acknowledged before the marker committed: exists=%v err=%v", exists, err)
		}
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "commit-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the acknowledgement after the second commit", func() bool { return observer.count() >= 1 })
	is(t, calls.Load(), int64(2))
}

// A crash between the publish and the mark loses nothing: the entry is published again after its
// lease, and the consumer handles the event once.
func TestACrashBetweenPublishAndMarkIsSkippedByTheConsumer(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	settings := consumerSettings()
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(settings))
	var calls atomic.Int64
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The event is appended, then a relay publishes it and crashes before marking it sent.
	if err := Publish(context.Background(), app, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	claimed, err := store.Outbox().Claim(context.Background(), "crashed-relay", 0, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %d, %v", len(claimed), err)
	}
	if err := events.Publish(context.Background(), newOutboxRelay(app).outgoing(claimed[0])); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The live relay publishes the entry again after the expired lease; the consumer handles the
	// first copy and skips the second.
	waitUntil(t, "the duplicate's acknowledgement", func() bool { return observer.count() >= 2 })
	is(t, calls.Load(), int64(1))
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, claimed[0].ID)); err != nil || !exists {
		t.Fatalf("marker: %v, %v", exists, err)
	}
	published := events.Events(DefaultEventsSource)
	if len(published) != 2 {
		t.Fatalf("the event store holds %d copies, want 2", len(published))
	}
	if published[0].Headers[HeaderID] != published[1].Headers[HeaderID] {
		t.Fatalf("the copies have different ids: %s and %s",
			published[0].Headers[HeaderID], published[1].Headers[HeaderID])
	}
	waitUntil(t, "the outbox's empty backlog", func() bool {
		pending, err := store.Outbox().Pending(context.Background())
		return err == nil && pending == 0
	})
}

// The relay publishes a committed append at once, not at its next poll.
func TestTheRelayWakesOnACommittedAppend(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	settings := consumerSettings()
	settings.poll = time.Hour
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(settings))
	handled := make(chan struct{}, 1)
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "wake-1", "orders.OrderPlaced", nil, nil)
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay did not wake on the committed append")
	}
}

// Without a wake the relay polls.
func TestTheRelayPollsWhenTheStoreCannotWake(t *testing.T) {
	inner := NewMemoryMessageStore()
	store := silentStore{MessageStore: inner}
	events := NewMemoryEventStore()
	observer := &ackObserver{EventStore: events}
	settings := consumerSettings()
	settings.poll = 5 * time.Millisecond
	app := eventsApp(t, WithStore(store), WithEventStore(observer), withEventSettings(settings))
	handled := make(chan struct{}, 1)
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "poll-1", "orders.OrderPlaced", nil, nil)
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("the polling relay did not publish the event")
	}
}

// One event type handled in two processors reaches both; a failure in one does not affect the
// other, and each has its own marker.
func TestTwoProcessorsHandleOneEventIndependently(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	settings := consumerSettings()
	settings.consumerTries = 2
	app := eventsApp(t, WithStore(store), WithEventStore(events), withEventSettings(settings))
	var defaultCalls, mailCalls atomic.Int64
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		defaultCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		mailCalls.Add(1)
		return errors.New("the mail processor fails")
	}, InProcessor("mail")); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	is(t, len(app.processors), 2)
	is(t, app.processors[0].subscription.Name, "orders-service.default")
	is(t, app.processors[1].subscription.Name, "orders-service.mail")
	publishTestEvent(t, events, "two-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the mail processor's attempts", func() bool { return mailCalls.Load() >= 2 })
	waitUntil(t, "the default processor's handling", func() bool { return defaultCalls.Load() >= 1 })
	// The default processor committed its marker; the failing one did not.
	waitUntil(t, "the default marker", func() bool {
		exists, err := store.Processed().Exists(context.Background(), "processor:default:two-1")
		return err == nil && exists
	})
	if exists, err := store.Processed().Exists(context.Background(), "processor:mail:two-1"); err != nil || exists {
		t.Fatalf("the mail marker: %v, %v", exists, err)
	}
}

// Shutdown stops the consumers and the relay, and waits for them.
func TestShutdownStopsTheEventBackground(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events), withEventSettings(consumerSettings()))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-app.bgDone:
	case <-time.After(time.Second):
		t.Fatal("the background work outlived Shutdown")
	}
}

// An event published while handling an event carries that event's correlation id, userId and
// trace context; an event carries a user id, never a token.
func TestAnEventHandlersPublishedEventCarriesTheEventIdentity(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events), withEventSettings(consumerSettings()), OutboxEnabled(false))
	var calls atomic.Int64
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		calls.Add(1)
		return Publish(ctx, app, OrderShipped{OrderID: event.OrderID})
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", nil, map[string]string{
		HeaderCorrelationID: "c-in",
		HeaderUserID:        "user-9",
		HeaderTraceparent:   testTraceparent,
	})
	waitUntil(t, "the handler's published event", func() bool { return calls.Load() >= 1 })
	waitUntil(t, "the outbox entry", func() bool {
		pending, err := store.Outbox().Pending(context.Background())
		return err == nil && pending == 1
	})
	entry := claimEntry(t, store.Outbox())
	is(t, entry.Type, "orders.OrderShipped")
	is(t, entry.Headers[HeaderCorrelationID], "c-in")
	is(t, entry.Headers[HeaderUserID], "user-9")
	is(t, entry.Headers[HeaderTraceparent], testTraceparent)
}

// A record without an id header is deduplicated by <source>-<segment>-<position>, as the
// specification says.
func TestAConsumerFallsBackToTheRecordPlaceForAnEventWithoutID(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := eventsApp(t, WithStore(store), WithEventStore(events), withEventSettings(consumerSettings()), OutboxEnabled(false))
	var calls atomic.Int64
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	err := events.Publish(context.Background(), OutgoingEvent{
		Source:  DefaultEventsSource,
		ID:      "unused",
		Type:    "orders.OrderPlaced",
		Headers: map[string]string{HeaderType: "orders.OrderPlaced", HeaderCorrelationID: "c-1"},
		Payload: []byte(`{"orderId":"o-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the handling", func() bool { return calls.Load() >= 1 })
	fallback := DefaultEventsSource + "-0-0"
	waitUntil(t, "the fallback marker", func() bool {
		exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, fallback))
		return err == nil && exists
	})
}
