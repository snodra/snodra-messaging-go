package snodra

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingEvents is an event store's publisher that records what it accepted and fails what the
// test says.
type recordingEvents struct {
	mu        sync.Mutex
	published []OutgoingEvent
	failures  map[string]error // by event id; once returned, the entry stays failing
	onPublish func(OutgoingEvent)
}

func newRecordingEvents() *recordingEvents {
	return &recordingEvents{failures: map[string]error{}}
}

func (r *recordingEvents) Name() string { return "recording" }

func (r *recordingEvents) Publish(ctx context.Context, event OutgoingEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	r.published = append(r.published, event)
	failure := r.failures[event.ID]
	onPublish := r.onPublish
	r.mu.Unlock()
	if onPublish != nil {
		onPublish(event)
	}
	return failure
}

func (r *recordingEvents) events() []OutgoingEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]OutgoingEvent(nil), r.published...)
}

// failing marks id's publishes failed with err.
func (r *recordingEvents) failing(id string, err error) {
	r.mu.Lock()
	r.failures[id] = err
	r.mu.Unlock()
}

// testRelay is the relay of store to events with the test's settings, outside any app.
func testRelay(t *testing.T, store MessageStore, events EventPublisher, settings eventSettings, now func() time.Time) *outboxRelay {
	t.Helper()
	if now == nil {
		now = time.Now
	}
	return &outboxRelay{
		log:         testLogger(),
		outbox:      store.Outbox(),
		events:      events,
		source:      settings.source,
		service:     "orders-service",
		owner:       "relay-test",
		poll:        settings.poll,
		batch:       settings.batch,
		maxAttempts: settings.outboxAttempts,
		lease:       settings.lease,
		sendTimeout: settings.sendTimeout,
		now:         now,
	}
}

// relaySettings are short settings for a relay test: a lease longer than its send.
func relaySettings() eventSettings {
	s := defaultEventSettings()
	s.poll = 5 * time.Millisecond
	s.batch = 10
	s.lease = 100 * time.Millisecond
	s.sendTimeout = 20 * time.Millisecond
	return s
}

func appendRelayEntry(t *testing.T, store *MemoryMessageStore, id, key string, seq int64, createdAt time.Time) {
	t.Helper()
	var keyPtr *string
	if key != "" {
		keyPtr = &key
	}
	entry := OutboxEntry{
		ID:        id,
		Type:      "orders.OrderPlaced",
		Key:       keyPtr,
		Headers:   map[string]string{HeaderCorrelationID: "c-" + id, HeaderUserID: "u"},
		Payload:   []byte(`{"orderId":"` + id + `"}`),
		CreatedAt: createdAt,
		Seq:       seq,
	}
	if err := store.Outbox().Append(context.Background(), entry); err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
}

// L2-OUTBOX: a rejected event is given up at once and the batch moves on; a failed one counts an
// attempt and ends the batch, so it is retried before the later entries.
func TestRelayGivesUpARejectedEntryAndRetriesAFailedOne(t *testing.T) {
	store := NewMemoryMessageStore()
	now := time.Unix(1_800_000_000, 0)
	appendRelayEntry(t, store, "a", "k", 0, now)
	appendRelayEntry(t, store, "b", "k", 1, now)
	appendRelayEntry(t, store, "c", "k", 2, now)
	events := newRecordingEvents()
	events.failing("a", ErrRejected)
	events.failing("b", ErrFailed)
	relay := testRelay(t, store, events, relaySettings(), nil)

	sent, retriable := relay.relayOnce(context.Background())
	is(t, sent, 0)
	is(t, retriable, true)
	got := events.events()
	is(t, len(got), 2) // a was tried and rejected, b was tried and failed; c never was
	is(t, got[0].ID, "a")
	is(t, got[1].ID, "b")
	// a is failed for good: never claimed again. b waits, first in line; c is behind it.
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 2 {
		t.Fatalf("pending: %d, %v (b and c were never sent, a is out)", pending, err)
	}
	is(t, len(claimIDs(t, store, "relay-test")), 2) // b and c, b first (a is out)

	events.mu.Lock()
	delete(events.failures, "b")
	events.mu.Unlock()
	sent, retriable = relay.relayOnce(context.Background())
	is(t, sent, 2)
	is(t, retriable, false)
	got = events.events()
	if len(got) != 4 || got[2].ID != "b" || got[3].ID != "c" {
		t.Fatalf("order after the retry: %v", eventIDs(got))
	}
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("pending at the end: %d, %v", pending, err)
	}
}

// An entry that fails its last attempt is given up, and the next entry goes on.
func TestRelayGivesUpAnEntryThatExhaustedItsAttempts(t *testing.T) {
	store := NewMemoryMessageStore()
	settings := relaySettings()
	settings.outboxAttempts = 2
	now := time.Unix(1_800_000_000, 0)
	appendRelayEntry(t, store, "a", "k", 0, now)
	appendRelayEntry(t, store, "b", "k", 1, now)
	events := newRecordingEvents()
	events.failing("a", ErrFailed)
	relay := testRelay(t, store, events, settings, nil)

	// The first attempt counts and ends the batch.
	_, retriable := relay.relayOnce(context.Background())
	is(t, retriable, true)
	// The second attempt is the last: a is given up, b is sent.
	sent, retriable := relay.relayOnce(context.Background())
	is(t, retriable, false)
	is(t, sent, 1)
	got := events.events()
	if len(got) != 3 || got[2].ID != "b" {
		t.Fatalf("published %v", eventIDs(got))
	}
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("pending: %d, %v", pending, err)
	}
	if claimed := claimIDs(t, store, "relay-test"); len(claimed) != 0 {
		t.Fatalf("a failed entry was claimed again: %v", claimed)
	}
}

// An event the event store rejects for good is failed at once, whatever attempts are left.
func TestRelayGivesUpARejectedEntryAtOnce(t *testing.T) {
	store := NewMemoryMessageStore()
	settings := relaySettings()
	settings.outboxAttempts = 10
	appendRelayEntry(t, store, "a", "k", 0, time.Unix(1_800_000_000, 0))
	events := newRecordingEvents()
	events.failing("a", ErrInvalid)
	relay := testRelay(t, store, events, settings, nil)
	sent, retriable := relay.relayOnce(context.Background())
	is(t, sent, 0)
	is(t, retriable, false)
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("pending: %d, %v", pending, err)
	}
}

// A batch stops a send timeout before its lease ends: a slow event store never lets another
// instance claim an entry this relay still sends.
func TestRelayStopsABatchBeforeItsLeaseCouldRunOut(t *testing.T) {
	store := NewMemoryMessageStore()
	settings := relaySettings()
	settings.lease = 100 * time.Millisecond
	settings.sendTimeout = 60 * time.Millisecond // sends may start until claimedAt + 40ms
	now := time.Unix(1_800_000_000, 0)
	appendRelayEntry(t, store, "a", "k", 0, now)
	appendRelayEntry(t, store, "b", "k", 1, now)
	clock := now
	events := newRecordingEvents()
	events.onPublish = func(OutgoingEvent) { clock = clock.Add(50 * time.Millisecond) }
	relay := testRelay(t, store, events, settings, func() time.Time { return clock })
	sent, _ := relay.relayOnce(context.Background())
	is(t, sent, 1)
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 1 {
		t.Fatalf("pending: %d, %v (b waits for the next claim)", pending, err)
	}
	// The next claim renews the lease of what this relay holds and sends the rest.
	clock = clock.Add(time.Millisecond)
	sent, _ = relay.relayOnce(context.Background())
	is(t, sent, 1)
}

// A crash between the publish and the mark leaves the entry pending: after the lease it is
// published again, and no event is lost. Consumers skip the duplicate (L2-CONSUME).
func TestACrashBetweenPublishAndMarkDoesNotLoseTheEvent(t *testing.T) {
	store := NewMemoryMessageStore()
	settings := relaySettings()
	now := time.Unix(1_800_000_000, 0)
	appendRelayEntry(t, store, "a", "k", 0, now)
	events := newRecordingEvents()
	// The crashed relay: it claimed with a lease that passed at once and published, then died
	// before MarkSent.
	claimed, err := store.Outbox().Claim(context.Background(), "crashed-relay", 0, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %d, %v", len(claimed), err)
	}
	if err := events.Publish(context.Background(), (&outboxRelay{
		service: "orders-service", source: settings.source,
	}).outgoing(claimed[0])); err != nil {
		t.Fatal(err)
	}
	is(t, len(events.events()), 1)
	// A live relay claims the expired entry and publishes it again.
	relay := testRelay(t, store, events, settings, nil)
	sent, _ := relay.relayOnce(context.Background())
	is(t, sent, 1)
	got := events.events()
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "a" {
		t.Fatalf("published %v, want the event twice with the same id", eventIDs(got))
	}
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("pending: %d, %v", pending, err)
	}
}

// The event carries the entry and the publishing service as its source header.
func TestRelayEventCarriesTheEntryAndTheService(t *testing.T) {
	store := NewMemoryMessageStore()
	key := "order-1"
	entry := OutboxEntry{
		ID: "e-1", Type: "orders.OrderPlaced", Key: &key,
		Headers: map[string]string{
			HeaderCorrelationID: "c-1",
			HeaderUserID:        "user-1",
			HeaderTraceparent:   testTraceparent,
			HeaderType:          "stale",
			HeaderSource:        "stale",
		},
		Payload:   []byte(`{"orderId":"o-1"}`),
		CreatedAt: time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC),
	}
	relay := testRelay(t, store, newRecordingEvents(), relaySettings(), nil)
	outgoing := relay.outgoing(entry)
	is(t, outgoing.Source, DefaultEventsSource)
	is(t, outgoing.ID, "e-1")
	is(t, outgoing.Type, "orders.OrderPlaced")
	is(t, outgoing.Headers[HeaderType], "orders.OrderPlaced")
	is(t, outgoing.Headers[HeaderSource], "orders-service")
	is(t, outgoing.Headers[HeaderTimestamp], "2026-11-26T00:00:00.000Z")
	is(t, outgoing.Headers[HeaderCorrelationID], "c-1")
	is(t, outgoing.Headers[HeaderUserID], "user-1")
	is(t, outgoing.Headers[HeaderTraceparent], testTraceparent)
	is(t, *outgoing.Key, "order-1")
	is(t, string(outgoing.Payload), `{"orderId":"o-1"}`)
}

// claimIDs claims everything pending for owner and returns the ids in claim order.
func claimIDs(t *testing.T, store *MemoryMessageStore, owner string) []string {
	t.Helper()
	claimed, err := store.Outbox().Claim(context.Background(), owner, time.Minute, 100)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	ids := make([]string, len(claimed))
	for i, entry := range claimed {
		ids[i] = entry.ID
	}
	return ids
}

// eventIDs are the ids of events.
func eventIDs(events []OutgoingEvent) []string {
	ids := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	return ids
}

// A context that is done is returned as it is by the relay's publish.
func TestRelayPublishReturnsTheCallersContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := newRecordingEvents()
	relay := testRelay(t, NewMemoryMessageStore(), events, relaySettings(), nil)
	err := relay.publish(ctx, OutboxEntry{ID: "a"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if len(events.events()) != 0 {
		t.Fatal("a cancelled publish reached the event store")
	}
}

// A full batch is followed by the next claim at once, not at the next poll: three entries with a
// batch size of 2 and an hour's poll all go out.
func TestRelayContinuesImmediatelyAfterAFullBatch(t *testing.T) {
	store := NewMemoryMessageStore()
	settings := relaySettings()
	settings.poll = time.Hour
	settings.batch = 2
	now := time.Unix(1_800_000_000, 0)
	for i, id := range []string{"a", "b", "c"} {
		appendRelayEntry(t, store, id, "k", int64(i), now)
	}
	events := newRecordingEvents()
	relay := testRelay(t, store, events, settings, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.run(ctx)
	}()
	waitUntil(t, "the three relayed entries", func() bool { return len(events.events()) == 3 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay did not stop")
	}
	if got := eventIDs(events.events()); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("published %v", got)
	}
}

// blockingPublisher never answers before its context ends: the relay's send timeout turns that
// into a failed send.
type blockingPublisher struct{}

func (blockingPublisher) Name() string { return "blocking" }

func (blockingPublisher) Publish(ctx context.Context, _ OutgoingEvent) error {
	<-ctx.Done()
	return ctx.Err()
}

// A send that gets no answer within the send timeout is a failed send (ErrFailed), so the relay
// counts an attempt and retries it; the shipped timeout is 40 s.
func TestRelaySendTimeoutIsAFailedSend(t *testing.T) {
	is(t, defaultEventSettings().sendTimeout, 40*time.Second)
	settings := relaySettings()
	settings.sendTimeout = 5 * time.Millisecond
	relay := testRelay(t, NewMemoryMessageStore(), blockingPublisher{}, settings, nil)
	err := relay.publish(context.Background(), OutboxEntry{ID: "slow"})
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("got %v, want ErrFailed", err)
	}
	if !strings.Contains(err.Error(), "no answer from the event store") {
		t.Fatalf("got %q, want it to name the timeout", err)
	}
}
