package snodra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// storeClock is a settable application clock for the memory store's tests.
type storeClock struct {
	mu  sync.Mutex
	now time.Time
}

// isStore is is with an optional message.
func isStore[T comparable](t testing.TB, got, want T, what ...string) {
	t.Helper()
	if got == want {
		return
	}
	if len(what) > 0 {
		t.Fatalf("%s: got %v, want %v", what[0], got, want)
	}
	t.Fatalf("got %v, want %v", got, want)
}

func newStoreClock() *storeClock {
	return &storeClock{now: time.Unix(1_800_000_000, 0)}
}

func (c *storeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *storeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// newMemoryStoreWithClock is an in-memory message store on a settable clock.
func newMemoryStoreWithClock() (*MemoryMessageStore, *storeClock) {
	clock := newStoreClock()
	store := NewMemoryMessageStore()
	store.UseClock(clock.Now)
	return store, clock
}

// memoryEntry is one outbox entry with a fresh id.
func memoryEntry(t testing.TB, id string, createdAt time.Time, seq int64) OutboxEntry {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"seq": seq, "text": "é", "nested": map[string]any{"ok": true}})
	if err != nil {
		t.Fatal(err)
	}
	key := "key-" + strconv.FormatInt(seq, 10)
	return OutboxEntry{
		ID:        id,
		Type:      "conformance.Ordered",
		Key:       &key,
		Headers:   map[string]string{HeaderCorrelationID: id, HeaderUserID: ""},
		Payload:   payload,
		CreatedAt: createdAt,
		Seq:       seq,
	}
}

// beginMemoryTx starts a transaction the test always ends.
func beginMemoryTx(t testing.TB, store *MemoryMessageStore) (context.Context, Tx) {
	t.Helper()
	ctx, tx, err := store.Begin(ctxWithin(t, 5*s))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	return ctx, tx
}

func TestMemoryMessageStoreName(t *testing.T) {
	store := NewMemoryMessageStore()
	isStore(t, store.Name(), "memory")
}

func TestMemoryMessageStoreZeroValueIsUsable(t *testing.T) {
	var store MemoryMessageStore
	ctx := context.Background()
	entry := memoryEntry(t, "zero", time.Unix(1_800_000_000, 0), 0)
	if err := store.Outbox().Append(ctx, entry); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Outbox().Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(claimed), 1)
	isStore(t, claimed[0].ID, "zero")
}

func TestMemoryMessageStoreClaimsInCreatedSeqOrder(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	outbox := store.Outbox()
	base := clock.Now()
	e1 := memoryEntry(t, "e1", base, 2)
	e2 := memoryEntry(t, "e2", base, 1)
	e3 := memoryEntry(t, "e3", base.Add(-time.Second), 9)
	e4 := memoryEntry(t, "e4", base.Add(time.Second), 0)
	txctx, tx := beginMemoryTx(t, store)
	if err := outbox.Append(txctx, e1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Append(ctx, e2); err != nil {
		t.Fatal(err)
	}
	txctx, tx = beginMemoryTx(t, store)
	if err := outbox.Append(txctx, e3); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Append(ctx, e4); err != nil {
		t.Fatal(err)
	}

	first, err := outbox.Claim(ctx, "relay-a", time.Minute, 2)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(first), 2)
	isStore(t, first[0].ID, "e3")
	isStore(t, first[1].ID, "e2")
	second, err := outbox.Claim(ctx, "relay-b", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(second), 2)
	isStore(t, second[0].ID, "e1")
	isStore(t, second[1].ID, "e4")

	if err := outbox.MarkAttemptFailed(ctx, "e3", "broker unavailable"); err != nil {
		t.Fatal(err)
	}
	again, err := outbox.Claim(ctx, "relay-a", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(again), 2)
	isStore(t, again[0].ID, "e3")
	is(t, again[0].Attempts, 1)
	isStore(t, again[1].ID, "e2")
	is(t, again[1].Attempts, 0)

	if err := outbox.MarkSent(ctx, []string{"e1", "e2", "e3", "e4"}); err != nil {
		t.Fatal(err)
	}
	left, err := outbox.Claim(ctx, "relay-c", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(left), 0)
}

func TestMemoryMessageStoreBreaksTiesByID(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	at := clock.Now()
	for _, id := range []string{"b", "a", "c"} {
		if err := store.Outbox().Append(ctx, memoryEntry(t, id, at, 5)); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := store.Outbox().Claim(ctx, "relay", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(claimed), 3)
	isStore(t, claimed[0].ID, "a")
	isStore(t, claimed[1].ID, "b")
	isStore(t, claimed[2].ID, "c")
}

func TestMemoryMessageStoreLeasesExpire(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	outbox := store.Outbox()
	now := clock.Now()
	entries := []OutboxEntry{
		memoryEntry(t, "l0", now, 0),
		memoryEntry(t, "l1", now, 1),
		memoryEntry(t, "l2", now, 2),
	}
	for _, entry := range entries {
		if err := outbox.Append(ctx, entry); err != nil {
			t.Fatal(err)
		}
	}
	crashed, err := outbox.Claim(ctx, "crashed", 3*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(crashed), 3)
	if err := outbox.MarkAttemptFailed(ctx, "l0", "broker unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := outbox.MarkFailed(ctx, "l2", "rejected"); err != nil {
		t.Fatal(err)
	}
	early, err := outbox.Claim(ctx, "survivor", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(early), 0)
	clock.Advance(2 * time.Second)
	early, err = outbox.Claim(ctx, "survivor", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(early), 0)
	clock.Advance(2 * time.Second)
	reclaimed, err := outbox.Claim(ctx, "survivor", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(reclaimed), 2)
	isStore(t, reclaimed[0].ID, "l0")
	is(t, reclaimed[0].Attempts, 1)
	isStore(t, reclaimed[1].ID, "l1")
	is(t, reclaimed[1].Attempts, 0)
	still, err := outbox.Claim(ctx, "crashed", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(still), 0)
}

func TestMemoryMessageStorePendingAndOldest(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	outbox := store.Outbox()
	now := clock.Now()
	for i, at := range []time.Time{now.Add(-time.Second), now.Add(-3 * time.Second), now.Add(-2 * time.Second)} {
		if err := outbox.Append(ctx, memoryEntry(t, fmt.Sprintf("p%d", i), at, int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is(t, pending, int64(3))
	oldest, ok, err := outbox.OldestPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is(t, ok, true)
	is(t, oldest, now.Add(-3*time.Second))
	if _, err := outbox.Claim(ctx, "relay", time.Minute, 3); err != nil {
		t.Fatal(err)
	}
	pending, err = outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, pending, int64(3), "held is pending")
	if err := outbox.MarkSent(ctx, []string{"p1"}); err != nil {
		t.Fatal(err)
	}
	oldest, ok, err = outbox.OldestPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is(t, ok, true)
	is(t, oldest, now.Add(-2*time.Second))
	if err := outbox.MarkFailed(ctx, "p2", "rejected"); err != nil {
		t.Fatal(err)
	}
	pending, err = outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is(t, pending, int64(1))
	if err := outbox.MarkSent(ctx, []string{"p0"}); err != nil {
		t.Fatal(err)
	}
	_, ok, err = outbox.OldestPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is(t, ok, false)
}

func TestMemoryMessageStoreRollbackLeavesNothing(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	outbox := store.Outbox()
	processed := store.Processed()
	now := clock.Now()
	entry := memoryEntry(t, "rolled-back", now, 0)
	marker := ProcessedMarker{ID: "processor:conformance:rolled-back", Type: "conformance.Happened", ProcessedAt: now}

	txctx, tx := beginMemoryTx(t, store)
	recorded, err := processed.Record(txctx, marker)
	if err != nil {
		t.Fatal(err)
	}
	is(t, recorded, true)
	if err := outbox.Append(txctx, entry); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := outbox.Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, len(claimed), 0, "the rolled-back append is gone")
	exists, err := processed.Exists(ctx, marker.ID)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, exists, false, "the marker rolled back with the handler")

	txctx, tx = beginMemoryTx(t, store)
	recorded, err = processed.Record(txctx, marker)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, recorded, true, "the rolled-back marker records again")
	exists, err = processed.Exists(ctx, marker.ID)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, exists, false, "not committed yet")
	if err := outbox.Append(txctx, entry); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	exists, err = processed.Exists(ctx, marker.ID)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, exists, true, "the marker committed with the handler")
	claimed, err = outbox.Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(claimed), 1)
	is(t, claimed[0].ID, entry.ID)
}

func TestMemoryMessageStoreTwoLiveWritersConflict(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	outbox := store.Outbox()
	processed := store.Processed()
	now := clock.Now()

	// Two live transactions appending one id: one writes, the other conflicts at its write.
	entry := memoryEntry(t, "twice", now, 0)
	firstCtx, first := beginMemoryTx(t, store)
	secondCtx, second := beginMemoryTx(t, store)
	if err := outbox.Append(firstCtx, entry); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Append(secondCtx, entry); !errors.Is(err, ErrConflict) {
		t.Fatalf("the second append: got %v, want ErrConflict", err)
	}
	if err := second.Rollback(secondCtx); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(firstCtx); err != nil {
		t.Fatal(err)
	}
	claimed, err := outbox.Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(claimed), 1)
	if err := outbox.MarkSent(ctx, []string{entry.ID}); err != nil {
		t.Fatal(err)
	}

	// Two live transactions recording one marker id: the duplicate is a conflict, not false.
	marker := ProcessedMarker{ID: "processor:conformance:raced", Type: "conformance.Happened", ProcessedAt: now}
	firstCtx, first = beginMemoryTx(t, store)
	secondCtx, second = beginMemoryTx(t, store)
	recorded, err := processed.Record(firstCtx, marker)
	if err != nil {
		t.Fatal(err)
	}
	is(t, recorded, true)
	if _, err := processed.Record(secondCtx, marker); !errors.Is(err, ErrConflict) {
		t.Fatalf("the second record: got %v, want ErrConflict", err)
	}
	if err := second.Rollback(secondCtx); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(firstCtx); err != nil {
		t.Fatal(err)
	}

	// A record a transaction wrote, changed outside it before the commit: the commit conflicts
	// and the outside write stays.
	stale := memoryEntry(t, "stale", now.Add(time.Second), 1)
	staleCtx, staleTx := beginMemoryTx(t, store)
	if err := outbox.Append(staleCtx, stale); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Append(ctx, stale); err != nil {
		t.Fatalf("the outside append: %v", err)
	}
	if err := staleTx.Commit(staleCtx); !errors.Is(err, ErrConflict) {
		t.Fatalf("the stale commit: got %v, want ErrConflict", err)
	}
	claimed, err = outbox.Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(claimed), 1)
	is(t, claimed[0].ID, stale.ID)
	if err := outbox.MarkSent(ctx, []string{stale.ID}); err != nil {
		t.Fatal(err)
	}

	// Two live transactions writing different records both commit.
	leftCtx, left := beginMemoryTx(t, store)
	rightCtx, right := beginMemoryTx(t, store)
	if err := outbox.Append(leftCtx, memoryEntry(t, "left", now.Add(2*time.Second), 2)); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Append(rightCtx, memoryEntry(t, "right", now.Add(3*time.Second), 3)); err != nil {
		t.Fatal(err)
	}
	if err := left.Commit(leftCtx); err != nil {
		t.Fatal(err)
	}
	if err := right.Commit(rightCtx); err != nil {
		t.Fatal(err)
	}
	pending, err := outbox.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is(t, pending, int64(2))
}

func TestMemoryMessageStoreCommittedAppendWakes(t *testing.T) {
	store := NewMemoryMessageStore()
	ctx := context.Background()
	outbox := store.Outbox()
	var wakes atomic.Int32
	cancel := outbox.OnAppend(func() { wakes.Add(1) })

	entry := memoryEntry(t, "wake-now", time.Unix(1_800_000_000, 0), 0)
	if err := outbox.Append(ctx, entry); err != nil {
		t.Fatal(err)
	}
	is(t, wakes.Load(), int32(1))

	txctx, tx := beginMemoryTx(t, store)
	if err := outbox.Append(txctx, memoryEntry(t, "wake-commit", time.Unix(1_800_000_001, 0), 1)); err != nil {
		t.Fatal(err)
	}
	isStore(t, wakes.Load(), int32(1), "not before the commit")
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	is(t, wakes.Load(), int32(2))

	txctx, tx = beginMemoryTx(t, store)
	if err := outbox.Append(txctx, memoryEntry(t, "wake-rollback", time.Unix(1_800_000_002, 0), 2)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	isStore(t, wakes.Load(), int32(2), "a rollback never wakes")

	bad := memoryEntry(t, "wake-bad", time.Unix(1_800_000_003, 0), 3)
	bad.Payload = []byte(`[]`)
	if err := outbox.Append(ctx, bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("the refused append: got %v, want ErrInvalid", err)
	}
	isStore(t, wakes.Load(), int32(2), "a refused append never wakes")

	if err := outbox.Append(ctx, entry); !errors.Is(err, ErrConflict) {
		t.Fatalf("the duplicate append: got %v, want ErrConflict", err)
	}
	isStore(t, wakes.Load(), int32(2), "a duplicate never wakes")

	cancel()
	cancel()
	if err := outbox.Append(ctx, memoryEntry(t, "wake-after", time.Unix(1_800_000_004, 0), 4)); err != nil {
		t.Fatal(err)
	}
	isStore(t, wakes.Load(), int32(2), "a cancelled listener is not called")
}

func TestMemoryMessageStoreProcessedMarkers(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	processed := store.Processed()
	var port ProcessedEvents = processed // the event-store kit's port
	now := clock.Now()

	first := ProcessedMarker{ID: "processor:conformance:first", Type: "conformance.Happened", ProcessedAt: now}
	recorded, err := port.Record(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	is(t, recorded, true)
	recorded, err = port.Record(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, recorded, false, "a recorded id is a duplicate")
	exists, err := processed.Exists(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	is(t, exists, true)

	old := ProcessedMarker{ID: "processor:conformance:old", Type: "conformance.Happened", ProcessedAt: now.Add(-time.Hour)}
	recorded, err = port.Record(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	is(t, recorded, true)
	purged, err := processed.Purge(ctx, now.Add(-30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	is(t, purged, int64(1))
	exists, err = processed.Exists(ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	is(t, exists, false)
	recorded, err = port.Record(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, recorded, true, "a purged id records again")
}

func TestMemoryMessageStoreDeletesOnlyTheMarkerRange(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	ctx := context.Background()
	processed := store.Processed()
	now := clock.Now()
	stage := func(processor, source, segment, position string) ProcessedMarker {
		marker := ProcessedMarker{
			ID:   fmt.Sprintf("processor:%s:%s:%s:%s", processor, source, segment, position),
			Type: "conformance.Happened", ProcessedAt: now, Processor: processor,
			Source: source, Segment: segment, Position: position,
		}
		recorded, err := processed.Record(ctx, marker)
		if err != nil {
			t.Fatal(err)
		}
		is(t, recorded, true)
		return marker
	}
	inRange := []ProcessedMarker{
		stage("p", "conformance.events", "0", "3"),
		stage("p", "conformance.events", "0", "4"),
		stage("p", "conformance.events", "0", "5"),
	}
	kept := []ProcessedMarker{
		stage("p", "conformance.events", "0", "6"), // the range is [3, 6)
		stage("p", "conformance.events", "1", "4"),
		stage("p", "conformance.other", "0", "4"),
		stage("other", "conformance.events", "0", "4"),
		{ID: "saga:conformance.Order:one", Type: "conformance.Happened", ProcessedAt: now},
	}
	// A marker without a position, recorded directly.
	if recorded, err := processed.Record(ctx, kept[len(kept)-1]); err != nil || !recorded {
		t.Fatalf("record without a position: %v, %v", recorded, err)
	}

	deleted, err := processed.DeleteMarkers(ctx, "p", nil)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, deleted, int64(0), "no ranges")
	deleted, err = processed.DeleteMarkers(ctx, "p", []ReplayRange{{
		Source: "conformance.events", Segment: "0", From: "3", Until: "6",
	}})
	if err != nil {
		t.Fatal(err)
	}
	is(t, deleted, int64(3))
	for _, marker := range inRange {
		exists, err := processed.Exists(ctx, marker.ID)
		if err != nil {
			t.Fatal(err)
		}
		isStore(t, exists, false, marker.ID)
	}
	for _, marker := range kept {
		exists, err := processed.Exists(ctx, marker.ID)
		if err != nil {
			t.Fatal(err)
		}
		isStore(t, exists, true, marker.ID)
	}

	// Positions are numbers when they are canonical decimals: [2, 10) keeps 10.
	for _, position := range []string{"2", "9", "10"} {
		stage("q", "conformance.events", "2", position)
	}
	deleted, err = processed.DeleteMarkers(ctx, "q", []ReplayRange{{
		Source: "conformance.events", Segment: "2", From: "2", Until: "10",
	}})
	if err != nil {
		t.Fatal(err)
	}
	is(t, deleted, int64(2))
	exists, err := processed.Exists(ctx, "processor:q:conformance.events:2:10")
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, exists, true, "the numeric range keeps 10")

	// Other positions are text, ordered by bytes.
	for _, position := range []string{"a", "b", "c"} {
		stage("t", "conformance.events", "x", position)
	}
	deleted, err = processed.DeleteMarkers(ctx, "t", []ReplayRange{{
		Source: "conformance.events", Segment: "x", From: "a", Until: "c",
	}})
	if err != nil {
		t.Fatal(err)
	}
	is(t, deleted, int64(2))
}

func TestMemoryMessageStoreCopiesWhatItStores(t *testing.T) {
	store := NewMemoryMessageStore()
	ctx := context.Background()
	outbox := store.Outbox()
	key := "key"
	headers := map[string]string{"correlationId": "c"}
	payload := []byte(`{"n":1}`)
	entry := OutboxEntry{ID: "copy", Type: "conformance.Copied", Key: &key, Headers: headers, Payload: payload, CreatedAt: time.Unix(1_800_000_000, 0)}
	if err := outbox.Append(ctx, entry); err != nil {
		t.Fatal(err)
	}
	key = "changed"
	headers["correlationId"] = "changed"
	payload[0] = 'X'
	claimed, err := outbox.Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(claimed), 1)
	isStore(t, *claimed[0].Key, "key")
	isStore(t, claimed[0].Headers["correlationId"], "c")
	is(t, string(claimed[0].Payload), `{"n":1}`)

	claimed[0].Headers["correlationId"] = "mutated"
	*claimed[0].Key = "mutated"
	claimed[0].Payload[0] = 'X'
	again, err := outbox.Claim(ctx, "relay", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	isStore(t, *again[0].Key, "key")
	isStore(t, again[0].Headers["correlationId"], "c")
	is(t, string(again[0].Payload), `{"n":1}`)
}

func TestMemoryMessageStoreUnsupportedPorts(t *testing.T) {
	store := NewMemoryMessageStore()
	ctx := context.Background()
	for name, err := range map[string]error{
		"dead letters":      func() error { _, err := store.DeadLetters().Count(ctx, ""); return err }(),
		"processor control": func() error { _, err := store.Control().Get(ctx, "p"); return err }(),
		"external calls":    func() error { _, err := store.ExternalCalls().Find(ctx, "k"); return err }(),
		"sagas":             func() error { _, err := store.Sagas().Sagas.FindByID(ctx, "id"); return err }(),
		"saga dispatch":     func() error { _, err := store.Sagas().Dispatch.Pending(ctx); return err }(),
		"saga deadlines":    func() error { _, err := store.Sagas().Deadlines.FindScheduled(ctx, "id"); return err }(),
		"saga failures":     func() error { _, err := store.Sagas().Failures.Find(ctx, "id"); return err }(),
		"saga markers":      func() error { _, err := store.Sagas().Markers.Exists(ctx, "id"); return err }(),
	} {
		if !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s: got %v, want ErrUnsupported", name, err)
		}
	}
}

func TestMemoryMessageStoreBeginWithACancelledContext(t *testing.T) {
	store := NewMemoryMessageStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := store.Begin(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

// L2-3 review: an empty owner is not a claimer; "" is the store's mark for an entry nobody holds.
func TestMemoryMessageStoreRefusesAnEmptyOutboxOwner(t *testing.T) {
	store := NewMemoryMessageStore()
	ctx := context.Background()
	if err := store.Outbox().Append(ctx, memoryEntry(t, "e-1", time.Unix(1_800_000_000, 0), 0)); err != nil {
		t.Fatal(err)
	}
	_, err := store.Outbox().Claim(ctx, "", time.Minute, 10)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Claim with an empty owner: got %v, want ErrInvalid", err)
	}
	claimed, err := store.Outbox().Claim(ctx, "relay", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("a named claim: %d entries, %v", len(claimed), err)
	}
}

// SG-31: a transaction abandoned without Commit or Rollback keeps the records it reserved: a
// second live transaction writing one conflicts at once, and a rollback releases them. The
// reference's in-memory store reserves the row the same way until its transaction ends
// (snodra-messaging@64dde1e:.../store/memory/MemoryTables.kt:77).
func TestAnAbandonedTransactionKeepsItsReservation(t *testing.T) {
	store := NewMemoryMessageStore()
	entry := memoryEntry(t, "e-abandoned", time.Unix(1_800_000_000, 0), 0)
	abandonedCtx, abandoned := beginMemoryTx(t, store)
	if err := store.Outbox().Append(abandonedCtx, entry); err != nil {
		t.Fatal(err)
	}
	// The transaction is neither committed nor rolled back: a second writer conflicts, and its
	// own transaction ends.
	secondCtx, second := beginMemoryTx(t, store)
	err := store.Outbox().Append(secondCtx, entry)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("a second live writer of the reserved entry: got %v, want ErrConflict", err)
	}
	if err := second.Rollback(secondCtx); err != nil {
		t.Fatal(err)
	}
	// Nothing of the abandoned transaction is visible.
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 0 {
		t.Fatalf("pending: %d, %v", pending, err)
	}
	// Rolling the abandoned transaction back releases the record.
	if err := abandoned.Rollback(abandonedCtx); err != nil {
		t.Fatal(err)
	}
	thirdCtx, third := beginMemoryTx(t, store)
	if err := store.Outbox().Append(thirdCtx, entry); err != nil {
		t.Fatalf("after the release: %v", err)
	}
	if err := third.Commit(thirdCtx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(context.Background()); err != nil || pending != 1 {
		t.Fatalf("pending after the commit: %d, %v", pending, err)
	}
}

// The in-memory store provides the optional lifecycle: a clock, no schema, and background work
// that ends with the app.
func TestMemoryMessageStoreLifecycle(t *testing.T) {
	store := NewMemoryMessageStore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- store.Background(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Background did not end with its context")
	}
	if err := store.InitSchema(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	isStore(t, store.Name(), MemoryMessageStoreName)
}
