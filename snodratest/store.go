package snodratest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The lease a store check takes when it must not run out while the check runs.
const storeLongLease = 60 * time.Second

// MessageStore runs the level-2 and level-3 message-store checks on the store open returns, each
// as a subtest named exactly as the Rust kit names it. open returns one store over empty state
// whose schema is ready; the kit calls it once per check, so the checks never see each other's
// data.
//
// The store must be the kit's alone while it runs. A check that fails names the check and the
// store; the other checks run in the Rust kit's order and the first failure stops the kit.
// Every check is exported, so a failing one can be run by itself.
func MessageStore(t *testing.T, open func() snodra.MessageStore) {
	t.Helper()
	if open == nil {
		t.Fatal("snodratest: open is nil")
	}
	name := "?"
	for _, check := range messageStoreChecks {
		check := check
		if !t.Run(check.id, func(t *testing.T) {
			store := open()
			if store == nil {
				t.Fatal("snodratest: open returned a nil message store")
			}
			name = store.Name()
			check.run(t, store)
		}) {
			t.Fatalf("message store conformance check %q failed on store %q", check.id, name)
		}
	}
}

// StoreCheckIDs returns the ids of the message-store checks the Go kit carries (level 2 and level
// 3), in the Rust kit's order.
func StoreCheckIDs() []string {
	ids := make([]string, len(messageStoreChecks))
	for i, check := range messageStoreChecks {
		ids[i] = check.id
	}
	return ids
}

// storeTB is what a check needs of testing.TB; testing.TB satisfies it, and a kit test can
// supply a recorder to prove a broken store fails a check.
type storeTB interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Fatal(args ...any)
}

// messageStoreChecks is the level-2 and level-3 set, in the Rust kit's order. store_checks.txt is
// the checked-in copy of these ids, and a test pins it to a literal copy of that part of the Rust
// list (its level-4 checks are not in the Go kit yet).
var messageStoreChecks = []struct {
	id  string
	run func(testing.TB, snodra.MessageStore)
}{
	{"appended_entry_is_claimed_in_created_seq_order", AppendedEntryIsClaimedInCreatedSeqOrder},
	{"outbox_rejects_a_payload_that_is_not_a_json_object", OutboxRejectsAPayloadThatIsNotAJSONObject},
	{"pending_counts_entries_neither_sent_nor_failed", PendingCountsEntriesNeitherSentNorFailed},
	{"rolled_back_transaction_leaves_no_outbox_entry", RolledBackTransactionLeavesNoOutboxEntry},
	{"rolled_back_handler_leaves_no_processed_marker", RolledBackHandlerLeavesNoProcessedMarker},
	{"processed_marker_records_once", ProcessedMarkerRecordsOnce},
	{"delete_markers_removes_only_the_range", DeleteMarkersRemovesOnlyTheRange},
	{"concurrent_relays_publish_each_entry_once", ConcurrentRelaysPublishEachEntryOnce},
	{"expired_lease_is_reclaimed", ExpiredLeaseIsReclaimed},
	{"purge_removes_only_sent_before", PurgeRemovesOnlySentBefore},
	{"dead_letters_sequence_per_key", DeadLettersSequencePerKey},
	{"dead_letter_parked_inside_rolled_back_transaction_is_kept", DeadLetterParkedInsideRolledBackTransactionIsKept},
	{"dead_letter_deleted_in_a_transaction_goes_with_it", DeadLetterDeletedInATransactionGoesWithIt},
	{"control_lock_is_exclusive_until_lease_expires", ControlLockIsExclusiveUntilLeaseExpires},
	{"pause_state_round_trips", PauseStateRoundTrips},
	{"replay_ranges_round_trip", ReplayRangesRoundTrip},
	{"processor_control_write_inside_rolled_back_transaction_is_kept", ProcessorControlWriteInsideRolledBackTransactionIsKept},
}

// appendedEntryIsClaimedInCreatedSeqOrder runs the check for a testing.TB or a recorder.
func appendedEntryIsClaimedInCreatedSeqOrder(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	base := time.Now().Add(-time.Minute)
	e1 := storeEntry(tb, "conformance.Ordered", base, 2)
	e2 := storeEntry(tb, "conformance.Ordered", base, 1)
	e3 := storeEntry(tb, "conformance.Ordered", base.Add(-time.Second), 9)
	e4 := storeEntry(tb, "conformance.Ordered", base.Add(time.Second), 0)

	txctx, tx := beginStore(tb, store)
	appendStore(tb, outbox, txctx, e1)
	commitStore(tb, tx, txctx)
	appendStore(tb, outbox, context.Background(), e2)
	txctx, tx = beginStore(tb, store)
	appendStore(tb, outbox, txctx, e3)
	commitStore(tb, tx, txctx)
	appendStore(tb, outbox, context.Background(), e4)

	first := claimStore(tb, outbox, "relay-a", storeLongLease, 2)
	if got, want := storeIDs(first), []string{e3.ID, e2.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("the oldest two, by (created_at, seq): got %v, want %v", got, want)
	}
	second := claimStore(tb, outbox, "relay-b", storeLongLease, 10)
	if got, want := storeIDs(second), []string{e1.ID, e4.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("another owner gets what relay-a does not hold: got %v, want %v", got, want)
	}
	var found *snodra.OutboxEntry
	for i := range second {
		if second[i].ID == e1.ID {
			found = &second[i]
		}
	}
	if found == nil {
		tb.Fatalf("%s was not claimed", e1.ID)
	}
	if found.Type != e1.Type {
		tb.Errorf("type: got %q, want %q", found.Type, e1.Type)
	}
	if !sameKey(found.Key, e1.Key) {
		tb.Errorf("key: got %v, want %v", shownKey(found.Key), shownKey(e1.Key))
	}
	if !reflect.DeepEqual(found.Headers, e1.Headers) {
		tb.Errorf("headers: got %v, want %v", found.Headers, e1.Headers)
	}
	sameJSON(tb, found.Payload, e1.Payload)
	if !found.CreatedAt.Equal(e1.CreatedAt) {
		tb.Errorf("created at: got %v, want %v", found.CreatedAt, e1.CreatedAt)
	}
	if found.Seq != e1.Seq {
		tb.Errorf("seq: got %d, want %d", found.Seq, e1.Seq)
	}
	if found.Attempts != 0 {
		tb.Errorf("attempts: got %d, want 0", found.Attempts)
	}

	markAttemptFailedStore(tb, outbox, e3.ID, "conformance: broker unavailable")
	again := claimStore(tb, outbox, "relay-a", storeLongLease, 10)
	if got, want := storeIDs(again), []string{e3.ID, e2.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("relay-a's own entries, again: got %v, want %v", got, want)
	}
	if again[0].Attempts != 1 {
		tb.Fatalf("a retried entry counts the failed attempt: got %d, want 1", again[0].Attempts)
	}

	all := []string{e1.ID, e2.ID, e3.ID, e4.ID}
	markSentStore(tb, outbox, all)
	if left := claimStore(tb, outbox, "relay-c", storeLongLease, 10); len(left) != 0 {
		tb.Fatalf("a sent entry is never claimed: %v", storeIDs(left))
	}
}

// outboxRejectsAPayloadThatIsNotAJSONObject runs the check for a testing.TB or a recorder.
func outboxRejectsAPayloadThatIsNotAJSONObject(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	tooDeep := []byte(storeNested(snodra.MaxPayloadDepth + 1))
	for _, payload := range [][]byte{
		[]byte("not json"),
		[]byte(`{"n":1e400}`),
		[]byte(`{"n":-1e400}`),
		[]byte(`{"s":"\ud800"}`),
		[]byte(`{"\udc00":1}`),
		append([]byte{0xEF, 0xBB, 0xBF}, []byte("{}")...),
		[]byte("{\"s\":\"\xff\"}"),
		tooDeep,
		[]byte("[1,2]"),
		[]byte(`"text"`),
		[]byte("null"),
		[]byte(`{"n":18446744073709551615}`),
		// Read as floats by a JSON parser, so only the text tells them apart.
		[]byte(`{"n":-9223372036854775809}`),
		[]byte(`{"n":18446744073709551616}`),
		[]byte(`{"n":123456789012345678901234567890}`),
		[]byte(`{"a":[1,{"b":[2,-99999999999999999999]}]}`),
	} {
		shown := string(payload)
		bad := storeEntryWithPayload(tb, "conformance.Invalid", time.Now(), 0, payload)
		err := outbox.Append(context.Background(), bad)
		if !errors.Is(err, snodra.ErrInvalid) {
			tb.Fatalf("%s on its own: got %v, want ErrInvalid", shown, err)
		}
		txctx, tx := beginStore(tb, store)
		err = outbox.Append(txctx, bad)
		rollbackStore(tb, tx, txctx)
		if !errors.Is(err, snodra.ErrInvalid) {
			tb.Fatalf("%s in a transaction: got %v, want ErrInvalid", shown, err)
		}
	}
	if claimed := claimStore(tb, outbox, "relay-a", storeLongLease, 100); len(claimed) != 0 {
		tb.Fatalf("nothing is appended: %v", storeIDs(claimed))
	}

	// The same digits in a string, a decimal, an exponent and the signed 64-bit bounds are
	// fine.
	fine := storeEntryWithPayload(tb, "conformance.Fine", time.Now(), 0,
		[]byte(`{"s":"123456789012345678901234567890","d":1.5,"e":1e30,"max":9223372036854775807,"min":-9223372036854775808}`))
	appendStore(tb, outbox, context.Background(), fine)
	claimed := claimStore(tb, outbox, "relay-a", storeLongLease, 100)
	if got, want := storeIDs(claimed), []string{fine.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("a payload within the contract: got %v, want %v", got, want)
	}
	markSentStore(tb, outbox, storeIDs(claimed))

	// The deepest payload allowed is kept as appended, on every store.
	deepest := storeEntryWithPayload(tb, "conformance.Deep", time.Now(), 0,
		[]byte(storeNested(snodra.MaxPayloadDepth)))
	appendStore(tb, outbox, context.Background(), deepest)
	claimed = claimStore(tb, outbox, "relay-a", storeLongLease, 100)
	if got, want := storeIDs(claimed), []string{deepest.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("the deepest payload allowed: got %v, want %v", got, want)
	}
	sameJSON(tb, claimed[0].Payload, deepest.Payload)
	markSentStore(tb, outbox, storeIDs(claimed))
}

// pendingCountsEntriesNeitherSentNorFailed runs the check for a testing.TB or a recorder.
func pendingCountsEntriesNeitherSentNorFailed(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	if pending := pendingStore(tb, outbox); pending != 0 {
		tb.Fatalf("drained: got %d pending, want 0", pending)
	}
	if _, ok := oldestPendingStore(tb, outbox); ok {
		tb.Fatal("drained: an oldest pending entry")
	}
	now := time.Now()
	e1 := storeEntry(tb, "conformance.Pending", now.Add(-3*time.Second), 1)
	e2 := storeEntry(tb, "conformance.Pending", now.Add(-2*time.Second), 2)
	e3 := storeEntry(tb, "conformance.Pending", now.Add(-time.Second), 3)
	for _, entry := range []snodra.OutboxEntry{e3, e1, e2} {
		appendStore(tb, outbox, context.Background(), entry)
	}
	if pending := pendingStore(tb, outbox); pending != 3 {
		tb.Fatalf("appended: got %d pending, want 3", pending)
	}
	if oldest, ok := oldestPendingStore(tb, outbox); !ok || !oldest.Equal(e1.CreatedAt) {
		tb.Fatalf("oldest: got %v (%v), want %v", oldest, ok, e1.CreatedAt)
	}
	if held := claimStore(tb, outbox, "relay-a", storeLongLease, 3); len(held) != 3 {
		tb.Fatalf("held: got %d entries, want 3", len(held))
	}
	if pending := pendingStore(tb, outbox); pending != 3 {
		tb.Fatalf("held is pending: got %d, want 3", pending)
	}
	if oldest, ok := oldestPendingStore(tb, outbox); !ok || !oldest.Equal(e1.CreatedAt) {
		tb.Fatalf("held is pending: got %v (%v), want %v", oldest, ok, e1.CreatedAt)
	}
	markSentStore(tb, outbox, []string{e1.ID})
	if pending := pendingStore(tb, outbox); pending != 2 {
		tb.Fatalf("sent is not pending: got %d, want 2", pending)
	}
	if oldest, ok := oldestPendingStore(tb, outbox); !ok || !oldest.Equal(e2.CreatedAt) {
		tb.Fatalf("sent is not pending: got %v (%v), want %v", oldest, ok, e2.CreatedAt)
	}
	markFailedStore(tb, outbox, e2.ID, "conformance: rejected")
	if pending := pendingStore(tb, outbox); pending != 1 {
		tb.Fatalf("failed is not pending: got %d, want 1", pending)
	}
	if oldest, ok := oldestPendingStore(tb, outbox); !ok || !oldest.Equal(e3.CreatedAt) {
		tb.Fatalf("failed is not pending: got %v (%v), want %v", oldest, ok, e3.CreatedAt)
	}
	markAttemptFailedStore(tb, outbox, e3.ID, "conformance: retry")
	if pending := pendingStore(tb, outbox); pending != 1 {
		tb.Fatalf("a retried entry is pending: got %d, want 1", pending)
	}
	markSentStore(tb, outbox, []string{e3.ID})
	if pending := pendingStore(tb, outbox); pending != 0 {
		tb.Fatalf("sent: got %d pending, want 0", pending)
	}
	if _, ok := oldestPendingStore(tb, outbox); ok {
		tb.Fatal("sent: an oldest pending entry")
	}
}

// rolledBackTransactionLeavesNoOutboxEntry runs the check for a testing.TB or a recorder.
func rolledBackTransactionLeavesNoOutboxEntry(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	rolledBack := storeEntry(tb, "conformance.RolledBack", time.Now(), 1)
	committed := storeEntry(tb, "conformance.Committed", time.Now(), 2)
	txctx, tx := beginStore(tb, store)
	appendStore(tb, outbox, txctx, rolledBack)
	rollbackStore(tb, tx, txctx)
	txctx, tx = beginStore(tb, store)
	appendStore(tb, outbox, txctx, committed)
	commitStore(tb, tx, txctx)

	claimed := claimStore(tb, outbox, "relay", storeLongLease, 100)
	if got, want := storeIDs(claimed), []string{committed.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("only the committed entry: got %v, want %v", got, want)
	}
	markSentStore(tb, outbox, storeIDs(claimed))
}

// rolledBackHandlerLeavesNoProcessedMarker runs the check for a testing.TB or a recorder.
func rolledBackHandlerLeavesNoProcessedMarker(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	processed := store.Processed()
	drainOutbox(tb, outbox)
	marker := storeMarker(time.Now())
	published := storeEntry(tb, "conformance.PublishedByHandler", time.Now(), 1)

	txctx, tx := beginStore(tb, store)
	if !recordStoreMarker(tb, processed, txctx, marker) {
		tb.Fatal("the marker was recorded already")
	}
	appendStore(tb, outbox, txctx, published)
	rollbackStore(tb, tx, txctx)

	if claimed := claimStore(tb, outbox, "relay", storeLongLease, 100); len(claimed) != 0 {
		tb.Fatalf("the rolled-back handler's event is gone: %v", storeIDs(claimed))
	}
	if existsStoreMarker(tb, processed, marker.ID) {
		tb.Fatal("the marker rolled back with the handler")
	}
	txctx, tx = beginStore(tb, store)
	if !recordStoreMarker(tb, processed, txctx, marker) {
		tb.Fatal("the rolled-back handler's marker is gone: the redelivery is not recorded")
	}
	if existsStoreMarker(tb, processed, marker.ID) {
		tb.Fatal("not committed yet")
	}
	commitStore(tb, tx, txctx)
	if !existsStoreMarker(tb, processed, marker.ID) {
		tb.Fatal("the marker committed with the handler")
	}
}

// processedMarkerRecordsOnce runs the check for a testing.TB or a recorder.
func processedMarkerRecordsOnce(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	processed := store.Processed()
	now := time.Now()
	first := stageOneMarker(tb, "conformance-"+strconv.FormatUint(nextNumber(), 10), "snodra.events", "3", 42)
	if existsStoreMarker(tb, processed, first.ID) {
		tb.Fatal("an unrecorded marker exists")
	}
	txctx, tx := beginStore(tb, store)
	if !recordStoreMarker(tb, processed, txctx, first) {
		tb.Fatal("the first record was a duplicate")
	}
	commitStore(tb, tx, txctx)
	if !existsStoreMarker(tb, processed, first.ID) {
		tb.Fatal("the recorded marker does not exist")
	}

	txctx, tx = beginStore(tb, store)
	if recordStoreMarker(tb, processed, txctx, first) {
		tb.Fatal("a recorded id is a duplicate")
	}
	rollbackStore(tb, tx, txctx)
	txctx, tx = beginStore(tb, store)
	sameID := first
	sameID.Type = "conformance.Other"
	sameID.ProcessedAt = now.Add(-time.Second)
	if recordStoreMarker(tb, processed, txctx, sameID) {
		tb.Fatal("the id alone decides")
	}
	rollbackStore(tb, tx, txctx)

	other := storeMarker(now)
	txctx, tx = beginStore(tb, store)
	if !recordStoreMarker(tb, processed, txctx, other) {
		tb.Fatal("another id does not record")
	}
	commitStore(tb, tx, txctx)

	old := storeMarker(now.Add(-time.Hour))
	txctx, tx = beginStore(tb, store)
	if !recordStoreMarker(tb, processed, txctx, old) {
		tb.Fatal("an old marker does not record")
	}
	commitStore(tb, tx, txctx)
	purged := purgeStoreMarkers(tb, processed, now.Add(-30*time.Minute))
	if purged < 1 {
		tb.Fatalf("the old marker is purged: purged %d", purged)
	}
	if existsStoreMarker(tb, processed, old.ID) {
		tb.Fatal("the old marker survived the purge")
	}
	if !existsStoreMarker(tb, processed, first.ID) {
		tb.Fatal("the purge removed the recent marker")
	}
	txctx, tx = beginStore(tb, store)
	if !recordStoreMarker(tb, processed, txctx, old) {
		tb.Fatal("a purged id does not record again")
	}
	commitStore(tb, tx, txctx)
	txctx, tx = beginStore(tb, store)
	if recordStoreMarker(tb, processed, txctx, first) {
		tb.Fatal("the purge did not keep the recent marker")
	}
	rollbackStore(tb, tx, txctx)
}

// concurrentRelaysPublishEachEntryOnce runs the check for a testing.TB or a recorder.
func concurrentRelaysPublishEachEntryOnce(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	const entries = 200
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	now := time.Now()
	appended := make(map[string]bool, entries)
	for seq := int64(0); seq < entries; seq++ {
		entry := storeEntry(tb, "conformance.Concurrent", now, seq)
		appended[entry.ID] = true
		appendStore(tb, outbox, context.Background(), entry)
	}

	start := make(chan struct{})
	// ready is closed once both relays made their first claim, or once one of them failed before
	// it could: an early error must not leave the other waiting at the barrier.
	var arrived atomic.Int32
	var readyOnce sync.Once
	var ready = make(chan struct{})
	open := func() { readyOnce.Do(func() { close(ready) }) }
	type result struct {
		published []string
		err       error
	}
	results := make(chan result, 2)
	relay := func(owner string) {
		<-start
		var published []string
		first := true
		for {
			batch, err := outbox.Claim(context.Background(), owner, storeLongLease, 7)
			if err != nil {
				open()
				results <- result{err: err}
				return
			}
			if first {
				first = false
				if arrived.Add(1) == 2 {
					open()
				}
				<-ready
			}
			if len(batch) == 0 {
				results <- result{published: published}
				return
			}
			ids := storeIDs(batch)
			if err := outbox.MarkSent(context.Background(), ids); err != nil {
				open()
				results <- result{err: err}
				return
			}
			published = append(published, ids...)
			runtime.Gosched()
		}
	}
	go relay("relay-a")
	go relay("relay-b")
	close(start)

	var a, b []string
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.err != nil {
				tb.Fatalf("relay: %v", got.err)
			}
			if i == 0 {
				a = got.published
			} else {
				b = got.published
			}
		case <-time.After(wait):
			tb.Fatal("a relay did not finish")
		}
	}
	aSet := sliceSet(a)
	bSet := sliceSet(b)
	if len(aSet) != len(a) {
		tb.Fatalf("relay-a published an entry twice: %v", a)
	}
	if len(bSet) != len(b) {
		tb.Fatalf("relay-b published an entry twice: %v", b)
	}
	if len(a) == 0 || len(b) == 0 {
		tb.Fatalf("both relays claimed entries: %d and %d", len(a), len(b))
	}
	for id := range aSet {
		if bSet[id] {
			tb.Fatalf("published by both relays: %s", id)
		}
	}
	for id := range appended {
		if !aSet[id] && !bSet[id] {
			tb.Fatalf("entry %s was never published", id)
		}
	}
	if len(aSet)+len(bSet) != len(appended) {
		tb.Fatalf("published %d entries, want %d", len(aSet)+len(bSet), len(appended))
	}
}

// expiredLeaseIsReclaimed runs the check for a testing.TB or a recorder.
func expiredLeaseIsReclaimed(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	now := time.Now()
	entries := []snodra.OutboxEntry{
		storeEntry(tb, "conformance.Leased", now, 0),
		storeEntry(tb, "conformance.Leased", now, 1),
		storeEntry(tb, "conformance.Leased", now, 2),
	}
	for _, entry := range entries {
		appendStore(tb, outbox, context.Background(), entry)
	}
	lease := 3 * time.Second
	crashed := claimStore(tb, outbox, "crashed", lease, 10)
	if got, want := storeIDs(crashed), storeIDs(entries); !reflect.DeepEqual(got, want) {
		tb.Fatalf("claimed: got %v, want %v", got, want)
	}
	markAttemptFailedStore(tb, outbox, entries[0].ID, "conformance: broker unavailable")
	markFailedStore(tb, outbox, entries[2].ID, "conformance: record too large")
	if early := claimStore(tb, outbox, "survivor", storeLongLease, 10); len(early) != 0 {
		tb.Fatalf("held while the lease runs: %v", storeIDs(early))
	}
	time.Sleep(lease + time.Second)
	reclaimed := claimStore(tb, outbox, "survivor", storeLongLease, 10)
	if got, want := storeIDs(reclaimed), storeIDs(entries[:2]); !reflect.DeepEqual(got, want) {
		tb.Fatalf("reclaimed in order after the lease, without the failed entry: got %v, want %v",
			got, want)
	}
	if reclaimed[0].Attempts != 1 {
		tb.Fatalf("the failed attempt is counted: got %d, want 1", reclaimed[0].Attempts)
	}
	if reclaimed[1].Attempts != 0 {
		tb.Fatalf("the untouched attempt: got %d, want 0", reclaimed[1].Attempts)
	}
	if held := claimStore(tb, outbox, "crashed", storeLongLease, 10); len(held) != 0 {
		tb.Fatalf("the survivor holds them now: %v", storeIDs(held))
	}
	markSentStore(tb, outbox, storeIDs(reclaimed))
}

// purgeRemovesOnlySentBefore runs the check for a testing.TB or a recorder.
func purgeRemovesOnlySentBefore(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	outbox := store.Outbox()
	drainOutbox(tb, outbox)
	purgeStore(tb, outbox, time.Now().Add(24*time.Hour))
	now := time.Now()
	entries := []snodra.OutboxEntry{
		storeEntry(tb, "conformance.Purged", now, 0),
		storeEntry(tb, "conformance.Purged", now, 1),
		storeEntry(tb, "conformance.Purged", now, 2),
		storeEntry(tb, "conformance.Purged", now, 3),
	}
	early, late, failed, pending := entries[0], entries[1], entries[2], entries[3]
	for _, entry := range entries {
		appendStore(tb, outbox, context.Background(), entry)
	}
	if claimed := claimStore(tb, outbox, "relay", storeLongLease, 10); len(claimed) != 4 {
		tb.Fatalf("claimed: got %d entries, want 4", len(claimed))
	}
	markSentStore(tb, outbox, []string{early.ID})
	time.Sleep(time.Second)
	bound := time.Now()
	time.Sleep(time.Second)
	markSentStore(tb, outbox, []string{late.ID})
	markFailedStore(tb, outbox, failed.ID, "conformance: given up")

	if purged := purgeStore(tb, outbox, bound); purged != 1 {
		tb.Fatalf("only the early sent entry: purged %d, want 1", purged)
	}
	if purged := purgeStore(tb, outbox, bound); purged != 0 {
		tb.Fatalf("purged twice: purged %d, want 0", purged)
	}
	left := claimStore(tb, outbox, "relay", storeLongLease, 10)
	if got, want := storeIDs(left), []string{pending.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("the pending entry stays, the failed one is not claimed: got %v, want %v", got, want)
	}
	markSentStore(tb, outbox, []string{pending.ID})
	if purged := purgeStore(tb, outbox, time.Now().Add(time.Minute)); purged != 2 {
		tb.Fatalf("the late and the pending entry once sent; never the failed one: purged %d, want 2",
			purged)
	}
}

// AppendedEntryIsClaimedInCreatedSeqOrder: entries are claimed in (CreatedAt, Seq) order
// whichever way they were appended (in a transaction or not); a claim skips what another owner
// holds, gives an owner back what it holds already, and returns every field as appended. A
// retried entry keeps its place and counts the attempt.
func AppendedEntryIsClaimedInCreatedSeqOrder(tb testing.TB, store snodra.MessageStore) {
	appendedEntryIsClaimedInCreatedSeqOrder(tb, store)
}

// OutboxRejectsAPayloadThatIsNotAJSONObject: an outbox payload is a JSON object whose integers
// fit in a signed 64-bit integer, at most MaxPayloadDepth deep (MySQL's JSON_DEPTH): anything
// else (not JSON, an array, a bare value, an integer above the signed 64-bit range, a number no
// double holds, a lone surrogate escape, a leading byte order mark, bytes that are not UTF-8,
// one deeper) is refused with ErrInvalid, appended on its own or in a transaction, and nothing
// is appended. The deepest payload allowed is kept by every store.
func OutboxRejectsAPayloadThatIsNotAJSONObject(tb testing.TB, store snodra.MessageStore) {
	outboxRejectsAPayloadThatIsNotAJSONObject(tb, store)
}

// PendingCountsEntriesNeitherSentNorFailed: Pending counts the entries neither sent nor failed,
// held by a relay or not, and OldestPending is the CreatedAt of the oldest of them.
func PendingCountsEntriesNeitherSentNorFailed(tb testing.TB, store snodra.MessageStore) {
	pendingCountsEntriesNeitherSentNorFailed(tb, store)
}

// RolledBackTransactionLeavesNoOutboxEntry: an entry appended in a transaction that is rolled
// back never reaches the relay; one appended in a committed transaction does.
func RolledBackTransactionLeavesNoOutboxEntry(tb testing.TB, store snodra.MessageStore) {
	rolledBackTransactionLeavesNoOutboxEntry(tb, store)
}

// RolledBackHandlerLeavesNoProcessedMarker: a handler's transaction that is rolled back leaves
// neither its processed-event marker nor the events it published: a redelivery handles the
// event again.
func RolledBackHandlerLeavesNoProcessedMarker(tb testing.TB, store snodra.MessageStore) {
	rolledBackHandlerLeavesNoProcessedMarker(tb, store)
}

// ProcessedMarkerRecordsOnce: a marker records once: a second record of its id (in a later
// transaction, whatever else it carries) is false, until a purge past its time removes it. A
// purge keeps the markers recorded after its bound.
func ProcessedMarkerRecordsOnce(tb testing.TB, store snodra.MessageStore) {
	processedMarkerRecordsOnce(tb, store)
}

// ConcurrentRelaysPublishEachEntryOnce: two relays claiming one outbox at the same time (started
// together) claim every entry exactly once, and each claims some while the other holds its first
// batch (a barrier after both first claims, before any entry is marked sent).
func ConcurrentRelaysPublishEachEntryOnce(tb testing.TB, store snodra.MessageStore) {
	concurrentRelaysPublishEachEntryOnce(tb, store)
}

// ExpiredLeaseIsReclaimed: a claimer that stops (crashes) holds its entries until its lease runs
// out; then another relay claims them, in order, with the attempts counted and without the entry
// given up on.
func ExpiredLeaseIsReclaimed(tb testing.TB, store snodra.MessageStore) {
	expiredLeaseIsReclaimed(tb, store)
}

// PurgeRemovesOnlySentBefore: a purge removes the entries sent before its bound and nothing
// else: neither later sent ones, nor pending ones, nor failed ones (a failed entry is never
// claimed again either).
func PurgeRemovesOnlySentBefore(tb testing.TB, store snodra.MessageStore) {
	purgeRemovesOnlySentBefore(tb, store)
}

// ---- helpers -----------------------------------------------------------------------------------

// beginStore starts a transaction. The context it returns stays alive until the test ends: the
// transaction's writes are made with it after this function returned.
func beginStore(tb storeTB, store snodra.MessageStore) (context.Context, snodra.Tx) {
	tb.Helper()
	txctx, tx, err := store.Begin(context.Background())
	if err != nil {
		tb.Fatalf("begin: %v", err)
	}
	if tx == nil {
		tb.Fatal("begin: a nil transaction")
	}
	return txctx, tx
}

func commitStore(tb storeTB, tx snodra.Tx, ctx context.Context) {
	tb.Helper()
	if err := tx.Commit(ctx); err != nil {
		tb.Fatalf("commit: %v", err)
	}
}

func rollbackStore(tb storeTB, tx snodra.Tx, ctx context.Context) {
	tb.Helper()
	if err := tx.Rollback(ctx); err != nil {
		tb.Fatalf("rollback: %v", err)
	}
}

func appendStore(tb storeTB, outbox snodra.OutboxStore, ctx context.Context, entry snodra.OutboxEntry) {
	tb.Helper()
	if err := outbox.Append(ctx, entry); err != nil {
		tb.Fatalf("append %s: %v", entry.ID, err)
	}
}

func claimStore(tb storeTB, outbox snodra.OutboxStore, owner string, lease time.Duration, limit int) []snodra.OutboxEntry {
	tb.Helper()
	claimed, err := outbox.Claim(context.Background(), owner, lease, limit)
	if err != nil {
		tb.Fatalf("claim (%s): %v", owner, err)
	}
	return claimed
}

func markSentStore(tb storeTB, outbox snodra.OutboxStore, ids []string) {
	tb.Helper()
	if err := outbox.MarkSent(context.Background(), ids); err != nil {
		tb.Fatalf("mark sent: %v", err)
	}
}

func markAttemptFailedStore(tb storeTB, outbox snodra.OutboxStore, id, cause string) {
	tb.Helper()
	if err := outbox.MarkAttemptFailed(context.Background(), id, cause); err != nil {
		tb.Fatalf("mark attempt failed: %v", err)
	}
}

func markFailedStore(tb storeTB, outbox snodra.OutboxStore, id, cause string) {
	tb.Helper()
	if err := outbox.MarkFailed(context.Background(), id, cause); err != nil {
		tb.Fatalf("mark failed: %v", err)
	}
}

func pendingStore(tb storeTB, outbox snodra.OutboxStore) int64 {
	tb.Helper()
	pending, err := outbox.Pending(context.Background())
	if err != nil {
		tb.Fatalf("pending: %v", err)
	}
	return pending
}

func oldestPendingStore(tb storeTB, outbox snodra.OutboxStore) (time.Time, bool) {
	tb.Helper()
	oldest, ok, err := outbox.OldestPending(context.Background())
	if err != nil {
		tb.Fatalf("oldest pending: %v", err)
	}
	return oldest, ok
}

func purgeStore(tb storeTB, outbox snodra.OutboxStore, before time.Time) int64 {
	tb.Helper()
	purged, err := outbox.Purge(context.Background(), before)
	if err != nil {
		tb.Fatalf("purge: %v", err)
	}
	return purged
}

func recordStoreMarker(tb storeTB, processed snodra.ProcessedEvents, ctx context.Context, marker snodra.ProcessedMarker) bool {
	tb.Helper()
	recorded, err := processed.Record(ctx, marker)
	if err != nil {
		tb.Fatalf("record marker %s: %v", marker.ID, err)
	}
	return recorded
}

func existsStoreMarker(tb storeTB, processed snodra.ProcessedStore, id string) bool {
	tb.Helper()
	exists, err := processed.Exists(context.Background(), id)
	if err != nil {
		tb.Fatalf("marker %s exists: %v", id, err)
	}
	return exists
}

func purgeStoreMarkers(tb storeTB, processed snodra.ProcessedStore, before time.Time) int64 {
	tb.Helper()
	purged, err := processed.Purge(context.Background(), before)
	if err != nil {
		tb.Fatalf("purge markers: %v", err)
	}
	return purged
}

// drainOutbox relays (marks sent) whatever another check left claimable, so the next claim sees
// only the calling check's entries.
func drainOutbox(tb storeTB, outbox snodra.OutboxStore) {
	tb.Helper()
	owner := fresh("drain")
	for {
		left := claimStore(tb, outbox, owner, storeLongLease, 1000)
		if len(left) == 0 {
			return
		}
		markSentStore(tb, outbox, storeIDs(left))
	}
}

// storeEntry is one outbox entry with a fresh id and correlation id.
func storeEntry(tb storeTB, typeName string, createdAt time.Time, seq int64) snodra.OutboxEntry {
	tb.Helper()
	return storeEntryWithPayload(tb, typeName, createdAt, seq,
		[]byte(fmt.Sprintf(`{"seq":%d,"text":"é","nested":{"ok":true}}`, seq)))
}

// storeEntryWithPayload is storeEntry with the payload given. The time is truncated to
// milliseconds, the precision every message store keeps (the SQL layout's TIMESTAMPTZ(3) and
// DATETIME(3)), so a claimed entry equals the one appended.
func storeEntryWithPayload(tb storeTB, typeName string, createdAt time.Time, seq int64, payload []byte) snodra.OutboxEntry {
	tb.Helper()
	key := "key-" + strconv.FormatInt(seq, 10)
	return snodra.OutboxEntry{
		ID:      fresh("entry"),
		Type:    typeName,
		Key:     &key,
		Headers: map[string]string{snodra.HeaderCorrelationID: fresh("correlation"), snodra.HeaderUserID: ""},
		Payload: payload,
		// TODO(spec-gap): SG-40 (the SQL layout keeps milliseconds; the kit builds the times it
		// compares at that precision, as the reference kit does).
		CreatedAt: createdAt.Truncate(time.Millisecond),
		Seq:       seq,
	}
}

// storeMarker is a marker that carries no position.
func storeMarker(processedAt time.Time) snodra.ProcessedMarker {
	return snodra.ProcessedMarker{
		ID:          "processor:conformance:" + fresh("event"),
		Type:        "conformance.Happened",
		ProcessedAt: processedAt,
	}
}

// stageOneMarker is the stage-1 marker of a record, carrying its processor, source, segment and
// position.
func stageOneMarker(tb storeTB, processor, source, segment string, position int64) snodra.ProcessedMarker {
	tb.Helper()
	return snodra.ProcessedMarker{
		ID:          "processor:" + processor + ":" + fresh("event"),
		Type:        "conformance.Happened",
		ProcessedAt: time.Now(),
		Processor:   processor,
		Source:      source,
		Segment:     segment,
		Position:    strconv.FormatInt(position, 10),
	}
}

// storeNested is a JSON object depth deep as MySQL's JSON_DEPTH counts: {"a":{"a":...{"a":1}...}},
// {"a":1} being depth 2.
func storeNested(depth int) string {
	objects := depth - 1
	return strings.Repeat(`{"a":`, objects) + "1" + strings.Repeat("}", objects)
}

// storeIDs is the entries' ids.
func storeIDs(entries []snodra.OutboxEntry) []string {
	ids := make([]string, len(entries))
	for i, entry := range entries {
		ids[i] = entry.ID
	}
	return ids
}

// sliceSet is the strings of values, each once.
func sliceSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

// sameJSON fails the check unless two payloads are the same JSON value.
func sameJSON(tb storeTB, got, want []byte) {
	tb.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		tb.Fatalf("the claimed payload is not JSON: %v", err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		tb.Fatalf("the appended payload is not JSON: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		tb.Fatalf("the payload changed: got %s, want %s", got, want)
	}
}
