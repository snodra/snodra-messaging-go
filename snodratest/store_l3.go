package snodratest

import (
	"bytes"
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The level-3 message-store checks, in the Rust kit's order: dead letters sequenced per key, a
// letter that commits on its own and one deleted with its transaction, the processor-control
// lock, a pause, the replay ranges, a control write outside the caller's transaction, and a
// reset's marker deletion.

// DeleteMarkersRemovesOnlyTheRange: a reset deletes the stage-1 markers of its processor in its
// ranges and nothing else: no other position, segment, source or processor, no marker without a
// position.
func DeleteMarkersRemovesOnlyTheRange(tb testing.TB, store snodra.MessageStore) {
	deleteMarkersRemovesOnlyTheRange(tb, store)
}

func deleteMarkersRemovesOnlyTheRange(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	processed := store.Processed()
	processor := fresh("conformance")
	other := fresh("conformance")
	const source = "snodra.events"
	record := func(processor, source, segment string, position int64) string {
		tb.Helper()
		marker := stageOneMarker(tb, processor, source, segment, position)
		if !recordStoreMarker(tb, processed, context.Background(), marker) {
			tb.Fatalf("recorded %s", marker.ID)
		}
		return marker.ID
	}
	var inRange []string
	for _, position := range []int64{3, 4, 5} {
		inRange = append(inRange, record(processor, source, "0", position))
	}
	inRange = append(inRange, record(processor, source, "2", 7))
	var kept []string
	for _, position := range []int64{0, 1, 2, 6} {
		kept = append(kept, record(processor, source, "0", position))
	}
	kept = append(kept,
		record(processor, source, "1", 4),
		record(processor, "conformance.other", "0", 4),
		record(other, source, "0", 4),
	)
	noPosition := storeMarker(time.Now())
	if !recordStoreMarker(tb, processed, context.Background(), noPosition) {
		tb.Fatalf("recorded %s", noPosition.ID)
	}
	kept = append(kept, noPosition.ID)

	rangeOf := func(segment string, from, until int64) snodra.ReplayRange {
		return snodra.ReplayRange{Source: source, Segment: segment,
			From: strconv.FormatInt(from, 10), Until: strconv.FormatInt(until, 10)}
	}
	deleted := func(ranges []snodra.ReplayRange) int64 {
		tb.Helper()
		count, err := processed.DeleteMarkers(context.Background(), processor, ranges)
		if err != nil {
			tb.Fatalf("delete markers: %v", err)
		}
		return count
	}
	if got := deleted(nil); got != 0 {
		tb.Fatalf("no ranges: deleted %d markers", got)
	}
	if got := deleted([]snodra.ReplayRange{rangeOf("0", 100, 200)}); got != 0 {
		tb.Fatalf("nothing there: deleted %d markers", got)
	}
	if got := deleted([]snodra.ReplayRange{rangeOf("0", 3, 6), rangeOf("2", 7, 8)}); got != 4 {
		tb.Fatalf("deleted %d markers, want 4", got)
	}
	for _, id := range inRange {
		if existsStoreMarker(tb, processed, id) {
			tb.Fatalf("%s was not deleted", id)
		}
	}
	for _, id := range kept {
		if !existsStoreMarker(tb, processed, id) {
			tb.Fatalf("%s was deleted", id)
		}
	}
	if got := deleted([]snodra.ReplayRange{rangeOf("0", 3, 6)}); got != 0 {
		tb.Fatalf("deleted once: %d markers again", got)
	}
}

// DeadLettersSequencePerKey: letters are sequenced per (processor, key): the head is the lowest
// position that is not INVALID, an earlier letter blocks a retry, a second park of a record finds
// the first letter, and a failed retry updates the letter in place.
func DeadLettersSequencePerKey(tb testing.TB, store snodra.MessageStore) {
	deadLettersSequencePerKey(tb, store)
}

func deadLettersSequencePerKey(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	letters := store.DeadLetters()
	processor := fresh("conformance")
	otherProcessor := fresh("conformance")
	t0 := storeTime(time.Now())
	head := storeLetter(tb, processor, "0", "5", "k", snodra.DeadLetterHandlerFailed, t0, 3)
	sequenced := storeLetter(tb, processor, "0", "10", "k", snodra.DeadLetterSequenced, t0.Add(time.Millisecond), 3)
	sequenced.BlockedBy = head.ID
	sequenced.Error = snodra.SequencedDeadLetterError(head.ID)
	invalid := storeLetter(tb, processor, "0", "3", "k", snodra.DeadLetterInvalid, t0.Add(2*time.Millisecond), 3)
	invalid.Payload = []byte{0xff, 0xfe, 0x00, 0x01}
	invalid.Error = snodra.InvalidDeadLetterError("not a JSON object")
	otherKey := storeLetter(tb, processor, "1", "6", "k2", snodra.DeadLetterHandlerFailed, t0.Add(3*time.Millisecond), 3)
	noKey := storeLetter(tb, processor, "0", "9", "", snodra.DeadLetterHandlerFailed, t0.Add(4*time.Millisecond), 3)
	elsewhere := storeLetter(tb, otherProcessor, "0", "1", "k", snodra.DeadLetterHandlerFailed, t0, 3)

	park := func(letter snodra.DeadLetter) snodra.ParkResult {
		tb.Helper()
		result, err := letters.Park(context.Background(), letter)
		if err != nil {
			tb.Fatalf("park %s: %v", letter.ID, err)
		}
		return result
	}
	for _, letter := range []snodra.DeadLetter{head, sequenced, invalid, otherKey, noKey, elsewhere} {
		if result := park(letter); result != snodra.ParkNew {
			tb.Fatalf("park %s: %v", letter.ID, result)
		}
	}
	if result := park(head); result != snodra.ParkAlready {
		tb.Fatalf("a redelivery: %v", result)
	}

	headID := func(processor, key string) (string, bool) {
		tb.Helper()
		id, ok, err := letters.Head(context.Background(), processor, key)
		if err != nil {
			tb.Fatalf("head of %s/%s: %v", processor, key, err)
		}
		return id, ok
	}
	if id, ok := headID(processor, "k"); !ok || id != head.ID {
		tb.Fatalf("head: %q (%t), want %s", id, ok, head.ID)
	}
	earlier := func(position string) (string, bool) {
		tb.Helper()
		id, ok, err := letters.Earlier(context.Background(), processor, "k", position)
		if err != nil {
			tb.Fatalf("earlier than %s: %v", position, err)
		}
		return id, ok
	}
	if id, ok := earlier("7"); !ok || id != head.ID {
		tb.Fatalf("earlier than 7: %q (%t), want %s", id, ok, head.ID)
	}
	if _, ok := earlier("5"); ok {
		tb.Fatal("an INVALID letter blocked a retry")
	}
	parkedKeys := func(segments ...string) []string {
		tb.Helper()
		keys, err := letters.ParkedKeys(context.Background(), processor, "snodra.events", segments)
		if err != nil {
			tb.Fatalf("parked keys %v: %v", segments, err)
		}
		return keys
	}
	if keys := sliceSet(parkedKeys("0", "1")); len(keys) != 2 || !keys["k"] || !keys["k2"] {
		tb.Fatalf("parked keys: %v", keys)
	}
	if keys := sliceSet(parkedKeys("1")); len(keys) != 1 || !keys["k2"] {
		tb.Fatalf("parked keys of segment 1: %v", keys)
	}
	if keys := parkedKeys(); len(keys) != 0 {
		tb.Fatalf("parked keys of no segments: %v", keys)
	}

	get := func(id string) *snodra.DeadLetter {
		tb.Helper()
		letter, err := letters.Get(context.Background(), id)
		if err != nil {
			tb.Fatalf("get %s: %v", id, err)
		}
		return letter
	}
	if got := get(head.ID); got == nil || !sameStoreLetter(*got, head) {
		tb.Fatalf("get %s: %+v", head.ID, got)
	}
	if got := get(invalid.ID); got == nil || !bytes.Equal(got.Payload, invalid.Payload) {
		tb.Fatalf("an invalid payload read back: %+v", got)
	}

	list := func(processor string, limit int) []snodra.DeadLetter {
		tb.Helper()
		found, err := letters.List(context.Background(), processor, limit)
		if err != nil {
			tb.Fatalf("list %s: %v", processor, err)
		}
		return found
	}
	listed := list(processor, 0)
	if got, want := storeLetterIDs(listed), []string{head.ID, sequenced.ID, invalid.ID, otherKey.ID, noKey.ID}; !reflect.DeepEqual(got, want) {
		tb.Fatalf("list: %v, want %v", got, want)
	}
	if listed := list(processor, 2); len(listed) != 2 {
		tb.Fatalf("list with limit 2 returned %d", len(listed))
	}
	count, err := letters.Count(context.Background(), processor)
	if err != nil || count != 5 {
		tb.Fatalf("count: %d, %v", count, err)
	}
	summaries, err := letters.Summaries(context.Background(), processor)
	if err != nil {
		tb.Fatalf("summaries: %v", err)
	}
	wantSummaries := []struct {
		letter snodra.DeadLetter
		key    string
	}{
		{invalid, "k"}, {head, "k"}, {otherKey, "k2"}, {noKey, ""}, {sequenced, "k"},
	}
	if len(summaries) != len(wantSummaries) {
		tb.Fatalf("summaries: %v", summaries)
	}
	for i, want := range wantSummaries {
		summary := summaries[i]
		if summary.ID != want.letter.ID || summary.Position != want.letter.Position ||
			summary.Reason != want.letter.Reason || !summary.ParkedAt.Equal(want.letter.ParkedAt) {
			tb.Fatalf("summary %d: %+v, want %+v", i, summary, want.letter)
		}
		if want.key == "" {
			if summary.Key != nil {
				tb.Fatalf("summary %d key: %v, want none", i, *summary.Key)
			}
		} else if summary.Key == nil || *summary.Key != want.key {
			tb.Fatalf("summary %d key: %v, want %q", i, summary.Key, want.key)
		}
	}

	deleted, err := letters.Delete(context.Background(), head.ID)
	if err != nil || !deleted {
		tb.Fatalf("delete %s: %t, %v", head.ID, deleted, err)
	}
	if deleted, err := letters.Delete(context.Background(), head.ID); err != nil || deleted {
		tb.Fatalf("delete again: %t, %v", deleted, err)
	}
	if id, ok := headID(processor, "k"); !ok || id != sequenced.ID {
		tb.Fatalf("the next letter of the key: %q (%t), want %s", id, ok, sequenced.ID)
	}
	failure := snodra.DeadLetterError{Type: "RemoteHandler", Message: "failed again"}
	failed, err := letters.RecordFailure(context.Background(), sequenced.ID, failure, t0.Add(time.Minute))
	if err != nil || !failed {
		tb.Fatalf("record failure: %t, %v", failed, err)
	}
	updated := get(sequenced.ID)
	if updated == nil || updated.Attempts != sequenced.Attempts+1 ||
		updated.Reason != snodra.DeadLetterHandlerFailed || updated.Error != failure ||
		!updated.LastAttemptAt.Equal(t0.Add(time.Minute)) || updated.BlockedBy != "" {
		tb.Fatalf("the letter after a failed retry: %+v", updated)
	}
	if failed, err := letters.RecordFailure(context.Background(), head.ID, failure, t0); err != nil || failed {
		tb.Fatalf("record failure of a deleted letter: %t, %v", failed, err)
	}
	for _, letter := range []snodra.DeadLetter{sequenced, invalid, otherKey, noKey, elsewhere} {
		if deleted, err := letters.Delete(context.Background(), letter.ID); err != nil || !deleted {
			tb.Fatalf("delete %s: %t, %v", letter.ID, deleted, err)
		}
	}
	if count, err := letters.Count(context.Background(), processor); err != nil || count != 0 {
		tb.Fatalf("count after deletes: %d, %v", count, err)
	}
}

// DeadLetterParkedInsideRolledBackTransactionIsKept: a dead letter commits on its own: parked,
// or its failure recorded, while a transaction that is then rolled back is open, it stays.
func DeadLetterParkedInsideRolledBackTransactionIsKept(tb testing.TB, store snodra.MessageStore) {
	deadLetterParkedInsideRolledBackTransactionIsKept(tb, store)
}

func deadLetterParkedInsideRolledBackTransactionIsKept(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	letters := store.DeadLetters()
	processor := fresh("conformance")
	t0 := storeTime(time.Now())
	parked := storeLetter(tb, processor, "0", "1", "k1", snodra.DeadLetterHandlerFailed, t0, 3)
	txctx, tx := beginStore(tb, store)
	if result, err := letters.Park(txctx, parked); err != nil || result != snodra.ParkNew {
		tb.Fatalf("park in a transaction: %v, %v", result, err)
	}
	rollbackStore(tb, tx, txctx)
	got, err := letters.Get(context.Background(), parked.ID)
	if err != nil || got == nil || !sameStoreLetter(*got, parked) {
		tb.Fatalf("a dead letter did not commit on its own: %+v, %v", got, err)
	}
	if id, ok, err := letters.Head(context.Background(), processor, "k1"); err != nil || !ok || id != parked.ID {
		tb.Fatalf("head: %q (%t), %v", id, ok, err)
	}
	failure := snodra.DeadLetterError{Type: "Boom", Message: "failed again"}
	txctx, tx = beginStore(tb, store)
	changed, err := letters.RecordFailure(txctx, parked.ID, failure, t0.Add(time.Second))
	if err != nil || !changed {
		tb.Fatalf("record failure in a transaction: %t, %v", changed, err)
	}
	rollbackStore(tb, tx, txctx)
	got, err = letters.Get(context.Background(), parked.ID)
	if err != nil || got == nil || got.Error != failure {
		tb.Fatalf("a retry's failure did not commit on its own: %+v, %v", got, err)
	}
	if _, err := letters.Delete(context.Background(), parked.ID); err != nil {
		tb.Fatalf("delete: %v", err)
	}
}

// DeadLetterDeletedInATransactionGoesWithIt: a retry deletes its letter in the record's
// transaction: a rollback keeps the letter, a commit removes it.
func DeadLetterDeletedInATransactionGoesWithIt(tb testing.TB, store snodra.MessageStore) {
	deadLetterDeletedInATransactionGoesWithIt(tb, store)
}

func deadLetterDeletedInATransactionGoesWithIt(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	letters := store.DeadLetters()
	processor := fresh("conformance")
	parked := storeLetter(tb, processor, "0", "7", "k1", snodra.DeadLetterHandlerFailed, storeTime(time.Now()), 3)
	if result, err := letters.Park(context.Background(), parked); err != nil || result != snodra.ParkNew {
		tb.Fatalf("park: %v, %v", result, err)
	}
	txctx, tx := beginStore(tb, store)
	if deleted, err := letters.DeleteIn(txctx, parked.ID); err != nil || !deleted {
		tb.Fatalf("delete in a transaction: %t, %v", deleted, err)
	}
	rollbackStore(tb, tx, txctx)
	if got, err := letters.Get(context.Background(), parked.ID); err != nil || got == nil || !sameStoreLetter(*got, parked) {
		tb.Fatalf("a rollback kept not the letter: %+v, %v", got, err)
	}
	txctx, tx = beginStore(tb, store)
	if deleted, err := letters.DeleteIn(txctx, parked.ID); err != nil || !deleted {
		tb.Fatalf("delete in a transaction: %t, %v", deleted, err)
	}
	commitStore(tb, tx, txctx)
	if got, err := letters.Get(context.Background(), parked.ID); err != nil || got != nil {
		tb.Fatalf("a committed delete left the letter: %+v, %v", got, err)
	}
	txctx, tx = beginStore(tb, store)
	if deleted, err := letters.DeleteIn(txctx, parked.ID); err != nil || deleted {
		tb.Fatalf("delete of a letter gone: %t, %v", deleted, err)
	}
	rollbackStore(tb, tx, txctx)
}

// ControlLockIsExclusiveUntilLeaseExpires: a reset lock is held by one operation until its lease
// passes: another operation is refused with the holder, the holder renews, and once the lease has
// passed another takes it. Only the holder releases it; an expiry clears it only past the lease.
func ControlLockIsExclusiveUntilLeaseExpires(tb testing.TB, store snodra.MessageStore) {
	controlLockIsExclusiveUntilLeaseExpires(tb, store)
}

func controlLockIsExclusiveUntilLeaseExpires(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	control := store.Control()
	processor := fresh("conformance")
	t0 := storeClockTime(1_800_000_000_000)
	get := func() snodra.ProcessorControlState {
		tb.Helper()
		state, err := control.Get(context.Background(), processor)
		if err != nil {
			tb.Fatalf("get: %v", err)
		}
		return state
	}
	lock := func(operation string, leaseUntil, now time.Time) snodra.LockOutcome {
		tb.Helper()
		outcome, err := control.Lock(context.Background(), processor, operation, leaseUntil, now)
		if err != nil {
			tb.Fatalf("lock %s: %v", operation, err)
		}
		return outcome
	}
	state := get()
	if state.Processor != processor || state.Paused || state.Operation != nil || len(state.Replay) != 0 {
		tb.Fatalf("an absent processor: %+v", state)
	}
	if outcome := lock("op-1", t0.Add(5*time.Second), t0); !outcome.Taken {
		tb.Fatalf("the first lock: %+v", outcome)
	}
	held := snodra.ControlOperation{ID: "op-1", Kind: snodra.ResetKind,
		LeaseUntil: t0.Add(5 * time.Second), PreparedAt: t0}
	outcome := lock("op-2", t0.Add(6*time.Second), t0.Add(time.Second))
	if outcome.Taken || !sameStoreOperation(outcome.Holder, held) {
		tb.Fatalf("another operation while the lease runs: %+v, want %+v", outcome, held)
	}
	if outcome := lock("op-1", t0.Add(8*time.Second), t0.Add(2*time.Second)); !outcome.Taken {
		tb.Fatalf("the holder renews: %+v", outcome)
	}
	renewed := get().Operation
	if renewed == nil || !renewed.LeaseUntil.Equal(t0.Add(8*time.Second)) || !renewed.PreparedAt.Equal(t0) {
		tb.Fatalf("the renewal: %+v", renewed)
	}
	if extended, err := control.Extend(context.Background(), processor, "op-1", t0.Add(9*time.Second), t0.Add(3*time.Second)); err != nil || !extended {
		tb.Fatalf("extend the holder: %t, %v", extended, err)
	}
	if extended, err := control.Extend(context.Background(), processor, "op-2", t0.Add(9*time.Second), t0.Add(3*time.Second)); err != nil || extended {
		tb.Fatalf("extend another operation: %t, %v", extended, err)
	}
	renewedHolder := snodra.ControlOperation{ID: "op-1", Kind: snodra.ResetKind,
		LeaseUntil: t0.Add(9 * time.Second), PreparedAt: t0}
	if outcome := lock("op-2", t0.Add(10*time.Second), t0.Add(8999*time.Millisecond)); outcome.Taken ||
		outcome.Holder.ID != renewedHolder.ID || !outcome.Holder.LeaseUntil.Equal(renewedHolder.LeaseUntil) {
		tb.Fatalf("another operation before the renewed lease passed: %+v, want %+v", outcome, renewedHolder)
	}
	if expired, err := control.Expire(context.Background(), processor, "op-1", t0.Add(8999*time.Millisecond)); err != nil || expired {
		tb.Fatalf("expire before the lease passed: %t, %v", expired, err)
	}
	if outcome := lock("op-2", t0.Add(15*time.Second), t0.Add(9*time.Second)); !outcome.Taken {
		tb.Fatalf("the lease has passed: %+v", outcome)
	}
	if released, err := control.Release(context.Background(), processor, "op-1", t0.Add(9500*time.Millisecond)); err != nil || released {
		tb.Fatalf("release by another operation: %t, %v", released, err)
	}
	if released, err := control.Release(context.Background(), processor, "op-2", t0.Add(9500*time.Millisecond)); err != nil || !released {
		tb.Fatalf("release by the holder: %t, %v", released, err)
	}
	if operation := get().Operation; operation != nil {
		tb.Fatalf("the operation after the release: %+v", operation)
	}
	if outcome := lock("op-3", t0.Add(20*time.Second), t0.Add(10*time.Second)); !outcome.Taken {
		tb.Fatalf("the next lock: %+v", outcome)
	}
	if expired, err := control.Expire(context.Background(), processor, "op-3", t0.Add(20*time.Second)); err != nil || !expired {
		tb.Fatalf("expire at the lease: %t, %v", expired, err)
	}
	if operation := get().Operation; operation != nil {
		tb.Fatalf("the operation after the expiry: %+v", operation)
	}
}

// PauseStateRoundTrips: a pause round-trips with who paused and when, idempotently; a resume
// clears both; a pause leaves the reset lock alone and the lock leaves the pause alone.
func PauseStateRoundTrips(tb testing.TB, store snodra.MessageStore) {
	pauseStateRoundTrips(tb, store)
}

func pauseStateRoundTrips(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	control := store.Control()
	processor := fresh("conformance")
	t0 := storeClockTime(1_800_000_000_000)
	setPaused := func(processor string, paused bool, by string, now time.Time) {
		tb.Helper()
		if err := control.SetPaused(context.Background(), processor, paused, by, now); err != nil {
			tb.Fatalf("set paused %t: %v", paused, err)
		}
	}
	get := func(processor string) snodra.ProcessorControlState {
		tb.Helper()
		state, err := control.Get(context.Background(), processor)
		if err != nil {
			tb.Fatalf("get %s: %v", processor, err)
		}
		return state
	}
	setPaused(processor, true, "control-1", t0)
	setPaused(processor, true, "control-1", t0)
	paused := get(processor)
	if want := (snodra.ProcessorControlState{Processor: processor, Paused: true, PausedAt: t0,
		PausedBy: "control-1", UpdatedAt: t0}); !sameControlState(paused, want) {
		tb.Fatalf("the paused state: %+v, want %+v", paused, want)
	}
	if outcome, err := control.Lock(context.Background(), processor, "op-1", t0.Add(30*time.Second), t0.Add(time.Second)); err != nil || !outcome.Taken {
		tb.Fatalf("lock while paused: %+v, %v", outcome, err)
	}
	if !get(processor).Paused {
		tb.Fatal("a lock cleared the pause")
	}
	setPaused(processor, false, "control-2", t0.Add(2*time.Second))
	resumed := get(processor)
	if want := (snodra.ProcessorControlState{Processor: processor, UpdatedAt: t0.Add(2 * time.Second),
		Operation: &snodra.ControlOperation{ID: "op-1", Kind: snodra.ResetKind,
			LeaseUntil: t0.Add(30 * time.Second), PreparedAt: t0.Add(time.Second)}}); !sameControlState(resumed, want) {
		tb.Fatalf("the resumed state: %+v, want %+v", resumed, want)
	}
	freshProcessor := fresh("conformance")
	setPaused(freshProcessor, false, "local", t0)
	freshState := get(freshProcessor)
	if want := (snodra.ProcessorControlState{Processor: freshProcessor, UpdatedAt: t0}); !sameControlState(freshState, want) {
		tb.Fatalf("a resume of an unknown processor: %+v, want %+v", freshState, want)
	}
}

// ReplayRangesRoundTrip: the replay ranges round-trip in order; a removal takes exactly that
// range of that processor, once; a replacement replaces them all. The ranges leave the lock
// alone.
func ReplayRangesRoundTrip(tb testing.TB, store snodra.MessageStore) {
	replayRangesRoundTrip(tb, store)
}

func replayRangesRoundTrip(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	control := store.Control()
	processor := fresh("conformance")
	otherProcessor := fresh("conformance")
	t0 := storeClockTime(1_800_000_000_000)
	r1 := snodra.ReplayRange{Source: "snodra.events", Segment: "0", From: "10", Until: "20"}
	r2 := snodra.ReplayRange{Source: "snodra.events", Segment: "1", From: "0", Until: "5"}
	r3 := snodra.ReplayRange{Source: "conformance.other", Segment: "0", From: "10", Until: "20"}
	get := func(processor string) snodra.ProcessorControlState {
		tb.Helper()
		state, err := control.Get(context.Background(), processor)
		if err != nil {
			tb.Fatalf("get %s: %v", processor, err)
		}
		return state
	}
	setReplay := func(processor string, ranges []snodra.ReplayRange, now time.Time) {
		tb.Helper()
		if err := control.SetReplay(context.Background(), processor, ranges, now); err != nil {
			tb.Fatalf("set replay: %v", err)
		}
	}
	if outcome, err := control.Lock(context.Background(), processor, "op-1", t0.Add(30*time.Second), t0); err != nil || !outcome.Taken {
		tb.Fatalf("lock: %+v, %v", outcome, err)
	}
	setReplay(processor, []snodra.ReplayRange{r1, r2, r3}, t0.Add(time.Second))
	state := get(processor)
	locked := &snodra.ControlOperation{ID: "op-1", Kind: snodra.ResetKind,
		LeaseUntil: t0.Add(30 * time.Second), PreparedAt: t0}
	if want := (snodra.ProcessorControlState{Processor: processor, Operation: locked,
		Replay: []snodra.ReplayRange{r1, r2, r3}, UpdatedAt: t0.Add(time.Second)}); !sameControlState(state, want) {
		tb.Fatalf("the ranges: %+v, want %+v", state, want)
	}
	remove := func(processor string, replay snodra.ReplayRange, now time.Time) bool {
		tb.Helper()
		removed, err := control.RemoveRange(context.Background(), processor, replay, now)
		if err != nil {
			tb.Fatalf("remove range: %v", err)
		}
		return removed
	}
	if !remove(processor, r2, t0.Add(2*time.Second)) {
		tb.Fatal("the range was not removed")
	}
	if remove(processor, r2, t0.Add(2*time.Second)) {
		tb.Fatal("the range was removed twice")
	}
	if remove(processor, snodra.ReplayRange{Source: r1.Source, Segment: r1.Segment, From: r1.From, Until: "21"}, t0.Add(2*time.Second)) {
		tb.Fatal("a range that is not there was removed")
	}
	if remove(otherProcessor, r1, t0.Add(2*time.Second)) {
		tb.Fatal("another processor's range was removed")
	}
	if want := (snodra.ProcessorControlState{Processor: processor, Operation: locked,
		Replay: []snodra.ReplayRange{r1, r3}, UpdatedAt: t0.Add(2 * time.Second)}); !sameControlState(get(processor), want) {
		tb.Fatalf("the ranges after a removal: %+v, want %+v", get(processor), want)
	}
	setReplay(processor, []snodra.ReplayRange{r2}, t0.Add(3*time.Second))
	if want := (snodra.ProcessorControlState{Processor: processor, Operation: locked,
		Replay: []snodra.ReplayRange{r2}, UpdatedAt: t0.Add(3 * time.Second)}); !sameControlState(get(processor), want) {
		tb.Fatalf("the replacement: %+v, want %+v", get(processor), want)
	}
	setReplay(processor, nil, t0.Add(4*time.Second))
	if want := (snodra.ProcessorControlState{Processor: processor, Operation: locked,
		UpdatedAt: t0.Add(4 * time.Second)}); !sameControlState(get(processor), want) {
		tb.Fatalf("the cleared ranges: %+v, want %+v", get(processor), want)
	}
}

// ProcessorControlWriteInsideRolledBackTransactionIsKept: a processor-control write commits on
// its own: made while a transaction is open that is then rolled back, it stays.
func ProcessorControlWriteInsideRolledBackTransactionIsKept(tb testing.TB, store snodra.MessageStore) {
	processorControlWriteInsideRolledBackTransactionIsKept(tb, store)
}

func processorControlWriteInsideRolledBackTransactionIsKept(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	control := store.Control()
	paused := fresh("conformance")
	locked := fresh("conformance")
	t0 := storeClockTime(1_800_000_000_000)
	rangeOf := snodra.ReplayRange{Source: "snodra.events", Segment: "0", From: "1", Until: "2"}
	txctx, tx := beginStore(tb, store)
	if err := control.SetPaused(txctx, paused, true, "control-1", t0); err != nil {
		tb.Fatalf("pause in a transaction: %v", err)
	}
	if outcome, err := control.Lock(txctx, locked, "op-1", t0.Add(30*time.Second), t0); err != nil || !outcome.Taken {
		tb.Fatalf("lock in a transaction: %+v, %v", outcome, err)
	}
	if err := control.SetReplay(txctx, locked, []snodra.ReplayRange{rangeOf}, t0); err != nil {
		tb.Fatalf("set replay in a transaction: %v", err)
	}
	rollbackStore(tb, tx, txctx)
	if state, err := control.Get(context.Background(), paused); err != nil || !state.Paused {
		tb.Fatalf("a pause did not commit on its own: %+v, %v", state, err)
	}
	state, err := control.Get(context.Background(), locked)
	if err != nil {
		tb.Fatalf("get: %v", err)
	}
	if state.Operation == nil || state.Operation.ID != "op-1" {
		tb.Fatalf("a lock did not commit on its own: %+v", state.Operation)
	}
	if !reflect.DeepEqual(state.Replay, []snodra.ReplayRange{rangeOf}) {
		tb.Fatalf("a replay range did not commit on its own: %+v", state.Replay)
	}
}

// ---- helpers -----------------------------------------------------------------------------------

// storeTime is a time at the millisecond precision every message store keeps.
func storeTime(now time.Time) time.Time { return now.Truncate(time.Millisecond) }

// storeClockTime is a fixed instant of the control checks, from Unix milliseconds.
func storeClockTime(milliseconds int64) time.Time { return time.UnixMilli(milliseconds).UTC() }

// storeLetter is a letter of the seq per-key check: the fixed event, headers and expiry the
// Rust kit builds.
func storeLetter(tb storeTB, processor, segment, position, key string, reason snodra.DeadLetterReason, parkedAt time.Time, attempts int) snodra.DeadLetter {
	tb.Helper()
	letter := snodra.DeadLetter{
		ID:        snodra.LetterID(processor, "snodra.events", segment, position),
		Processor: processor,
		Source:    "snodra.events",
		Segment:   segment,
		Position:  position,
		EventID:   fresh("event"),
		EventType: "conformance.Happened",
		Headers:   map[string]string{"type": "conformance.Happened"},
		Payload:   []byte(`{"x":1}`),
		Reason:    reason,
		Error:     snodra.DeadLetterError{Type: "RemoteHandler", Message: "boom"},
		Attempts:  attempts,
		ParkedAt:  parkedAt,
		// TODO(spec-gap): SG-40 (the SQL layout keeps milliseconds; the kit builds the times it
		// compares at that precision, as the reference kit does).
		LastAttemptAt: parkedAt,
		ExpireAt:      parkedAt.Add(24 * time.Hour),
	}
	if key != "" {
		letter.Key = &key
	}
	if reason == snodra.DeadLetterSequenced {
		letter.BlockedBy = snodra.LetterID(processor, "snodra.events", segment, position)
	}
	return letter
}

// storeLetterIDs is the letters' ids.
func storeLetterIDs(letters []snodra.DeadLetter) []string {
	ids := make([]string, len(letters))
	for i, letter := range letters {
		ids[i] = letter.ID
	}
	return ids
}

// sameStoreLetter compares what a store must keep of a letter, times at the store's precision.
func sameStoreLetter(got, want snodra.DeadLetter) bool {
	if got.ID != want.ID || got.Processor != want.Processor || got.Source != want.Source ||
		got.Segment != want.Segment || got.Position != want.Position || got.EventID != want.EventID ||
		got.EventType != want.EventType || got.Reason != want.Reason || got.Error != want.Error ||
		got.BlockedBy != want.BlockedBy || got.Attempts != want.Attempts || got.Replay != want.Replay ||
		!got.ParkedAt.Equal(want.ParkedAt) || !got.LastAttemptAt.Equal(want.LastAttemptAt) ||
		!got.ExpireAt.Equal(want.ExpireAt) || !bytes.Equal(got.Payload, want.Payload) ||
		!reflect.DeepEqual(got.Headers, want.Headers) {
		return false
	}
	if (got.Key == nil) != (want.Key == nil) {
		return false
	}
	if got.Key != nil && *got.Key != *want.Key {
		return false
	}
	return true
}

// sameStoreOperation compares two control operations.
func sameStoreOperation(got, want snodra.ControlOperation) bool {
	return got.ID == want.ID && got.Kind == want.Kind &&
		got.LeaseUntil.Equal(want.LeaseUntil) && got.PreparedAt.Equal(want.PreparedAt)
}

// sameControlState compares two control states, times and replay ranges included.
func sameControlState(got, want snodra.ProcessorControlState) bool {
	if got.Processor != want.Processor || got.Paused != want.Paused ||
		got.PausedBy != want.PausedBy || !got.PausedAt.Equal(want.PausedAt) ||
		!got.UpdatedAt.Equal(want.UpdatedAt) || len(got.Replay) != len(want.Replay) {
		return false
	}
	for i := range got.Replay {
		if got.Replay[i] != want.Replay[i] {
			return false
		}
	}
	if (got.Operation == nil) != (want.Operation == nil) {
		return false
	}
	if got.Operation != nil && !sameStoreOperation(*got.Operation, *want.Operation) {
		return false
	}
	return true
}
