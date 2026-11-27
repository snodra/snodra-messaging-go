package snodra

import (
	"context"
	"errors"
	"testing"
	"time"
)

// letterFor is a letter of processor at source/segment/position with the given key and reason.
func letterFor(processor, source, segment, position, key string, reason DeadLetterReason) DeadLetter {
	var pointer *string
	if key != "" {
		pointer = &key
	}
	at := time.Unix(1_800_000_000, 0)
	return DeadLetter{
		ID:            LetterID(processor, source, segment, position),
		Processor:     processor,
		Source:        source,
		Segment:       segment,
		Position:      position,
		Key:           pointer,
		EventID:       "evt-" + position,
		EventType:     "orders.PlaceOrder",
		Headers:       map[string]string{HeaderID: "evt-" + position, HeaderType: "orders.PlaceOrder"},
		Payload:       []byte(`{"orderId":"o-` + position + `"}`),
		Reason:        reason,
		Error:         NewDeadLetterError("errorString", "boom"),
		Attempts:      2,
		ParkedAt:      at,
		LastAttemptAt: at,
		ExpireAt:      at.Add(30 * 24 * time.Hour),
	}
}

func TestMemoryDeadLettersParkHeadAndSequence(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	letters := store.DeadLetters()
	ctx := context.Background()
	_ = clock

	head := letterFor("orders", "snodra.events", "0", "1", "k1", DeadLetterHandlerFailed)
	if result, err := letters.Park(ctx, head); err != nil || result != ParkNew {
		t.Fatalf("Park: %v, %v", result, err)
	}
	if result, err := letters.Park(ctx, head); err != nil || result != ParkAlready {
		t.Fatalf("a redelivery: %v, %v", result, err)
	}
	follower := letterFor("orders", "snodra.events", "0", "2", "k1", DeadLetterSequenced)
	if _, err := letters.Park(ctx, follower); err != nil {
		t.Fatal(err)
	}
	invalid := letterFor("orders", "snodra.events", "0", "0", "k1", DeadLetterInvalid)
	if _, err := letters.Park(ctx, invalid); err != nil { // earlier, but INVALID never blocks
		t.Fatal(err)
	}
	other := letterFor("orders", "snodra.events", "1", "0", "k2", DeadLetterHandlerFailed)
	if _, err := letters.Park(ctx, other); err != nil {
		t.Fatal(err)
	}

	id, ok, err := letters.Head(ctx, "orders", "k1")
	if err != nil || !ok || id != head.ID {
		t.Fatalf("Head: %q, %v, %v", id, ok, err)
	}
	id, ok, err = letters.Earlier(ctx, "orders", "k1", "2")
	if err != nil || !ok || id != head.ID {
		t.Fatalf("Earlier: %q, %v, %v", id, ok, err)
	}
	if id, ok, err := letters.Earlier(ctx, "orders", "k1", "1"); err != nil || ok {
		t.Fatalf("Earlier at the head: %q, %v, %v", id, ok, err)
	}
	if _, ok, err := letters.Head(ctx, "orders", "nobody"); err != nil || ok {
		t.Fatalf("Head of an unknown key: %v, %v", ok, err)
	}

	keys, err := letters.ParkedKeys(ctx, "orders", "snodra.events", []string{"0"})
	if err != nil || len(keys) != 1 || keys[0] != "k1" {
		t.Fatalf("ParkedKeys: %v, %v", keys, err)
	}
	if keys, err := letters.ParkedKeys(ctx, "orders", "snodra.events", nil); err != nil || keys != nil {
		t.Fatalf("ParkedKeys of no segments: %v, %v", keys, err)
	}

	count, err := letters.Count(ctx, "orders")
	if err != nil || count != 4 {
		t.Fatalf("Count: %d, %v", count, err)
	}
	count, err = letters.Count(ctx, "")
	if err != nil || count != 4 {
		t.Fatalf("Count every processor: %d, %v", count, err)
	}

	// A letter the caller mutates afterwards does not change the store.
	got, err := letters.Get(ctx, follower.ID)
	if err != nil || got == nil {
		t.Fatalf("Get: %v, %v", got, err)
	}
	got.Headers[HeaderType] = "changed"
	*got.Key = "changed"
	got.Payload[0] = 'X'
	again, err := letters.Get(ctx, follower.ID)
	if err != nil || again == nil {
		t.Fatalf("Get again: %v, %v", again, err)
	}
	isStore(t, again.Headers[HeaderType], "orders.PlaceOrder")
	isStore(t, *again.Key, "k1")
	isStore(t, string(again.Payload), `{"orderId":"o-2"}`)

	// Summaries are the letters by position, then id.
	summaries, err := letters.Summaries(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{invalid.ID, other.ID, head.ID, follower.ID}
	if len(summaries) != len(want) {
		t.Fatalf("Summaries: %v", summaries)
	}
	for i, summary := range summaries {
		isStore(t, summary.ID, want[i], "summary order")
	}
}

func TestMemoryDeadLettersListLimitDeleteAndRecordFailure(t *testing.T) {
	store, _ := newMemoryStoreWithClock()
	letters := store.DeadLetters()
	ctx := context.Background()
	base := time.Unix(1_800_000_000, 0)
	first := letterFor("orders", "snodra.events", "0", "0", "k1", DeadLetterHandlerFailed)
	first.ParkedAt, first.LastAttemptAt = base, base
	second := letterFor("orders", "snodra.events", "0", "1", "k2", DeadLetterHandlerFailed)
	second.ParkedAt, second.LastAttemptAt = base.Add(time.Second), base.Add(time.Second)
	third := letterFor("mail", "snodra.events", "1", "0", "k3", DeadLetterInvalid)
	third.ParkedAt, third.LastAttemptAt = base.Add(2*time.Second), base.Add(2*time.Second)
	for _, letter := range []DeadLetter{third, second, first} {
		if _, err := letters.Park(ctx, letter); err != nil {
			t.Fatal(err)
		}
	}

	list, err := letters.List(ctx, "orders", 1)
	if err != nil || len(list) != 1 || list[0].ID != first.ID {
		t.Fatalf("List limit 1: %v, %v", list, err)
	}
	list, err = letters.List(ctx, "", 0)
	if err != nil || len(list) != 3 || list[0].ID != first.ID || list[2].ID != third.ID {
		t.Fatalf("List every processor by ParkedAt: %v, %v", list, err)
	}

	if deleted, err := letters.Delete(ctx, first.ID); err != nil || !deleted {
		t.Fatalf("Delete: %v, %v", deleted, err)
	}
	if deleted, err := letters.Delete(ctx, first.ID); err != nil || deleted {
		t.Fatalf("Delete again: %v, %v", deleted, err)
	}
	if got, err := letters.Get(ctx, first.ID); err != nil || got != nil {
		t.Fatalf("Get a deleted letter: %v, %v", got, err)
	}

	failure := NewDeadLetterError("errorString", "still broken")
	if changed, err := letters.RecordFailure(ctx, second.ID, failure, base.Add(time.Minute)); err != nil || !changed {
		t.Fatalf("RecordFailure: %v, %v", changed, err)
	}
	got, err := letters.Get(ctx, second.ID)
	if err != nil || got == nil {
		t.Fatal(err)
	}
	isStore(t, got.Attempts, 3)
	isStore(t, got.Error.Message, "still broken")
	isStore(t, got.Reason, DeadLetterHandlerFailed)
	isStore(t, got.LastAttemptAt, base.Add(time.Minute))
	if changed, err := letters.RecordFailure(ctx, "gone", failure, base); err != nil || changed {
		t.Fatalf("RecordFailure of a missing letter: %v, %v", changed, err)
	}
}

func TestMemoryDeadLetterDeletedInATransactionGoesWithIt(t *testing.T) {
	store := NewMemoryMessageStore()
	letters := store.DeadLetters()
	ctx := context.Background()
	letter := letterFor("orders", "snodra.events", "0", "0", "k1", DeadLetterHandlerFailed)

	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := letters.DeleteIn(txctx, letter.ID); err != nil || deleted {
		t.Fatalf("DeleteIn before it exists: %v, %v", deleted, err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}

	if _, err := letters.Park(ctx, letter); err != nil {
		t.Fatal(err)
	}
	txctx, tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := letters.DeleteIn(txctx, letter.ID); err != nil || !deleted {
		t.Fatalf("DeleteIn: %v, %v", deleted, err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	if got, err := letters.Get(ctx, letter.ID); err != nil || got == nil {
		t.Fatalf("a rollback kept no letter? %v, %v", got, err)
	}

	txctx, tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := letters.DeleteIn(txctx, letter.ID); err != nil || !deleted {
		t.Fatalf("DeleteIn again: %v, %v", deleted, err)
	}
	if deleted, err := letters.DeleteIn(txctx, letter.ID); err != nil || deleted {
		t.Fatalf("DeleteIn twice in one transaction: %v, %v", deleted, err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	if got, err := letters.Get(ctx, letter.ID); err != nil || got != nil {
		t.Fatalf("the committed deletion left the letter: %v, %v", got, err)
	}
}

func TestMemoryDeadLettersExpireOnTheStoresClock(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	letters := store.DeadLetters()
	ctx := context.Background()
	letter := letterFor("orders", "snodra.events", "0", "0", "k1", DeadLetterHandlerFailed)
	letter.ExpireAt = clock.Now().Add(time.Minute)
	if _, err := letters.Park(ctx, letter); err != nil {
		t.Fatal(err)
	}
	clock.Advance(59 * time.Second)
	if count, err := letters.Count(ctx, "orders"); err != nil || count != 1 {
		t.Fatalf("before the retention: %d, %v", count, err)
	}
	clock.Advance(2 * time.Second)
	if count, err := letters.Count(ctx, "orders"); err != nil || count != 0 {
		t.Fatalf("after the retention: %d, %v", count, err)
	}
	if _, ok, err := letters.Head(ctx, "orders", "k1"); err != nil || ok {
		t.Fatalf("an expired letter still blocks its key: %v, %v", ok, err)
	}
}

func TestMemoryProcessorControlPause(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	control := store.Control()
	ctx := context.Background()
	at := clock.Now()

	state, err := control.Get(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if state.Paused || state.Operation != nil || len(state.Replay) != 0 {
		t.Fatalf("a processor nobody wrote: %+v", state)
	}
	if err := control.SetPaused(ctx, "orders", true, "control-1", at); err != nil {
		t.Fatal(err)
	}
	state, err = control.Get(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !state.Paused || state.PausedBy != "control-1" || !state.PausedAt.Equal(at) {
		t.Fatalf("paused: %+v", state)
	}
	if err := control.SetPaused(ctx, "orders", false, "control-2", at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	state, err = control.Get(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if state.Paused || state.PausedBy != "" || !state.PausedAt.IsZero() {
		t.Fatalf("resumed: %+v", state)
	}
}

func TestMemoryProcessorControlLockLeaseAndReplay(t *testing.T) {
	store, clock := newMemoryStoreWithClock()
	control := store.Control()
	ctx := context.Background()
	now := clock.Now()

	outcome, err := control.Lock(ctx, "orders", "op-1", now.Add(30*time.Second), now)
	if err != nil || !outcome.Taken {
		t.Fatalf("Lock: %+v, %v", outcome, err)
	}
	outcome, err = control.Lock(ctx, "orders", "op-2", now.Add(30*time.Second), now)
	if err != nil || outcome.Taken || outcome.Holder.ID != "op-1" {
		t.Fatalf("a second operation's Lock: %+v, %v", outcome, err)
	}
	outcome, err = control.Lock(ctx, "orders", "op-1", now.Add(time.Minute), now.Add(time.Second))
	if err != nil || !outcome.Taken {
		t.Fatalf("a renewal: %+v, %v", outcome, err)
	}
	state, err := control.Get(ctx, "orders")
	if err != nil || state.Operation == nil {
		t.Fatalf("Get: %+v, %v", state, err)
	}
	isStore(t, state.Operation.Kind, ResetKind)
	isStore(t, state.Operation.LeaseUntil, now.Add(time.Minute))
	isStore(t, state.Operation.PreparedAt, now, "a renewal keeps PreparedAt")

	if extended, err := control.Extend(ctx, "orders", "op-2", now.Add(2*time.Minute), now); err != nil || extended {
		t.Fatalf("Extend another operation: %v, %v", extended, err)
	}
	if extended, err := control.Extend(ctx, "orders", "op-1", now.Add(2*time.Minute), now); err != nil || !extended {
		t.Fatalf("Extend: %v, %v", extended, err)
	}
	if expired, err := control.Expire(ctx, "orders", "op-1", now.Add(90*time.Second)); err != nil || expired {
		t.Fatalf("Expire a renewed lock: %v, %v", expired, err)
	}
	if expired, err := control.Expire(ctx, "orders", "op-1", now.Add(2*time.Minute+time.Second)); err != nil || !expired {
		t.Fatalf("Expire a passed lease: %v, %v", expired, err)
	}
	if released, err := control.Release(ctx, "orders", "op-1", now); err != nil || released {
		t.Fatalf("Release an expired lock: %v, %v", released, err)
	}

	// An expired holder does not block a new operation.
	take := now.Add(4 * time.Minute)
	if outcome, err := control.Lock(ctx, "orders", "op-3", take.Add(30*time.Second), take); err != nil || !outcome.Taken {
		t.Fatalf("Lock after the lease passed: %+v, %v", outcome, err)
	}
	if outcome, err := control.Lock(ctx, "orders", "op-3", take.Add(time.Minute), take.Add(time.Second)); err != nil || !outcome.Taken {
		t.Fatalf("Lock a renewal of op-3: %+v, %v", outcome, err)
	}
	if released, err := control.Release(ctx, "orders", "op-4", take); err != nil || released {
		t.Fatalf("Release another operation: %v, %v", released, err)
	}
	if released, err := control.Release(ctx, "orders", "op-3", take); err != nil || !released {
		t.Fatalf("Release: %v, %v", released, err)
	}

	ranges := []ReplayRange{
		{Source: "snodra.events", Segment: "0", From: "0", Until: "12"},
		{Source: "snodra.events", Segment: "1", From: "3", Until: "4"},
	}
	if err := control.SetReplay(ctx, "orders", ranges, now); err != nil {
		t.Fatal(err)
	}
	state, err = control.Get(ctx, "orders")
	if err != nil || len(state.Replay) != 2 {
		t.Fatalf("SetReplay: %+v, %v", state, err)
	}
	ranges[0].Until = "changed"
	state.Replay[1].Until = "changed"
	state, err = control.Get(ctx, "orders")
	if err != nil || state.Replay[0].Until != "12" || state.Replay[1].Until != "4" {
		t.Fatalf("the store kept the caller's ranges: %+v, %v", state, err)
	}
	if removed, err := control.RemoveRange(ctx, "orders", ranges[0], now); err != nil || removed {
		t.Fatalf("RemoveRange of a changed copy: %v, %v", removed, err)
	}
	if removed, err := control.RemoveRange(ctx, "orders", state.Replay[1], now); err != nil || !removed {
		t.Fatalf("RemoveRange: %v, %v", removed, err)
	}
	state, err = control.Get(ctx, "orders")
	if err != nil || len(state.Replay) != 1 || state.Replay[0].Segment != "0" {
		t.Fatalf("after RemoveRange: %+v, %v", state, err)
	}
	if removed, err := control.RemoveRange(ctx, "nobody", state.Replay[0], now); err != nil || removed {
		t.Fatalf("RemoveRange of an unknown processor: %v, %v", removed, err)
	}
}

func TestMemoryProcessorControlMutationAndCancelledContexts(t *testing.T) {
	store := NewMemoryMessageStore()
	control := store.Control()
	ctx := context.Background()
	at := time.Unix(1_800_000_000, 0)
	if _, err := control.Lock(ctx, "orders", "op-1", at.Add(time.Minute), at); err != nil {
		t.Fatal(err)
	}
	state, err := control.Get(ctx, "orders")
	if err != nil || state.Operation == nil {
		t.Fatal(err)
	}
	state.Operation.ID = "mutated"
	if again, err := control.Get(ctx, "orders"); err != nil || again.Operation.ID != "op-1" {
		t.Fatalf("the store kept the caller's operation: %+v, %v", again, err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := control.Get(cancelled, "orders"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get with a cancelled context: %v", err)
	}
	if err := control.SetPaused(cancelled, "orders", true, "local", at); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetPaused with a cancelled context: %v", err)
	}
	if _, err := store.DeadLetters().Count(cancelled, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("Count with a cancelled context: %v", err)
	}
}
