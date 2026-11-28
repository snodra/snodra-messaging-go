package snodratest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The level-4 message-store checks, in the Rust kit's order: the global idempotent-external-call
// record, the saga repository, dispatch, deadlines and failures, the two concurrent-write
// checks and the saga stage's markers.

// ExternalCallIdempotencyReturnsFirstResult: the first call of a key wins; a repeat finds its
// record; once it succeeded it answers with the first reply. Every write after the insert is the
// owner's only; a lapsed call is taken over or given up only when the record is still the one read.
func ExternalCallIdempotencyReturnsFirstResult(tb testing.TB, store snodra.MessageStore) {
	externalCallIdempotencyReturnsFirstResult(tb, store)
}

func externalCallIdempotencyReturnsFirstResult(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	calls := store.ExternalCalls()
	t0 := storeClockTime(1_800_000_000_000)
	key := fresh("conformance")
	first := snodra.ExternalCall{Key: key, Status: snodra.ExternalCallInProgress,
		Attempts: 1, UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "owner-1"}
	insert := func(call snodra.ExternalCall) bool {
		tb.Helper()
		inserted, err := calls.Insert(context.Background(), call)
		if err != nil {
			tb.Fatalf("insert: %v", err)
		}
		return inserted
	}
	find := func(key string) *snodra.ExternalCall {
		tb.Helper()
		call, err := calls.Find(context.Background(), key)
		if err != nil {
			tb.Fatalf("find %s: %v", key, err)
		}
		return call
	}
	if !insert(first) {
		tb.Fatal("the first insert did not insert")
	}
	if insert(snodra.ExternalCall{Key: key, Status: snodra.ExternalCallInProgress,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "owner-2"}) {
		tb.Fatal("the key was taken twice")
	}
	if got := find(key); got == nil || !sameExternalCall(*got, first) {
		tb.Fatalf("the first record: %+v", got)
	}
	succeed := func(owner, reply string, now time.Time) bool {
		tb.Helper()
		ok, err := calls.Succeed(context.Background(), key, owner, reply, now)
		if err != nil {
			tb.Fatalf("succeed: %v", err)
		}
		return ok
	}
	if succeed("owner-2", `{"n":2}`, t0.Add(time.Second)) {
		tb.Fatal("a non-owner succeeded")
	}
	if !succeed("owner-1", `{"n":1}`, t0.Add(2*time.Second)) {
		tb.Fatal("the owner did not succeed")
	}
	if succeed("owner-1", `{"n":3}`, t0.Add(3*time.Second)) {
		tb.Fatal("a settled record was written again")
	}
	if insert(first) {
		tb.Fatal("the settled key was inserted again")
	}
	settled := find(key)
	if settled == nil || settled.Status != snodra.ExternalCallSucceeded || settled.Reply != `{"n":1}` ||
		settled.Owner != "" || !settled.LeaseUntil.IsZero() || !settled.UpdatedAt.Equal(t0.Add(2*time.Second)) {
		tb.Fatalf("the settled record: %+v", settled)
	}

	lapsedKey := fresh("conformance")
	lapsed := snodra.ExternalCall{Key: lapsedKey, Status: snodra.ExternalCallInProgress,
		Attempts: 1, UpdatedAt: t0, LeaseUntil: t0.Add(10 * time.Millisecond), Owner: "owner-1"}
	if !insert(lapsed) {
		tb.Fatal("the lapsed insert did not insert")
	}
	taken, err := calls.TakeOver(context.Background(), lapsed, "owner-2", t0.Add(time.Minute), t0.Add(20*time.Millisecond))
	if err != nil || taken == nil {
		tb.Fatalf("take over: %+v, %v", taken, err)
	}
	if taken.Attempts != 2 || taken.Owner != "owner-2" || !taken.LeaseUntil.Equal(t0.Add(time.Minute)) {
		tb.Fatalf("the taken record: %+v", taken)
	}
	if again, err := calls.TakeOver(context.Background(), lapsed, "owner-3", t0.Add(time.Minute), t0.Add(21*time.Millisecond)); err != nil || again != nil {
		tb.Fatalf("a changed record was taken over: %+v, %v", again, err)
	}
	if ok, err := calls.Fail(context.Background(), lapsedKey, "owner-1", `{"code":"X"}`, t0.Add(22*time.Millisecond)); err != nil || ok {
		tb.Fatalf("the old owner failed the record: %t, %v", ok, err)
	}
	if ok, err := calls.Fail(context.Background(), lapsedKey, "owner-2", `{"code":"HANDLER_ERROR","message":"m"}`, t0.Add(23*time.Millisecond)); err != nil || !ok {
		tb.Fatalf("the owner failed the record: %t, %v", ok, err)
	}
	failed := find(lapsedKey)
	if failed == nil || failed.Status != snodra.ExternalCallFailed ||
		failed.Error != `{"code":"HANDLER_ERROR","message":"m"}` || failed.Owner != "" || !failed.LeaseUntil.IsZero() {
		tb.Fatalf("the failed record: %+v", failed)
	}

	unknownKey := fresh("conformance")
	stale := snodra.ExternalCall{Key: unknownKey, Status: snodra.ExternalCallInProgress,
		UpdatedAt: t0, LeaseUntil: t0.Add(10 * time.Millisecond), Owner: "owner-1"}
	if !insert(stale) {
		tb.Fatal("the stale insert did not insert")
	}
	changed := stale
	changed.Owner = "someone-else"
	if ok, err := calls.Expire(context.Background(), changed, t0.Add(20*time.Millisecond)); err != nil || ok {
		tb.Fatalf("a changed record was expired: %t, %v", ok, err)
	}
	if ok, err := calls.Expire(context.Background(), stale, t0.Add(20*time.Millisecond)); err != nil || !ok {
		tb.Fatalf("the record was not expired: %t, %v", ok, err)
	}
	unknown := find(unknownKey)
	if unknown == nil || unknown.Status != snodra.ExternalCallUnknown || unknown.Owner != "" || !unknown.LeaseUntil.IsZero() {
		tb.Fatalf("the unknown record: %+v", unknown)
	}

	ownedKey := fresh("conformance")
	if !insert(snodra.ExternalCall{Key: ownedKey, Status: snodra.ExternalCallInProgress,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "owner-1"}) {
		tb.Fatal("the owned insert did not insert")
	}
	if ok, err := calls.MarkUnknown(context.Background(), ownedKey, "owner-2", t0.Add(time.Second)); err != nil || ok {
		tb.Fatalf("a non-owner marked unknown: %t, %v", ok, err)
	}
	if ok, err := calls.MarkUnknown(context.Background(), ownedKey, "owner-1", t0.Add(time.Second)); err != nil || !ok {
		tb.Fatalf("the owner marked unknown: %t, %v", ok, err)
	}
	if got := find(ownedKey); got == nil || got.Status != snodra.ExternalCallUnknown {
		tb.Fatalf("the marked record: %+v", got)
	}

	resetKey := fresh("conformance")
	if !insert(snodra.ExternalCall{Key: resetKey, Status: snodra.ExternalCallInProgress,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "owner-1"}) {
		tb.Fatal("the reset insert did not insert")
	}
	if ok, err := calls.Reset(context.Background(), resetKey, "owner-2"); err != nil || ok {
		tb.Fatalf("a non-owner reset: %t, %v", ok, err)
	}
	if ok, err := calls.Reset(context.Background(), resetKey, "owner-1"); err != nil || !ok {
		tb.Fatalf("the owner reset: %t, %v", ok, err)
	}
	if got := find(resetKey); got != nil {
		tb.Fatalf("the reset record: %+v", got)
	}
	if !insert(snodra.ExternalCall{Key: resetKey, Status: snodra.ExternalCallInProgress,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "owner-2"}) {
		tb.Fatal("the key was not begun afresh")
	}
}

// SagaPortsRoundTrip: each saga port writes what it reads back in a committed transaction; an
// aborted one leaves nothing.
func SagaPortsRoundTrip(tb testing.TB, store snodra.MessageStore) {
	sagaPortsRoundTrip(tb, store)
}

func sagaPortsRoundTrip(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	sagas := store.Sagas()
	now := storeTime(time.Now())
	typeName := "conformance.Saga" + fresh("")
	startKey := typeName + "|orderId=o-1"
	saga := snodra.Saga{
		ID: fresh("saga"), Type: typeName, State: `{"step":1}`,
		Associations: []snodra.SagaAssociation{{Key: "orderId", Value: "o-1"}},
		StartKey:     &startKey, Version: 0, CreatedAt: now, UpdatedAt: now,
	}
	txctx, tx := beginStore(tb, store)
	if err := sagas.Sagas.Insert(txctx, saga); err != nil {
		tb.Fatalf("insert: %v", err)
	}
	commitStore(tb, tx, txctx)
	find := func(id string) *snodra.Saga {
		tb.Helper()
		found, err := sagas.Sagas.FindByID(context.Background(), id)
		if err != nil {
			tb.Fatalf("find %s: %v", id, err)
		}
		return found
	}
	if got := find(saga.ID); got == nil || !sameSaga(*got, saga) {
		tb.Fatalf("the inserted saga: %+v", got)
	}
	live := func(key, value string) []snodra.Saga {
		tb.Helper()
		found, err := sagas.Sagas.FindLive(context.Background(), typeName, key, value)
		if err != nil {
			tb.Fatalf("find live: %v", err)
		}
		return found
	}
	if got := live("orderId", "o-1"); len(got) != 1 || !sameSaga(got[0], saga) {
		tb.Fatalf("the live saga: %+v", got)
	}
	if got := live("orderId", "o-2"); len(got) != 0 {
		tb.Fatalf("a saga for another value: %+v", got)
	}
	updated := saga
	updated.State = `{"step":2}`
	updated.Version = 1
	txctx, tx = beginStore(tb, store)
	if ok, err := sagas.Sagas.Update(txctx, updated, 0); err != nil || !ok {
		tb.Fatalf("update: %t, %v", ok, err)
	}
	commitStore(tb, tx, txctx)
	counts, err := sagas.Sagas.CountLiveByType(context.Background())
	if err != nil || counts[typeName] != 1 {
		tb.Fatalf("live by type: %v, %v", counts, err)
	}
	txctx, tx = beginStore(tb, store)
	if ok, err := sagas.Sagas.Update(txctx, updated, 0); err != nil || ok {
		tb.Fatalf("a stale update: %t, %v", ok, err)
	}
	rollbackStore(tb, tx, txctx)
	twin := saga
	twin.ID = fresh("saga")
	txctx, tx = beginStore(tb, store)
	if err := sagas.Sagas.Insert(txctx, twin); err == nil || !errors.Is(err, snodra.ErrConflict) {
		tb.Fatalf("a twin start key: %v", err)
	}
	rollbackStore(tb, tx, txctx)
	rolledBack := saga
	rolledBack.ID = fresh("saga")
	rolledBack.StartKey = nil
	txctx, tx = beginStore(tb, store)
	if err := sagas.Sagas.Insert(txctx, rolledBack); err != nil {
		tb.Fatalf("insert rolled back: %v", err)
	}
	rollbackStore(tb, tx, txctx)
	if got := find(rolledBack.ID); got != nil {
		tb.Fatalf("a rolled-back insert: %+v", got)
	}

	routingKey := "o-1"
	dispatch := snodra.SagaDispatch{
		ID: saga.ID + ":1000", SagaID: saga.ID, Seq: 1000, Kind: snodra.DispatchCommand,
		Type: "conformance.Charge", Payload: `{"amount":42}`,
		Headers: map[string]string{"correlation-id": "c-1",
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		RoutingKey: &routingKey, CreatedAt: now,
	}
	deadline := func(name string) snodra.SagaDeadline {
		payload := `{"why":"timeout"}`
		payloadType := "conformance.Timeout"
		return snodra.SagaDeadline{ID: fresh("deadline"), SagaType: typeName, SagaID: saga.ID,
			Name: name, Payload: &payload, PayloadType: &payloadType,
			DueAt: now.Add(time.Minute), Status: snodra.DeadlineScheduled, CorrelationID: "c-1"}
	}
	fired, byID, byName, rest := deadline("fired"), deadline("by-id"), deadline("by-name"), deadline("rest")
	txctx, tx = beginStore(tb, store)
	if err := sagas.Dispatch.Record(txctx, []snodra.SagaDispatch{dispatch}); err != nil {
		tb.Fatalf("record dispatch: %v", err)
	}
	if err := sagas.Dispatch.Record(txctx, nil); err != nil {
		tb.Fatalf("record nothing: %v", err)
	}
	for _, d := range []snodra.SagaDeadline{fired, byID, byName, rest} {
		if err := sagas.Deadlines.Insert(txctx, d); err != nil {
			tb.Fatalf("insert deadline %s: %v", d.Name, err)
		}
	}
	commitStore(tb, tx, txctx)
	next, err := sagas.Dispatch.NextPending(context.Background(), saga.ID)
	if err != nil || next == nil || !sameDispatch(*next, dispatch) {
		tb.Fatalf("the recorded dispatch: %+v, %v", next, err)
	}
	later := now.Add(time.Second)
	txctx, tx = beginStore(tb, store)
	foundDeadline, err := sagas.Deadlines.FindScheduled(txctx, fired.ID)
	if err != nil || foundDeadline == nil || !sameDeadline(*foundDeadline, fired) {
		tb.Fatalf("the scheduled deadline: %+v, %v", foundDeadline, err)
	}
	if ok, err := sagas.Deadlines.MarkFired(txctx, fired.ID, later); err != nil || !ok {
		tb.Fatalf("mark fired: %t, %v", ok, err)
	}
	if ok, err := sagas.Deadlines.MarkFired(txctx, fired.ID, later); err != nil || ok {
		tb.Fatalf("mark fired again: %t, %v", ok, err)
	}
	if err := sagas.Deadlines.Cancel(txctx, saga.ID, byID.ID, later); err != nil {
		tb.Fatalf("cancel: %v", err)
	}
	if err := sagas.Deadlines.CancelByName(txctx, saga.ID, byName.Name, later); err != nil {
		tb.Fatalf("cancel by name: %v", err)
	}
	if found, err := sagas.Deadlines.FindScheduled(txctx, rest.ID); err != nil || found == nil || !sameDeadline(*found, rest) {
		tb.Fatalf("the untouched deadline: %+v, %v", found, err)
	}
	if err := sagas.Deadlines.CancelAll(txctx, saga.ID, later); err != nil {
		tb.Fatalf("cancel all: %v", err)
	}
	commitStore(tb, tx, txctx)
	for _, d := range []snodra.SagaDeadline{fired, byID, byName, rest} {
		if found, err := sagas.Deadlines.FindScheduled(context.Background(), d.ID); err != nil || found != nil {
			tb.Fatalf("%s is still scheduled: %+v, %v", d.Name, found, err)
		}
	}

	failure := snodra.SagaFailure{ID: typeName + ":" + fresh("event"), SagaType: typeName,
		EventID: fresh("event"), EventType: "conformance.Happened", Payload: `{"x":1}`,
		Headers: map[string]string{"correlationId": "c-1"}, Error: "boom", Attempts: 3,
		FirstFailedAt: now, LastFailedAt: now}
	txctx, tx = beginStore(tb, store)
	if err := sagas.Failures.Save(txctx, failure); err != nil {
		tb.Fatalf("save failure: %v", err)
	}
	commitStore(tb, tx, txctx)
	if found, err := sagas.Failures.Find(context.Background(), failure.ID); err != nil || found == nil || !sameFailure(*found, failure) {
		tb.Fatalf("the saved failure: %+v, %v", found, err)
	}
	if ok, err := sagas.Failures.FailedAgain(context.Background(), failure.ID, "boom again", 2, later); err != nil || !ok {
		tb.Fatalf("failed again: %t, %v", ok, err)
	}
	again := failure
	again.Attempts = 5
	again.Error = "boom again"
	again.LastFailedAt = later
	if found, err := sagas.Failures.Find(context.Background(), failure.ID); err != nil || found == nil || !sameFailure(*found, again) {
		tb.Fatalf("the failure after a retry: %+v, %v", found, err)
	}
	if list, err := sagas.Failures.List(context.Background(), typeName); err != nil || len(list) != 1 || !sameFailure(list[0], again) {
		tb.Fatalf("the failures: %+v, %v", list, err)
	}
	if ok, err := sagas.Failures.Delete(context.Background(), failure.ID); err != nil || !ok {
		tb.Fatalf("delete failure: %t, %v", ok, err)
	}
	if ok, err := sagas.Failures.Delete(context.Background(), failure.ID); err != nil || ok {
		tb.Fatalf("delete failure again: %t, %v", ok, err)
	}
	if ok, err := sagas.Failures.FailedAgain(context.Background(), failure.ID, "x", 1, later); err != nil || ok {
		tb.Fatalf("failed again after a delete: %t, %v", ok, err)
	}
	if list, err := sagas.Failures.List(context.Background(), typeName); err != nil || len(list) != 0 {
		tb.Fatalf("the failures after a delete: %+v, %v", list, err)
	}

	markerID := "saga:" + typeName + ":" + fresh("event")
	txctx, tx = beginStore(tb, store)
	if exists, err := sagas.Markers.Exists(txctx, markerID); err != nil || exists {
		tb.Fatalf("marker exists before: %t, %v", exists, err)
	}
	if err := sagas.Markers.Insert(txctx, markerID, "conformance.Happened", now); err != nil {
		tb.Fatalf("insert marker: %v", err)
	}
	commitStore(tb, tx, txctx)
	txctx, tx = beginStore(tb, store)
	if exists, err := sagas.Markers.Exists(txctx, markerID); err != nil || !exists {
		tb.Fatalf("marker exists after: %t, %v", exists, err)
	}
	if err := sagas.Markers.Insert(txctx, markerID, "conformance.Happened", now); err == nil || !errors.Is(err, snodra.ErrConflict) {
		tb.Fatalf("a duplicate marker: %v", err)
	}
	rollbackStore(tb, tx, txctx)

	ended := updated
	ended.Ended = true
	ended.EndedAt = later
	ended.Version = 2
	ended.UpdatedAt = later
	txctx, tx = beginStore(tb, store)
	if ok, err := sagas.Sagas.Update(txctx, ended, 1); err != nil || !ok {
		tb.Fatalf("end the saga: %t, %v", ok, err)
	}
	commitStore(tb, tx, txctx)
	if got := find(saga.ID); got == nil || !sameSaga(*got, ended) {
		tb.Fatalf("the ended saga: %+v", got)
	}
	counts, err = sagas.Sagas.CountLiveByType(context.Background())
	if err != nil {
		tb.Fatalf("live by type: %v", err)
	}
	if _, ok := counts[typeName]; ok {
		tb.Fatalf("an ended saga is live: %v", counts)
	}
	txctx, tx = beginStore(tb, store)
	if err := sagas.Dispatch.MarkDispatched(txctx, dispatch.ID, later); err != nil {
		tb.Fatalf("mark dispatched: %v", err)
	}
	commitStore(tb, tx, txctx)
}

// TwoTransactionsStartingOneSagaKeyLeaveOneLiveSaga: two live transactions each start a saga
// holding one start key and both try to commit: exactly one commits, the other conflicts, and one
// live saga holds the key.
func TwoTransactionsStartingOneSagaKeyLeaveOneLiveSaga(tb testing.TB, store snodra.MessageStore) {
	twoTransactionsStartingOneSagaKeyLeaveOneLiveSaga(tb, store)
}

func twoTransactionsStartingOneSagaKeyLeaveOneLiveSaga(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	typeName := "conformance.Race" + fresh("")
	startKey := typeName + "|orderId=o-1"
	saga := func(id string) snodra.Saga {
		return snodra.Saga{ID: id, Type: typeName, State: `{"by":"` + id + `"}`,
			Associations: []snodra.SagaAssociation{{Key: "orderId", Value: "o-1"}},
			StartKey:     &startKey, CreatedAt: storeTime(time.Now()), UpdatedAt: storeTime(time.Now())}
	}
	firstID, secondID := fresh("race"), fresh("race")
	tx1, tx1body := beginStore(tb, store)
	tx2, tx2body := beginStore(tb, store)
	// Both transactions are open, both insert, and only then does either commit: the loser must
	// conflict instead of waiting for the winner.
	inserted := []error{store.Sagas().Sagas.Insert(tx1, saga(firstID)), store.Sagas().Sagas.Insert(tx2, saga(secondID))}
	outcomes := make([]error, 2)
	for i, pair := range []struct {
		ctx context.Context
		tx  snodra.Tx
	}{{tx1, tx1body}, {tx2, tx2body}} {
		if inserted[i] != nil {
			outcomes[i] = inserted[i]
			rollbackStore(tb, pair.tx, pair.ctx)
			continue
		}
		outcomes[i] = pair.tx.Commit(pair.ctx)
		if outcomes[i] != nil {
			rollbackStore(tb, pair.tx, pair.ctx)
		}
	}
	won := -1
	for i, err := range outcomes {
		if err == nil {
			if won >= 0 {
				tb.Fatalf("both transactions committed: %v", outcomes)
			}
			won = i
			continue
		}
		if !errors.Is(err, snodra.ErrConflict) {
			tb.Fatalf("the losing transaction: %v", err)
		}
	}
	if won < 0 {
		tb.Fatalf("no transaction committed: %v", outcomes)
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), typeName, "orderId", "o-1")
	if err != nil {
		tb.Fatalf("find live: %v", err)
	}
	want := firstID
	if won == 1 {
		want = secondID
	}
	if len(live) != 1 || live[0].ID != want {
		tb.Fatalf("the live sagas: %+v, want %s", live, want)
	}
}

// TwoTransactionsUpdatingOneSagaVersionCommitOnce: two live transactions update one saga from one
// version and both try to commit: exactly one commits, the other conflicts, and the stored saga is
// the winner's.
func TwoTransactionsUpdatingOneSagaVersionCommitOnce(tb testing.TB, store snodra.MessageStore) {
	twoTransactionsUpdatingOneSagaVersionCommitOnce(tb, store)
}

func twoTransactionsUpdatingOneSagaVersionCommitOnce(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	typeName := "conformance.Race" + fresh("")
	saga := snodra.Saga{ID: fresh("race"), Type: typeName, State: `{"step":0}`,
		Associations: []snodra.SagaAssociation{{Key: "orderId", Value: "o-1"}},
		CreatedAt:    storeTime(time.Now()), UpdatedAt: storeTime(time.Now())}
	txctx, tx := beginStore(tb, store)
	if err := store.Sagas().Sagas.Insert(txctx, saga); err != nil {
		tb.Fatalf("insert: %v", err)
	}
	commitStore(tb, tx, txctx)
	next := func(step string) snodra.Saga {
		copied := saga
		copied.State = `{"step":"` + step + `"}`
		copied.Version = 1
		return copied
	}
	a, b := next("a"), next("b")
	tx1, tx1body := beginStore(tb, store)
	tx2, tx2body := beginStore(tb, store)
	type update struct {
		ctx  context.Context
		tx   snodra.Tx
		saga snodra.Saga
		ok   bool
		err  error
	}
	updates := []update{{ctx: tx1, tx: tx1body, saga: a}, {ctx: tx2, tx: tx2body, saga: b}}
	for i := range updates {
		updates[i].ok, updates[i].err = store.Sagas().Sagas.Update(updates[i].ctx, updates[i].saga, 0)
	}
	outcomes := make([]error, 2)
	for i, u := range updates {
		if u.err != nil {
			outcomes[i] = u.err
			rollbackStore(tb, u.tx, u.ctx)
			continue
		}
		if !u.ok {
			tb.Fatal("an update at the version both read was not refused by the check")
		}
		outcomes[i] = u.tx.Commit(u.ctx)
		if outcomes[i] != nil {
			rollbackStore(tb, u.tx, u.ctx)
		}
	}
	won := -1
	for i, err := range outcomes {
		if err == nil {
			if won >= 0 {
				tb.Fatalf("both transactions committed: %v", outcomes)
			}
			won = i
			continue
		}
		if !errors.Is(err, snodra.ErrConflict) {
			tb.Fatalf("the losing transaction: %v", err)
		}
	}
	if won < 0 {
		tb.Fatalf("no transaction committed: %v", outcomes)
	}
	stored, err := store.Sagas().Sagas.FindByID(context.Background(), saga.ID)
	if err != nil || stored == nil {
		tb.Fatalf("find: %+v, %v", stored, err)
	}
	want := a
	if won == 1 {
		want = b
	}
	if !sameSaga(*stored, want) {
		tb.Fatalf("the stored saga: %+v, want %+v", stored, want)
	}
}

// sameExternalCall compares two external-call records.
func sameExternalCall(got, want snodra.ExternalCall) bool {
	return got.Key == want.Key && got.Status == want.Status && got.Reply == want.Reply &&
		got.Error == want.Error && got.Attempts == want.Attempts &&
		got.UpdatedAt.Equal(want.UpdatedAt) && got.LeaseUntil.Equal(want.LeaseUntil) &&
		got.Owner == want.Owner
}

// sameSaga compares two saga instances.
func sameSaga(got, want snodra.Saga) bool {
	if got.ID != want.ID || got.Type != want.Type || got.State != want.State ||
		got.Ended != want.Ended || got.Version != want.Version ||
		!got.EndedAt.Equal(want.EndedAt) || !got.CreatedAt.Equal(want.CreatedAt) ||
		!got.UpdatedAt.Equal(want.UpdatedAt) || !reflect.DeepEqual(got.Associations, want.Associations) {
		return false
	}
	if (got.StartKey == nil) != (want.StartKey == nil) {
		return false
	}
	return got.StartKey == nil || *got.StartKey == *want.StartKey
}

// sameDispatch compares two saga-dispatch entries.
func sameDispatch(got, want snodra.SagaDispatch) bool {
	if got.ID != want.ID || got.SagaID != want.SagaID || got.Seq != want.Seq ||
		got.Kind != want.Kind || got.Type != want.Type || got.Payload != want.Payload ||
		!reflect.DeepEqual(got.Headers, want.Headers) || got.Attempts != want.Attempts ||
		got.FailureReason != want.FailureReason || got.FailureMessage != want.FailureMessage ||
		got.NoticePending != want.NoticePending || got.NoticeAttempts != want.NoticeAttempts ||
		!got.CreatedAt.Equal(want.CreatedAt) || !got.DispatchedAt.Equal(want.DispatchedAt) ||
		!got.NextAttemptAt.Equal(want.NextAttemptAt) || !got.FailedAt.Equal(want.FailedAt) ||
		!got.NoticeNextAt.Equal(want.NoticeNextAt) {
		return false
	}
	if (got.RoutingKey == nil) != (want.RoutingKey == nil) {
		return false
	}
	return got.RoutingKey == nil || *got.RoutingKey == *want.RoutingKey
}

// sameDeadline compares two deadlines.
func sameDeadline(got, want snodra.SagaDeadline) bool {
	if got.ID != want.ID || got.SagaType != want.SagaType || got.SagaID != want.SagaID ||
		got.Name != want.Name || got.Status != want.Status || got.ClaimedBy != want.ClaimedBy ||
		got.CorrelationID != want.CorrelationID || !got.DueAt.Equal(want.DueAt) ||
		!got.ClaimedUntil.Equal(want.ClaimedUntil) || !got.FinishedAt.Equal(want.FinishedAt) {
		return false
	}
	if (got.Payload == nil) != (want.Payload == nil) || (got.PayloadType == nil) != (want.PayloadType == nil) {
		return false
	}
	if got.Payload != nil && *got.Payload != *want.Payload {
		return false
	}
	return got.PayloadType == nil || *got.PayloadType == *want.PayloadType
}

// sameFailure compares two parked saga failures.
func sameFailure(got, want snodra.SagaFailure) bool {
	return got.ID == want.ID && got.SagaType == want.SagaType && got.EventID == want.EventID &&
		got.EventType == want.EventType && got.Payload == want.Payload &&
		reflect.DeepEqual(got.Headers, want.Headers) && got.Error == want.Error &&
		got.Attempts == want.Attempts && got.FirstFailedAt.Equal(want.FirstFailedAt) &&
		got.LastFailedAt.Equal(want.LastFailedAt)
}

// SagaDispatchStoreRefusesAnEventTheOutboxWouldRefuse: an EVENT entry whose payload the outbox
// would refuse is refused inside the saga's transaction, and nothing of the attempt commits. A
// COMMAND entry's payload is not the outbox's.
func SagaDispatchStoreRefusesAnEventTheOutboxWouldRefuse(tb testing.TB, store snodra.MessageStore) {
	sagaDispatchStoreRefusesAnEventTheOutboxWouldRefuse(tb, store)
}

func sagaDispatchStoreRefusesAnEventTheOutboxWouldRefuse(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	queue := store.Sagas().Dispatch
	now := storeTime(time.Now())
	refusedPayloads := []string{
		"[1]", "null", `{"n":18446744073709551615}`, `{"n":-9223372036854775809}`,
		`{"n":123456789012345678901234567890}`, `{"n":1e400}`, `{"s":"\ud800"}`,
		storeNested(snodra.MaxPayloadDepth + 1),
	}
	for _, payload := range refusedPayloads {
		sagaID := fresh("saga")
		fine := storeDispatch(tb, sagaID, 1000, snodra.DispatchEvent, `{"ok":true}`, now)
		refused := storeDispatch(tb, sagaID, 1001, snodra.DispatchEvent, payload, now)
		txctx, tx := beginStore(tb, store)
		err := queue.Record(txctx, []snodra.SagaDispatch{fine, refused})
		rollbackStore(tb, tx, txctx)
		if err == nil || !errors.Is(err, snodra.ErrInvalid) {
			tb.Fatalf("a refused event payload %q: %v", payload, err)
		}
		if next, err := queue.NextPending(context.Background(), sagaID); err != nil || next != nil {
			tb.Fatalf("%q recorded something: %+v, %v", payload, next, err)
		}
	}
	sagaID := fresh("saga")
	command := storeDispatch(tb, sagaID, 1000, snodra.DispatchCommand, `null`, now)
	txctx, tx := beginStore(tb, store)
	if err := queue.Record(txctx, []snodra.SagaDispatch{command}); err != nil {
		tb.Fatalf("a command records: %v", err)
	}
	commitStore(tb, tx, txctx)
	if err := queue.MarkDispatched(context.Background(), command.ID, now); err != nil {
		tb.Fatalf("mark dispatched: %v", err)
	}
}

// DueDeadlineIsClaimedOnceUntilItsLeasePasses: the due deadline is claimed by one scheduler at a
// time, the earliest first, until its claim passes; one not yet due, fired or cancelled never is.
func DueDeadlineIsClaimedOnceUntilItsLeasePasses(tb testing.TB, store snodra.MessageStore) {
	dueDeadlineIsClaimedOnceUntilItsLeasePasses(tb, store)
}

func dueDeadlineIsClaimedOnceUntilItsLeasePasses(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	deadlines := store.Sagas().Deadlines
	now := storeClockTime(1_800_000_000_000)
	sagaID := fresh("saga")
	firstName := "first-" + fresh("")
	secondName := "second-" + fresh("")
	deadline := func(name string, dueAt time.Time) snodra.SagaDeadline {
		return snodra.SagaDeadline{ID: fresh("deadline"), SagaType: "conformance.Deadlines",
			SagaID: sagaID, Name: name, DueAt: dueAt, Status: snodra.DeadlineScheduled}
	}
	first := deadline(firstName, now.Add(-2*time.Second))
	second := deadline(secondName, now.Add(-time.Second))
	later := deadline(firstName, now.Add(time.Minute))
	txctx, tx := beginStore(tb, store)
	for _, d := range []snodra.SagaDeadline{second, later, first} {
		if err := deadlines.Insert(txctx, d); err != nil {
			tb.Fatalf("insert deadline %s: %v", d.Name, err)
		}
	}
	commitStore(tb, tx, txctx)
	overdue := func(at time.Time) map[string]int64 {
		tb.Helper()
		counts, err := deadlines.CountOverdueByName(context.Background(), at)
		if err != nil {
			tb.Fatalf("overdue: %v", err)
		}
		return counts
	}
	if counts := overdue(now); counts[firstName] != 1 || counts[secondName] != 1 {
		tb.Fatalf("the overdue deadlines: %v", counts)
	}
	claim := func(owner string, at, until time.Time) *snodra.SagaDeadline {
		tb.Helper()
		for {
			d, err := deadlines.ClaimDue(context.Background(), owner, at, until)
			if err != nil {
				tb.Fatalf("claim due: %v", err)
			}
			if d == nil || d.SagaID == sagaID {
				return d
			}
		}
	}
	until := now.Add(30 * time.Second)
	a := claim("scheduler-a", now, until)
	if a == nil || a.ID != first.ID || a.ClaimedBy != "scheduler-a" || !a.ClaimedUntil.Equal(until) {
		tb.Fatalf("the earliest due deadline: %+v", a)
	}
	b := claim("scheduler-b", now, until)
	if b == nil || b.ID != second.ID {
		tb.Fatalf("the next due deadline: %+v", b)
	}
	if c := claim("scheduler-a", now, until); c != nil {
		tb.Fatalf("a third claim: %+v", c)
	}
	if counts := overdue(now); counts[firstName] != 1 || counts[secondName] != 1 {
		tb.Fatalf("the overdue after claims: %v", counts)
	}
	past := until.Add(time.Millisecond)
	if again := claim("scheduler-b", past, past.Add(30*time.Second)); again == nil || again.ID != first.ID {
		tb.Fatalf("a passed claim: %+v", again)
	}
	txctx, tx = beginStore(tb, store)
	if ok, err := deadlines.MarkFired(txctx, first.ID, past); err != nil || !ok {
		tb.Fatalf("mark fired: %t, %v", ok, err)
	}
	if err := deadlines.Cancel(txctx, sagaID, second.ID, past); err != nil {
		tb.Fatalf("cancel: %v", err)
	}
	commitStore(tb, tx, txctx)
	far := now.Add(time.Hour)
	if d := claim("scheduler-a", far, far.Add(30*time.Second)); d == nil || d.ID != later.ID {
		tb.Fatalf("the later deadline: %+v", d)
	}
	if counts := overdue(far); counts[firstName] != 1 || counts[secondName] != 0 {
		tb.Fatalf("the overdue after firing and cancelling: %v", counts)
	}
	txctx, tx = beginStore(tb, store)
	if err := deadlines.CancelAll(txctx, sagaID, far); err != nil {
		tb.Fatalf("cancel all: %v", err)
	}
	commitStore(tb, tx, txctx)
}

// AnEmptyDeadlineCorrelationIDReadsAsAbsent: a deadline written with an empty correlation id
// reads back without one, from find_scheduled and from claim_due.
func AnEmptyDeadlineCorrelationIDReadsAsAbsent(tb testing.TB, store snodra.MessageStore) {
	anEmptyDeadlineCorrelationIDReadsAsAbsent(tb, store)
}

func anEmptyDeadlineCorrelationIDReadsAsAbsent(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	deadlines := store.Sagas().Deadlines
	now := storeClockTime(1_800_000_000_000)
	sagaID := fresh("saga")
	deadline := snodra.SagaDeadline{ID: fresh("deadline"), SagaType: "conformance.Deadlines",
		SagaID: sagaID, Name: "empty-" + fresh(""), DueAt: now.Add(-time.Second),
		Status: snodra.DeadlineScheduled}
	txctx, tx := beginStore(tb, store)
	if err := deadlines.Insert(txctx, deadline); err != nil {
		tb.Fatalf("insert: %v", err)
	}
	commitStore(tb, tx, txctx)
	found, err := deadlines.FindScheduled(context.Background(), deadline.ID)
	if err != nil || found == nil {
		tb.Fatalf("find scheduled: %+v, %v", found, err)
	}
	if found.CorrelationID != "" {
		tb.Fatalf("find scheduled correlation id: %q", found.CorrelationID)
	}
	claimed, err := deadlines.ClaimDue(context.Background(), "scheduler-empty", now, now.Add(30*time.Second))
	if err != nil || claimed == nil {
		tb.Fatalf("claim due: %+v, %v", claimed, err)
	}
	if claimed.CorrelationID != "" {
		tb.Fatalf("claim due correlation id: %q", claimed.CorrelationID)
	}
	txctx, tx = beginStore(tb, store)
	if err := deadlines.CancelAll(txctx, sagaID, now); err != nil {
		tb.Fatalf("cancel all: %v", err)
	}
	commitStore(tb, tx, txctx)
}

// TwoSchedulersClaimEachDueDeadlineOnce: two schedulers claiming at once claim every due deadline
// exactly once, and each claims some.
func TwoSchedulersClaimEachDueDeadlineOnce(tb testing.TB, store snodra.MessageStore) {
	twoSchedulersClaimEachDueDeadlineOnce(tb, store)
}

func twoSchedulersClaimEachDueDeadlineOnce(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	const count = 40
	deadlines := store.Sagas().Deadlines
	now := storeClockTime(1_800_000_000_000)
	sagaID := fresh("saga")
	ids := make(map[string]bool, count)
	txctx, tx := beginStore(tb, store)
	for n := 0; n < count; n++ {
		d := snodra.SagaDeadline{ID: fresh("deadline"), SagaType: "conformance.Raced",
			SagaID: sagaID, Name: "raced", DueAt: now.Add(-10*time.Second + time.Duration(n)*time.Millisecond),
			Status: snodra.DeadlineScheduled}
		ids[d.ID] = true
		if err := deadlines.Insert(txctx, d); err != nil {
			tb.Fatalf("insert: %v", err)
		}
	}
	commitStore(tb, tx, txctx)
	start := make(chan struct{})
	firstClaims := make(chan struct{}, 2)
	allFirst := make(chan struct{})
	go func() {
		<-firstClaims
		<-firstClaims
		close(allFirst)
	}()
	results := make([]map[string]bool, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, owner := range []string{"scheduler-a", "scheduler-b"} {
		i, owner := i, owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed := map[string]bool{}
			first := true
			for {
				d, err := deadlines.ClaimDue(context.Background(), owner, now, now.Add(time.Minute))
				if err != nil {
					errs[i] = err
					return
				}
				if first {
					first = false
					firstClaims <- struct{}{}
					select {
					case <-allFirst:
					case <-time.After(30 * time.Second):
						errs[i] = fmt.Errorf("timed out waiting for the other scheduler's first claim")
						return
					}
				}
				if d == nil {
					results[i] = claimed
					return
				}
				if d.SagaID == sagaID {
					if claimed[d.ID] {
						errs[i] = fmt.Errorf("claimed %s twice", d.ID)
						return
					}
					claimed[d.ID] = true
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			tb.Fatal(err)
		}
	}
	union := map[string]bool{}
	for _, got := range results {
		for id := range got {
			if union[id] {
				tb.Fatalf("%s claimed by both schedulers", id)
			}
			union[id] = true
		}
	}
	if len(union) != count {
		tb.Fatalf("claimed %d deadlines, want %d", len(union), count)
	}
	if len(results[0]) == 0 || len(results[1]) == 0 {
		tb.Fatalf("one scheduler claimed nothing: %d and %d", len(results[0]), len(results[1]))
	}
	txctx, tx = beginStore(tb, store)
	if err := deadlines.CancelAll(txctx, sagaID, now); err != nil {
		tb.Fatalf("cancel all: %v", err)
	}
	commitStore(tb, tx, txctx)
}

// DispatchHandsOutEachSagaInSeqOrder: a saga's pending entries come in seq order, the sagas whose
// first pending entry is due oldest first, a saga's lease is held by one relay until it passes,
// an entry dispatched in a rolled-back transaction stays pending, a retried entry waits for its
// next attempt, and a failed one carries its outcome and a notice claimed once until cleared.
func DispatchHandsOutEachSagaInSeqOrder(tb testing.TB, store snodra.MessageStore) {
	dispatchHandsOutEachSagaInSeqOrder(tb, store)
}

func dispatchHandsOutEachSagaInSeqOrder(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	queue := store.Sagas().Dispatch
	now := storeClockTime(1_800_000_000_000)
	before, err := queue.Pending(context.Background())
	if err != nil {
		tb.Fatalf("pending before: %v", err)
	}
	older, newer := fresh("saga"), fresh("saga")
	routingKey := "o-1"
	command := storeDispatch(tb, newer, 1000, snodra.DispatchCommand, `{"amount":42}`, now.Add(-time.Second))
	command.RoutingKey = &routingKey
	event := storeDispatch(tb, newer, 1001, snodra.DispatchEvent, `{"amount":42}`, now.Add(-time.Second))
	old := storeDispatch(tb, older, 0, snodra.DispatchEvent, `{"amount":42}`, now.Add(-5*time.Second))
	txctx, tx := beginStore(tb, store)
	if err := queue.Record(txctx, []snodra.SagaDispatch{event, command}); err != nil {
		tb.Fatalf("record: %v", err)
	}
	if err := queue.Record(txctx, []snodra.SagaDispatch{old}); err != nil {
		tb.Fatalf("record old: %v", err)
	}
	commitStore(tb, tx, txctx)
	if got, err := queue.Pending(context.Background()); err != nil || got != before+3 {
		tb.Fatalf("pending: %d, %v, want %d", got, err, before+3)
	}
	due, err := queue.DueSagas(context.Background(), now, nil, 10000)
	if err != nil {
		tb.Fatalf("due sagas: %v", err)
	}
	at := func(id string) int {
		for i, sagaID := range due {
			if sagaID == id {
				return i
			}
		}
		return -1
	}
	if at(older) < 0 || at(newer) < 0 || at(older) >= at(newer) {
		tb.Fatalf("the due sagas: %v", due)
	}
	excluded, err := queue.DueSagas(context.Background(), now, []string{older}, 10000)
	if err != nil {
		tb.Fatalf("due sagas excluded: %v", err)
	}
	for _, id := range excluded {
		if id == older {
			tb.Fatalf("an in-flight saga is due: %v", excluded)
		}
	}
	if next, err := queue.NextPending(context.Background(), newer); err != nil || next == nil || !sameDispatch(*next, command) {
		tb.Fatalf("next pending: %+v, %v", next, err)
	}

	leaseUntil := now.Add(time.Minute)
	claimLease := func(sagaID, owner string, at, until time.Time) bool {
		tb.Helper()
		ok, err := queue.ClaimLease(context.Background(), sagaID, owner, at, until)
		if err != nil {
			tb.Fatalf("claim lease: %v", err)
		}
		return ok
	}
	if !claimLease(newer, "relay-a", now, leaseUntil) {
		tb.Fatal("relay-a did not claim the lease")
	}
	if claimLease(newer, "relay-b", now, leaseUntil) {
		tb.Fatal("relay-b claimed a held lease")
	}
	if renewed, err := queue.RenewLease(context.Background(), newer, "relay-b", leaseUntil); err != nil || renewed {
		tb.Fatalf("relay-b renewed: %t, %v", renewed, err)
	}
	if renewed, err := queue.RenewLease(context.Background(), newer, "relay-a", now.Add(2*time.Minute)); err != nil || !renewed {
		tb.Fatalf("relay-a renewed: %t, %v", renewed, err)
	}
	if claimLease(newer, "relay-b", now.Add(61*time.Second), now.Add(121*time.Second)) {
		tb.Fatal("a renewed lease was claimed")
	}
	if err := queue.ReleaseLease(context.Background(), newer, "relay-b"); err != nil {
		tb.Fatalf("relay-b released: %v", err)
	}
	if claimLease(newer, "relay-b", now, leaseUntil) {
		tb.Fatal("a non-holder's release freed the lease")
	}
	if err := queue.ReleaseLease(context.Background(), newer, "relay-a"); err != nil {
		tb.Fatalf("relay-a released: %v", err)
	}
	if !claimLease(newer, "relay-b", now, leaseUntil) {
		tb.Fatal("a released lease is free")
	}
	if err := queue.ReleaseLease(context.Background(), newer, "relay-b"); err != nil {
		tb.Fatalf("release: %v", err)
	}
	if !claimLease(older, "relay-a", now, now.Add(time.Second)) {
		tb.Fatal("the old saga's lease")
	}
	if !claimLease(older, "relay-b", now.Add(2*time.Second), now.Add(5*time.Second)) {
		tb.Fatal("a passed lease is free")
	}
	if err := queue.ReleaseLease(context.Background(), older, "relay-b"); err != nil {
		tb.Fatalf("release: %v", err)
	}

	txctx, tx = beginStore(tb, store)
	if err := queue.MarkDispatched(txctx, command.ID, now); err != nil {
		tb.Fatalf("mark dispatched: %v", err)
	}
	rollbackStore(tb, tx, txctx)
	if next, err := queue.NextPending(context.Background(), newer); err != nil || next == nil || !sameDispatch(*next, command) {
		tb.Fatalf("a rolled-back dispatch: %+v, %v", next, err)
	}
	txctx, tx = beginStore(tb, store)
	if err := queue.MarkDispatched(txctx, command.ID, now); err != nil {
		tb.Fatalf("mark dispatched: %v", err)
	}
	commitStore(tb, tx, txctx)
	if next, err := queue.NextPending(context.Background(), newer); err != nil || next == nil || !sameDispatch(*next, event) {
		tb.Fatalf("the next entry: %+v, %v", next, err)
	}
	if err := queue.ScheduleRetry(context.Background(), event.ID, 1, now.Add(time.Minute)); err != nil {
		tb.Fatalf("schedule retry: %v", err)
	}
	retried := event
	retried.Attempts = 1
	retried.NextAttemptAt = now.Add(time.Minute)
	if next, err := queue.NextPending(context.Background(), newer); err != nil || next == nil || !sameDispatch(*next, retried) {
		tb.Fatalf("the retried entry: %+v, %v", next, err)
	}
	due, err = queue.DueSagas(context.Background(), now, nil, 10000)
	if err != nil {
		tb.Fatalf("due sagas: %v", err)
	}
	for _, id := range due {
		if id == newer {
			tb.Fatal("a retried saga is due before its next attempt")
		}
	}
	due, err = queue.DueSagas(context.Background(), now.Add(time.Minute), nil, 10000)
	if err != nil {
		tb.Fatalf("due sagas: %v", err)
	}
	found := false
	for _, id := range due {
		found = found || id == newer
	}
	if !found {
		tb.Fatal("a retried saga is not due at its next attempt")
	}

	attempts := 2
	noticeAt := now.Add(30 * time.Second)
	if err := queue.MarkFailed(context.Background(), event.ID, snodra.DispatchFailure{
		Attempts: &attempts, Reason: snodra.FailureExhausted, Message: "gave up",
		FailedAt: now, NoticeAt: &noticeAt}); err != nil {
		tb.Fatalf("mark failed: %v", err)
	}
	if next, err := queue.NextPending(context.Background(), newer); err != nil || next != nil {
		tb.Fatalf("a failed entry is pending: %+v, %v", next, err)
	}
	claimNotice := func(at, reserve time.Time, id string) *snodra.SagaDispatch {
		tb.Helper()
		for {
			entry, err := queue.ClaimDueNotice(context.Background(), at, reserve)
			if err != nil {
				tb.Fatalf("claim notice: %v", err)
			}
			if entry == nil || entry.ID == id {
				return entry
			}
		}
	}
	if entry := claimNotice(now, now.Add(90*time.Second), event.ID); entry != nil {
		tb.Fatalf("a notice before its time: %+v", entry)
	}
	failed := event
	failed.Attempts = 2
	failed.NextAttemptAt = now.Add(time.Minute)
	failed.FailedAt = now
	failed.FailureReason = snodra.FailureExhausted
	failed.FailureMessage = "gave up"
	failed.NoticePending = true
	failed.NoticeAttempts = 0
	failed.NoticeNextAt = now.Add(90 * time.Second)
	if entry := claimNotice(now.Add(30*time.Second), now.Add(90*time.Second), event.ID); entry == nil || !sameDispatch(*entry, failed) {
		tb.Fatalf("the claimed notice: %+v, want %+v", entry, failed)
	}
	if entry := claimNotice(now.Add(30*time.Second), now.Add(90*time.Second), event.ID); entry != nil {
		tb.Fatalf("a reserved notice was claimed again: %+v", entry)
	}
	if err := queue.ScheduleNoticeRetry(context.Background(), event.ID, 1, now.Add(100*time.Second)); err != nil {
		tb.Fatalf("schedule notice retry: %v", err)
	}
	retriedNotice := failed
	retriedNotice.NoticeAttempts = 1
	retriedNotice.NoticeNextAt = now.Add(160 * time.Second)
	if entry := claimNotice(now.Add(100*time.Second), now.Add(160*time.Second), event.ID); entry == nil || !sameDispatch(*entry, retriedNotice) {
		tb.Fatalf("the retried notice: %+v, want %+v", entry, retriedNotice)
	}
	if err := queue.ClearNotice(context.Background(), event.ID); err != nil {
		tb.Fatalf("clear notice: %v", err)
	}
	if entry := claimNotice(now.Add(time.Hour), now.Add(time.Hour+time.Minute), event.ID); entry != nil {
		tb.Fatalf("a cleared notice was claimed: %+v", entry)
	}
	if err := queue.MarkFailed(context.Background(), old.ID, snodra.DispatchFailure{
		Reason: snodra.FailureInvalid, Message: "not a JSON object", FailedAt: now}); err != nil {
		tb.Fatalf("mark an event failed: %v", err)
	}
	if next, err := queue.NextPending(context.Background(), older); err != nil || next != nil {
		tb.Fatalf("a failed event is pending: %+v, %v", next, err)
	}
	if entry := claimNotice(now.Add(time.Hour), now.Add(time.Hour+time.Minute), old.ID); entry != nil {
		tb.Fatalf("an event has a notice: %+v", entry)
	}
	if got, err := queue.Pending(context.Background()); err != nil || got != before {
		tb.Fatalf("pending after: %d, %v, want %d", got, err, before)
	}
}

// TwoRelaysNeverHoldOneSagaLease: two relays claiming the same sagas' leases at once: each
// saga's lease goes to exactly one of them.
func TwoRelaysNeverHoldOneSagaLease(tb testing.TB, store snodra.MessageStore) {
	twoRelaysNeverHoldOneSagaLease(tb, store)
}

func twoRelaysNeverHoldOneSagaLease(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	const count = 40
	queue := store.Sagas().Dispatch
	now := storeClockTime(1_800_000_000_000)
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fresh("saga")
	}
	start := make(chan struct{})
	results := make([]map[string]bool, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, owner := range []string{"relay-a", "relay-b"} {
		i, owner := i, owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			held := map[string]bool{}
			for _, id := range ids {
				ok, err := queue.ClaimLease(context.Background(), id, owner, now, now.Add(time.Minute))
				if err != nil {
					errs[i] = err
					return
				}
				if ok {
					held[id] = true
				}
			}
			results[i] = held
		}()
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			tb.Fatal(err)
		}
	}
	for id := range results[0] {
		if results[1][id] {
			tb.Fatalf("%s held by both relays", id)
		}
	}
	if len(results[0])+len(results[1]) != count {
		tb.Fatalf("leases held: %d + %d, want %d", len(results[0]), len(results[1]), count)
	}
	for _, id := range ids {
		owner := "relay-b"
		if results[0][id] {
			owner = "relay-a"
		}
		if err := queue.ReleaseLease(context.Background(), id, owner); err != nil {
			tb.Fatalf("release lease: %v", err)
		}
	}
}

// ConcurrentNoticeClaimsReturnEachNoticeOnce: two relays claiming due failure notices at once,
// each until none is left, return every notice exactly once.
func ConcurrentNoticeClaimsReturnEachNoticeOnce(tb testing.TB, store snodra.MessageStore) {
	concurrentNoticeClaimsReturnEachNoticeOnce(tb, store)
}

func concurrentNoticeClaimsReturnEachNoticeOnce(tb storeTB, store snodra.MessageStore) {
	tb.Helper()
	const count = 40
	queue := store.Sagas().Dispatch
	now := storeClockTime(1_800_000_000_000)
	entries := make([]snodra.SagaDispatch, count)
	for i := range entries {
		entries[i] = storeDispatch(tb, fresh("saga"), 1000, snodra.DispatchCommand, `{"ok":true}`, now.Add(-time.Second))
	}
	txctx, tx := beginStore(tb, store)
	if err := queue.Record(txctx, entries); err != nil {
		tb.Fatalf("record: %v", err)
	}
	commitStore(tb, tx, txctx)
	attempts := 1
	for _, entry := range entries {
		if err := queue.MarkFailed(context.Background(), entry.ID, snodra.DispatchFailure{
			Attempts: &attempts, Reason: snodra.FailureNoHandler, Message: "nobody",
			FailedAt: now, NoticeAt: &now}); err != nil {
			tb.Fatalf("mark failed: %v", err)
		}
	}
	mine := make(map[string]bool, count)
	for _, entry := range entries {
		mine[entry.ID] = true
	}
	start := make(chan struct{})
	results := make([]map[string]bool, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range results {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed := map[string]bool{}
			for {
				entry, err := queue.ClaimDueNotice(context.Background(), now, now.Add(time.Minute))
				if err != nil {
					errs[i] = err
					return
				}
				if entry == nil {
					results[i] = claimed
					return
				}
				if mine[entry.ID] {
					if claimed[entry.ID] {
						errs[i] = fmt.Errorf("claimed %s twice", entry.ID)
						return
					}
					claimed[entry.ID] = true
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			tb.Fatal(err)
		}
	}
	total := 0
	for _, got := range results {
		for id := range got {
			if results[0][id] && results[1][id] {
				tb.Fatalf("%s claimed by both relays", id)
			}
			if !mine[id] {
				tb.Fatalf("another check's notice was counted: %s", id)
			}
			total++
		}
	}
	if total != count {
		tb.Fatalf("claimed %d notices, want %d", total, count)
	}
	for id := range mine {
		if err := queue.ClearNotice(context.Background(), id); err != nil {
			tb.Fatalf("clear notice: %v", err)
		}
	}
}

// storeDispatch is one pending dispatch entry of the kit's checks.
func storeDispatch(tb storeTB, sagaID string, seq int64, kind snodra.DispatchKind, payload string, createdAt time.Time) snodra.SagaDispatch {
	tb.Helper()
	return snodra.SagaDispatch{ID: sagaID + ":" + fmt.Sprint(seq), SagaID: sagaID, Seq: seq,
		Kind: kind, Type: "conformance.Dispatched", Payload: payload,
		Headers: map[string]string{"correlation-id": "c-1"}, CreatedAt: createdAt}
}
