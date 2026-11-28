package snodra

import (
	"context"
	"errors"
	"testing"
	"time"
)

// L4-RACE: two live transactions each saw a start key free and inserted a live saga with it;
// the commit of the second must conflict, and one live saga remains.
func TestMemoryStoreRefusesOneStartKeyAcrossTwoTransactions(t *testing.T) {
	store := NewMemoryMessageStore()
	startKey := "conformance.Saga|orderId=o-1"
	saga := func(id string) Saga {
		now := time.Now().Truncate(time.Millisecond)
		return Saga{ID: id, Type: "conformance.Saga", State: `{"step":1}`,
			Associations: []SagaAssociation{{Key: "orderId", Value: "o-1"}},
			StartKey:     &startKey, CreatedAt: now, UpdatedAt: now}
	}
	firstCtx, first, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, second, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Sagas().Sagas.Insert(firstCtx, saga("first")); err != nil {
		t.Fatalf("the first insert: %v", err)
	}
	if err := store.Sagas().Sagas.Insert(secondCtx, saga("second")); err != nil {
		t.Fatalf("the second insert: %v", err)
	}
	outcomes := []error{first.Commit(firstCtx), second.Commit(secondCtx)}
	one, conflicted := 0, 0
	for _, err := range outcomes {
		switch {
		case err == nil:
			one++
		case errors.Is(err, ErrConflict):
			conflicted++
		default:
			t.Fatalf("a commit: %v", err)
		}
	}
	if one != 1 || conflicted != 1 {
		t.Fatalf("the commits: %v, want one commit and one conflict", outcomes)
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), "conformance.Saga", "orderId", "o-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 {
		t.Fatalf("the live sagas: %+v", live)
	}
}

// L4-START-KEY: an update cannot give a live saga a start key another live saga holds.
func TestMemoryStoreRefusesAnUpdateToATakenStartKey(t *testing.T) {
	store := NewMemoryMessageStore()
	now := time.Now().Truncate(time.Millisecond)
	key := func(value string) string { return "conformance.Saga|orderId=" + value }
	firstKey, secondKey := key("o-1"), key("o-2")
	first := Saga{ID: "first", Type: "conformance.Saga", State: `{}`, StartKey: &firstKey, CreatedAt: now, UpdatedAt: now}
	second := Saga{ID: "second", Type: "conformance.Saga", State: `{}`, StartKey: &secondKey, CreatedAt: now, UpdatedAt: now}
	for _, saga := range []Saga{first, second} {
		txctx, tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Sagas().Sagas.Insert(txctx, saga); err != nil {
			t.Fatalf("insert %s: %v", saga.ID, err)
		}
		if err := tx.Commit(txctx); err != nil {
			t.Fatalf("commit %s: %v", saga.ID, err)
		}
	}
	adopting := second
	adopting.StartKey = &firstKey
	adopting.Version = 1
	txctx, tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sagas().Sagas.Update(txctx, adopting, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("the update to a taken start key: %v, want ErrConflict", err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Sagas().Sagas.FindByID(context.Background(), second.ID)
	if err != nil || stored == nil || stored.StartKey == nil || *stored.StartKey != secondKey {
		t.Fatalf("the stored saga: %+v, %v", stored, err)
	}
}
