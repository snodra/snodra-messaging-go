package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

func TestTheObservedProcessedPortTellsHandledFromDuplicate(t *testing.T) {
	ctx := context.Background()
	inner := snodra.NewMemoryMessageStore()
	led := newLedger()
	store := newObservedStore(inner, familyPostgres, led)
	marker := snodra.ProcessedMarker{
		ID:        markerID(familyPostgres, "event-1"),
		Type:      eventType,
		Processor: processorName(familyPostgres),
		Source:    sourceName("run42", familyPostgres),
		Segment:   "0",
		Position:  "7",
	}
	recorded, err := store.Processed().Record(ctx, marker)
	if err != nil || !recorded {
		t.Fatalf("the first record = %v, %v", recorded, err)
	}
	recorded, err = store.Processed().Record(ctx, marker)
	if err != nil || recorded {
		t.Fatalf("the duplicate record = %v, %v", recorded, err)
	}
	led.observeHandled(OrderPlaced{ID: "event-1", Run: "run42", Publisher: "go", Family: familyPostgres, Key: "k-1", Seq: 3}, nil)
	deliveries, handled, duplicates := led.snapshot()
	if handled != 1 || duplicates != 1 {
		t.Fatalf("handled, duplicates = %d, %d", handled, duplicates)
	}
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %+v", deliveries)
	}
	if deliveries[0].Outcome != "handled" || deliveries[0].Publisher != "go" || deliveries[0].Key != "k-1" {
		t.Fatalf("the handled delivery = %+v", deliveries[0])
	}
	if deliveries[1].Outcome != "duplicate" || deliveries[1].Segment != "0" || deliveries[1].Position != "7" {
		t.Fatalf("the duplicate delivery = %+v", deliveries[1])
	}
}

func TestTheLedgerShowsOnlyCommittedTransactions(t *testing.T) {
	ctx := context.Background()
	inner := snodra.NewMemoryMessageStore()
	led := newLedger()
	store := newObservedStore(inner, familyPostgres, led)
	marker := snodra.ProcessedMarker{
		ID:        markerID(familyPostgres, "event-1"),
		Type:      eventType,
		Processor: processorName(familyPostgres),
		Source:    sourceName("run42", familyPostgres),
		Segment:   "0",
		Position:  "7",
	}
	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	recorded, err := store.Processed().Record(txctx, marker)
	if err != nil || !recorded {
		t.Fatalf("the record = %v, %v", recorded, err)
	}
	led.observeHandled(OrderPlaced{ID: "event-1", Run: "run42", Publisher: "go", Family: familyPostgres, Key: "k-1", Seq: 3}, txRecordFrom(txctx))
	if deliveries, handled, duplicates := led.snapshot(); len(deliveries) != 0 || handled != 0 || duplicates != 0 {
		t.Fatalf("an uncommitted transaction was visible: %+v", deliveries)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	deliveries, handled, duplicates := led.snapshot()
	if handled != 1 || duplicates != 0 || len(deliveries) != 1 {
		t.Fatalf("after the commit: %+v, %d, %d", deliveries, handled, duplicates)
	}
	if deliveries[0].Publisher != "go" || deliveries[0].Key != "k-1" {
		t.Fatalf("the committed delivery = %+v", deliveries[0])
	}

	marker.ID = markerID(familyPostgres, "event-2")
	txctx, tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if recorded, err = store.Processed().Record(txctx, marker); err != nil || !recorded {
		t.Fatalf("the rolled-back record = %v, %v", recorded, err)
	}
	led.observeHandled(OrderPlaced{ID: "event-2", Run: "run42", Publisher: "go", Family: familyPostgres, Key: "k-2", Seq: 4}, txRecordFrom(txctx))
	if err := tx.Rollback(txctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if deliveries, handled, duplicates := led.snapshot(); len(deliveries) != 1 || handled != 1 || duplicates != 0 {
		t.Fatalf("a rolled-back transaction was visible: %+v, %d, %d", deliveries, handled, duplicates)
	}
}

func TestAMarkerOfAnotherProcessorIsNotObserved(t *testing.T) {
	led := newLedger()
	led.observeMarker(familyMongo, snodra.ProcessedMarker{ID: "processor:other:event-1"}, true, nil)
	if deliveries, handled, duplicates := led.snapshot(); len(deliveries) != 0 || handled != 0 || duplicates != 0 {
		t.Fatalf("another processor's marker was observed: %+v", deliveries)
	}
}

func TestPublishAppendsToTheOutboxAndTheRelayCanClaimIt(t *testing.T) {
	values := baseEnv()
	values["INTEROP_FAMILIES"] = "kafka"
	s, err := settingsFromEnv(env(values))
	if err != nil {
		t.Fatal(err)
	}
	p, err := newProbe(s, familyKafka, slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError})))
	if err != nil {
		t.Fatal(err)
	}
	defer p.close(slog.Default())
	ctx := context.Background()
	reply, err := p.publish(ctx, Publish{
		Family:    familyKafka,
		Publisher: "go",
		Run:       "run42",
		Count:     2,
		Keys:      []string{"k-0", "k-1"},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(reply.IDs) != 2 || reply.Instance != "go-kafka" {
		t.Fatalf("reply = %+v", reply)
	}
	claimed, err := p.store.Outbox().Claim(ctx, "test-relay", time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d entries, want 2", len(claimed))
	}
	if claimed[0].Type != eventType || claimed[0].Key == nil || *claimed[0].Key != "k-0" {
		t.Fatalf("entry = %+v", claimed[0])
	}
	var payload OrderPlaced
	if err := json.Unmarshal(claimed[0].Payload, &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if payload.ID != reply.IDs[0] || payload.Publisher != "go" || payload.Run != "run42" || payload.Family != familyKafka {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestPublishRefusesAnotherFamilysCommand(t *testing.T) {
	values := baseEnv()
	values["INTEROP_FAMILIES"] = "kafka"
	s, err := settingsFromEnv(env(values))
	if err != nil {
		t.Fatal(err)
	}
	p, err := newProbe(s, familyKafka, slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError})))
	if err != nil {
		t.Fatal(err)
	}
	defer p.close(slog.Default())
	if _, err := p.publish(context.Background(), Publish{
		Family: familyPostgres, Publisher: "go", Run: "run42", Count: 1, Keys: []string{"k"},
	}); err == nil {
		t.Fatal("a command for another family was accepted")
	}
	if _, err := p.publish(context.Background(), Publish{
		Family: familyKafka, Publisher: "go", Run: "another-run", Count: 1, Keys: []string{"k"},
	}); err == nil {
		t.Fatal("a command for another run was accepted")
	}
}

// testWriter sends a logger's output into the test's log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
