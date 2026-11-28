package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// ExternalCharge is the command of the idempotent adapter tests.
type ExternalCharge struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
	Mode   string `json:"mode,omitempty"`
}

func (ExternalCharge) MessageType() string { return "orders.Charge" }

// ExternalReceipt is the adapter's reply.
type ExternalReceipt struct {
	ID     string `json:"id"`
	Ticket int    `json:"ticket"`
}

func (ExternalReceipt) MessageType() string { return "orders.ChargeReceipt" }

func externalTestApp(t *testing.T) (*App, *MemoryMessageStore) {
	t.Helper()
	store := NewMemoryMessageStore()
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), OutboxEnabled(false))
	return app, store
}

func externalKey(key string) SendOption { return Header("idempotency-key", key) }

func TestExternalCallIdempotencyReturnsFirstResult(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}); err != nil {
		t.Fatal(err)
	}
	first, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Ticket != 1 {
		t.Fatalf("the first (%+v) and second (%+v) replies", first, second)
	}
	if bodies.Load() != 1 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil {
		t.Fatalf("the record: %+v, %v", record, err)
	}
	if record.Status != ExternalCallSucceeded || record.Attempts != 1 || record.Owner != "" || !record.LeaseUntil.IsZero() {
		t.Fatalf("the settled record: %+v", record)
	}
	third, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-2"))
	if err != nil || third.Ticket != 2 {
		t.Fatalf("another key: %+v, %v", third, err)
	}
}

func TestTheFirstExternalCallWins(t *testing.T) {
	app, store := externalTestApp(t)
	t0 := time.Now()
	app.now = func() time.Time { return t0 }
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		n := bodies.Add(1)
		if n == 1 {
			started <- struct{}{}
			<-release
		}
		return ExternalReceipt{ID: c.ID, Ticket: int(n)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan struct {
		receipt ExternalReceipt
		err     error
	}, 1)
	go func() {
		receipt, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
		firstDone <- struct {
			receipt ExternalReceipt
			err     error
		}{receipt, err}
	}()
	<-started
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallInProgress ||
		record.Attempts != 1 || record.Owner == "" || !record.LeaseUntil.Equal(t0.Add(5*time.Minute)) {
		t.Fatalf("the in-progress record: %+v, %v", record, err)
	}
	_, secondErr := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsRetryable(secondErr) {
		t.Fatalf("the call in flight: %v, want retryable", secondErr)
	}
	var second *Error
	if !errors.As(secondErr, &second) || string(second.Details) != `{"snodra":"retryable"}` {
		t.Fatalf("the retryable marker: %v", secondErr)
	}
	close(release)
	first := <-firstDone
	if first.err != nil || first.receipt.Ticket != 1 {
		t.Fatalf("the first call: %+v, %v", first.receipt, first.err)
	}
	if bodies.Load() != 1 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
	record, err = store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallSucceeded {
		t.Fatalf("the record: %+v, %v", record, err)
	}
}

func TestAFailedExternalCallReplaysTheStoredError(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		return ExternalReceipt{}, errors.New("card declined")
	}); err != nil {
		t.Fatal(err)
	}
	first, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err == nil {
		t.Fatalf("the failed call answered %+v", first)
	}
	var firstError *Error
	if !errors.As(err, &firstError) || firstError.Code != CodeHandlerError || firstError.Message != "card declined" {
		t.Fatalf("the first error: %v", err)
	}
	_, err = Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err == nil || !strings.Contains(err.Error(), "card declined") {
		t.Fatalf("the replayed error: %v", err)
	}
	if bodies.Load() != 1 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallFailed {
		t.Fatalf("the failed record: %+v, %v", record, err)
	}
	if record.Error != `{"code":"HANDLER_ERROR","message":"card declined"}` {
		t.Fatalf("the stored error: %q", record.Error)
	}
}

func TestARetryableExternalCallRunsAgain(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		if bodies.Add(1) == 1 {
			return ExternalReceipt{}, Retryable(errors.New("partner busy"))
		}
		return ExternalReceipt{ID: c.ID, Ticket: 7}, nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsRetryable(err) {
		t.Fatalf("the retryable failure: %v", err)
	}
	var protocol *Error
	if !errors.As(err, &protocol) || string(protocol.Details) != `{"snodra":"retryable"}` {
		t.Fatalf("the retryable marker: %v", err)
	}
	if record, err := store.ExternalCalls().Find(context.Background(), "job-1"); err != nil || record != nil {
		t.Fatalf("the record after the retryable failure: %+v, %v", record, err)
	}
	second, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil || second.Ticket != 7 || bodies.Load() != 2 {
		t.Fatalf("the retried call: %+v, %v, bodies %d", second, err, bodies.Load())
	}
}

func TestAnUnknownExternalOutcomeStaysUnknown(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		return ExternalReceipt{}, UnknownOutcome(errors.New("connection reset after the request"))
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsUnknownOutcome(err) {
		t.Fatalf("the unknown outcome: %v", err)
	}
	var protocol *Error
	if !errors.As(err, &protocol) || string(protocol.Details) != `{"snodra":"unknown-outcome"}` {
		t.Fatalf("the unknown-outcome marker: %v", err)
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallUnknown {
		t.Fatalf("the unknown record: %+v, %v", record, err)
	}
	_, err = Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsUnknownOutcome(err) || bodies.Load() != 1 {
		t.Fatalf("the repeated unknown outcome: %v, bodies %d", err, bodies.Load())
	}
}

func TestALapsedCallIsGivenUpAsUnknown(t *testing.T) {
	app, store := externalTestApp(t)
	t0 := time.Now()
	if _, err := store.ExternalCalls().Insert(context.Background(), ExternalCall{
		Key: "job-1", Status: ExternalCallInProgress, Attempts: 1,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "old-call",
	}); err != nil {
		t.Fatal(err)
	}
	app.now = func() time.Time { return t0.Add(2 * time.Minute) }
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsUnknownOutcome(err) {
		t.Fatalf("the lapsed call: %v", err)
	}
	if bodies.Load() != 0 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallUnknown {
		t.Fatalf("the given-up record: %+v, %v", record, err)
	}
}

func TestALapsedCallIsTakenOverWhenThePartnerIsIdempotent(t *testing.T) {
	app, store := externalTestApp(t)
	t0 := time.Now()
	if _, err := store.ExternalCalls().Insert(context.Background(), ExternalCall{
		Key: "job-1", Status: ExternalCallInProgress, Attempts: 1,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "old-call",
	}); err != nil {
		t.Fatal(err)
	}
	app.now = func() time.Time { return t0.Add(2 * time.Minute) }
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}, ExternalIdempotent()); err != nil {
		t.Fatal(err)
	}
	receipt, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil || receipt.Ticket != 1 || bodies.Load() != 1 {
		t.Fatalf("the taken-over call: %+v, %v, bodies %d", receipt, err, bodies.Load())
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallSucceeded || record.Attempts != 2 {
		t.Fatalf("the taken-over record: %+v, %v", record, err)
	}
}

func TestAMissingIdempotencyKeyIsAHandlerError(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"})
	if err == nil || !strings.Contains(err.Error(), "idempotency-key") {
		t.Fatalf("the missing key: %v", err)
	}
	if bodies.Load() != 0 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
	if record, err := store.ExternalCalls().Find(context.Background(), "job-1"); err != nil || record != nil {
		t.Fatalf("a record without a key: %+v, %v", record, err)
	}
}

// The record's writes are their own, so the caller's rollback does not forget the external call.
func TestAnExternalCallRecordSurvivesTheCallersRollback(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}); err != nil {
		t.Fatal(err)
	}
	txctx, tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first, err := Send[ExternalReceipt](txctx, app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallSucceeded {
		t.Fatalf("the record after the rollback: %+v, %v", record, err)
	}
	second, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil || second != first || bodies.Load() != 1 {
		t.Fatalf("the replay after the rollback: %+v, %v, bodies %d", second, err, bodies.Load())
	}
}

// A stale read never expires a record that another call settled meanwhile: it is read again and
// its reply is returned.
type racingExternalCalls struct {
	ExternalCallStore
	once  sync.Once
	base  ExternalCallStore
	key   string
	reply string
	now   time.Time
}

func (r *racingExternalCalls) Find(ctx context.Context, key string) (*ExternalCall, error) {
	call, err := r.ExternalCallStore.Find(ctx, key)
	if err == nil && call != nil {
		r.once.Do(func() {
			_, _ = r.base.Succeed(ctx, r.key, call.Owner, r.reply, r.now)
		})
	}
	return call, err
}

type racingExternalStore struct {
	*MemoryMessageStore
	calls ExternalCallStore
}

func (s *racingExternalStore) ExternalCalls() ExternalCallStore { return s.calls }

func TestALapsedCallIsNotGivenUpOverAChangedRecord(t *testing.T) {
	base := NewMemoryMessageStore()
	t0 := time.Now()
	if _, err := base.ExternalCalls().Insert(context.Background(), ExternalCall{
		Key: "job-1", Status: ExternalCallInProgress, Attempts: 1,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "old-call",
	}); err != nil {
		t.Fatal(err)
	}
	racing := &racingExternalCalls{ExternalCallStore: base.ExternalCalls(), base: base.ExternalCalls(),
		key: "job-1", reply: `{"id":"c-1","ticket":42}`, now: t0.Add(2 * time.Minute)}
	store := &racingExternalStore{MemoryMessageStore: base, calls: racing}
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), OutboxEnabled(false))
	app.now = func() time.Time { return t0.Add(2 * time.Minute) }
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}); err != nil {
		t.Fatal(err)
	}
	receipt, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil || receipt.Ticket != 42 {
		t.Fatalf("the changed record's reply: %+v, %v", receipt, err)
	}
	if bodies.Load() != 0 {
		t.Fatalf("the body ran %d times over a changed record", bodies.Load())
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallSucceeded {
		t.Fatalf("the changed record: %+v, %v", record, err)
	}
}

func TestALapsedCallIsNotTakenOverOverAChangedRecord(t *testing.T) {
	base := NewMemoryMessageStore()
	t0 := time.Now()
	if _, err := base.ExternalCalls().Insert(context.Background(), ExternalCall{
		Key: "job-1", Status: ExternalCallInProgress, Attempts: 1,
		UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "old-call",
	}); err != nil {
		t.Fatal(err)
	}
	racing := &racingExternalCalls{ExternalCallStore: base.ExternalCalls(), base: base.ExternalCalls(),
		key: "job-1", reply: `{"id":"c-1","ticket":42}`, now: t0.Add(2 * time.Minute)}
	store := &racingExternalStore{MemoryMessageStore: base, calls: racing}
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), OutboxEnabled(false))
	app.now = func() time.Time { return t0.Add(2 * time.Minute) }
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}, ExternalIdempotent()); err != nil {
		t.Fatal(err)
	}
	receipt, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil || receipt.Ticket != 42 {
		t.Fatalf("the changed record's reply: %+v, %v", receipt, err)
	}
	if bodies.Load() != 0 {
		t.Fatalf("the body ran %d times over a changed record", bodies.Load())
	}
}

func TestAnIdempotentCommandNeedsAMessageStore(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"), OutboxEnabled(false))
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	err := app.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "idempotent command handler for orders.Charge") ||
		!strings.Contains(err.Error(), "message store") {
		t.Fatalf("Start without a store: %v", err)
	}
}

type noExternalCallsStore struct{ *MemoryMessageStore }

func (s *noExternalCallsStore) ExternalCalls() ExternalCallStore { return nil }

type unsupportedExternalCalls struct{}

func (unsupportedExternalCalls) Insert(context.Context, ExternalCall) (bool, error) {
	return false, ErrUnsupported
}
func (unsupportedExternalCalls) Find(context.Context, string) (*ExternalCall, error) {
	return nil, ErrUnsupported
}
func (unsupportedExternalCalls) TakeOver(context.Context, ExternalCall, string, time.Time, time.Time) (*ExternalCall, error) {
	return nil, ErrUnsupported
}
func (unsupportedExternalCalls) Expire(context.Context, ExternalCall, time.Time) (bool, error) {
	return false, ErrUnsupported
}
func (unsupportedExternalCalls) Succeed(context.Context, string, string, string, time.Time) (bool, error) {
	return false, ErrUnsupported
}
func (unsupportedExternalCalls) Fail(context.Context, string, string, string, time.Time) (bool, error) {
	return false, ErrUnsupported
}
func (unsupportedExternalCalls) MarkUnknown(context.Context, string, string, time.Time) (bool, error) {
	return false, ErrUnsupported
}
func (unsupportedExternalCalls) Reset(context.Context, string, string) (bool, error) {
	return false, ErrUnsupported
}

type unsupportedExternalStore struct {
	*MemoryMessageStore
}

func (s *unsupportedExternalStore) ExternalCalls() ExternalCallStore {
	return unsupportedExternalCalls{}
}

func TestAnIdempotentCommandProbesTheExternalCallPort(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(&noExternalCallsStore{MemoryMessageStore: NewMemoryMessageStore()}),
		OutboxEnabled(false))
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	err := app.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "external calls") ||
		!strings.Contains(err.Error(), "does not provide") {
		t.Fatalf("Start without external calls: %v", err)
	}
	unmarked := newApp(t, Hubs("127.0.0.1:1"), WithStore(&unsupportedExternalStore{MemoryMessageStore: NewMemoryMessageStore()}),
		OutboxEnabled(false))
	if err := HandleIdempotentCommand(unmarked, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	err = unmarked.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "external calls") || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Start with an unsupported external-call port: %v", err)
	}
}

func TestAnIdempotentOnlyServiceStartsWithoutAnEventStore(t *testing.T) {
	store := NewMemoryMessageStore()
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), OutboxEnabled(false))
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start without an event store: %v", err)
	}
	receipt, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if err != nil || receipt.Ticket != 1 {
		t.Fatalf("the idempotent command after Start: %+v, %v", receipt, err)
	}
}

func TestL4AdapterMarkers(t *testing.T) {
	t.Run("in-process", func(t *testing.T) {
		app, _ := externalTestApp(t)
		if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
			if c.Mode == "retryable" {
				return ExternalReceipt{}, Retryable(errors.New("partner busy"))
			}
			return ExternalReceipt{}, UnknownOutcome(errors.New("connection reset"))
		}); err != nil {
			t.Fatal(err)
		}
		_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1", Mode: "retryable"}, externalKey("marker-r"))
		if !IsRetryable(err) {
			t.Fatalf("the retryable marker: %v", err)
		}
		var protocol *Error
		if !errors.As(err, &protocol) || protocol.Code != CodeHandlerError || string(protocol.Details) != `{"snodra":"retryable"}` {
			t.Fatalf("the retryable answer: %v", err)
		}
		_, err = Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1", Mode: "unknown"}, externalKey("marker-u"))
		if !IsUnknownOutcome(err) {
			t.Fatalf("the unknown-outcome marker: %v", err)
		}
		if !errors.As(err, &protocol) || protocol.Code != CodeHandlerError || string(protocol.Details) != `{"snodra":"unknown-outcome"}` {
			t.Fatalf("the unknown-outcome answer: %v", err)
		}
	})

	t.Run("written through the hub", func(t *testing.T) {
		hub := fakehub.Start(t)
		store := NewMemoryMessageStore()
		_, s := serving(t, hub, func(app *App) {
			if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
				if c.Mode == "retryable" {
					return ExternalReceipt{}, Retryable(errors.New("partner busy"))
				}
				return ExternalReceipt{}, UnknownOutcome(errors.New("connection reset"))
			}); err != nil {
				t.Fatal(err)
			}
		}, WithStore(store), OutboxEnabled(false))
		for i, wanted := range []string{`{"snodra":"retryable"}`, `{"snodra":"unknown-outcome"}`} {
			mode := "retryable"
			if i == 1 {
				mode = "unknown"
			}
			s.Send(t, fakehub.Forward(&hubpb.Request{Id: fmt.Sprintf("r-%d", i), Kind: hubpb.RequestKind_COMMAND,
				Type: "orders.Charge", Payload: []byte(`{"id":"c-1","mode":"` + mode + `"}`),
				Headers: map[string]string{"idempotency-key": fmt.Sprintf("hub-%d", i)}}))
			reply := nextReply(t, s)
			if reply.GetError().GetCode() != hubpb.ErrorCode_HANDLER_ERROR ||
				string(reply.GetError().GetDetails()) != wanted {
				t.Fatalf("%s through the hub: %v", mode, reply)
			}
		}
	})

	t.Run("read from the hub", func(t *testing.T) {
		hub := fakehub.Start(t)
		app, s := serving(t, hub, func(*App) {})
		for i, marker := range []string{`{"snodra":"retryable"}`, `{"snodra":"unknown-outcome"}`} {
			done := make(chan error, 1)
			go func() {
				_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey(fmt.Sprintf("read-%d", i)))
				done <- err
			}()
			req := s.RecvRequest(t)
			s.Send(t, failWith(req.GetId(), hubpb.ErrorCode_HANDLER_ERROR, "busy", marker))
			err := receive(t, done)
			var protocol *Error
			if !errors.As(err, &protocol) || protocol.Code != CodeHandlerError || string(protocol.Details) != marker {
				t.Fatalf("%s from the hub: %v", marker, err)
			}
			if marker == `{"snodra":"retryable"}` && !IsRetryable(err) {
				t.Fatalf("IsRetryable: %v", err)
			}
			if marker == `{"snodra":"unknown-outcome"}` && !IsUnknownOutcome(err) {
				t.Fatalf("IsUnknownOutcome: %v", err)
			}
		}
	})
}

// losingSucceedCalls makes the SUCCEEDED write fail, as a lost lease or a store outage would.
type losingSucceedCalls struct{ ExternalCallStore }

func (losingSucceedCalls) Succeed(context.Context, string, string, string, time.Time) (bool, error) {
	return false, nil
}

type losingSucceedStore struct {
	*MemoryMessageStore
	calls ExternalCallStore
}

func (s *losingSucceedStore) ExternalCalls() ExternalCallStore { return s.calls }

func losingSucceedApp(t *testing.T, partner bool) (*App, *MemoryMessageStore, *losingSucceedStore) {
	t.Helper()
	base := NewMemoryMessageStore()
	store := &losingSucceedStore{MemoryMessageStore: base, calls: losingSucceedCalls{ExternalCallStore: base.ExternalCalls()}}
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), OutboxEnabled(false))
	var bodies atomic.Int64
	opts := []HandlerOption{}
	if partner {
		opts = append(opts, ExternalIdempotent())
	}
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}, opts...); err != nil {
		t.Fatal(err)
	}
	app.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	return app, base, store
}

// A body that ran but whose record cannot say so is an unknown outcome, never a success.
func TestAnUnrecordedExternalSuccessIsUnknown(t *testing.T) {
	app, base, _ := losingSucceedApp(t, false)
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsUnknownOutcome(err) {
		t.Fatalf("the unrecorded success: %v", err)
	}
	record, err := base.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallUnknown {
		t.Fatalf("the record after the unrecorded success: %+v, %v", record, err)
	}
}

// With a de-duplicating partner, the same case is retryable and leaves the record for its lease.
func TestAnUnrecordedExternalSuccessIsRetryableForADeDuplicatingPartner(t *testing.T) {
	app, base, _ := losingSucceedApp(t, true)
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsRetryable(err) {
		t.Fatalf("the unrecorded success with a de-duplicating partner: %v", err)
	}
	record, err := base.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallInProgress {
		t.Fatalf("the record after the retryable unrecorded success: %+v, %v", record, err)
	}
}

func TestExternalIdempotentAppliesToIdempotentCommandsOnly(t *testing.T) {
	app, _ := externalTestApp(t)
	err := HandleCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{}, nil
	}, ExternalIdempotent())
	if err == nil || !strings.Contains(err.Error(), "HandleIdempotentCommand") {
		t.Fatalf("ExternalIdempotent on a plain command: %v", err)
	}
}

// The relay compares the markers with surrounding whitespace trimmed; the helpers read them the
// same way.
func TestExternalMarkersAreReadWithWhitespaceTrimmed(t *testing.T) {
	retryable := &Error{Code: CodeHandlerError, Details: []byte(" {\"snodra\":\"retryable\"}\n")}
	unknown := &Error{Code: CodeHandlerError, Details: []byte("\t{\"snodra\":\"unknown-outcome\"} ")}
	if !IsRetryable(retryable) || IsUnknownOutcome(retryable) {
		t.Fatalf("the padded retryable marker: %v", retryable)
	}
	if !IsUnknownOutcome(unknown) || IsRetryable(unknown) {
		t.Fatalf("the padded unknown-outcome marker: %v", unknown)
	}
	if IsRetryable(errors.New("retryable")) || IsUnknownOutcome(errors.New("unknown-outcome")) {
		t.Fatal("a plain error carried a marker")
	}
}

// markFailDispatch loses the first mark-dispatched write, as a crash between the send and the
// mark would.
type markFailDispatch struct {
	SagaDispatchStore
	once atomic.Bool
}

func (d *markFailDispatch) MarkDispatched(ctx context.Context, id string, now time.Time) error {
	if d.once.CompareAndSwap(false, true) {
		return errors.New("the mark was lost")
	}
	return d.SagaDispatchStore.MarkDispatched(ctx, id, now)
}

type markFailStore struct {
	*MemoryMessageStore
	sagas SagaStores
}

func (s *markFailStore) Sagas() SagaStores { return s.sagas }

// L4-SAGA-DISPATCH/L4-ADAPTER-MARKERS: a re-send after a lost mark uses the same idempotency key,
// so the saga's adapter body runs once.
func TestASagaCommandResentAfterALostMarkRunsTheAdapterOnce(t *testing.T) {
	base := NewMemoryMessageStore()
	store := &markFailStore{MemoryMessageStore: base}
	store.sagas = base.Sagas()
	store.sagas.Dispatch = &markFailDispatch{SagaDispatchStore: store.sagas.Dispatch}
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), OutboxEnabled(false))
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{ID: c.ID, Ticket: int(bodies.Add(1))}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(ExternalCharge{ID: event.OrderID}, "")
			}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	live, err := store.Sagas().Sagas.FindLive(context.Background(), "orders", "orderId", "o-1")
	if err != nil || len(live) != 1 {
		t.Fatalf("the saga: %+v, %v", live, err)
	}
	entry, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID)
	if err != nil || entry == nil {
		t.Fatalf("the dispatch entry: %+v, %v", entry, err)
	}
	relay := newSagaRelay(app)
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bodies.Load() != 1 {
		t.Fatalf("the adapter ran %d times before the lost mark", bodies.Load())
	}
	if pending, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID); err != nil || pending == nil {
		t.Fatalf("the entry was marked dispatched despite the lost write: %+v, %v", pending, err)
	}
	if err := relay.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bodies.Load() != 1 {
		t.Fatalf("the adapter ran %d times after the re-send", bodies.Load())
	}
	record, err := store.ExternalCalls().Find(context.Background(), entry.ID)
	if err != nil || record == nil || record.Status != ExternalCallSucceeded {
		t.Fatalf("the external call under the saga key %q: %+v, %v", entry.ID, record, err)
	}
	if pending, err := store.Sagas().Dispatch.NextPending(context.Background(), live[0].ID); err != nil || pending != nil {
		t.Fatalf("the entry after the re-send: %+v, %v", pending, err)
	}
}

// A panic in the adapter body is settled as UNKNOWN, not left IN_PROGRESS and not called a known
// failure: the external system may have acted before the panic.
func TestAnAdapterPanicIsAnUnknownOutcome(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		panic("adapter exploded")
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
	if !IsUnknownOutcome(err) {
		t.Fatalf("the adapter panic: %v, want the unknown-outcome marker", err)
	}
	var protocol *Error
	if !errors.As(err, &protocol) || protocol.Code != CodeHandlerError ||
		string(protocol.Details) != `{"snodra":"unknown-outcome"}` {
		t.Fatalf("the panic's answer: %v", err)
	}
	record, err := store.ExternalCalls().Find(context.Background(), "job-1")
	if err != nil || record == nil || record.Status != ExternalCallUnknown {
		t.Fatalf("the panic's record: %+v, %v", record, err)
	}
	if bodies.Load() != 1 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
}

// A marker nested in another error is still written and read, and still settles the record.
func TestNestedExternalMarkers(t *testing.T) {
	app, store := externalTestApp(t)
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(_ context.Context, c ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		if c.Mode == "retryable" {
			return ExternalReceipt{}, fmt.Errorf("outer: %w", Retryable(errors.New("busy")))
		}
		return ExternalReceipt{}, fmt.Errorf("outer: %w", UnknownOutcome(errors.New("connection reset")))
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1", Mode: "retryable"}, externalKey("nested-r"))
	if !IsRetryable(err) {
		t.Fatalf("the nested retryable marker: %v", err)
	}
	var protocol *Error
	if !errors.As(err, &protocol) || string(protocol.Details) != `{"snodra":"retryable"}` {
		t.Fatalf("the nested retryable answer: %v", err)
	}
	if record, err := store.ExternalCalls().Find(context.Background(), "nested-r"); err != nil || record != nil {
		t.Fatalf("the nested retryable record: %+v, %v", record, err)
	}
	_, err = Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1", Mode: "unknown"}, externalKey("nested-u"))
	if !IsUnknownOutcome(err) {
		t.Fatalf("the nested unknown marker: %v", err)
	}
	if !errors.As(err, &protocol) || string(protocol.Details) != `{"snodra":"unknown-outcome"}` {
		t.Fatalf("the nested unknown answer: %v", err)
	}
	record, err := store.ExternalCalls().Find(context.Background(), "nested-u")
	if err != nil || record == nil || record.Status != ExternalCallUnknown {
		t.Fatalf("the nested unknown record: %+v, %v", record, err)
	}
	if bodies.Load() != 2 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
}

// failingExternalCalls makes one ExternalCallStore operation answer an error.
type failingExternalCalls struct {
	ExternalCallStore
	op  string
	err error
}

func (f *failingExternalCalls) Insert(ctx context.Context, call ExternalCall) (bool, error) {
	if f.op == "insert" {
		return false, f.err
	}
	return f.ExternalCallStore.Insert(ctx, call)
}

func (f *failingExternalCalls) Find(ctx context.Context, key string) (*ExternalCall, error) {
	if f.op == "find" {
		return nil, f.err
	}
	return f.ExternalCallStore.Find(ctx, key)
}

func (f *failingExternalCalls) TakeOver(ctx context.Context, seen ExternalCall, owner string, leaseUntil, now time.Time) (*ExternalCall, error) {
	if f.op == "takeover" {
		return nil, f.err
	}
	return f.ExternalCallStore.TakeOver(ctx, seen, owner, leaseUntil, now)
}

func (f *failingExternalCalls) Expire(ctx context.Context, seen ExternalCall, now time.Time) (bool, error) {
	if f.op == "expire" {
		return false, f.err
	}
	return f.ExternalCallStore.Expire(ctx, seen, now)
}

type externalFailureStore struct {
	*MemoryMessageStore
	calls ExternalCallStore
}

func (s *externalFailureStore) ExternalCalls() ExternalCallStore { return s.calls }

// A store failure before the body is a plain HANDLER_ERROR; no record is settled and the body
// never runs.
func TestStoreFailuresBeforeTheBodyArePermanentHandlerErrors(t *testing.T) {
	t0 := time.Now()
	for _, c := range []struct {
		name   string
		op     string
		seed   bool
		lapsed bool
	}{
		{"insert", "insert", false, false},
		{"find", "find", true, false},
		{"takeover", "takeover", true, true},
		{"expire", "expire", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := NewMemoryMessageStore()
			failing := &failingExternalCalls{ExternalCallStore: base.ExternalCalls(), op: c.op, err: errors.New("store unavailable")}
			store := &externalFailureStore{MemoryMessageStore: base, calls: failing}
			app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), OutboxEnabled(false))
			app.now = func() time.Time { return t0.Add(2 * time.Minute) }
			if c.seed {
				if _, err := base.ExternalCalls().Insert(context.Background(), ExternalCall{
					Key: "job-1", Status: ExternalCallInProgress, Attempts: 1,
					UpdatedAt: t0, LeaseUntil: t0.Add(time.Minute), Owner: "old-call",
				}); err != nil {
					t.Fatal(err)
				}
			}
			var bodies atomic.Int64
			opts := []HandlerOption{}
			if c.op == "takeover" {
				opts = append(opts, ExternalIdempotent())
			}
			if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
				bodies.Add(1)
				return ExternalReceipt{}, nil
			}, opts...); err != nil {
				t.Fatal(err)
			}
			_, err := Send[ExternalReceipt](context.Background(), app, ExternalCharge{ID: "c-1"}, externalKey("job-1"))
			var protocol *Error
			if !errors.As(err, &protocol) || protocol.Code != CodeHandlerError || protocol.Message != "store unavailable" {
				t.Fatalf("the %s failure: %v", c.op, err)
			}
			if len(protocol.Details) != 0 || IsRetryable(err) || IsUnknownOutcome(err) {
				t.Fatalf("the %s failure carries a marker: %v", c.op, err)
			}
			if bodies.Load() != 0 {
				t.Fatalf("the body ran %d times", bodies.Load())
			}
		})
	}
}

// The same store failure on a saga command is the permanent FAILED outcome, not a retry.
func TestAStoreFailureBeforeTheBodyIsAPermanentSagaFailure(t *testing.T) {
	base := NewMemoryMessageStore()
	failing := &failingExternalCalls{ExternalCallStore: base.ExternalCalls(), op: "insert", err: errors.New("store unavailable")}
	store := &externalFailureStore{MemoryMessageStore: base, calls: failing}
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(store), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), OutboxEnabled(false))
	var bodies atomic.Int64
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		bodies.Add(1)
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	var failed SagaCommandFailed
	if err := HandleSaga[sagaState](app, "orders", func(d *SagaDefinition[sagaState]) {
		d.StartOn(StartedOn(Associate("orderId", func(e OrderPlaced) string { return e.OrderID }),
			func(_ context.Context, saga *SagaContext, _ *sagaState, event OrderPlaced) error {
				return saga.Send(ExternalCharge{ID: event.OrderID}, "")
			}))
		d.OnCommandFailed(CommandFailed(func(_ context.Context, _ *SagaContext, _ *sagaState, event SagaCommandFailed) error {
			failed = event
			return nil
		}))
	}); err != nil {
		t.Fatal(err)
	}
	app.sagaRuntime = newSagaRuntime(app)
	if _, err := app.sagaRuntime.process(context.Background(), sagaMessage{id: "e-1", typeName: "orders.OrderPlaced"},
		[]byte(`{"orderId":"o-1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := newSagaRelay(app).poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if failed.Reason != SagaCommandFailedFailed {
		t.Fatalf("the saga's failed notice: %+v", failed)
	}
	if bodies.Load() != 0 {
		t.Fatalf("the body ran %d times", bodies.Load())
	}
}

// A typed nil ExternalCalls value is not a supplied port: Start refuses it by reflection rather
// than panicking on the probe.
type typedNilExternalCalls struct{ ExternalCallStore }

type typedNilExternalStore struct{ *MemoryMessageStore }

func (s *typedNilExternalStore) ExternalCalls() ExternalCallStore {
	var calls *typedNilExternalCalls
	return calls
}

func TestATypedNilExternalCallsPortIsRefusedAtStartup(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"), WithStore(&typedNilExternalStore{MemoryMessageStore: NewMemoryMessageStore()}),
		OutboxEnabled(false))
	if err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
		return ExternalReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	err := app.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "external calls") || !strings.Contains(err.Error(), "does not provide") {
		t.Fatalf("the typed nil external-calls port: %v", err)
	}
}

func TestIdempotentCommandRegistration(t *testing.T) {
	t.Run("nil handler", func(t *testing.T) {
		app := newApp(t, Hubs("127.0.0.1:1"))
		err := HandleIdempotentCommand[ExternalCharge, ExternalReceipt](app, nil)
		if err == nil || !strings.Contains(err.Error(), "nil handler") {
			t.Fatalf("the nil handler: %v", err)
		}
	})
	t.Run("after Start", func(t *testing.T) {
		app := newApp(t, Hubs("127.0.0.1:1"), WithStore(NewMemoryMessageStore()), OutboxEnabled(false))
		if err := app.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
			return ExternalReceipt{}, nil
		})
		if !errors.Is(err, ErrRegistrationClosed) {
			t.Fatalf("registration after Start: %v", err)
		}
	})
	t.Run("duplicate type", func(t *testing.T) {
		app := newApp(t, Hubs("127.0.0.1:1"))
		h := func(context.Context, ExternalCharge) (ExternalReceipt, error) { return ExternalReceipt{}, nil }
		if err := HandleIdempotentCommand(app, h); err != nil {
			t.Fatal(err)
		}
		if err := HandleIdempotentCommand(app, h); err == nil || !strings.Contains(err.Error(), "already registered") {
			t.Fatalf("the duplicate: %v", err)
		}
	})
	t.Run("Shared", func(t *testing.T) {
		app := newApp(t, Hubs("127.0.0.1:1"))
		err := HandleIdempotentCommand(app, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
			return ExternalReceipt{}, nil
		}, Shared())
		if err == nil || !strings.Contains(err.Error(), "commands are never shared") {
			t.Fatalf("the shared command: %v", err)
		}
	})
	t.Run("ExternalIdempotent on a query, stream or subscription", func(t *testing.T) {
		query := newApp(t, Hubs("127.0.0.1:1"))
		if err := HandleQuery(query, func(context.Context, ExternalCharge) (ExternalReceipt, error) {
			return ExternalReceipt{}, nil
		}, ExternalIdempotent()); err == nil || !strings.Contains(err.Error(), "HandleIdempotentCommand") {
			t.Fatalf("the query: %v", err)
		}
		stream := newApp(t, Hubs("127.0.0.1:1"))
		if err := HandleStream[ExternalCharge, ExternalReceipt](stream,
			func(context.Context, ExternalCharge, *StreamWriter[ExternalReceipt]) error { return nil },
			ExternalIdempotent()); err == nil || !strings.Contains(err.Error(), "HandleIdempotentCommand") {
			t.Fatalf("the stream: %v", err)
		}
		subscription := newApp(t, Hubs("127.0.0.1:1"))
		if _, err := HandleSubscription[ExternalCharge, ExternalReceipt, ExternalReceipt](subscription,
			func(context.Context, ExternalCharge) (ExternalReceipt, error) { return ExternalReceipt{}, nil },
			ExternalIdempotent()); err == nil || !strings.Contains(err.Error(), "HandleIdempotentCommand") {
			t.Fatalf("the subscription: %v", err)
		}
	})
}
