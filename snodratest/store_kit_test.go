package snodratest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// recorderTB is a testing.TB stand-in: it records what a check reports and stops the check with
// a panic where testing.TB would stop it.
type recorderTB struct {
	mu       sync.Mutex
	failures []string
}

func (*recorderTB) Helper() {}

func (r *recorderTB) Errorf(format string, args ...any) {
	r.record(fmt.Sprintf(format, args...))
}

func (r *recorderTB) Fatalf(format string, args ...any) {
	r.record(fmt.Sprintf(format, args...))
	panic(checkFailed{})
}

func (r *recorderTB) Fatal(args ...any) {
	r.record(fmt.Sprint(args...))
	panic(checkFailed{})
}

func (r *recorderTB) record(failure string) {
	r.mu.Lock()
	r.failures = append(r.failures, failure)
	r.mu.Unlock()
}

func (r *recorderTB) failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.failures) > 0
}

// checkFailed stops a check running on a recorderTB, as t.Fatal stops it.
type checkFailed struct{}

// expectCheckFailure runs check on a broken store and fails the test when the check passes.
func expectCheckFailure(t *testing.T, check func(storeTB)) {
	t.Helper()
	recorder := &recorderTB{}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if _, ok := recovered.(checkFailed); !ok {
					panic(recovered)
				}
			}
		}()
		check(recorder)
	}()
	if !recorder.failed() {
		t.Fatal("the check passed on a broken store")
	}
}

// reversedOutbox hands a claim's entries back in the order opposite to the store's.
type reversedOutbox struct {
	snodra.OutboxStore
}

func (r reversedOutbox) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	entries, err := r.OutboxStore.Claim(ctx, owner, lease, limit)
	slices.Reverse(entries)
	return entries, err
}

type reversedClaimStore struct {
	snodra.MessageStore
}

func (r reversedClaimStore) Outbox() snodra.OutboxStore {
	return reversedOutbox{r.MessageStore.Outbox()}
}

// TestTheKitFailsAClaimOutOfOrder proves the claim-order check bites.
func TestTheKitFailsAClaimOutOfOrder(t *testing.T) {
	expectCheckFailure(t, func(tb storeTB) {
		appendedEntryIsClaimedInCreatedSeqOrder(tb, reversedClaimStore{snodra.NewMemoryMessageStore()})
	})
}

// longLeaseOutbox claims every entry for a day, whatever lease it was asked for.
type longLeaseOutbox struct {
	snodra.OutboxStore
}

func (l longLeaseOutbox) Claim(ctx context.Context, owner string, _ time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	return l.OutboxStore.Claim(ctx, owner, 24*time.Hour, limit)
}

type longLeaseStore struct {
	snodra.MessageStore
}

func (l longLeaseStore) Outbox() snodra.OutboxStore {
	return longLeaseOutbox{l.MessageStore.Outbox()}
}

// TestTheKitFailsALeaseThatNeverEnds proves the lease-expiry check bites.
func TestTheKitFailsALeaseThatNeverEnds(t *testing.T) {
	expectCheckFailure(t, func(tb storeTB) {
		expiredLeaseIsReclaimed(tb, longLeaseStore{snodra.NewMemoryMessageStore()})
	})
}

// ignoringTxOutbox commits every append at once, whatever transaction its context carries.
type ignoringTxOutbox struct {
	snodra.OutboxStore
}

func (i ignoringTxOutbox) Append(ctx context.Context, entry snodra.OutboxEntry) error {
	return i.OutboxStore.Append(context.Background(), entry)
}

type ignoringTxStore struct {
	snodra.MessageStore
}

func (i ignoringTxStore) Outbox() snodra.OutboxStore {
	return ignoringTxOutbox{i.MessageStore.Outbox()}
}

// TestTheKitFailsAnAppendThatIgnoresTheTransaction proves the rollback check bites.
func TestTheKitFailsAnAppendThatIgnoresTheTransaction(t *testing.T) {
	expectCheckFailure(t, func(tb storeTB) {
		rolledBackTransactionLeavesNoOutboxEntry(tb, ignoringTxStore{snodra.NewMemoryMessageStore()})
	})
}

// lyingProcessed reports every record as new.
type lyingProcessed struct {
	snodra.ProcessedStore
}

func (l lyingProcessed) Record(ctx context.Context, marker snodra.ProcessedMarker) (bool, error) {
	if _, err := l.ProcessedStore.Record(ctx, marker); err != nil {
		return false, err
	}
	return true, nil
}

type lyingProcessedStore struct {
	snodra.MessageStore
}

func (l lyingProcessedStore) Processed() snodra.ProcessedStore {
	return lyingProcessed{ProcessedStore: l.MessageStore.Processed()}
}

// TestTheKitFailsADuplicateMarkerThatRecordsAgain proves the marker check bites.
func TestTheKitFailsADuplicateMarkerThatRecordsAgain(t *testing.T) {
	expectCheckFailure(t, func(tb storeTB) {
		processedMarkerRecordsOnce(tb, lyingProcessedStore{snodra.NewMemoryMessageStore()})
	})
}

// failingRelayOutbox refuses relay-a's claims only: relay-b's first claim succeeds, so with a
// barrier that waits for a second token from a failed relay-a, relay-b blocks there.
type failingRelayOutbox struct {
	snodra.OutboxStore
}

func (f failingRelayOutbox) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	if owner == "relay-a" {
		return nil, errors.New("the claim failed for the test")
	}
	return f.OutboxStore.Claim(ctx, owner, lease, limit)
}

type failingRelayStore struct {
	snodra.MessageStore
}

func (f failingRelayStore) Outbox() snodra.OutboxStore {
	return failingRelayOutbox{f.MessageStore.Outbox()}
}

// softTB records a check's failures without stopping it, so a check that is meant to fail promptly
// can be watched for a hang.
type softTB struct {
	mu       sync.Mutex
	failures []string
}

func (*softTB) Helper() {}

func (s *softTB) Errorf(format string, args ...any) { s.record(format, args...) }
func (s *softTB) Fatalf(format string, args ...any) { s.record(format, args...) }
func (s *softTB) Fatal(args ...any)                 { s.record("%s", fmt.Sprint(args...)) }

func (s *softTB) record(format string, args ...any) {
	s.mu.Lock()
	s.failures = append(s.failures, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

func (s *softTB) failed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.failures) > 0
}

// TestTheKitReportsAnEarlyRelayError proves the concurrent-relay check reports a claim error
// promptly: an early error of relay-a must not leave relay-b blocked at the barrier. The check
// itself would wait a minute for relay-b; the bound here is the regression.
func TestTheKitReportsAnEarlyRelayError(t *testing.T) {
	recorder := &softTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		concurrentRelaysPublishEachEntryOnce(recorder, failingRelayStore{snodra.NewMemoryMessageStore()})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the concurrent-relay check blocked on relay-a's early error")
	}
	if !recorder.failed() {
		t.Fatal("the check passed on a store whose relay-a cannot claim")
	}
}
