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

// failingRelayOutbox refuses the two relays' claims: the check must report the error instead of
// leaving the other relay at the barrier.
type failingRelayOutbox struct {
	snodra.OutboxStore
}

func (f failingRelayOutbox) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	if owner == "relay-a" || owner == "relay-b" {
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

// TestTheKitReportsAnEarlyRelayError proves the concurrent-relay check reports a claim error
// promptly: an early error must not leave the other relay blocked at the barrier.
func TestTheKitReportsAnEarlyRelayError(t *testing.T) {
	started := time.Now()
	expectCheckFailure(t, func(tb storeTB) {
		concurrentRelaysPublishEachEntryOnce(tb, failingRelayStore{snodra.NewMemoryMessageStore()})
	})
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the check took %v to report the claim error", elapsed)
	}
}
