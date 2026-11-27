package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// Two instances prepare the same reset operation concurrently, as the hub does: both must hold
// the operation's lock. The renewal of a prepare that lost the race to the other instance's
// commit must be recognised as the same operation, not reported as another holder.
func TestTwoInstancesPreparingOneOperationBothHoldTheLock(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		database := server.database(t)
		url := server.url(database)
		first := newBareStore(t, url)
		second := newBareStore(t, url)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := first.InitSchema(ctx, nil); err != nil {
			t.Fatalf("applying the schema: %v", err)
		}
		for attempt := 0; attempt < 30; attempt++ {
			processor := "concurrent-" + time.Now().Format("150405.000000000")
			now := time.Now().Truncate(time.Millisecond)
			lease := now.Add(30 * time.Second)
			start := make(chan struct{})
			outcomes := make(chan snodra.LockOutcome, 2)
			errs := make(chan error, 2)
			var wait sync.WaitGroup
			for _, store := range []*Store{first, second} {
				store := store
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					outcome, err := store.Control().Lock(ctx, processor, "op-1", lease, now)
					outcomes <- outcome
					errs <- err
				}()
			}
			close(start)
			wait.Wait()
			close(outcomes)
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatalf("attempt %d: locking: %v", attempt, err)
				}
			}
			for outcome := range outcomes {
				if !outcome.Taken {
					t.Fatalf("attempt %d: one instance did not hold the operation's lock: %+v", attempt, outcome)
				}
			}
		}
	})
}
