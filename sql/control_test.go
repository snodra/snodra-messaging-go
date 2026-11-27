package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// A concurrent preparation of the same operation is a renewal: the second instance's renew
// statement runs before the first instance's take commits, so it misses; its take then queues
// behind the first instance's row lock and, after the commit, matches no free row. The fixed
// store reads the holder, sees its own operation and renews it. The barriers below hold each
// instance at those two points, making the interleaving deterministic; the same test fails when
// the recovery block is removed.
//
// PostgreSQL is the engine where the interleaving is reachable through Lock: an INSERT ... ON
// CONFLICT DO NOTHING on the existing control row does not hold it. MySQL's no-op ON DUPLICATE
// KEY UPDATE locks the row in ensureControl, so two Lock calls there serialise before the renew;
// its stress test below stays defensive.
func TestAConcurrentPreparationOnPostgresIsARenewal(t *testing.T) {
	server := postgresContainer(t)
	database := server.database(t)
	url := server.url(database)
	first := newBareStore(t, url)
	second := newBareStore(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := first.InitSchema(ctx, nil); err != nil {
		t.Fatalf("applying the schema: %v", err)
	}

	now := time.Now().Truncate(time.Millisecond)
	lease := now.Add(30 * time.Second)
	processor := "concurrent-" + time.Now().Format("150405.000000000")
	if err := first.Control().SetPaused(ctx, processor, false, "test", now); err != nil {
		t.Fatalf("creating the control row: %v", err)
	}

	var (
		aRenewed     = make(chan struct{})
		bRenewed     = make(chan struct{})
		aTook        = make(chan struct{})
		allowATake   = make(chan struct{})
		allowBTake   = make(chan struct{})
		allowACommit = make(chan struct{})
	)
	first.lockStep = func(step string) {
		switch step {
		case "renewed":
			close(aRenewed)
			<-allowATake
		case "taken":
			close(aTook)
			<-allowACommit
		}
	}
	second.lockStep = func(step string) {
		if step == "renewed" {
			close(bRenewed)
			<-allowBTake
		}
	}

	type result struct {
		outcome snodra.LockOutcome
		err     error
	}
	lock := func(store *Store) <-chan result {
		done := make(chan result, 1)
		go func() {
			outcome, err := store.Control().Lock(ctx, processor, "op-1", lease, now)
			done <- result{outcome, err}
		}()
		return done
	}

	a := lock(first)
	<-aRenewed
	b := lock(second)
	<-bRenewed

	close(allowATake)
	<-aTook
	close(allowBTake)
	// B's take queues behind A's row lock; give it time to reach the statement before A commits.
	time.Sleep(200 * time.Millisecond)
	close(allowACommit)

	for _, done := range []<-chan result{a, b} {
		got := <-done
		if got.err != nil {
			t.Fatalf("locking: %v", got.err)
		}
		if !got.outcome.Taken {
			t.Fatalf("one instance did not hold the operation's lock: %+v", got.outcome)
		}
	}
}

// Two instances prepare the same reset operation concurrently, as the hub does: both must hold
// the operation's lock. On MySQL the store's no-op ON DUPLICATE KEY UPDATE serialises the two
// calls before the renew, so this stress run is defensive there; it is the regression test on
// PostgreSQL only when the interleaving is forced (the test above).
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
