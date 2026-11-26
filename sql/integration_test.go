package sql

import (
	"context"
	"crypto/rand"
	stdSQL "database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
)

// bothServers runs the body once on a PostgreSQL container and once on a MySQL one; without
// Docker each subtest skips.
func bothServers(t *testing.T, body func(t *testing.T, server *testServer)) {
	t.Helper()
	t.Run("postgres", func(t *testing.T) { body(t, postgresContainer(t)) })
	t.Run("mysql", func(t *testing.T) { body(t, mysqlContainer(t)) })
}

// The nine level-2 message-store checks on both engines.
func TestTheSQLMessageStorePassesTheKit(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		snodratest.MessageStore(t, server.kitStores(t, len(snodratest.StoreCheckIDs())))
	})
}

// Instances starting together apply the schema once: both startups succeed, snodra_schema holds
// one version 1 row, and a check after them passes. An empty database fails the check with the
// file to apply.
func TestTheSchemaLockSerialisesTwoStartups(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		database := server.database(t)
		url := server.url(database)
		a := newBareStore(t, url)
		b := newBareStore(t, url)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		err := a.CheckSchema(ctx)
		if err == nil || !strings.Contains(err.Error(), "schema is at version 0") {
			t.Fatalf("checking an empty database: %v", err)
		}
		if !strings.Contains(err.Error(), "V1__snodra.sql") {
			t.Fatalf("the refusal does not name the schema file: %v", err)
		}

		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for _, store := range []*Store{a, b} {
			store := store
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- store.InitSchema(ctx, nil)
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("a startup: %v", err)
			}
		}
		var count int
		if err := a.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM snodra_schema").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("snodra_schema holds %d rows, want 1", count)
		}
		for _, store := range []*Store{a, b} {
			if err := store.CheckSchema(ctx); err != nil {
				t.Fatalf("checking the applied schema: %v", err)
			}
			if err := store.InitSchema(ctx, nil); err != nil {
				t.Fatalf("applying it again: %v", err)
			}
		}
	})
}

// A startup waits for the schema lock another instance holds, then applies the schema once that
// instance lets go.
func TestTheSchemaLockWaitsForItsHolder(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, false)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		holder, err := stdSQL.Open(store.driverName(), store.dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Close() }()
		conn, err := holder.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		if server.driver == Postgres {
			if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", pgSchemaLockKey); err != nil {
				t.Fatalf("holding the schema lock: %v", err)
			}
		} else {
			var got stdSQL.NullInt64
			if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", mysqlSchemaLock).Scan(&got); err != nil || !got.Valid || got.Int64 != 1 {
				t.Fatalf("holding the schema lock: %v (%v)", got, err)
			}
		}

		done := make(chan error, 1)
		go func() { done <- store.InitSchema(ctx, nil) }()
		select {
		case err := <-done:
			t.Fatalf("the startup did not wait for the lock: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
		if server.driver == Postgres {
			if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", pgSchemaLockKey); err != nil {
				t.Fatalf("releasing the schema lock: %v", err)
			}
		} else {
			if _, err := conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", mysqlSchemaLock); err != nil {
				t.Fatalf("releasing the schema lock: %v", err)
			}
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("the startup after the lock: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the startup did not finish after the lock was released")
		}
		if err := store.CheckSchema(ctx); err != nil {
			t.Fatalf("checking the applied schema: %v", err)
		}
	})
}

// A claim in its own transaction skips the rows another transaction holds (FOR UPDATE SKIP
// LOCKED), in (created_at, seq) order; once that transaction ends the next claim takes them.
func TestSkipLockedClaimOrder(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, true)
		ctx := context.Background()
		base := time.Now().Add(-time.Minute)
		entries := []snodra.OutboxEntry{
			testEntry("conformance.Ordered", base, 1),
			testEntry("conformance.Ordered", base.Add(time.Second), 2),
			testEntry("conformance.Ordered", base.Add(2*time.Second), 3),
		}
		for _, entry := range entries {
			if err := store.Outbox().Append(ctx, entry); err != nil {
				t.Fatalf("append %s: %v", entry.ID, err)
			}
		}

		raw, err := stdSQL.Open(store.driverName(), store.dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = raw.Close() }()
		holder, err := raw.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := holder.ExecContext(ctx,
			store.bind("SELECT id FROM snodra_outbox WHERE id = ? FOR UPDATE"), entries[1].ID); err != nil {
			_ = holder.Rollback()
			t.Fatalf("holding the second entry: %v", err)
		}
		claimed, err := store.Outbox().Claim(ctx, "relay-a", time.Minute, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if got, want := outboxIDs(claimed), []string{entries[0].ID, entries[2].ID}; !equalStrings(got, want) {
			t.Fatalf("the held entry is skipped, the others in order: got %v, want %v", got, want)
		}
		if err := holder.Rollback(); err != nil {
			t.Fatal(err)
		}
		claimed, err = store.Outbox().Claim(ctx, "relay-b", time.Minute, 10)
		if err != nil {
			t.Fatalf("claim after the holder ended: %v", err)
		}
		if got, want := outboxIDs(claimed), []string{entries[1].ID}; !equalStrings(got, want) {
			t.Fatalf("the next claim takes the released entry: got %v, want %v", got, want)
		}
	})
}

// DeleteMarkers removes exactly the stage-1 markers of one processor in a range: numeric
// positions by value, text positions by bytes, and never a marker without a position, of another
// processor, source or segment.
func TestDeleteMarkersRemovesOnlyTheRange(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, true)
		ctx := context.Background()
		record := func(processor, source, segment, position string) string {
			t.Helper()
			id := "processor:" + processor + ":" + strings.ToLower(rand.Text())
			marker := snodra.ProcessedMarker{
				ID:          id,
				Type:        "conformance.Happened",
				ProcessedAt: time.Now(),
				Processor:   processor,
				Source:      source,
				Segment:     segment,
				Position:    position,
			}
			recorded, err := store.Processed().Record(ctx, marker)
			if err != nil || !recorded {
				t.Fatalf("record %s: %v (%v)", id, recorded, err)
			}
			return id
		}
		nine := record("default", "s", "0", "9")
		ten := record("default", "s", "0", "10")
		text := record("default", "s", "0", "x7")
		none := record("default", "s", "0", "")
		otherProcessor := record("mail", "s", "0", "10")
		otherSource := record("default", "other", "0", "10")
		otherSegment := record("default", "s", "1", "10")

		deleted, err := store.Processed().DeleteMarkers(ctx, "default", []snodra.ReplayRange{
			{Source: "s", Segment: "0", From: "10", Until: "20"},
			{Source: "s", Segment: "0", From: "x0", Until: "y"},
		})
		if err != nil {
			t.Fatalf("delete markers: %v", err)
		}
		if deleted != 2 {
			t.Fatalf("deleted %d markers, want 2", deleted)
		}
		for id, want := range map[string]bool{
			nine: true, ten: false, text: false, none: true,
			otherProcessor: true, otherSource: true, otherSegment: true,
		} {
			exists, err := store.Processed().Exists(ctx, id)
			if err != nil {
				t.Fatalf("exists %s: %v", id, err)
			}
			if exists != want {
				t.Fatalf("marker %s exists=%v, want %v", id, exists, want)
			}
		}
	})
}

// A service's own *sql.Tx (WithTx) and a service's connection in a transaction it keeps itself
// (WithConn) commit and roll back with the library's writes.
func TestTheServiceTransactionBindsTheStoresWrites(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, true)
		ctx := context.Background()

		txctx, begun, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := store.Tx(txctx)
		if !ok || got == nil {
			t.Fatal("Begin's context carries no *sql.Tx")
		}
		if err := begun.Rollback(txctx); err != nil {
			t.Fatal(err)
		}
		if _, ok := store.Tx(txctx); ok {
			t.Fatal("an ended transaction is still returned")
		}
		if _, ok := store.Tx(ctx); ok {
			t.Fatal("a plain context carries a transaction")
		}

		entry := testEntry("conformance.ServiceTx", time.Now(), 1)
		marker := testMarker()
		tx, err := store.DB().BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		sctx := store.WithTx(ctx, tx)
		if err := store.Outbox().Append(sctx, entry); err != nil {
			t.Fatalf("append in the service's transaction: %v", err)
		}
		if recorded, err := store.Processed().Record(sctx, marker); err != nil || !recorded {
			t.Fatalf("record in the service's transaction: %v (%v)", recorded, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 0 {
			t.Fatalf("a rollback left %d entries (%v)", pending, err)
		}
		if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || exists {
			t.Fatalf("a rollback left the marker (%v, %v)", exists, err)
		}

		entry = testEntry("conformance.ServiceTx", time.Now(), 2)
		marker = testMarker()
		tx, err = store.DB().BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		sctx = store.WithTx(ctx, tx)
		if err := store.Outbox().Append(sctx, entry); err != nil {
			t.Fatalf("append in the committed transaction: %v", err)
		}
		if recorded, err := store.Processed().Record(sctx, marker); err != nil || !recorded {
			t.Fatalf("record in the committed transaction: %v (%v)", recorded, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 1 {
			t.Fatalf("a commit left %d entries, want 1 (%v)", pending, err)
		}
		if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || !exists {
			t.Fatalf("a commit left no marker (%v, %v)", exists, err)
		}

		conn, err := store.DB().Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
		cctx := store.WithConn(ctx, conn)
		entry = testEntry("conformance.ServiceConn", time.Now(), 3)
		marker = testMarker()
		if err := store.Outbox().Append(cctx, entry); err != nil {
			t.Fatalf("append on the service's connection: %v", err)
		}
		if recorded, err := store.Processed().Record(cctx, marker); err != nil || !recorded {
			t.Fatalf("record on the service's connection: %v (%v)", recorded, err)
		}
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			t.Fatal(err)
		}
		if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 1 {
			t.Fatalf("a connection rollback changed the count to %d (%v)", pending, err)
		}
		if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || exists {
			t.Fatalf("a connection rollback left the marker (%v, %v)", exists, err)
		}

		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			t.Fatal(err)
		}
		entry = testEntry("conformance.ServiceConn", time.Now(), 4)
		marker = testMarker()
		if err := store.Outbox().Append(cctx, entry); err != nil {
			t.Fatalf("append on the committed connection: %v", err)
		}
		if recorded, err := store.Processed().Record(cctx, marker); err != nil || !recorded {
			t.Fatalf("record on the committed connection: %v (%v)", recorded, err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			t.Fatal(err)
		}
		if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 2 {
			t.Fatalf("a connection commit left %d entries, want 2 (%v)", pending, err)
		}
		if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || !exists {
			t.Fatalf("a connection commit left no marker (%v, %v)", exists, err)
		}
	})
}

// The store's own transactions run at READ COMMITTED: a row another connection commits after the
// transaction began is visible to its next read. MySQL's default (REPEATABLE READ) would not see
// it, so this pins the isolation the claims and the purge depend on.
func TestOwnTransactionsAreReadCommitted(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, true)
		ctx := context.Background()
		tx, err := store.beginOwn(ctx)
		if err != nil {
			t.Fatalf("beginning the store's own transaction: %v", err)
		}
		defer func() { _ = tx.Rollback() }()
		var before, after int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM snodra_outbox").Scan(&before); err != nil {
			t.Fatalf("the first read: %v", err)
		}
		if err := store.Outbox().Append(ctx, testEntry("conformance.Isolation", time.Now(), 1)); err != nil {
			t.Fatalf("appending on another connection: %v", err)
		}
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM snodra_outbox").Scan(&after); err != nil {
			t.Fatalf("the second read: %v", err)
		}
		if before != 0 || after != 1 {
			t.Fatalf("the own transaction saw %d then %d rows, want 0 then 1: it is not READ COMMITTED", before, after)
		}
	})
}

// On PostgreSQL an append's transaction notifies snodra_outbox: a commit wakes the relay, a
// rollback wakes nobody. MySQL has no notification and the relay polls.
func TestACommitWakesAndARollbackDoesNot(t *testing.T) {
	server := postgresContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()
	var woken atomic.Int64
	cancel := store.Outbox().OnAppend(func() { woken.Add(1) })
	defer cancel()
	waitFor(t, "the listener to establish LISTEN", 10*time.Second, func() bool { return store.listens() >= 1 })
	waitFor(t, "the listener's start-up wake", 5*time.Second, func() bool { return woken.Load() >= 1 })
	woken.Store(0)

	// The committed append that follows is the barrier: its notification reaches the listener
	// after the rolled-back transaction ended, so a rollback that notified would be counted
	// before this wait succeeds.
	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Outbox().Append(txctx, testEntry("conformance.Notify", time.Now(), 1)); err != nil {
		t.Fatalf("append in the rolled-back transaction: %v", err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	if got := woken.Load(); got != 0 {
		t.Fatalf("the rolled-back append woke the relay %d times", got)
	}

	txctx, tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Outbox().Append(txctx, testEntry("conformance.Notify", time.Now(), 2)); err != nil {
		t.Fatalf("append in the committed transaction: %v", err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a commit to wake the relay", 5*time.Second, func() bool { return woken.Load() >= 1 })
	// The commit accounts for one wake. A duplicate delivery or a listener restart may add one;
	// more than that is not one commit's worth, so the rolled-back append may have notified.
	if got := woken.Load(); got < 1 || got > 2 {
		t.Fatalf("one commit woke the relay %d times; want one, tolerating one duplicate", got)
	}
}

// The store stamps the times it sets itself (a claim's lease, an entry's sending and its failure)
// with the application's clock, never the database's.
func TestTheApplicationClockStampsTheStoresTimes(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, true)
		at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
		store.UseClock(func() time.Time { return at })
		ctx := context.Background()
		outbox := store.Outbox()
		sent := testEntry("conformance.Clock", time.Now(), 1)
		failed := testEntry("conformance.Clock", time.Now(), 2)
		for _, entry := range []snodra.OutboxEntry{sent, failed} {
			if err := outbox.Append(ctx, entry); err != nil {
				t.Fatalf("append %s: %v", entry.ID, err)
			}
		}
		claimed, err := outbox.Claim(ctx, "relay-a", 30*time.Second, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(claimed) != 2 {
			t.Fatalf("claimed %d entries, want 2", len(claimed))
		}
		read := func(id string) (sentAt, failedAt, leaseUntil stdSQL.NullTime) {
			t.Helper()
			if err := store.DB().QueryRowContext(ctx,
				store.bind("SELECT sent_at, failed_at, lease_until FROM snodra_outbox WHERE id = ?"), id).
				Scan(&sentAt, &failedAt, &leaseUntil); err != nil {
				t.Fatalf("reading %s: %v", id, err)
			}
			return sentAt, failedAt, leaseUntil
		}
		_, _, lease := read(sent.ID)
		if want := at.Add(30 * time.Second); !lease.Valid || !lease.Time.Equal(want) {
			t.Fatalf("the claim's lease: got %v, want %v", lease, want)
		}
		if err := outbox.MarkSent(ctx, []string{sent.ID}); err != nil {
			t.Fatal(err)
		}
		sentAt, _, lease := read(sent.ID)
		if !sentAt.Valid || !sentAt.Time.Equal(at) {
			t.Fatalf("sent at: got %v, want %v", sentAt, at)
		}
		if !lease.Valid || !lease.Time.Equal(at.Add(30*time.Second)) {
			t.Fatalf("mark sent kept the lease: got %v", lease)
		}
		if err := outbox.MarkFailed(ctx, failed.ID, "conformance: given up"); err != nil {
			t.Fatal(err)
		}
		_, failedAt, _ := read(failed.ID)
		if !failedAt.Valid || !failedAt.Time.Equal(at) {
			t.Fatalf("failed at: got %v, want %v", failedAt, at)
		}
	})
}

// Background runs the purge on its interval and returns when the context ends.
func TestBackgroundPurgesOnItsInterval(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		database := server.database(t)
		store, err := New(Config{
			URL:             server.url(database),
			PurgeInterval:   20 * time.Millisecond,
			OutboxRetention: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("the store over %s: %v", database, err)
		}
		t.Cleanup(func() { _ = store.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := store.InitSchema(ctx, nil); err != nil {
			t.Fatalf("applying the schema: %v", err)
		}

		runCtx, stop := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- store.Background(runCtx) }()
		entry := testEntry("conformance.Background", time.Now(), 1)
		if err := store.Outbox().Append(ctx, entry); err != nil {
			t.Fatal(err)
		}
		claimed, err := store.Outbox().Claim(ctx, "relay", time.Minute, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim: %d entries (%v)", len(claimed), err)
		}
		if err := store.Outbox().MarkSent(ctx, []string{entry.ID}); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the background purge to remove the sent entry", 10*time.Second, func() bool {
			var count int
			if err := store.DB().QueryRowContext(context.Background(),
				"SELECT COUNT(*) FROM snodra_outbox").Scan(&count); err != nil {
				return false
			}
			return count == 0
		})
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("background: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("background did not return after the context ended")
		}
	})
}

// Every retention removes its old rows and keeps the fresh, pending, live and scheduled ones.
func TestPurgeExpiresEveryRetention(t *testing.T) {
	bothServers(t, func(t *testing.T, server *testServer) {
		store := server.newStore(t, true)
		ctx := context.Background()
		old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		fresh := time.Now()
		future := fresh.Add(24 * time.Hour)
		json := store.jsonType()
		insert := func(query string, args ...any) {
			t.Helper()
			if _, err := store.DB().ExecContext(ctx, store.bind(query), args...); err != nil {
				t.Fatalf("insert: %v\n%s", err, query)
			}
		}
		markerHash := strings.Repeat("a", 64)
		freshHash := strings.Repeat("b", 64)
		callHash := strings.Repeat("c", 64)
		failureHash := strings.Repeat("f", 64)
		insert(fmt.Sprintf("INSERT INTO snodra_outbox (id, type, headers, payload, created_at, seq, attempts, sent_at) VALUES ('old-outbox', 't', CAST(? AS %s), CAST(? AS %s), ?, 1, 0, ?)", json, json), "{}", "{}", old, old)
		insert(fmt.Sprintf("INSERT INTO snodra_outbox (id, type, headers, payload, created_at, seq, attempts, sent_at) VALUES ('fresh-outbox', 't', CAST(? AS %s), CAST(? AS %s), ?, 1, 0, ?)", json, json), "{}", "{}", fresh, fresh)
		insert("INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES (?, 'old-marker', 't', ?)", markerHash, old)
		insert("INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES (?, 'fresh-marker', 't', ?)", freshHash, fresh)
		insert(fmt.Sprintf("INSERT INTO snodra_dead_letters (id, processor, source, segment, position, headers, reason, error_type, error_message, attempts, parked_at, last_attempt_at, expire_at) VALUES ('old-letter', 'p', 's', '0', '1', CAST(? AS %s), 'INVALID', 'x', 'x', 1, ?, ?, ?)", json), "{}", old, old, old)
		insert(fmt.Sprintf("INSERT INTO snodra_dead_letters (id, processor, source, segment, position, headers, reason, error_type, error_message, attempts, parked_at, last_attempt_at, expire_at) VALUES ('fresh-letter', 'p', 's', '0', '2', CAST(? AS %s), 'INVALID', 'x', 'x', 1, ?, ?, ?)", json), "{}", old, old, future)
		insert("INSERT INTO snodra_external_calls (id_hash, id, status, attempts, updated_at) VALUES (?, 'old-call', 'SUCCEEDED', 1, ?)", callHash, old)
		insert("INSERT INTO snodra_sagas (id, type, state, ended, ended_at, version, created_at, updated_at) VALUES ('old-saga', 't', '{}', TRUE, ?, 1, ?, ?)", old, old, old)
		insert("INSERT INTO snodra_sagas (id, type, state, ended, version, created_at, updated_at) VALUES ('live-saga', 't', '{}', FALSE, 1, ?, ?)", old, old)
		insert(fmt.Sprintf("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, created_at, attempts, dispatched_at) VALUES ('old-sent', 's', 1, 'EVENT', 't', '{}', CAST(? AS %s), ?, 0, ?)", json), "{}", old, old)
		insert(fmt.Sprintf("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, created_at, attempts, failed_at) VALUES ('old-failed', 's', 2, 'COMMAND', 't', '{}', CAST(? AS %s), ?, 0, ?)", json), "{}", old, old)
		insert(fmt.Sprintf("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, created_at, attempts) VALUES ('pending', 's', 3, 'COMMAND', 't', '{}', CAST(? AS %s), ?, 0)", json), "{}", old)
		insert("INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES ('old-lease', 'r', ?)", old)
		insert("INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES ('fresh-lease', 'r', ?)", future)
		insert("INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, due_at, correlation_id, status, finished_at) VALUES ('old-deadline', 't', 's', 'n', ?, 'c', 'FIRED', ?)", old, old)
		insert("INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, due_at, correlation_id, status) VALUES ('scheduled', 't', 's', 'n', ?, 'c', 'SCHEDULED')", old)
		insert(fmt.Sprintf("INSERT INTO snodra_saga_failures (id_hash, id, saga_type, event_id, event_type, payload, headers, error, attempts, first_failed_at, last_failed_at) VALUES (?, 'old-failure', 't', 'e', 't', '{}', CAST(? AS %s), 'x', 1, ?, ?)", json), failureHash, "{}", old, old)

		purged, err := store.purgeExpired(ctx)
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		if purged != 10 {
			t.Fatalf("purged %d rows, want 10", purged)
		}
		for _, c := range []struct {
			table string
			want  int
		}{
			{"snodra_outbox", 1},
			{"snodra_processed_events", 1},
			{"snodra_dead_letters", 1},
			{"snodra_external_calls", 0},
			{"snodra_sagas", 1},
			{"snodra_saga_dispatch", 1},
			{"snodra_saga_dispatch_leases", 1},
			{"snodra_deadlines", 1},
			{"snodra_saga_failures", 0},
		} {
			var count int
			if err := store.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table).Scan(&count); err != nil {
				t.Fatalf("%s: %v", c.table, err)
			}
			if count != c.want {
				t.Fatalf("%s holds %d rows, want %d", c.table, count, c.want)
			}
		}
	})
}

// newBareStore is a store over url with no schema applied; the caller closes it.
func newBareStore(t *testing.T, url string) *Store {
	t.Helper()
	store, err := New(Config{URL: url})
	if err != nil {
		t.Fatalf("the store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// outboxIDs is the ids of entries.
func outboxIDs(entries []snodra.OutboxEntry) []string {
	ids := make([]string, len(entries))
	for i, entry := range entries {
		ids[i] = entry.ID
	}
	return ids
}

// equalStrings reports whether two slices hold the same strings in the same order.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
