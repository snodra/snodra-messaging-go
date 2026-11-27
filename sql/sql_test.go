package sql

import (
	"context"
	"errors"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

func TestNewValidatesTheConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		config Config
	}{
		{"no URL", Config{}},
		{"unknown scheme", Config{URL: "oracle://host/db"}},
		{"one connection", Config{URL: "postgres://user:password@127.0.0.1:1/db", MaxOpenConns: 1}},
		{"negative idle", Config{URL: "postgres://user:password@127.0.0.1:1/db", MaxIdleConns: -1}},
		{"negative lifetime", Config{URL: "postgres://user:password@127.0.0.1:1/db", ConnMaxLifetime: -time.Second}},
		{"negative retention", Config{URL: "postgres://user:password@127.0.0.1:1/db", OutboxRetention: -time.Second}},
		{"MySQL without a database", Config{URL: "mysql://root@127.0.0.1:3306"}},
	}
	for _, c := range cases {
		store, err := New(c.config)
		if err == nil {
			_ = store.Close()
			t.Fatalf("%s was accepted", c.name)
		}
		if !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("%s: %v, want ErrInvalid", c.name, err)
		}
	}
}

func TestNewNamesTheStoreAndFillsTheDefaults(t *testing.T) {
	for _, c := range []struct {
		url  string
		name string
	}{
		{"postgres://user:password@127.0.0.1:1/db", NamePostgres},
		{"postgresql://user:password@127.0.0.1:1/db", NamePostgres},
		{"mysql://root:secret@127.0.0.1:3306/db", NameMySQL},
		{"root:secret@tcp(127.0.0.1:3306)/db", NameMySQL},
	} {
		store, err := New(Config{URL: c.url})
		if err != nil {
			t.Fatalf("%s: %v", c.url, err)
		}
		if got := store.Name(); got != c.name {
			t.Fatalf("%s: name %q, want %q", c.url, got, c.name)
		}
		settings := store.Settings()
		if settings.Driver != Driver(c.name) {
			t.Fatalf("%s: settings driver %q, want %q", c.url, settings.Driver, c.name)
		}
		if settings.MaxOpenConns != DefaultMaxOpenConns || settings.MaxIdleConns != DefaultMaxIdleConns {
			t.Fatalf("%s: pool %d/%d", c.url, settings.MaxOpenConns, settings.MaxIdleConns)
		}
		if settings.PurgeInterval != DefaultPurgeInterval {
			t.Fatalf("%s: purge interval %v", c.url, settings.PurgeInterval)
		}
		if settings.OutboxRetention != DefaultOutboxRetention {
			t.Fatalf("%s: outbox retention %v", c.url, settings.OutboxRetention)
		}
		if store.DB() == nil {
			t.Fatalf("%s: no pool", c.url)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("%s: close: %v", c.url, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("%s: closing twice: %v", c.url, err)
		}
	}
}

func TestAnExplicitDriverIsHonoured(t *testing.T) {
	if _, err := New(Config{URL: "mysql://root@127.0.0.1:3306/db", Driver: Postgres}); err == nil {
		t.Fatal("a MySQL URL with the PostgreSQL driver was accepted")
	}
	store, err := New(Config{URL: "root@tcp(127.0.0.1:3306)/db", Driver: MySQL})
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
}

func TestFromEnvReadsTheSQLSettings(t *testing.T) {
	t.Setenv("SNODRA_STORE_SQL_URL", "postgres://user:password@127.0.0.1:1/db")
	t.Setenv("SNODRA_STORE_SQL_DRIVER", "postgres")
	t.Setenv("SNODRA_STORE_SQL_NOTIFY", "false")
	t.Setenv("SNODRA_STORE_SQL_PURGE_INTERVAL_MS", "0")
	config := FromEnv()
	if config.URL != "postgres://user:password@127.0.0.1:1/db" || config.Driver != Postgres {
		t.Fatalf("FromEnv: %+v", config)
	}
	if !config.DisableNotify {
		t.Fatal("notify was not turned off")
	}
	if config.PurgeInterval >= 0 {
		t.Fatalf("the purge interval is %v, want it off", config.PurgeInterval)
	}
	store, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if store.notifier != nil {
		t.Fatal("a notifying listener was created with notifications off")
	}
	_ = store.Close()
}

func TestTheUnsupportedPortsSaySo(t *testing.T) {
	store := &Store{driver: Postgres}
	var errs []error
	_, err := store.ExternalCalls().Reset(context.Background(), "key", "owner")
	errs = append(errs, err)
	errs = append(errs, store.Sagas().Sagas.Insert(context.Background(), snodra.Saga{}))
	errs = append(errs, store.Sagas().Dispatch.ClearNotice(context.Background(), "id"))
	errs = append(errs, store.Sagas().Deadlines.CancelAll(context.Background(), "saga", time.Now()))
	errs = append(errs, store.Sagas().Failures.Save(context.Background(), snodra.SagaFailure{}))
	errs = append(errs, store.Sagas().Markers.Insert(context.Background(), "id", "type", time.Now()))
	for _, err := range errs {
		if !errors.Is(err, snodra.ErrUnsupported) {
			t.Fatalf("%v, want ErrUnsupported", err)
		}
	}
}

func TestMySQLOnAppendRegistersNothing(t *testing.T) {
	store, err := New(Config{URL: "mysql://root@127.0.0.1:3306/db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	called := false
	cancel := store.Outbox().OnAppend(func() { called = true })
	cancel()
	cancel()
	if called {
		t.Fatal("MySQL woke a relay without a notification")
	}
	if store.listens() != 0 {
		t.Fatal("MySQL opened a listener")
	}
}

func TestWithTxAndWithConnIgnoreNil(t *testing.T) {
	store, err := New(Config{URL: "postgres://user:password@127.0.0.1:1/db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if got := store.WithTx(ctx, nil); got != ctx {
		t.Fatal("WithTx(nil) returned a new context")
	}
	if got := store.WithConn(ctx, nil); got != ctx {
		t.Fatal("WithConn(nil) returned a new context")
	}
}

// With the purge off, Background has nothing to run and waits for the app to end.
func TestBackgroundWithThePurgeOffWaitsForTheContext(t *testing.T) {
	store, err := New(Config{URL: "postgres://user:password@127.0.0.1:1/db", PurgeInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- store.Background(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("background: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background did not return after the context ended")
	}
}

// A listener never starts after Close: registering after the notifier stopped, or after a fresh
// store closed, is a no-op, and its cancel is safe to call.
func TestAListenerDoesNotStartAfterClose(t *testing.T) {
	newStore := func() *Store {
		t.Helper()
		store, err := New(Config{URL: "postgres://user:password@127.0.0.1:1/db"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}
	add := func(what string, store *Store, register func(func()) func()) {
		t.Helper()
		called := false
		cancel := register(func() { called = true })
		store.notifier.mu.Lock()
		running := store.notifier.cancel != nil
		_, registered := store.notifier.wakers[0]
		store.notifier.mu.Unlock()
		if running || registered {
			t.Fatalf("%s: a listener was started after the store stopped", what)
		}
		cancel()
		cancel()
		store.notifier.wakeAll()
		if called {
			t.Fatalf("%s: a wake registered after the store stopped was called", what)
		}
	}
	stopped := newStore()
	stopped.notifier.stop()
	add("stopped", stopped, stopped.notifier.register)

	closed := newStore()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	add("closed", closed, closed.Outbox().OnAppend)
}

// A transaction of another SQL store in the context is refused, never run on this store's pool.
func TestAnotherStoresTransactionIsRefused(t *testing.T) {
	first, err := New(Config{URL: "postgres://user:password@127.0.0.1:1/db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	second, err := New(Config{URL: "postgres://user:password@127.0.0.1:1/db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	ctx := context.WithValue(context.Background(), storeTxKey{}, &storeTx{store: first})
	if err := second.Outbox().Append(ctx, testEntry("conformance.Foreign", time.Now(), 1)); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("append with another store's transaction: %v, want ErrInvalid", err)
	}
	if _, err := second.Processed().Record(ctx, testMarker()); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("record with another store's transaction: %v, want ErrInvalid", err)
	}
}

func TestClosingTwiceIsANoOp(t *testing.T) {
	store, err := New(Config{URL: "postgres://user:password@127.0.0.1:1/db"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
