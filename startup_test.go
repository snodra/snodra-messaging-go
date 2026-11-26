package snodra

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// lifecycleStore is a message store that implements StoreLifecycle and records the calls.
type lifecycleStore struct {
	MessageStore
	mu        sync.Mutex
	clock     func() time.Time
	config    *Config
	initCalls int
	checkCall int
	initErr   error
	checkErr  error
	bgErr     error
	bgDone    chan struct{}
}

func newLifecycleStore(inner MessageStore) *lifecycleStore {
	return &lifecycleStore{MessageStore: inner, bgDone: make(chan struct{})}
}

func (s *lifecycleStore) UseClock(now func() time.Time) {
	s.mu.Lock()
	s.clock = now
	s.mu.Unlock()
}

func (s *lifecycleStore) InitSchema(_ context.Context, config *Config) error {
	s.mu.Lock()
	s.initCalls++
	s.config = config
	s.mu.Unlock()
	return s.initErr
}

func (s *lifecycleStore) CheckSchema(context.Context) error {
	s.mu.Lock()
	s.checkCall++
	s.mu.Unlock()
	return s.checkErr
}

func (s *lifecycleStore) Background(ctx context.Context) error {
	if s.bgErr != nil {
		return s.bgErr
	}
	<-ctx.Done()
	close(s.bgDone)
	return nil
}

func (s *lifecycleStore) calls() (initCalls, checkCalls int, clock func() time.Time, config *Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initCalls, s.checkCall, s.clock, s.config
}

// A service with only command, query and subscription-query handlers starts without a message
// store and an event store.
func TestACommandOnlyServiceStartsWithoutStores(t *testing.T) {
	app := eventsApp(t)
	if err := HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	is(t, len(app.processors), 0)
	if app.bgDone != nil {
		t.Fatal("a command-only service started background work")
	}
}

// L2-NO-ADD-ON: an event handler without the stores fails at Start, naming the missing add-on.
func TestAnEventHandlerWithoutStoresFailsAtStart(t *testing.T) {
	register := func(app *App) {
		t.Helper()
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name  string
		opts  []Option
		wants []string
	}{
		{"no stores", nil, []string{"event handler for orders.OrderPlaced", "message store", "WithStore"}},
		{"no event store", []Option{WithStore(NewMemoryMessageStore())}, []string{"event store", "WithEventStore"}},
		{"no message store", []Option{WithEventStore(NewMemoryEventStore())}, []string{"message store", "WithStore"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := eventsApp(t, test.opts...)
			register(app)
			err := app.Start(context.Background())
			if err == nil {
				t.Fatal("Start succeeded without the stores an event handler needs")
			}
			for _, want := range test.wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Start error %q does not name %q", err, want)
				}
			}
		})
	}
}

// L2-NO-ADD-ON: a first publish without the stores fails naming the missing add-on, since a
// service that only handles commands looks the same at startup.
func TestPublishWithoutAddOnsNamesThem(t *testing.T) {
	app := eventsApp(t)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	err := Publish(context.Background(), app, OrderPlaced{OrderID: "o-1"})
	if err == nil || !strings.Contains(err.Error(), "message store") || !strings.Contains(err.Error(), "WithStore") {
		t.Fatalf("Publish without stores: %v", err)
	}
	withStore := eventsApp(t, WithStore(NewMemoryMessageStore()))
	err = Publish(context.Background(), withStore, OrderPlaced{OrderID: "o-1"})
	if err == nil || !strings.Contains(err.Error(), "event store") || !strings.Contains(err.Error(), "WithEventStore") {
		t.Fatalf("Publish without an event store: %v", err)
	}
	withEvents := eventsApp(t, WithEventStore(NewMemoryEventStore()))
	err = Publish(context.Background(), withEvents, OrderPlaced{OrderID: "o-1"})
	if err == nil || !strings.Contains(err.Error(), "message store") {
		t.Fatalf("Publish without a message store: %v", err)
	}
	var nilApp *App
	if err := Publish(context.Background(), nilApp, OrderPlaced{}); err == nil {
		t.Fatal("Publish on a nil App succeeded")
	}
}

// The builder hands the store the application's clock, prepares its schema and starts its
// background work, which the shutdown stops.
func TestTheBuilderBindsTheStoreLifecycle(t *testing.T) {
	inner := NewMemoryMessageStore()
	store := newLifecycleStore(inner)
	app := eventsApp(t, WithStore(store))
	at := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	app.now = func() time.Time { return at }
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	initCalls, checkCalls, clock, config := store.calls()
	is(t, initCalls, 1)
	is(t, checkCalls, 0)
	if clock == nil || !clock().Equal(at) {
		t.Fatalf("the store's clock is not the app's: %v", clock())
	}
	if config == nil || config.Service != "orders-service" || config.EventsSource != DefaultEventsSource {
		t.Fatalf("the store's config is not the app's: %+v", config)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-store.bgDone:
	case <-time.After(time.Second):
		t.Fatal("the store's background work did not stop at Shutdown")
	}
}

// With schema initialization off, the builder checks the schema instead.
func TestTheBuilderChecksTheSchemaWhenInitializationIsOff(t *testing.T) {
	store := newLifecycleStore(NewMemoryMessageStore())
	settings := defaultEventSettings()
	settings.storeInitSchema = false
	app := eventsApp(t, WithStore(store), withEventSettings(settings))
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	initCalls, checkCalls, _, _ := store.calls()
	is(t, initCalls, 0)
	is(t, checkCalls, 1)
}

// A schema that cannot be prepared fails Start, naming the store.
func TestASchemaFailureFailsStart(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare bool
		opts    func(*lifecycleStore)
		want    string
	}{
		{"init", true, func(s *lifecycleStore) { s.initErr = errors.New("no permission") }, "prepare its schema"},
		{"check", false, func(s *lifecycleStore) { s.checkErr = errors.New("no schema") }, "check its schema"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newLifecycleStore(NewMemoryMessageStore())
			test.opts(store)
			settings := defaultEventSettings()
			settings.storeInitSchema = test.prepare
			app := eventsApp(t, WithStore(store), withEventSettings(settings))
			err := app.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), store.Name()) {
				t.Fatalf("Start: %v", err)
			}
		})
	}
}

// An error from the store's background work is logged while the app runs; it does not fail the
// app.
func TestABackgroundErrorIsLogged(t *testing.T) {
	store := newLifecycleStore(NewMemoryMessageStore())
	store.bgErr = errors.New("the background work failed")
	recorder := &logRecorder{}
	app := eventsApp(t, WithStore(store), Logger(slog.New(recorder)))
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitUntil(t, "the background error in the log", func() bool {
		return recorder.count(slog.LevelError, "the message store's background work failed") == 1
	})
}

// An empty events source is refused.
func TestEventsSourceIsRequired(t *testing.T) {
	if _, err := New(Service("orders-service"), Hubs("127.0.0.1:1"), EventsSource(" ")); err == nil ||
		!strings.Contains(err.Error(), "EventsSource") {
		t.Fatalf("New: %v", err)
	}
	app := eventsApp(t)
	is(t, app.cfg.events.source, DefaultEventsSource)
}

// The message store is the only store with a lifecycle: an add-on that implements MessageStore
// alone is used as it is.
func TestAStoreWithoutALifecycleIsAccepted(t *testing.T) {
	store := silentStore{MessageStore: NewMemoryMessageStore()}
	app := eventsApp(t, WithStore(store), WithEventStore(NewMemoryEventStore()))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// The builder warns once that the in-memory stores are not durable.
func TestTheBuilderWarnsAboutInMemoryStores(t *testing.T) {
	recorder := &logRecorder{}
	app := eventsApp(t, WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()), Logger(slog.New(recorder)))
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	is(t, recorder.count(slog.LevelWarn, "the in-memory message store is not durable: everything it holds is gone when the process ends; use a database add-on in production"), 1)
	is(t, recorder.count(slog.LevelWarn, "the in-memory event store is not durable: everything it holds is gone when the process ends; use an event-store add-on in production"), 1)
}

// The background work gets at most five seconds and never more than the shutdown context has left.
func TestBackgroundStopBudget(t *testing.T) {
	is(t, backgroundStopBudget(context.Background()), eventsStopBudget)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	within(t, backgroundStopBudget(ctx), 900*time.Millisecond, time.Second)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if budget := backgroundStopBudget(expired); budget > 0 {
		t.Fatalf("an expired context left a budget of %v", budget)
	}
}
