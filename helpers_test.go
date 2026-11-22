package snodramessaging

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

const ms, s = time.Millisecond, time.Second

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// within fails the test unless lo <= got <= hi.
func within(t testing.TB, got, lo, hi time.Duration) {
	t.Helper()
	if got < lo || got > hi {
		t.Fatalf("got %v, want between %v and %v", got, lo, hi)
	}
}

// ctxWithin is a context that ends after d, or with the test.
func ctxWithin(t testing.TB, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// scaled are the timings of the unit tests: the shipped ones, shortened.
func scaled() timings {
	t := defaultTimings()
	t.heartbeatBeforeAck = 100 * ms
	t.handshakeTimeout = 300 * ms
	t.backoffMin, t.backoffMax = 50*ms, 200*ms
	t.deregisterWait = 100 * ms
	t.closeWait = 200 * ms
	t.handlerDrain = 1 * s
	return t
}

// newApp builds an App as orders-service, instance i-1, on the scaled timings, followed by
// opts, and shuts it down when the test ends.
func newApp(t testing.TB, opts ...Option) *App {
	t.Helper()
	all := append([]Option{Service("orders-service"), Instance("i-1"), withTimings(scaled())}, opts...)
	app, err := New(all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*s)
		defer cancel()
		if err := app.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return app
}

// startApp is newApp, started.
func startApp(t testing.TB, opts ...Option) *App {
	t.Helper()
	app := newApp(t, opts...)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return app
}

// logRecorder is a slog handler keeping every record.
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}
func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

// count is the number of records at level whose message is msg.
func (r *logRecorder) count(level slog.Level, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.records {
		if rec.Level == level && rec.Message == msg {
			n++
		}
	}
	return n
}

// PlaceOrder is a message with a value receiver.
type PlaceOrder struct {
	SKU string `json:"sku"`
}

func (PlaceOrder) MessageType() string { return "orders.PlaceOrder" }

// CancelOrder is a message with a pointer receiver.
type CancelOrder struct {
	ID string `json:"id"`
}

func (*CancelOrder) MessageType() string { return "orders.CancelOrder" }
