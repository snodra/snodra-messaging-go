package snodra

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A scripted event store for the re-subscribe waits: every Subscribe takes the next step, and one
// past the script never settles until the app stops.
type scriptStep struct {
	// deliver hands one handled event out before the subscription fails.
	deliver bool
	// stable moves the application's clock past the stable threshold before it fails.
	stable bool
}

type scriptedEventStore struct {
	*MemoryEventStore
	mu    sync.Mutex
	steps []scriptStep
	clock *atomic.Int64
}

func (s *scriptedEventStore) Subscribe(ctx context.Context, _ SubscriptionSpec) (EventSubscription, error) {
	s.mu.Lock()
	step, ok := scriptStep{}, false
	if len(s.steps) > 0 {
		step, ok = s.steps[0], true
		s.steps = s.steps[1:]
	}
	s.mu.Unlock()
	if !ok {
		return &blockingSubscription{}, nil
	}
	return &scriptedSubscription{step: step, clock: s.clock}, nil
}

type scriptedSubscription struct {
	step   scriptStep
	clock  *atomic.Int64
	served bool
}

func (s *scriptedSubscription) Next(ctx context.Context) (SubscriptionEvent, error) {
	if !s.served {
		s.served = true
		if s.step.deliver {
			return Delivery{
				Segment:  "0",
				Position: "1",
				Headers: map[string]string{
					HeaderType:          "orders.OrderPlaced",
					HeaderID:            "scripted",
					HeaderCorrelationID: "c-scripted",
				},
				Payload: []byte(`{"orderId":"o-1"}`),
			}, nil
		}
		if s.step.stable {
			s.clock.Add(int64(30 * time.Second))
		}
	}
	return nil, errors.New("the subscription failed for the test")
}

func (*scriptedSubscription) Ack(string, string) error       { return nil }
func (*scriptedSubscription) Redeliver(string, string) error { return nil }
func (*scriptedSubscription) Pause()                         {}
func (*scriptedSubscription) Resume()                        {}
func (*scriptedSubscription) Segments() []SegmentStatus      { return nil }
func (*scriptedSubscription) TakeError() string              { return "" }
func (*scriptedSubscription) Close(context.Context) error    { return nil }

// blockingSubscription is what follows a script: it ends when the app does.
type blockingSubscription struct{}

func (*blockingSubscription) Next(ctx context.Context) (SubscriptionEvent, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*blockingSubscription) Ack(string, string) error       { return nil }
func (*blockingSubscription) Redeliver(string, string) error { return nil }
func (*blockingSubscription) Pause()                         {}
func (*blockingSubscription) Resume()                        {}
func (*blockingSubscription) Segments() []SegmentStatus      { return nil }
func (*blockingSubscription) TakeError() string              { return "" }
func (*blockingSubscription) Close(context.Context) error    { return nil }

// runResubscribeScript runs a processor through script, recording the waits before each
// subscription; the clock is the script's own, so a stable step needs no real wait.
func runResubscribeScript(t *testing.T, script []scriptStep) []time.Duration {
	t.Helper()
	var mu sync.Mutex
	var waits []time.Duration
	settings := consumerSettings()
	settings.resubscribeMin = time.Second
	settings.resubscribeMax = 30 * time.Second
	settings.sleep = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return true
	}
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	events := &scriptedEventStore{MemoryEventStore: NewMemoryEventStore(), steps: script, clock: clock}
	app := eventsApp(t, WithStore(NewMemoryMessageStore()), WithEventStore(events),
		withEventSettings(settings), OutboxEnabled(false))
	app.now = func() time.Time { return time.Unix(0, clock.Load()) }
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitUntil(t, "the script's waits", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(waits) >= len(script)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]time.Duration(nil), waits[:len(script)]...)
}

// SG-35: a subscription that fails without delivering waits 1 s, then 2, 4, 8, 16, 30, 30; a
// delivery or a subscription that lived 30 s starts the wait over, and the next failure waits 1 s
// then doubles again.
func TestTheResubscribeWaitsDoubleAndReset(t *testing.T) {
	second := time.Second
	for _, test := range []struct {
		name   string
		script []scriptStep
		want   []time.Duration
	}{
		{
			name:   "assigned and failing each time",
			script: []scriptStep{{}, {}, {}, {}, {}, {}, {}},
			want:   []time.Duration{second, 2 * second, 4 * second, 8 * second, 16 * second, 30 * second, 30 * second},
		},
		{
			name:   "a delivery starts it over",
			script: []scriptStep{{}, {}, {deliver: true}, {}},
			want:   []time.Duration{second, 2 * second, second, 2 * second},
		},
		{
			name:   "a subscription that lived 30 s starts it over",
			script: []scriptStep{{stable: true}, {}},
			want:   []time.Duration{second, 2 * second},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := runResubscribeScript(t, test.script)
			if len(got) != len(test.want) {
				t.Fatalf("waits %v, want %v", got, test.want)
			}
			for i := range got {
				if got[i] != test.want[i] {
					t.Fatalf("waits %v, want %v", got, test.want)
				}
			}
		})
	}
}
