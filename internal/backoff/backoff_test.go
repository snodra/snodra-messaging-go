package backoff_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/backoff"
)

const ms, s = time.Millisecond, time.Second

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestBackoff(t *testing.T) {
	seq := []time.Duration{500 * ms}
	for i := 0; i < 5; i++ {
		seq = append(seq, backoff.Next(seq[len(seq)-1], 500*ms, 5*s))
	}
	is(t, fmt.Sprint(seq), fmt.Sprint([]time.Duration{500 * ms, 1 * s, 2 * s, 4 * s, 5 * s, 5 * s}))
	is(t, backoff.Jitter(1*s, 5*s, 0), 800*ms)
	is(t, backoff.Jitter(1*s, 5*s, 1), 1200*ms)
	is(t, backoff.Jitter(5*s, 5*s, 0), 4*s)
	is(t, backoff.Jitter(4500*ms, 5*s, 1), 5*s) // never above the max
}

func TestJitterAtTheCapStaysWithinEightyToOneHundredPercent(t *testing.T) {
	is(t, backoff.Jitter(5*s, 5*s, 0.5), 4500*ms) // spread over [0.8, 1.0] x max, not clamped
	is(t, backoff.Jitter(5*s, 5*s, 1), 5*s)
	is(t, backoff.Jitter(1*s, 5*s, 0.5), 1*s)
	is(t, backoff.Jitter(1*s, 5*s, -1), 800*ms) // the unit is clamped to [0, 1]
	is(t, backoff.Jitter(1*s, 5*s, 2), 1200*ms)
}

func TestNextStartsFromTheFloor(t *testing.T) {
	is(t, backoff.Next(0, 500*ms, 5*s), 500*ms)
	is(t, backoff.Next(300*ms, 500*ms, 5*s), 600*ms)
	is(t, backoff.Next(6*s, 500*ms, 5*s), 5*s)
}
