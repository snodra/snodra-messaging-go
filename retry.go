package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/backoff"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// RetryPolicy is how a failed [Send] or [Query] is sent again: at most MaxAttempts attempts in
// all (1: no retry), the first wait InitialBackoff, each next one Multiplier times longer, at
// most MaxBackoff; every wait is jittered by ±20 % (never above MaxBackoff).
//
// Only failures where the handler never ran are retried for a command ([ErrNeverSent],
// NO_HANDLER, RESOURCE_EXHAUSTED); a query is also retried after an unknown outcome
// ([ErrOutcomeUnknown], TIMEOUT, HANDLER_DISCONNECTED). Every attempt has a fresh request id, and
// all attempts share one time budget: the caller's timeout, else the hub's timeout for the type,
// else 5 s. A retry starts only with at least max(InitialBackoff, 250 ms) of it left, and sends
// what is left as the request's timeout. See
// https://snodra.damoreira.com/library-authors/protocol/ (Never sent or outcome unknown).
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	Multiplier     float64
	MaxBackoff     time.Duration
}

// RetryNeverRan is the default policy: 3 attempts, 100 ms doubling up to 2 s. Retries are
// opt-in: pass it to [DefaultRetry] or [Retry] (or set SNODRA_SERVER_RETRY_ENABLED).
var RetryNeverRan = RetryPolicy{MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond, Multiplier: 2, MaxBackoff: 2 * time.Second}

// noRetry is one attempt.
var noRetry = RetryPolicy{MaxAttempts: 1, InitialBackoff: 100 * time.Millisecond, Multiplier: 2, MaxBackoff: 2 * time.Second}

// validate refuses a policy that cannot work.
func (p RetryPolicy) validate() error {
	switch {
	case p.MaxAttempts < 1:
		return fmt.Errorf("snodra: a retry policy needs MaxAttempts of at least 1 (1 = no retry), got %d", p.MaxAttempts)
	case p.MaxAttempts == 1:
		return nil // the waits are never used
	case math.IsNaN(p.Multiplier) || math.IsInf(p.Multiplier, 0) || p.Multiplier < 1:
		return fmt.Errorf("snodra: a retry policy needs a finite Multiplier of at least 1, got %v", p.Multiplier)
	case p.InitialBackoff < 0 || p.MaxBackoff < 0:
		return fmt.Errorf("snodra: a retry policy needs backoffs of 0 or more, got %v and %v", p.InitialBackoff, p.MaxBackoff)
	}
	return nil
}

// backoff is the wait after failed attempt n (1-based) before its jitter: InitialBackoff ×
// Multiplier^(n-1), at most MaxBackoff.
func (p RetryPolicy) backoff(n int) time.Duration {
	f := float64(p.InitialBackoff) * math.Pow(max(p.Multiplier, 1), float64(n-1))
	if f >= float64(p.MaxBackoff) {
		return p.MaxBackoff
	}
	return time.Duration(f)
}

// wait is the jittered wait after failed attempt n.
func (p RetryPolicy) wait(n int) time.Duration {
	return backoff.Jitter(p.backoff(n), p.MaxBackoff, rand.Float64())
}

// retryable reports whether a request of kind that failed with err may be sent again (with a
// fresh id): a command only when its handler never ran, a query also after an unknown outcome.
// A call whose context ended is never retried.
func retryable(kind hubpb.RequestKind, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrNeverSent) {
		return true
	}
	unknown := errors.Is(err, ErrOutcomeUnknown)
	var e *Error
	if errors.As(err, &e) {
		switch e.Code {
		case CodeNoHandler, CodeResourceExhausted:
			return true
		case CodeTimeout, CodeHandlerDisconnected:
			unknown = true
		}
	}
	return unknown && kind == hubpb.RequestKind_QUERY
}

// retryBudget is the time every attempt of a call shares: the caller's chosen timeout; else the
// hub's per-type override, else its default request timeout (from ack); else 5 s.
// TODO(spec-gap): SG-2 (the budget when the caller chose no timeout).
func retryBudget(ack *hubpb.RegisterAck, typ string, chosen time.Duration) time.Duration {
	if chosen > 0 {
		return chosen
	}
	if n := ack.GetTypeTimeoutsMs()[typ]; n > 0 {
		return time.Duration(n) * time.Millisecond
	}
	if n := ack.GetDefaultTimeoutMs(); n > 0 {
		return time.Duration(n) * time.Millisecond
	}
	return defaultBudget
}

// retrying runs attempt under policy: once with chosen when the policy makes one attempt, else
// within the call's budget, each attempt given what is left of it (at least 1 ms).
func retrying[T any](ctx context.Context, kind hubpb.RequestKind, policy RetryPolicy, budget time.Duration,
	chosen time.Duration, attempt func(timeout time.Duration) (T, error)) (T, error) {
	var zero T
	if policy.MaxAttempts <= 1 {
		return attempt(chosen)
	}
	start := time.Now()
	// TODO(spec-gap): SG-2 (no retry with less than max(initial backoff, 250 ms) left).
	minLeft := max(policy.InitialBackoff, retryMinRemaining)
	remaining := budget
	for n := 1; ; n++ {
		v, err := attempt(max(remaining, minTimeout))
		if err == nil {
			return v, nil
		}
		if n >= policy.MaxAttempts || !retryable(kind, err) {
			return zero, err
		}
		wait := policy.wait(n)
		left := budget - time.Since(start) - wait
		if left < minLeft {
			return zero, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return zero, fmt.Errorf("%w (waiting to retry after: %w)", ctx.Err(), err)
		}
		remaining = left
	}
}
