// Package backoff computes the jittered exponential waits of reconnects, retries and token
// refreshes: a wait doubles from a floor up to a cap, and each wait is spread by ±20 % without
// ever exceeding the cap. See https://snodra.damoreira.com/docs/library-authors/protocol/ (Reconnect
// and backoff).
package backoff

import "time"

// Next is the wait that follows prev: twice prev, at least minWait and at most maxWait.
func Next(prev, minWait, maxWait time.Duration) time.Duration {
	next := 2 * prev
	if next < prev || next > maxWait { // next < prev: overflow
		next = maxWait
	}
	return max(next, minWait)
}

// Jitter spreads base by ±20 %: base × (0.8 + 0.4 × unit), never above maxWait, for a unit in
// [0, 1] (clamped). At the cap (base >= maxWait) it spreads downward only, over
// [0.8, 1.0] × maxWait, so that a fleet waiting at the cap does not land on exactly the maximum
// together.
func Jitter(base, maxWait time.Duration, unit float64) time.Duration {
	unit = min(max(unit, 0), 1)
	if base >= maxWait {
		return time.Duration(float64(maxWait) * (0.8 + 0.2*unit))
	}
	return min(time.Duration(float64(base)*(0.8+0.4*unit)), maxWait)
}
