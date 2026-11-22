package snodramessaging

import "time"

// timings are the library's clocks. The shipped values are those of
// https://snodra.damoreira.com/library-authors/protocol/ (Timing summary); the unit tests run on
// shortened ones through withTimings.
type timings struct {
	// heartbeatBeforeAck is the heartbeat period until the RegisterAck names the hub's.
	heartbeatBeforeAck time.Duration
	// handshakeTimeout bounds a stream without a RegisterAck: it is closed and opened again.
	handshakeTimeout time.Duration
	// silentIntervals heartbeat intervals without an echo drop an acknowledged stream.
	silentIntervals int
	// keepaliveTime and keepaliveTimeout are the HTTP/2 keepalive pings, also while idle.
	keepaliveTime, keepaliveTimeout time.Duration
	// backoffMin doubles up to backoffMax between attempts; back to backoffMin after an ack.
	backoffMin, backoffMax time.Duration
	// deregisterWait bounds the wait for the acks of the empty Register at shutdown.
	deregisterWait time.Duration
	// handlerDrain bounds the wait for the handlers in flight at shutdown.
	handlerDrain time.Duration
	// subscriptionEnd bounds the writing of the served subscriptions' ends at shutdown.
	subscriptionEnd time.Duration
	// closeWait bounds the wait for the hub to finish its side after the half-close.
	closeWait time.Duration
	// permitFlushAfter is how long a batch of permit grants waits after its first one.
	permitFlushAfter time.Duration
}

// permitBatch pending permit grants are written at once; it also caps the low-water mark
// min(permitBatch, initial_permits/2) at or below which a grant goes back at once.
const permitBatch uint32 = 16

// defaultTimings are the shipped values.
func defaultTimings() timings {
	return timings{
		heartbeatBeforeAck: 3 * time.Second,
		handshakeTimeout:   10 * time.Second,
		silentIntervals:    3,
		keepaliveTime:      10 * time.Second,
		keepaliveTimeout:   5 * time.Second,
		backoffMin:         500 * time.Millisecond,
		backoffMax:         5 * time.Second,
		deregisterWait:     1 * time.Second,
		handlerDrain:       10 * time.Second,
		subscriptionEnd:    2 * time.Second,
		closeWait:          2 * time.Second,
		permitFlushAfter:   50 * time.Millisecond,
	}
}

// withTimings replaces every timing (tests).
func withTimings(t timings) Option {
	return func(c *config) error {
		c.timings = t
		return nil
	}
}
