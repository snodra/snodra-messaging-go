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

// The sender's clocks (https://snodra.damoreira.com/library-authors/protocol/, Timeouts and
// Never sent or outcome unknown).
const (
	// guardMargin is added to the hub's bound for a request: the local guard gives up only after
	// it, so the hub's own TIMEOUT normally arrives first.
	guardMargin = 1 * time.Second
	// fallbackMaxTimeout is the hub's maximum for an ack that names none (a hub always does).
	fallbackMaxTimeout = 60 * time.Second
	// defaultBudget is the time the attempts of a retried call share when neither the caller nor
	// the hub's ack gives one (no hub acknowledged yet).
	defaultBudget = 5 * time.Second
	// retryMinRemaining is the least of a call's budget a retry must have left after its wait
	// (or the policy's initial backoff, when larger).
	retryMinRemaining = 250 * time.Millisecond
	// minTimeout is the smallest Request.timeout_ms a chosen timeout is sent as (0 would mean
	// "the hub decides").
	minTimeout = time.Millisecond
	// outboundLimit is how many requests may wait in a stream's outbound buffer; a send waits for
	// room beyond it (the library's own frames never wait).
	outboundLimit = 256
)

// The checking of the callers' tokens (https://snodra.damoreira.com/library-authors/messages/,
// Forwarded identity).
const (
	// jwksMinRefresh is how often, at most, a token with a key id the cached JWKS does not know
	// fetches the JWKS again.
	jwksMinRefresh = 60 * time.Second
	// jwksFetchTimeout bounds a JWKS fetch.
	jwksFetchTimeout = 5 * time.Second
)

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
