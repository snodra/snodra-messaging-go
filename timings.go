package snodra

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
	// certCheckInterval is how often the client certificate files are read for a renewal.
	certCheckInterval time.Duration
	// rollDelayMax bounds the random delay before a rollover to a renewed client certificate
	// (also bounded by a tenth of the presented certificate's remaining validity).
	rollDelayMax time.Duration
	// rollIdleLimit bounds the wait for a rolled hub's requests to finish before its stream is
	// half-closed.
	rollIdleLimit time.Duration
	// rollReconnectLimit bounds the wait for a rolled hub to register again with the renewed
	// certificate before the next hub is rolled.
	rollReconnectLimit time.Duration
	// tokenFetchWait bounds the wait for an access token: tokenFetchTimeout and a grace, for a
	// token source that ignores its context.
	tokenFetchWait time.Duration
	// creditWait bounds how long a streaming handler waits for credit before its stream ends
	// with TIMEOUT "no credit within ..." ([StreamCreditTimeout]).
	creditWait time.Duration
	// statusInterval is how often every live hub gets an InstanceStatus ([StatusInterval]).
	statusInterval time.Duration
	// dnsRefresh is how often a dns:/// target is resolved again.
	dnsRefresh time.Duration
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
	// defaultCredit is a stream's Request.initial_credit when the caller chose none (what the hub
	// reads 0 as).
	defaultCredit uint32 = 64
	// defaultMaxSubscriptions is how many subscriptions an instance serves at once when nothing
	// sets it ([MaxSubscriptions]): the hub's own default limit per instance.
	defaultMaxSubscriptions = 1024
)

// Several hubs (https://snodra.damoreira.com/library-authors/protocol/, Several hubs).
const (
	// maxUnwrittenMoves is how often one request may move to another hub because its stream was
	// lost before the request was written; past that the call fails with [ErrNeverSent].
	maxUnwrittenMoves = 8
	// dnsDrainLimit bounds how long a hub address that left the DNS answer stays open for the
	// requests of the instance in flight on it.
	dnsDrainLimit = 65 * time.Second
	// dnsDrainPoll is how often a hub address that left the DNS answer is checked for being idle.
	dnsDrainPoll = 100 * time.Millisecond
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

// The instance's own credentials towards the hubs
// (https://snodra.damoreira.com/library-authors/protocol/, Credentials).
const (
	// tokenFetchTimeout bounds one request to the OAuth2 token endpoint (one TokenSource call).
	tokenFetchTimeout = 10 * time.Second
	// defaultTokenLifetime is the lifetime of an access token whose expiry is unknown.
	defaultTokenLifetime = 60 * time.Second
	// minRefreshMargin is the least time before its expiry an access token is renewed.
	minRefreshMargin = 5 * time.Second
	// tokenRetryMin is the wait after a failed renewal, doubling up to tokenRetryMax; it is also
	// the least wait between two renewals.
	tokenRetryMin = 1 * time.Second
	tokenRetryMax = 30 * time.Second
	// tokenWakeInterval is how often, at least, the renewal looks at the clocks again: a
	// suspended machine stops the timers, not the wall clock.
	tokenWakeInterval = 5 * time.Second
	// rollSettle is the pause after a rolled hub's empty Register before its requests are counted.
	rollSettle = 50 * time.Millisecond
	// rollPoll is how often a rolled hub's requests are counted again.
	rollPoll = 10 * time.Millisecond
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
		handlerDrain:       5 * time.Second,
		subscriptionEnd:    2 * time.Second,
		closeWait:          2 * time.Second,
		permitFlushAfter:   50 * time.Millisecond,
		certCheckInterval:  3 * time.Second,
		rollDelayMax:       30 * time.Second,
		rollIdleLimit:      10 * time.Second,
		rollReconnectLimit: 30 * time.Second,
		tokenFetchWait:     tokenFetchTimeout + time.Second,
		creditWait:         10 * time.Minute,
		statusInterval:     5 * time.Second,
		dnsRefresh:         30 * time.Second,
	}
}

// withTimings replaces every timing (tests).
func withTimings(t timings) Option {
	return func(c *config) error {
		c.timings = t
		return nil
	}
}
