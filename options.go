package snodra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// config is what the options set; New validates it.
type config struct {
	service          string
	instance         string
	hubs             []string // host:port, in order, without repeats; or one dns:///name:port
	resolve          resolver // resolves a dns:/// target
	maxConcurrent    int
	maxSubscriptions int // the subscriptions served at once ([MaxSubscriptions])
	loadFactor       uint32
	tags             map[string]string
	logger           *slog.Logger
	codec            PayloadCodec
	timings          timings
	telemetry        Instrumentation // nil: nothing is reported

	defaultTimeout time.Duration    // the sends' timeout when a call chooses none; 0: none
	retry          *RetryPolicy     // the sends' retry policy; nil: no retry
	priorities     map[string]int32 // Request.priority by type when a call chooses none
	// alwaysRouteThroughHub sends every request through a hub, also those of a type this instance
	// handles itself ([AlwaysRouteThroughHub]).
	alwaysRouteThroughHub bool

	// The message store and the event store of the service, both optional: only events need
	// them.
	store      MessageStore
	eventStore EventStore
	// events is the event side's settings: source, relay, consumer and store schema.
	events eventSettings

	jwksURL, issuer string // the callers' tokens are verified against them; "": no token is an identity
	rolesClaim      string // where a token's roles are; "": roles
	forwardJWT      bool   // nested requests carry the handled request's authorization header

	// The instance's own credential towards the hubs: one kind (credentials.go, tls.go).
	token                 *secret     // Register.credentials.token; nil: none
	tokenFile             string      // the token's file, read before every connect; wins over token
	tokenSource           TokenSource // Register.credentials.jwt from it; nil: none
	tls                   *TLSConfig  // TLS to the hubs; nil: plain text
	clientCert, clientKey string      // the client certificate pair (mutual TLS)

	// writeHook, when set (tests), runs before a stream's writer takes each frame from the
	// outbound buffer; ctx ends with the stream.
	writeHook func(ctx context.Context, m *hubpb.ClientMessage)
}

// Format prints the instance's identity and hubs for every verb that reaches a Formatter
// (not %p or %w), never a credential.
func (c config) Format(f fmt.State, _ rune) {
	_, _ = fmt.Fprintf(f, "config{service: %s, instance: %s, hubs: %v}", c.service, c.instance, c.hubs)
}

func defaultConfig() config {
	return config{
		maxConcurrent:    256,
		maxSubscriptions: defaultMaxSubscriptions,
		loadFactor:       100,
		codec:            JSON,
		timings:          defaultTimings(),
		events:           defaultEventSettings(),
		forwardJWT:       true,
		resolve:          lookupTargets,
	}
}

// An Option configures an [App] in [New]. Options apply in order: a later one overrides an
// earlier one, [FromEnv] included.
type Option func(*config) error

// Service is the logical service name the instance registers (Register.service_name), for
// example "orders-service". Required (or SNODRA_SERVICE_NAME with [FromEnv]).
func Service(name string) Option {
	return func(c *config) error {
		c.service = strings.TrimSpace(name)
		return nil
	}
}

// Instance is the instance id (Register.instance_id), unique per process, for example a pod
// name. The default is a fresh random UUID.
func Instance(id string) Option {
	return func(c *config) error {
		id = strings.TrimSpace(id)
		if id == "" {
			return errors.New("snodra: Instance needs a non-empty id")
		}
		c.instance = id
		return nil
	}
}

// Hubs are the hubs to connect to: each "host:port" (a "static://", "http://" or "https://"
// prefix is accepted; an "https://" hub is reached over TLS with the system's roots even
// without [TLS]), or one "dns:///name:port" whose every address is a hub, resolved again every
// 30 s (an address that leaves the answer is drained and closed, a new one connected). The
// instance opens one stream to every hub, each with the identical registration, and knows a hub
// by its RegisterAck.hub_id: a second address answering an id already served is closed and
// parked until that hub has no other address. See
// https://snodra.damoreira.com/docs/library-authors/protocol/ (Several hubs). Required (or
// SNODRA_SERVER_ADDRESS with [FromEnv]).
func Hubs(addresses ...string) Option {
	return func(c *config) error {
		targets, err := parseTargets(addresses)
		if err != nil {
			return err
		}
		c.hubs = targetStrings(targets)
		return nil
	}
}

// targetStrings are the targets as configured.
func targetStrings(targets []target) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = t.String()
	}
	return out
}

// StatusInterval is how often every hub with an acknowledged stream gets an InstanceStatus (the
// hubs it is connected to, its handlers in flight); one also goes out whenever the set of
// acknowledged hubs changes. The default is 5 s, which is what the protocol asks for at most; it
// must be at least 1 ms.
func StatusInterval(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Millisecond {
			return fmt.Errorf("snodra: StatusInterval must be at least 1ms, got %v", d)
		}
		c.timings.statusInterval = d
		return nil
	}
}

// MaxConcurrent is how many handlers the instance runs at once (the permits it offers the
// hubs). The default is 256.
func MaxConcurrent(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("snodra: MaxConcurrent must be at least 1 (the hub refuses a Register without permits), got %d", n)
		}
		c.maxConcurrent = n
		return nil
	}
}

// ReconnectBackoff is the wait between two attempts to reach a hub: min, doubling up to max,
// each wait jittered by ±20 % (never above max). A refused Register waits 80 to 100 % of max.
// The default is 500 ms to 5 s. The specification caps max at 5 s and wants at least 1 s after
// a refusal, so keep max between 1.25 s and 5 s; values outside
// that range are accepted (see SPEC-GAPS.md, SG-16).
func ReconnectBackoff(min, max time.Duration) Option {
	return func(c *config) error {
		if err := checkBackoff(min, max, "ReconnectBackoff"); err != nil {
			return err
		}
		c.timings.backoffMin, c.timings.backoffMax = min, max
		return nil
	}
}

// LoadFactor is the instance's relative capacity (Register.load_factor); 0 reads as 100, the
// default.
func LoadFactor(n uint32) Option {
	return func(c *config) error {
		c.loadFactor = n
		return nil
	}
}

// Tags are free labels the instance registers (Register.tags); unkeyed requests prefer
// instances whose tags match more of the sender's.
func Tags(tags map[string]string) Option {
	return func(c *config) error {
		c.tags = maps.Clone(tags)
		return nil
	}
}

// Codec is the payload codec of every handler and send; the default is [JSON]. Every library
// of the suite reads and writes JSON payloads
// (https://snodra.damoreira.com/docs/library-authors/messages/): change it only for a codec that
// writes the same JSON.
func Codec(c PayloadCodec) Option {
	return func(cfg *config) error {
		if c == nil {
			return errors.New("snodra: Codec needs a codec")
		}
		cfg.codec = c
		return nil
	}
}

// DefaultTimeout is the timeout of every call ([Send], [Query], [Gather], [Stream], [Subscribe])
// that sets none with [Timeout] (sent as Request.timeout_ms; for a [Stream], the longest wait for
// its next part). The default is none (0): the hub applies its per-type override or its default
// request timeout, which is what the protocol asks for.
func DefaultTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("snodra: DefaultTimeout must not be negative, got %v", d)
		}
		c.defaultTimeout = d
		return nil
	}
}

// DefaultRetry is the retry policy of every call ([Send], [Query], [Gather], [Stream],
// [Subscribe]) that sets none with [Retry] or [NoRetry]. The default is no retry; [RetryNeverRan]
// is the usual policy.
func DefaultRetry(p RetryPolicy) Option {
	return func(c *config) error {
		if err := p.validate(); err != nil {
			return err
		}
		c.retry = &p
		return nil
	}
}

// Priorities are the Request.priority of the sends of each type that set none with [Priority]
// (higher is served first while the target's queue at the hub is not empty); other types send 0.
func Priorities(p map[string]int32) Option {
	return func(c *config) error {
		c.priorities = maps.Clone(p)
		return nil
	}
}

// AlwaysRouteThroughHub, when on, sends every [Send] and [Query] through a hub, also those of a
// type this instance handles itself (SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB with [FromEnv]). Off,
// the default, a command, or a query that is not [Shared], of a type registered here is answered
// in-process: its handler runs on the caller's goroutine, without a hub, a permit or a handler
// slot (see [Send]). Turn it on where every request must be seen by the hubs (routing-key
// affinity across instances, a conformance or an interoperability test). [Gather], [Stream] and
// [Subscribe] always go through a hub. The library has no mode without hubs, so the option
// always applies.
func AlwaysRouteThroughHub(on bool) Option {
	return func(c *config) error {
		c.alwaysRouteThroughHub = on
		return nil
	}
}

// WithStore sets the message store: the service's own database, where the outbox, the
// processed-event markers, the dead letters, the processors' control state, the idempotent calls
// and the sagas live. A store is the add-on of one database ([MessageStore]); the in-memory one is
// [NewMemoryMessageStore]. Events need it; commands, queries and subscription queries do not, and
// a service with only those starts without it. See
// https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/.
func WithStore(store MessageStore) Option {
	return func(c *config) error {
		if store == nil {
			return errors.New("snodra: WithStore needs a message store")
		}
		c.store = store
		return nil
	}
}

// WithEventStore sets the event store: where the relay publishes the service's events and where
// its processors read them ([EventStore]). The in-memory one is [NewMemoryEventStore]. Events need
// it; commands, queries and subscription queries do not, and a service with only those starts
// without it. See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/.
func WithEventStore(store EventStore) Option {
	return func(c *config) error {
		if store == nil {
			return errors.New("snodra: WithEventStore needs an event store")
		}
		c.eventStore = store
		return nil
	}
}

// EventsSource is the event store's source the service publishes its events to and its processors
// read (SNODRA_EVENTS_SOURCE). The default is [DefaultEventsSource], "snodra.events".
func EventsSource(source string) Option {
	return func(c *config) error {
		source = strings.TrimSpace(source)
		if source == "" {
			return errors.New("snodra: EventsSource needs a non-empty source")
		}
		c.events.source = source
		return nil
	}
}

// OutboxEnabled says whether every instance runs the outbox relay (SNODRA_OUTBOX_ENABLED). On by
// default. Turning it off on every instance stops the service's events being published; turning it
// off on all but one is how a service keeps the order per key of what one instance publishes (see
// https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-rule).
func OutboxEnabled(on bool) Option {
	return func(c *config) error {
		c.events.outboxEnabled = on
		return nil
	}
}

// ConsumerEnabled says whether the instances read their event processors' subscriptions
// (SNODRA_CONSUMER_ENABLED). On by default; a tool that only publishes turns it off.
func ConsumerEnabled(on bool) Option {
	return func(c *config) error {
		c.events.consumerEnabled = on
		return nil
	}
}

// withEventSettings replaces the event side's settings (tests).
func withEventSettings(settings eventSettings) Option {
	return func(c *config) error {
		c.events = settings
		return nil
	}
}

// withWriteHook sets the writer's hook (tests).
func withWriteHook(h func(ctx context.Context, m *hubpb.ClientMessage)) Option {
	return func(c *config) error {
		c.writeHook = h
		return nil
	}
}

// Logger is where the library logs; the default is [slog.Default].
func Logger(l *slog.Logger) Option {
	return func(c *config) error {
		c.logger = l
		return nil
	}
}

// checkBackoff refuses what cannot work: a minimum of 0 or less, a maximum below the minimum.
// TODO(spec-gap): SG-16 (a maximum above 5 s, or below 1.25 s, is accepted).
func checkBackoff(min, max time.Duration, setting string) error {
	if min <= 0 {
		return fmt.Errorf("snodra: the reconnect backoff minimum (%s) must be positive, got %v", setting, min)
	}
	if max < min {
		return fmt.Errorf("snodra: the reconnect backoff maximum (%s, %v) must not be below its minimum (%v)", setting, max, min)
	}
	return nil
}

// validate checks the settings once every option applied, and fills the defaults that depend
// on nothing else.
func (c *config) validate() error {
	if c.service == "" {
		return errors.New("snodra: no service name: set it with Service or SNODRA_SERVICE_NAME")
	}
	if len(c.hubs) == 0 {
		return errors.New("snodra: no hub: set it with Hubs or SNODRA_SERVER_ADDRESS")
	}
	if c.maxConcurrent > math.MaxUint32 {
		return fmt.Errorf("snodra: MaxConcurrent %d is too large", c.maxConcurrent)
	}
	if err := checkBackoff(c.timings.backoffMin, c.timings.backoffMax,
		"ReconnectBackoff, SNODRA_RECONNECT_BACKOFF_MIN_MS / SNODRA_RECONNECT_BACKOFF_MAX_MS"); err != nil {
		return err
	}
	if c.jwksURL != "" && c.issuer == "" {
		return errors.New("snodra: the issuer is required with the JWKS URL: set it with JWKS or SNODRA_ISSUER")
	}
	if c.instance == "" {
		c.instance = uuid.New()
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if err := c.validateEvents(); err != nil {
		return err
	}
	return c.validateCredentials()
}

// validateEvents checks the event side's settings: a source, a poll interval, a batch, the
// attempts, and a lease longer than one send (a batch stops a send timeout before its lease ends,
// so an entry is never claimable by another instance while this one still sends it).
func (c *config) validateEvents() error {
	if strings.TrimSpace(c.events.source) == "" {
		return errors.New("snodra: no events source: set it with EventsSource or SNODRA_EVENTS_SOURCE")
	}
	if c.events.poll < time.Millisecond {
		return fmt.Errorf("snodra: the outbox poll interval must be at least 1ms, got %v", c.events.poll)
	}
	if c.events.batch < 1 {
		return fmt.Errorf("snodra: the outbox batch size must be at least 1, got %d", c.events.batch)
	}
	if c.events.outboxAttempts < 1 {
		return fmt.Errorf("snodra: the outbox must try an entry at least once, got %d", c.events.outboxAttempts)
	}
	if c.events.sendTimeout < time.Millisecond {
		return fmt.Errorf("snodra: the outbox send timeout must be at least 1ms, got %v", c.events.sendTimeout)
	}
	if c.events.lease <= c.events.sendTimeout {
		return fmt.Errorf("snodra: the outbox lease (%v, SNODRA_OUTBOX_LEASE_MS) must be longer than a send (%v)",
			c.events.lease, c.events.sendTimeout)
	}
	if c.events.consumerTries < 1 {
		return fmt.Errorf("snodra: the consumer must try a record at least once, got %d", c.events.consumerTries)
	}
	if c.events.consumerBackoff < 0 {
		return fmt.Errorf("snodra: the consumer's backoff must not be negative, got %v", c.events.consumerBackoff)
	}
	if c.events.resubscribeMin < time.Millisecond || c.events.resubscribeMax < c.events.resubscribeMin {
		return fmt.Errorf("snodra: the re-subscribe backoff must be at least 1ms and not above its maximum (got %v and %v)",
			c.events.resubscribeMin, c.events.resubscribeMax)
	}
	switch c.events.deadLetterMode {
	case DeadLetterModeCollection, DeadLetterModeLog:
	case DeadLetterModeEventStore:
		// The mode that sends a record to the event store's dead-letter destination arrives with
		// that sender; refusing it here means no configuration can silently lose records.
		return fmt.Errorf("snodra: the event-store dead-letter mode is not available yet: use collection (the default)")
	default:
		return fmt.Errorf("snodra: an unknown dead-letter mode %q (collection or log)",
			c.events.deadLetterMode)
	}
	if c.events.deadLetterMode == DeadLetterModeCollection && c.events.deadLetterRetention < time.Second {
		return fmt.Errorf("snodra: the dead-letter retention must be at least 1s, got %v",
			c.events.deadLetterRetention)
	}
	if c.events.sleep == nil {
		c.events.sleep = sleepCtx
	}
	return nil
}

// validateCredentials checks the instance's credential towards the hubs: one kind, a token file
// that holds a token, a token source's settings, a CA file that holds a certificate, and a
// client certificate pair the hub would accept now.
func (c *config) validateCredentials() error {
	var kinds []string
	switch {
	case c.tokenFile != "":
		kinds = append(kinds, "a static token (TokenFile, SNODRA_SERVER_TOKEN_FILE)")
	case c.token != nil:
		kinds = append(kinds, "a static token (Token, SNODRA_SERVER_TOKEN)")
	}
	if c.tokenSource != nil {
		kinds = append(kinds, "a JWT (JWT, SNODRA_SERVER_OAUTH2_TOKEN_URI)")
	}
	if c.clientCert != "" || c.clientKey != "" {
		kinds = append(kinds, "a client certificate (ClientCertificate, SNODRA_SERVER_TLS_CLIENT_CERT)")
	}
	if len(kinds) > 1 {
		return fmt.Errorf("snodra: %s and %s are both set: use one credential", kinds[0], kinds[1])
	}
	if c.tokenFile != "" {
		// TODO(spec-gap): SG-20 (a token file that cannot be read or is empty at start is refused).
		tok, err := readTokenFile(c.tokenFile)
		if err != nil {
			return fmt.Errorf("snodra: the token file (TokenFile, SNODRA_SERVER_TOKEN_FILE) '%s' cannot be read: %w", c.tokenFile, err)
		}
		if tok == "" {
			return fmt.Errorf("snodra: the token file (TokenFile, SNODRA_SERVER_TOKEN_FILE) '%s' is empty", c.tokenFile)
		}
	}
	if cc, ok := c.tokenSource.(*clientCredentials); ok {
		checked, err := cc.prepared(c.service)
		if err != nil {
			return err
		}
		if checked.plainHTTPOverANetwork() {
			c.logger.Warn("the OAuth2 token endpoint is plain http over a routed network: the client secret and the tokens travel unencrypted; use https")
		}
		c.tokenSource = checked
	}
	if c.clientCert != "" || c.clientKey != "" {
		if c.clientCert == "" || c.clientKey == "" {
			return errors.New("snodra: a client certificate needs both files (ClientCertificate, SNODRA_SERVER_TLS_CLIENT_CERT and SNODRA_SERVER_TLS_CLIENT_KEY)")
		}
		if c.tls == nil {
			c.tls = &TLSConfig{} // a client certificate implies TLS
		}
		id, err := loadClientIdentity(c.clientCert, c.clientKey, c.service)
		if err == nil {
			err = id.validAt(time.Now())
		}
		if err != nil {
			return fmt.Errorf("snodra: %w", err)
		}
	}
	if c.tls != nil && c.tls.CAFile != "" {
		if _, err := readCAFile(c.tls.CAFile); err != nil {
			return fmt.Errorf("snodra: the hub CA file (TLSConfig.CAFile, SNODRA_SERVER_TLS_CA) '%s' %w", c.tls.CAFile, err)
		}
	}
	return nil
}
