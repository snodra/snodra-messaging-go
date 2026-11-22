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

	defaultTimeout time.Duration    // the sends' timeout when a call chooses none; 0: none
	retry          *RetryPolicy     // the sends' retry policy; nil: no retry
	priorities     map[string]int32 // Request.priority by type when a call chooses none

	jwksURL, issuer string // the callers' tokens are verified against them; "": read unverified
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

// Format prints the instance's identity and hubs whatever the verb, never a credential.
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
// https://snodra.damoreira.com/library-authors/protocol/ (Several hubs). Required (or
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
// hubs it is connected to, its handlers in flight). The default is 5 s, which is what the
// protocol asks for at most; it must be at least 1 ms.
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
// a refusal, so keep max between 1.25 s and 5 s; as in the reference libraries, values outside
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
// (https://snodra.damoreira.com/library-authors/messages/): change it only for a codec that
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

// DefaultTimeout is the timeout of every [Send] and [Query] that sets none with [Timeout]
// (sent as Request.timeout_ms). The default is none (0): the hub applies its per-type override or
// its default request timeout, which is what the protocol asks for.
func DefaultTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("snodra: DefaultTimeout must not be negative, got %v", d)
		}
		c.defaultTimeout = d
		return nil
	}
}

// DefaultRetry is the retry policy of every [Send] and [Query] that sets none with [Retry] or
// [NoRetry]. The default is no retry; [RetryNeverRan] is the usual policy.
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
// TODO(spec-gap): SG-16 (a maximum above 5 s, or below 1.25 s, is accepted as by the reference libraries).
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
	return c.validateCredentials()
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
