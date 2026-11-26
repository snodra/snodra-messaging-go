// Package rabbitmq is the RabbitMQ Streams event store add-on: the core's snodra.EventStore on
// RabbitMQ 3.13 or later with the stream plugin.
//
// A source is a super stream (`snodra.events`, partition streams `snodra.events-0` to `-N`,
// created by the infrastructure); a source that is a plain stream (the dead-letter stream
// `snodra.events.dlt`) is read and written as one partition. A segment is the partition stream's
// suffix after `<source>-` (`3`), `0` for a plain stream; a position is the offset in decimal.
// A subscription is a single active consumer named `<service>.<processor>` on every partition,
// with the `snodra-member` property telling its instances apart to the management API. The
// stored offset is the next offset to read.
//
// The layout on the wire is the one every official Snodra library writes
// (https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules):
//
//   - a message is one AMQP 1.0 message per event, published with protocol version 1, no
//     sub-entry batching and no compression: exactly one data section (the JSON payload), the
//     event id as the string message-id, `application/json` as the content type, the key as the
//     symbol-keyed message annotation `x-routing-key` (absent when the event has none), and
//     every header as an application property with a string value;
//   - the partition is the stream clients' hash routing: murmur3 x86_32 of the routing key's
//     UTF-8 bytes (the event's key, else its id), seed 104729, taken unsigned, modulo the
//     partitions in the broker's order. The pinned values for 6 partitions are `order-1` -> 0,
//     `order-2` -> 2, `customer-123` -> 5, `order-7` -> 3 (a signed modulo gets it wrong) and
//     `""` -> 4;
//   - a processor's subscription is the consumer name `<service>.<processor>` on every
//     partition, with `single-active-consumer=true`, `name`, `super-stream=<source>` (absent on
//     a plain stream) and `snodra-member=<a random id per instance>`;
//   - the offset stored per partition under the consumer name is the next offset to read (the
//     acknowledged offset plus one), stored every 10 000 acknowledgements or 5 s and always on
//     deactivation and close; a consumer with none starts at the first offset, and on activation
//     it answers the broker's consumer update with the stored offset;
//   - a dead letter goes to the plain stream `<source>.dlt`.
//
// The store declares `lag` and `native-dead-letter` always, and `replay` only when the
// management API is configured (only it lists the instances reading a subscription, which a
// reset needs). It has no event browser and declares no `browse`. It never creates or changes
// streams: the infrastructure does.
//
// # The client spike
//
// TODO(spec-gap): SG-43 (the official Go client cannot set snodra-member; this add-on speaks
// the low-level protocol itself).
//
// The official Go client (`rabbitmq-stream-go-client` v1.10.0) exposes no low-level client and
// hard-codes the subscribe properties of its consumers (`name`, `single-active-consumer`,
// `super-stream`, `filter.*`), so it cannot set `snodra-member`, and the wire rules require it.
// This add-on therefore speaks the stream protocol's low-level commands itself
// (`internal/streamwire`) and keeps the official client for AMQP 1.0 message encoding
// (`pkg/amqp`); the tests use its high-level API to create streams.
//
// The type is safe for concurrent use. Every subscription owns its connections; Close releases
// the publisher's connections and does not close the subscriptions (the caller closes them).
package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/rabbitmq/internal/streamwire"
)

// Name is the name this add-on reports as the processors' event store.
const Name = "rabbitmq-streams"

// storeConnection is the connection name the store's connections carry.
const storeConnection = "snodra-messaging"

// The add-on's own bounds and timeouts. A Config may replace each of them.
const (
	// DefaultPort is the stream protocol's port (5552; 5551 with TLS).
	DefaultPort = 5552
	// DefaultTLSPort is the stream protocol's TLS port.
	DefaultTLSPort = 5551
	// DefaultConfirmTimeout is the longest a publish waits for the broker's confirm.
	DefaultConfirmTimeout = 30 * time.Second
	// DefaultRequestTimeout is the longest one stream request waits.
	DefaultRequestTimeout = 10 * time.Second
	// DefaultOffsetStoreMessages is how many acknowledgements a subscription stores its offset
	// after, the Java client's automatic tracking.
	DefaultOffsetStoreMessages = 10_000
	// DefaultOffsetStoreInterval is how often it stores its offset, whichever comes first.
	DefaultOffsetStoreInterval = 5 * time.Second
	// DefaultManagementTimeout is the longest one management API request waits.
	DefaultManagementTimeout = 5 * time.Second
)

// Config is how the store reaches RabbitMQ.
type Config struct {
	// Hosts are the nodes to connect to, `host` or `host:port`, tried in order; the default is
	// `rabbitmq`.
	Hosts []string
	// Port is the port of a host given without one (default 5552, 5551 with TLS).
	Port int
	// VirtualHost is the vhost (default "/").
	VirtualHost string
	// Username and Password authenticate (default "guest").
	Username string
	Password string
	// TLS connects with TLS; nil connects in the clear.
	TLS *tls.Config
	// ManagementURL is the management API's root (`http://rabbitmq:15672`, no credentials in
	// it). Setting it declares `replay`; Open asks it once. An https URL is verified against
	// the system roots: there is no separate management CA setting here (the stream
	// connection's TLS configuration is Config.TLS).
	ManagementURL string
	// ManagementUsername and ManagementPassword authenticate to it (default: the stream user's).
	ManagementUsername string
	ManagementPassword string
	// ManagementTimeout is the longest one management API request waits (default 5 s).
	ManagementTimeout time.Duration
	// ConfirmTimeout is the longest a publish waits for its confirm (default 30 s).
	ConfirmTimeout time.Duration
	// RequestTimeout is the longest one stream request waits (default 10 s).
	RequestTimeout time.Duration
	// OffsetStoreMessages is how many acknowledgements a subscription stores its offset after
	// (default 10 000).
	OffsetStoreMessages uint64
	// OffsetStoreInterval is how often it stores its offset, whichever comes first (default
	// 5 s).
	OffsetStoreInterval time.Duration
}

// FromEnv returns a Config from the environment: SNODRA_RABBITMQ_HOSTS (`host[:port]` entries,
// comma separated, default `rabbitmq`), SNODRA_RABBITMQ_PORT, SNODRA_RABBITMQ_VIRTUAL_HOST,
// SNODRA_RABBITMQ_USERNAME, SNODRA_RABBITMQ_PASSWORD, SNODRA_RABBITMQ_TLS (`true`) and
// SNODRA_RABBITMQ_MANAGEMENT_URL. The other fields keep their defaults; set them on the returned
// value.
func FromEnv() Config {
	config := Config{
		VirtualHost:   os.Getenv("SNODRA_RABBITMQ_VIRTUAL_HOST"),
		Username:      os.Getenv("SNODRA_RABBITMQ_USERNAME"),
		Password:      os.Getenv("SNODRA_RABBITMQ_PASSWORD"),
		ManagementURL: os.Getenv("SNODRA_RABBITMQ_MANAGEMENT_URL"),
	}
	for _, host := range strings.Split(os.Getenv("SNODRA_RABBITMQ_HOSTS"), ",") {
		if host = strings.TrimSpace(host); host != "" {
			config.Hosts = append(config.Hosts, host)
		}
	}
	if len(config.Hosts) == 0 {
		config.Hosts = []string{"rabbitmq"}
	}
	if port, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SNODRA_RABBITMQ_PORT"))); err == nil && port > 0 {
		config.Port = port
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("SNODRA_RABBITMQ_TLS")), "true") {
		config.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return config
}

// EventStore is the RabbitMQ Streams event store. Construct it with New or Open.
type EventStore struct {
	config     Config
	management *management
	publisher  *publisher
	live       *liveRegistry
	closed     atomic.Bool
}

// New creates the store without connecting (nothing connects before the first call). It
// validates the configuration and declares `replay` when a management API is configured; Open
// asks the API whether it can tell who reads a subscription.
//
// An empty host list, a host that is not `host[:port]`, a port outside 1 to 65535, a management
// URL that is not http(s) or carries credentials, or a zero bound fail with snodra.ErrInvalid.
func New(config Config) (*EventStore, error) {
	if err := validateConfig(&config); err != nil {
		return nil, err
	}
	managementAPI, err := newManagement(config)
	if err != nil {
		return nil, err
	}
	store := &EventStore{
		config:     config,
		management: managementAPI,
		live:       newLiveRegistry(),
	}
	store.publisher = newPublisher(store)
	return store, nil
}

// Open is New, then, with a management API, one ask whether its metrics collection is on:
//
// TODO(spec-gap): SG-47 (no page says whether disabled metrics is a startup failure or a
// store without replay).
// without metrics its consumer list answers nothing, so `replay` is not declared. A management
// API that does not answer fails with snodra.ErrFailed: the setting is one URL, so a store that
// declares `replay` cannot start while that node is down.
func Open(ctx context.Context, config Config) (*EventStore, error) {
	store, err := New(config)
	if err != nil {
		return nil, err
	}
	if store.management != nil {
		enabled, err := store.management.statsEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if !enabled {
			store.management = nil
		}
	}
	return store, nil
}

func validateConfig(config *Config) error {
	if len(config.Hosts) == 0 {
		config.Hosts = []string{"rabbitmq"}
	}
	for _, host := range config.Hosts {
		if strings.TrimSpace(host) == "" {
			return fmt.Errorf("%w: a RabbitMQ host is empty", snodra.ErrInvalid)
		}
		if _, _, err := net.SplitHostPort(host); err == nil {
			continue
		}
		if strings.Contains(host, ":") {
			return fmt.Errorf("%w: the RabbitMQ host %q is not host or host:port", snodra.ErrInvalid, host)
		}
	}
	if config.Port == 0 {
		if config.TLS != nil {
			config.Port = DefaultTLSPort
		} else {
			config.Port = DefaultPort
		}
	}
	if config.Port < 1 || config.Port > 65535 {
		return fmt.Errorf("%w: the RabbitMQ port %d is outside 1 to 65535", snodra.ErrInvalid, config.Port)
	}
	if config.VirtualHost == "" {
		config.VirtualHost = "/"
	}
	if config.Username == "" {
		config.Username = "guest"
	}
	if config.Password == "" {
		config.Password = "guest"
	}
	if config.ConfirmTimeout < 0 {
		return fmt.Errorf("%w: the RabbitMQ confirm timeout must be positive", snodra.ErrInvalid)
	}
	if config.ConfirmTimeout == 0 {
		config.ConfirmTimeout = DefaultConfirmTimeout
	}
	if config.RequestTimeout < 0 {
		return fmt.Errorf("%w: the RabbitMQ request timeout must be positive", snodra.ErrInvalid)
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = DefaultRequestTimeout
	}
	if config.ManagementTimeout < 0 {
		return fmt.Errorf("%w: the RabbitMQ management timeout must be positive", snodra.ErrInvalid)
	}
	if config.ManagementTimeout == 0 {
		config.ManagementTimeout = DefaultManagementTimeout
	}
	if config.OffsetStoreMessages == 0 {
		config.OffsetStoreMessages = DefaultOffsetStoreMessages
	}
	if config.OffsetStoreInterval < 0 {
		return fmt.Errorf("%w: the RabbitMQ offset store interval must be positive", snodra.ErrInvalid)
	}
	if config.OffsetStoreInterval == 0 {
		config.OffsetStoreInterval = DefaultOffsetStoreInterval
	}
	if config.ManagementUsername == "" {
		config.ManagementUsername = config.Username
	}
	if config.ManagementPassword == "" {
		config.ManagementPassword = config.Password
	}
	if config.ManagementURL != "" {
		parsed, err := url.Parse(config.ManagementURL)
		if err != nil {
			return fmt.Errorf("%w: the RabbitMQ management URL: %w", snodra.ErrInvalid, err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return fmt.Errorf("%w: the RabbitMQ management URL must be http or https", snodra.ErrInvalid)
		}
		if parsed.User != nil {
			return fmt.Errorf("%w: the RabbitMQ management URL carries credentials; use ManagementUsername and ManagementPassword", snodra.ErrInvalid)
		}
	}
	return nil
}

// Name is the event store's name ("rabbitmq-streams").
func (s *EventStore) Name() string { return Name }

// Capabilities is what the store offers: `lag` and `native-dead-letter` always, `replay` only
// with the management API. No event browser, so no `browse`.
//
// TODO(spec-gap): SG-46 (the published RabbitMQ page lists browse, but this library has no
// event browser).
func (s *EventStore) Capabilities() []snodra.Capability {
	capabilities := []snodra.Capability{
		snodra.CapabilityLag,
		snodra.CapabilityNativeDeadLetter,
	}
	if s.management != nil {
		capabilities = append([]snodra.Capability{snodra.CapabilityReplay}, capabilities...)
	}
	return capabilities
}

// Publish appends event to its source and returns once the broker confirmed it. A blank source
// is snodra.ErrInvalid; an event over the 1 MiB frame, an access refusal or a frame-too-large
// refusal is snodra.ErrRejected (the relay gives it up); any other failure is snodra.ErrFailed
// (another try may succeed). There is no producer name, so no broker deduplication: the
// processed-event markers skip an event the relay publishes again after a crash.
func (s *EventStore) Publish(ctx context.Context, event snodra.OutgoingEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return snodra.ErrClosed
	}
	if strings.TrimSpace(event.Source) == "" {
		return fmt.Errorf("%w: the event's source is empty", snodra.ErrInvalid)
	}
	return s.publisher.publish(ctx, event)
}

// Subscribe joins spec as one more instance of its subscription. A done context is returned
// as-is.
func (s *EventStore) Subscribe(ctx context.Context, spec snodra.SubscriptionSpec) (snodra.EventSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, snodra.ErrClosed
	}
	if spec.Name == "" || spec.Source == "" {
		return nil, fmt.Errorf("%w: a subscription needs a name and a source", snodra.ErrInvalid)
	}
	return newSubscription(ctx, s, spec)
}

// Admin is the reset side of spec. It fails with an *UnsupportedError naming `replay` when no
// management API is configured.
func (s *EventStore) Admin(spec snodra.SubscriptionSpec) (snodra.SubscriptionAdmin, error) {
	if s.closed.Load() {
		return nil, snodra.ErrClosed
	}
	if spec.Name == "" || spec.Source == "" {
		return nil, fmt.Errorf("%w: a subscription needs a name and a source", snodra.ErrInvalid)
	}
	if s.management == nil {
		return nil, &snodra.UnsupportedError{Capability: snodra.CapabilityReplay}
	}
	return &admin{store: s, spec: spec}, nil
}

// Compare orders two offsets: decimal offsets by value; text that is not an offset comes after
// every offset, by its bytes among themselves.
func (s *EventStore) Compare(segment, a, b string) int { return comparePositions(a, b) }

// Close releases the publisher's connections. It does not close the subscriptions: the caller
// closes each. Close is idempotent.
func (s *EventStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.publisher.close()
	return nil
}

// address is a host as a dial address.
func (s *EventStore) address(host string) string {
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return net.JoinHostPort(host, strconv.Itoa(s.config.Port))
}

// dial opens one connection to addr.
func (s *EventStore) dial(ctx context.Context, addr, name string) (*streamwire.Conn, error) {
	connection, err := streamwire.Dial(ctx, streamwire.Config{
		Address:        addr,
		VirtualHost:    s.config.VirtualHost,
		Username:       s.config.Username,
		Password:       s.config.Password,
		TLS:            s.config.TLS,
		ConnectionName: name,
		RequestTimeout: s.config.RequestTimeout,
		Heartbeat:      30 * time.Second,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %w", snodra.ErrFailed, err)
	}
	return connection, nil
}

// dialAny opens one connection to the first host that answers.
func (s *EventStore) dialAny(ctx context.Context, name string) (*streamwire.Conn, error) {
	var last error
	for _, host := range s.config.Hosts {
		connection, err := s.dial(ctx, s.address(host), name)
		if err == nil {
			return connection, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("%w: no RabbitMQ host answered: %w", snodra.ErrFailed, last)
}

// sourcePart is one partition of a source.
type sourcePart struct {
	segment string
	stream  string
}

// sourceLayout is how a source is partitioned.
type sourceLayout struct {
	parts []sourcePart
	super bool
}

// layoutOf reads how source is partitioned, on a seed connection: its super stream partitions,
// or the plain stream itself (one segment "0").
func layoutOf(ctx context.Context, connection *streamwire.Conn, source string) (sourceLayout, error) {
	partitions, err := connection.Partitions(ctx, source)
	if err == nil && len(partitions) > 0 {
		parts := make([]sourcePart, 0, len(partitions))
		for _, stream := range partitions {
			segment := segmentOf(source, stream)
			if segment == "" || len(segment) > snodra.MaxSegmentBytes {
				return sourceLayout{}, fmt.Errorf("%w: the partition %s of %s names a segment over %d bytes",
					snodra.ErrInvalid, stream, source, snodra.MaxSegmentBytes)
			}
			parts = append(parts, sourcePart{segment: segment, stream: stream})
		}
		return sourceLayout{parts: parts, super: true}, nil
	}
	if err != nil {
		var refused *streamwire.ResponseError
		if !errors.As(err, &refused) || refused.Code != streamwire.CodeStreamDoesNotExist {
			return sourceLayout{}, fmt.Errorf("%w: the partitions of %s: %w", snodra.ErrFailed, source, err)
		}
	}
	metadata, err := connection.Metadata(ctx, source)
	if err != nil {
		return sourceLayout{}, fmt.Errorf("%w: the metadata of %s: %w", snodra.ErrFailed, source, err)
	}
	found, ok := metadata.Streams[source]
	if !ok {
		return sourceLayout{}, fmt.Errorf("%w: the broker names no stream %s", snodra.ErrFailed, source)
	}
	switch found.ResponseCode {
	case streamwire.CodeOK:
		return sourceLayout{parts: []sourcePart{{segment: singleSegment, stream: source}}}, nil
	case streamwire.CodeStreamDoesNotExist:
		return sourceLayout{}, fmt.Errorf("%w: neither a super stream nor a stream named %s exists: create it (rabbitmq-streams add_super_stream %s --partitions 6)",
			snodra.ErrFailed, source, source)
	default:
		return sourceLayout{}, fmt.Errorf("%w: the stream %s: response code 0x%02x", snodra.ErrFailed, source, found.ResponseCode)
	}
}

// leaderOf reads the leader's address of stream.
func leaderOf(ctx context.Context, connection *streamwire.Conn, stream string) (string, error) {
	metadata, err := connection.Metadata(ctx, stream)
	if err != nil {
		return "", fmt.Errorf("%w: the metadata of %s: %w", snodra.ErrFailed, stream, err)
	}
	found, ok := metadata.Streams[stream]
	if !ok {
		return "", fmt.Errorf("%w: the metadata of %s names no leader (it may have none during an election, or not exist)", snodra.ErrFailed, stream)
	}
	if found.ResponseCode == streamwire.CodeStreamDoesNotExist {
		return "", fmt.Errorf("%w: the broker says the stream %s does not exist", snodra.ErrFailed, stream)
	}
	if found.ResponseCode != streamwire.CodeOK || found.Leader.Host == "" {
		return "", fmt.Errorf("%w: the stream %s is unavailable right now (response code 0x%02x)", snodra.ErrFailed, stream, found.ResponseCode)
	}
	return net.JoinHostPort(found.Leader.Host, strconv.Itoa(int(found.Leader.Port))), nil
}

// management answers who reads a subscription. The stream protocol cannot list consumers.
type management struct {
	url      string
	username string
	password string
	vhost    string
	client   *http.Client
}

// newManagement is the configured management API, or nil without one.
func newManagement(config Config) (*management, error) {
	if config.ManagementURL == "" {
		return nil, nil
	}
	return &management{
		url:      strings.TrimRight(config.ManagementURL, "/"),
		username: config.ManagementUsername,
		password: config.ManagementPassword,
		vhost:    config.VirtualHost,
		client:   &http.Client{Timeout: config.ManagementTimeout},
	}, nil
}
