package snodramessaging

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// config is what the options set; New validates it.
type config struct {
	service       string
	instance      string
	hubs          []string // host:port, in order, without repeats
	maxConcurrent int
	loadFactor    uint32
	tags          map[string]string
	logger        *slog.Logger
	codec         PayloadCodec
	timings       timings
}

func defaultConfig() config {
	return config{
		maxConcurrent: 256,
		loadFactor:    100,
		codec:         JSON,
		timings:       defaultTimings(),
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

// Hubs are the hubs to connect to, each "host:port" (a "static://" prefix is accepted). The
// instance opens one stream to every one of them. Required (or SNODRA_SERVER_ADDRESS with
// [FromEnv]).
func Hubs(addresses ...string) Option {
	return func(c *config) error {
		hubs, err := parseHubs(addresses)
		if err != nil {
			return err
		}
		c.hubs = hubs
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
	if c.instance == "" {
		c.instance = uuid.New()
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return nil
}

// parseHubs reads hub addresses: "host:port", with an optional "static://" prefix, in order,
// exact repeats removed.
func parseHubs(addresses []string) ([]string, error) {
	var hubs []string
	seen := map[string]bool{}
	for _, a := range addresses {
		entry := strings.TrimSpace(a)
		hostport := strings.TrimPrefix(entry, "static://")
		host, port, err := net.SplitHostPort(hostport)
		if err != nil || host == "" || port == "" {
			return nil, fmt.Errorf("snodra: hub address %q (Hubs, SNODRA_SERVER_ADDRESS) is not host:port", a)
		}
		if !seen[hostport] {
			seen[hostport] = true
			hubs = append(hubs, hostport)
		}
	}
	return hubs, nil
}
