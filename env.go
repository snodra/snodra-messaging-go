package snodramessaging

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// defaultHubAddress is the hub of SNODRA_SERVER_ADDRESS when the variable is not set.
const defaultHubAddress = "static://snodra-server:7000"

// FromEnv reads the settings from the environment, the same variables the other Snodra
// libraries read. A variable that is not set, or blank, changes nothing; a later option
// overrides what FromEnv set.
//
//   - SNODRA_SERVICE_NAME: [Service]
//   - SNODRA_INSTANCE_ID: [Instance] (default: a fresh random UUID)
//   - SNODRA_SERVER_ADDRESS: [Hubs], a comma-separated list of host:port, "static://" optional
//     (default "static://snodra-server:7000" when no hub is configured yet)
//   - SNODRA_MAX_CONCURRENT_HANDLERS: [MaxConcurrent] (default 256)
//   - SNODRA_RECONNECT_BACKOFF_MIN_MS, SNODRA_RECONNECT_BACKOFF_MAX_MS: [ReconnectBackoff] in
//     milliseconds (default 500 and 5000)
//   - SNODRA_SERVER_LOAD_FACTOR: [LoadFactor] (default 100)
//   - SNODRA_SERVER_TAGS: [Tags] as "key=value,key=value"
//   - SNODRA_SERVER_REQUEST_TIMEOUT_MS: [DefaultTimeout] in milliseconds (0, the default: none)
//   - SNODRA_SERVER_PRIORITIES: [Priorities] as "type=n,type=n"
//   - SNODRA_SERVER_RETRY_ENABLED: true sets [DefaultRetry] to the policy of
//     SNODRA_SERVER_RETRY_MAX_ATTEMPTS (default 3, at least 1),
//     SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS (100), SNODRA_SERVER_RETRY_MULTIPLIER (2, a finite
//     number of at least 1) and SNODRA_SERVER_RETRY_MAX_BACKOFF_MS (2000); default false. The
//     policy variables are checked even when retry is off.
func FromEnv() Option {
	return func(c *config) error { return c.applyEnv(os.Getenv) }
}

func (c *config) applyEnv(getenv func(string) string) error {
	get := func(name string) (string, bool) {
		v := strings.TrimSpace(getenv(name))
		return v, v != ""
	}
	if v, ok := get("SNODRA_SERVICE_NAME"); ok {
		c.service = v
	}
	if v, ok := get("SNODRA_INSTANCE_ID"); ok {
		c.instance = v
	}
	address, ok := get("SNODRA_SERVER_ADDRESS")
	if !ok && len(c.hubs) == 0 {
		address, ok = defaultHubAddress, true
	}
	if ok {
		var entries []string
		for _, e := range strings.Split(address, ",") {
			if e = strings.TrimSpace(e); e != "" {
				entries = append(entries, e)
			}
		}
		hubs, err := parseHubs(entries)
		if err != nil {
			return err
		}
		c.hubs = hubs
	}
	if v, ok := get("SNODRA_MAX_CONCURRENT_HANDLERS"); ok {
		n, err := envNumber("SNODRA_MAX_CONCURRENT_HANDLERS", v, math.MaxUint32)
		if err != nil {
			return err
		}
		if n < 1 {
			return fmt.Errorf("snodra: SNODRA_MAX_CONCURRENT_HANDLERS must be at least 1, got %q", v)
		}
		c.maxConcurrent = int(n)
	}
	if v, ok := get("SNODRA_RECONNECT_BACKOFF_MIN_MS"); ok {
		n, err := envNumber("SNODRA_RECONNECT_BACKOFF_MIN_MS", v, math.MaxInt64/int64(time.Millisecond))
		if err != nil {
			return err
		}
		c.timings.backoffMin = time.Duration(n) * time.Millisecond
	}
	if v, ok := get("SNODRA_RECONNECT_BACKOFF_MAX_MS"); ok {
		n, err := envNumber("SNODRA_RECONNECT_BACKOFF_MAX_MS", v, math.MaxInt64/int64(time.Millisecond))
		if err != nil {
			return err
		}
		c.timings.backoffMax = time.Duration(n) * time.Millisecond
	}
	if v, ok := get("SNODRA_SERVER_LOAD_FACTOR"); ok {
		n, err := envNumber("SNODRA_SERVER_LOAD_FACTOR", v, math.MaxUint32)
		if err != nil {
			return err
		}
		c.loadFactor = uint32(n)
	}
	if v, ok := get("SNODRA_SERVER_TAGS"); ok {
		tags, err := envPairs("SNODRA_SERVER_TAGS", v)
		if err != nil {
			return err
		}
		c.tags = tags
	}
	return c.applySendEnv(get)
}

// applySendEnv reads the sends' settings.
func (c *config) applySendEnv(get func(string) (string, bool)) error {
	millis := func(name, v string) (time.Duration, error) {
		n, err := envNumber(name, v, math.MaxInt64/int64(time.Millisecond))
		return time.Duration(n) * time.Millisecond, err
	}
	if v, ok := get("SNODRA_SERVER_REQUEST_TIMEOUT_MS"); ok {
		d, err := millis("SNODRA_SERVER_REQUEST_TIMEOUT_MS", v)
		if err != nil {
			return err
		}
		c.defaultTimeout = d
	}
	if v, ok := get("SNODRA_SERVER_PRIORITIES"); ok {
		pairs, err := envPairs("SNODRA_SERVER_PRIORITIES", v)
		if err != nil {
			return err
		}
		c.priorities = map[string]int32{}
		for k, val := range pairs {
			n, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return fmt.Errorf("snodra: SNODRA_SERVER_PRIORITIES: the priority of %q must be an integer, got %q", k, val)
			}
			c.priorities[k] = int32(n)
		}
	}
	enabled := false
	if v, ok := get("SNODRA_SERVER_RETRY_ENABLED"); ok {
		switch strings.ToLower(v) {
		case "true", "1", "yes":
			enabled = true
		case "false", "0", "no":
		default:
			return fmt.Errorf("snodra: SNODRA_SERVER_RETRY_ENABLED must be true or false, got %q", v)
		}
	}
	p := RetryNeverRan
	if v, ok := get("SNODRA_SERVER_RETRY_MAX_ATTEMPTS"); ok {
		n, err := envNumber("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", v, math.MaxInt32)
		if err != nil {
			return err
		}
		if n < 1 {
			return fmt.Errorf("snodra: SNODRA_SERVER_RETRY_MAX_ATTEMPTS must be at least 1 (1 = no retry), got %q", v)
		}
		p.MaxAttempts = int(n)
	}
	if v, ok := get("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS"); ok {
		d, err := millis("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS", v)
		if err != nil {
			return err
		}
		p.InitialBackoff = d
	}
	if v, ok := get("SNODRA_SERVER_RETRY_MULTIPLIER"); ok {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 1 {
			return fmt.Errorf("snodra: SNODRA_SERVER_RETRY_MULTIPLIER must be a finite number of at least 1, got %q", v)
		}
		p.Multiplier = f
	}
	if v, ok := get("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS"); ok {
		d, err := millis("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS", v)
		if err != nil {
			return err
		}
		p.MaxBackoff = d
	}
	if enabled {
		c.retry = &p
	}
	return nil
}

// envNumber reads a non-negative integer of at most limit.
func envNumber(name, v string, limit int64) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 || n > limit {
		return 0, fmt.Errorf("snodra: %s must be a non-negative integer of at most %d, got %q", name, limit, v)
	}
	return n, nil
}

// envPairs reads "key=value,key=value"; blanks around entries and a trailing comma are
// ignored.
func envPairs(name, v string) (map[string]string, error) {
	out := map[string]string{}
	for _, entry := range strings.Split(v, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		k, val, ok := strings.Cut(entry, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("snodra: %s: entry %q is not key=value", name, entry)
		}
		out[k] = strings.TrimSpace(val)
	}
	return out, nil
}
