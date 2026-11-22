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
