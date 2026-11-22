package main

import (
	"fmt"
	"strconv"
	"strings"

	snodra "github.com/snodra/snodra-messaging-go"
)

// env is the CONFORMANCE_* environment the runner starts the service with (the conformance
// contract, "Starting the service").
type env struct {
	service  string   // CONFORMANCE_SERVICE_NAME
	instance string   // CONFORMANCE_INSTANCE_ID
	hubs     []string // CONFORMANCE_HUBS, comma-separated host:port, in order
	capacity int      // CONFORMANCE_CAPACITY, default 64
	retry    bool     // CONFORMANCE_RETRY_MAX_ATTEMPTS is set: a retry policy applies to the sends
}

// readEnv reads the environment through getenv; a blank variable is an absent one.
func readEnv(getenv func(string) string) (env, error) {
	get := func(name string) string { return strings.TrimSpace(getenv(name)) }
	e := env{service: get("CONFORMANCE_SERVICE_NAME"), instance: get("CONFORMANCE_INSTANCE_ID"), capacity: 64,
		retry: get("CONFORMANCE_RETRY_MAX_ATTEMPTS") != ""}
	for _, h := range strings.Split(get("CONFORMANCE_HUBS"), ",") {
		if h = strings.TrimSpace(h); h != "" {
			e.hubs = append(e.hubs, h)
		}
	}
	switch {
	case e.service == "":
		return env{}, fmt.Errorf("CONFORMANCE_SERVICE_NAME is required")
	case e.instance == "":
		return env{}, fmt.Errorf("CONFORMANCE_INSTANCE_ID is required")
	case len(e.hubs) == 0:
		return env{}, fmt.Errorf("CONFORMANCE_HUBS is required")
	}
	if v := get("CONFORMANCE_CAPACITY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return env{}, fmt.Errorf("CONFORMANCE_CAPACITY must be a positive integer, got %q", v)
		}
		e.capacity = n
	}
	return e, nil
}

// options are the library's settings for e.
func (e env) options() []snodra.Option {
	return []snodra.Option{
		snodra.Service(e.service),
		snodra.Instance(e.instance),
		snodra.Hubs(e.hubs...),
		snodra.MaxConcurrent(e.capacity),
	}
}
