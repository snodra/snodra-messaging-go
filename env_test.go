package snodramessaging

import (
	"strings"
	"testing"
)

// clearEnv unsets (sets empty, which reads as unset) every variable FromEnv reads.
func clearEnv(t *testing.T) {
	for _, name := range []string{
		"SNODRA_SERVICE_NAME", "SNODRA_INSTANCE_ID", "SNODRA_SERVER_ADDRESS",
		"SNODRA_MAX_CONCURRENT_HANDLERS", "SNODRA_RECONNECT_BACKOFF_MIN_MS", "SNODRA_RECONNECT_BACKOFF_MAX_MS",
		"SNODRA_SERVER_LOAD_FACTOR", "SNODRA_SERVER_TAGS",
	} {
		t.Setenv(name, "")
	}
}

func TestFromEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	app, err := New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.service, "orders-service")
	is(t, len(app.cfg.instance), 36) // a fresh UUID
	is(t, strings.Join(app.cfg.hubs, ","), "snodra-server:7000")
	is(t, app.cfg.maxConcurrent, 256)
	is(t, app.cfg.timings.backoffMin, 500*ms)
	is(t, app.cfg.timings.backoffMax, 5*s)
	is(t, app.cfg.loadFactor, uint32(100))
	is(t, len(app.cfg.tags), 0)

	t.Setenv("SNODRA_INSTANCE_ID", "orders-1")
	t.Setenv("SNODRA_SERVER_ADDRESS", "static://hub-1:7000, hub-2:7100,")
	t.Setenv("SNODRA_MAX_CONCURRENT_HANDLERS", "32")
	t.Setenv("SNODRA_RECONNECT_BACKOFF_MIN_MS", "100")
	t.Setenv("SNODRA_RECONNECT_BACKOFF_MAX_MS", "2000")
	t.Setenv("SNODRA_SERVER_LOAD_FACTOR", "250")
	t.Setenv("SNODRA_SERVER_TAGS", " zone=eu-1 , tier = gold,")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.instance, "orders-1")
	is(t, strings.Join(app.cfg.hubs, ","), "hub-1:7000,hub-2:7100")
	is(t, app.cfg.maxConcurrent, 32)
	is(t, app.cfg.timings.backoffMin, 100*ms)
	is(t, app.cfg.timings.backoffMax, 2*s)
	is(t, app.cfg.loadFactor, uint32(250))
	is(t, len(app.cfg.tags), 2)
	is(t, app.cfg.tags["zone"], "eu-1")
	is(t, app.cfg.tags["tier"], "gold")

	// A later option overrides FromEnv.
	app, err = New(FromEnv(), Service("billing-service"), MaxConcurrent(4), Hubs("hub-3:7000"))
	is(t, err, nil)
	is(t, app.cfg.service, "billing-service")
	is(t, app.cfg.maxConcurrent, 4)
	is(t, strings.Join(app.cfg.hubs, ","), "hub-3:7000")
}

func TestFromEnvRefusesBadValues(t *testing.T) {
	for name, value := range map[string]string{
		"SNODRA_MAX_CONCURRENT_HANDLERS":  "many",
		"SNODRA_RECONNECT_BACKOFF_MIN_MS": "-1",
		"SNODRA_RECONNECT_BACKOFF_MAX_MS": "1.5",
		"SNODRA_SERVER_LOAD_FACTOR":       "x",
		"SNODRA_SERVER_TAGS":              "zone",
	} {
		t.Run(name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
			t.Setenv(name, value)
			_, err := New(FromEnv())
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("got %v, want an error naming %s", err, name)
			}
		})
	}
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	t.Setenv("SNODRA_RECONNECT_BACKOFF_MIN_MS", "6000")
	_, err := New(FromEnv())
	if err == nil || !strings.Contains(err.Error(), "SNODRA_RECONNECT_BACKOFF_MIN_MS") {
		t.Fatalf("got %v, want an error naming SNODRA_RECONNECT_BACKOFF_MIN_MS", err)
	}
}
