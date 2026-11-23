package snodra

import (
	"math"
	"strings"
	"testing"
	"time"
)

// clearEnv unsets (sets empty, which reads as unset) every variable FromEnv reads.
func clearEnv(t *testing.T) {
	for _, name := range []string{
		"SNODRA_SERVICE_NAME", "SNODRA_INSTANCE_ID", "SNODRA_SERVER_ADDRESS",
		"SNODRA_MAX_CONCURRENT_HANDLERS", "SNODRA_RECONNECT_BACKOFF_MIN_MS", "SNODRA_RECONNECT_BACKOFF_MAX_MS",
		"SNODRA_SERVER_LOAD_FACTOR", "SNODRA_SERVER_TAGS",
		"SNODRA_SERVER_REQUEST_TIMEOUT_MS", "SNODRA_SERVER_PRIORITIES", "SNODRA_SERVER_RETRY_ENABLED",
		"SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", "SNODRA_SERVER_MAX_SUBSCRIPTIONS",
		"SNODRA_SERVER_RETRY_MAX_ATTEMPTS", "SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS",
		"SNODRA_SERVER_RETRY_MULTIPLIER", "SNODRA_SERVER_RETRY_MAX_BACKOFF_MS",
		"SNODRA_JWKS_URL", "SNODRA_ISSUER", "SNODRA_SECURITY_ROLES_CLAIM", "SNODRA_FORWARD_JWT",
		"SNODRA_ALWAYS_THROUGH_HUB",
		"SNODRA_SERVER_TOKEN", "SNODRA_SERVER_TOKEN_FILE", "SNODRA_SERVER_TLS", "SNODRA_SERVER_TLS_CA",
		"SNODRA_SERVER_TLS_SERVER_NAME", "SNODRA_SERVER_TLS_CLIENT_CERT", "SNODRA_SERVER_TLS_CLIENT_KEY",
		"SNODRA_SERVER_OAUTH2_TOKEN_URI", "SNODRA_SERVER_OAUTH2_CLIENT_ID", "SNODRA_SERVER_OAUTH2_CLIENT_SECRET",
		"SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE", "SNODRA_SERVER_OAUTH2_SCOPE", "SNODRA_SERVER_OAUTH2_CA",
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
		"SNODRA_MAX_CONCURRENT_HANDLERS":         "many",
		"SNODRA_RECONNECT_BACKOFF_MIN_MS":        "-1",
		"SNODRA_RECONNECT_BACKOFF_MAX_MS":        "1.5",
		"SNODRA_SERVER_LOAD_FACTOR":              "x",
		"SNODRA_SERVER_TAGS":                     "zone",
		"SNODRA_SERVER_REQUEST_TIMEOUT_MS":       "soon",
		"SNODRA_SERVER_PRIORITIES":               "orders.PlaceOrder=high",
		"SNODRA_SERVER_RETRY_ENABLED":            "maybe",
		"SNODRA_SERVER_RETRY_MAX_ATTEMPTS":       "0",
		"SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS": "-5",
		"SNODRA_SERVER_RETRY_MULTIPLIER":         "0.5",
		"SNODRA_SERVER_RETRY_MAX_BACKOFF_MS":     "x",
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

func TestFromEnvSendSettings(t *testing.T) {
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	app, err := New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.defaultTimeout, time.Duration(0))
	is(t, app.cfg.retry == nil, true) // no retry unless enabled
	is(t, len(app.cfg.priorities), 0)
	is(t, app.cfg.timings.creditWait, 10*time.Minute)

	t.Setenv("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", "2500")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.timings.creditWait, 2500*ms)
	t.Setenv("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", "0")
	_, err = New(FromEnv())
	if err == nil || !strings.Contains(err.Error(), "SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS") {
		t.Fatalf("got %v, want an error naming SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", err)
	}
	t.Setenv("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", "")

	is(t, app.cfg.maxSubscriptions, 1024)
	t.Setenv("SNODRA_SERVER_MAX_SUBSCRIPTIONS", "3")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.maxSubscriptions, 3)
	for _, v := range []string{"0", "-1", "many"} {
		t.Setenv("SNODRA_SERVER_MAX_SUBSCRIPTIONS", v)
		_, err = New(FromEnv())
		if err == nil || !strings.Contains(err.Error(), "SNODRA_SERVER_MAX_SUBSCRIPTIONS") {
			t.Fatalf("SNODRA_SERVER_MAX_SUBSCRIPTIONS=%s: got %v, want an error naming it", v, err)
		}
	}
	t.Setenv("SNODRA_SERVER_MAX_SUBSCRIPTIONS", "")

	t.Setenv("SNODRA_SERVER_REQUEST_TIMEOUT_MS", "0") // 0: not chosen
	t.Setenv("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", "5") // read, but retry is off
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.defaultTimeout, time.Duration(0))
	is(t, app.cfg.retry == nil, true)

	t.Setenv("SNODRA_SERVER_REQUEST_TIMEOUT_MS", "1500")
	t.Setenv("SNODRA_SERVER_PRIORITIES", "orders.PlaceOrder=5, orders.GetOrder=-1,")
	t.Setenv("SNODRA_SERVER_RETRY_ENABLED", "true")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.defaultTimeout, 1500*ms)
	is(t, app.cfg.priorities["orders.PlaceOrder"], int32(5))
	is(t, app.cfg.priorities["orders.GetOrder"], int32(-1))
	is(t, *app.cfg.retry, RetryPolicy{MaxAttempts: 5, InitialBackoff: 100 * ms, Multiplier: 2, MaxBackoff: 2 * s})

	t.Setenv("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS", "50")
	t.Setenv("SNODRA_SERVER_RETRY_MULTIPLIER", "1.5")
	t.Setenv("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS", "800")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, *app.cfg.retry, RetryPolicy{MaxAttempts: 5, InitialBackoff: 50 * ms, Multiplier: 1.5, MaxBackoff: 800 * ms})
}

func TestFromEnvIdentitySettings(t *testing.T) {
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	app, err := New(FromEnv())
	is(t, err, nil)
	is(t, app.identity.verifier == nil, true) // no JWKS: tokens read unverified
	is(t, len(app.identity.roles.path), 1)
	is(t, app.identity.roles.path[0], "roles")
	is(t, app.cfg.forwardJWT, true)

	t.Setenv("SNODRA_JWKS_URL", "https://issuer.test/jwks")
	t.Setenv("SNODRA_ISSUER", "https://issuer.test")
	t.Setenv("SNODRA_SECURITY_ROLES_CLAIM", "realm_access.roles")
	t.Setenv("SNODRA_FORWARD_JWT", "no")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.jwksURL, "https://issuer.test/jwks")
	is(t, app.cfg.issuer, "https://issuer.test")
	is(t, app.identity.verifier != nil, true)
	is(t, strings.Join(app.identity.roles.path, "."), "realm_access.roles")
	is(t, app.cfg.forwardJWT, false)

	is(t, app.cfg.alwaysThroughHub, false)
	t.Setenv("SNODRA_ALWAYS_THROUGH_HUB", "true")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.alwaysThroughHub, true)
	t.Setenv("SNODRA_ALWAYS_THROUGH_HUB", "sometimes")
	_, err = New(FromEnv())
	if err == nil || !strings.Contains(err.Error(), "SNODRA_ALWAYS_THROUGH_HUB") {
		t.Fatalf("got %v, want an error naming SNODRA_ALWAYS_THROUGH_HUB", err)
	}
	t.Setenv("SNODRA_ALWAYS_THROUGH_HUB", "")

	t.Setenv("SNODRA_FORWARD_JWT", "maybe")
	_, err = New(FromEnv())
	if err == nil || !strings.Contains(err.Error(), "SNODRA_FORWARD_JWT") {
		t.Fatalf("got %v, want an error naming SNODRA_FORWARD_JWT", err)
	}
	t.Setenv("SNODRA_FORWARD_JWT", "")
	t.Setenv("SNODRA_ISSUER", "")
	_, err = New(FromEnv())
	if err == nil || !strings.Contains(err.Error(), "SNODRA_ISSUER") {
		t.Fatalf("got %v, want an error naming SNODRA_ISSUER", err)
	}
}

func TestSendOptionsOfTheApp(t *testing.T) {
	_, err := New(Service("orders-service"), Hubs("hub:7000"), DefaultTimeout(-ms))
	is(t, err != nil, true)
	_, err = New(Service("orders-service"), Hubs("hub:7000"), DefaultRetry(RetryPolicy{MaxAttempts: 0}))
	is(t, err != nil, true)
	_, err = New(Service("orders-service"), Hubs("hub:7000"), DefaultRetry(RetryPolicy{MaxAttempts: 3, Multiplier: math.Inf(1)}))
	is(t, err != nil, true)
	app, err := New(Service("orders-service"), Hubs("hub:7000"), DefaultRetry(RetryNeverRan), DefaultTimeout(2*s),
		Priorities(map[string]int32{"orders.PlaceOrder": 3}))
	is(t, err, nil)
	is(t, *app.cfg.retry, RetryNeverRan)
	is(t, app.cfg.defaultTimeout, 2*s)
	is(t, app.cfg.priorities["orders.PlaceOrder"], int32(3))
}

func TestCredentialsFromEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	t.Setenv("SNODRA_SERVER_TOKEN", " s3cr3t-token \n")
	app, err := New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.token.reveal(), "s3cr3t-token")
	is(t, app.cfg.tls == nil, true)

	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	t.Setenv("SNODRA_SERVER_OAUTH2_TOKEN_URI", "http://idp:8080/token")
	t.Setenv("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", "secret")
	t.Setenv("SNODRA_SERVER_OAUTH2_SCOPE", "hub")
	app, err = New(FromEnv())
	is(t, err, nil)
	cc := app.cfg.tokenSource.(*clientCredentials).cfg
	is(t, cc.TokenURL, "http://idp:8080/token")
	is(t, cc.ClientID, "orders-service") // the service name by default
	is(t, cc.Scope, "hub")

	ca := newTestCA(t, "operator CA")
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	t.Setenv("SNODRA_SERVER_TLS", "true")
	t.Setenv("SNODRA_SERVER_TLS_CA", ca.caFile(t))
	t.Setenv("SNODRA_SERVER_TLS_SERVER_NAME", "hub.test")
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.tls.ServerName, "hub.test")

	// A client certificate implies TLS.
	certFile, keyFile := pairFiles(t, t.TempDir(), ca.client(t, "orders-service", time.Time{}))
	clearEnv(t)
	t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
	t.Setenv("SNODRA_SERVER_TLS_CLIENT_CERT", certFile)
	t.Setenv("SNODRA_SERVER_TLS_CLIENT_KEY", keyFile)
	app, err = New(FromEnv())
	is(t, err, nil)
	is(t, app.cfg.tls != nil, true)
	is(t, app.cfg.clientCert, certFile)

	for _, c := range []struct {
		what string
		vars map[string]string
		want string
	}{
		{"a client certificate without TLS", map[string]string{"SNODRA_SERVER_TLS": "false", "SNODRA_SERVER_TLS_CLIENT_CERT": certFile, "SNODRA_SERVER_TLS_CLIENT_KEY": keyFile}, "need TLS"},
		{"a CA without TLS", map[string]string{"SNODRA_SERVER_TLS_CA": "/ca.pem"}, "need SNODRA_SERVER_TLS=true"},
		{"a bad TLS flag", map[string]string{"SNODRA_SERVER_TLS": "maybe"}, "true or false"},
		{"OAuth2 settings without the URI", map[string]string{"SNODRA_SERVER_OAUTH2_CLIENT_SECRET": "x"}, "need SNODRA_SERVER_OAUTH2_TOKEN_URI"},
		{"a token and a JWT", map[string]string{"SNODRA_SERVER_TOKEN": "x", "SNODRA_SERVER_OAUTH2_TOKEN_URI": "http://idp/token", "SNODRA_SERVER_OAUTH2_CLIENT_SECRET": "x"}, "use one credential"},
		{"only the key", map[string]string{"SNODRA_SERVER_TLS_CLIENT_KEY": keyFile}, "both files"},
	} {
		clearEnv(t)
		t.Setenv("SNODRA_SERVICE_NAME", "orders-service")
		for k, v := range c.vars {
			t.Setenv(k, v)
		}
		_, err := New(FromEnv())
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.what, err)
		}
	}
}
