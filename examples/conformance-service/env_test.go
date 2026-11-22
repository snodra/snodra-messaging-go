package main

import (
	"strings"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

func lookup(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestEnvMapsOntoOptions(t *testing.T) {
	e, err := readEnv(lookup(map[string]string{
		"CONFORMANCE_SERVICE_NAME": "conformance-go",
		"CONFORMANCE_INSTANCE_ID":  "i-7",
		"CONFORMANCE_HUBS":         "127.0.0.1:7000, 127.0.0.1:7100",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if e.service != "conformance-go" || e.instance != "i-7" || e.capacity != 64 {
		t.Fatalf("got %+v", e)
	}
	if got := strings.Join(e.hubs, ","); got != "127.0.0.1:7000,127.0.0.1:7100" {
		t.Fatalf("hubs %q", got)
	}
	if n := len(e.options()); n == 0 {
		t.Fatal("no options")
	}

	e, err = readEnv(lookup(map[string]string{
		"CONFORMANCE_SERVICE_NAME": "conformance-go",
		"CONFORMANCE_INSTANCE_ID":  "i-7",
		"CONFORMANCE_HUBS":         "127.0.0.1:7000",
		"CONFORMANCE_CAPACITY":     "4",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if e.capacity != 4 {
		t.Fatalf("capacity %d", e.capacity)
	}
	if e.creditTimeout != 0 {
		t.Fatalf("credit timeout %v", e.creditTimeout)
	}

	e, err = readEnv(lookup(map[string]string{
		"CONFORMANCE_SERVICE_NAME":             "conformance-go",
		"CONFORMANCE_INSTANCE_ID":              "i-7",
		"CONFORMANCE_HUBS":                     "127.0.0.1:7000",
		"CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS": "2000",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if e.creditTimeout != 2*time.Second {
		t.Fatalf("credit timeout %v", e.creditTimeout)
	}
	if _, err := snodra.New(append(e.options(), snodra.Hubs("127.0.0.1:7000"))...); err != nil {
		t.Fatal(err)
	}
	if _, err := readEnv(lookup(map[string]string{
		"CONFORMANCE_SERVICE_NAME":             "conformance-go",
		"CONFORMANCE_INSTANCE_ID":              "i-7",
		"CONFORMANCE_HUBS":                     "127.0.0.1:7000",
		"CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS": "0",
	})); err == nil || !strings.Contains(err.Error(), "CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS") {
		t.Fatalf("a bad credit timeout: %v", err)
	}

	for _, missing := range []string{"CONFORMANCE_SERVICE_NAME", "CONFORMANCE_INSTANCE_ID", "CONFORMANCE_HUBS"} {
		vars := map[string]string{
			"CONFORMANCE_SERVICE_NAME": "conformance-go",
			"CONFORMANCE_INSTANCE_ID":  "i-7",
			"CONFORMANCE_HUBS":         "127.0.0.1:7000",
		}
		delete(vars, missing)
		if _, err := readEnv(lookup(vars)); err == nil || !strings.Contains(err.Error(), missing) {
			t.Fatalf("without %s: %v", missing, err)
		}
	}
	if _, err := readEnv(lookup(map[string]string{
		"CONFORMANCE_SERVICE_NAME": "conformance-go",
		"CONFORMANCE_INSTANCE_ID":  "i-7",
		"CONFORMANCE_HUBS":         "127.0.0.1:7000",
		"CONFORMANCE_CAPACITY":     "lots",
	})); err == nil || !strings.Contains(err.Error(), "CONFORMANCE_CAPACITY") {
		t.Fatalf("a bad capacity: %v", err)
	}
}

func TestEnvReadsTheCallersIssuer(t *testing.T) {
	base := map[string]string{
		"CONFORMANCE_SERVICE_NAME": "conformance-go",
		"CONFORMANCE_INSTANCE_ID":  "i-7",
		"CONFORMANCE_HUBS":         "127.0.0.1:7000",
	}
	with := func(extra map[string]string) map[string]string {
		vars := map[string]string{}
		for k, v := range base {
			vars[k] = v
		}
		for k, v := range extra {
			vars[k] = v
		}
		return vars
	}
	e, err := readEnv(lookup(with(map[string]string{
		"CONFORMANCE_JWKS_URL": "http://127.0.0.1:9000/jwks",
		"CONFORMANCE_ISSUER":   "http://127.0.0.1:9000/conformance",
	})))
	if err != nil {
		t.Fatal(err)
	}
	if e.jwksURL != "http://127.0.0.1:9000/jwks" || e.issuer != "http://127.0.0.1:9000/conformance" {
		t.Fatalf("got %+v", e)
	}
	if _, err := snodra.New(e.options()...); err != nil {
		t.Fatalf("New: %v", err)
	}
	// The URL without the issuer: the library refuses to start.
	e, err = readEnv(lookup(with(map[string]string{"CONFORMANCE_JWKS_URL": "http://127.0.0.1:9000/jwks"})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snodra.New(e.options()...); err == nil {
		t.Fatal("New accepted a JWKS without an issuer")
	}
}

func TestEnvReadsTheCredentials(t *testing.T) {
	base := map[string]string{
		"CONFORMANCE_SERVICE_NAME": "conformance-go",
		"CONFORMANCE_INSTANCE_ID":  "i-7",
		"CONFORMANCE_HUBS":         "127.0.0.1:7000",
	}
	with := func(extra map[string]string) env {
		t.Helper()
		vars := map[string]string{}
		for k, v := range base {
			vars[k] = v
		}
		for k, v := range extra {
			vars[k] = v
		}
		e, err := readEnv(lookup(vars))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := with(map[string]string{"CONFORMANCE_HUB_TOKEN": "conformance-token"})
	if e.hubToken != "conformance-token" {
		t.Fatalf("token %q", e.hubToken)
	}
	if _, err := snodra.New(e.options()...); err != nil {
		t.Fatalf("New with a token: %v", err)
	}
	e = with(map[string]string{
		"CONFORMANCE_OAUTH2_TOKEN_URL":     "http://127.0.0.1:9000/token",
		"CONFORMANCE_OAUTH2_CLIENT_ID":     "conformance",
		"CONFORMANCE_OAUTH2_CLIENT_SECRET": "conformance-secret",
	})
	if e.oauth2 == nil || e.oauth2.TokenURL != "http://127.0.0.1:9000/token" || e.oauth2.ClientID != "conformance" ||
		e.oauth2.ClientSecret != "conformance-secret" {
		t.Fatalf("oauth2 %v", e.oauth2)
	}
	if _, err := snodra.New(e.options()...); err != nil {
		t.Fatalf("New with a JWT: %v", err)
	}
	// Both: the library refuses them, which proves both reached it.
	e = with(map[string]string{
		"CONFORMANCE_HUB_TOKEN":            "conformance-token",
		"CONFORMANCE_OAUTH2_TOKEN_URL":     "http://127.0.0.1:9000/token",
		"CONFORMANCE_OAUTH2_CLIENT_SECRET": "conformance-secret",
	})
	if _, err := snodra.New(e.options()...); err == nil || !strings.Contains(err.Error(), "use one credential") {
		t.Fatalf("New with a token and a JWT: %v", err)
	}
	e = with(map[string]string{
		"CONFORMANCE_TLS_CA":          "/run/ca.pem",
		"CONFORMANCE_TLS_SERVER_NAME": "conformance-hub.test",
		"CONFORMANCE_TLS_CLIENT_CERT": "/run/client.crt",
		"CONFORMANCE_TLS_CLIENT_KEY":  "/run/client.key",
	})
	if e.tlsCA != "/run/ca.pem" || e.tlsServerName != "conformance-hub.test" || e.clientCert != "/run/client.crt" ||
		e.clientKey != "/run/client.key" {
		t.Fatalf("tls %+v", e)
	}
	// The files do not exist: New reads them, which proves they reached it.
	if _, err := snodra.New(e.options()...); err == nil || !strings.Contains(err.Error(), "/run/client.crt") {
		t.Fatalf("New with missing certificate files: %v", err)
	}
}
