package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// env is the CONFORMANCE_* environment the runner starts the service with (the conformance
// contract, "Starting the service").
type env struct {
	service  string   // CONFORMANCE_SERVICE_NAME
	instance string   // CONFORMANCE_INSTANCE_ID
	hubs     []string // CONFORMANCE_HUBS, comma-separated host:port, in order
	capacity int      // CONFORMANCE_CAPACITY, default 64
	// retry is the policy of every send, from CONFORMANCE_RETRY_MAX_ATTEMPTS and
	// CONFORMANCE_RETRY_INITIAL_BACKOFF_MS (default 100); nil when the first is not set.
	retry *snodra.RetryPolicy
	// jwksURL and issuer check the callers' tokens (CONFORMANCE_JWKS_URL, CONFORMANCE_ISSUER);
	// without a URL no caller is verified.
	jwksURL, issuer string
	// hubToken is the static token towards the hubs (CONFORMANCE_HUB_TOKEN).
	hubToken string
	// oauth2 is the JWT's client-credentials grant (CONFORMANCE_OAUTH2_TOKEN_URL, _CLIENT_ID,
	// _CLIENT_SECRET); nil without a token URL.
	oauth2 *snodra.ClientCredentials
	// tlsCA and tlsServerName are TLS to the hubs (CONFORMANCE_TLS_CA, CONFORMANCE_TLS_SERVER_NAME).
	tlsCA, tlsServerName string
	// clientCert and clientKey are the client certificate pair (CONFORMANCE_TLS_CLIENT_CERT,
	// CONFORMANCE_TLS_CLIENT_KEY).
	clientCert, clientKey string
	// creditTimeout bounds a streaming handler's wait for credit
	// (CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS); 0: the library's default.
	creditTimeout time.Duration
	// maxSubscriptions is how many subscriptions the instance serves at once
	// (CONFORMANCE_MAX_SUBSCRIPTIONS); 0: the library's default.
	maxSubscriptions int
}

// readEnv reads the environment through getenv; a blank variable is an absent one.
func readEnv(getenv func(string) string) (env, error) {
	get := func(name string) string { return strings.TrimSpace(getenv(name)) }
	e := env{service: get("CONFORMANCE_SERVICE_NAME"), instance: get("CONFORMANCE_INSTANCE_ID"), capacity: 64,
		jwksURL: get("CONFORMANCE_JWKS_URL"), issuer: get("CONFORMANCE_ISSUER"),
		hubToken: get("CONFORMANCE_HUB_TOKEN"), tlsCA: get("CONFORMANCE_TLS_CA"),
		tlsServerName: get("CONFORMANCE_TLS_SERVER_NAME"), clientCert: get("CONFORMANCE_TLS_CLIENT_CERT"),
		clientKey: get("CONFORMANCE_TLS_CLIENT_KEY")}
	if url := get("CONFORMANCE_OAUTH2_TOKEN_URL"); url != "" {
		e.oauth2 = &snodra.ClientCredentials{
			TokenURL: url, ClientID: get("CONFORMANCE_OAUTH2_CLIENT_ID"), ClientSecret: get("CONFORMANCE_OAUTH2_CLIENT_SECRET"),
		}
	}
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
	if v := get("CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 1 {
			return env{}, fmt.Errorf("CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS must be a positive integer, got %q", v)
		}
		e.creditTimeout = time.Duration(ms) * time.Millisecond
	}
	if v := get("CONFORMANCE_MAX_SUBSCRIPTIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return env{}, fmt.Errorf("CONFORMANCE_MAX_SUBSCRIPTIONS must be a positive integer, got %q", v)
		}
		e.maxSubscriptions = n
	}
	if v := get("CONFORMANCE_RETRY_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return env{}, fmt.Errorf("CONFORMANCE_RETRY_MAX_ATTEMPTS must be a positive integer, got %q", v)
		}
		p := snodra.RetryNeverRan
		p.MaxAttempts = n
		if v := get("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS"); v != "" {
			ms, err := strconv.Atoi(v)
			if err != nil || ms < 0 {
				return env{}, fmt.Errorf("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS must be a non-negative integer, got %q", v)
			}
			p.InitialBackoff = time.Duration(ms) * time.Millisecond
		}
		e.retry = &p
	}
	return e, nil
}

// options are the library's settings for e.
func (e env) options() []snodra.Option {
	opts := []snodra.Option{
		snodra.Service(e.service),
		snodra.Instance(e.instance),
		snodra.Hubs(e.hubs...),
		snodra.MaxConcurrent(e.capacity),
		// The runner checks what reaches the hub: a send to one of the service's own types must
		// go through it, not be answered in-process.
		snodra.AlwaysThroughHub(true),
	}
	if e.retry != nil {
		opts = append(opts, snodra.DefaultRetry(*e.retry))
	}
	if e.jwksURL != "" {
		opts = append(opts, snodra.JWKS(e.jwksURL, e.issuer))
	}
	if e.hubToken != "" {
		opts = append(opts, snodra.Token(e.hubToken))
	}
	if e.oauth2 != nil {
		opts = append(opts, snodra.JWT(snodra.NewClientCredentials(*e.oauth2)))
	}
	if e.tlsCA != "" || e.tlsServerName != "" {
		opts = append(opts, snodra.TLS(snodra.TLSConfig{CAFile: e.tlsCA, ServerName: e.tlsServerName}))
	}
	if e.clientCert != "" || e.clientKey != "" {
		opts = append(opts, snodra.ClientCertificate(e.clientCert, e.clientKey))
	}
	if e.maxSubscriptions > 0 {
		opts = append(opts, snodra.MaxSubscriptions(e.maxSubscriptions))
	}
	if e.creditTimeout > 0 {
		opts = append(opts, snodra.StreamCreditTimeout(e.creditTimeout))
	}
	return opts
}
