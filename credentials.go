package snodra

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// The instance's credentials towards the hubs: a static token, a JWT renewed before it expires,
// or a client certificate (tls.go). One kind per instance.
// See https://snodra.damoreira.com/library-authors/protocol/ (Credentials).

// secret holds a credential behind a closure: fmt prints a func as an address, so the value
// does not show even where fmt prints struct fields raw (unexported ones, or after a verb that
// does not fit); its String hides it too.
type secret struct{ get func() string }

// newSecret is v, trimmed, as a secret; nil for a blank v.
func newSecret(v string) *secret {
	if v = strings.TrimSpace(v); v == "" {
		return nil
	}
	return hide(v)
}

// hide is v as a secret, as it is.
func hide(v string) *secret { return &secret{get: func() string { return v }} }

func (s *secret) String() string { return "***" }

// Format prints "***" for every verb that reaches a Formatter (not %p or %w: see printRedacted).
func (s *secret) Format(f fmt.State, _ rune) { printRedacted(f, s.String()) }

// LogValue is "***", for log/slog.
func (s *secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

// printRedacted writes text, a value's redacted form: the types that hold a credential
// implement fmt.Formatter with it, so the verbs that fit them (%v, %+v, %#v, %s, %q, %d, %x ...)
// never reach their fields. Not covered: %p and %w, which fmt rejects as bad verbs before
// calling a Formatter and prints the struct raw (go vet flags both), and encoding/json, which
// writes the exported fields.
func printRedacted(f fmt.State, text string) { _, _ = io.WriteString(f, text) }

// reveal is the value, to put on the wire; "" for none.
func (s *secret) reveal() string {
	if s == nil {
		return ""
	}
	return s.get()
}

// Token is the static token the instance presents to every hub (Register.credentials.token),
// for a hub whose access control asks for one. It is never logged. A blank token is none.
// An instance uses one kind of credential: Token or [TokenFile], [JWT], or [ClientCertificate].
func Token(token string) Option {
	return func(c *config) error {
		c.token = newSecret(token)
		return nil
	}
}

// TokenFile is a file holding the static token (surrounding blanks ignored), for a mounted
// secret; it wins over [Token]. It is read again before every connection to a hub, and on every
// heartbeat of a registered stream, whose hub gets a changed token as a credentials refresh: a
// rotated secret applies without a restart. [New] fails when the file cannot be read or is
// empty; later, a file that cannot be read or is empty (a rotation caught half way) keeps the
// last token read.
func TokenFile(path string) Option {
	return func(c *config) error {
		c.tokenFile = strings.TrimSpace(path)
		return nil
	}
}

// readTokenFile is the token in path, "" when the file holds only blanks.
func readTokenFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// AccessToken is a JWT the instance registers with, and when it expires. Printing it with %v,
// %+v, %#v or %s hides the token; %p and %w print the struct raw, and json.Marshal includes
// Value.
type AccessToken struct {
	// Value is the token, as sent in Register.credentials.jwt.
	Value string
	// Expiry is when it stops being valid; the zero time is unknown, read as 60 s after the fetch.
	Expiry time.Time
}

// String shows the expiry, never the token.
func (t AccessToken) String() string {
	if t.Expiry.IsZero() {
		return "AccessToken{Value: ***}"
	}
	return "AccessToken{Value: ***, Expiry: " + t.Expiry.Format(time.RFC3339) + "}"
}

// Format prints String for every verb that reaches a Formatter. %p and %w never reach it (fmt
// prints the struct raw for them, and go vet flags both), nor does encoding/json, which writes
// the exported fields.
func (t AccessToken) Format(f fmt.State, _ rune) { printRedacted(f, t.String()) }

// LogValue is String, for log/slog.
func (t AccessToken) LogValue() slog.Value { return slog.StringValue(t.String()) }

// A TokenSource gives the instance a JWT to register with. The library asks for one before a
// stream opens when it holds none still fresh, and again ahead of its expiry (see [JWT]); one
// call at a time, bounded by a 10 s context. [NewClientCredentials] is the usual source.
type TokenSource interface {
	Token(ctx context.Context) (AccessToken, error)
}

// JWT makes the instance register with an access token from src (Register.credentials.jwt),
// for a hub whose access control reads JWTs (its azp claim must be the service name). The token
// is kept while more than its renewal margin is left: a fifth of its lifetime, at least 5 s, at
// most half of it (so renewed at 80 % of a long lifetime, at half of a 10 s one). Before that
// point a new token is fetched and sent once on every registered stream as a credentials
// refresh; a failed renewal keeps the token and is retried after 1 s, doubling up to 30 s. A
// stream the hub closes with UNAUTHENTICATED drops the token it presented, so the next stream
// registers with a new one; PERMISSION_DENIED keeps it. When no token can be had, no stream is
// opened: the failure is the latest [Rejection] and the attempt is retried at the reconnect
// backoff.
func JWT(src TokenSource) Option {
	return func(c *config) error {
		if src == nil {
			return errors.New("snodra: JWT needs a token source")
		}
		c.tokenSource = src
		return nil
	}
}

// ClientCredentials are the settings of an OAuth2 client-credentials grant
// (see [NewClientCredentials]). Printing it with %v, %+v, %#v or %s hides the secret; %p and %w
// print the struct raw, and json.Marshal includes ClientSecret.
type ClientCredentials struct {
	// TokenURL is the token endpoint, http:// or https:// (SNODRA_SERVER_OAUTH2_TOKEN_URI).
	TokenURL string
	// ClientID is the OAuth2 client; "" is the service name, which the hub requires as azp
	// (SNODRA_SERVER_OAUTH2_CLIENT_ID).
	ClientID string
	// ClientSecret is the client's secret (SNODRA_SERVER_OAUTH2_CLIENT_SECRET).
	ClientSecret string
	// ClientSecretFile is a file holding the secret, read at every fetch; it wins over
	// ClientSecret (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE).
	ClientSecretFile string
	// Scope is sent with the grant when set, space-separated (SNODRA_SERVER_OAUTH2_SCOPE).
	Scope string
	// CAFile is a PEM file of CA certificates trusted for the endpoint, beside the system's
	// (SNODRA_SERVER_OAUTH2_CA).
	CAFile string
}

// String shows the settings, never the secret.
func (c ClientCredentials) String() string {
	return fmt.Sprintf("ClientCredentials{TokenURL: %q, ClientID: %q, ClientSecret: ***, ClientSecretFile: %q, Scope: %q, CAFile: %q}",
		c.TokenURL, c.ClientID, c.ClientSecretFile, c.Scope, c.CAFile)
}

// Format prints String for every verb that reaches a Formatter. %p and %w never reach it (fmt
// prints the struct raw for them, and go vet flags both), nor does encoding/json, which writes
// the exported fields.
func (c ClientCredentials) Format(f fmt.State, _ rune) { printRedacted(f, c.String()) }

// LogValue is String, for log/slog.
func (c ClientCredentials) LogValue() slog.Value { return slog.StringValue(c.String()) }

// NewClientCredentials is a [TokenSource] fetching tokens with the OAuth2 client-credentials
// grant: a POST of grant_type=client_credentials (and scope) to TokenURL, the client
// authenticated with HTTP Basic (id and secret form-urlencoded first, RFC 6749 2.3.1), 10 s at
// most, no redirect followed. The answer's access_token is required, its token_type must be
// Bearer when present, and expires_in gives the expiry (unknown without it). Each call fetches;
// [JWT] keeps the token. The secret and the tokens never appear in an error. [New] checks the
// settings.
func NewClientCredentials(c ClientCredentials) TokenSource {
	c.TokenURL, c.ClientID = strings.TrimSpace(c.TokenURL), strings.TrimSpace(c.ClientID)
	c.ClientSecret, c.ClientSecretFile = strings.TrimSpace(c.ClientSecret), strings.TrimSpace(c.ClientSecretFile)
	c.Scope, c.CAFile = strings.TrimSpace(c.Scope), strings.TrimSpace(c.CAFile)
	s := newSecret(c.ClientSecret)
	c.ClientSecret = "" // held as a secret only
	return &clientCredentials{cfg: c, secret: s}
}

// The most of a token response that is read, and the most of an error text that is kept.
const (
	maxTokenResponse = 1 << 20
	errorClip        = 200
)

// clientCredentials is the [TokenSource] of [NewClientCredentials].
type clientCredentials struct {
	cfg    ClientCredentials // without ClientSecret
	secret *secret           // ClientSecret

	once sync.Once
	hc   *http.Client
	err  error

	observe func(result string) // reports each fetch's result; nil: none
}

// String shows the settings, never the secret.
func (cc *clientCredentials) String() string { return cc.cfg.String() }

// Format prints String for every verb that reaches a Formatter (not %p or %w: see printRedacted).
func (cc *clientCredentials) Format(f fmt.State, _ rune) { printRedacted(f, cc.String()) }

// LogValue is String, for log/slog.
func (cc *clientCredentials) LogValue() slog.Value { return slog.StringValue(cc.String()) }

// prepared checks the settings for service and builds the HTTP client: a copy with ClientID
// defaulted to service.
func (cc *clientCredentials) prepared(service string) (*clientCredentials, error) {
	c := cc.cfg
	if c.ClientID == "" {
		c.ClientID = service
	}
	if !httpURL(c.TokenURL) {
		return nil, fmt.Errorf("snodra: ClientCredentials.TokenURL (SNODRA_SERVER_OAUTH2_TOKEN_URI) must be an http:// or https:// URL, got %q", c.TokenURL)
	}
	switch {
	case c.ClientSecretFile != "":
		s, err := readTokenFile(c.ClientSecretFile)
		if err != nil {
			return nil, fmt.Errorf("snodra: ClientCredentials.ClientSecretFile (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE) '%s' cannot be read: %w", c.ClientSecretFile, err)
		}
		if s == "" {
			return nil, fmt.Errorf("snodra: ClientCredentials.ClientSecretFile (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE) '%s' is empty", c.ClientSecretFile)
		}
	case cc.secret == nil:
		return nil, errors.New("snodra: ClientCredentials needs a client secret: set ClientSecret (SNODRA_SERVER_OAUTH2_CLIENT_SECRET) or ClientSecretFile (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE)")
	}
	out := &clientCredentials{cfg: c, secret: cc.secret}
	if _, err := out.client(); err != nil {
		return nil, fmt.Errorf("snodra: ClientCredentials.CAFile (SNODRA_SERVER_OAUTH2_CA) '%s' %w", c.CAFile, err)
	}
	return out, nil
}

// client is the HTTP client of the endpoint, built once: the CA file added to the system's
// roots, the fetch timeout, no redirect (the Basic credentials go to the configured endpoint
// only).
func (cc *clientCredentials) client() (*http.Client, error) {
	cc.once.Do(func() {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if cc.cfg.CAFile != "" {
			pool, err := systemRootsWith(cc.cfg.CAFile)
			if err != nil {
				cc.err = err
				return
			}
			transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
		cc.hc = &http.Client{
			Timeout:       tokenFetchTimeout,
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	})
	return cc.hc, cc.err
}

// plainHTTPOverANetwork is true for an http:// endpoint that is not loopback or a single-label
// host (a compose service): the secret then crosses a network unencrypted.
func (cc *clientCredentials) plainHTTPOverANetwork() bool {
	u, err := url.Parse(cc.cfg.TokenURL)
	if err != nil || u.Scheme != "http" {
		return false
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsLoopback()
	}
	return !strings.EqualFold(host, "localhost") && strings.Contains(strings.TrimSuffix(host, "."), ".")
}

func httpURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

// Token fetches a token, and reports the fetch's result when the app has telemetry.
func (cc *clientCredentials) Token(ctx context.Context) (AccessToken, error) {
	tok, result, err := cc.fetch(ctx)
	if cc.observe != nil {
		cc.observe(result)
	}
	return tok, err
}

// The results of a fetch, the label of the telemetry's token fetch counter.
const (
	fetchOK          = "ok"
	fetchHTTPError   = "http_error"
	fetchOAuthError  = "oauth_error"
	fetchIOError     = "io_error"
	fetchBadResponse = "bad_response"
)

// fetch fetches a token and says how it went: ok; an io_error when the request did not complete
// (or the secret could not be read); an oauth_error for an answer with an OAuth2 error body, an
// http_error for another non-success status; a bad_response for a success answer that is no
// usable token response.
func (cc *clientCredentials) fetch(ctx context.Context) (AccessToken, string, error) {
	hc, err := cc.client()
	if err != nil {
		return AccessToken{}, fetchIOError, fmt.Errorf("OAuth2 CA file '%s' %w", cc.cfg.CAFile, err)
	}
	secretValue := cc.secret.reveal()
	if f := cc.cfg.ClientSecretFile; f != "" {
		if secretValue, err = readTokenFile(f); err != nil {
			return AccessToken{}, fetchIOError, fmt.Errorf("OAuth2 client secret file '%s' cannot be read: %w", f, err)
		}
	}
	if secretValue == "" {
		return AccessToken{}, fetchIOError, errors.New("no OAuth2 client secret (the client secret file is empty)")
	}
	credentials := formEncode(cc.cfg.ClientID) + ":" + formEncode(secretValue)
	basic := base64.StdEncoding.EncodeToString([]byte(credentials))
	// What an endpoint might echo back in an error text, masked before the text is clipped.
	mask := masker{"Basic " + basic, basic, credentials, formEncode(secretValue), secretValue}
	body := "grant_type=client_credentials"
	if cc.cfg.Scope != "" {
		body += "&scope=" + formEncode(cc.cfg.Scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cc.cfg.TokenURL, strings.NewReader(body))
	if err != nil {
		return AccessToken{}, fetchIOError, mask.error("OAuth2 token endpoint: " + err.Error())
	}
	req.Header.Set("Authorization", "Basic "+basic)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // without the URL
		}
		return AccessToken{}, fetchIOError, mask.error("OAuth2 token endpoint: " + err.Error())
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponse+1))
	if err != nil {
		return AccessToken{}, fetchIOError, mask.error("OAuth2 token endpoint: " + err.Error())
	}
	if len(raw) > maxTokenResponse {
		return AccessToken{}, fetchBadResponse, mask.error(fmt.Sprintf("OAuth2 token endpoint answered with more than %d bytes", maxTokenResponse))
	}
	var doc map[string]any
	parsed := json.Unmarshal(raw, &doc) == nil && doc != nil
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return AccessToken{}, refusalResult(doc), refusal(resp, doc, mask)
	}
	bad := func(what string) error { return mask.error("OAuth2 token endpoint response " + what) }
	if !parsed {
		return AccessToken{}, fetchBadResponse, bad("is not valid JSON")
	}
	value, _ := doc["access_token"].(string)
	if value == "" {
		return AccessToken{}, fetchBadResponse, bad("has no access_token")
	}
	if kind, present := doc["token_type"]; present {
		k, _ := kind.(string)
		if !strings.EqualFold(k, "bearer") {
			return AccessToken{}, fetchBadResponse, bad(fmt.Sprintf("has token_type '%s', not Bearer", k))
		}
	}
	tok := AccessToken{Value: value}
	switch v := doc["expires_in"].(type) {
	case nil: // unknown: the default lifetime
	default:
		secs, ok := seconds(v)
		if !ok {
			return AccessToken{}, fetchBadResponse, bad("has an expires_in that is not a positive number of seconds")
		}
		tok.Expiry = time.Now().Add(time.Duration(secs * float64(time.Second)))
	}
	return tok, fetchOK, nil
}

// refusalResult is the fetch result of a non-success answer with the body doc: oauth_error when
// it names an OAuth2 error, else http_error.
func refusalResult(doc map[string]any) string {
	if code, _ := doc["error"].(string); code != "" {
		return fetchOAuthError
	}
	return fetchHTTPError
}

// seconds reads expires_in: a number or a numeric string, finite, at least 1, capped at ten
// years.
func seconds(v any) (float64, bool) {
	var f float64
	switch x := v.(type) {
	case float64:
		f = x
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}
		f = n
	default:
		return 0, false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f < 1 {
		return 0, false
	}
	return min(f, 10*365*86400), true
}

// refusal is the error of a non-success answer: the OAuth2 error body when there is one, else
// the status.
func refusal(resp *http.Response, doc map[string]any, mask masker) error {
	field := func(name string) string { s, _ := doc[name].(string); return s }
	code, desc := field("error"), field("error_description")
	switch {
	case code != "" && desc != "":
		return mask.error(fmt.Sprintf("OAuth2 token endpoint refused (HTTP %d): %s: %s", resp.StatusCode, code, desc))
	case code != "":
		return mask.error(fmt.Sprintf("OAuth2 token endpoint refused (HTTP %d): %s", resp.StatusCode, code))
	}
	return mask.error("OAuth2 token endpoint answered HTTP " + resp.Status)
}

// masker hides the client credentials in a text an endpoint sent back (some echo the request).
type masker []string

func (m masker) error(text string) error {
	for _, needle := range m {
		if len(needle) >= 3 { // shorter would turn any text into asterisks
			text = strings.ReplaceAll(text, needle, "***")
		}
	}
	return errors.New(clip(text))
}

// clip turns control characters into spaces (a server's text must not forge a log line) and
// cuts a text longer than errorClip characters with an ellipsis.
func clip(text string) string {
	r := []rune(strings.Map(func(c rune) rune {
		if unicode.IsControl(c) {
			return ' '
		}
		return c
	}, text))
	if len(r) <= errorClip {
		return string(r)
	}
	return string(r[:errorClip-1]) + "…"
}

// formEncode is the application/x-www-form-urlencoded form of one value: letters, digits and
// *-._ as they are, a space as +, every other byte as %XX.
func formEncode(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '*', c == '-', c == '.', c == '_':
			b.WriteByte(c)
		case c == ' ':
			b.WriteByte('+')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// refreshMargin is how long before its expiry a token of lifetime is replaced: a fifth of it,
// at least minRefreshMargin, at most half of it.
// TODO(spec-gap): SG-5 (the specification says "at 80 % of the token's lifetime").
func refreshMargin(lifetime time.Duration) time.Duration {
	return min(max(lifetime/5, minRefreshMargin), lifetime/2)
}

// refreshAt is when, after its fetch, a token of lifetime is renewed.
func refreshAt(lifetime time.Duration) time.Duration { return lifetime - refreshMargin(lifetime) }

// issuedToken is an access token the provider fetched.
type issuedToken struct {
	secret   *secret   // the token
	issued   time.Time // with the monotonic clock
	lifetime time.Duration
	expiry   time.Time // wall clock only
	gen      uint64    // 1, 2, ... in fetch order
}

// value is the token, to put on the wire.
func (t *issuedToken) value() string { return t.secret.reveal() }

// String shows the token's generation and expiry, never its value.
func (t *issuedToken) String() string {
	return fmt.Sprintf("issuedToken{gen: %d, expiry: %s}", t.gen, t.expiry.Format(time.RFC3339))
}

// Format prints String for every verb that reaches a Formatter (not %p or %w: see printRedacted).
func (t *issuedToken) Format(f fmt.State, _ rune) { printRedacted(f, t.String()) }

// LogValue is String, for log/slog.
func (t *issuedToken) LogValue() slog.Value { return slog.StringValue(t.String()) }

// remaining is how long the token still lives: the lesser of what the monotonic clock says
// (immune to the wall clock being set) and what the wall clock says (immune to a suspended
// machine, whose monotonic clock may stand still).
func (t *issuedToken) remaining() time.Duration {
	return min(t.lifetime-time.Since(t.issued), time.Until(t.expiry))
}

// untilRefresh is how long until the token's renewal margin begins.
func (t *issuedToken) untilRefresh() time.Duration {
	return t.remaining() - refreshMargin(t.lifetime)
}

// fresh is true while more than the renewal margin is left.
func (t *issuedToken) fresh() bool { return t.untilRefresh() > 0 }

// tokenFlight is a fetch in progress, shared by everyone who needs a new token meanwhile; tok
// and err are set before done is closed.
type tokenFlight struct {
	done chan struct{}
	tok  *issuedToken
	err  error
}

// tokenProvider holds the instance's access token for every hub connection: the token while it
// is fresh, one fetch shared by concurrent callers, the renewal goroutine, and the tokens
// published to the streams.
type tokenProvider struct {
	src TokenSource
	log *slog.Logger

	mu        sync.Mutex
	current   *issuedToken  // the token to hand out; nil after a hub refused it
	published *issuedToken  // the newest token fetched, which the streams follow
	changed   chan struct{} // closed, and replaced, when a token is published
	flight    *tokenFlight
	gen       uint64
	renewing  bool // the renewal goroutine runs
	warned    bool // the unknown expiry was warned about
	stopped   bool
	stop      chan struct{}
	stopCtx   context.Context // ends at close: the fetches and the renewal's wait
	cancel    context.CancelFunc
	wait      time.Duration // the longest anyone waits for a fetch
	renewDone chan struct{} // closed when the renewal goroutine has returned
}

func newTokenProvider(src TokenSource, log *slog.Logger, wait time.Duration) *tokenProvider {
	ctx, cancel := context.WithCancel(context.Background())
	return &tokenProvider{src: src, log: log, changed: make(chan struct{}), stop: make(chan struct{}),
		stopCtx: ctx, cancel: cancel, wait: wait, renewDone: make(chan struct{})}
}

// String names the provider, never a token.
func (p *tokenProvider) String() string { return "tokenProvider{}" }

// Format prints String for every verb that reaches a Formatter (not %p or %w: see printRedacted).
func (p *tokenProvider) Format(f fmt.State, _ rune) { printRedacted(f, p.String()) }

// LogValue is String, for log/slog.
func (p *tokenProvider) LogValue() slog.Value { return slog.StringValue(p.String()) }

var errTokensStopped = errors.New("the access token provider is stopped")

// token is a token to register with: the current one while it is fresh, else a new one. A
// token past its margin is never returned when the fetch fails: the error is.
func (p *tokenProvider) token(ctx context.Context) (*issuedToken, error) {
	p.mu.Lock()
	cur := p.current
	p.mu.Unlock()
	if cur != nil && cur.fresh() {
		return cur, nil
	}
	return p.refresh(ctx)
}

// refresh fetches a new token, joining a fetch under way. The fetch is detached from ctx:
// ctx ending returns ctx's error without failing the others. The wait is bounded by p.wait, for
// a source that ignores its context (the fetch stays shared until it returns).
func (p *tokenProvider) refresh(ctx context.Context) (*issuedToken, error) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil, errTokensStopped
	}
	f := p.flight
	if f == nil {
		f = &tokenFlight{done: make(chan struct{})}
		p.flight = f
		go p.fetch(f)
	}
	p.mu.Unlock()
	timer := time.NewTimer(p.wait)
	defer timer.Stop()
	select {
	case <-f.done:
		return f.tok, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("the token source did not answer within %v", p.wait)
	}
}

// fetch asks the source for a token, bounded by tokenFetchTimeout and the provider's stop, and
// publishes it. The flight ends even if the source panics.
func (p *tokenProvider) fetch(f *tokenFlight) {
	var tok *issuedToken
	var err error
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("the token source panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			tok, err = nil, fmt.Errorf("the token source panicked: %v", r)
		}
		p.mu.Lock()
		p.flight, f.tok, f.err = nil, tok, err
		p.mu.Unlock()
		close(f.done)
	}()
	ctx, cancel := context.WithTimeout(p.stopCtx, tokenFetchTimeout)
	defer cancel()
	at, err := p.src.Token(ctx)
	if err != nil {
		return
	}
	tok, err = p.publish(at, time.Now())
}

// publish makes at the current and published token, issued at now.
func (p *tokenProvider) publish(at AccessToken, now time.Time) (*issuedToken, error) {
	if at.Value == "" {
		return nil, errors.New("the token source gave no access token")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	lifetime := defaultTokenLifetime
	if at.Expiry.IsZero() {
		if !p.warned {
			p.warned = true
			p.log.Warn("the access token has no expiry; assuming the default lifetime", "lifetime", defaultTokenLifetime)
		}
	} else if lifetime = at.Expiry.Sub(now); lifetime <= 0 {
		return nil, errors.New("the access token from the token source has already expired")
	}
	if prev := p.published; prev != nil && prev.value() == at.Value {
		// The source gave the token it gave before (it caches): not a renewal to send again.
		tok := &issuedToken{secret: hide(at.Value), issued: now, lifetime: lifetime, expiry: now.Add(lifetime).Round(0), gen: prev.gen}
		p.current, p.published = tok, tok
		return tok, nil
	}
	p.gen++
	tok := &issuedToken{secret: hide(at.Value), issued: now, lifetime: lifetime, expiry: now.Add(lifetime).Round(0), gen: p.gen}
	p.current, p.published = tok, tok
	close(p.changed)
	p.changed = make(chan struct{})
	if !p.renewing && !p.stopped {
		p.renewing = true
		go p.renew()
	}
	return tok, nil
}

// follow is the newest token fetched (nil before the first) and a channel closed when a newer
// one is published.
func (p *tokenProvider) follow() (*issuedToken, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.published, p.changed
}

// invalidate drops value, which a hub refused as unauthenticated, when it is still the current
// token: the next stream fetches a new one. A newer token is left alone.
func (p *tokenProvider) invalidate(value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current != nil && p.current.value() == value {
		p.current = nil
	}
}

// close stops the renewal and every fetch for good.
func (p *tokenProvider) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stopped {
		p.stopped = true
		close(p.stop)
		p.cancel()
	}
}

// renew is the renewal goroutine: it waits for the current token's margin and fetches; after a
// failure it retries after tokenRetryMin, doubling up to tokenRetryMax, keeping the token it
// has. It wakes at least every tokenWakeInterval to look at the clocks again.
func (p *tokenProvider) renew() {
	defer close(p.renewDone)
	failures := 0
	logged := "" // the failure logged at warn last; a repeat goes to debug
	for {
		wait := tokenRetryMin
		if failures > 0 {
			wait = retryDelay(failures)
		} else {
			p.mu.Lock()
			if cur := p.current; cur != nil {
				wait = max(cur.untilRefresh(), tokenRetryMin)
			}
			p.mu.Unlock()
		}
		timer := time.NewTimer(min(wait, tokenWakeInterval))
		select {
		case <-timer.C:
		case <-p.stop:
			timer.Stop()
			return
		}
		p.mu.Lock()
		cur := p.current
		p.mu.Unlock()
		if cur != nil && cur.fresh() { // not due yet, or replaced by a stream's own fetch
			failures, logged = 0, ""
			continue
		}
		tok, err := p.refresh(p.stopCtx)
		switch {
		case errors.Is(err, errTokensStopped) || p.stopCtx.Err() != nil:
			return
		case err != nil:
			failures++
			level := slog.LevelWarn
			if err.Error() == logged {
				level = slog.LevelDebug
			}
			logged = err.Error()
			p.log.Log(context.Background(), level, "the access token renewal failed; keeping the current token",
				"reason", err.Error(), "retry_in", retryDelay(failures))
		default:
			if failures > 0 {
				p.log.Info("the access token is renewed again", "lifetime", tok.lifetime)
			}
			failures, logged = 0, ""
		}
	}
}

// retryDelay is the wait after the n-th consecutive failed renewal: 1 s, 2 s, 4 s ... 30 s.
func retryDelay(n int) time.Duration {
	d := tokenRetryMin << min(n-1, 8)
	return min(d, tokenRetryMax)
}

// noTokenError is a connection attempt that opened no stream: no access token could be had.
type noTokenError struct{ msg string }

func (e *noTokenError) Error() string { return e.msg }

// resolveToken fixes the static token of the stream about to open: the one in the token file
// (read now; when it cannot be read or is empty, the last one read is kept, an empty file being
// warned about once), else the configured one. Called from the connection's goroutine only.
func (c *hubConn) resolveToken() error {
	cfg := c.app.cfg
	if cfg.tokenFile == "" {
		c.token = cfg.token
		return nil
	}
	tok, err := readTokenFile(cfg.tokenFile)
	switch {
	case err != nil && c.token == nil:
		return status.Errorf(codes.FailedPrecondition, "the hub token file '%s' cannot be read: %v", cfg.tokenFile, err)
	case err != nil:
		c.log.Warn("the hub token file cannot be read; keeping the last token", "path", cfg.tokenFile, "error", err.Error())
	case tok == "":
		// A non-atomic rotation caught between the truncate and the write.
		if c.token != nil && !c.emptyWarned {
			c.emptyWarned = true
			c.log.Warn("the hub token file is empty (a rotation in progress?); keeping the last token", "path", cfg.tokenFile)
		}
	default:
		c.token, c.emptyWarned = hide(tok), false
	}
	return nil
}

// followTokenFile sends a static token that changed in its file to the registered stream as a
// credentials refresh (a rotation without a reconnect), as the reference libraries do. A file
// that cannot be read or is empty changes nothing.
// TODO(spec-gap): SG-20 (a rotated static token on a live stream).
func (c *hubConn) followTokenFile(ref *streamRef) {
	path := c.app.cfg.tokenFile
	if path == "" {
		return
	}
	tok, err := readTokenFile(path)
	if err != nil || tok == "" || tok == c.token.reveal() {
		return
	}
	c.token, c.emptyWarned = hide(tok), false
	if ref.send(credentialsFrame(&hubpb.Credentials{Kind: &hubpb.Credentials_Token{Token: tok}})) == nil {
		c.app.tel.credentialsRefreshSent()
		c.log.Debug("the hub token file changed; refreshed on the hub stream")
	}
}

// followToken takes tok, the provider's newest token: later Registers carry it, and a
// registered stream that has not had it yet gets it once as a credentials refresh (before the
// ack, the ack sends it).
func (c *hubConn) followToken(ref *streamRef, registered bool, tok *issuedToken) {
	if tok == nil || tok.gen <= c.sentGen {
		return
	}
	c.jwt = tok
	if !registered {
		return
	}
	c.sentGen = tok.gen
	if ref.send(credentialsFrame(&hubpb.Credentials{Kind: &hubpb.Credentials_Jwt{Jwt: tok.value()}})) == nil {
		c.app.tel.credentialsRefreshSent()
		c.presentedJWT = tok.secret
		c.log.Debug("the access token is refreshed on the hub stream", "expires_at", tok.expiry)
	}
}

// retirePresentedToken drops the access token the stream presented, which the hub turned down
// as unauthenticated: the next stream registers with a new one. Not for PERMISSION_DENIED, which
// a new token would not change.
func (c *hubConn) retirePresentedToken() {
	if p := c.app.tokens; p != nil && c.presentedJWT != nil {
		p.invalidate(c.presentedJWT.reveal())
		c.presentedJWT = nil
	}
}

// credentials is what a Register carries: the access token, else the static token; nil for none.
func (c *hubConn) credentials() *hubpb.Credentials {
	switch {
	case c.jwt != nil:
		return &hubpb.Credentials{Kind: &hubpb.Credentials_Jwt{Jwt: c.jwt.value()}}
	case c.token != nil:
		return &hubpb.Credentials{Kind: &hubpb.Credentials_Token{Token: c.token.reveal()}}
	}
	return nil
}

// emptyRegister is the Register with no types (a shutdown, a rollover, an address that left the
// DNS answer), with the credentials, offering the permits the stream offered so far.
func (c *hubConn) emptyRegister(offered uint32) *hubpb.Register {
	r := c.app.emptyRegisterMsg()
	r.InitialPermits = offered
	r.Credentials = c.credentials()
	return r
}

// register is the Register of the connection's streams offering permits: the instance's types,
// none once the connection is withdrawn (its address left the DNS answer).
func (c *hubConn) register(permits uint32) *hubpb.Register {
	if c.withdrawn.Load() {
		return c.emptyRegister(permits)
	}
	r := c.app.registerMsg()
	r.InitialPermits = permits
	r.Credentials = c.credentials()
	return r
}

func credentialsFrame(cr *hubpb.Credentials) *hubpb.ClientMessage {
	return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Credentials{Credentials: cr}}
}
