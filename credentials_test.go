package snodra

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
)

const testToken = "s3cr3t-token"

// lockedBuffer is a bytes.Buffer safe for the logger's goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// writeFile replaces path's content atomically, as a secret mount does.
func writeFile(t testing.TB, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func TestTheTokenIsOnEveryStream(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	startApp(t, Hubs(a.Addr(), b.Addr()), Token(testToken))
	sa, sb := a.Next(t), b.Next(t)
	is(t, sa.RecvRegister(t).GetCredentials().GetToken(), testToken)
	is(t, sb.RecvRegister(t).GetCredentials().GetToken(), testToken)
	sa.Close(codes.Unavailable, "restart")
	is(t, a.Next(t).RecvRegister(t).GetCredentials().GetToken(), testToken)
}

func TestTokenIsNeverLogged(t *testing.T) {
	var buf lockedBuffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), Token(testToken), Logger(log))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetToken(), testToken)
	s.Close(codes.Unauthenticated, "token not accepted")
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	rej, _ := app.Rejection()

	_, errBoth := New(Service("orders-service"), Hubs(hub.Addr()), Token(testToken), JWT(staticSource{}))
	_, errFile := New(Service("orders-service"), Hubs(hub.Addr()), TokenFile(filepath.Join(t.TempDir(), "absent")))
	for what, text := range map[string]string{
		"the log":            buf.String(),
		"the config":         fmt.Sprintf("%+v", app.cfg),
		"the config (%#v)":   fmt.Sprintf("%#v", app.cfg),
		"the rejection":      fmt.Sprintf("%+v", rej),
		"a New error":        fmt.Sprint(errBoth),
		"a token file error": fmt.Sprint(errFile),
	} {
		if strings.Contains(text, testToken) {
			t.Fatalf("%s shows the token: %s", what, text)
		}
	}
	if !strings.Contains(buf.String(), "token not accepted") {
		t.Fatal("the refusal was not logged at all (the test would prove nothing)")
	}
}

func TestTokenFileIsReadBeforeEveryConnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	writeFile(t, path, "t-1\n")
	rec := &logRecorder{}
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), TokenFile(path), Logger(slog.New(rec)))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetToken(), "t-1")
	s.Ack(t, fakehub.Ack("hub-1"))
	writeFile(t, path, "t-2")
	s.Close(codes.Unavailable, "restart")
	s = hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetToken(), "t-2")
	s.Ack(t, fakehub.Ack("hub-1"))
	writeFile(t, path, "") // a rotation caught half way
	s.Close(codes.Unavailable, "restart")
	s = hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetToken(), "t-2")
	s.Ack(t, fakehub.Ack("hub-1"))
	s.Close(codes.Unavailable, "restart")
	is(t, hub.Next(t).RecvRegister(t).GetCredentials().GetToken(), "t-2")
	const warning = "the hub token file is empty (a rotation in progress?); keeping the last token"
	is(t, rec.count(slog.LevelWarn, warning), 1)
}

func TestAnEmptyOrMissingTokenFileIsRefusedAtStart(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	writeFile(t, empty, " \n")
	_, err := New(Service("orders-service"), Hubs("hub:7000"), TokenFile(empty))
	if err == nil || !strings.Contains(err.Error(), "is empty") || !strings.Contains(err.Error(), empty) {
		t.Fatalf("an empty token file: %v", err)
	}
	_, err = New(Service("orders-service"), Hubs("hub:7000"), TokenFile(filepath.Join(dir, "absent")))
	if err == nil || !strings.Contains(err.Error(), "cannot be read") {
		t.Fatalf("a missing token file: %v", err)
	}
}

func TestARotatedTokenFileReachesTheLiveStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	writeFile(t, path, "t-1")
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), TokenFile(path))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetToken(), "t-1")
	s.Ack(t, ackEvery("hub-1", 50*ms))
	writeFile(t, path, "t-2")
	is(t, s.RecvCredentials(t).GetToken(), "t-2")
	s.Nothing(t, 300*ms, "credentials") // once per change
	writeFile(t, path, "")              // an empty file changes nothing
	s.Nothing(t, 200*ms, "credentials")
	s.Recv(t) // the stream is alive
}

func TestRefreshAt(t *testing.T) { // SG-5: margin max(20 %, 5 s), at most half the lifetime
	is(t, refreshAt(100*time.Second), 80*time.Second)
	is(t, refreshAt(60*time.Second), 48*time.Second)
	is(t, refreshAt(10*time.Second), 5*time.Second)
	is(t, refreshAt(8*time.Second), 4*time.Second)
}

// staticSource always gives the same token, valid for an hour.
type staticSource struct{}

func (staticSource) Token(context.Context) (AccessToken, error) {
	return AccessToken{Value: "jwt-static", Expiry: time.Now().Add(time.Hour)}, nil
}

// tokenEndpoint is an OAuth2 token endpoint issuing "jwt-1", "jwt-2", ... for expires_in
// seconds, checking the client-credentials request.
type tokenEndpoint struct {
	*httptest.Server
	t         testing.TB
	expiresIn atomic.Int64
	status    atomic.Int64
	fetches   atomic.Int64
	bad       atomic.Value // the first malformed request, as a string
}

const (
	endpointClient = "orders-service"
	endpointSecret = "s3:cr/t pass"
	endpointScope  = "hub.connect"
)

func newTokenEndpoint(t testing.TB, expiresIn int64) *tokenEndpoint {
	t.Helper()
	e := &tokenEndpoint{t: t}
	e.expiresIn.Store(expiresIn)
	e.status.Store(http.StatusOK)
	// form-urlencoded before base64 (RFC 6749 2.3.1)
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("orders-service:s3%3Acr%2Ft+pass"))
	e.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch {
		case r.Method != http.MethodPost:
			e.bad.CompareAndSwap(nil, "method "+r.Method)
		case r.Header.Get("Authorization") != basic:
			e.bad.CompareAndSwap(nil, "authorization "+r.Header.Get("Authorization"))
		case r.Header.Get("Content-Type") != "application/x-www-form-urlencoded":
			e.bad.CompareAndSwap(nil, "content type "+r.Header.Get("Content-Type"))
		case r.PostForm.Get("grant_type") != "client_credentials" || r.PostForm.Get("scope") != endpointScope:
			e.bad.CompareAndSwap(nil, "form "+r.PostForm.Encode())
		}
		n := e.fetches.Add(1)
		if code := int(e.status.Load()); code != http.StatusOK {
			http.Error(w, "boom", code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": fmt.Sprintf("jwt-%d", n), "token_type": "Bearer", "expires_in": e.expiresIn.Load(),
		})
	}))
	t.Cleanup(e.Close)
	t.Cleanup(func() {
		if v := e.bad.Load(); v != nil {
			t.Errorf("the token request was malformed: %v", v)
		}
	})
	return e
}

func (e *tokenEndpoint) source() TokenSource {
	return NewClientCredentials(ClientCredentials{
		TokenURL: e.URL + "/token", ClientID: endpointClient, ClientSecret: endpointSecret, Scope: endpointScope,
	})
}

func TestTheJWTRegistersAndEachRenewalIsSentOnce(t *testing.T) {
	e := newTokenEndpoint(t, 2)
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), JWT(e.source()))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetJwt(), "jwt-1")
	s.Ack(t, ackEvery("hub-1", 200*ms))
	seen := map[string]bool{"jwt-1": true}
	renewals := 0
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		m := s.Recv(t)
		c := m.GetCredentials()
		if c == nil {
			continue
		}
		if seen[c.GetJwt()] {
			t.Fatalf("token %q sent again", c.GetJwt())
		}
		seen[c.GetJwt()] = true
		renewals++
	}
	if renewals < 3 {
		t.Fatalf("%d renewals of a 2 s token in 5 s, want at least 3", renewals)
	}
	if f := e.fetches.Load(); f > int64(renewals)+2 { // +1, and one maybe in flight at the end
		t.Fatalf("%d fetches for %d renewals", f, renewals)
	}
}

func TestAnUnauthenticatedCloseRegistersWithANewlyFetchedToken(t *testing.T) {
	e := newTokenEndpoint(t, 3600)
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), JWT(e.source()))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetJwt(), "jwt-1")
	s.Ack(t, fakehub.Ack("hub-1"))
	s.Close(codes.Unauthenticated, "credentials expired")
	is(t, hub.Next(t).RecvRegister(t).GetCredentials().GetJwt(), "jwt-2")
	is(t, e.fetches.Load(), int64(2))
}

func TestAPermissionDeniedCloseFetchesNothing(t *testing.T) {
	e := newTokenEndpoint(t, 3600)
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), JWT(e.source()))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetJwt(), "jwt-1")
	s.Ack(t, fakehub.Ack("hub-1"))
	s.Close(codes.PermissionDenied, "no ACL entry")
	is(t, hub.Next(t).RecvRegister(t).GetCredentials().GetJwt(), "jwt-1")
	is(t, e.fetches.Load(), int64(1))
}

func TestATokenEndpointFailureIsExposedAndRetried(t *testing.T) { // SG-7
	e := newTokenEndpoint(t, 3600)
	e.status.Store(http.StatusInternalServerError)
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), JWT(e.source()), ReconnectBackoff(50*ms, 2*s))
	hub.NoStreamWithin(t, 300*ms)
	rej, ok := app.Rejection()
	if !ok || !strings.Contains(rej.Message, "access token") || !strings.Contains(rej.Message, "HTTP 500") {
		t.Fatalf("rejection %+v, %v", rej, ok)
	}
	is(t, rej.Code, "UNAUTHENTICATED")
	// Retried at the ordinary backoff (50, 100, 200 ms ...), not at the cap (1.6 to 2 s).
	if f := e.fetches.Load(); f < 3 {
		t.Fatalf("%d fetches in 300 ms", f)
	}
	e.status.Store(http.StatusOK)
	tok := hub.Next(t).RecvRegister(t).GetCredentials().GetJwt()
	if !strings.HasPrefix(tok, "jwt-") {
		t.Fatalf("registered with %q", tok)
	}
}

func TestOneCredentialKind(t *testing.T) {
	for _, c := range []struct {
		what       string
		opts       []Option
		name, also string
	}{
		{"token and JWT", []Option{Token("x"), JWT(staticSource{})}, "Token", "JWT"},
		{"token file and JWT", []Option{TokenFile("/nowhere"), JWT(staticSource{})}, "TokenFile", "JWT"},
		{"token and certificate", []Option{Token("x"), ClientCertificate("c.pem", "k.pem")}, "Token", "ClientCertificate"},
		{"JWT and certificate", []Option{JWT(staticSource{}), ClientCertificate("c.pem", "k.pem")}, "JWT", "ClientCertificate"},
	} {
		_, err := New(append([]Option{Service("orders-service"), Hubs("hub:7000")}, c.opts...)...)
		if err == nil || !strings.Contains(err.Error(), c.name) || !strings.Contains(err.Error(), c.also) {
			t.Fatalf("%s: %v", c.what, err)
		}
	}
}

func TestClientCredentialsAreChecked(t *testing.T) {
	dir := t.TempDir()
	emptySecret := filepath.Join(dir, "secret")
	writeFile(t, emptySecret, "\n")
	for _, c := range []struct {
		what string
		cc   ClientCredentials
		want string
	}{
		{"no URL", ClientCredentials{ClientSecret: "x"}, "http:// or https://"},
		{"another scheme", ClientCredentials{TokenURL: "ftp://idp/token", ClientSecret: "x"}, "http:// or https://"},
		{"no secret", ClientCredentials{TokenURL: "http://idp/token"}, "client secret"},
		{"an empty secret file", ClientCredentials{TokenURL: "http://idp/token", ClientSecretFile: emptySecret}, "is empty"},
		{"a missing CA", ClientCredentials{TokenURL: "https://idp/token", ClientSecret: "x", CAFile: filepath.Join(dir, "absent")}, "cannot be read"},
	} {
		_, err := New(Service("orders-service"), Hubs("hub:7000"), JWT(NewClientCredentials(c.cc)))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.what, err)
		}
	}
}

func TestTheClientIDDefaultsToTheServiceName(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _, _ := r.BasicAuth()
		got.Store(id)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "jwt-1", "expires_in": 60})
	}))
	t.Cleanup(srv.Close)
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), JWT(NewClientCredentials(ClientCredentials{TokenURL: srv.URL, ClientSecret: "x"})))
	is(t, hub.Next(t).RecvRegister(t).GetCredentials().GetJwt(), "jwt-1")
	is(t, got.Load(), any("orders-service"))
}

func TestTokenEndpointErrorsNeverShowTheSecret(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "invalid_client", "error_description": "bad credentials " + r.Header.Get("Authorization") + " for " + endpointSecret + "\nforged line",
		})
	}))
	t.Cleanup(srv.Close)
	src := NewClientCredentials(ClientCredentials{TokenURL: srv.URL, ClientID: endpointClient, ClientSecret: endpointSecret})
	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("no error")
	}
	msg := err.Error()
	if strings.Contains(msg, endpointSecret) || strings.Contains(msg, "czM") || strings.Contains(msg, "\n") {
		t.Fatalf("the error shows the secret or a line break: %q", msg)
	}
	if !strings.Contains(msg, "HTTP 401") || !strings.Contains(msg, "invalid_client") {
		t.Fatalf("the error does not say what the endpoint said: %q", msg)
	}
	if s := fmt.Sprintf("%+v %v", ClientCredentials{ClientSecret: endpointSecret}, AccessToken{Value: testToken}); strings.Contains(s, endpointSecret) || strings.Contains(s, testToken) {
		t.Fatalf("printing shows a secret: %s", s)
	}
}

func TestATokenResponseIsChecked(t *testing.T) {
	for _, c := range []struct {
		what, body, want string
	}{
		{"no access token", `{"expires_in": 60}`, "no access_token"},
		{"another token type", `{"access_token": "a", "token_type": "MAC"}`, "not Bearer"},
		{"a negative lifetime", `{"access_token": "a", "expires_in": -1}`, "expires_in"},
		{"not JSON", `nope`, "not valid JSON"},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(c.body))
		}))
		_, err := NewClientCredentials(ClientCredentials{TokenURL: srv.URL, ClientID: "a", ClientSecret: "b"}).Token(context.Background())
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.what, err)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token": "a", "expires_in": "30"}`))
	}))
	defer srv.Close()
	tok, err := NewClientCredentials(ClientCredentials{TokenURL: srv.URL, ClientID: "a", ClientSecret: "b"}).Token(context.Background())
	is(t, err, nil)
	within(t, time.Until(tok.Expiry), 29*s, 30*s)
}

// printings are every way fmt and log/slog may show v.
func printings(v any) map[string]string {
	out := map[string]string{}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%d", "%x", "%q"} {
		out[verb] = fmt.Sprintf(verb, v)
	}
	var text, js bytes.Buffer
	slog.New(slog.NewTextHandler(&text, nil)).Info("m", "v", v)
	slog.New(slog.NewJSONHandler(&js, nil)).Info("m", "v", v)
	out["slog text"], out["slog json"] = text.String(), js.String()
	return out
}

func TestNothingPrintsASecret(t *testing.T) {
	const secretValue = "TOPSECRET-value"
	src := NewClientCredentials(ClientCredentials{TokenURL: "http://idp/token", ClientID: "a", ClientSecret: secretValue})
	p := newTokenProvider(staticSecretSource{secretValue}, slog.New(slog.DiscardHandler), time.Second)
	t.Cleanup(p.close)
	tok, err := p.token(context.Background())
	is(t, err, nil)
	app := newApp(t, Hubs("hub:7000"), Token(secretValue))
	conn := newHubConn(app, "hub:7000")
	conn.token, conn.presentedJWT, conn.jwt = newSecret(secretValue), newSecret(secretValue), tok
	id := &clientIdentity{cert: func() tls.Certificate { return tls.Certificate{} }, service: "orders-service"}
	for what, v := range map[string]any{
		"the client-credentials source": src,
		"the settings":                  ClientCredentials{ClientSecret: secretValue},
		"an access token":               AccessToken{Value: secretValue},
		"a secret":                      newSecret(secretValue),
		"an issued token":               tok,
		"the token provider":            p,
		"a connection":                  conn,
		"the config":                    app.cfg,
		"the app":                       app,
		"a certificate pair":            id,
	} {
		for how, text := range printings(v) {
			if strings.Contains(text, secretValue) || strings.Contains(text, fmt.Sprintf("%x", secretValue)) {
				t.Fatalf("%s printed with %s shows the secret: %s", what, how, text)
			}
		}
	}
}

// staticSecretSource gives v as the token, for an hour.
type staticSecretSource struct{ v string }

func (s staticSecretSource) Token(context.Context) (AccessToken, error) {
	return AccessToken{Value: s.v, Expiry: time.Now().Add(time.Hour)}, nil
}

// blockingSource answers its first `answers` calls with a token living `life`, then blocks,
// ignoring its context, until the test ends.
type blockingSource struct {
	answers int64
	life    time.Duration
	calls   atomic.Int64
	release chan struct{}
}

func newBlockingSource(t *testing.T, answers int64, life time.Duration) *blockingSource {
	b := &blockingSource{answers: answers, life: life, release: make(chan struct{})}
	t.Cleanup(func() { close(b.release) })
	return b
}

func (b *blockingSource) Token(context.Context) (AccessToken, error) {
	if n := b.calls.Add(1); n <= b.answers {
		return AccessToken{Value: fmt.Sprintf("jwt-%d", n), Expiry: time.Now().Add(b.life)}, nil
	}
	<-b.release
	return AccessToken{}, errors.New("released")
}

func TestATokenSourceThatNeverAnswersIsATokenFailure(t *testing.T) { // SG-7
	src := newBlockingSource(t, 0, 0)
	tm := scaled()
	tm.tokenFetchWait = 200 * ms
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), JWT(src), withTimings(tm))
	hub.NoStreamWithin(t, 400*ms)
	rej, ok := app.Rejection()
	if !ok || !strings.Contains(rej.Message, "access token") || !strings.Contains(rej.Message, "did not answer") {
		t.Fatalf("rejection %+v, %v", rej, ok)
	}
	start := time.Now()
	is(t, app.Shutdown(ctxWithin(t, 2*time.Second)), nil)
	within(t, time.Since(start), 0, 500*ms)
}

func TestTheRenewalEndsAtShutdownWhenTheSourceHangs(t *testing.T) {
	src := newBlockingSource(t, 1, 2*time.Second) // renewed after 1 s, then the source hangs
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), JWT(src))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetCredentials().GetJwt(), "jwt-1")
	s.Ack(t, fakehub.Ack("hub-1"))
	deadline := time.Now().Add(3 * time.Second)
	for src.calls.Load() < 2 { // the renewal is waiting on the hung source
		if time.Now().After(deadline) {
			t.Fatal("no renewal")
		}
		time.Sleep(10 * ms)
	}
	is(t, app.Shutdown(ctxWithin(t, 3*time.Second)), nil)
	select {
	case <-app.tokens.renewDone:
	case <-time.After(time.Second):
		t.Fatal("the renewal goroutine outlived the shutdown")
	}
}

// panicSource panics on every call.
type panicSource struct{}

func (panicSource) Token(context.Context) (AccessToken, error) { panic("source defect") }

func TestAPanickingTokenSourceIsLoggedWithItsStack(t *testing.T) {
	rec := &logRecorder{}
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()), JWT(panicSource{}), Logger(slog.New(rec)))
	hub.NoStreamWithin(t, 200*ms)
	if rec.count(slog.LevelError, "the token source panicked") == 0 {
		t.Fatal("the panic was not logged at error")
	}
	rec.mu.Lock()
	hasStack := false
	for _, r := range rec.records {
		if r.Message == "the token source panicked" {
			r.Attrs(func(a slog.Attr) bool {
				hasStack = hasStack || (a.Key == "stack" && strings.Contains(a.Value.String(), "goroutine"))
				return true
			})
		}
	}
	rec.mu.Unlock()
	is(t, hasStack, true)
	rej, _ := app.Rejection()
	if !strings.Contains(rej.Message, "source defect") {
		t.Fatalf("rejection %+v", rej)
	}
}
