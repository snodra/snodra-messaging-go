package snodra

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

const testIssuer = "https://issuer.test"

var (
	rsaOnce            sync.Once
	rsaKey, rsaForeign *rsa.PrivateKey
	rsaErr             error
)

// testKeys are the issuer's RSA key and one its JWKS never publishes, made once per run.
func testKeys(t *testing.T) (published, foreign *rsa.PrivateKey) {
	t.Helper()
	rsaOnce.Do(func() {
		if rsaKey, rsaErr = rsa.GenerateKey(rand.Reader, 2048); rsaErr == nil {
			rsaForeign, rsaErr = rsa.GenerateKey(rand.Reader, 2048)
		}
	})
	if rsaErr != nil {
		t.Fatal(rsaErr)
	}
	return rsaKey, rsaForeign
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func jsonSegment(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b64url(b)
}

// mint is an RS256 token of claims under key id kid, signed with key.
func mint(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	input := jsonSegment(t, map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}) + "." + jsonSegment(t, claims)
	sum := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64url(sig)
}

// readableToken is a signed-looking token whose signature nobody checks (a service without a
// JWKS reads its claims unverified).
func readableToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	return jsonSegment(t, map[string]any{"alg": "RS256", "typ": "JWT"}) + "." + jsonSegment(t, claims) + "." + b64url([]byte("signature"))
}

// userClaims are the issuer's claims for sub, expiring after exp, with extra claims added.
func userClaims(sub string, exp time.Duration, extra map[string]any) map[string]any {
	now := time.Now().Unix()
	c := map[string]any{"iss": testIssuer, "sub": sub, "iat": now, "exp": now + int64(exp/time.Second)}
	for k, v := range extra {
		c[k] = v
	}
	return c
}

// testJWKS serves the published key as k1, counting the fetches.
func testJWKS(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	key, _ := testKeys(t)
	var fetches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
			"n": b64url(key.N.Bytes()), "e": b64url(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	t.Cleanup(srv.Close)
	return srv, &fetches
}

// guarded serves orders.PlaceOrder with the guard options given (none: unguarded); the
// handler's CallerFrom goes to the returned channel.
func guarded(t *testing.T, opts []Option, guard ...HandlerOption) (*fakehub.Stream, <-chan *Caller) {
	t.Helper()
	callers := make(chan *Caller, 16)
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
			if c, ok := CallerFrom(ctx); ok {
				callers <- &c
			} else {
				callers <- nil
			}
			return Empty{}, nil
		}, guard...), nil)
	}, opts...)
	return s, callers
}

// placeOrder forwards orders.PlaceOrder with authorization (none when "") and payload, and
// returns the error code of its Reply (0 when it was answered) and the error's message.
func placeOrder(t *testing.T, s *fakehub.Stream, authorization, payload string) (hubpb.ErrorCode, string) {
	t.Helper()
	var headers map[string]string
	if authorization != "" {
		headers = map[string]string{"authorization": authorization}
	}
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
		Payload: []byte(payload), Headers: headers}))
	r := nextReply(t, s)
	return r.GetError().GetCode(), r.GetError().GetMessage()
}

func TestTheCallerIsCheckedBeforeDecoding(t *testing.T) {
	s, _ := guarded(t, nil, RequireAuthenticated())
	code, msg := placeOrder(t, s, "", "not json")
	is(t, code, hubpb.ErrorCode_UNAUTHENTICATED) // not INVALID_REQUEST: an anonymous caller learns nothing of the payload
	is(t, msg, "handler requires authentication")
	code, _ = placeOrder(t, s, "Bearer "+readableToken(t, map[string]any{"sub": "u-1"}), "not json")
	is(t, code, hubpb.ErrorCode_INVALID_REQUEST) // an authenticated caller gets the decode error
}

func TestCallerValidation(t *testing.T) {
	key, foreign := testKeys(t)
	srv, _ := testJWKS(t)
	opts := []Option{JWKS(srv.URL, testIssuer)}
	s, callers := guarded(t, opts, RequireCaller(func(c Caller) bool { return c.Subject != "conformance-refused" }))
	bearer := func(tok string) string { return "Bearer " + tok }
	roles := map[string]any{"roles": []any{"orders-user"}}
	for _, c := range []struct {
		what, auth string
		want       hubpb.ErrorCode
		msg        string
	}{
		{"no token", "", hubpb.ErrorCode_UNAUTHENTICATED, "handler requires authentication"},
		{"not a JWT", "Bearer abc", hubpb.ErrorCode_UNAUTHENTICATED, "unreadable token"},
		{"not a bearer token", "Basic dXNlcjpwYXNz", hubpb.ErrorCode_UNAUTHENTICATED, "malformed authorization header"},
		{"expired", bearer(mint(t, key, "k1", userClaims("u-1", -10*time.Minute, nil))), hubpb.ErrorCode_UNAUTHENTICATED, "invalid token"},
		{"unknown kid", bearer(mint(t, foreign, "k9", userClaims("u-1", time.Minute, nil))), hubpb.ErrorCode_UNAUTHENTICATED, "unknown signing key"},
		{"foreign key under the published kid", bearer(mint(t, foreign, "k1", userClaims("u-1", time.Minute, nil))), hubpb.ErrorCode_UNAUTHENTICATED, "invalid token"},
		{"another issuer", bearer(mint(t, key, "k1", userClaims("u-1", time.Minute, map[string]any{"iss": "https://other.test"}))), hubpb.ErrorCode_UNAUTHENTICATED, "invalid token"},
		{"a refused caller", bearer(mint(t, key, "k1", userClaims("conformance-refused", time.Minute, nil))), hubpb.ErrorCode_PERMISSION_DENIED, "access denied"},
	} {
		code, msg := placeOrder(t, s, c.auth, `{}`)
		if code != c.want || msg != c.msg {
			t.Fatalf("%s: got %v %q, want %v %q", c.what, code, msg, c.want, c.msg)
		}
	}
	code, _ := placeOrder(t, s, bearer(mint(t, key, "k1", userClaims("u-1", time.Minute, roles))), `{}`)
	is(t, code, hubpb.ErrorCode(0))
	got := receive(t, callers)
	is(t, got != nil, true)
	is(t, got.Subject, "u-1")
	is(t, slices.Equal(got.Roles, []string{"orders-user"}), true)
	is(t, got.Verified, true)
	is(t, got.HasRole("orders-user"), true)
	is(t, got.HasRole("admin"), false)

	// RequireRole, and every guard of a handler must pass.
	s2, _ := guarded(t, opts, RequireRole("orders-user"), RequireCaller(func(c Caller) bool { return c.Subject == "u-1" }))
	code, _ = placeOrder(t, s2, bearer(mint(t, key, "k1", userClaims("u-1", time.Minute, roles))), `{}`)
	is(t, code, hubpb.ErrorCode(0))
	code, _ = placeOrder(t, s2, bearer(mint(t, key, "k1", userClaims("u-1", time.Minute, nil))), `{}`)
	is(t, code, hubpb.ErrorCode_PERMISSION_DENIED)
	code, _ = placeOrder(t, s2, bearer(mint(t, key, "k1", userClaims("u-2", time.Minute, roles))), `{}`)
	is(t, code, hubpb.ErrorCode_PERMISSION_DENIED)
}

func TestRolesClaim(t *testing.T) {
	claims := map[string]any{
		"roles":        []any{"a", 7, "b"},
		"realm_access": map[string]any{"roles": []any{"orders-user"}},
		"scope":        "read  write",
		"groups":       "not a list",
	}
	for _, c := range []struct {
		claim string
		want  []string
	}{
		{"", []string{"a", "b"}}, // the default, roles; what is not a string is skipped
		{"roles", []string{"a", "b"}},
		{"realm_access.roles", []string{"orders-user"}},
		{"scope", []string{"read", "write"}}, // space separated
		{"SCOPE", []string{"read", "write"}},
		{"groups", nil},
		{"missing.path", nil},
	} {
		if got := parseRolesClaim(c.claim).of(claims); !slices.Equal(got, c.want) {
			t.Fatalf("%q: got %v, want %v", c.claim, got, c.want)
		}
	}
	// Through an app: RolesClaim names where the roles are.
	key, _ := testKeys(t)
	srv, _ := testJWKS(t)
	s, callers := guarded(t, []Option{JWKS(srv.URL, testIssuer), RolesClaim("realm_access.roles")}, RequireRole("orders-user"))
	tok := mint(t, key, "k1", userClaims("u-1", time.Minute, map[string]any{"realm_access": map[string]any{"roles": []any{"orders-user"}}}))
	code, _ := placeOrder(t, s, "Bearer "+tok, `{}`)
	is(t, code, hubpb.ErrorCode(0))
	is(t, receive(t, callers).Roles[0], "orders-user")
	s2, callers2 := guarded(t, []Option{JWKS(srv.URL, testIssuer), RolesClaim("scope")}, RequireRole("write"))
	tok = mint(t, key, "k1", userClaims("u-1", time.Minute, map[string]any{"scope": "read write"}))
	code, _ = placeOrder(t, s2, "Bearer "+tok, `{}`)
	is(t, code, hubpb.ErrorCode(0))
	is(t, slices.Equal(receive(t, callers2).Roles, []string{"read", "write"}), true)
}

func TestAnUnreachableJWKSIsUnauthenticated(t *testing.T) {
	key, _ := testKeys(t)
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	s, _ := guarded(t, []Option{JWKS(gone.URL, testIssuer)}, RequireAuthenticated())
	code, msg := placeOrder(t, s, "Bearer "+mint(t, key, "k1", userClaims("u-1", time.Minute, nil)), `{}`)
	is(t, code, hubpb.ErrorCode_UNAUTHENTICATED) // never an internal error
	is(t, msg, "signing keys unavailable")
	code, _ = placeOrder(t, s, "", `{}`) // and the stream is still served
	is(t, code, hubpb.ErrorCode_UNAUTHENTICATED)
}

func TestAnUnknownKidRefreshesTheJWKSAtMostOncePerMinute(t *testing.T) {
	key, foreign := testKeys(t)
	srv, fetches := testJWKS(t)
	s, _ := guarded(t, []Option{JWKS(srv.URL, testIssuer)}, RequireAuthenticated())
	for range 3 {
		code, msg := placeOrder(t, s, "Bearer "+mint(t, foreign, "k-rotated", userClaims("u-1", time.Minute, nil)), `{}`)
		is(t, code, hubpb.ErrorCode_UNAUTHENTICATED)
		is(t, msg, "unknown signing key")
	}
	is(t, fetches.Load(), int32(2)) // the first load, and one refresh for the unknown kid within the minute
	code, _ := placeOrder(t, s, "Bearer "+mint(t, key, "k1", userClaims("u-1", time.Minute, nil)), `{}`)
	is(t, code, hubpb.ErrorCode(0))
	is(t, fetches.Load(), int32(2)) // a known kid is served from the cache
}

func TestWithoutJWKSSignedTokensAreReadUnverified(t *testing.T) { // SG-6
	rec := &logRecorder{}
	s, callers := guarded(t, []Option{Logger(slog.New(rec))}, RequireAuthenticated())
	for range 2 {
		code, _ := placeOrder(t, s, "Bearer "+readableToken(t, map[string]any{"sub": "u-1", "roles": []any{"orders-user"}}), `{}`)
		is(t, code, hubpb.ErrorCode(0)) // the guard passes
		got := receive(t, callers)
		is(t, got.Subject, "u-1")
		is(t, got.Verified, false)
		is(t, got.Roles[0], "orders-user")
	}
	is(t, rec.count(slog.LevelWarn, warnUnverified), 1) // once
	unsigned := jsonSegment(t, map[string]any{"alg": "none"}) + "." + jsonSegment(t, map[string]any{"sub": "u-1"}) + "."
	code, msg := placeOrder(t, s, "Bearer "+unsigned, `{}`)
	is(t, code, hubpb.ErrorCode_UNAUTHENTICATED)
	is(t, msg, "unsigned token")
}

func TestABadTokenIsRefusedEvenWithoutAGuard(t *testing.T) { // SG-18
	s, callers := guarded(t, nil)
	code, msg := placeOrder(t, s, "Bearer abc", `{}`)
	is(t, code, hubpb.ErrorCode_UNAUTHENTICATED)
	is(t, msg, "unreadable token")
	code, _ = placeOrder(t, s, "", `{}`) // no token: an anonymous caller, served
	is(t, code, hubpb.ErrorCode(0))
	is(t, receive(t, callers) == nil, true)
	_, ok := CallerFrom(context.Background())
	is(t, ok, false)
	// Before the lookup too: a bad token for a type nobody serves.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-2", Kind: hubpb.RequestKind_COMMAND, Type: "orders.Unknown",
		Payload: []byte(`{}`), Headers: map[string]string{"authorization": "Bearer abc"}}))
	is(t, nextReply(t, s).GetError().GetCode(), hubpb.ErrorCode_UNAUTHENTICATED)
}

func TestIdentitySettings(t *testing.T) {
	_, err := New(Service("orders-service"), Hubs("127.0.0.1:7000"), JWKS("", testIssuer))
	is(t, err != nil, true)
	_, err = New(Service("orders-service"), Hubs("127.0.0.1:7000"), JWKS("https://issuer.test/jwks", ""))
	is(t, err != nil, true)
	app := newApp(t, Hubs("127.0.0.1:7000"))
	is(t, app.cfg.forwardJWT, true) // the default
	is(t, app.identity.verifier == nil, true)
	app = newApp(t, Hubs("127.0.0.1:7000"), JWKS("https://issuer.test/jwks", testIssuer), RolesClaim("scope"), ForwardJWT(false))
	is(t, app.cfg.forwardJWT, false)
	is(t, app.identity.verifier != nil, true)
	is(t, app.identity.roles.scope, true)
	// A guard needs a function.
	app = newApp(t, Hubs("127.0.0.1:7000"))
	is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil }, RequireCaller(nil)) != nil, true)
}
