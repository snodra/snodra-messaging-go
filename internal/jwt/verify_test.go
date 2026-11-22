package jwt_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/jwt"
)

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

const issuer = "https://issuer.test"

var (
	keysOnce        sync.Once
	signing, other  *rsa.PrivateKey
	errNoKeysForRun error
)

// keys are two RSA keys made once per run: the published one and one the JWKS never publishes.
func keys(t *testing.T) (*rsa.PrivateKey, *rsa.PrivateKey) {
	t.Helper()
	keysOnce.Do(func() {
		if signing, errNoKeysForRun = rsa.GenerateKey(rand.Reader, 2048); errNoKeysForRun == nil {
			other, errNoKeysForRun = rsa.GenerateKey(rand.Reader, 2048)
		}
	})
	if errNoKeysForRun != nil {
		t.Fatal(errNoKeysForRun)
	}
	return signing, other
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func segment(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b64(b)
}

// sign makes an RS256 token of claims under header, signed with key.
func sign(t *testing.T, key *rsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	input := segment(t, header) + "." + segment(t, claims)
	sum := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64(sig)
}

func claims(sub string, exp time.Duration) map[string]any {
	now := time.Now().Unix()
	return map[string]any{"iss": issuer, "sub": sub, "iat": now, "exp": now + int64(exp/time.Second)}
}

func header(kid string) map[string]any {
	return map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}
}

// jwk is key's public half as a JWKS entry.
func jwk(kid string, key *rsa.PrivateKey) map[string]any {
	return map[string]any{"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
		"n": b64(key.N.Bytes()), "e": b64(big.NewInt(int64(key.E)).Bytes())}
}

// jwksServer serves a JWKS whose keys can change, counting the fetches.
type jwksServer struct {
	*httptest.Server
	fetches atomic.Int32
	mu      sync.Mutex
	keys    []map[string]any
	status  int
}

func newJWKS(t *testing.T, keys ...map[string]any) *jwksServer {
	t.Helper()
	j := &jwksServer{keys: keys, status: http.StatusOK}
	j.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		j.fetches.Add(1)
		j.mu.Lock()
		defer j.mu.Unlock()
		w.WriteHeader(j.status)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": j.keys})
	}))
	t.Cleanup(j.Close)
	return j
}

func (j *jwksServer) set(status int, keys ...map[string]any) {
	j.mu.Lock()
	j.status, j.keys = status, keys
	j.mu.Unlock()
}

func reason(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func TestAValidTokenGivesItsClaims(t *testing.T) {
	key, _ := keys(t)
	j := newJWKS(t, jwk("k1", key))
	v := jwt.NewVerifier(j.URL, issuer, nil, time.Minute)
	got, err := v.Verify(context.Background(), sign(t, key, header("k1"), claims("u-1", 5*time.Minute)))
	is(t, err, nil)
	is(t, got["sub"], any("u-1"))
	is(t, got["iss"], any(issuer))
	// The keys are cached: a second token fetches nothing.
	_, err = v.Verify(context.Background(), sign(t, key, header("k1"), claims("u-2", 5*time.Minute)))
	is(t, err, nil)
	is(t, j.fetches.Load(), int32(1))
}

func TestInvalidTokensAreRefusedWithAShortReason(t *testing.T) {
	key, foreign := keys(t)
	j := newJWKS(t, jwk("k1", key))
	v := jwt.NewVerifier(j.URL, issuer, nil, time.Minute)
	now := time.Now().Unix()
	noExp := claims("u-1", time.Minute)
	delete(noExp, "exp")
	noIss := claims("u-1", time.Minute)
	delete(noIss, "iss")
	otherIss := claims("u-1", time.Minute)
	otherIss["iss"] = "https://other.test"
	notYet := claims("u-1", time.Hour)
	notYet["nbf"] = now + 600
	hs := header("k1")
	hs["alg"] = "HS256"
	noKid := header("")
	delete(noKid, "kid")
	valid := sign(t, key, header("k1"), claims("u-1", time.Minute))
	parts := strings.Split(valid, ".")
	for _, c := range []struct {
		what, token, want string
	}{
		{"not a JWT", "abc", "unreadable token"},
		{"two segments", parts[0] + "." + parts[1], "unreadable token"},
		{"a header that is not JSON", b64([]byte("{")) + "." + parts[1] + "." + parts[2], "unreadable token"},
		{"another algorithm", sign(t, key, hs, claims("u-1", time.Minute)), "unsupported token algorithm"},
		{"no kid", sign(t, key, noKid, claims("u-1", time.Minute)), "token has no kid"},
		{"a key the JWKS does not publish, under its kid", sign(t, foreign, header("k1"), claims("u-1", time.Minute)), "invalid token"},
		{"a changed payload", parts[0] + "." + segment(t, claims("admin", time.Minute)) + "." + parts[2], "invalid token"},
		{"expired", sign(t, key, header("k1"), claims("u-1", -10*time.Minute)), "invalid token"},
		{"not yet valid", sign(t, key, header("k1"), notYet), "invalid token"},
		{"another issuer", sign(t, key, header("k1"), otherIss), "invalid token"},
		{"no exp", sign(t, key, header("k1"), noExp), "invalid token"},
		{"no iss", sign(t, key, header("k1"), noIss), "invalid token"},
	} {
		_, err := v.Verify(context.Background(), c.token)
		if reason(err) != c.want {
			t.Fatalf("%s: got %q, want %q", c.what, reason(err), c.want)
		}
		var je *jwt.Error
		is(t, errors.As(err, &je), true)
	}
}

func TestExpiryAllowsAMinuteOfClockSkew(t *testing.T) { // as the reference libraries' decoders
	key, _ := keys(t)
	j := newJWKS(t, jwk("k1", key))
	v := jwt.NewVerifier(j.URL, issuer, nil, time.Minute)
	_, err := v.Verify(context.Background(), sign(t, key, header("k1"), claims("u-1", -30*time.Second)))
	is(t, err, nil)
	_, err = v.Verify(context.Background(), sign(t, key, header("k1"), claims("u-1", -90*time.Second)))
	is(t, reason(err), "invalid token")
}

func TestAnUnreachableJWKSIsSigningKeysUnavailable(t *testing.T) {
	key, _ := keys(t)
	token := sign(t, key, header("k1"), claims("u-1", time.Minute))
	j := newJWKS(t, jwk("k1", key))
	j.set(http.StatusInternalServerError)
	v := jwt.NewVerifier(j.URL, issuer, nil, time.Minute)
	_, err := v.Verify(context.Background(), token)
	is(t, reason(err), "signing keys unavailable")
	// The first load is not throttled: once the endpoint answers, the keys load.
	j.set(http.StatusOK, jwk("k1", key))
	_, err = v.Verify(context.Background(), token)
	is(t, err, nil)

	gone := newJWKS(t)
	gone.Close()
	_, err = jwt.NewVerifier(gone.URL, issuer, nil, time.Minute).Verify(context.Background(), token)
	is(t, reason(err), "signing keys unavailable")
}

func TestAnUnknownKidRefreshesAtMostOncePerInterval(t *testing.T) {
	key, foreign := keys(t)
	j := newJWKS(t, jwk("k1", key))
	v := jwt.NewVerifier(j.URL, issuer, nil, 300*time.Millisecond)
	unknown := sign(t, foreign, header("k2"), claims("u-1", time.Minute))
	_, err := v.Verify(context.Background(), unknown)
	is(t, reason(err), "unknown signing key")
	is(t, j.fetches.Load(), int32(2)) // the first load, then one refresh for the unknown kid
	for range 3 {
		_, err = v.Verify(context.Background(), unknown)
		is(t, reason(err), "unknown signing key")
	}
	is(t, j.fetches.Load(), int32(2)) // throttled
	// The provider rotates its keys: after the interval an unknown kid refreshes again.
	j.set(http.StatusOK, jwk("k1", key), jwk("k2", foreign))
	time.Sleep(350 * time.Millisecond) // the interval is the behaviour under test
	_, err = v.Verify(context.Background(), unknown)
	is(t, err, nil)
	is(t, j.fetches.Load(), int32(3))
}

func TestAKeyThatIsNotRSAIsUnusable(t *testing.T) {
	key, _ := keys(t)
	j := newJWKS(t, map[string]any{"kty": "EC", "kid": "k1", "crv": "P-256", "x": "AA", "y": "AA"})
	v := jwt.NewVerifier(j.URL, issuer, nil, time.Minute)
	_, err := v.Verify(context.Background(), sign(t, key, header("k1"), claims("u-1", time.Minute)))
	is(t, reason(err), "unusable signing key")
}

func TestUnverifiedReadsOnlySignedTokens(t *testing.T) {
	key, _ := keys(t)
	token := sign(t, key, header("k1"), claims("u-1", time.Minute))
	got, err := jwt.Unverified(token)
	is(t, err, nil)
	is(t, got["sub"], any("u-1"))
	parts := strings.Split(token, ".")
	none := segment(t, map[string]any{"alg": "none"})
	noneUpper := segment(t, map[string]any{"alg": "NONE"})
	for _, c := range []struct {
		what, token, want string
	}{
		{"alg none", none + "." + parts[1] + ".", "unsigned token"},
		{"alg NONE with a signature", noneUpper + "." + parts[1] + "." + parts[2], "unsigned token"},
		{"no signature", parts[0] + "." + parts[1] + ".", "unsigned token"},
		{"an encrypted token (five segments)", "a.b.c.d.e", "unsupported token"},
		{"not a JWT", "abc", "unreadable token"},
		{"a payload that is not base64", parts[0] + ".!!!." + parts[2], "unreadable token"},
		{"a payload that is not an object", parts[0] + "." + b64([]byte("[1]")) + "." + parts[2], "unreadable token"},
	} {
		_, err := jwt.Unverified(c.token)
		if reason(err) != c.want {
			t.Fatalf("%s: got %q, want %q", c.what, reason(err), c.want)
		}
	}
}
