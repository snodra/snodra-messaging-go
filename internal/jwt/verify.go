// Package jwt checks the callers' forwarded tokens: RS256 signatures against a JWKS, the issuer
// and the expiry. See https://snodra.damoreira.com/library-authors/messages/ (Forwarded identity).
package jwt

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// leeway is the clock skew allowed on exp and nbf.
const leeway = 60 * time.Second

// fetchTimeout bounds a JWKS fetch when NewVerifier is given no HTTP client.
const fetchTimeout = 5 * time.Second

// Error is a refused token. Its text is a short reason, safe to send back to the caller (never
// the token); Err, when set, is the detail for the logs.
type Error struct {
	Reason string
	Err    error
}

func (e *Error) Error() string { return e.Reason }
func (e *Error) Unwrap() error { return e.Err }

func refused(reason string, err error) *Error { return &Error{Reason: reason, Err: err} }

// Verifier checks RS256 tokens against the keys of a JWKS endpoint, and their issuer, expiry
// and not-before. The keys are fetched at the first token and cached; a token whose key id the
// cache does not know fetches them again, at most once per minRefresh.
type Verifier struct {
	url, issuer string
	hc          *http.Client
	minRefresh  time.Duration

	mu         sync.Mutex
	fetched    bool
	keys       map[string]*rsa.PublicKey // by key id; nil: published but unusable
	lastForced time.Time                 // the last fetch an unknown key id caused
	inflight   *flight                   // the fetch in progress, shared; nil when none
}

// NewVerifier checks tokens of issuer against the JWKS at jwksURL, fetched with hc (nil: a
// client with a 5 s timeout).
func NewVerifier(jwksURL, issuer string, hc *http.Client, minRefresh time.Duration) *Verifier {
	if hc == nil {
		hc = &http.Client{Timeout: fetchTimeout}
	}
	return &Verifier{url: jwksURL, issuer: issuer, hc: hc, minRefresh: minRefresh}
}

// header is what a token's header says.
type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

// Verify returns the claims of token once its signature, issuer, expiry and not-before hold; an
// *Error otherwise ("signing keys unavailable" when the JWKS cannot be fetched).
func (v *Verifier) Verify(ctx context.Context, token string) (map[string]any, error) {
	parts := strings.SplitN(token, ".", 6) // at most 6: a token of many dots costs nothing
	if len(parts) != 3 {
		return nil, refused("unreadable token", nil)
	}
	var h header
	if err := decodeSegment(parts[0], &h); err != nil {
		return nil, refused("unreadable token", err)
	}
	if h.Alg != "RS256" {
		return nil, refused("unsupported token algorithm", nil)
	}
	if h.Kid == "" {
		return nil, refused("token has no kid", nil)
	}
	key, err := v.key(ctx, h.Kid)
	if err != nil {
		return nil, err
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, refused("invalid token", err)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return nil, refused("invalid token", err)
	}
	var claims map[string]any
	if err := decodeSegment(parts[1], &claims); err != nil || claims == nil {
		return nil, refused("invalid token", errors.New("the claims are not a JSON object"))
	}
	if err := v.check(claims, time.Now()); err != nil {
		return nil, refused("invalid token", err)
	}
	return claims, nil
}

// check holds claims against the issuer and the clock: exp and iss are required.
func (v *Verifier) check(claims map[string]any, now time.Time) error {
	exp, ok := claims["exp"].(float64)
	if !ok {
		return errors.New("no numeric exp claim")
	}
	if time.Unix(int64(exp), 0).Add(leeway).Before(now) {
		return errors.New("expired")
	}
	if nbf, present := claims["nbf"]; present {
		n, ok := nbf.(float64)
		if !ok {
			return errors.New("the nbf claim is not a number")
		}
		if time.Unix(int64(n), 0).After(now.Add(leeway)) {
			return errors.New("not valid yet")
		}
	}
	if iss, _ := claims["iss"].(string); iss != v.issuer {
		return fmt.Errorf("issuer %q, want %q", iss, v.issuer)
	}
	return nil
}

// Unverified returns the claims of a signed token without checking its signature: what a
// service without a JWKS can judge of a token (its claims are never an identity). It refuses an unsigned token (alg "none", or no signature), an encrypted one
// (five segments) and anything that is not a JWS.
func Unverified(token string) (map[string]any, error) {
	parts := strings.SplitN(token, ".", 6) // at most 6: a token of many dots costs nothing
	if len(parts) == 5 {
		return nil, refused("unsupported token", nil)
	}
	if len(parts) != 3 {
		return nil, refused("unreadable token", nil)
	}
	var h header
	if err := decodeSegment(parts[0], &h); err != nil {
		return nil, refused("unreadable token", err)
	}
	if strings.EqualFold(h.Alg, "none") || parts[2] == "" {
		return nil, refused("unsigned token", nil)
	}
	var claims map[string]any
	if err := decodeSegment(parts[1], &claims); err != nil || claims == nil {
		return nil, refused("unreadable token", err)
	}
	return claims, nil
}

// decodeSegment decodes a base64url segment (padding tolerated) holding a JSON object into v.
func decodeSegment(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	if err != nil {
		return err
	}
	b = bytes.TrimSpace(b)
	if len(b) == 0 || b[0] != '{' {
		return errors.New("not a JSON object")
	}
	return json.Unmarshal(b, v)
}
