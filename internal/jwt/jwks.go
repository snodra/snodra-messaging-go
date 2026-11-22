package jwt

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"
)

// maxJWKS bounds the JWKS document read.
const maxJWKS = 1 << 20

// key is the public key with id kid: from the cache, loaded at the first token, fetched again
// (throttled) when the cache does not know kid.
func (v *Verifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	if err := v.load(ctx, false); err != nil {
		return nil, err
	}
	k, known := v.lookup(kid)
	if !known {
		if err := v.load(ctx, true); err != nil {
			return nil, err
		}
		if k, known = v.lookup(kid); !known {
			return nil, refused("unknown signing key", nil)
		}
	}
	if k == nil {
		return nil, refused("unusable signing key", nil)
	}
	return k, nil
}

func (v *Verifier) lookup(kid string) (*rsa.PublicKey, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	k, ok := v.keys[kid]
	return k, ok
}

// load fetches the keys: the first load (forced false) until one succeeds, a forced one (an
// unknown key id) at most once per minRefresh, whether it succeeds or not.
func (v *Verifier) load(ctx context.Context, forced bool) error {
	v.fetchMu.Lock()
	defer v.fetchMu.Unlock()
	v.mu.Lock()
	switch {
	case forced && !v.lastForced.IsZero() && time.Since(v.lastForced) < v.minRefresh:
		v.mu.Unlock()
		return nil
	case forced:
		v.lastForced = time.Now()
	case v.fetched:
		v.mu.Unlock()
		return nil
	}
	v.mu.Unlock()
	keys, err := v.fetch(ctx)
	if err != nil {
		return refused("signing keys unavailable", err)
	}
	v.mu.Lock()
	v.keys, v.fetched = keys, true
	v.mu.Unlock()
	return nil
}

// jwk is one key of a JWKS document.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// fetch reads the JWKS document: its keys by id, the first of a repeated id winning, nil for a
// key that is not a usable RSA key.
func (v *Verifier) fetch(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("the JWKS endpoint answered %s", resp.Status)
	}
	var doc struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxJWKS)).Decode(&doc); err != nil {
		return nil, fmt.Errorf("the JWKS does not parse: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, k := range doc.Keys {
		if _, seen := keys[k.Kid]; seen {
			continue
		}
		keys[k.Kid], _ = rsaKey(k)
	}
	return keys, nil
}

// rsaKey is the RSA public key k describes.
func rsaKey(k jwk) (*rsa.PublicKey, error) {
	if k.Kty != "RSA" {
		return nil, fmt.Errorf("key type %q", k.Kty)
	}
	n, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(k.N, "="))
	if err != nil || len(n) == 0 {
		return nil, errors.New("bad modulus")
	}
	e, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(k.E, "="))
	if err != nil || len(e) == 0 || len(e) > 4 {
		return nil, errors.New("bad exponent")
	}
	exp := new(big.Int).SetBytes(e).Int64()
	if exp < 3 {
		return nil, errors.New("bad exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(exp)}, nil
}
