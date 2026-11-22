package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/snodra/snodra-messaging-go/internal/jwt"
)

// Caller is who sent a request: the acting user of its forwarded token (the authorization
// header, "Bearer <JWT>"). See https://snodra.damoreira.com/library-authors/messages/
// (Forwarded identity).
type Caller struct {
	// Subject is the token's sub claim.
	Subject string
	// Roles are read from the token's roles claim (see [RolesClaim]).
	Roles []string
	// Verified is true when this process checked the token against the issuer's JWKS (see
	// [JWKS]); false when the token was read without verification.
	Verified bool
}

// HasRole reports whether the caller has role.
func (c Caller) HasRole(role string) bool { return slices.Contains(c.Roles, role) }

// RequireAuthenticated lets only a caller with a valid token run the handler: a request without
// one is answered UNAUTHENTICATED, decided before the payload is decoded.
func RequireAuthenticated() HandlerOption {
	return RequireCaller(func(Caller) bool { return true })
}

// RequireRole lets only a caller with role run the handler: an anonymous caller is answered
// UNAUTHENTICATED, a caller without the role PERMISSION_DENIED, decided before the payload is
// decoded.
func RequireRole(role string) HandlerOption {
	return RequireCaller(func(c Caller) bool { return c.HasRole(role) })
}

// RequireCaller lets only a caller allow accepts run the handler: an anonymous caller is
// answered UNAUTHENTICATED, a caller allow refuses PERMISSION_DENIED, decided before the
// payload is decoded. When a handler has several of these options, every one must accept.
func RequireCaller(allow func(Caller) bool) HandlerOption {
	return func(c *handlerConfig) {
		if allow == nil {
			c.err = errors.Join(c.err, errors.New("snodra: RequireCaller needs a function"))
			return
		}
		c.guards = append(c.guards, allow)
	}
}

// JWKS checks every forwarded token against the issuer's signing keys at url (RS256), its
// issuer (the iss claim must be issuer) and its expiry, before a handler runs; a token that
// fails is answered UNAUTHENTICATED, also when the keys cannot be fetched. Without it, the claims
// of a signed token are read without verification and a warning is logged once (see
// SPEC-GAPS.md, SG-6): set it wherever the hub is reachable by callers you do not trust.
func JWKS(url, issuer string) Option {
	return func(c *config) error {
		url, issuer = strings.TrimSpace(url), strings.TrimSpace(issuer)
		if url == "" {
			return errors.New("snodra: JWKS needs the URL of the issuer's signing keys")
		}
		if issuer == "" {
			return errors.New("snodra: JWKS needs the issuer")
		}
		c.jwksURL, c.issuer = url, issuer
		return nil
	}
}

// RolesClaim is where a token's roles are read: a dotted path to a list of strings
// ("realm_access.roles", "groups"), or "scope" for the space-separated scopes. The default is
// "roles".
func RolesClaim(path string) Option {
	return func(c *config) error {
		c.rolesClaim = path
		return nil
	}
}

// warnUnverified is logged once when a token is read without a JWKS.
const warnUnverified = "forwarded identities are trusted without verification: no JWKS configured (JWKS, SNODRA_JWKS_URL)"

// identityResolver turns a request's authorization header into its caller.
type identityResolver struct {
	verifier *jwt.Verifier // nil: signed tokens are read unverified
	roles    rolesClaim
	warned   atomic.Bool
	log      *slog.Logger
}

func newIdentityResolver(cfg config, log *slog.Logger) *identityResolver {
	r := &identityResolver{roles: parseRolesClaim(cfg.rolesClaim), log: log}
	if cfg.jwksURL != "" {
		r.verifier = jwt.NewVerifier(cfg.jwksURL, cfg.issuer, &http.Client{Timeout: jwksFetchTimeout}, jwksMinRefresh)
	}
	return r
}

// resolve is the caller of a request whose authorization header is authorization: nil (an
// anonymous caller) when it is absent; an error, whose text is the short reason the request is
// answered UNAUTHENTICATED with, for a header that is not a bearer token or a token that fails.
// TODO(spec-gap): SG-6 (without a JWKS, signed tokens are read unverified).
func (r *identityResolver) resolve(ctx context.Context, authorization string) (*Caller, error) {
	raw := strings.TrimSpace(authorization)
	if raw == "" {
		return nil, nil
	}
	token, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok {
		token, ok = strings.CutPrefix(raw, "bearer ")
	}
	if token = strings.TrimSpace(token); !ok || token == "" {
		return nil, errors.New("malformed authorization header")
	}
	var (
		claims map[string]any
		err    error
	)
	if r.verifier != nil {
		claims, err = r.verifier.Verify(ctx, token)
	} else {
		if !r.warned.Swap(true) {
			r.log.Warn(warnUnverified)
		}
		claims, err = jwt.Unverified(token)
	}
	if err != nil {
		var je *jwt.Error
		if errors.As(err, &je) && je.Err != nil {
			level := slog.LevelDebug
			if je.Reason == "signing keys unavailable" {
				level = slog.LevelWarn
			}
			r.log.Log(ctx, level, "forwarded token refused", "reason", je.Reason, "error", je.Err.Error())
		}
		return nil, err // its text is the short reason
	}
	sub, _ := claims["sub"].(string)
	return &Caller{Subject: sub, Roles: r.roles.of(claims), Verified: r.verifier != nil}, nil
}

// rolesClaim is where a token's roles are: the space-separated scope claim, or a path of claim
// names to a list of strings.
type rolesClaim struct {
	scope bool
	path  []string
}

// parseRolesClaim reads "scope" (any letter case) or a dotted path; blank is "roles".
func parseRolesClaim(v string) rolesClaim {
	v = strings.TrimSpace(v)
	switch {
	case v == "":
		return rolesClaim{path: []string{"roles"}}
	case strings.EqualFold(v, "scope"):
		return rolesClaim{scope: true}
	}
	var path []string
	for _, seg := range strings.Split(v, ".") {
		if seg != "" {
			path = append(path, seg)
		}
	}
	return rolesClaim{path: path}
}

// of is the roles the claim holds in claims; nil when it is absent or not of its shape (the
// entries of a list that are not strings are skipped).
func (rc rolesClaim) of(claims map[string]any) []string {
	if rc.scope {
		s, _ := claims["scope"].(string)
		return strings.Fields(s)
	}
	if len(rc.path) == 0 {
		return nil
	}
	node, ok := claims[rc.path[0]]
	for _, seg := range rc.path[1:] {
		obj, isObj := node.(map[string]any)
		if !ok || !isObj {
			return nil
		}
		node, ok = obj[seg]
	}
	list, _ := node.([]any)
	var roles []string
	for _, v := range list {
		if s, isStr := v.(string); isStr {
			roles = append(roles, s)
		}
	}
	return roles
}

// guardRefusal is the code and message a request of type typ from caller is refused with by the
// guards of cfg; refused is false when every guard accepts (or there is none). A guard that panics
// refuses the caller, and the panic is logged.
func (a *App) guardRefusal(cfg handlerConfig, typ string, caller *Caller) (code Code, msg string, refused bool) {
	if len(cfg.guards) == 0 {
		return 0, "", false
	}
	if caller == nil {
		return CodeUnauthenticated, "handler requires authentication", true
	}
	for _, allow := range cfg.guards {
		if !a.accepts(allow, typ, *caller) {
			return CodePermissionDenied, "access denied", true
		}
	}
	return 0, "", false
}

func (a *App) accepts(allow func(Caller) bool, typ string, c Caller) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("a guard panicked; the caller is refused", "type", typ, "panic", fmt.Sprint(r))
			ok = false
		}
	}()
	return allow(c)
}
