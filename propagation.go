package snodramessaging

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// The request headers the library sets itself
// (https://snodra.damoreira.com/library-authors/messages/, Request headers).
const (
	authorizationHeader = "authorization"
	traceparentHeader   = "traceparent"
)

// reservedHeaders are the names, in lower case, that [Header] refuses.
var reservedHeaders = map[string]bool{
	"authorization":  true,
	"traceparent":    true,
	"tracestate":     true,
	"correlationid":  true,
	"correlation-id": true,
}

// Header adds the header name with value to this call's request; the handler sees it in
// [MessageInfo].Headers. The names the library sets itself (authorization, traceparent,
// tracestate, correlationId, correlation-id, in any letter case) are refused: the call fails
// with [ErrReservedHeader] before anything is sent. A later Header of the same name wins.
func Header(name, value string) SendOption {
	return func(c *sendConfig) {
		if c.headers == nil {
			c.headers = map[string]string{}
		}
		c.headers[name] = value
	}
}

// ForwardJWT says whether a request sent while handling one carries the handled request's
// authorization header on, so that the downstream handler acts for the same user. The default
// is true.
func ForwardJWT(on bool) Option {
	return func(c *config) error {
		c.forwardJWT = on
		return nil
	}
}

// checkHeaders refuses a header of own the library sets itself, compared ignoring letter case.
func checkHeaders(own map[string]string, typ string) error {
	for _, name := range slices.Sorted(maps.Keys(own)) {
		if reservedHeaders[strings.ToLower(name)] {
			return fmt.Errorf("%w: header %q is set by the library and cannot be given with Header (sending %s)",
				ErrReservedHeader, name, typ)
		}
	}
	return nil
}

// outgoingHeaders are the headers of a request sent with ctx: the caller's own (already
// checked), then the correlation id of the request ctx is handling (its correlationId, else its
// id), a new UUID outside any handling; its authorization header, when forwardJWT; and its
// traceparent, only when valid. See https://snodra.damoreira.com/library-authors/messages/
// (Correlation id, Trace context, Forwarded identity).
func outgoingHeaders(ctx context.Context, own map[string]string, forwardJWT bool) map[string]string {
	out := make(map[string]string, len(own)+3)
	maps.Copy(out, own)
	h := handlingFrom(ctx)
	correlation := ""
	if h != nil {
		correlation = h.info.CorrelationID
	}
	if correlation == "" {
		correlation = uuid.New()
	}
	out[correlationIDHeader] = correlation
	if h != nil && forwardJWT && h.authorization != "" {
		out[authorizationHeader] = h.authorization
	}
	if h != nil && h.traceparent != "" {
		out[traceparentHeader] = h.traceparent
	}
	return out
}
