package snodra

import "context"

// MessageInfo describes the request a handler is handling.
type MessageInfo struct {
	// ID is the forwarded Request.id: the sender's id for a command or a point query, a
	// derived one ("<id>#<n>") for the multi-part modes. Never use it to detect a retried
	// command: a retry has a fresh id.
	ID string
	// Type is the message type, for example "orders.PlaceOrder".
	Type string
	// CorrelationID is the request's correlationId header, else (no such header) its ID. It is
	// empty when the header is present but empty; the requests sent while handling then carry a
	// new correlation id.
	CorrelationID string
	// Headers are the request's headers, as the hub forwarded them.
	Headers map[string]string
}

// correlationIDHeader is the header that carries the correlation id
// (https://snodra.damoreira.com/docs/library-authors/messages/).
const correlationIDHeader = "correlationId"

// handling is what the dispatcher puts in a handler's context: the request being handled, what
// the requests sent while handling it carry on, and its caller.
type handling struct {
	info MessageInfo
	// authorization is the request's authorization header as it came ("Bearer <token>").
	authorization string
	// traceparent is the request's traceparent header when it is valid, else "".
	traceparent string
	// caller is who sent the request; nil for an anonymous caller.
	caller *Caller
	// event is set when an event handler runs: the handling is the delivered event, whose userId
	// header is the acting user (an event carries no token).
	event bool
	// saga is set inside a saga handler (an event's stage 2, a notice or a deadline): the
	// app-level Send, Query and Publish are refused there; SagaContext records the effects.
	saga bool
}

type handlingKey struct{}

func withHandling(ctx context.Context, h *handling) context.Context {
	return context.WithValue(ctx, handlingKey{}, h)
}

// handlingFrom is the request ctx is handling; nil outside a handler.
func handlingFrom(ctx context.Context) *handling {
	h, _ := ctx.Value(handlingKey{}).(*handling)
	return h
}

// MessageFrom returns what the handler's context says of the request being handled, and false
// outside a handler.
func MessageFrom(ctx context.Context) (MessageInfo, bool) {
	if h := handlingFrom(ctx); h != nil {
		return h.info, true
	}
	return MessageInfo{}, false
}

// CallerFrom returns who sent the request being handled, read from its forwarded token verified
// against the issuer's keys ([JWKS]), and false for an anonymous caller (no token, or no JWKS to
// verify it) and outside a handler. Do not change the returned Roles.
func CallerFrom(ctx context.Context) (Caller, bool) {
	if h := handlingFrom(ctx); h != nil && h.caller != nil {
		return *h.caller, true
	}
	return Caller{}, false
}
