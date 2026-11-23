package snodra

import (
	"context"
	"fmt"
	"maps"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// localHandler is the handler of this instance that answers a call of kind (a [Send] or a
// [Query]) of m in-process, and m's type; nil when the call goes through a hub: with
// [AlwaysRouteThroughHub], for a type this instance does not handle in that kind, and for a [Shared]
// query type (the hub spreads it over every owner, this instance included). The handler of a
// query type may be a streaming or a subscription query's: it answers INVALID_REQUEST, as through
// the hub.
func (a *App) localHandler(kind hubpb.RequestKind, m Message) (*handler, string) {
	if a == nil || a.cfg.alwaysRouteThroughHub || m == nil {
		return nil, ""
	}
	typ, err := messageName(m)
	if err != nil {
		return nil, "" // the call through the hub reports it
	}
	a.mu.Lock() // the handlers change before Start only, under the lock
	h := a.handlers[typ]
	a.mu.Unlock()
	switch {
	case h == nil:
		return nil, ""
	case kind == hubpb.RequestKind_COMMAND:
		if h.kind != kindCommand {
			return nil, ""
		}
	case h.kind == kindCommand || h.cfg.shared:
		return nil, ""
	}
	return h, typ
}

// local is a call of m, of type typ, answered by this instance's handler h on the caller's
// goroutine, as the reference library answers a request for a type its instance handles: no
// hub, no permit, no handler slot, no retry, no timeout of its own (the handler runs under ctx),
// nothing to the telemetry. The handler sees a request with a fresh id, the caller's [Header]s
// and the correlation id; it acts for the caller of the request ctx is handling (anonymous
// outside a handler), whose token and trace go on with the requests it sends. Its guards, its
// errors, a panic and an answer that does not decode fail the call as they would through a hub.
func local[R any](ctx context.Context, app *App, h *handler, kind hubpb.RequestKind, typ string, m Message, opts []SendOption) (R, error) {
	var zero R
	cfg, _, err := sendSettings(app, typ, opts)
	if err != nil {
		return zero, err
	}
	payload, err := app.cfg.codec.Marshal(m)
	if err != nil {
		return zero, fmt.Errorf("snodra: cannot encode %s: %w", typ, err)
	}
	if err := ctx.Err(); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrNeverSent, err)
	}
	answer, werr := app.serveLocally(ctx, h, kind, typ, payload, cfg.headers)
	if werr != nil {
		if err := ctx.Err(); err != nil {
			return zero, err // the caller's context ended the handler
		}
		return zero, errorFromWire(typ, werr)
	}
	r, err := decodeAnswer[R](app.cfg.codec, answer)
	if err != nil {
		return zero, &undecodableReply{typ: typ, err: err}
	}
	return r, nil
}

// serveLocally runs h on payload, a request of kind and typ with the caller's own headers, under
// ctx: its answer, or the error a hub request would have been answered with.
func (a *App) serveLocally(ctx context.Context, h *handler, kind hubpb.RequestKind, typ string, payload []byte, own map[string]string) ([]byte, *hubpb.Error) {
	id := uuid.New()
	fail := func(code hubpb.ErrorCode, msg string) *hubpb.Error {
		return &hubpb.Error{RequestId: id, Code: code, Message: msg}
	}
	switch h.kind {
	case kindStream:
		return nil, fail(hubpb.ErrorCode_INVALID_REQUEST, streamingQueryText(typ))
	case kindSubscription:
		return nil, fail(hubpb.ErrorCode_INVALID_REQUEST, subscriptionQueryText(typ))
	}
	parent := handlingFrom(ctx)
	correlation := ""
	if parent != nil {
		correlation = parent.info.CorrelationID
	}
	if correlation == "" {
		correlation = uuid.New()
	}
	headers := make(map[string]string, len(own)+1)
	maps.Copy(headers, own)
	headers[correlationIDHeader] = correlation
	scope := &handling{info: MessageInfo{ID: id, Type: typ, CorrelationID: correlation, Headers: headers}}
	if parent != nil { // the caller of the request being handled calls on
		scope.authorization, scope.traceparent, scope.caller = parent.authorization, parent.traceparent, parent.caller
	}
	if code, msg, refused := anonymousRefusal(h.cfg, scope.caller); refused {
		return nil, fail(hubpb.ErrorCode(code), msg)
	}
	req := &hubpb.Request{Id: id, Kind: kind, Type: typ, Payload: payload, Headers: maps.Clone(headers)}
	return a.invoke(ctx, h, req, scope)
}
