package snodra

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/traceparent"
)

// dispatch serves a request the hub forwarded on the stream ref, also before its RegisterAck.
// Called from the stream's read loop: the request counts against the stream's permits at once
// and is recorded as running (so a hub Cancel or Flow that follows it finds it), then runs on
// its own goroutine, which writes to ref only (dropped if that stream is gone; nothing at all
// once the hub cancelled the request) exactly one terminal message (a Reply, or for a streaming
// request its parts and one ReplyEnd) and gives the permit back to ref's hub once the request is
// finished: a streaming request holds it until its end. No user code runs on the read loop, and
// a defect that panics there or on the request's goroutine fails that request alone (lastResort).
// See https://snodra.damoreira.com/library-authors/protocol/ (Handling a request).
func (a *App) dispatch(req *hubpb.Request, ref *streamRef, permits *streamPermits) {
	defer a.lastResort(req, func(werr *hubpb.Error) { failRequest(req, ref, werr) })
	if req.GetMode() == hubpb.RequestMode_SUBSCRIPTION {
		// A subscription target is passive: no permit, no handler slot, not in flight.
		a.subscribe(req, ref)
		return
	}
	permits.arrived()
	a.inflight.add()
	var credit *streamCredit
	if req.GetMode() == hubpb.RequestMode_STREAMING {
		credit = newStreamCredit(req.GetInitialCredit())
	}
	run := a.running.add(a.handlerCtx, req.GetId(), ref, credit)
	go func() {
		defer a.inflight.done()
		defer permits.finished()
		defer a.lastResort(req, func(werr *hubpb.Error) {
			a.running.remove(req.GetId(), run)
			if !run.isCancelled() {
				failRequest(req, ref, werr)
			}
		})
		if credit != nil {
			a.serveStream(run, req, ref)
			a.running.remove(req.GetId(), run)
		} else {
			reply := a.answer(run, req)
			a.running.remove(req.GetId(), run)
			if reply != nil && !run.isCancelled() {
				msg := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Reply{Reply: reply}}
				if err := ref.send(msg); err != nil {
					a.log.Debug("reply dropped: its hub stream is gone", "request", req.GetId(), "type", req.GetType())
				}
			}
		}
	}()
}

// route hands a frame of the hub that answers this instance's requests (Reply, Error, ReplyPart,
// ReplyEnd) or steers the requests it serves (Cancel, Flow) to its owner. Called from the
// stream's read loop; a defect that panics there is logged and the frame dropped: the stream
// goes on.
func (a *App) route(m *hubpb.ServerMessage) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("a frame from the hub could not be handled; it is dropped, the stream goes on",
				"frame", fmt.Sprintf("%T", m.GetMessage()), "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	switch k := m.GetMessage().(type) {
	case *hubpb.ServerMessage_Reply:
		ans := answer{payload: k.Reply.GetPayload()}
		if e, ok := k.Reply.GetResult().(*hubpb.Reply_Error); ok {
			ans = answer{failure: e.Error}
		}
		a.calls.reply(k.Reply.GetRequestId(), ans)
	case *hubpb.ServerMessage_Error:
		a.calls.deliver(k.Error.GetRequestId(), answer{failure: k.Error})
	case *hubpb.ServerMessage_Part:
		a.calls.part(k.Part)
	case *hubpb.ServerMessage_End:
		a.calls.end(k.End)
	case *hubpb.ServerMessage_Cancel:
		a.cancelRequest(k.Cancel.GetRequestId(), k.Cancel.GetReason())
	case *hubpb.ServerMessage_Flow:
		a.flow(k.Flow.GetRequestId(), k.Flow.GetCredits())
	}
}

// lastResort, deferred where a request from the hub is served (on the stream's read loop and on
// the request's own goroutine), turns a panic that escaped every other recover, a defect of the
// library, into the failure of that request alone: logged at error with its stack, then fail
// answers HANDLER_ERROR "handler panicked". The stream and the process go on.
func (a *App) lastResort(req *hubpb.Request, fail func(werr *hubpb.Error)) {
	r := recover()
	if r == nil {
		return
	}
	a.log.Error("serving a request from the hub panicked; the request fails, the stream goes on",
		"request", req.GetId(), "type", req.GetType(), "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
	defer func() { _ = recover() }() // a failure that cannot be answered is dropped
	fail(errorToWire(req.GetId(), errors.New("handler panicked")))
}

// failRequest answers req, which arrived on ref, with werr: a ReplyEnd for a streaming or a
// subscription request, else a Reply.
func failRequest(req *hubpb.Request, ref *streamRef, werr *hubpb.Error) {
	switch req.GetMode() {
	case hubpb.RequestMode_STREAMING, hubpb.RequestMode_SUBSCRIPTION:
		_ = ref.send(subscriptionEnd(req.GetId(), 0, werr))
	default:
		_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Reply{Reply: &hubpb.Reply{
			RequestId: req.GetId(), Result: &hubpb.Reply_Error{Error: werr}}}})
	}
}

// cancelRequest is the hub's Cancel{id, reason} for a request this instance serves (a
// scatter-gather target or a streaming request, say): the handler's context is cancelled and
// nothing more is sent for the request, whatever the handler returns or streams; its permit goes
// back once the handler returned (at once for a handler that watches its context, or one still
// waiting for a handler slot). A subscription target (<id>#n) is removed: nothing more is
// emitted to it. A Cancel for an id that is not running (it already ended) is ignored.
// See https://snodra.damoreira.com/library-authors/protocol/ (Cancel).
func (a *App) cancelRequest(id, reason string) {
	if run := a.running.take(id); run != nil {
		a.log.Debug("request cancelled by the hub", "request", id, "reason", reason)
		run.cancelRun()
		return
	}
	if a.subs.cancel(id) {
		a.log.Debug("subscription cancelled by the hub", "request", id, "reason", reason)
		return
	}
	a.log.Debug("cancel for an unknown request ignored", "request", id, "reason", reason)
}

// flow is the hub's Flow{id, credits} for a streaming request served here: its handler may send
// that many more parts. Ignored for an id that is not a running streaming request.
func (a *App) flow(id string, credits uint32) {
	if run := a.running.get(id); run != nil && run.credit != nil {
		run.credit.add(credits)
		return
	}
	a.log.Debug("flow for an unknown request ignored", "request", id, "credits", credits)
}

// runningRequest is a forwarded request being served: its handler's context, whether the hub
// cancelled it, the stream it arrived on and, for a streaming request, its credit.
type runningRequest struct {
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled chan struct{} // closed by the hub's Cancel (or the loss of its stream, for a streaming request)
	once      sync.Once
	ref       *streamRef
	credit    *streamCredit // nil for a unary request
}

// cancelRun marks the request cancelled, then ends its context.
func (r *runningRequest) cancelRun() {
	r.once.Do(func() { close(r.cancelled) })
	r.cancel()
}

func (r *runningRequest) isCancelled() bool {
	select {
	case <-r.cancelled:
		return true
	default:
		return false
	}
}

// runningRequests are the forwarded requests being served, by id, over every hub stream.
type runningRequests struct {
	mu   sync.Mutex
	byID map[string]*runningRequest
}

func newRunningRequests() *runningRequests {
	return &runningRequests{byID: map[string]*runningRequest{}}
}

// add records request id, which arrived on ref, as running, its context derived from base;
// credit is a streaming request's (nil for a unary one).
func (rs *runningRequests) add(base context.Context, id string, ref *streamRef, credit *streamCredit) *runningRequest {
	ctx, cancel := context.WithCancel(base)
	r := &runningRequest{ctx: ctx, cancel: cancel, cancelled: make(chan struct{}), ref: ref, credit: credit}
	rs.mu.Lock()
	rs.byID[id] = r
	rs.mu.Unlock()
	return r
}

// remove forgets r (only if id still names it) and releases its context.
func (rs *runningRequests) remove(id string, r *runningRequest) {
	rs.mu.Lock()
	if rs.byID[id] == r {
		delete(rs.byID, id)
	}
	rs.mu.Unlock()
	r.cancel()
}

// get is the running request id; nil when none.
func (rs *runningRequests) get(id string) *runningRequest {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.byID[id]
}

// streamLost ends the streaming requests that arrived on ref, a stream that is gone: the hub
// forgot them with the session, and their handlers would otherwise wait for credit, holding
// their permits. Each is cancelled as by the hub; unary requests run on (their replies are
// dropped).
func (rs *runningRequests) streamLost(ref *streamRef) int {
	rs.mu.Lock()
	var lost []*runningRequest
	for id, r := range rs.byID {
		if r.ref == ref && r.credit != nil {
			delete(rs.byID, id)
			lost = append(lost, r)
		}
	}
	rs.mu.Unlock()
	for _, r := range lost {
		r.cancelRun()
	}
	return len(lost)
}

// take removes and returns the running request id; nil when none.
func (rs *runningRequests) take(id string) *runningRequest {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	r := rs.byID[id]
	delete(rs.byID, id)
	return r
}

// answer is the Reply to req, run as run (its handler's context); nil when the app was stopped
// hard, or the hub cancelled the request, before its handler could run.
func (a *App) answer(run *runningRequest, req *hubpb.Request) *hubpb.Reply {
	id := req.GetId()
	h, scope, werr := a.admit(run, req, false)
	if werr != nil {
		return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Error{Error: werr}}
	}
	if h == nil {
		return nil
	}
	defer func() { <-a.slots }()
	payload, werr := a.run(run.ctx, h, req, scope)
	if werr != nil {
		return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Error{Error: werr}}
	}
	return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Payload{Payload: payload}}
}

// admit is what a forwarded request (streaming when streaming) goes through before its handler,
// in order: the caller (a token that fails is UNAUTHENTICATED, whatever the handler), the kind,
// the handler for that mode, its guards, a handler slot. It returns the handler and the request's
// scope, holding a slot (the caller gives it back), or the error to answer; nothing at all (all
// nil) when the app was stopped hard, or the hub cancelled the request, while it waited for a slot.
func (a *App) admit(run *runningRequest, req *hubpb.Request, streaming bool) (*handler, *handling, *hubpb.Error) {
	id := req.GetId()
	fail := func(code hubpb.ErrorCode, msg string) *hubpb.Error {
		return &hubpb.Error{RequestId: id, Code: code, Message: lineBreaks.Replace(msg)}
	}
	// TODO(spec-gap): SG-18 (every forwarded token is checked, also for a handler without a guard).
	authorization := req.GetHeaders()[authorizationHeader]
	caller, err := a.identity.resolve(run.ctx, authorization)
	if err != nil {
		a.log.Debug("request refused: its caller's token", "request", id, "type", req.GetType(), "reason", err.Error())
		return nil, nil, fail(hubpb.ErrorCode_UNAUTHENTICATED, err.Error())
	}
	h, code, msg := a.lookup(req, streaming)
	if h == nil {
		return nil, nil, fail(code, msg)
	}
	// Before decoding: an anonymous caller cannot probe the payload's shape. The guards
	// themselves run after the decoding (callerCheck), as in the reference library.
	if code, msg, refused := anonymousRefusal(h.cfg, caller); refused {
		return nil, nil, fail(hubpb.ErrorCode(code), msg)
	}
	select {
	case a.slots <- struct{}{}:
	case <-a.killed:
		return nil, nil, nil
	case <-run.cancelled: // by the hub: nothing is answered
		return nil, nil, nil
	}
	scope := &handling{
		info: MessageInfo{
			ID:            id,
			Type:          req.GetType(),
			CorrelationID: req.GetHeaders()[correlationIDHeader],
			Headers:       maps.Clone(req.GetHeaders()),
		},
		authorization: authorization,
		caller:        caller,
	}
	// The id stands in only for a missing header: a present but empty one stays empty, and the
	// requests sent while handling get a new correlation id, as in the reference library.
	if _, present := req.GetHeaders()[correlationIDHeader]; !present {
		scope.info.CorrelationID = id
	}
	if tp := req.GetHeaders()[traceparentHeader]; traceparent.Valid(tp) {
		scope.traceparent = tp
	}
	return h, scope, nil
}

// lookup is the handler of req in its mode (streaming or unary), or the error code and message
// to answer, as the reference library answers. A unary request: NO_HANDLER for a type not served
// here in the request's kind, INVALID_REQUEST for an unknown kind and for a streaming or a
// subscription query handler (the caller used the wrong call, which no retry fixes; a
// subscription's initial request <id>#0 excepted). A streaming request is served by a streaming
// query handler whatever its kind: NO_HANDLER for a type without a query handler (a command
// type, a type not served), INVALID_REQUEST for a query handler of another mode.
// TODO(spec-gap): SG-12 (the answers to a request in a mode its handler does not serve).
func (a *App) lookup(req *hubpb.Request, streaming bool) (*handler, hubpb.ErrorCode, string) {
	typ := req.GetType()
	h := a.handlers[typ] // read-only once started
	if streaming {
		switch {
		case h == nil || h.kind == kindCommand:
			return nil, hubpb.ErrorCode_NO_HANDLER, noHandlerWireText(typ)
		case h.kind != kindStream:
			return nil, hubpb.ErrorCode_INVALID_REQUEST, typ + " is not a streaming query handler (it answers mode " + h.mode() + ")"
		}
		return h, 0, ""
	}
	switch req.GetKind() {
	case hubpb.RequestKind_COMMAND:
		if h == nil || h.kind != kindCommand {
			return nil, hubpb.ErrorCode_NO_HANDLER, noHandlerWireText(typ)
		}
		return h, 0, ""
	case hubpb.RequestKind_QUERY:
	default:
		return nil, hubpb.ErrorCode_INVALID_REQUEST, "unsupported request kind " + req.GetKind().String()
	}
	switch {
	case h == nil || h.kind == kindCommand:
		return nil, hubpb.ErrorCode_NO_HANDLER, noHandlerWireText(typ)
	case h.kind == kindSubscription:
		// A subscription's initial result is the unary request <id>#0, answered by the
		// subscription's initial handler; any other unary request used the wrong call.
		// TODO(spec-gap): SG-13 (recognised by the #0 suffix; the hub gathers no declared subscription type).
		if strings.HasSuffix(req.GetId(), "#0") {
			return h, 0, ""
		}
		return nil, hubpb.ErrorCode_INVALID_REQUEST, subscriptionQueryText(typ)
	case h.kind == kindStream:
		return nil, hubpb.ErrorCode_INVALID_REQUEST, streamingQueryText(typ)
	}
	return h, 0, ""
}

// streamingQueryText is the INVALID_REQUEST message of a unary request of the streaming query typ.
func streamingQueryText(typ string) string {
	return typ + " is a streaming query: send it with Stream (mode STREAMING)"
}

// subscriptionQueryText is the INVALID_REQUEST message of a unary request of the subscription
// query typ that is not a subscription's initial request.
func subscriptionQueryText(typ string) string {
	return typ + " is a subscription query: send it with Subscribe (mode SUBSCRIPTION)"
}

// run runs h on req under recover: the answer's payload, or the error to answer. A panic, of
// the handler or of its error, is HANDLER_ERROR "handler panicked", the reference library's
// text: the panic value and the stack go to the log only.
func (a *App) run(ctx context.Context, h *handler, req *hubpb.Request, scope *handling) (payload []byte, werr *hubpb.Error) {
	ctx, done := a.tel.startHandling(ctx, handlingOf(req, scope))
	payload, werr = a.invoke(ctx, h, req, scope)
	if done != nil {
		done(handlingErr(werr))
	}
	return payload, werr
}

// invoke runs h on req under recover, as run does, without the telemetry.
func (a *App) invoke(ctx context.Context, h *handler, req *hubpb.Request, scope *handling) (payload []byte, werr *hubpb.Error) {
	defer a.recoverHandler(req, &werr)
	payload, err := h.invoke(withHandling(ctx, scope), a.cfg.codec, req.GetPayload(), a.callerCheck(h.cfg, req.GetType(), scope.caller))
	if err != nil {
		return nil, handlerFailure(req.GetId(), err)
	}
	return payload, nil
}

// handlingOf is what the telemetry is told of the request req about to be handled, with its
// scope.
func handlingOf(req *hubpb.Request, scope *handling) Handling {
	h := Handling{Type: req.GetType(), Kind: kindLabel(req.GetKind() == hubpb.RequestKind_COMMAND),
		Traceparent: scope.traceparent}
	if h.Traceparent != "" {
		h.Tracestate = req.GetHeaders()[tracestateHeader]
	}
	return h
}

// handlingErr is the error of a handling that answered werr (nil: it succeeded).
func handlingErr(werr *hubpb.Error) error {
	if werr == nil {
		return nil
	}
	return errors.New(werr.GetMessage())
}

// recoverHandler, deferred, turns a panic of the handler of req into HANDLER_ERROR "handler
// panicked" in werr; the panic value and the stack go to the log only.
func (a *App) recoverHandler(req *hubpb.Request, werr **hubpb.Error) {
	if r := recover(); r != nil {
		a.log.Error("handler panicked", "request", req.GetId(), "type", req.GetType(),
			"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		*werr = errorToWire(req.GetId(), errors.New("handler panicked"))
	}
}

// handlerFailure is the error a handler's err answers for request id: INVALID_REQUEST for a
// payload that does not decode, else what errorToWire makes of it.
func handlerFailure(id string, err error) *hubpb.Error {
	var de *decodeError
	if errors.As(err, &de) {
		return &hubpb.Error{RequestId: id, Code: hubpb.ErrorCode_INVALID_REQUEST, Message: lineBreaks.Replace(de.Error())}
	}
	return errorToWire(id, err)
}

// inflight counts the requests being served, for the drain at shutdown.
type inflight struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed while n is 0
}

func newInflight() *inflight {
	f := &inflight{idle: make(chan struct{})}
	close(f.idle)
	return f
}

func (f *inflight) add() {
	f.mu.Lock()
	if f.n == 0 {
		f.idle = make(chan struct{})
	}
	f.n++
	f.mu.Unlock()
}

func (f *inflight) done() {
	f.mu.Lock()
	f.n--
	if f.n == 0 {
		close(f.idle)
	}
	f.mu.Unlock()
}

// drained is closed once no request is being served.
func (f *inflight) drained() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.idle
}

// count is the requests being served now.
func (f *inflight) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}
