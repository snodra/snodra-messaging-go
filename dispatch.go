package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"sync"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/traceparent"
)

// dispatch serves a request the hub forwarded on the stream ref, also before its RegisterAck.
// Called from the stream's read loop: the request counts against the stream's permits at once
// and is recorded as running (so a hub Cancel that follows it finds it), then runs on its own
// goroutine, which writes exactly one Reply to ref (dropped if that stream is gone; none at all
// once the hub cancelled the request) and gives the permit back to ref's hub.
// See https://snodra.damoreira.com/library-authors/protocol/ (Handling a request).
func (a *App) dispatch(req *hubpb.Request, ref *streamRef, permits *streamPermits) {
	permits.arrived()
	a.inflight.add()
	run := a.running.add(a.handlerCtx, req.GetId())
	go func() {
		defer a.inflight.done()
		reply := a.answer(run, req)
		a.running.remove(req.GetId(), run)
		if reply != nil && !run.isCancelled() {
			msg := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Reply{Reply: reply}}
			if err := ref.send(msg); err != nil {
				a.log.Debug("reply dropped: its hub stream is gone", "request", req.GetId(), "type", req.GetType())
			}
		}
		permits.finished()
	}()
}

// cancelRequest is the hub's Cancel{id, reason} for a request this instance serves (a
// scatter-gather target, say): the handler's context is cancelled and nothing is sent for the
// request, whatever the handler returns; its permit goes back once the handler returned (at
// once for a handler that watches its context, or one still waiting for a handler slot). A
// Cancel for an id that is not running (it already ended) is ignored.
// See https://snodra.damoreira.com/library-authors/protocol/ (Cancel).
func (a *App) cancelRequest(id, reason string) {
	if run := a.running.take(id); run != nil {
		a.log.Debug("request cancelled by the hub", "request", id, "reason", reason)
		run.cancelRun()
		return
	}
	a.log.Debug("cancel for an unknown request ignored", "request", id, "reason", reason)
}

// runningRequest is a forwarded request being served: its handler's context and whether the
// hub cancelled it.
type runningRequest struct {
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled chan struct{} // closed by the hub's Cancel
	once      sync.Once
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

// add records request id as running, its context derived from base.
func (rs *runningRequests) add(base context.Context, id string) *runningRequest {
	ctx, cancel := context.WithCancel(base)
	r := &runningRequest{ctx: ctx, cancel: cancel, cancelled: make(chan struct{})}
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

// take removes and returns the running request id; nil when none.
func (rs *runningRequests) take(id string) *runningRequest {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	r := rs.byID[id]
	delete(rs.byID, id)
	return r
}

// answer is the Reply to req, run as run (its handler's context); nil when the app was stopped
// hard, or the hub cancelled the request, before its handler could run. In order: the caller (a
// token that fails is UNAUTHENTICATED, whatever the handler), the kind, the handler, its guards,
// a handler slot, then the decode and the handler.
func (a *App) answer(run *runningRequest, req *hubpb.Request) *hubpb.Reply {
	id := req.GetId()
	fail := func(code hubpb.ErrorCode, msg string) *hubpb.Reply {
		return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Error{Error: &hubpb.Error{
			RequestId: id, Code: code, Message: lineBreaks.Replace(msg),
		}}}
	}
	// TODO(spec-gap): SG-18 (every forwarded token is checked, also for a handler without a guard).
	authorization := req.GetHeaders()[authorizationHeader]
	caller, err := a.identity.resolve(run.ctx, authorization)
	if err != nil {
		a.log.Debug("request refused: its caller's token", "request", id, "type", req.GetType(), "reason", err.Error())
		return fail(hubpb.ErrorCode_UNAUTHENTICATED, err.Error())
	}
	var want handlerKind
	switch req.GetKind() {
	case hubpb.RequestKind_COMMAND:
		want = kindCommand
	case hubpb.RequestKind_QUERY:
		want = kindQuery
	default:
		return fail(hubpb.ErrorCode_INVALID_REQUEST, fmt.Sprintf("unsupported request kind %d", req.GetKind()))
	}
	h := a.handlers[req.GetType()] // read-only once started
	if h == nil || h.kind != want {
		return fail(hubpb.ErrorCode_NO_HANDLER, noHandlerText(req.GetType()))
	}
	// Before decoding: an anonymous caller cannot probe the payload's shape.
	if code, msg, refused := a.guardRefusal(h.cfg, req.GetType(), caller); refused {
		return fail(hubpb.ErrorCode(code), msg)
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	case <-a.killed:
		return nil
	case <-run.cancelled: // by the hub: nothing is answered
		return nil
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
	payload, werr := a.run(run.ctx, h, req, scope)
	if werr != nil {
		return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Error{Error: werr}}
	}
	return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Payload{Payload: payload}}
}

// run runs h on req under recover: the answer's payload, or the error to answer. A panic, of
// the handler or of its error, is HANDLER_ERROR "handler panicked", the reference library's
// text: the panic value and the stack go to the log only.
func (a *App) run(ctx context.Context, h *handler, req *hubpb.Request, scope *handling) (payload []byte, werr *hubpb.Error) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("handler panicked", "request", req.GetId(), "type", req.GetType(),
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			payload, werr = nil, errorToWire(req.GetId(), errors.New("handler panicked"))
		}
	}()
	payload, err := h.invoke(withHandling(ctx, scope), a.cfg.codec, req.GetPayload())
	if err == nil {
		return payload, nil
	}
	var de *decodeError
	if errors.As(err, &de) {
		return nil, &hubpb.Error{RequestId: req.GetId(), Code: hubpb.ErrorCode_INVALID_REQUEST,
			Message: lineBreaks.Replace(de.Error())}
	}
	return nil, errorToWire(req.GetId(), err)
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
