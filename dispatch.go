package snodramessaging

import (
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"sync"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// dispatch serves a request the hub forwarded on the stream ref, also before its RegisterAck.
// Called from the stream's read loop: the request counts against the stream's permits at once,
// then runs on its own goroutine, which writes exactly one Reply to ref (dropped if that stream
// is gone) and gives the permit back to ref's hub.
// See https://snodra.damoreira.com/library-authors/protocol/ (Handling a request).
func (a *App) dispatch(req *hubpb.Request, ref *streamRef, permits *streamPermits) {
	permits.arrived()
	a.inflight.add()
	go func() {
		defer a.inflight.done()
		if reply := a.answer(req); reply != nil {
			msg := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Reply{Reply: reply}}
			if err := ref.send(msg); err != nil {
				a.log.Debug("reply dropped: its hub stream is gone", "request", req.GetId(), "type", req.GetType())
			}
		}
		permits.finished()
	}()
}

// answer is the Reply to req; nil when the app was stopped hard before its handler could run.
func (a *App) answer(req *hubpb.Request) *hubpb.Reply {
	id := req.GetId()
	fail := func(code hubpb.ErrorCode, msg string) *hubpb.Reply {
		return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Error{Error: &hubpb.Error{
			RequestId: id, Code: code, Message: lineBreaks.Replace(msg),
		}}}
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
		return fail(hubpb.ErrorCode_NO_HANDLER, fmt.Sprintf("No handler registered for '%s'", req.GetType()))
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	case <-a.killed:
		return nil
	}
	payload, werr := a.run(h, req)
	if werr != nil {
		return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Error{Error: werr}}
	}
	return &hubpb.Reply{RequestId: id, Result: &hubpb.Reply_Payload{Payload: payload}}
}

// run runs h on req under recover: the answer's payload, or the error to answer. A panic, of
// the handler or of its error, is HANDLER_ERROR "handler panicked", the reference library's
// text: the panic value and the stack go to the log only.
func (a *App) run(h *handler, req *hubpb.Request) (payload []byte, werr *hubpb.Error) {
	defer func() {
		if r := recover(); r != nil {
			a.log.Error("handler panicked", "request", req.GetId(), "type", req.GetType(),
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			payload, werr = nil, errorToWire(req.GetId(), errors.New("handler panicked"))
		}
	}()
	info := MessageInfo{
		ID:            req.GetId(),
		Type:          req.GetType(),
		CorrelationID: req.GetHeaders()[correlationIDHeader],
		Headers:       maps.Clone(req.GetHeaders()),
	}
	if info.CorrelationID == "" {
		info.CorrelationID = info.ID
	}
	payload, err := h.invoke(withMessageInfo(a.handlerCtx, info), a.cfg.codec, req.GetPayload())
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
