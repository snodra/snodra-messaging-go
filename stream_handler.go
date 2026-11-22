package snodra

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// HandleStream registers h as the handler of the streaming query type Q, by Q's MessageType: a
// request in mode STREAMING is answered by the items h sends with its [StreamWriter], one
// ReplyPart each, numbered 1, 2, 3, ..., then one ReplyEnd when h returns (with h's error, as
// [HandleQuery] maps it, when it returns one). Q is registered in Register.query_types (with
// [Shared], in Register.shared_query_types); a unary request of Q is answered INVALID_REQUEST.
//
// The items go out under the caller's credit: [StreamWriter.Send] waits for credit, at most
// [StreamCreditTimeout] (10 minutes by default), after which the stream ends with TIMEOUT. When
// the hub cancels the stream (the caller stopped reading, a limit, the hub's idle timeout) or its
// hub stream is lost, h's context is cancelled, Send fails and nothing more is sent, whatever h
// returns. The request holds its handler slot and its permit until h returns.
//
// Registration errors are those of [HandleCommand].
// See https://snodra.damoreira.com/library-authors/protocol/ (Streaming).
func HandleStream[Q Message, T any](app *App, h func(context.Context, Q, *StreamWriter[T]) error, opts ...HandlerOption) error {
	if app == nil {
		return errors.New("snodra: registration on a nil App")
	}
	name, cfg, err := handlerSetup[Q](kindStream, h == nil, opts)
	return app.addHandler(&handler{kind: kindStream, name: name, stream: streamInvokerOf(h), cfg: cfg}, err)
}

// streamInvokerOf adapts h to the dispatcher: decode a fresh Q, run h with a writer on out.
func streamInvokerOf[Q Message, T any](h func(context.Context, Q, *StreamWriter[T]) error) streamInvoker {
	return func(ctx context.Context, codec PayloadCodec, payload []byte, out *streamOut) error {
		msg, err := decodeMessage[Q](codec, payload)
		if err != nil {
			return err
		}
		return h(ctx, msg, &StreamWriter[T]{out: out, codec: codec})
	}
}

// StreamWriter sends the items of one streaming request ([HandleStream]). It is safe for
// concurrent use; the items go out in the order Send is called.
type StreamWriter[T any] struct {
	out   *streamOut
	codec PayloadCodec
}

// Send encodes item with the app's codec and sends it as the stream's next part, once the caller
// granted credit for it. It returns an error, and sends nothing, when ctx ends first, when the
// hub cancelled the stream or its hub stream is gone, when no credit came within
// [StreamCreditTimeout] (the stream then ended with TIMEOUT), or once the handler returned.
func (w *StreamWriter[T]) Send(ctx context.Context, item T) error {
	payload, err := encodeResult(w.codec, item)
	if err != nil {
		return err
	}
	return w.out.part(ctx, payload)
}

// StreamCreditTimeout bounds how long a streaming handler's [StreamWriter.Send] waits for the
// caller's credit: then the stream ends with TIMEOUT "no credit within <d>" and the handler's
// context is cancelled, so that a hub that forgot the stream (a lost Flow or Cancel) cannot hold
// the handler and its permit forever. The default is 10 minutes, the hub's default maximum
// stream duration; d must be at least 1 ms.
func StreamCreditTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Millisecond {
			return fmt.Errorf("snodra: StreamCreditTimeout must be at least 1ms, got %v", d)
		}
		c.timings.creditWait = d
		return nil
	}
}

// errStreamOver is the error of a Send to a stream that is over.
var errStreamOver = errors.New("snodra: the stream is over")

// streamCredit is a streaming request's remaining credit: Request.initial_credit plus every
// Flow, less the parts sent.
type streamCredit struct {
	mu   sync.Mutex
	n    uint64
	more chan struct{} // signalled when credit is added
}

func newStreamCredit(initial uint32) *streamCredit {
	return &streamCredit{n: uint64(initial), more: make(chan struct{}, 1)}
}

// add grants n more parts.
func (c *streamCredit) add(n uint32) {
	c.mu.Lock()
	c.n += uint64(n)
	c.mu.Unlock()
	select {
	case c.more <- struct{}{}:
	default:
	}
}

// errNoCredit is a credit wait that expired.
var errNoCredit = errors.New("no credit")

// take uses one credit, waiting for it at most wait: errNoCredit then, ctx's error when ctx ends
// first, errStreamOver when one of stops closes first.
func (c *streamCredit) take(ctx context.Context, wait time.Duration, stops ...<-chan struct{}) error {
	var timer *time.Timer
	for {
		c.mu.Lock()
		if c.n > 0 {
			c.n--
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(wait)
			defer timer.Stop()
		}
		select {
		case <-c.more:
		case <-ctx.Done():
			return ctx.Err()
		case <-stops[0]:
			return errStreamOver
		case <-stops[1]:
			return errStreamOver
		case <-stops[2]:
			return errStreamOver
		case <-timer.C:
			return errNoCredit
		}
	}
}

// streamOut is a streaming request's side of its arrival stream: its parts, numbered from 1,
// under its credit, and its one ReplyEnd; nothing once the hub cancelled it.
type streamOut struct {
	run        *runningRequest
	ref        *streamRef
	id         string
	creditWait time.Duration
	ended      chan struct{} // closed once the end is written, or must not be

	sendMu sync.Mutex // one part at a time, in order

	mu   sync.Mutex
	seq  uint64
	why  error // why a Send fails now; nil while the stream is open
	once sync.Once
}

func newStreamOut(a *App, run *runningRequest, ref *streamRef, id string) *streamOut {
	return &streamOut{run: run, ref: ref, id: id, creditWait: a.cfg.timings.creditWait, ended: make(chan struct{})}
}

// finish marks the stream over with why (the first reason wins); true when it was open.
func (o *streamOut) finish(why error) bool {
	o.mu.Lock()
	open := o.why == nil
	if open {
		o.why = why
	}
	o.mu.Unlock()
	o.once.Do(func() { close(o.ended) })
	return open
}

// state is why a Send fails now (the hub cancelled the stream, its hub stream is gone, it
// ended); nil while the stream is open. Called with o.mu held.
func (o *streamOut) state() error {
	if o.why == nil {
		select {
		case <-o.run.cancelled:
			o.why = fmt.Errorf("%w: the hub cancelled it", errStreamOver)
		case <-o.ref.gone:
			o.why = fmt.Errorf("%w: its hub stream is gone", errStreamOver)
		default:
		}
	}
	return o.why
}

// stateNow is state, taking o.mu.
func (o *streamOut) stateNow() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.state()
}

// part sends payload as the next part once a credit is available.
func (o *streamOut) part(ctx context.Context, payload []byte) error {
	o.sendMu.Lock()
	defer o.sendMu.Unlock()
	if err := o.stateNow(); err != nil {
		return err
	}
	switch err := o.run.credit.take(ctx, o.creditWait, o.ended, o.run.cancelled, o.ref.gone); {
	case errors.Is(err, errNoCredit):
		msg := "no credit within " + rustDuration(o.creditWait)
		if o.end(&hubpb.Error{RequestId: o.id, Code: hubpb.ErrorCode_TIMEOUT, Message: msg}) {
			o.run.cancel() // the handler need not go on
		}
		return o.stateNow()
	case errors.Is(err, errStreamOver):
		return o.stateNow()
	case err != nil:
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.state(); err != nil {
		return err
	}
	o.seq++
	msg := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Part{Part: &hubpb.ReplyPart{
		RequestId: o.id, Seq: o.seq, Payload: payload,
	}}}
	if err := o.ref.send(msg); err != nil {
		o.why = fmt.Errorf("%w: its hub stream is gone", errStreamOver)
		return o.why
	}
	return nil
}

// end writes the stream's ReplyEnd, with werr when it failed, unless the stream is over already
// (ended, cancelled by the hub, its stream gone); true when it wrote it.
func (o *streamOut) end(werr *hubpb.Error) bool {
	why := errStreamOver
	if werr != nil {
		why = fmt.Errorf("%w: %s", errStreamOver, werr.GetMessage())
	}
	if !o.finish(why) || o.run.isCancelled() {
		return false // over already, or cancelled by the hub: nothing more is sent
	}
	end := &hubpb.ReplyEnd{RequestId: o.id, Error: werr}
	return o.ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_End{End: end}}) == nil
}

// serveStream serves the streaming request req that arrived on ref, run as run: its parts and
// one ReplyEnd (also for a request refused before its handler ran), nothing once the hub
// cancelled it or its stream is gone.
func (a *App) serveStream(run *runningRequest, req *hubpb.Request, ref *streamRef) {
	out := newStreamOut(a, run, ref, req.GetId())
	h, scope, werr := a.admit(run, req, true)
	switch {
	case werr != nil:
		a.log.Debug("streaming request rejected", "request", req.GetId(), "type", req.GetType(), "reason", werr.GetMessage())
		out.end(werr)
		return
	case h == nil:
		out.finish(errStreamOver)
		return
	}
	defer func() { <-a.slots }()
	if werr := a.runStream(run.ctx, h, req, scope, out); !out.end(werr) && werr != nil {
		a.log.Debug("streaming handler's error dropped: the stream was over", "request", req.GetId(), "type", req.GetType())
	}
}

// runStream runs the streaming handler h on req under recover: nil when it ended cleanly, else
// the error to end the stream with ("handler panicked" for a panic, as for a unary handler).
func (a *App) runStream(ctx context.Context, h *handler, req *hubpb.Request, scope *handling, out *streamOut) (werr *hubpb.Error) {
	defer a.recoverHandler(req, &werr)
	if err := h.stream(withHandling(ctx, scope), a.cfg.codec, req.GetPayload(), out); err != nil {
		return handlerFailure(req.GetId(), err)
	}
	return nil
}

// rustDuration writes d as the reference library writes a duration in its messages (Rust's
// Debug of a Duration): whole and fractional seconds from 1 s ("600s", "1.5s"), else
// milliseconds, microseconds or nanoseconds ("300ms", "1.5ms", "7µs", "12ns").
func rustDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	unit := func(n, per int64, suffix string) string {
		whole, frac := n/per, n%per
		s := strconv.FormatInt(whole, 10)
		if frac != 0 {
			digits := len(strconv.FormatInt(per, 10)) - 1
			f := strconv.FormatInt(frac, 10)
			s += "." + strings.TrimRight(strings.Repeat("0", digits-len(f))+f, "0")
		}
		return s + suffix
	}
	n := int64(d)
	switch {
	case d >= time.Second:
		return unit(n, int64(time.Second), "s")
	case d >= time.Millisecond:
		return unit(n, int64(time.Millisecond), "ms")
	case d >= time.Microsecond:
		return unit(n, int64(time.Microsecond), "µs")
	}
	return strconv.FormatInt(n, 10) + "ns"
}
