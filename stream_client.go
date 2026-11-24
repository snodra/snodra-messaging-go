package snodra

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// InitialCredit is the credit window of a [Stream] or a [Subscribe] (Request.initial_credit):
// the parts the hub may relay before the first refill (a subscription's initial result
// included). The caller gives back half of it every time half of it was read, so the hub never
// relays more than a window ahead of the caller. 0, the default, reads as 64. The other calls
// ignore it.
func InitialCredit(n uint32) SendOption { return func(c *sendConfig) { c.initialCredit = n } }

// Stream streams the result of the streaming query q from one instance of its owner ([HandleStream]
// on the other side): ranging over the returned sequence sends one Request in mode STREAMING to
// one hub, unkeyed, and yields the handler's items, decoded as T, in the handler's order, until
// the hub's end. Nothing is sent until the range starts; each range sends the query anew.
//
//	for line, err := range snodra.Stream[OrderLine](ctx, app, ListOrders{}) {
//		if err != nil {
//			return err // the last thing the sequence yields
//		}
//		...
//	}
//
// An error is the last thing the sequence yields: one of the errors of [Query] before the first
// item; after it, the hub's end with an error (an [*Error] by its code: the handler's error, the
// hub's idle timeout, HANDLER_DISCONNECTED, ...), [ErrOutcomeUnknown] when the hub stream was
// lost, [ErrLostParts] when the hub dropped items (a gap in the parts' numbers, or an end that
// counts more parts than arrived), the context's error (with [ErrOutcomeUnknown]), or an item that
// does not decode. Breaking out of the loop, an item that does not decode, lost parts and the
// local guard cancel the stream at the hub, which tells the handler.
//
// The options are those of [Query]: [Timeout] (or [DefaultTimeout], or ctx's deadline) is the idle
// timeout, the longest the hub waits for the next item, sent as Request.timeout_ms; the local
// guard waits the hub's bound plus 1 s between items and twice the bound plus 1 s for the first;
// [InitialCredit] is the credit window; [MaxResults] ends the stream after that many items; the
// retry policy applies until the first item (a retry is a new stream with a fresh id), never
// after it. [RoutingKey] is ignored: a stream goes to one hub, unkeyed.
// See https://snodra.damoreira.com/docs/library-authors/protocol/ (Streaming).
func Stream[T any](ctx context.Context, app *App, q Message, opts ...SendOption) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		out, err := prepare(ctx, app, hubpb.RequestKind_QUERY, hubpb.RequestMode_STREAMING, q, opts)
		if err != nil {
			yield(zero, err)
			return
		}
		var s *streaming
		defer func() {
			if s != nil {
				s.cancel(s.callerGone())
			}
		}()
		// TODO(spec-gap): SG-21 (retried until the first item, each attempt sending the chosen idle timeout).
		first, err := retrying(ctx, hubpb.RequestKind_QUERY, out.policy, out.budget, out.chosen, func(time.Duration) (*hubpb.ReplyPart, error) {
			opened, err := app.openStream(ctx, out.tmpl, out.chosen)
			if err != nil {
				return nil, err
			}
			s = opened
			return s.next(ctx)
		})
		if err != nil {
			yield(zero, err)
			return
		}
		for part := first; part != nil; {
			v, err := decodeAnswer[T](app.cfg.codec, part.GetPayload())
			if err != nil {
				err = fmt.Errorf("snodra: cannot decode a part of %s from %s/%s: %w",
					out.typ, part.GetResponderService(), part.GetResponderInstance(), err)
				s.cancel(collectorFailed(err))
				yield(zero, err)
				return
			}
			s.consumed()
			if !yield(v, nil) {
				return
			}
			if part, err = s.next(ctx); err != nil {
				yield(zero, err)
				return
			}
		}
	}
}

// streamEvent is what came for a streaming query: a part, its end or the hub's error.
type streamEvent struct {
	part    *hubpb.ReplyPart
	end     *hubpb.ReplyEnd
	failure *hubpb.Error
}

// streamInbox holds what came for a streaming query, in arrival order, until the caller reads
// it: the hub sends at most the credit ahead of the caller.
type streamInbox struct {
	mu     sync.Mutex
	events []streamEvent
	wake   chan struct{} // signalled when an event is pushed
}

func newStreamInbox() *streamInbox { return &streamInbox{wake: make(chan struct{}, 1)} }

func (b *streamInbox) push(e streamEvent) {
	b.mu.Lock()
	b.events = append(b.events, e)
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *streamInbox) pop() (streamEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return streamEvent{}, false
	}
	e := b.events[0]
	b.events[0] = streamEvent{}
	b.events = b.events[1:]
	return e, true
}

// streaming is one streaming query or subscription written (or being written) to a hub stream:
// its parts as they arrive, checked, then its end. Flow and Cancel go to the stream it was
// written to.
type streaming struct {
	app      *App
	typ      string
	tmpl     *hubpb.Request // what a move to another hub writes again
	chosen   time.Duration  // the timeout the request is written with
	deadline time.Time      // no move starts after it (hubWait from the open)
	moves    int            // the moves to another hub of a request never written
	tried    triedHubs      // the hubs that did not take it, not tried again
	c        *call
	ref      *streamRef
	conn     *hubConn        // the hub of ref
	what     string          // "stream" or "subscription", for the errors
	closed   <-chan struct{} // a subscription's Close; nil for a stream
	guard    time.Duration   // the hub's bound plus 1 s: a stream's idle guard, a subscription's for its initial part
	refill   uint32          // the credit given back every time that many items were read
	first    bool            // no part arrived yet
	received uint64          // parts received: the next must carry seq received+1
	read     uint32          // items read since the last refill

	// mu guards done: a subscription's Close cancels it from any goroutine, also while a range
	// reads it. Everything else is the reader's own.
	mu   sync.Mutex
	done bool // ended: nothing more to read, nothing to cancel
}

// openStream writes a new streaming query or subscription of tmpl (fresh id, timeout chosen) to
// a hub.
func (a *App) openStream(ctx context.Context, tmpl *hubpb.Request, chosen time.Duration) (*streaming, error) {
	c := &call{id: uuid.New(), inbox: newStreamInbox()}
	deadline := hubDeadline(chosen)
	var tried triedHubs
	ref, guard, conn, err := a.write(ctx, tmpl, chosen, deadline, c, &tried)
	if err != nil {
		return nil, err
	}
	what := "stream"
	if tmpl.GetMode() == hubpb.RequestMode_SUBSCRIPTION {
		what = "subscription"
	}
	return &streaming{app: a, typ: tmpl.GetType(), tmpl: tmpl, chosen: chosen, deadline: deadline, tried: tried, c: c, ref: ref, conn: conn, what: what, guard: guard,
		refill: max(1, tmpl.GetInitialCredit()/2), first: true}, nil
}

// move writes the request, never written to its lost stream, to the next hub with the same id:
// it was never sent (https://snodra.damoreira.com/docs/library-authors/protocol/, Several hubs), at
// most maxUnwrittenMoves times, and none after the deadline of its open (as a unary request).
// The hub that lost it is not tried again (see triedHubs).
// TODO(spec-gap): SG-10 (at most 8 moves).
func (s *streaming) move(ctx context.Context) error {
	s.moves++
	if s.moves > maxUnwrittenMoves {
		return fmt.Errorf("%w: '%s' not written: %d hub streams lost before it was", ErrNeverSent, s.typ, s.moves)
	}
	if !time.Now().Before(s.deadline) {
		return fmt.Errorf("%w: no hub took '%s' within %v", ErrNeverSent, s.typ, hubWait(s.chosen))
	}
	s.app.log.Debug("the hub stream was lost before the "+s.what+" was written; trying the next hub", "type", s.typ, "request", s.c.id)
	c := &call{id: s.c.id, inbox: newStreamInbox()}
	s.tried.hubs = append(s.tried.hubs, s.conn)
	ref, guard, conn, err := s.app.write(ctx, s.tmpl, s.chosen, s.deadline, c, &s.tried)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.c, s.ref, s.guard, s.conn = c, ref, guard, conn
	s.mu.Unlock()
	return nil
}

// errClosed ends the reading of a subscription its owner closed.
var errClosed = errors.New("snodra: the subscription is closed")

// firstPartGuard is the local guard of a stream's first part, from its idle guard: twice the
// hub's bound plus 1 s (the hub bounds a queue wait by the timeout, then starts its idle clock
// at the forward).
// TODO(spec-gap): SG-3 (the local guards of a stream).
func firstPartGuard(idle time.Duration) time.Duration {
	return idle + max(idle-guardMargin, 0)
}

// next is the stream's next part; nil at its clean end. An error ends the stream; errClosed
// is a subscription its owner closed (it is not cancelled here).
func (s *streaming) next(ctx context.Context) (*hubpb.ReplyPart, error) {
	if s.isDone() {
		select {
		case <-s.closed: // ended by its owner's Close
			return nil, errClosed
		default:
			return nil, nil
		}
	}
	// A caller that gave up gets its context's error at once, before what is buffered, and the
	// hub its Cancel without delay.
	if ctx.Err() != nil {
		return nil, s.dropped(ctx)
	}
	select {
	case <-s.closed:
		return nil, errClosed
	default:
	}
	var timer *time.Timer
	var expired <-chan time.Time
	arm := func() {
		if timer != nil {
			timer.Stop()
		}
		if wait, ok := s.guardNow(); ok {
			timer = time.NewTimer(wait)
			expired = timer.C
		}
	}
	arm()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if e, ok := s.c.inbox.pop(); ok {
			return s.handle(e)
		}
		select {
		case <-s.c.inbox.wake:
		case <-s.ref.gone:
			if e, ok := s.c.inbox.pop(); ok { // what came before the loss first
				return s.handle(e)
			}
			if s.ref.withdraw(s.c) {
				// Never written: the next hub gets it, with the same id and a new guard.
				if err := s.move(ctx); err != nil {
					s.finish()
					return nil, err
				}
				arm()
				continue
			}
			s.finish()
			return nil, fmt.Errorf("%w: the hub stream closed during the %s of %s", ErrOutcomeUnknown, s.what, s.typ)
		case <-s.closed:
			if ctx.Err() != nil {
				return nil, s.dropped(ctx)
			}
			return nil, errClosed
		case <-expired:
			// The hub ends an idle stream itself; the guard covers a hub that never does (told to
			// cancel, so that the handler stops holding its permit).
			s.cancel(s.guardFired())
			return nil, &Error{Code: CodeTimeout, Message: timeoutText(s.typ)}
		case <-ctx.Done():
			return nil, s.dropped(ctx)
		}
	}
}

// isDone reports whether the stream ended here: nothing more to read or cancel.
func (s *streaming) isDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// end marks the stream ended; true when it was not before (the caller ends it).
func (s *streaming) end() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	was := s.done
	s.done = true
	return !was
}

// guardNow is how long the local guard waits for the next part, and false when nothing guards
// it: twice the hub's bound plus 1 s for a stream's first part, the bound plus 1 s between its
// parts and for a subscription's initial part, nothing after that (a quiet subscription is not a
// broken one).
// TODO(spec-gap): SG-3 (the local guards of a stream and of a subscription).
func (s *streaming) guardNow() (time.Duration, bool) {
	switch {
	case s.what == "subscription":
		return s.guard, s.first
	case s.first:
		return firstPartGuard(s.guard), true
	}
	return s.guard, true
}

// dropped cancels the stream of a caller whose ctx ended and is its error: ctx's error, with
// ErrOutcomeUnknown once the request was written, else with ErrNeverSent.
func (s *streaming) dropped(ctx context.Context) error {
	if !s.cancel(s.callerGone()) {
		return fmt.Errorf("%w: %w", ErrNeverSent, ctx.Err())
	}
	return fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
}

// handle is what event e makes of the stream: its next part, its clean end (nil, nil) or its
// failure.
func (s *streaming) handle(e streamEvent) (*hubpb.ReplyPart, error) {
	switch {
	case e.part != nil:
		// The hub drops a part it cannot buffer in time and counts it anyway: parts are numbered
		// 1, 2, 3, ... in arrival order, so a gap is that many lost parts.
		expected := s.received + 1
		switch seq := e.part.GetSeq(); {
		case seq > expected:
			s.cancel(fmt.Sprintf("%s %s lost %d part(s)", s.what, s.typ, seq-expected))
			return nil, s.lost(seq - expected)
		case seq < expected:
			reason := fmt.Sprintf("%s %s part seq %d out of order (expected %d)", s.what, s.typ, seq, expected)
			s.cancel(reason)
			return nil, errors.New("snodra: " + reason)
		}
		s.received, s.first = expected, false
		return e.part, nil
	case e.end != nil:
		s.finish()
		if werr := e.end.GetError(); werr != nil {
			return nil, s.fromWire(werr)
		}
		// ReplyEnd.parts counts what the hub sent: a clean end with more lost its last parts.
		if n := uint64(e.end.GetParts()); n > s.received {
			return nil, s.lost(n - s.received)
		}
		return nil, nil
	default:
		s.finish()
		return nil, s.fromWire(e.failure)
	}
}

// fromWire is the caller's error for the hub's or the handler's werr ending the stream.
func (s *streaming) fromWire(werr *hubpb.Error) *Error {
	if s.what == "subscription" {
		return subscriptionErrorFromWire(s.typ, werr)
	}
	return errorFromWire(s.typ, werr)
}

// The reasons of the Cancel a sender writes.

// callerGone is the reason when the caller stopped reading or its context ended.
func (s *streaming) callerGone() string {
	if s.what == "subscription" {
		return "subscriber cancelled"
	}
	return "collector cancelled"
}

// guardFired is the reason when the local guard fired.
func (s *streaming) guardFired() string {
	if s.what == "subscription" {
		return "no initial result within the local guard"
	}
	return "no part within the local guard"
}

// collectorFailed is the reason when what was read could not be used.
func collectorFailed(err error) string { return "collector failed: " + err.Error() }

// lost is the error of n parts the hub sent (or counted) that never arrived.
func (s *streaming) lost(n uint64) error {
	return fmt.Errorf("%w: %s %s lost %d part(s)", ErrLostParts, s.what, s.typ, n)
}

// consumed counts an item the caller read: every refill items, the hub gets a Flow for that many.
func (s *streaming) consumed() {
	if s.isDone() {
		return // closed meanwhile: no credit for a subscription the hub was told to cancel
	}
	s.read++
	if s.read < s.refill {
		return
	}
	s.read -= s.refill
	flow := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Flow{Flow: &hubpb.Flow{RequestId: s.c.id, Credits: s.refill}}}
	if err := s.ref.send(flow); err != nil {
		s.app.log.Debug("flow not written: its hub stream is gone", "request", s.c.id, "type", s.typ)
	}
}

// finish ends the stream here: nothing more is read or cancelled.
func (s *streaming) finish() {
	s.end()
	s.app.calls.remove(s.c.id)
}

// cancel ends the stream here and cancels it at the hub with reason (the handler is told),
// unless it ended already: the hub gets at most one Cancel, whichever goroutine calls first. A
// request still in the outbound buffer is withdrawn instead. It reports whether the request had
// been written.
func (s *streaming) cancel(reason string) bool {
	_, written := s.cancelOnce(reason)
	return written
}

// cancelOnce is cancel, also reporting whether this call ended the stream.
func (s *streaming) cancelOnce(reason string) (ended, written bool) {
	s.mu.Lock()
	was := s.done
	s.done = true
	c, ref := s.c, s.ref
	s.mu.Unlock()
	if was {
		return false, true
	}
	s.app.calls.remove(c.id)
	if ref.withdraw(c) {
		return true, false
	}
	msg := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Cancel{Cancel: &hubpb.Cancel{RequestId: c.id, Reason: reason}}}
	if err := ref.send(msg); err != nil {
		s.app.log.Debug("cancel not written: its hub stream is gone", "request", c.id, "type", s.typ)
	}
	return true, true
}
