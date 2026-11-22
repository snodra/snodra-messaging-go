package snodra

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// InitialCredit is a [Stream]'s credit window (Request.initial_credit): the items the handler
// may send before the first refill. The stream gives back half of it every time half of it was
// read, so the handler never runs more than a window ahead of the caller. 0, the default, reads
// as 64. The other calls ignore it.
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
// See https://snodra.damoreira.com/library-authors/protocol/ (Streaming).
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
				s.cancel("dropped by the sender")
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
				s.cancel("undecodable part")
				yield(zero, fmt.Errorf("snodra: cannot decode a part of %s from %s/%s: %w",
					out.typ, part.GetResponderService(), part.GetResponderInstance(), err))
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

// streaming is one streaming query written (or being written) to a hub stream: its parts as they
// arrive, checked, then its end. Flow and Cancel go to the stream it was written to.
type streaming struct {
	app      *App
	typ      string
	c        *call
	ref      *streamRef
	guard    time.Duration // the idle guard: the hub's bound plus 1 s
	refill   uint32        // the credit given back every time that many items were read
	first    bool          // no part arrived yet
	received uint64        // parts received: the next must carry seq received+1
	read     uint32        // items read since the last refill
	done     bool          // ended: nothing more to read, nothing to cancel
}

// openStream writes a new streaming query of tmpl (fresh id, idle timeout chosen) to a hub.
func (a *App) openStream(ctx context.Context, tmpl *hubpb.Request, chosen time.Duration) (*streaming, error) {
	c := &call{id: uuid.New(), inbox: newStreamInbox()}
	ref, guard, err := a.write(ctx, tmpl, chosen, c)
	if err != nil {
		return nil, err
	}
	return &streaming{app: a, typ: tmpl.GetType(), c: c, ref: ref, guard: guard,
		refill: max(1, tmpl.GetInitialCredit()/2), first: true}, nil
}

// firstPartGuard is the local guard of a stream's first part, from its idle guard: twice the
// hub's bound plus 1 s (the hub bounds a queue wait by the timeout, then starts its idle clock
// at the forward).
// TODO(spec-gap): SG-3 (the local guards of a stream).
func firstPartGuard(idle time.Duration) time.Duration {
	return idle + max(idle-guardMargin, 0)
}

// next is the stream's next part; nil at its clean end. An error ends the stream.
func (s *streaming) next(ctx context.Context) (*hubpb.ReplyPart, error) {
	if s.done {
		return nil, nil
	}
	wait := s.guard
	if s.first {
		wait = firstPartGuard(s.guard)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
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
			s.finish()
			if s.ref.withdraw(s.c) {
				return nil, fmt.Errorf("%w: the hub stream closed before %s was written", ErrNeverSent, s.typ)
			}
			return nil, fmt.Errorf("%w: the hub stream closed during the stream of %s", ErrOutcomeUnknown, s.typ)
		case <-timer.C:
			// The hub ends an idle stream itself; the guard covers a hub that never does (told to
			// cancel, so that the handler stops holding its permit).
			s.cancel("local guard timeout")
			return nil, &Error{Code: CodeTimeout, Message: timeoutText(s.typ)}
		case <-ctx.Done():
			if !s.cancel("dropped by the sender") {
				return nil, fmt.Errorf("%w: %w", ErrNeverSent, ctx.Err())
			}
			return nil, fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
		}
	}
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
			s.cancel("lost parts")
			return nil, s.lost(seq - expected)
		case seq < expected:
			s.cancel("part out of order")
			return nil, fmt.Errorf("snodra: stream %s part %d out of order (expected %d)", s.typ, seq, expected)
		}
		s.received, s.first = expected, false
		return e.part, nil
	case e.end != nil:
		s.finish()
		if werr := e.end.GetError(); werr != nil {
			return nil, errorFromWire(s.typ, werr)
		}
		// ReplyEnd.parts counts what the hub sent: a clean end with more lost its last parts.
		if n := uint64(e.end.GetParts()); n > s.received {
			return nil, s.lost(n - s.received)
		}
		return nil, nil
	default:
		s.finish()
		return nil, errorFromWire(s.typ, e.failure)
	}
}

// lost is the error of n parts the hub sent (or counted) that never arrived, with the reference
// library's text.
func (s *streaming) lost(n uint64) error {
	return fmt.Errorf("%w: stream %s lost %d part(s)", ErrLostParts, s.typ, n)
}

// consumed counts an item the caller read: every refill items, the hub gets a Flow for that many.
func (s *streaming) consumed() {
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
	s.done = true
	s.app.calls.remove(s.c.id)
}

// cancel ends the stream here and cancels it at the hub with reason (the handler is told),
// unless it ended already; a request still in the outbound buffer is withdrawn instead. It
// reports whether the request had been written.
func (s *streaming) cancel(reason string) bool {
	if s.done {
		return true
	}
	s.finish()
	if s.ref.withdraw(s.c) {
		return false
	}
	msg := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Cancel{Cancel: &hubpb.Cancel{RequestId: s.c.id, Reason: reason}}}
	if err := s.ref.send(msg); err != nil {
		s.app.log.Debug("cancel not written: its hub stream is gone", "request", s.c.id, "type", s.typ)
	}
	return true
}
