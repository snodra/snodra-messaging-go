package snodra

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// Subscription is an open subscription query ([Subscribe]): its initial result, and the updates
// that follow until every owner completed, the hub ended it or it is closed.
type Subscription[I, U any] struct {
	// Initial is the initial result, answered by one instance of an owner.
	Initial I

	ctx context.Context
	s   *streaming

	mu       sync.Mutex
	ranging  bool          // a range over Updates is under way: it owns s
	isClosed bool          // Close was called
	closed   chan struct{} // closed by Close
	closeErr error         // what a later range yields: the context's error when it closed the subscription
	stop     func() bool   // stops the watch on ctx
}

// Subscribe subscribes to the subscription query q ([HandleSubscription] on the other side):
// one Request in mode SUBSCRIPTION to one hub, unkeyed. The hub subscribes every live instance
// of every owner of q's type, as they are now (an instance that registers later is not added,
// one that leaves is not replaced: the updates arrive at most once, and neither the hub nor the
// library subscribes again), and asks one of them for the initial result.
//
//	sub, err := snodra.Subscribe[OrderView, OrderUpdate](ctx, app, WatchOrders{ID: "o-42"})
//	if err != nil {
//		return err
//	}
//	defer sub.Close()
//	render(sub.Initial)
//	for u, err := range sub.Updates() {
//		if err != nil {
//			return err // the last thing the sequence yields
//		}
//		...
//	}
//
// Subscribe returns once the initial result arrived, so its errors are those of [Query]: an
// [*Error] (NO_HANDLER without an owner, INVALID_REQUEST for a type served in another mode,
// TIMEOUT when the initial result is not in time, the handler's error, RESOURCE_EXHAUSTED, ...),
// [ErrNeverSent], [ErrOutcomeUnknown], the context's error, or a local failure (an initial result
// that does not decode, a hub that ended the subscription without one). The retry policy applies
// until then, as for a query: a retry is a new subscription with a fresh id, sending what is
// left of the call's time budget.
//
// The updates are hot: the hub relays them from the open on, up to the credit window
// ([InitialCredit], the initial result taking one credit), and buffers up to its own limit beyond
// it, ending the subscription with RESOURCE_EXHAUSTED when the caller falls further behind;
// reading them gives credit back. ctx bounds the whole subscription: when it ends, the
// subscription is cancelled at the hub, ranging or not, and [Subscription.Updates] yields ctx's
// error (with [ErrOutcomeUnknown]).
//
// The options are those of [Query]: [Timeout] (or [DefaultTimeout], or ctx's deadline) bounds the
// initial result only (the local guard waits the hub's bound plus 1 s for it; the updates have no
// timeout and no guard); [InitialCredit]; [MaxResults] ends the subscription after that many
// updates. [RoutingKey] is ignored.
// See https://snodra.damoreira.com/library-authors/protocol/ (Subscription).
func Subscribe[I, U any](ctx context.Context, app *App, q Message, opts ...SendOption) (*Subscription[I, U], error) {
	out, err := prepare(ctx, app, hubpb.RequestKind_QUERY, hubpb.RequestMode_SUBSCRIPTION, q, opts)
	if err != nil {
		return nil, err
	}
	var s *streaming
	// Retried until the initial part, each attempt sending what is left of the budget (as a
	// unary query, unlike a stream).
	first, err := retrying(ctx, hubpb.RequestKind_QUERY, out.policy, out.budget, out.chosen, func(timeout time.Duration) (*hubpb.ReplyPart, error) {
		opened, err := app.openStream(ctx, out.tmpl, timeout)
		if err != nil {
			return nil, err
		}
		s = opened
		part, err := s.next(ctx)
		if err == nil && part == nil {
			// The hub never ends a subscription before its initial part.
			return nil, fmt.Errorf("snodra: subscription %s ended without an initial result", out.typ)
		}
		return part, err
	})
	if err != nil {
		return nil, err
	}
	initial, err := decodeAnswer[I](app.cfg.codec, first.GetPayload())
	if err != nil {
		s.cancel("undecodable part")
		return nil, fmt.Errorf("snodra: cannot decode the initial result of %s from %s/%s: %w",
			out.typ, first.GetResponderService(), first.GetResponderInstance(), err)
	}
	s.consumed() // the initial part took one credit
	sub := &Subscription[I, U]{Initial: initial, ctx: ctx, s: s, closed: make(chan struct{})}
	s.closed = sub.closed
	sub.mu.Lock()
	sub.stop = context.AfterFunc(ctx, sub.Close)
	sub.mu.Unlock()
	return sub, nil
}

// Updates is the sequence of the subscription's updates, decoded as U, in the hub's arrival
// order (from every owner instance), until every owner completed (or [MaxResults] updates came).
// It can be ranged over once; a later range yields nothing more.
//
// An error is the last thing the sequence yields: the hub's end with an error (an [*Error] by its
// code: an owner's failure, RESOURCE_EXHAUSTED when the hub's buffer for the caller overflowed,
// HANDLER_DISCONNECTED when the last owner left, CANCELLED at the hub's maximum duration, ...),
// [ErrOutcomeUnknown] when the hub stream was lost, [ErrLostParts] when the hub dropped updates,
// ctx's error (with [ErrOutcomeUnknown]), or an update that does not decode. Breaking out of the
// loop cancels the subscription at the hub, as [Subscription.Close] does; a Close during the
// range ends it without an error.
func (sub *Subscription[I, U]) Updates() iter.Seq2[U, error] {
	return func(yield func(U, error) bool) {
		var zero U
		if sub == nil || sub.s == nil {
			return
		}
		sub.mu.Lock()
		switch {
		case sub.ranging:
			sub.mu.Unlock()
			yield(zero, errors.New("snodra: the updates of a subscription are already being ranged over"))
			return
		case sub.isClosed:
			err := sub.closeErr
			sub.mu.Unlock()
			if err != nil {
				yield(zero, err)
			}
			return
		case sub.s.done:
			sub.mu.Unlock()
			return
		}
		sub.ranging = true
		sub.mu.Unlock()
		defer func() {
			sub.mu.Lock()
			sub.ranging = false
			sub.s.cancel("dropped by the sender") // a break, a panic in the loop body or a Close
			if sub.stop != nil {
				sub.stop() // over: ctx has nothing more to end
			}
			sub.mu.Unlock()
		}()
		for {
			part, err := sub.s.next(sub.ctx)
			switch {
			case errors.Is(err, errClosed):
				return
			case err != nil:
				yield(zero, err)
				return
			case part == nil:
				return
			}
			v, err := decodeAnswer[U](sub.s.app.cfg.codec, part.GetPayload())
			if err != nil {
				sub.s.cancel("undecodable part")
				yield(zero, fmt.Errorf("snodra: cannot decode an update of %s from %s/%s: %w",
					sub.s.typ, part.GetResponderService(), part.GetResponderInstance(), err))
				return
			}
			sub.s.consumed()
			if !yield(v, nil) {
				return
			}
		}
	}
}

// Close cancels the subscription at the hub, which tells every owner instance, unless it ended
// already. It is safe to call more than once, and while another goroutine ranges over
// [Subscription.Updates], whose range then ends.
func (sub *Subscription[I, U]) Close() {
	if sub == nil || sub.s == nil {
		return
	}
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.isClosed {
		return
	}
	sub.isClosed = true
	if err := sub.ctx.Err(); err != nil && !sub.s.done {
		sub.closeErr = fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
	}
	close(sub.closed)
	if sub.stop != nil {
		sub.stop()
	}
	if !sub.ranging {
		sub.s.cancel("dropped by the sender")
	}
}
