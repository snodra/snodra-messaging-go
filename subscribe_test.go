package snodra

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// update is the ReplyPart seq of subscription id carrying {"value": seq}.
func update(id string, seq uint64) *hubpb.ServerMessage {
	return partOf(id, seq, "B", "b1", `{"value":`+strconv.FormatUint(seq, 10)+`}`, nil)
}

// subscribing subscribes to WatchOrders on topic t in the background.
func subscribing(app *App, ctx context.Context, opts ...SendOption) <-chan outcome[*Subscription[OrderView, OrderUpdate]] {
	return async(func() (*Subscription[OrderView, OrderUpdate], error) {
		return Subscribe[OrderView, OrderUpdate](ctx, app, WatchOrders{Topic: "t"}, opts...)
	})
}

// opened is a subscription whose request was sent and whose initial part (seq 1) arrived.
func opened(t *testing.T, app *App, s *fakehub.Stream, opts ...SendOption) (*Subscription[OrderView, OrderUpdate], *hubpb.Request) {
	t.Helper()
	sub := subscribing(app, context.Background(), opts...)
	r := s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 1, "B", "b1", `{"topic":"t","value":0}`, nil))
	o := await(t, sub)
	is(t, o.err, nil)
	return o.value, r
}

func TestSubscribeReturnsTheInitialThenTheUpdates(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sub := subscribing(app, context.Background(), InitialCredit(2), RoutingKey("ignored"), MaxResults(5))
	r := s.RecvRequest(t)
	is(t, r.GetMode(), hubpb.RequestMode_SUBSCRIPTION)
	is(t, r.GetKind(), hubpb.RequestKind_QUERY)
	is(t, r.GetType(), "orders.WatchOrders")
	is(t, r.GetInitialCredit(), uint32(2))
	is(t, r.GetMaxResults(), uint32(5))
	is(t, r.GetRoutingKey(), "") // unkeyed, as in the reference library
	is(t, r.GetTimeoutMs(), uint32(0))
	is(t, string(r.GetPayload()), `{"topic":"t"}`)
	id := r.GetId()
	// A hub keeping to the credit: the initial part and one update, then one part per Flow.
	s.Send(t, partOf(id, 1, "B", "b1", `{"topic":"t","value":0}`, nil))
	s.Send(t, update(id, 2))
	o := await(t, sub)
	is(t, o.err, nil)
	is(t, o.value.Initial, OrderView{Topic: "t"})
	f := s.RecvFlow(t) // the initial part counts as consumed
	is(t, f.GetRequestId(), id)
	is(t, f.GetCredits(), uint32(1))
	s.Send(t, update(id, 3))
	read := async(func() ([]OrderUpdate, error) { return collect(o.value.Updates()) })
	for seq := uint64(4); seq <= 6; seq++ {
		is(t, s.RecvFlow(t).GetCredits(), uint32(1))
		s.Send(t, update(id, seq))
	}
	is(t, s.RecvFlow(t).GetCredits(), uint32(1))
	s.Send(t, streamEnd(id, 6, nil))
	got := await(t, read)
	is(t, got.err, nil)
	is(t, len(got.value), 5)
	for i, u := range got.value {
		is(t, u.Value, i+2)
	}
	is(t, s.RecvFlow(t).GetCredits(), uint32(1)) // the last update consumed
	s.Nothing(t, 200*ms, "cancel")               // an ended subscription is not cancelled
	o.value.Close()
	s.Nothing(t, 200*ms, "cancel")
	is(t, waiting(app), 0)
}

func TestSubscribeDetectsLostParts(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sub, r := opened(t, app, s, InitialCredit(10))
	s.Send(t, update(r.GetId(), 2))
	s.Send(t, update(r.GetId(), 4)) // 3 was dropped by the hub
	got, err := collect(sub.Updates())
	is(t, len(got), 1)
	is(t, errors.Is(err, ErrLostParts), true)
	is(t, err.Error(), "snodra: parts of the answer were lost: subscription orders.WatchOrders lost 1 part(s)")
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "lost parts")

	sub, r = opened(t, app, s, InitialCredit(10))
	s.Send(t, update(r.GetId(), 2))
	s.Send(t, streamEnd(r.GetId(), 4, nil)) // the hub counted two parts that never came
	got, err = collect(sub.Updates())
	is(t, len(got), 1)
	is(t, errors.Is(err, ErrLostParts), true)
	s.Nothing(t, 200*ms, "cancel") // it ended: nothing to cancel
	is(t, waiting(app), 0)
}

func TestAFailedInitialResultFailsSubscribe(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sub := subscribing(app, context.Background())
	r := s.RecvRequest(t)
	s.Send(t, streamEnd(r.GetId(), 0, &hubpb.Error{RequestId: r.GetId(), Code: hubpb.ErrorCode_NO_HANDLER, Message: "no owner"}))
	o := await(t, sub)
	var e *Error
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeNoHandler)
	is(t, e.Message, "No handler registered for 'orders.WatchOrders'")

	sub = subscribing(app, context.Background())
	r = s.RecvRequest(t)
	s.Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_INVALID_REQUEST, "type orders.WatchOrders is a command"))
	o = await(t, sub)
	is(t, errors.As(o.err, &e), true)
	is(t, e.Code, CodeInvalidRequest)

	sub = subscribing(app, context.Background())
	r = s.RecvRequest(t)
	s.Send(t, streamEnd(r.GetId(), 0, nil)) // the hub never ends a subscription before its initial part
	o = await(t, sub)
	is(t, o.err != nil, true)
	is(t, o.err.Error(), "snodra: subscription orders.WatchOrders ended without an initial result")

	sub = subscribing(app, context.Background())
	r = s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 1, "B", "b1", `{"value":"zero"}`, nil)) // does not decode
	o = await(t, sub)
	is(t, o.err != nil, true)
	is(t, errors.As(o.err, &e), false) // a local failure
	is(t, s.RecvCancel(t).GetReason(), "undecodable part")
	is(t, waiting(app), 0)
}

func TestCloseWithoutRangingCancels(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	before := runtime.NumGoroutine()
	sub, r := opened(t, app, s)
	sub.Close()
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "dropped by the sender") // the reference library's reason
	sub.Close()                                   // again: nothing more
	got, err := collect(sub.Updates())            // ranged after Close: nothing
	is(t, len(got), 0)
	is(t, err, nil)
	s.Send(t, update(r.GetId(), 2)) // late frames find nothing
	s.Nothing(t, 200*ms, "cancel", "flow")
	is(t, waiting(app), 0)
	poll(t, func() bool { return runtime.NumGoroutine() <= before })
}

func TestBreakingTheUpdatesLoopCancels(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sub, r := opened(t, app, s)
	for seq := uint64(2); seq <= 4; seq++ {
		s.Send(t, update(r.GetId(), seq))
	}
	var got []OrderUpdate
	for u, err := range sub.Updates() {
		is(t, err, nil)
		got = append(got, u)
		if len(got) == 2 {
			break
		}
	}
	is(t, len(got), 2)
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "dropped by the sender")
	again, err := collect(sub.Updates()) // over: a later range yields nothing
	is(t, len(again), 0)
	is(t, err, nil)
	is(t, waiting(app), 0)
}

func TestCloseEndsARangeInProgress(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sub, r := opened(t, app, s)
	items := make(chan OrderUpdate, 4)
	read := async(func() (int, error) {
		n := 0
		for u, err := range sub.Updates() {
			if err != nil {
				return n, err
			}
			n++
			items <- u
		}
		return n, nil
	})
	s.Send(t, update(r.GetId(), 2))
	is(t, receive(t, items).Value, 2) // the range waits for the next update
	sub.Close()
	o := await(t, read)
	is(t, o.err, nil) // closed by its owner: a clean end
	is(t, o.value, 1)
	is(t, s.RecvCancel(t).GetRequestId(), r.GetId())
	is(t, waiting(app), 0)
}

func TestSubscribeEndsWithItsContext(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := subscribing(app, ctx)
	r := s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 1, "B", "b1", `{"topic":"t","value":0}`, nil))
	o := await(t, sub)
	is(t, o.err, nil)
	cancel() // never ranged: the subscription is cancelled all the same
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "dropped by the sender")
	_, err := collect(o.value.Updates())
	is(t, errors.Is(err, context.Canceled), true)
	is(t, errors.Is(err, ErrOutcomeUnknown), true)

	// Before the initial result: Subscribe returns the context's error.
	ctx, cancel = context.WithCancel(context.Background())
	sub = subscribing(app, ctx)
	r = s.RecvRequest(t)
	cancel()
	o = await(t, sub)
	is(t, errors.Is(o.err, context.Canceled), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	is(t, s.RecvCancel(t).GetRequestId(), r.GetId())
	is(t, waiting(app), 0)
}

func TestTheSubscriptionsLocalGuardCoversItsInitialResult(t *testing.T) { // SG-3
	app, _, s := sending(t, &hubpb.RegisterAck{HubId: "hub-1", HeartbeatIntervalMs: 3000, HeartbeatTimeoutMs: 10000,
		DefaultTimeoutMs: 200, MaxTimeoutMs: 200})
	sub := subscribing(app, context.Background())
	r := s.RecvRequest(t)
	o := await(t, sub)
	var e *Error
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeTimeout)
	within(t, o.elapsed, 1150*ms, 2000*ms) // the hub's bound (200 ms) plus 1 s
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "local guard timeout")

	// After it, a quiet subscription is not a broken one: no guard.
	opened2, r2 := opened(t, app, s)
	read := async(func() ([]OrderUpdate, error) { return collect(opened2.Updates()) })
	s.Nothing(t, 1600*ms, "cancel")
	s.Send(t, update(r2.GetId(), 2))
	s.Send(t, streamEnd(r2.GetId(), 2, nil))
	got := await(t, read)
	is(t, got.err, nil)
	is(t, len(got.value), 1)
}

func TestSubscribeIsRetriedUntilTheInitialResult(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	p := RetryNeverRan
	p.InitialBackoff = 10 * ms
	sub := subscribing(app, context.Background(), Retry(p), Timeout(2*time.Second))
	first := s.RecvRequest(t)
	is(t, first.GetTimeoutMs(), uint32(2000))
	s.Send(t, fakehub.HubError(first.GetId(), hubpb.ErrorCode_NO_HANDLER, "no live instance"))
	second := s.RecvRequest(t)
	is(t, second.GetId() != first.GetId(), true)
	// What is left of the budget, as for a unary request (unlike a stream, SG-21).
	if ms := second.GetTimeoutMs(); ms == 0 || ms >= 2000 {
		t.Fatalf("the retry sent timeout_ms %d, want what is left of 2000", ms)
	}
	s.Send(t, partOf(second.GetId(), 1, "B", "b1", `{"topic":"t","value":0}`, nil))
	o := await(t, sub)
	is(t, o.err, nil)
	s.Send(t, streamEnd(second.GetId(), 1, &hubpb.Error{RequestId: second.GetId(), Code: hubpb.ErrorCode_HANDLER_DISCONNECTED}))
	_, err := collect(o.value.Updates()) // after the initial result nothing is retried
	var e *Error
	is(t, errors.As(err, &e), true)
	is(t, e.Code, CodeHandlerDisconnected)
	s.Nothing(t, 200*ms, "request")
}

func TestSubscribeRefusesWhatItCannotSend(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	_, err := Subscribe[OrderView, OrderUpdate](context.Background(), app, nil)
	is(t, err != nil, true)
	_, err = Subscribe[OrderView, OrderUpdate](context.Background(), app, WatchOrders{}, Header("authorization", "x"))
	is(t, errors.Is(err, ErrReservedHeader), true)
	_, err = Subscribe[OrderView, OrderUpdate](context.Background(), nil, WatchOrders{})
	is(t, err != nil, true)
	s.Nothing(t, 200*ms, "request")
	var nilSub *Subscription[OrderView, OrderUpdate]
	nilSub.Close() // harmless
}
