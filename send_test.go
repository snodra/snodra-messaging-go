package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// sending starts an app without handlers on a fake hub and returns its first stream,
// acknowledged with ack.
func sending(t *testing.T, ack *hubpb.RegisterAck, opts ...Option) (*App, *fakehub.Hub, *fakehub.Stream) {
	t.Helper()
	hub := fakehub.Start(t)
	app := startApp(t, append([]Option{Hubs(hub.Addr())}, opts...)...)
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Recv(t) // the heartbeat after the Register, before the ack (see fakehub.Stream.Ack)
	s.Ack(t, ack)
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	return app, hub, s
}

// outcome is what a call returned, and when.
type outcome[R any] struct {
	value   R
	err     error
	elapsed time.Duration
}

// async runs call on its own goroutine.
func async[R any](call func() (R, error)) <-chan outcome[R] {
	ch := make(chan outcome[R], 1)
	start := time.Now()
	go func() {
		v, err := call()
		ch <- outcome[R]{v, err, time.Since(start)}
	}()
	return ch
}

// await waits at most 5 s for the call's outcome.
func await[R any](t *testing.T, ch <-chan outcome[R]) outcome[R] {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(5 * time.Second):
		t.Fatal("the call did not return within 5 s")
		return outcome[R]{}
	}
}

// replyTo answers request id with payload.
func replyTo(id, payload string) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Reply{Reply: &hubpb.Reply{
		RequestId: id, Result: &hubpb.Reply_Payload{Payload: []byte(payload)},
	}}}
}

// failWith answers request id with a handler's error.
func failWith(id string, code hubpb.ErrorCode, msg, details string) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Reply{Reply: &hubpb.Reply{
		RequestId: id, Result: &hubpb.Reply_Error{Error: &hubpb.Error{
			RequestId: id, Code: code, Message: msg, Details: []byte(details),
		}},
	}}}
}

// waiting is how many requests the app waits for.
func waiting(app *App) int {
	app.calls.mu.Lock()
	defer app.calls.mu.Unlock()
	return len(app.calls.byID)
}

func TestRequestTiming(t *testing.T) {
	ack := &hubpb.RegisterAck{MaxTimeoutMs: 2000, DefaultTimeoutMs: 5000, TypeTimeoutsMs: map[string]uint32{"orders.GetOrder": 1500}}
	ms_, g := requestTiming(ack, "orders.PlaceOrder", 0)
	is(t, ms_, uint32(0))
	is(t, g, 3000*time.Millisecond)
	ms_, g = requestTiming(ack, "orders.GetOrder", 0)
	is(t, ms_, uint32(0))
	is(t, g, 2500*time.Millisecond)
	ms_, g = requestTiming(ack, "orders.PlaceOrder", 1200*time.Millisecond)
	is(t, ms_, uint32(1200))
	is(t, g, 2200*time.Millisecond)
	ms_, g = requestTiming(ack, "orders.GetOrder", 1800*time.Millisecond)
	is(t, ms_, uint32(1800))
	is(t, g, 2500*time.Millisecond)
	ms_, _ = requestTiming(ack, "orders.PlaceOrder", 400*time.Microsecond)
	is(t, ms_, uint32(1)) // never 0 when chosen
	_, g = requestTiming(&hubpb.RegisterAck{}, "x.Y", 0)
	is(t, g, 61*time.Second) // no max: 60 s fallback
	ack.TypeTimeoutsMs["orders.Zero"] = 0
	_, g = requestTiming(ack, "orders.Zero", 0)
	is(t, g, 3000*time.Millisecond) // a zero override counts as none
}

func TestSendWritesTheRequestAndDecodesTheReply(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx := context.Background() // no deadline: no caller timeout (await bounds the wait)
	sent := async(func() (Receipt, error) { return Send[Receipt](ctx, app, PlaceOrder{OrderID: "o-1"}, RoutingKey("o-1")) })
	r := s.RecvRequest(t)
	is(t, r.GetKind(), hubpb.RequestKind_COMMAND)
	is(t, r.GetType(), "orders.PlaceOrder")
	is(t, r.GetRoutingKey(), "o-1")
	is(t, r.GetTimeoutMs(), uint32(0))
	is(t, r.GetMode(), hubpb.RequestMode_UNARY)
	is(t, r.GetPriority(), int32(0))
	is(t, r.GetId() != "", true)
	is(t, string(r.GetPayload()), `{"orderId":"o-1"}`)
	s.Send(t, replyTo(r.GetId(), `{"orderId":"o-1","total":42}`))
	o := await(t, sent)
	is(t, o.err, nil)
	is(t, o.value, Receipt{OrderID: "o-1", Total: 42})

	queried := async(func() (Receipt, error) { return Query[Receipt](ctx, app, GetOrder{OrderID: "o-1"}) })
	r = s.RecvRequest(t)
	is(t, r.GetKind(), hubpb.RequestKind_QUERY)
	is(t, r.GetType(), "orders.GetOrder")
	s.Send(t, replyTo(r.GetId(), `{"orderId":"o-1","total":7}`))
	is(t, await(t, queried).value.Total, 7)

	// A command without a result: Empty, also for a Reply without a result.
	empty := async(func() (Empty, error) { return Send[Empty](ctx, app, &CancelOrder{ID: "o-1"}) })
	r = s.RecvRequest(t)
	is(t, r.GetType(), "orders.CancelOrder")
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Reply{Reply: &hubpb.Reply{RequestId: r.GetId()}}})
	is(t, await(t, empty).err, nil)
	is(t, waiting(app), 0)
}

func TestPriorityPerCallElsePerType(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"), Priorities(map[string]int32{"orders.PlaceOrder": 5}))
	ctx := ctxWithin(t, 5*time.Second)
	for _, c := range []struct {
		opts []SendOption
		want int32
	}{{nil, 5}, {[]SendOption{Priority(9)}, 9}, {[]SendOption{Priority(0)}, 0}} {
		sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}, c.opts...) })
		r := s.RecvRequest(t)
		is(t, r.GetPriority(), c.want)
		s.Send(t, replyTo(r.GetId(), `{}`))
		is(t, await(t, sent).err, nil)
	}
	sent := async(func() (Empty, error) { return Query[Empty](ctx, app, GetOrder{}) })
	r := s.RecvRequest(t)
	is(t, r.GetPriority(), int32(0)) // no default for that type
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

func TestEveryAttemptHasAFreshID(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx := ctxWithin(t, 10*time.Second)
	ids := map[string]bool{}
	var calls []<-chan outcome[Empty]
	for range 10 {
		calls = append(calls, async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) }))
	}
	var held []string
	for range 10 { // ten pending at once
		held = append(held, s.RecvRequest(t).GetId())
	}
	is(t, waiting(app), 10)
	for _, id := range held {
		ids[id] = true
		s.Send(t, replyTo(id, `{}`))
	}
	for _, c := range calls {
		is(t, await(t, c).err, nil)
	}
	for range 5 {
		sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
		id := s.RecvRequest(t).GetId()
		ids[id] = true
		s.Send(t, replyTo(id, `{}`))
		is(t, await(t, sent).err, nil)
	}
	is(t, len(ids), 15)
	is(t, ids[""], false)
}

func TestTheLocalGuardWaitsTheHubsBoundPlusOneSecond(t *testing.T) {
	ack := fakehub.Ack("hub-1")
	ack.MaxTimeoutMs = 200
	app, _, s := sending(t, ack)
	sent := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	id := s.RecvRequest(t).GetId() // and the fake stays silent
	o := await(t, sent)
	var e *Error
	is(t, errors.As(o.err, &e), true)
	is(t, e.Code, CodeTimeout)
	is(t, e.Message, "Timed out waiting for reply to 'orders.PlaceOrder'")
	within(t, o.elapsed, 1050*ms, 1400*ms)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), false) // a TIMEOUT, as the hub's own
	is(t, waiting(app), 0)
	s.Send(t, replyTo(id, `{}`)) // late: dropped
	again := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{}`))
	is(t, await(t, again).err, nil)
}

func TestNoAcknowledgedStreamIsNeverSent(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	_, err := Send[Empty](context.Background(), app, PlaceOrder{}) // before Start
	is(t, errors.Is(err, ErrNeverSent), true)
	is(t, app.Start(context.Background()), nil)
	s := hub.Next(t)
	s.RecvRegister(t)
	s.EchoHeartbeats(true) // echoes, but no ack
	start := time.Now()
	_, err = Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}, Timeout(2*time.Second))
	is(t, errors.Is(err, ErrNeverSent), true)
	is(t, errors.Is(fmt.Errorf("place order: %w", err), ErrNeverSent), true) // through a wrapper
	is(t, errors.Is(err, ErrOutcomeUnknown), false)
	within(t, time.Since(start), 0, 100*ms) // at once: it waits for no ack
	s.Nothing(t, 200*ms, "request")
	is(t, s.Recv(t).GetHeartbeat() != nil, true) // the stream is alive: nothing was written to it
}

// blockRequests is a write hook holding the writer before it takes a Request, until release
// is closed or the stream ends; held gets the request.
func blockRequests(held chan<- *hubpb.Request, release <-chan struct{}) Option {
	return withWriteHook(func(ctx context.Context, m *hubpb.ClientMessage) {
		if r := m.GetRequest(); r != nil {
			held <- r
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
}

func TestBufferedRequestIsNeverSentWhenTheStreamBreaks(t *testing.T) {
	held := make(chan *hubpb.Request, 1)
	app, hub, s := sending(t, fakehub.Ack("hub-1"), blockRequests(held, nil))
	sent := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	receive(t, held) // in the outbound buffer, not taken by the writer
	s.Close(codes.Unavailable, "restart")
	o := await(t, sent)
	is(t, errors.Is(o.err, ErrNeverSent), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), false)
	is(t, waiting(app), 0)
	hub.Next(t).RecvRegister(t) // the next stream does not carry it either
}

func TestAWrittenRequestIsOutcomeUnknownWhenTheStreamBreaks(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sent := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	s.RecvRequest(t)
	s.Close(codes.Unavailable, "restart")
	o := await(t, sent)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	is(t, errors.Is(fmt.Errorf("place order: %w", o.err), ErrOutcomeUnknown), true) // through a wrapper
	is(t, errors.Is(o.err, ErrNeverSent), false)
	is(t, waiting(app), 0)
}

func TestEveryErrorCodeReachesTheCaller(t *testing.T) { // SG-17 for the messages
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx := ctxWithin(t, 10*time.Second)
	call := func(answer func(id string) *hubpb.ServerMessage) *Error {
		t.Helper()
		sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
		s.Send(t, answer(s.RecvRequest(t).GetId()))
		err := await(t, sent).err
		var e *Error
		if !errors.As(fmt.Errorf("place order: %w", err), &e) { // through a wrapper
			t.Fatalf("got %v, want an *Error", err)
		}
		return e
	}
	library := map[Code]string{ // the reference library's own texts
		CodeNoHandler:           "No handler registered for 'orders.PlaceOrder'",
		CodeTimeout:             "Timed out waiting for reply to 'orders.PlaceOrder'",
		CodeHandlerDisconnected: "Handler for 'orders.PlaceOrder' disconnected",
		CodeResourceExhausted:   "Handler for 'orders.PlaceOrder' is busy",
		CodeCancelled:           "'orders.PlaceOrder': hub says 9", // the hub's message, prefixed with the type
	}
	for _, c := range []struct {
		wire int32
		want Code
	}{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9}, {0, 4}, {42, 4}} {
		e := call(func(id string) *hubpb.ServerMessage {
			return fakehub.HubError(id, hubpb.ErrorCode(c.wire), fmt.Sprintf("hub says %d", c.wire))
		})
		is(t, e.Code, c.want)
		want, ok := library[c.want]
		if !ok {
			want = fmt.Sprintf("hub says %d", c.wire)
		}
		is(t, e.Message, want)
		is(t, len(e.Details), 0)
	}
	e := call(func(id string) *hubpb.ServerMessage {
		return failWith(id, hubpb.ErrorCode_HANDLER_ERROR, "insufficient stock", `{"sku":"a"}`)
	})
	is(t, e.Code, CodeHandlerError)
	is(t, e.Message, "insufficient stock")
	is(t, string(e.Details), `{"sku":"a"}`)
	for _, code := range []hubpb.ErrorCode{hubpb.ErrorCode_UNAUTHENTICATED, hubpb.ErrorCode_PERMISSION_DENIED, hubpb.ErrorCode_INVALID_REQUEST} {
		e := call(func(id string) *hubpb.ServerMessage { return failWith(id, code, "said the handler", `{"x":1}`) })
		is(t, e.Code, Code(code))
		is(t, e.Message, "said the handler")
		is(t, len(e.Details), 0) // details only for HANDLER_ERROR
	}
}

func TestAReplyThatDoesNotDecodeIsALocalError(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	sent := async(func() (Receipt, error) { return Send[Receipt](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{"total":"many"}`))
	err := await(t, sent).err
	var e *Error
	is(t, err != nil && !errors.As(err, &e) && !errors.Is(err, ErrOutcomeUnknown) && !errors.Is(err, ErrNeverSent), true)
	is(t, strings.Contains(err.Error(), "orders.PlaceOrder"), true)
}

func TestContextDeadlineIsTheCallersTimeout(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx, cancel := context.WithTimeout(context.Background(), 800*ms)
	defer cancel()
	sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
	r := s.RecvRequest(t)
	is(t, r.GetTimeoutMs() > 700 && r.GetTimeoutMs() <= 800, true)
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)

	// A shorter Timeout wins over the deadline, and the deadline over a longer Timeout.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 800*ms)
	defer cancel2()
	sent = async(func() (Empty, error) { return Send[Empty](ctx2, app, PlaceOrder{}, Timeout(300*ms)) })
	r = s.RecvRequest(t)
	is(t, r.GetTimeoutMs(), uint32(300))
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
	sent = async(func() (Empty, error) { return Send[Empty](ctx2, app, PlaceOrder{}, Timeout(5*time.Second)) })
	r = s.RecvRequest(t)
	is(t, r.GetTimeoutMs() > 0 && r.GetTimeoutMs() <= 800, true)
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)

	// An expired deadline sends nothing.
	expired, cancel3 := context.WithTimeout(context.Background(), -time.Second)
	defer cancel3()
	_, err := Send[Empty](expired, app, PlaceOrder{})
	is(t, errors.Is(err, context.DeadlineExceeded), true)
	is(t, errors.Is(err, ErrNeverSent), true)
	s.Nothing(t, 100*ms, "request")
	is(t, s.Recv(t).GetHeartbeat() != nil, true) // the stream is alive: nothing was written to it
}

func TestATimeoutPerCallWinsOverTheDefault(t *testing.T) { // as in the reference libraries
	app, _, s := sending(t, fakehub.Ack("hub-1"), DefaultTimeout(2*time.Second))
	ctx := ctxWithin(t, 5*time.Second)
	for _, c := range []struct {
		opts []SendOption
		want uint32
	}{{nil, 2000}, {[]SendOption{Timeout(3 * time.Second)}, 3000}, {[]SendOption{Timeout(500 * ms)}, 500}} {
		sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}, c.opts...) })
		r := s.RecvRequest(t)
		is(t, r.GetTimeoutMs(), c.want)
		s.Send(t, replyTo(r.GetId(), `{}`))
		is(t, await(t, sent).err, nil)
	}
}

func TestCancelledContextAfterTheWriteIsOutcomeUnknown(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx, cancel := context.WithCancel(context.Background())
	sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
	s.RecvRequest(t)
	cancelled := time.Now()
	cancel()
	o := await(t, sent)
	within(t, time.Since(cancelled), 0, 100*ms)
	is(t, errors.Is(o.err, context.Canceled), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	is(t, waiting(app), 0)
}

func TestCancelledContextBeforeTheWriteIsNeverSent(t *testing.T) {
	held := make(chan *hubpb.Request, 1)
	release := make(chan struct{})
	app, _, s := sending(t, fakehub.Ack("hub-1"), blockRequests(held, release))
	ctx, cancel := context.WithCancel(context.Background())
	sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
	receive(t, held)
	cancel()
	o := await(t, sent)
	is(t, errors.Is(o.err, context.Canceled), true)
	is(t, errors.Is(o.err, ErrNeverSent), true)
	close(release)
	s.Nothing(t, 200*ms, "request")              // withdrawn from the buffer: never written
	is(t, s.Recv(t).GetHeartbeat() != nil, true) // and the stream is alive
}

func TestSendRefusesWhatItCannotSend(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx := ctxWithin(t, 5*time.Second)
	_, err := Send[Empty](ctx, app, nil)
	is(t, err != nil, true)
	_, err = Send[Empty](ctx, nil, PlaceOrder{})
	is(t, err != nil, true)
	_, err = Send[Empty](ctx, app, PlaceOrder{}, Timeout(-time.Second))
	is(t, err != nil, true)
	_, err = Send[Empty](ctx, app, PlaceOrder{}, Retry(RetryPolicy{MaxAttempts: 0}))
	is(t, err != nil, true)
	_, err = Send[Empty](ctx, app, PlaceOrder{}, Retry(RetryPolicy{MaxAttempts: 2, Multiplier: 0.5}))
	is(t, err != nil, true)
	s.Nothing(t, 100*ms, "request")
	is(t, s.Recv(t).GetHeartbeat() != nil, true) // the stream is alive: nothing was written to it
}

func TestTheOutboundBufferHoldsAtMost256Requests(t *testing.T) {
	ref := newStreamRef(nil)
	req := func(id string) *hubpb.ClientMessage {
		return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Request{Request: &hubpb.Request{Id: id}}}
	}
	for i := range outboundLimit {
		is(t, ref.sendRequest(context.Background(), req(fmt.Sprint(i)), &call{}), nil)
	}
	is(t, ref.send(heartbeat()), nil) // the library's own frames never wait
	short, cancel := context.WithTimeout(context.Background(), 50*ms)
	defer cancel()
	is(t, errors.Is(ref.sendRequest(short, req("late"), &call{}), context.DeadlineExceeded), true)
	waiting := async(func() (Empty, error) {
		return Empty{}, ref.sendRequest(context.Background(), req("next"), &call{})
	})
	select {
	case <-waiting:
		t.Fatal("a request went into a full buffer")
	case <-time.After(50 * ms):
	}
	done := make(chan struct{})
	defer close(done)
	m, _, ok := ref.next(context.Background(), done)
	is(t, ok && m.GetRequest().GetId() == "0", true)
	is(t, await(t, waiting).err, nil) // taking one request made room for the waiting one
	// The frames leave in the order they were queued: the other requests, the heartbeat, the
	// request that waited.
	for i := 1; i < outboundLimit; i++ {
		m, _, _ = ref.next(context.Background(), done)
		is(t, m.GetRequest().GetId(), fmt.Sprint(i))
	}
	m, _, _ = ref.next(context.Background(), done)
	is(t, m.GetHeartbeat() != nil, true)
	m, _, _ = ref.next(context.Background(), done)
	is(t, m.GetRequest().GetId(), "next")
	ref.end()
	is(t, errors.Is(ref.sendRequest(context.Background(), req("gone"), &call{}), errStreamGone), true)
}

// fullBuffer fills the outbound buffer of app's only stream (its writer held by a blockRequests
// hook) with outboundLimit requests and starts one more Send, which waits for room; it returns
// that Send's outcome.
func fullBuffer(t *testing.T, app *App) <-chan outcome[Empty] {
	t.Helper()
	ref := app.pool.conns[0].stream()
	for range outboundLimit {
		go func() { _, _ = Send[Empty](context.Background(), app, PlaceOrder{}) }() // they end with the stream
	}
	poll(t, func() bool { ref.mu.Lock(); defer ref.mu.Unlock(); return ref.requests == outboundLimit })
	waiting := async(func() (Empty, error) { return Send[Empty](context.Background(), app, PlaceOrder{}) })
	poll(t, func() bool { ref.mu.Lock(); defer ref.mu.Unlock(); return ref.room != nil }) // it waits for room
	return waiting
}

// poll waits at most 5 s for cond.
func poll(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("the condition did not hold within 5 s")
		}
		time.Sleep(5 * ms)
	}
}

func TestASendWaitingForRoomEndsWithTheStream(t *testing.T) {
	held := make(chan *hubpb.Request, 2)
	app, _, s := sending(t, fakehub.Ack("hub-1"), blockRequests(held, nil))
	waiting := fullBuffer(t, app)
	s.Close(codes.Unavailable, "restart")
	o := await(t, waiting)
	is(t, errors.Is(o.err, ErrNeverSent), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), false)
}

func TestASendWaitingForRoomEndsWithShutdown(t *testing.T) {
	held := make(chan *hubpb.Request, 2)
	app, _, _ := sending(t, fakehub.Ack("hub-1"), blockRequests(held, nil))
	waiting := fullBuffer(t, app)
	stopped := async(func() (Empty, error) { return Empty{}, app.Shutdown(ctxWithin(t, 5*time.Second)) })
	o := await(t, waiting)
	is(t, errors.Is(o.err, ErrNeverSent), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), false)
	await(t, stopped)
}
