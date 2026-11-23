package snodra

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// serving starts an app on hub with the handlers register adds and returns its first stream,
// acknowledged.
func serving(t *testing.T, hub *fakehub.Hub, register func(*App), opts ...Option) (*App, *fakehub.Stream) {
	t.Helper()
	app := newApp(t, append([]Option{Hubs(hub.Addr())}, opts...)...)
	register(app)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Recv(t) // the heartbeat after the Register, before the ack (see fakehub.Stream.Ack)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	return app, s
}

// receive waits at most 5 s for a value on ch.
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not run within 5 s")
		var zero T
		return zero
	}
}

// command is a forwarded command.
func command(id, typ, payload string) *hubpb.ServerMessage {
	return fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_COMMAND, Type: typ, Payload: []byte(payload)})
}

// nextReply returns the next Reply on s, skipping Permits, Heartbeat and InstanceStatus frames;
// it fails the test after 5 s.
func nextReply(t *testing.T, s *fakehub.Stream) *hubpb.Reply {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("no Reply within 5 s")
		}
		m := s.Recv(t)
		switch {
		case m.GetReply() != nil:
			return m.GetReply()
		case m.GetPermits() != nil, m.GetHeartbeat() != nil, m.GetStatus() != nil:
		default:
			t.Fatalf("expected a Reply, got %v", m)
		}
	}
}

func TestAUnaryRequestIsAnswered(t *testing.T) {
	hub := fakehub.Start(t)
	seen := make(chan MessageInfo, 2)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, p PlaceOrder) (Receipt, error) {
			info, ok := MessageFrom(ctx)
			is(t, ok, true)
			seen <- info
			return Receipt{OrderID: p.OrderID, Total: 42}, nil
		}), nil)
	})
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
		Payload: []byte(`{"orderId":"o-1"}`), Headers: map[string]string{"x-tenant": "t-1"}}))
	r := s.RecvReply(t)
	is(t, r.GetRequestId(), "r-1")
	is(t, string(r.GetPayload()), `{"orderId":"o-1","total":42}`)
	info := receive(t, seen)
	is(t, info.ID, "r-1")
	is(t, info.Type, "orders.PlaceOrder")
	is(t, info.Headers["x-tenant"], "t-1")
	is(t, info.CorrelationID, "r-1") // the request id stands in for a missing correlation id
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-2", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
		Payload: []byte(`{}`), Headers: map[string]string{"correlationId": "c-1"}}))
	is(t, nextReply(t, s).GetRequestId(), "r-2")
	is(t, receive(t, seen).CorrelationID, "c-1")
	_, ok := MessageFrom(context.Background())
	is(t, ok, false)
}

func TestACommandWithoutResultAnswersAnEmptyObject(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil }), nil)
	})
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	is(t, string(s.RecvReply(t).GetPayload()), "{}")
}

func TestNilResultAnswersEmptyObject(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleQuery(app, func(context.Context, GetOrder) (*Receipt, error) { return nil, nil }), nil)
	})
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetOrder", Payload: []byte(`{}`)}))
	r := s.RecvReply(t)
	is(t, r.GetError() == nil, true)
	is(t, string(r.GetPayload()), "{}")
}

func TestPointerMessageTypes(t *testing.T) {
	hub := fakehub.Start(t)
	got := make(chan *CancelOrder, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(_ context.Context, c *CancelOrder) (*Receipt, error) {
			got <- c
			return &Receipt{OrderID: c.ID}, nil
		}), nil)
	})
	s.Send(t, command("r-1", "orders.CancelOrder", `{"id":"o-9"}`))
	is(t, string(s.RecvReply(t).GetPayload()), `{"orderId":"o-9","total":0}`)
	c := receive(t, got)
	is(t, c != nil, true)
	is(t, c.ID, "o-9")
}

func TestRequestsBeforeTheAckAreServed(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()), MaxConcurrent(1))
	is(t, HandleCommand(app, place), nil)
	is(t, app.Start(context.Background()), nil)
	s := hub.Next(t)
	s.RecvRegister(t)
	for _, id := range []string{"r-1", "r-2"} { // the second only once the first permit came back
		s.Send(t, command(id, "orders.PlaceOrder", `{"orderId":"o-1"}`))
		is(t, s.RecvReply(t).GetRequestId(), id)
		is(t, s.RecvPermits(t).GetCount(), uint32(1))
	}
	is(t, len(app.HubIDs()), 0) // still not acknowledged
}

func TestOutcomesAreMessagesNeverADroppedStream(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(_ context.Context, p PlaceOrder) (Receipt, error) {
			switch p.SKU {
			case "none":
				return Receipt{}, errors.New("out of stock")
			case "panic":
				panic("boom\nat line 3")
			case "denied":
				return Receipt{}, PermissionDenied("orders-user required")
			}
			return Receipt{OrderID: p.OrderID}, nil
		}), nil)
		is(t, HandleQuery(app, get), nil)
	})
	cases := []struct {
		name string
		req  *hubpb.Request
		code hubpb.ErrorCode
		msg  string
	}{
		{"type not served", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.Missing", Payload: []byte(`{}`)},
			hubpb.ErrorCode_NO_HANDLER, "orders.Missing"},
		{"a query sent as a command", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.GetOrder", Payload: []byte(`{}`)},
			hubpb.ErrorCode_NO_HANDLER, "orders.GetOrder"},
		{"no kind", &hubpb.Request{Type: "orders.PlaceOrder", Payload: []byte(`{}`)},
			hubpb.ErrorCode_INVALID_REQUEST, "kind"},
		{"wrong shape", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`{"orderId":42}`)},
			hubpb.ErrorCode_INVALID_REQUEST, "cannot decode payload"},
		{"not json", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`not json`)},
			hubpb.ErrorCode_INVALID_REQUEST, "cannot decode payload"},
		{"an error", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`{"sku":"none"}`)},
			hubpb.ErrorCode_HANDLER_ERROR, "out of stock"},
		{"a panic", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`{"sku":"panic"}`)},
			hubpb.ErrorCode_HANDLER_ERROR, "handler panicked"},
		{"a coded error", &hubpb.Request{Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`{"sku":"denied"}`)},
			hubpb.ErrorCode_PERMISSION_DENIED, "orders-user required"},
	}
	for i, c := range cases {
		c.req.Id = "bad-" + c.name
		s.Send(t, fakehub.Forward(c.req))
		e := nextReply(t, s).GetError()
		if e.GetCode() != c.code || !strings.Contains(e.GetMessage(), c.msg) || strings.ContainsAny(e.GetMessage(), "\r\n") {
			t.Fatalf("%s: got %v %q, want %v containing %q", c.name, e.GetCode(), e.GetMessage(), c.code, c.msg)
		}
		is(t, e.GetRequestId(), c.req.Id)
		if c.name == "a panic" {
			is(t, e.GetMessage(), "handler panicked") // the reference library's text; the value goes to the log
		}
		ok := "ok-" + string(rune('a'+i))
		s.Send(t, command(ok, "orders.PlaceOrder", `{"orderId":"o-1"}`)) // the stream is still up
		r := nextReply(t, s)
		is(t, r.GetRequestId(), ok)
		is(t, r.GetError() == nil, true)
	}
	hub.NoStreamWithin(t, 50*ms)
}

func TestTheReplyGoesOnlyToTheArrivalStream(t *testing.T) {
	hub := fakehub.Start(t)
	started := make(chan struct{}, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
			started <- struct{}{}
			time.Sleep(200 * ms)
			return Empty{}, nil
		}), nil)
	})
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	receive(t, started) // the handler runs: its stream closes under it
	s.Close(codes.Unavailable, "restart")
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.Ack(t, fakehub.Ack("hub-1"))
	s2.Nothing(t, 400*ms, "reply", "permits")
}

func TestHandlersNeverExceedTheCapacity(t *testing.T) {
	hub := fakehub.Start(t)
	var running, most atomic.Int32
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
			n := running.Add(1)
			for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
			}
			time.Sleep(100 * ms)
			running.Add(-1)
			return Empty{}, nil
		}), nil)
	}, MaxConcurrent(2))
	for i := 0; i < 4; i++ { // a hub ignoring the permits
		s.Send(t, command("r-"+string(rune('1'+i)), "orders.PlaceOrder", `{}`))
	}
	for i := 0; i < 4; i++ {
		nextReply(t, s)
	}
	is(t, most.Load(), int32(2))
}

func TestShutdownReturnsByItsDeadlineWithAStuckHandler(t *testing.T) {
	hub := fakehub.Start(t)
	release, started := make(chan struct{}), make(chan context.Context, 1)
	t.Cleanup(func() { close(release) })
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
			started <- ctx
			<-release // ignores ctx
			return Empty{}, nil
		}), nil)
	})
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	hctx := receive(t, started)
	start := time.Now()
	err := app.Shutdown(ctxWithin(t, 300*ms))
	is(t, errors.Is(err, context.DeadlineExceeded), true)
	within(t, time.Since(start), 300*ms, 300*ms+killGrace)
	select {
	case <-hctx.Done(): // the handler's context ends with the hard stop
	case <-time.After(time.Second):
		t.Fatal("the stuck handler's context was not cancelled")
	}
}

func TestShutdownDrainsTheHandlersBeforeTheHalfClose(t *testing.T) {
	hub := fakehub.Start(t)
	started := make(chan struct{}, 1)
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
			started <- struct{}{}
			time.Sleep(250 * ms) // beyond the 100 ms deregister wait
			return Empty{}, nil
		}), nil)
	})
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	receive(t, started)
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	is(t, len(s.RecvRegister(t).GetCommandTypes()), 0) // the empty Register first
	is(t, nextReply(t, s).GetRequestId(), "r-1")       // then the handler's reply
	s.HalfClosed(t, time.Second)                       // then the half-close
	is(t, <-done, nil)
}

// countingCodec is JSON, counting its calls.
type countingCodec struct{ decoded, encoded atomic.Int32 }

func (c *countingCodec) Marshal(v any) ([]byte, error) { c.encoded.Add(1); return JSON.Marshal(v) }
func (c *countingCodec) Unmarshal(b []byte, v any) error {
	c.decoded.Add(1)
	return JSON.Unmarshal(b, v)
}

func TestTheCodecOptionDecodesAndEncodes(t *testing.T) {
	hub := fakehub.Start(t)
	codec := &countingCodec{}
	_, s := serving(t, hub, func(app *App) { is(t, HandleCommand(app, place), nil) }, Codec(codec))
	s.Send(t, command("r-1", "orders.PlaceOrder", `{"orderId":"o-1"}`))
	is(t, string(s.RecvReply(t).GetPayload()), `{"orderId":"o-1","total":42}`)
	is(t, codec.decoded.Load(), int32(1))
	is(t, codec.encoded.Load(), int32(1))
	_, err := New(Service("orders-service"), Hubs("127.0.0.1:7000"), Codec(nil))
	is(t, err != nil && strings.Contains(err.Error(), "Codec"), true)
}

func TestAPermitComesBackAfterEveryFailure(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(_ context.Context, p PlaceOrder) (Empty, error) {
			if p.SKU == "panic" {
				panic("boom")
			}
			return Empty{}, nil
		}), nil)
	}, MaxConcurrent(1))
	for _, c := range []struct {
		typ, payload string
		want         hubpb.ErrorCode
	}{
		{"orders.Unknown", `{}`, hubpb.ErrorCode_NO_HANDLER},
		{"orders.PlaceOrder", `{"sku":1}`, hubpb.ErrorCode_INVALID_REQUEST},
		{"orders.PlaceOrder", `{"sku":"panic"}`, hubpb.ErrorCode_HANDLER_ERROR},
	} {
		s.Send(t, command("r-"+c.payload, c.typ, c.payload))
		is(t, nextReply(t, s).GetError().GetCode(), c.want)
		is(t, s.RecvPermits(t).GetCount(), uint32(1)) // the one permit, back at once
	}
}

func TestShutdownHalfClosesWhenTheHandlerDrainExpires(t *testing.T) {
	hub := fakehub.Start(t)
	rec := &logRecorder{}
	started := make(chan struct{}, 1)
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
			started <- struct{}{}
			<-ctx.Done() // until the hard stop
			return Empty{}, ctx.Err()
		}), nil)
	}, Logger(slog.New(rec)))
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	receive(t, started)
	begin := time.Now()
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	is(t, len(s.RecvRegister(t).GetCommandTypes()), 0) // not acked: the 100 ms wait runs out
	s.HalfClosed(t, 2*time.Second)
	within(t, time.Since(begin), 1100*ms, 1500*ms) // 100 ms, then the 1 s handler drain
	is(t, <-done, nil)
	is(t, rec.count(slog.LevelWarn, "handlers still running after the drain; closing the hub streams anyway"), 1)
}

func TestAHandlerInFlightAtTheHalfCloseIsAnsweredFirst(t *testing.T) {
	hub := fakehub.Start(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
			started <- struct{}{}
			<-release
			return Empty{}, nil
		}), nil)
	})
	// A request that arrived after the shutdown's drain found no handler running: the
	// half-close, due now, waits for it (within the drain's bound).
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	receive(t, started)
	app.setDrainDeadline(time.Now().Add(5 * time.Second))
	app.pool.conns[0].close()
	time.AfterFunc(100*ms, func() { close(release) })
	is(t, nextReply(t, s).GetRequestId(), "r-1") // answered on the stream, not dropped
	s.HalfClosed(t, time.Second)                 // then half-closed
}

func TestACancelledTargetSendsNothing(t *testing.T) {
	hub := fakehub.Start(t)
	started := make(chan struct{}, 1)
	stopped := make(chan error, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleQuery(app, func(ctx context.Context, _ GetStock) (Stock, error) {
			started <- struct{}{}
			select {
			case <-time.After(2 * time.Second):
				return Stock{Count: 1}, nil
			case <-ctx.Done():
				stopped <- ctx.Err()
				return Stock{}, ctx.Err()
			}
		}), nil)
	}, MaxConcurrent(1))
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "g#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetStock", Payload: []byte(`{}`)}))
	receive(t, started)
	s.Send(t, fakehub.Cancel("g#1", "max results"))
	is(t, receive(t, stopped), context.Canceled)  // the handler's ctx is done
	is(t, s.RecvPermits(t).GetCount(), uint32(1)) // the permit goes back
	s.Nothing(t, 2500*time.Millisecond, "reply")
	s.Send(t, command("r-2", "orders.Missing", `{}`)) // the stream is alive: a later request is answered
	is(t, nextReply(t, s).GetRequestId(), "r-2")
}

func TestACancelBeforeTheHandlerStartsSendsNothing(t *testing.T) {
	hub := fakehub.Start(t)
	started := make(chan string, 2)
	release := make(chan struct{})
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleQuery(app, func(ctx context.Context, q GetStock) (Stock, error) {
			started <- q.SKU
			<-release
			return Stock{}, nil
		}), nil)
	}, MaxConcurrent(1))
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "g#0", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetStock", Payload: []byte(`{"sku":"a"}`)}))
	is(t, receive(t, started), "a")
	// g#1 waits for the one handler slot; its Cancel ends the wait.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "g#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetStock", Payload: []byte(`{"sku":"b"}`)}))
	s.Send(t, fakehub.Cancel("g#1", "deadline"))
	is(t, s.RecvPermits(t).GetCount(), uint32(1)) // g#1's permit, back without running it
	close(release)
	is(t, nextReply(t, s).GetRequestId(), "g#0")
	s.Nothing(t, 300*ms, "reply")
	select {
	case sku := <-started:
		t.Fatalf("the cancelled request %q ran", sku)
	default:
	}
}

func TestACancelForAnUnknownIDIsIgnored(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) { is(t, HandleCommand(app, place), nil) })
	s.Send(t, fakehub.Cancel("gone#3", "deadline"))
	s.Send(t, command("r-1", "orders.PlaceOrder", `{"orderId":"o-1"}`))
	is(t, string(nextReply(t, s).GetPayload()), `{"orderId":"o-1","total":42}`)
}

// takeFrame waits at most 5 s for the next frame queued on ref other than Permits, and takes it.
func takeFrame(t *testing.T, ref *streamRef) *hubpb.ClientMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ref.mu.Lock()
		for len(ref.queue) > 0 {
			m := ref.queue[0].m
			ref.queue = ref.queue[1:]
			if m.GetPermits() == nil {
				ref.mu.Unlock()
				return m
			}
		}
		ref.mu.Unlock()
		time.Sleep(5 * ms)
	}
	t.Fatal("no frame queued within 5 s")
	return nil
}

func TestALastResortFailsOnlyTheRequest(t *testing.T) {
	const lastResort = "serving a request from the hub panicked; the request fails, the stream goes on"
	rec := &logRecorder{}
	app := newApp(t, Hubs("localhost:7000"), Logger(slog.New(rec)))
	is(t, HandleCommand(app, place), nil)
	updates, err := HandleSubscription(app, watchOrders)
	is(t, err, nil)
	ref := newStreamRef(nil)
	permits := newStreamPermits(ref, 4, time.Hour)
	auth := map[string]string{"authorization": "Bearer a.b.c"}

	// A defect on the stream's read loop (here: no permits): the request fails, the loop goes on.
	app.dispatch(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`{}`)}, ref, nil)
	r := takeFrame(t, ref).GetReply()
	is(t, r.GetRequestId(), "r-1")
	is(t, r.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, r.GetError().GetMessage(), "handler panicked")
	is(t, rec.count(slog.LevelError, lastResort), 1)

	// A defect on the request's goroutine (here: no identity resolver), for a unary request and
	// for a subscription: each fails alone; the permit and the place come back.
	app.identity = nil
	app.dispatch(&hubpb.Request{Id: "r-2", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
		Payload: []byte(`{}`), Headers: auth}, ref, permits)
	r = takeFrame(t, ref).GetReply()
	is(t, r.GetRequestId(), "r-2")
	is(t, r.GetError().GetMessage(), "handler panicked")
	poll(t, func() bool { return !permits.busy() && app.inflight.count() == 0 })
	app.dispatch(&hubpb.Request{Id: "w#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.WatchOrders",
		Mode: hubpb.RequestMode_SUBSCRIPTION, Payload: []byte(`{"topic":"t"}`), Headers: auth}, ref, permits)
	end := takeFrame(t, ref).GetEnd()
	is(t, end.GetRequestId(), "w#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, end.GetError().GetMessage(), "handler panicked")
	is(t, app.SubscriptionsServed(), 0)
	is(t, updates.Emit(nil, OrderUpdate{Value: 1}), 0) // and an Emit does not wait for it
	is(t, rec.count(slog.LevelError, lastResort), 3)

	// A defect while routing a frame for this instance's own calls: dropped and logged.
	app.calls = nil
	app.route(&hubpb.ServerMessage{Message: &hubpb.ServerMessage_Part{Part: &hubpb.ReplyPart{RequestId: "q-1"}}})
	is(t, rec.count(slog.LevelError, "a frame from the hub could not be handled; it is dropped, the stream goes on"), 1)
}

// TestEveryModeAgainstEveryHandler pins what a request in each mode (unary, streaming,
// subscription) is answered with by a handler of each kind (command, query, streaming query,
// subscription query) and for a type not served: the code and the frame it rides in, as the
// reference library answers. The kind of a streaming or subscription request is not looked at:
// only query handlers serve those modes.
func TestEveryModeAgainstEveryHandler(t *testing.T) {
	hub := fakehub.Start(t)
	var updates *Updates[WatchOrders]
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, place), nil)
		is(t, HandleQuery(app, get), nil)
		is(t, HandleStream(app, listOrders), nil)
		u, err := HandleSubscription(app, watchOrders)
		is(t, err, nil)
		updates = u
	})
	const (
		reply = "Reply"
		end   = "ReplyEnd"
		ok    = hubpb.ErrorCode_ERROR_CODE_UNSPECIFIED // answered, no error
		cmd   = hubpb.RequestKind_COMMAND
		qry   = hubpb.RequestKind_QUERY
		unary = hubpb.RequestMode_UNARY
		strm  = hubpb.RequestMode_STREAMING
		sub   = hubpb.RequestMode_SUBSCRIPTION
	)
	cells := []struct {
		id    string
		kind  hubpb.RequestKind
		typ   string
		mode  hubpb.RequestMode
		frame string
		code  hubpb.ErrorCode
		msg   string
	}{
		// Unary requests: a Reply.
		{"u-1", cmd, "orders.PlaceOrder", unary, reply, ok, ""},
		{"u-2", qry, "orders.GetOrder", unary, reply, ok, ""},
		{"u-3", qry, "orders.ListOrders", unary, reply, hubpb.ErrorCode_INVALID_REQUEST,
			"orders.ListOrders is a streaming query: send it with Stream (mode STREAMING)"},
		{"u-4", qry, "orders.WatchOrders", unary, reply, hubpb.ErrorCode_INVALID_REQUEST,
			"orders.WatchOrders is a subscription query: send it with Subscribe (mode SUBSCRIPTION)"},
		{"u-5#0", qry, "orders.WatchOrders", unary, reply, ok, ""}, // a subscription's initial request
		{"u-6", qry, "orders.Missing", unary, reply, hubpb.ErrorCode_NO_HANDLER, ""},
		{"u-7", cmd, "orders.Missing", unary, reply, hubpb.ErrorCode_NO_HANDLER, ""},
		{"u-8", qry, "orders.PlaceOrder", unary, reply, hubpb.ErrorCode_NO_HANDLER, ""}, // a command type asked as a query
		{"u-9", cmd, "orders.GetOrder", unary, reply, hubpb.ErrorCode_NO_HANDLER, ""},   // a query type sent as a command
		// Streaming requests: a ReplyEnd (after the parts, when served).
		{"s-1#1", qry, "orders.PlaceOrder", strm, end, hubpb.ErrorCode_NO_HANDLER, ""},
		{"s-2#1", qry, "orders.GetOrder", strm, end, hubpb.ErrorCode_INVALID_REQUEST,
			"orders.GetOrder is not a streaming query handler (it answers mode UNARY)"},
		{"s-3#1", qry, "orders.ListOrders", strm, end, ok, ""},
		{"s-4#1", qry, "orders.WatchOrders", strm, end, hubpb.ErrorCode_INVALID_REQUEST,
			"orders.WatchOrders is not a streaming query handler (it answers mode SUBSCRIPTION)"},
		{"s-5#1", qry, "orders.Missing", strm, end, hubpb.ErrorCode_NO_HANDLER, ""},
		{"s-6#1", cmd, "orders.ListOrders", strm, end, ok, ""}, // the kind is not looked at
		// Subscription requests: a ReplyEnd when refused, nothing when admitted.
		{"w-1#1", qry, "orders.PlaceOrder", sub, end, hubpb.ErrorCode_INVALID_REQUEST,
			"no subscription handler for orders.PlaceOrder on this instance"},
		{"w-2#1", qry, "orders.GetOrder", sub, end, hubpb.ErrorCode_INVALID_REQUEST,
			"orders.GetOrder is not a subscription query handler (it answers mode UNARY)"},
		{"w-3#1", qry, "orders.ListOrders", sub, end, hubpb.ErrorCode_INVALID_REQUEST,
			"orders.ListOrders is not a subscription query handler (it answers mode STREAMING)"},
		{"w-4#1", qry, "orders.WatchOrders", sub, "", ok, ""}, // admitted
		{"w-5#1", qry, "orders.Missing", sub, end, hubpb.ErrorCode_INVALID_REQUEST,
			"no subscription handler for orders.Missing on this instance"},
		{"w-6#1", cmd, "orders.WatchOrders", sub, "", ok, ""}, // the kind is not looked at
	}
	for _, c := range cells {
		credit := uint32(0)
		if c.mode == strm {
			credit = 4
		}
		s.Send(t, fakehub.Forward(&hubpb.Request{Id: c.id, Kind: c.kind, Type: c.typ, Mode: c.mode,
			InitialCredit: credit, Payload: []byte(`{"count":1,"topic":"` + c.id + `"}`)}))
		if c.frame == "" {
			match := func(q WatchOrders) bool { return q.Topic == c.id }
			poll(t, func() bool { return updates.Emit(match, OrderUpdate{Value: 1}) == 1 })
			p := nextPart(t, s)
			if p.GetRequestId() != c.id {
				t.Fatalf("%s: a part of %s, want one of the admitted subscription", c.id, p.GetRequestId())
			}
			continue
		}
		m := streamFrame(t, s)
		if c.frame == end && c.code == ok {
			for m.GetPart() != nil { // a served stream's parts come first
				m = streamFrame(t, s)
			}
		}
		var werr *hubpb.Error
		switch {
		case c.frame == reply && m.GetReply() != nil:
			is(t, m.GetReply().GetRequestId(), c.id)
			werr = m.GetReply().GetError()
		case c.frame == end && m.GetEnd() != nil:
			is(t, m.GetEnd().GetRequestId(), c.id)
			werr = m.GetEnd().GetError()
		default:
			t.Fatalf("%s %s %s: got %v, want a %s", c.id, c.mode, c.typ, m, c.frame)
		}
		if werr.GetCode() != c.code {
			t.Fatalf("%s %s %s: answered %v (%q), want %v", c.id, c.mode, c.typ, werr.GetCode(), werr.GetMessage(), c.code)
		}
		if c.msg != "" {
			is(t, werr.GetMessage(), c.msg)
		}
	}
	is(t, app.SubscriptionsServed(), 2)
}
