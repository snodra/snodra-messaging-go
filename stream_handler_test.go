package snodra

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// ListOrders is a streaming query: Count order lines, then the error Fail when set.
type ListOrders struct {
	Count int    `json:"count"`
	Fail  string `json:"fail,omitempty"`
}

func (ListOrders) MessageType() string { return "orders.ListOrders" }

// OrderLine is an item of ListOrders.
type OrderLine struct {
	N int `json:"n"`
}

// listOrders streams q.Count lines, then fails with q.Fail when set.
func listOrders(ctx context.Context, q ListOrders, w *StreamWriter[OrderLine]) error {
	for n := 1; n <= q.Count; n++ {
		if err := w.Send(ctx, OrderLine{N: n}); err != nil {
			return err
		}
	}
	if q.Fail != "" {
		return errors.New(q.Fail)
	}
	return nil
}

// streamRequest is a forwarded streaming query of ListOrders.
func streamRequest(id string, credit uint32, payload string) *hubpb.ServerMessage {
	return fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_QUERY, Type: "orders.ListOrders",
		Mode: hubpb.RequestMode_STREAMING, InitialCredit: credit, Payload: []byte(payload)})
}

// streamFrame is the next frame on s that is not a Permits, Heartbeat or InstanceStatus, within
// 5 s in all.
func streamFrame(t *testing.T, s *fakehub.Stream) *hubpb.ClientMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m := s.Recv(t)
		if m.GetPermits() == nil && m.GetHeartbeat() == nil && m.GetStatus() == nil {
			return m
		}
	}
	t.Fatal("no frame within 5 s")
	return nil
}

// nextPart is the next ReplyPart on s (permits, heartbeats and statuses skipped).
func nextPart(t *testing.T, s *fakehub.Stream) *hubpb.ReplyPart {
	t.Helper()
	m := streamFrame(t, s)
	if m.GetPart() == nil {
		t.Fatalf("expected a ReplyPart, got %v", m)
	}
	return m.GetPart()
}

// nextEnd is the next ReplyEnd on s (permits, heartbeats and statuses skipped).
func nextEnd(t *testing.T, s *fakehub.Stream) *hubpb.ReplyEnd {
	t.Helper()
	m := streamFrame(t, s)
	if m.GetEnd() == nil {
		t.Fatalf("expected a ReplyEnd, got %v", m)
	}
	return m.GetEnd()
}

// partIs checks a part of s#1: its sequence number and its line.
func partIs(t *testing.T, p *hubpb.ReplyPart, id string, seq uint64) {
	t.Helper()
	is(t, p.GetRequestId(), id)
	is(t, p.GetSeq(), seq)
	is(t, string(p.GetPayload()), `{"n":`+string(rune('0'+seq))+`}`)
}

func TestStreamPartsAreNumberedUnderTheCredit(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) { is(t, HandleStream(app, listOrders), nil) })
	s.Send(t, streamRequest("s#1", 2, `{"count":5}`))
	partIs(t, nextPart(t, s), "s#1", 1)
	partIs(t, nextPart(t, s), "s#1", 2)
	s.Nothing(t, 200*ms, "part", "end") // the credit is spent
	s.Send(t, fakehub.Flow("s#1", 2))
	partIs(t, nextPart(t, s), "s#1", 3)
	partIs(t, nextPart(t, s), "s#1", 4)
	s.Nothing(t, 200*ms, "part", "end")
	s.Send(t, fakehub.Flow("s#1", 1))
	partIs(t, nextPart(t, s), "s#1", 5)
	end := nextEnd(t, s) // the end needs no credit
	is(t, end.GetRequestId(), "s#1")
	is(t, end.GetError() == nil, true)
}

func TestAStreamStopsOnCancel(t *testing.T) {
	hub := fakehub.Start(t)
	sent := make(chan error, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, func(ctx context.Context, _ ListOrders, w *StreamWriter[OrderLine]) error {
			for n := 1; ; n++ {
				if err := w.Send(ctx, OrderLine{N: n}); err != nil {
					sent <- err
					return err
				}
			}
		}), nil)
	}, MaxConcurrent(1))
	s.Send(t, streamRequest("s#1", 1, `{}`))
	partIs(t, nextPart(t, s), "s#1", 1)
	s.Send(t, fakehub.Cancel("s#1", "sender disconnected"))
	if err := receive(t, sent); err == nil {
		t.Fatal("Send went on after the hub's Cancel")
	}
	is(t, s.RecvPermits(t).GetCount(), uint32(1)) // the permit goes back, with no part and no end before it
	s.Send(t, fakehub.Flow("s#1", 5))             // a late Flow changes nothing
	s.Nothing(t, 300*ms, "part", "end")
	s.Send(t, command("r-2", "orders.Missing", `{}`)) // the stream is alive: a later request is answered
	is(t, nextReply(t, s).GetRequestId(), "r-2")
}

func TestNoCreditWithinTheBoundEndsWithTimeout(t *testing.T) {
	hub := fakehub.Start(t)
	sent := make(chan error, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, func(ctx context.Context, q ListOrders, w *StreamWriter[OrderLine]) error {
			err := listOrders(ctx, q, w)
			sent <- err
			return err
		}), nil)
	}, StreamCreditTimeout(300*ms), MaxConcurrent(1))
	s.Send(t, streamRequest("s#1", 1, `{"count":2}`))
	partIs(t, nextPart(t, s), "s#1", 1)
	start := time.Now()
	end := nextEnd(t, s)
	within(t, time.Since(start), 250*ms, 1500*ms)
	is(t, end.GetRequestId(), "s#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_TIMEOUT)
	is(t, end.GetError().GetMessage(), "no credit within 300 ms")
	if err := receive(t, sent); err == nil {
		t.Fatal("Send returned no error at the credit timeout")
	}
	is(t, s.RecvPermits(t).GetCount(), uint32(1))
	s.Nothing(t, 200*ms, "part", "end") // one end only
}

func TestAStreamHandlersFailureEndsTheStream(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, func(ctx context.Context, q ListOrders, w *StreamWriter[OrderLine]) error {
			if q.Fail == "panic" {
				panic("out of ink")
			}
			return listOrders(ctx, q, w)
		}), nil)
	})
	s.Send(t, streamRequest("s#1", 4, `{"count":1,"fail":"out of paper\nnow"}`))
	partIs(t, nextPart(t, s), "s#1", 1)
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "s#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, end.GetError().GetMessage(), "out of paper now") // on one line
	is(t, end.GetError().GetRequestId(), "s#1")

	s.Send(t, streamRequest("s#2", 4, `{"fail":"panic"}`))
	end = nextEnd(t, s)
	is(t, end.GetRequestId(), "s#2")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, end.GetError().GetMessage(), "handler panicked")

	s.Send(t, streamRequest("s#3", 4, `{"count":"many"}`))
	end = nextEnd(t, s)
	is(t, end.GetRequestId(), "s#3")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
}

func TestAStreamHoldsItsPermitUntilItEnds(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) { is(t, HandleStream(app, listOrders), nil) }, MaxConcurrent(1))
	s.Send(t, streamRequest("s#1", 1, `{"count":2}`))
	is(t, s.RecvPart(t).GetSeq(), uint64(1))
	s.Nothing(t, 300*ms, "permits") // the handler waits for credit, holding its permit
	s.Send(t, fakehub.Flow("s#1", 1))
	is(t, s.RecvPart(t).GetSeq(), uint64(2))
	is(t, s.RecvEnd(t).GetError() == nil, true)
	is(t, s.RecvPermits(t).GetCount(), uint32(1)) // only after the end
}

func TestModeMismatchIsInvalidRequest(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, listOrders), nil)
		is(t, HandleQuery(app, get), nil)
		is(t, HandleCommand(app, place), nil)
	})
	// A unary request to a streaming handler: a Reply.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "q-1", Kind: hubpb.RequestKind_QUERY, Type: "orders.ListOrders", Payload: []byte(`{}`)}))
	r := nextReply(t, s)
	is(t, r.GetRequestId(), "q-1")
	is(t, r.GetError().GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
	is(t, r.GetError().GetMessage(), "orders.ListOrders is a streaming query: send it with Stream (mode STREAMING)")

	// A streaming request to a unary query handler: an end (SG-12).
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "s#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetOrder",
		Mode: hubpb.RequestMode_STREAMING, InitialCredit: 4, Payload: []byte(`{}`)}))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "s#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
	is(t, end.GetError().GetMessage(), "orders.GetOrder is not a streaming query handler (it answers mode UNARY)")

	// A streaming request of a type not served, or of a command type: NO_HANDLER, as an end.
	for _, req := range []*hubpb.Request{
		{Id: "s#2", Kind: hubpb.RequestKind_QUERY, Type: "orders.Missing", Mode: hubpb.RequestMode_STREAMING, InitialCredit: 4},
		{Id: "s#3", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Mode: hubpb.RequestMode_STREAMING, InitialCredit: 4},
	} {
		s.Send(t, fakehub.Forward(req))
		end := nextEnd(t, s)
		is(t, end.GetRequestId(), req.GetId())
		is(t, end.GetError().GetCode(), hubpb.ErrorCode_NO_HANDLER)
	}

	// A streaming handler is no command handler: a unary command of its type is NO_HANDLER.
	s.Send(t, command("c-1", "orders.ListOrders", `{}`))
	r = nextReply(t, s)
	is(t, r.GetError().GetCode(), hubpb.ErrorCode_NO_HANDLER)
}

func TestStreamAnswersOnTheArrivalStream(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	app := newApp(t, Hubs(a.Addr(), b.Addr()))
	is(t, HandleStream(app, listOrders), nil)
	is(t, app.Start(context.Background()), nil)
	sa, sb := a.Next(t), b.Next(t)
	sa.RecvRegister(t)
	sb.RecvRegister(t)
	sa.Ack(t, fakehub.Ack("hub-a"))
	sb.Ack(t, fakehub.Ack("hub-b"))
	sb.Send(t, streamRequest("s#1", 2, `{"count":3}`))
	partIs(t, nextPart(t, sb), "s#1", 1)
	partIs(t, nextPart(t, sb), "s#1", 2)
	sb.Send(t, fakehub.Flow("s#1", 1))
	partIs(t, nextPart(t, sb), "s#1", 3)
	is(t, nextEnd(t, sb).GetRequestId(), "s#1")
	is(t, sb.RecvPermits(t).GetCount(), uint32(1)) // the permit goes back to hub-b, which spent it
	sa.Nothing(t, 300*ms, "part", "end", "reply", "permits")
}

func TestAStreamOnALostArrivalStreamEnds(t *testing.T) {
	hub := fakehub.Start(t)
	sent := make(chan error, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, func(ctx context.Context, q ListOrders, w *StreamWriter[OrderLine]) error {
			err := listOrders(ctx, q, w)
			sent <- err
			return err
		}), nil)
	}, MaxConcurrent(1), ReconnectBackoff(50*ms, 200*ms))
	s.Send(t, streamRequest("s#1", 1, `{"count":2}`))
	is(t, nextPart(t, s).GetSeq(), uint64(1))
	s.Close(codes.Unavailable, "restart") // UNAVAILABLE: the handler waiting for credit stops at once
	if err := receive(t, sent); err == nil {
		t.Fatal("Send went on after its hub stream was lost")
	}
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.Ack(t, fakehub.Ack("hub-1"))
	s2.Nothing(t, 300*ms, "part", "end", "permits") // nothing for it on the new stream
	s2.Send(t, command("r-1", "orders.Missing", `{}`))
	is(t, nextReply(t, s2).GetRequestId(), "r-1") // the handler slot is free again
}

func TestStreamCreditTimeoutMustBePositive(t *testing.T) {
	for _, d := range []time.Duration{0, -s, 500 * time.Microsecond} {
		_, err := New(Service("orders-service"), Hubs("localhost:7000"), StreamCreditTimeout(d))
		if err == nil || !strings.Contains(err.Error(), "StreamCreditTimeout") {
			t.Fatalf("StreamCreditTimeout(%v): got %v, want an error naming it", d, err)
		}
	}
	app, err := New(Service("orders-service"), Hubs("localhost:7000"), StreamCreditTimeout(2*s))
	is(t, err, nil)
	is(t, app.cfg.timings.creditWait, 2*s)
}

func TestHandleStreamRegistersAQueryType(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	is(t, HandleStream(app, listOrders), nil)
	is(t, HandleStream(app, func(context.Context, GetStock, *StreamWriter[Stock]) error { return nil }, Shared()), nil)
	is(t, HandleQuery(app, get), nil)
	if HandleQuery(app, func(context.Context, ListOrders) (Empty, error) { return Empty{}, nil }) == nil {
		t.Fatal("a type registered both as a streaming query and a query was accepted")
	}
	if HandleStream[GetOrder, Receipt](app, nil) == nil {
		t.Fatal("a nil streaming handler was accepted")
	}
	if HandleStream[Message, Receipt](app, func(context.Context, Message, *StreamWriter[Receipt]) error { return nil }) == nil {
		t.Fatal("an interface type parameter was accepted")
	}
	is(t, app.Start(context.Background()) != nil, true) // the registration errors are kept
	app2 := newApp(t, Hubs(hub.Addr()))
	is(t, HandleStream(app2, listOrders), nil)
	is(t, HandleStream(app2, func(context.Context, GetStock, *StreamWriter[Stock]) error { return nil }, Shared()), nil)
	is(t, HandleQuery(app2, get), nil)
	is(t, app2.Start(context.Background()), nil)
	r := hub.Next(t).RecvRegister(t)
	is(t, strings.Join(r.GetQueryTypes(), ","), "orders.GetOrder,orders.ListOrders")
	is(t, strings.Join(r.GetSharedQueryTypes(), ","), "orders.GetStock")
}

func TestASendWaitingBehindAnotherHonoursItsContext(t *testing.T) {
	hub := fakehub.Start(t)
	type result struct {
		err     error
		elapsed time.Duration
	}
	second := make(chan result, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, func(ctx context.Context, _ ListOrders, w *StreamWriter[OrderLine]) error {
			// No credit: the first Send waits, holding the writer (polled at most 5 s, off the test
			// goroutine).
			go func() { _ = w.Send(ctx, OrderLine{N: 1}) }()
			for i := 0; i < 1000 && len(w.out.sendSlot) == 0; i++ {
				time.Sleep(5 * ms)
			}
			sctx, cancel := context.WithTimeout(ctx, 100*ms)
			defer cancel()
			start := time.Now()
			err := w.Send(sctx, OrderLine{N: 2})
			second <- result{err, time.Since(start)}
			return nil
		}), nil)
	})
	s.Send(t, streamRequest("s#1", 0, `{}`))
	r := receive(t, second)
	is(t, errors.Is(r.err, context.DeadlineExceeded), true)
	within(t, r.elapsed, 90*ms, time.Second) // not the credit wait of the Send ahead of it
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "s#1")
	is(t, end.GetError() == nil, true)
}
