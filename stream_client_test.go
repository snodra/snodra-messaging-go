package snodra

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// collect ranges over seq to its end and returns what it yielded, stopping at the first error.
func collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var out []T
	for v, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

// line is the ReplyPart seq of stream id with the line n = seq.
func line(id string, seq uint64) *hubpb.ServerMessage {
	return partOf(id, seq, "B", "b1", `{"n":`+strconv.FormatUint(seq, 10)+`}`, nil)
}

// streamEnd is the ReplyEnd of stream id: parts relayed, and e when it failed.
func streamEnd(id string, parts uint32, e *hubpb.Error) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_End{End: &hubpb.ReplyEnd{RequestId: id, Parts: parts, Error: e}}}
}

func TestStreamSendsTheCreditAndRefills(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{Count: 12}, InitialCredit(4), RoutingKey("ignored")))
	})
	r := s.RecvRequest(t)
	is(t, r.GetMode(), hubpb.RequestMode_STREAMING)
	is(t, r.GetKind(), hubpb.RequestKind_QUERY)
	is(t, r.GetType(), "orders.ListOrders")
	is(t, r.GetInitialCredit(), uint32(4))
	is(t, r.GetRoutingKey(), "") // unkeyed
	is(t, r.GetTimeoutMs(), uint32(0))
	is(t, string(r.GetPayload()), `{"count":12}`)
	id := r.GetId()
	// A hub keeping to the credit: 4 parts, then 2 more for every Flow of 2.
	for seq := uint64(1); seq <= 4; seq++ {
		s.Send(t, line(id, seq))
	}
	for seq := uint64(5); seq <= 12; seq += 2 {
		f := s.RecvFlow(t)
		is(t, f.GetRequestId(), id)
		is(t, f.GetCredits(), uint32(2))
		s.Send(t, line(id, seq))
		s.Send(t, line(id, seq+1))
	}
	s.Send(t, streamEnd(id, 12, nil))
	o := await(t, read)
	is(t, o.err, nil)
	is(t, len(o.value), 12)
	for i, v := range o.value {
		is(t, v.N, i+1)
	}
	is(t, waiting(app), 0)
	is(t, s.RecvFlow(t).GetCredits(), uint32(2)) // items 11 and 12 consumed
	is(t, s.RecvFlow(t).GetCredits(), uint32(2))
	s.Nothing(t, 200*ms, "cancel", "flow") // a completed stream is not cancelled

	// Without InitialCredit: 64, refilled by 32.
	read = async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}, Timeout(1500*ms), MaxResults(3)))
	})
	r = s.RecvRequest(t)
	is(t, r.GetInitialCredit(), uint32(64))
	is(t, r.GetTimeoutMs(), uint32(1500)) // the idle timeout
	is(t, r.GetMaxResults(), uint32(3))
	s.Send(t, streamEnd(r.GetId(), 0, nil))
	o = await(t, read)
	is(t, o.err, nil)
	is(t, len(o.value), 0)
}

func TestBreakingTheLoopCancelsAndLeaksNothing(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	_ = Stream[OrderLine](context.Background(), app, ListOrders{}) // never ranged: nothing is sent
	s.Nothing(t, 200*ms, "request")
	before := runtime.NumGoroutine()
	read := async(func() ([]OrderLine, error) {
		var got []OrderLine
		for v, err := range Stream[OrderLine](context.Background(), app, ListOrders{Count: 5}) {
			if err != nil {
				return got, err
			}
			got = append(got, v)
			if len(got) == 2 {
				break
			}
		}
		return got, nil
	})
	r := s.RecvRequest(t)
	for seq := uint64(1); seq <= 3; seq++ {
		s.Send(t, line(r.GetId(), seq))
	}
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "collector cancelled")
	o := await(t, read)
	is(t, o.err, nil)
	is(t, len(o.value), 2)
	is(t, waiting(app), 0)
	s.Send(t, line(r.GetId(), 4)) // late frames find nothing
	s.Send(t, streamEnd(r.GetId(), 4, nil))
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * ms)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("%d goroutines left, %d before the stream", n, before)
	}
	sent := async(func() (Empty, error) { return Query[Empty](context.Background(), app, GetOrder{}) })
	q := s.RecvRequest(t) // the stream is alive
	s.Send(t, replyTo(q.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

func TestAGapOrAShortEndIsLostParts(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r := s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	s.Send(t, line(r.GetId(), 2))
	s.Send(t, line(r.GetId(), 4)) // 3 was dropped by the hub
	o := await(t, read)
	is(t, len(o.value), 2)
	is(t, errors.Is(o.err, ErrLostParts), true)
	is(t, o.err.Error(), "snodra: parts of the answer were lost: stream orders.ListOrders lost 1 part(s)")
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "stream orders.ListOrders lost 1 part(s)")

	read = async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r = s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	s.Send(t, line(r.GetId(), 2))
	s.Send(t, streamEnd(r.GetId(), 3, nil)) // the hub counted a part that never came
	o = await(t, read)
	is(t, len(o.value), 2)
	is(t, errors.Is(o.err, ErrLostParts), true)
	s.Nothing(t, 200*ms, "cancel") // it ended: nothing to cancel
	is(t, waiting(app), 0)
}

func TestTheEndsErrorReachesTheCaller(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r := s.RecvRequest(t)
	s.Send(t, streamEnd(r.GetId(), 0, &hubpb.Error{RequestId: r.GetId(), Code: hubpb.ErrorCode_TIMEOUT, Message: "idle timeout"}))
	o := await(t, read)
	var e *Error
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeTimeout)
	is(t, e.Message, "Timed out waiting for reply to 'orders.ListOrders'")

	read = async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r = s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	s.Send(t, streamEnd(r.GetId(), 1, &hubpb.Error{RequestId: r.GetId(), Code: hubpb.ErrorCode_HANDLER_ERROR,
		Message: "out of paper", Details: []byte(`{"tray":2}`)}))
	o = await(t, read)
	is(t, len(o.value), 1)
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeHandlerError)
	is(t, e.Message, "out of paper")
	is(t, string(e.Details), `{"tray":2}`)

	read = async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r = s.RecvRequest(t)
	s.Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "no live instance"))
	o = await(t, read)
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeNoHandler)

	read = async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r = s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 1, "B", "b1", `{"n":"one"}`, nil)) // does not decode
	o = await(t, read)
	is(t, o.err != nil, true)
	is(t, errors.As(o.err, &e), false) // a local failure
	is(t, strings.HasPrefix(s.RecvCancel(t).GetReason(), "collector failed: snodra: cannot decode a part of orders.ListOrders"), true)
	is(t, waiting(app), 0)
}

func TestAStreamOnALostHubStreamFails(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r := s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	s.Close(codes.Unavailable, "restart")
	o := await(t, read)
	is(t, len(o.value), 1) // the part that came before the loss
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	is(t, errors.Is(o.err, ErrNeverSent), false)
	is(t, waiting(app), 0)
}

func TestAStreamCancelledByItsContext(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	read := async(func() ([]OrderLine, error) {
		var got []OrderLine
		for v, err := range Stream[OrderLine](ctx, app, ListOrders{}) {
			if err != nil {
				return got, err
			}
			got = append(got, v)
			if len(got) == 2 {
				cancel() // the loop goes on: the next item is the context's error
			}
		}
		return got, nil
	})
	r := s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	s.Send(t, line(r.GetId(), 2))
	o := await(t, read)
	is(t, len(o.value), 2)
	is(t, errors.Is(o.err, context.Canceled), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true) // as for Send: it was written
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "collector cancelled")
	is(t, waiting(app), 0)
}

func TestTheStreamsLocalGuard(t *testing.T) { // SG-3
	app, _, s := sending(t, &hubpb.RegisterAck{HubId: "hub-1", HeartbeatIntervalMs: 3000, HeartbeatTimeoutMs: 10000,
		DefaultTimeoutMs: 200, MaxTimeoutMs: 200})
	// The first part: twice the hub's bound (200 ms) plus 1 s.
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r := s.RecvRequest(t)
	o := await(t, read)
	var e *Error
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeTimeout)
	is(t, e.Message, "Timed out waiting for reply to 'orders.ListOrders'")
	within(t, o.elapsed, 1350*ms, 2500*ms)
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "no part within the local guard")

	// Between parts: the bound plus 1 s.
	read = async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r = s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	sent := time.Now()
	o = await(t, read)
	idle := time.Since(sent)
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeTimeout)
	is(t, len(o.value), 1)
	within(t, idle, 1150*ms, 1700*ms)
	is(t, s.RecvCancel(t).GetReason(), "no part within the local guard")
	is(t, waiting(app), 0)
}

func TestAStreamIsRetriedUntilItsFirstPart(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	p := RetryNeverRan
	p.InitialBackoff = 10 * ms
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}, Retry(p), Timeout(2*time.Second)))
	})
	first := s.RecvRequest(t)
	s.Send(t, fakehub.HubError(first.GetId(), hubpb.ErrorCode_NO_HANDLER, "no live instance"))
	second := s.RecvRequest(t)
	is(t, second.GetId() != first.GetId(), true)
	is(t, second.GetTimeoutMs(), uint32(2000)) // the idle timeout, on every attempt (SG-21)
	s.Send(t, line(second.GetId(), 1))
	s.Send(t, streamEnd(second.GetId(), 1, &hubpb.Error{RequestId: second.GetId(), Code: hubpb.ErrorCode_HANDLER_DISCONNECTED}))
	o := await(t, read) // after the first part nothing is retried
	is(t, len(o.value), 1)
	var e *Error
	is(t, errors.As(o.err, &e), true)
	is(t, e.Code, CodeHandlerDisconnected)
	s.Nothing(t, 200*ms, "request")
}

func TestStreamRefusesWhatItCannotSend(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	_, err := collect(Stream[OrderLine](context.Background(), app, nil))
	is(t, err != nil, true)
	_, err = collect(Stream[OrderLine](context.Background(), app, ListOrders{}, Header("traceparent", "x")))
	is(t, errors.Is(err, ErrReservedHeader), true)
	_, err = collect(Stream[OrderLine](context.Background(), nil, ListOrders{}))
	is(t, err != nil, true)
	s.Nothing(t, 200*ms, "request")
	sent := async(func() (Empty, error) { return Query[Empty](context.Background(), app, GetOrder{}) })
	r := s.RecvRequest(t) // the stream is alive
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

// buffered is how many events wait in the inbox of the stream id.
func buffered(app *App, id string) int {
	app.calls.mu.Lock()
	c := app.calls.byID[id]
	app.calls.mu.Unlock()
	if c == nil || c.inbox == nil {
		return 0
	}
	c.inbox.mu.Lock()
	defer c.inbox.mu.Unlock()
	return len(c.inbox.events)
}

func TestACancelledContextWinsOverBufferedParts(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	read := async(func() ([]OrderLine, error) {
		var got []OrderLine
		for v, err := range Stream[OrderLine](ctx, app, ListOrders{}) {
			if err != nil {
				return got, err
			}
			got = append(got, v)
			if len(got) == 1 {
				<-release // parts 2 to 5 arrive meanwhile
				cancel()
			}
		}
		return got, nil
	})
	r := s.RecvRequest(t)
	for seq := uint64(1); seq <= 5; seq++ {
		s.Send(t, line(r.GetId(), seq))
	}
	poll(t, func() bool { return buffered(app, r.GetId()) == 4 })
	close(release)
	o := await(t, read)
	is(t, len(o.value), 1) // none of the 4 buffered parts after the cancellation
	is(t, errors.Is(o.err, context.Canceled), true)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "collector cancelled")
	is(t, waiting(app), 0)
}

func TestAPanicInTheLoopBodyCancelsTheStream(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		for _, err := range Stream[OrderLine](context.Background(), app, ListOrders{}) {
			if err == nil {
				panic("loop body defect")
			}
		}
	}()
	r := s.RecvRequest(t)
	s.Send(t, line(r.GetId(), 1))
	is(t, receive(t, recovered), any("loop body defect"))
	c := s.RecvCancel(t)
	is(t, c.GetRequestId(), r.GetId())
	is(t, c.GetReason(), "collector cancelled")
	is(t, waiting(app), 0)
}

func TestEachRangeSendsTheStreamAnew(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	seq := Stream[OrderLine](context.Background(), app, ListOrders{Count: 1})
	var ids []string
	for range 2 {
		read := async(func() ([]OrderLine, error) { return collect(seq) })
		r := s.RecvRequest(t)
		ids = append(ids, r.GetId())
		s.Send(t, line(r.GetId(), 1))
		s.Send(t, streamEnd(r.GetId(), 1, nil))
		o := await(t, read)
		is(t, o.err, nil)
		is(t, len(o.value), 1)
	}
	is(t, ids[0] != ids[1], true) // a new request, with its own id
	is(t, waiting(app), 0)
}

func TestTheRolloverIgnoresWhatThisInstanceConsumes(t *testing.T) {
	ref := newStreamRef(nil)
	p := newPendingCalls()
	unary := &call{id: "u", answer: make(chan answer, 1)}
	gather := &call{id: "g", answer: make(chan answer, 1), gather: true}
	stream := &call{id: "s", inbox: newStreamInbox()} // a stream or a subscription read here
	for _, c := range []*call{unary, gather, stream} {
		c.on.Store(ref)
		p.add(c)
	}
	is(t, p.writtenTo(ref, false), 2) // the rollover waits for these two only
	is(t, p.writtenTo(ref, true), 3)  // the drain of an address that left DNS waits for the stream too
	p.remove("u")
	p.remove("g")
	is(t, p.writtenTo(ref, false), 0)
	is(t, p.writtenTo(ref, true), 1)
}
