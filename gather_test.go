package snodra

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// GetStock is a query many services answer.
type GetStock struct {
	SKU string `json:"sku"`
}

func (GetStock) MessageType() string { return "orders.GetStock" }

// Stock is an answer to GetStock.
type Stock struct {
	Count int `json:"count"`
}

// partOf is a ReplyPart of gather id from service/instance: a payload, or e when set.
func partOf(id string, seq uint64, service, instance, payload string, e *hubpb.Error) *hubpb.ServerMessage {
	p := &hubpb.ReplyPart{RequestId: id, Seq: seq, ResponderService: service, ResponderInstance: instance, Error: e}
	if e == nil {
		p.Payload = []byte(payload)
	}
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Part{Part: p}}
}

// endOf is the ReplyEnd of gather id.
func endOf(id string, parts, failed, timedOut uint32) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_End{End: &hubpb.ReplyEnd{
		RequestId: id, Parts: parts, Failed: failed, TimedOut: timedOut,
	}}}
}

func TestGatherCollectsThePartsUntilTheEnd(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	gathered := async(func() (*Gathered[Stock], error) {
		return Gather[Stock](context.Background(), app, GetStock{SKU: "sku-1"}, RoutingKey("ignored"))
	})
	r := s.RecvRequest(t)
	is(t, r.GetMode(), hubpb.RequestMode_SCATTER_GATHER)
	is(t, r.GetKind(), hubpb.RequestKind_QUERY)
	is(t, r.GetType(), "orders.GetStock")
	is(t, r.GetMaxResults(), uint32(0))
	is(t, r.GetRoutingKey(), "") // a gather goes to every target: unkeyed, as in the reference library
	is(t, r.GetTimeoutMs(), uint32(0))
	is(t, string(r.GetPayload()), `{"sku":"sku-1"}`)
	id := r.GetId()
	s.Send(t, partOf(id, 0, "B", "b2", `{"count":2}`, nil))
	s.Send(t, partOf(id, 1, "A", "a1", "", &hubpb.Error{RequestId: id, Code: hubpb.ErrorCode_HANDLER_ERROR,
		Message: "out of stock", Details: []byte(`{"sku":"sku-1"}`)}))
	s.Send(t, partOf(id, 2, "B", "b1", `{"count":1}`, nil))
	s.Send(t, replyTo(id, `{"count":9}`)) // a Reply is no answer to a gather: ignored
	s.Send(t, endOf(id, 3, 1, 0))
	o := await(t, gathered)
	is(t, o.err, nil)
	g := o.value
	is(t, len(g.Parts), 3) // in arrival order
	is(t, g.Parts[0].Service, "B")
	is(t, g.Parts[0].Instance, "b2")
	is(t, g.Parts[0].Err, nil)
	is(t, g.Parts[0].Value.Count, 2)
	var e *Error
	if !errors.As(g.Parts[1].Err, &e) {
		t.Fatalf("part 1: got %v, want an *Error", g.Parts[1].Err)
	}
	is(t, e.Code, CodeHandlerError)
	is(t, e.Message, "out of stock")
	is(t, string(e.Details), `{"sku":"sku-1"}`)
	is(t, g.Parts[1].Service, "A")
	is(t, g.Parts[1].Instance, "a1")
	is(t, g.Parts[2].Instance, "b1")
	is(t, g.Parts[2].Value.Count, 1)
	is(t, g.Failed, 1)
	is(t, g.TimedOut, 0)
	is(t, g.Targets, 3)
	is(t, waiting(app), 0)
}

func TestGatherSendsMaxResults(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	gathered := async(func() (*Gathered[Stock], error) {
		return Gather[Stock](context.Background(), app, GetStock{}, MaxResults(2))
	})
	r := s.RecvRequest(t)
	is(t, r.GetMaxResults(), uint32(2))
	// The hub ends the gather after the second success and cancels the rest: they are in
	// neither count, so Targets is the parts sent (5 targets here, Targets 2).
	s.Send(t, partOf(r.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	s.Send(t, partOf(r.GetId(), 1, "B", "b2", `{"count":2}`, nil))
	s.Send(t, endOf(r.GetId(), 2, 0, 0))
	o := await(t, gathered)
	is(t, o.err, nil)
	is(t, len(o.value.Parts), 2)
	is(t, o.value.TimedOut, 0)
	is(t, o.value.Targets, 2)
}

func TestGatherCountsTheTimedOutTargets(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	gathered := async(func() (*Gathered[Stock], error) {
		return Gather[Stock](context.Background(), app, GetStock{}, Timeout(1500*ms))
	})
	r := s.RecvRequest(t)
	is(t, r.GetMaxResults(), uint32(0))
	is(t, r.GetTimeoutMs(), uint32(1500)) // the deadline of the whole gather
	s.Send(t, partOf(r.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	s.Send(t, endOf(r.GetId(), 1, 0, 2)) // the deadline passed with two targets silent
	o := await(t, gathered)
	is(t, o.err, nil)
	is(t, len(o.value.Parts), 1)
	is(t, o.value.Failed, 0)
	is(t, o.value.TimedOut, 2)
	is(t, o.value.Targets, 3)
}

func TestFramesAfterTheGatherEndedAreIgnored(t *testing.T) {
	app, _, s := sending(t, &hubpb.RegisterAck{HubId: "hub-1", HeartbeatIntervalMs: 3000, HeartbeatTimeoutMs: 10000,
		DefaultTimeoutMs: 200, MaxTimeoutMs: 200})
	// After the end: a part and a second end of the same gather change nothing.
	gathered := async(func() (*Gathered[Stock], error) { return Gather[Stock](context.Background(), app, GetStock{}) })
	r := s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	s.Send(t, endOf(r.GetId(), 1, 0, 0))
	o := await(t, gathered)
	is(t, o.err, nil)
	is(t, len(o.value.Parts), 1)
	s.Send(t, partOf(r.GetId(), 1, "B", "b2", `{"count":2}`, nil))
	s.Send(t, endOf(r.GetId(), 2, 0, 0)) // harmless: the clean gather at the end proves it

	// After the local guard fired (the hub's bound, 200 ms, plus 1 s).
	gathered = async(func() (*Gathered[Stock], error) { return Gather[Stock](context.Background(), app, GetStock{}) })
	guarded := s.RecvRequest(t)
	o = await(t, gathered)
	var e *Error
	is(t, errors.As(o.err, &e), true)
	is(t, e.Code, CodeTimeout)
	s.Send(t, partOf(guarded.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	s.Send(t, endOf(guarded.GetId(), 1, 0, 0))

	// After the caller cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	gathered = async(func() (*Gathered[Stock], error) { return Gather[Stock](ctx, app, GetStock{}) })
	cancelled := s.RecvRequest(t)
	cancel()
	o = await(t, gathered)
	is(t, errors.Is(o.err, context.Canceled), true)
	s.Send(t, partOf(cancelled.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	s.Send(t, endOf(cancelled.GetId(), 1, 0, 0))

	// None of them reached a later gather, and nothing is left waiting.
	gathered = async(func() (*Gathered[Stock], error) { return Gather[Stock](context.Background(), app, GetStock{}) })
	later := s.RecvRequest(t)
	s.Send(t, endOf(later.GetId(), 0, 0, 0))
	o = await(t, gathered)
	is(t, o.err, nil)
	is(t, len(o.value.Parts), 0)
	is(t, waiting(app), 0)
}

func TestAPartThatDoesNotDecodeIsAFailedPart(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	gathered := async(func() (*Gathered[Stock], error) { return Gather[Stock](context.Background(), app, GetStock{}) })
	r := s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 0, "B", "b1", `{"count":"many"}`, nil))
	s.Send(t, partOf(r.GetId(), 1, "B", "b2", ``, nil)) // an empty payload reads as {}
	s.Send(t, endOf(r.GetId(), 2, 0, 0))
	o := await(t, gathered)
	is(t, o.err, nil)
	is(t, len(o.value.Parts), 2)
	if o.value.Parts[0].Err == nil {
		t.Fatal("a payload that does not decode is a failed part")
	}
	var e *Error
	is(t, errors.As(o.value.Parts[0].Err, &e), false) // a local failure, not the target's error
	is(t, o.value.Parts[1].Err, nil)
	is(t, o.value.Failed, 1) // counted from the parts, as in the reference library
}

func TestGatherWithoutTargetsIsNoHandler(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	gathered := async(func() (*Gathered[Stock], error) { return Gather[Stock](context.Background(), app, GetStock{}) })
	r := s.RecvRequest(t)
	s.Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "no live instance"))
	o := await(t, gathered)
	is(t, o.value, (*Gathered[Stock])(nil))
	var e *Error
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeNoHandler)
	is(t, waiting(app), 0)
}

func TestGatherOnALostStreamIsOutcomeUnknown(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	gathered := async(func() (*Gathered[Stock], error) { return Gather[Stock](ctxWithin(t, 5*time.Second), app, GetStock{}) })
	r := s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 0, "B", "b1", `{"count":1}`, nil)) // dropped with the gather
	s.Close(codes.Unavailable, "restart")
	o := await(t, gathered)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	is(t, errors.Is(o.err, ErrNeverSent), false)
	is(t, o.value, (*Gathered[Stock])(nil))
	is(t, waiting(app), 0)
}

func TestGatherIsBoundedByTheLocalGuard(t *testing.T) {
	app, _, s := sending(t, &hubpb.RegisterAck{HubId: "hub-1", HeartbeatIntervalMs: 3000, HeartbeatTimeoutMs: 10000,
		DefaultTimeoutMs: 200, MaxTimeoutMs: 200})
	gathered := async(func() (*Gathered[Stock], error) { return Gather[Stock](context.Background(), app, GetStock{}) })
	r := s.RecvRequest(t)
	s.Send(t, partOf(r.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	o := await(t, gathered) // the hub never ends it: its bound (200 ms) plus 1 s
	var e *Error
	if !errors.As(o.err, &e) {
		t.Fatalf("got %v, want an *Error", o.err)
	}
	is(t, e.Code, CodeTimeout)
	is(t, e.Message, "Timed out waiting for reply to 'orders.GetStock'")
	within(t, o.elapsed, 1100*ms, 2500*ms)
	is(t, waiting(app), 0)
}

func TestGatherIsRetriedLikeAQuery(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	p := RetryNeverRan
	p.InitialBackoff = 10 * ms
	gathered := async(func() (*Gathered[Stock], error) {
		return Gather[Stock](context.Background(), app, GetStock{}, Retry(p), Timeout(3*time.Second))
	})
	first := s.RecvRequest(t)
	s.Send(t, fakehub.HubError(first.GetId(), hubpb.ErrorCode_NO_HANDLER, "no live instance"))
	second := s.RecvRequest(t)
	is(t, second.GetId() != first.GetId(), true) // a new gather, collected from scratch
	is(t, second.GetMode(), hubpb.RequestMode_SCATTER_GATHER)
	s.Send(t, partOf(second.GetId(), 0, "B", "b1", `{"count":1}`, nil))
	s.Send(t, endOf(second.GetId(), 1, 0, 0))
	o := await(t, gathered)
	is(t, o.err, nil)
	is(t, len(o.value.Parts), 1)
}

func TestGatherRefusesWhatItCannotSend(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	_, err := Gather[Stock](context.Background(), app, nil)
	is(t, err != nil, true)
	_, err = Gather[Stock](context.Background(), app, GetStock{}, Header("traceparent", "x"))
	is(t, errors.Is(err, ErrReservedHeader), true)
	_, err = Gather[Stock](context.Background(), nil, GetStock{})
	is(t, err != nil, true)
	s.Nothing(t, 200*ms, "request")
	sent := async(func() (Empty, error) { return Query[Empty](context.Background(), app, GetStock{}) })
	r := s.RecvRequest(t) // the stream is alive: a later request is written
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

func TestASharedQueryIsRegisteredAsShared(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	is(t, HandleQuery(app, func(context.Context, GetStock) (Stock, error) { return Stock{}, nil }, Shared()), nil)
	is(t, HandleQuery(app, get), nil)
	is(t, app.Start(context.Background()), nil)
	r := hub.Next(t).RecvRegister(t)
	is(t, len(r.GetSharedQueryTypes()), 1)
	is(t, r.GetSharedQueryTypes()[0], "orders.GetStock")
	is(t, len(r.GetQueryTypes()), 1)
	is(t, r.GetQueryTypes()[0], "orders.GetOrder") // not in query_types
	is(t, len(r.GetCommandTypes()), 0)
}

func TestACommandCannotBeShared(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	err := HandleCommand(app, place, Shared())
	if err == nil {
		t.Fatal("a shared command was accepted")
	}
	if app.Start(context.Background()) == nil {
		t.Fatal("Start ignored the registration error")
	}
}

func TestASharedQueryAnswersPointQueries(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleQuery(app, func(_ context.Context, q GetStock) (Stock, error) { return Stock{Count: len(q.SKU)}, nil }, Shared()), nil)
	})
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "q-1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetStock", Payload: []byte(`{"sku":"abc"}`)}))
	r := nextReply(t, s)
	is(t, r.GetRequestId(), "q-1")
	is(t, string(r.GetPayload()), `{"count":3}`)
}
