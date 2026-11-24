package snodra

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// WatchOrders is a subscription query: the orders of Topic.
type WatchOrders struct {
	Topic string `json:"topic"`
}

func (WatchOrders) MessageType() string { return "orders.WatchOrders" }

// OrderView is the initial result of WatchOrders.
type OrderView struct {
	Topic string `json:"topic"`
	Value int    `json:"value"`
}

// OrderUpdate is an update of WatchOrders.
type OrderUpdate struct {
	Value int `json:"value"`
}

// watchOrders answers the initial result of q.
func watchOrders(_ context.Context, q WatchOrders) (OrderView, error) {
	return OrderView{Topic: q.Topic}, nil
}

// topic matches the subscriptions of WatchOrders on t.
func topic(t string) func(WatchOrders) bool { return func(q WatchOrders) bool { return q.Topic == t } }

// subscriptionRequest is a forwarded subscription target id of WatchOrders on topic t, with
// headers.
func subscriptionRequest(id, t string, headers map[string]string) *hubpb.ServerMessage {
	return fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_QUERY, Type: "orders.WatchOrders",
		Mode: hubpb.RequestMode_SUBSCRIPTION, Payload: []byte(`{"topic":"` + t + `"}`), Headers: headers})
}

// initialRequest is a forwarded unary request id of WatchOrders on topic t.
func initialRequest(id, t string) *hubpb.ServerMessage {
	return fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_QUERY, Type: "orders.WatchOrders",
		Payload: []byte(`{"topic":"` + t + `"}`)})
}

// watching serves WatchOrders on hub (with opts) and returns its Updates, the app and its first
// stream, acknowledged.
func watching(t *testing.T, hub *fakehub.Hub, guard []HandlerOption, opts ...Option) (*Updates[WatchOrders, OrderUpdate], *App, *fakehub.Stream) {
	t.Helper()
	var updates *Updates[WatchOrders, OrderUpdate]
	app, s := serving(t, hub, func(app *App) {
		u, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders, guard...)
		is(t, err, nil)
		updates = u
		is(t, HandleCommand(app, place), nil)
	}, opts...)
	return updates, app, s
}

// open opens subscription id on topic t and proves it admitted: an update emitted to it alone
// arrives as its first part.
func open(t *testing.T, u *Updates[WatchOrders, OrderUpdate], s *fakehub.Stream, id, tp string) {
	t.Helper()
	s.Send(t, subscriptionRequest(id, tp, nil))
	poll(t, func() bool { return u.Emit(topic(tp), OrderUpdate{Value: 0}) == 1 }) // once it is read
	p := nextPart(t, s)
	is(t, p.GetRequestId(), id)
	is(t, p.GetSeq(), uint64(1))
}

// alive proves s alive: a command forwarded on it is answered.
func alive(t *testing.T, s *fakehub.Stream, id string) {
	t.Helper()
	s.Send(t, command(id, "orders.PlaceOrder", `{"orderId":"o-1"}`))
	is(t, nextReply(t, s).GetRequestId(), id)
}

func TestASubscriptionRegistersOnItsTargetRequest(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil)
	s.Send(t, subscriptionRequest("w#1", "t", nil))
	s.Send(t, initialRequest("w#0", "t"))
	r := nextReply(t, s)
	is(t, r.GetRequestId(), "w#0")
	is(t, string(r.GetPayload()), `{"topic":"t","value":0}`)
	is(t, app.SubscriptionsServed(), 1)
	// Emitted right after the initial result: kept, whether the admission is decided or not.
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 1)
	is(t, updates.Emit(topic("other"), OrderUpdate{Value: 9}), 0)
	is(t, updates.Emit(nil, OrderUpdate{Value: 2}), 1) // nil matches every subscription
	p := nextPart(t, s)
	is(t, p.GetRequestId(), "w#1")
	is(t, p.GetSeq(), uint64(1))
	is(t, string(p.GetPayload()), `{"value":1}`)
	is(t, nextPart(t, s).GetSeq(), uint64(2))
	updates.Complete(topic("t"))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "w#1")
	is(t, end.GetError() == nil, true)
	is(t, end.GetParts(), uint32(2)) // the updates sent
	is(t, app.SubscriptionsServed(), 0)
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 3}), 0)
	s.Nothing(t, 200*ms, "part", "end")
	alive(t, s, "r-1")
}

// slowJWKS serves the test JWKS after delay.
func slowJWKS(t *testing.T, delay time.Duration) *httptest.Server {
	t.Helper()
	inner, _ := testJWKS(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		resp, err := http.Get(inner.URL)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestEmitsBeforeTheAdmissionAreBuffered(t *testing.T) {
	key, _ := testKeys(t)
	auth := map[string]string{"authorization": "Bearer " + mint(t, key, "k1", userClaims("alice", time.Hour, nil))}

	// Admitted after the slow JWKS: what was emitted meanwhile goes out first, in order.
	updates, app, s := watching(t, fakehub.Start(t), []HandlerOption{RequireAuthenticated()},
		JWKS(slowJWKS(t, 500*ms).URL, testIssuer))
	s.Send(t, subscriptionRequest("w#1", "t", auth))
	poll(t, func() bool { return app.SubscriptionsServed() == 1 }) // registered before its admission
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 1)
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 2}), 1)
	s.Nothing(t, 200*ms, "part", "end") // held until the admission
	p := nextPart(t, s)
	is(t, p.GetSeq(), uint64(1))
	is(t, string(p.GetPayload()), `{"value":1}`)
	is(t, nextPart(t, s).GetSeq(), uint64(2))

	// Refused after the slow JWKS: what was emitted meanwhile is dropped, the refusal is all.
	updates, app, s = watching(t, fakehub.Start(t), []HandlerOption{RequireRole("orders-admin")},
		JWKS(slowJWKS(t, 500*ms).URL, testIssuer))
	s.Send(t, subscriptionRequest("w#2", "t", auth))
	poll(t, func() bool { return app.SubscriptionsServed() == 1 })
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 1)
	end := nextEnd(t, s) // no part before it
	is(t, end.GetRequestId(), "w#2")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_PERMISSION_DENIED)
	is(t, end.GetError().GetRequestId(), "w#2")
	is(t, app.SubscriptionsServed(), 0)
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 2}), 0)
	s.Nothing(t, 200*ms, "part", "end")
}

func TestCancelRemovesASubscription(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil)
	open(t, updates, s, "w#1", "t")
	s.Send(t, fakehub.Cancel("w#1", "sender disconnected"))
	poll(t, func() bool { return app.SubscriptionsServed() == 0 })
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 0)
	updates.Complete(topic("t"))
	s.Nothing(t, 200*ms, "part", "end")
	alive(t, s, "r-1")
}

func TestBeyondTheLimitIsResourceExhausted(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil, MaxSubscriptions(2))
	open(t, updates, s, "w1#1", "a")
	open(t, updates, s, "w2#1", "b")
	s.Send(t, subscriptionRequest("w3#1", "c", nil))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "w3#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_RESOURCE_EXHAUSTED)
	is(t, end.GetError().GetMessage(), "subscription limit (2) reached") // the reference library's text
	is(t, app.SubscriptionsServed(), 2)
	is(t, updates.Emit(topic("c"), OrderUpdate{}), 0)
	// A subscription that ends frees its place.
	updates.Complete(topic("a"))
	is(t, nextEnd(t, s).GetRequestId(), "w1#1")
	open(t, updates, s, "w4#1", "c")
}

func TestFailEndsWithTheError(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil)
	open(t, updates, s, "w1#1", "a")
	open(t, updates, s, "w2#1", "b")
	open(t, updates, s, "w3#1", "c")
	updates.Fail(topic("a"), errors.New("feed closed"))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "w1#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, end.GetError().GetMessage(), "feed closed")
	is(t, end.GetError().GetRequestId(), "w1#1")
	is(t, end.GetParts(), uint32(1))
	// Always HANDLER_ERROR, with the details attached, on one line.
	updates.Fail(topic("b"), WithDetails(Unauthenticated("feed\nlost"), map[string]int{"feed": 2}))
	end = nextEnd(t, s)
	is(t, end.GetRequestId(), "w2#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, end.GetError().GetMessage(), "feed lost")
	is(t, string(end.GetError().GetDetails()), `{"feed":2}`)
	updates.Fail(topic("c"), nil)
	end = nextEnd(t, s)
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, app.SubscriptionsServed(), 0)
	updates.Fail(nil, errors.New("again")) // nothing left: nothing sent
	s.Nothing(t, 200*ms, "end")
}

func TestAUnaryRequestThatIsNotTheInitialOneIsInvalidRequest(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		_, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders)
		is(t, err, nil)
		is(t, HandleQuery(app, get), nil)
		is(t, HandleStream(app, listOrders), nil)
		is(t, HandleCommand(app, place), nil)
	})
	s.Send(t, initialRequest("x", "t"))
	r := nextReply(t, s)
	is(t, r.GetRequestId(), "x")
	is(t, r.GetError().GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
	is(t, r.GetError().GetMessage(), "orders.WatchOrders is a subscription query: send it with Subscribe (mode SUBSCRIPTION)")
	s.Send(t, initialRequest("x#1", "t")) // a scatter-gather target id other than #0
	is(t, nextReply(t, s).GetError().GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
	s.Send(t, command("c-1", "orders.WatchOrders", `{}`)) // no command handler of the type
	is(t, nextReply(t, s).GetError().GetCode(), hubpb.ErrorCode_NO_HANDLER)

	// A subscription request to a handler of another mode, of a type not served, of a command
	// type, in a stream's mode, and one whose query does not decode: one end each.
	for _, c := range []struct {
		req  *hubpb.Request
		code hubpb.ErrorCode
		msg  string
	}{
		{&hubpb.Request{Id: "w1#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetOrder", Mode: hubpb.RequestMode_SUBSCRIPTION},
			hubpb.ErrorCode_INVALID_REQUEST, "orders.GetOrder is not a subscription query handler (it answers mode UNARY)"},
		{&hubpb.Request{Id: "w2#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.ListOrders", Mode: hubpb.RequestMode_SUBSCRIPTION},
			hubpb.ErrorCode_INVALID_REQUEST, "orders.ListOrders is not a subscription query handler (it answers mode STREAMING)"},
		{&hubpb.Request{Id: "w3#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.Missing", Mode: hubpb.RequestMode_SUBSCRIPTION},
			hubpb.ErrorCode_INVALID_REQUEST, "no subscription handler for orders.Missing on this instance"},
		{&hubpb.Request{Id: "w4#1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Mode: hubpb.RequestMode_SUBSCRIPTION},
			hubpb.ErrorCode_INVALID_REQUEST, "no subscription handler for orders.PlaceOrder on this instance"},
		{&hubpb.Request{Id: "s#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.WatchOrders", Mode: hubpb.RequestMode_STREAMING, InitialCredit: 4},
			hubpb.ErrorCode_INVALID_REQUEST, "orders.WatchOrders is not a streaming query handler (it answers mode SUBSCRIPTION)"},
		{&hubpb.Request{Id: "w5#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.WatchOrders", Mode: hubpb.RequestMode_SUBSCRIPTION,
			Payload: []byte(`{"topic":7}`)}, hubpb.ErrorCode_INVALID_REQUEST, ""},
	} {
		s.Send(t, fakehub.Forward(c.req))
		end := nextEnd(t, s)
		is(t, end.GetRequestId(), c.req.GetId())
		is(t, end.GetError().GetCode(), c.code)
		if c.msg != "" {
			is(t, end.GetError().GetMessage(), c.msg)
		}
	}
	alive(t, s, "r-1")
}

func TestASubscriptionTakesNoPermit(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil, MaxConcurrent(1))
	open(t, updates, s, "w#1", "t")
	s.Nothing(t, 200*ms, "permits") // nothing to give back: it took none
	is(t, app.inflight.count(), 0)  // and no handler is counted for it
	s.Send(t, command("r-1", "orders.PlaceOrder", `{"orderId":"o-1"}`))
	is(t, nextReply(t, s).GetRequestId(), "r-1") // the only handler slot is free
	is(t, s.RecvPermits(t).GetCount(), uint32(1))
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 1)
	is(t, nextPart(t, s).GetSeq(), uint64(2))
}

func TestNothingAfterCancelForAStreamOrAGatherTarget(t *testing.T) {
	hub := fakehub.Start(t)
	started := make(chan struct{}, 1)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleStream(app, listOrders), nil)
		is(t, HandleQuery(app, func(ctx context.Context, _ GetOrder) (Receipt, error) {
			started <- struct{}{}
			<-ctx.Done()
			return Receipt{OrderID: "late"}, nil // answered after the Cancel: dropped
		}), nil)
		is(t, HandleCommand(app, place), nil)
	})
	s.Send(t, streamRequest("s#1", 1, `{"count":5}`))
	is(t, nextPart(t, s).GetSeq(), uint64(1))
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "g#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.GetOrder", Payload: []byte(`{}`)}))
	receive(t, started)
	s.Send(t, fakehub.Cancel("s#1", "max results"))
	s.Send(t, fakehub.Cancel("g#1", "max results"))
	s.Send(t, fakehub.Flow("s#1", 5))
	s.Nothing(t, 300*ms, "part", "end", "reply")
	alive(t, s, "r-1")
}

func TestShutdownEndsServedSubscriptions(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil)
	open(t, updates, s, "w#1", "t")
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 1)
	is(t, nextPart(t, s).GetSeq(), uint64(2))
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	is(t, len(s.RecvRegister(t).GetQueryTypes()), 0) // the empty Register first
	s.Ack(t, fakehub.Ack("hub-1"))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "w#1")
	is(t, end.GetError() == nil, true)
	is(t, end.GetParts(), uint32(2))
	s.HalfClosed(t, time.Second) // after the end
	s.Close(codes.OK, "")
	is(t, <-done, nil)
	is(t, app.SubscriptionsServed(), 0)
	is(t, updates.Emit(nil, OrderUpdate{}), 0)
}

func TestConcurrentEmitCancelAndComplete(t *testing.T) {
	// The emitters stop once the library has read every request and Cancel below: the handler of
	// the command sent after them runs then. And they keep at most about backlog parts unread:
	// the reply to that command comes after the parts emitted before it, which a slow machine
	// would otherwise read for longer than the test waits.
	const backlog = 2000
	var unread atomic.Int64 // parts emitted (to the subscriptions reached) and not read yet
	stop := make(chan struct{})
	halt := sync.OnceFunc(func() { close(stop) })
	var updates *Updates[WatchOrders, OrderUpdate]
	app, s := serving(t, fakehub.Start(t), func(app *App) {
		u, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders)
		is(t, err, nil)
		updates = u
		is(t, HandleCommand(app, func(ctx context.Context, p PlaceOrder) (Receipt, error) {
			halt()
			return place(ctx, p)
		}), nil)
	})
	const subs = 40
	var emitters sync.WaitGroup
	t.Cleanup(func() { halt(); emitters.Wait() }) // never left spinning by a failure
	for g := range 8 {
		emitters.Add(1)
		go func() {
			defer emitters.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if unread.Load() >= backlog {
					time.Sleep(ms) // until the test reads
					continue
				}
				unread.Add(int64(updates.Emit(nil, OrderUpdate{Value: g})))
				unread.Add(int64(updates.Emit(func(q WatchOrders) bool { return strings.HasSuffix(q.Topic, strconv.Itoa(g)) }, OrderUpdate{Value: g})))
			}
		}()
	}
	cancelled := map[string]bool{}
	for i := range subs {
		id := "w" + strconv.Itoa(i) + "#1"
		s.Send(t, subscriptionRequest(id, "t"+strconv.Itoa(i), nil))
		switch i % 3 {
		case 0:
			s.Send(t, fakehub.Cancel(id, "sender disconnected"))
			cancelled[id] = true
		case 1:
			updates.Complete(topic("t" + strconv.Itoa(i)))
		}
	}
	// The read loop takes frames in order: once this command is handled, every request and
	// Cancel above was read, and the emitters stop. Once it is answered, every subscription
	// left completes.
	s.Send(t, command("r-sync", "orders.PlaceOrder", `{"orderId":"o-1"}`))

	// Per subscription: parts numbered 1, 2, 3, ... and nothing after its one end, which
	// counts them; no end for a cancelled one (its Cancel follows its request at once, before
	// any Complete of it can match).
	ends := map[string]int{}
	seqs := map[string]uint64{}
	want := subs - len(cancelled)
	deadline := time.Now().Add(10 * time.Second)
	for len(ends) < want {
		if time.Now().After(deadline) {
			t.Fatalf("%d ends of %d within 10 s", len(ends), want)
		}
		m := s.Recv(t)
		switch {
		case m.GetReply() != nil:
			is(t, m.GetReply().GetRequestId(), "r-sync")
			emitters.Wait()
			updates.Complete(nil)
		case m.GetPart() != nil:
			unread.Add(-1)
			p := m.GetPart()
			id := p.GetRequestId()
			if ends[id] > 0 {
				t.Fatalf("a part of %s after its end", id)
			}
			if p.GetSeq() != seqs[id]+1 {
				t.Fatalf("%s: part %d after %d", id, p.GetSeq(), seqs[id])
			}
			seqs[id] = p.GetSeq()
		case m.GetEnd() != nil:
			id := m.GetEnd().GetRequestId()
			if cancelled[id] {
				t.Fatalf("an end for the cancelled %s", id)
			}
			ends[id]++
			if ends[id] > 1 {
				t.Fatalf("two ends for %s", id)
			}
			is(t, uint64(m.GetEnd().GetParts()), seqs[id])
		}
	}
	is(t, app.SubscriptionsServed(), 0)
	s.Nothing(t, 200*ms, "part", "end")
	alive(t, s, "r-1")
}

func TestHandleSubscriptionRegistersAQueryType(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	u, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders)
	is(t, err, nil)
	is(t, u != nil, true)
	_, err = HandleSubscription[GetStock, Stock, Stock](app, func(context.Context, GetStock) (Stock, error) { return Stock{}, nil }, Shared())
	is(t, err, nil)
	if _, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders); err == nil {
		t.Fatal("a subscription type registered twice was accepted")
	}
	if err := HandleQuery(app, func(context.Context, WatchOrders) (Empty, error) { return Empty{}, nil }); err == nil {
		t.Fatal("a type registered both as a subscription query and a query was accepted")
	}
	if _, err := HandleSubscription[GetOrder, Receipt, Receipt](app, nil); err == nil {
		t.Fatal("a nil initial handler was accepted")
	}
	if _, err := HandleSubscription[Message, Receipt, Receipt](app, func(context.Context, Message) (Receipt, error) { return Receipt{}, nil }); err == nil {
		t.Fatal("an interface type parameter was accepted")
	}
	if _, err := HandleSubscription[GetOrder, Receipt, Receipt](nil, get); err == nil {
		t.Fatal("a registration on a nil App was accepted")
	}
	is(t, app.Start(context.Background()) != nil, true) // the registration errors are kept

	app2 := newApp(t, Hubs(hub.Addr()))
	_, err = HandleSubscription[WatchOrders, OrderView, OrderUpdate](app2, watchOrders)
	is(t, err, nil)
	_, err = HandleSubscription[GetStock, Stock, Stock](app2, func(context.Context, GetStock) (Stock, error) { return Stock{}, nil }, Shared())
	is(t, err, nil)
	is(t, app2.Start(context.Background()), nil)
	r := hub.Next(t).RecvRegister(t)
	is(t, strings.Join(r.GetQueryTypes(), ","), "orders.WatchOrders")
	is(t, strings.Join(r.GetSharedQueryTypes(), ","), "orders.GetStock")
	if _, err := HandleSubscription[GetOrder, Receipt, Receipt](app2, func(context.Context, GetOrder) (Receipt, error) { return Receipt{}, nil }); !errors.Is(err, ErrRegistrationClosed) {
		t.Fatalf("got %v, want ErrRegistrationClosed", err)
	}
}

func TestMaxSubscriptionsMustBePositive(t *testing.T) {
	for _, n := range []int{0, -1} {
		_, err := New(Service("orders-service"), Hubs("localhost:7000"), MaxSubscriptions(n))
		if err == nil || !strings.Contains(err.Error(), "MaxSubscriptions") {
			t.Fatalf("MaxSubscriptions(%d): got %v, want an error naming it", n, err)
		}
	}
	app, err := New(Service("orders-service"), Hubs("localhost:7000"))
	is(t, err, nil)
	is(t, app.cfg.maxSubscriptions, 1024)
	app, err = New(Service("orders-service"), Hubs("localhost:7000"), MaxSubscriptions(3))
	is(t, err, nil)
	is(t, app.cfg.maxSubscriptions, 3)
}

func TestAPanickingMatchSkipsItsSubscription(t *testing.T) {
	updates, _, s := watching(t, fakehub.Start(t), nil)
	open(t, updates, s, "w1#1", "a")
	open(t, updates, s, "w2#1", "b")
	n := updates.Emit(func(q WatchOrders) bool {
		if q.Topic == "a" {
			panic("match defect")
		}
		return true
	}, OrderUpdate{Value: 1})
	is(t, n, 1)
	is(t, nextPart(t, s).GetRequestId(), "w2#1")
	// A match may emit again (it runs outside the library's lock).
	n = updates.Emit(func(q WatchOrders) bool {
		if q.Topic == "a" {
			updates.Emit(topic("b"), OrderUpdate{Value: 2})
		}
		return false
	}, OrderUpdate{Value: 3})
	is(t, n, 0)
	p := nextPart(t, s)
	is(t, p.GetRequestId(), "w2#1")
	is(t, string(p.GetPayload()), `{"value":2}`)
	// An update that does not encode (a U of a type the codec refuses) reaches nobody.
	unencodable := &Updates[WatchOrders, any]{app: updates.app, typ: updates.typ}
	is(t, unencodable.Emit(nil, func() {}), 0)
	s.Nothing(t, 200*ms, "part")
}

// BrittleWatch is a subscription query whose decoding panics on the topic "boom" and, on a topic
// with a gate in brittleGates, waits until the gate is closed: a buggy or a slow UnmarshalJSON.
type BrittleWatch struct {
	Topic string `json:"topic"`
}

func (BrittleWatch) MessageType() string { return "orders.BrittleWatch" }

// brittleGates holds the decoding of a BrittleWatch of a topic (key) until its channel closes.
var brittleGates sync.Map

func (b *BrittleWatch) UnmarshalJSON(data []byte) error {
	var raw struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Topic == "boom" {
		panic("boom in UnmarshalJSON") // a defect of the decoder
	}
	if gate, ok := brittleGates.Load(raw.Topic); ok {
		<-gate.(chan struct{})
	}
	b.Topic = raw.Topic
	return nil
}

// brittleRequest is a forwarded subscription target id of BrittleWatch on topic tp.
func brittleRequest(id, tp string) *hubpb.ServerMessage {
	return fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_QUERY, Type: "orders.BrittleWatch",
		Mode: hubpb.RequestMode_SUBSCRIPTION, Payload: []byte(`{"topic":"` + tp + `"}`)})
}

// brittle serves BrittleWatch (and PlaceOrder) on a fresh hub and returns its Updates, the app
// and its first stream, acknowledged.
func brittle(t *testing.T, opts ...Option) (*Updates[BrittleWatch, OrderUpdate], *App, *fakehub.Stream) {
	t.Helper()
	var updates *Updates[BrittleWatch, OrderUpdate]
	app, s := serving(t, fakehub.Start(t), func(app *App) {
		u, err := HandleSubscription[BrittleWatch, OrderView, OrderUpdate](app, func(_ context.Context, q BrittleWatch) (OrderView, error) {
			return OrderView{Topic: q.Topic}, nil
		})
		is(t, err, nil)
		updates = u
		is(t, HandleCommand(app, place), nil)
	}, opts...)
	return updates, app, s
}

func TestASubscriptionQueryWhoseDecodingPanicsIsAHandlerError(t *testing.T) {
	rec := &logRecorder{}
	updates, app, s := brittle(t, Logger(slog.New(rec)))
	s.Send(t, brittleRequest("b#1", "boom"))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "b#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_HANDLER_ERROR)
	is(t, end.GetError().GetMessage(), "handler panicked") // as for a unary handler; the value goes to the log
	is(t, end.GetError().GetRequestId(), "b#1")
	is(t, end.GetParts(), uint32(0))
	is(t, app.SubscriptionsServed(), 0)
	is(t, rec.count(slog.LevelError, "handler panicked"), 1)
	// The stream and the process go on: a command is answered, another subscription is served.
	alive(t, s, "r-1")
	s.Send(t, brittleRequest("b#2", "fine"))
	poll(t, func() bool { return updates.Emit(nil, OrderUpdate{Value: 1}) == 1 })
	p := nextPart(t, s)
	is(t, p.GetRequestId(), "b#2")
	is(t, p.GetSeq(), uint64(1))
}

func TestASlowSubscriptionQueryDecodingDoesNotHoldTheStream(t *testing.T) {
	gate := make(chan struct{})
	brittleGates.Store("slow", gate)
	t.Cleanup(func() { brittleGates.Delete("slow") })
	updates, app, s := brittle(t)
	s.Send(t, brittleRequest("b#1", "slow"))
	poll(t, func() bool { return app.SubscriptionsServed() == 1 }) // registered at once, before its decoding
	// The stream's loop is free while the query decodes: a command is answered meanwhile.
	alive(t, s, "r-1")
	// An update emitted after the request was read still reaches it: Emit waits for the decoding.
	emitted := make(chan int, 1)
	go func() { emitted <- updates.Emit(topicOf("slow"), OrderUpdate{Value: 1}) }()
	select {
	case n := <-emitted:
		t.Fatalf("Emit returned %d before the query was decoded", n)
	case <-time.After(100 * ms):
	}
	close(gate)
	is(t, receive(t, emitted), 1)
	p := nextPart(t, s)
	is(t, p.GetRequestId(), "b#1")
	is(t, string(p.GetPayload()), `{"value":1}`)
	// A cancel while the query decodes ends it without a word.
	gate2 := make(chan struct{})
	brittleGates.Store("slow-2", gate2)
	t.Cleanup(func() { brittleGates.Delete("slow-2") })
	s.Send(t, brittleRequest("b#2", "slow-2"))
	poll(t, func() bool { return app.SubscriptionsServed() == 2 })
	s.Send(t, fakehub.Cancel("b#2", "sender disconnected"))
	poll(t, func() bool { return app.SubscriptionsServed() == 1 })
	close(gate2)
	is(t, updates.Emit(topicOf("slow-2"), OrderUpdate{Value: 2}), 0)
	s.Nothing(t, 200*ms, "part", "end")
}

// topicOf matches the subscriptions of BrittleWatch on tp.
func topicOf(tp string) func(BrittleWatch) bool {
	return func(q BrittleWatch) bool { return q.Topic == tp }
}

// The updates of a subscription query are of one type, U, as its subscribers read them
// (Subscribe[I, U]): Emit takes a U.
func TestUpdatesAreTyped(t *testing.T) {
	var updates *Updates[WatchOrders, OrderUpdate]
	_, s := serving(t, fakehub.Start(t), func(app *App) {
		u, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders)
		is(t, err, nil)
		updates = u
	})
	s.Send(t, subscriptionRequest("w#1", "t", nil))
	poll(t, func() bool { return updates.Emit(topic("t"), OrderUpdate{Value: 4}) == 1 })
	is(t, string(nextPart(t, s).GetPayload()), `{"value":4}`)
}

// A subscription request with the id of one open here is refused, the open one kept, as in the
// reference library.
func TestASecondSubscriptionWithAnOpenIDIsRefused(t *testing.T) {
	updates, app, s := watching(t, fakehub.Start(t), nil)
	open(t, updates, s, "w#1", "t")
	s.Send(t, subscriptionRequest("w#1", "t", nil))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "w#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_INVALID_REQUEST)
	is(t, end.GetError().GetMessage(), "subscription w#1 is already open")
	is(t, app.SubscriptionsServed(), 1)
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 5}), 1) // the open one goes on
	p := nextPart(t, s)
	is(t, p.GetRequestId(), "w#1")
	is(t, p.GetSeq(), uint64(2))
}

// The limit is decided when the request is read, before its token, its handler or its query.
func TestTheLimitIsDecidedFirst(t *testing.T) {
	updates, _, s := watching(t, fakehub.Start(t), nil, MaxSubscriptions(1))
	open(t, updates, s, "w#1", "t")
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "w2#1", Kind: hubpb.RequestKind_QUERY, Type: "orders.WatchOrders",
		Mode: hubpb.RequestMode_SUBSCRIPTION, Payload: []byte(`{"topic":7}`), Headers: map[string]string{"authorization": "Bearer abc"}}))
	end := nextEnd(t, s)
	is(t, end.GetRequestId(), "w2#1")
	is(t, end.GetError().GetCode(), hubpb.ErrorCode_RESOURCE_EXHAUSTED)
}

// The subscribers are told at once: the served subscriptions end before the handlers in flight
// are waited for, as in the reference library.
func TestShutdownEndsTheSubscriptionsBeforeTheHandlerDrain(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var updates *Updates[WatchOrders, OrderUpdate]
	app, s := serving(t, fakehub.Start(t), func(app *App) {
		u, err := HandleSubscription[WatchOrders, OrderView, OrderUpdate](app, watchOrders)
		is(t, err, nil)
		updates = u
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
			close(started)
			<-release
			return Empty{}, nil
		}), nil)
	})
	open(t, updates, s, "w#1", "t")
	s.Send(t, command("c-1", "orders.PlaceOrder", `{}`))
	<-started
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	is(t, len(s.RecvRegister(t).GetQueryTypes()), 0)
	s.Ack(t, fakehub.Ack("hub-1"))
	end := nextEnd(t, s) // while the handler still runs
	is(t, end.GetRequestId(), "w#1")
	close(release)
	is(t, nextReply(t, s).GetRequestId(), "c-1")
	s.HalfClosed(t, time.Second)
	s.Close(codes.OK, "")
	is(t, <-done, nil)
}
