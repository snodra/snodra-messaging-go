package snodra

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/rendezvous"
)

// fastAck is the RegisterAck of hubID with heartbeats every 100 ms, so that a heartbeat proves
// a stream alive quickly.
func fastAck(hubID string) *hubpb.RegisterAck {
	a := fakehub.Ack(hubID)
	a.HeartbeatIntervalMs = 100
	return a
}

// registered takes hub's next stream, reads its Register and the heartbeat after it, and
// acknowledges it as hubID.
func registered(t *testing.T, hub *fakehub.Hub, hubID string) (*fakehub.Stream, *hubpb.Register) {
	t.Helper()
	s := hub.Next(t)
	r := s.RecvRegister(t)
	s.Recv(t) // the heartbeat after the Register (see fakehub.Stream.Ack)
	s.Ack(t, fastAck(hubID))
	return s, r
}

// hubIDsAre waits (bounded) until the app's acknowledged hubs are want, sorted.
func hubIDsAre(t *testing.T, app *App, want ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * s)
	for {
		got := app.HubIDs()
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("hub ids %v, want %v", got, want)
		}
		time.Sleep(5 * ms)
	}
}

// isAlive fails the test unless a heartbeat arrives on s (fastAck: within 100 ms or so).
func isAlive(t *testing.T, s *fakehub.Stream) {
	t.Helper()
	deadline := time.Now().Add(2 * s_)
	for time.Now().Before(deadline) {
		if _, ok := s.Recv(t).GetMessage().(*hubpb.ClientMessage_Heartbeat); ok {
			return
		}
	}
	t.Fatal("no heartbeat: the stream is not alive")
}

// s_ is a second where s names a stream.
const s_ = time.Second

// twoHubs starts an app on two fake hubs, both acknowledged (hub-1 and hub-2). With
// MaxConcurrent(1) each stream offers 1 permit, so a hub that leaves tops nothing up: no Permits
// frame gets in the way of the Recv helpers.
func twoHubs(t *testing.T, opts ...Option) (*App, [2]*fakehub.Hub, [2]*fakehub.Stream) {
	t.Helper()
	h := [2]*fakehub.Hub{fakehub.Start(t), fakehub.Start(t)}
	app := startApp(t, append([]Option{Hubs(h[0].Addr(), h[1].Addr()), MaxConcurrent(1)}, opts...)...)
	s1, _ := registered(t, h[0], "hub-1")
	s2, _ := registered(t, h[1], "hub-2")
	hubIDsAre(t, app, "hub-1", "hub-2")
	return app, h, [2]*fakehub.Stream{s1, s2}
}

// keyFor is a routing key whose rendezvous winner among hub-1 and hub-2 is winner.
func keyFor(t *testing.T, winner string) string {
	t.Helper()
	return keyForIDs(t, "hub-1", "hub-2", winner)
}

// roundTrip sends a command and answers it on s, the stream it must arrive on.
func roundTrip(t *testing.T, app *App, s *fakehub.Stream, opts ...SendOption) *hubpb.Request {
	t.Helper()
	sent := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*s_), app, PlaceOrder{}, opts...) })
	r := s.RecvRequest(t)
	s.Send(t, replyTo(r.GetId(), "{}"))
	is(t, await(t, sent).err, nil)
	return r
}

func TestUnkeyedRequestsGoRoundRobinOverAcknowledgedHubs(t *testing.T) {
	app, h, st := twoHubs(t)
	for i := range 4 {
		roundTrip(t, app, st[i%2]) // hub-1, hub-2, hub-1, hub-2
	}
	// hub-2 reconnecting (its new stream not acknowledged): every request goes to hub-1.
	st[1].Close(codes.Unavailable, "restart")
	hubIDsAre(t, app, "hub-1")
	h[1].Next(t).RecvRegister(t)
	for range 4 {
		roundTrip(t, app, st[0])
	}
}

func TestAKeyedRequestGoesToTheRendezvousWinner(t *testing.T) {
	app, _, st := twoHubs(t)
	is(t, roundTrip(t, app, st[1], RoutingKey("order-42")).GetRoutingKey(), "order-42")
	roundTrip(t, app, st[1], RoutingKey("order-42")) // the same hub every time
	roundTrip(t, app, st[0], RoutingKey(keyFor(t, "hub-1")))
	roundTrip(t, app, st[1], RoutingKey(keyFor(t, "hub-2")))
}

func TestPickSkipsADrainingConnection(t *testing.T) {
	app, _, st := twoHubs(t)
	app.pool.all()[0].setState(stateDraining) // hub-1 leaving
	for range 3 {
		roundTrip(t, app, st[1])
	}
	roundTrip(t, app, st[1], RoutingKey(keyFor(t, "hub-1")))
	// With every hub leaving (the shutdown), the leaving hubs still take what is sent.
	app.pool.all()[1].setState(stateDraining)
	c, err := app.pool.pick("")
	is(t, err, nil)
	is(t, c != nil, true)
}

func TestOneStreamPerHubID(t *testing.T) {
	a, b, c := fakehub.Start(t), fakehub.Start(t), fakehub.Start(t)
	app := startApp(t, Hubs(a.Addr(), b.Addr(), c.Addr()))
	sa, ra := registered(t, a, "hub-x")
	_, rb := registered(t, b, "hub-y")
	hubIDsAre(t, app, "hub-x", "hub-y")
	sc, rc := registered(t, c, "hub-x") // another target of hub-x: closed, never made the owner
	sc.Ended(t, 2*s_)
	// On one real hub the alias's Register replaced the owner's session: the hub ends the
	// owner's stream, and the owner reconnects, once.
	sa.Close(codes.Unavailable, "session replaced")
	sa2, _ := registered(t, a, "hub-x")
	hubIDsAre(t, app, "hub-x", "hub-y")
	isAlive(t, sa2)
	a.NoStreamWithin(t, 500*ms)
	c.NoStreamWithin(t, 500*ms) // the duplicate is closed for good (SG-22)
	is(t, strings.Join(app.HubIDs(), ","), "hub-x,hub-y")
	// The Registers are identical apart from initial_permits (each stream's share).
	for _, r := range []*hubpb.Register{ra, rb, rc} {
		is(t, r.GetInitialPermits(), uint32(86)) // ceil(256/3): three hubs on their first attempt
		r.InitialPermits = 0
	}
	is(t, proto.Equal(ra, rb), true)
	is(t, proto.Equal(ra, rc), true)
}

func TestPermitsAreSplitAndToppedUp(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	app := startApp(t, Hubs(a.Addr(), b.Addr()), MaxConcurrent(5))
	sa, ra := registered(t, a, "hub-a")
	_, rb := registered(t, b, "hub-b")
	is(t, ra.GetInitialPermits(), uint32(3))
	is(t, rb.GetInitialPermits(), uint32(3))
	hubIDsAre(t, app, "hub-a", "hub-b")
	// hub-b gone: hub-a takes its share.
	b.Down()
	p := sa.RecvPermits(t)
	is(t, p.GetCount(), uint32(2))
	is(t, p.GetGrant(), uint32(5))
	// hub-b back: its new stream offers ceil(5/2); nothing is revoked from hub-a.
	b.Up()
	_, rb = registered(t, b, "hub-b")
	is(t, rb.GetInitialPermits(), uint32(3))
	hubIDsAre(t, app, "hub-a", "hub-b")
	// hub-b gone again: hub-a offered 5 already, it gets nothing more.
	b.Down()
	hubIDsAre(t, app, "hub-a")
	sa.Nothing(t, 300*ms, "permits")
	isAlive(t, sa)
	is(t, cap(app.slots), 5) // the local bound stays MaxConcurrent
}

func TestAFailedFirstAttemptShrinksN(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	b.Down() // refuses TCP from the start
	app := startApp(t, Hubs(a.Addr(), b.Addr()), MaxConcurrent(5))
	sa, ra := registered(t, a, "hub-a")
	hubIDsAre(t, app, "hub-a")
	offered := ra.GetInitialPermits()
	if offered == 3 { // hub-b's first attempt was not over when the Register was written
		p := sa.RecvPermits(t)
		is(t, p.GetGrant(), uint32(5))
		offered += p.GetCount()
	}
	is(t, offered, uint32(5))
	sa.Nothing(t, 300*ms, "permits")
	isAlive(t, sa)
}

// blockFirstRequest holds the first request the writers meet in the outbound buffer until its
// stream ends (it is never written) and passes every later one.
func blockFirstRequest(held chan<- *hubpb.Request) Option {
	var once atomic.Bool
	return withWriteHook(func(ctx context.Context, m *hubpb.ClientMessage) {
		if r := m.GetRequest(); r != nil && once.CompareAndSwap(false, true) {
			held <- r
			<-ctx.Done()
		}
	})
}

func TestUnwrittenRequestMovesToAnotherHubWithTheSameID(t *testing.T) {
	held := make(chan *hubpb.Request, 1)
	app, _, st := twoHubs(t, blockFirstRequest(held))
	sent := async(func() (Receipt, error) {
		return Send[Receipt](ctxWithin(t, 5*s_), app, PlaceOrder{}, RoutingKey(keyFor(t, "hub-1")))
	})
	first := receive(t, held) // in hub-1's outbound buffer, not written
	st[0].Close(codes.Unavailable, "restart")
	r := st[1].RecvRequest(t)
	is(t, r.GetId(), first.GetId())
	st[1].Send(t, replyTo(r.GetId(), `{"orderId":"o-1","total":3}`))
	o := await(t, sent)
	is(t, o.err, nil)
	is(t, o.value.Total, 3)
	is(t, waiting(app), 0)
}

func TestAnUnwrittenRequestMovesAtMostEightTimes(t *testing.T) { // SG-10
	held := make(chan *hubpb.Request, 16)
	h := [2]*fakehub.Hub{fakehub.Start(t), fakehub.Start(t)}
	app := startApp(t, Hubs(h[0].Addr(), h[1].Addr()), MaxConcurrent(1), withWriteHook(func(ctx context.Context, m *hubpb.ClientMessage) {
		if r := m.GetRequest(); r != nil {
			held <- r // never written: held until its stream ends
			<-ctx.Done()
		}
	}))
	st := [2]*fakehub.Stream{}
	st[0], _ = registered(t, h[0], "hub-1")
	st[1], _ = registered(t, h[1], "hub-2")
	hubIDsAre(t, app, "hub-1", "hub-2")
	sent := async(func() (Empty, error) {
		return Send[Empty](ctxWithin(t, 20*s_), app, PlaceOrder{}, RoutingKey(keyFor(t, "hub-1")))
	})
	id := receive(t, held).GetId()
	at := 0 // the hub whose buffer holds the request
	for lost := 1; lost <= 9; lost++ {
		st[at].Close(codes.Unavailable, "restart")
		if lost == 9 {
			break
		}
		is(t, receive(t, held).GetId(), id) // moved to the other hub, same id
		at = 1 - at
		st[1-at], _ = registered(t, h[1-at], fmt.Sprintf("hub-%d", 2-at)) // the lost one is back
		hubIDsAre(t, app, "hub-1", "hub-2")
	}
	o := await(t, sent)
	is(t, errors.Is(o.err, ErrNeverSent), true)
	is(t, strings.Contains(o.err.Error(), "9 hub streams lost"), true)
	is(t, waiting(app), 0)
}

func TestNoHandlerIsTriedOnTheOtherHubs(t *testing.T) {
	app, _, st := twoHubs(t)
	// hub-2 (the winner of order-42) has no handler: the same request goes to hub-1.
	sent := async(func() (Receipt, error) {
		return Send[Receipt](ctxWithin(t, 5*s_), app, PlaceOrder{}, RoutingKey("order-42"), Timeout(2*s_))
	})
	r := st[1].RecvRequest(t)
	st[1].Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "no handler"))
	r1 := st[0].RecvRequest(t)
	is(t, r1.GetId(), r.GetId())
	if r1.GetTimeoutMs() == 0 || r1.GetTimeoutMs() > 2000 {
		t.Fatalf("the second hub got timeout_ms %d, want what is left of 2000", r1.GetTimeoutMs())
	}
	st[0].Send(t, replyTo(r1.GetId(), `{"total":7}`))
	o := await(t, sent)
	is(t, o.err, nil)
	is(t, o.value.Total, 7)
	// Both hubs NO_HANDLER: the last answer comes back at once.
	sent = async(func() (Receipt, error) {
		return Send[Receipt](ctxWithin(t, 5*s_), app, PlaceOrder{}, RoutingKey("order-42"))
	})
	r = st[1].RecvRequest(t)
	st[1].Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "no handler"))
	r1 = st[0].RecvRequest(t)
	is(t, r1.GetId(), r.GetId())
	last := time.Now()
	st[0].Send(t, fakehub.HubError(r1.GetId(), hubpb.ErrorCode_NO_HANDLER, "no handler"))
	o = await(t, sent)
	within(t, time.Since(last), 0, 100*ms)
	var e *Error
	is(t, errors.As(o.err, &e), true)
	is(t, e.Code, CodeNoHandler)
	st[1].Nothing(t, 200*ms, "request") // each hub at most once
	isAlive(t, st[1])
	is(t, waiting(app), 0)
}

func TestAGatherIsNotWalkedOverTheHubsOnNoHandler(t *testing.T) {
	app, _, st := twoHubs(t)
	got := async(func() (*Gathered[Receipt], error) { return Gather[Receipt](ctxWithin(t, 5*s_), app, GetOrder{}) })
	r := st[0].RecvRequest(t)
	st[0].Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "no handler"))
	var e *Error
	is(t, errors.As(await(t, got).err, &e), true)
	is(t, e.Code, CodeNoHandler)
	st[1].Nothing(t, 200*ms, "request")
	isAlive(t, st[1])
}

func TestStreamsStayOnTheirHub(t *testing.T) {
	app, _, st := twoHubs(t)
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	r := st[0].RecvRequest(t)
	st[0].Send(t, line(r.GetId(), 1))
	st[0].Close(codes.Unavailable, "restart")
	o := await(t, read)
	is(t, len(o.value), 1)
	is(t, errors.Is(o.err, ErrOutcomeUnknown), true)
	st[1].Nothing(t, 300*ms, "request") // never moved
	isAlive(t, st[1])
	is(t, waiting(app), 0)
}

func TestAnUnwrittenStreamMovesToAnotherHub(t *testing.T) {
	held := make(chan *hubpb.Request, 1)
	app, _, st := twoHubs(t, blockFirstRequest(held))
	read := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	first := receive(t, held)
	st[0].Close(codes.Unavailable, "restart")
	r := st[1].RecvRequest(t)
	is(t, r.GetId(), first.GetId())
	is(t, r.GetMode(), hubpb.RequestMode_STREAMING)
	st[1].Send(t, line(r.GetId(), 1))
	st[1].Send(t, streamEnd(r.GetId(), 1, nil))
	o := await(t, read)
	is(t, o.err, nil)
	is(t, len(o.value), 1)
	is(t, waiting(app), 0)
}

func TestAnUnwrittenSubscriptionMovesToAnotherHub(t *testing.T) {
	held := make(chan *hubpb.Request, 1)
	app, _, st := twoHubs(t, blockFirstRequest(held))
	sub := subscribing(app, context.Background())
	first := receive(t, held)
	st[0].Close(codes.Unavailable, "restart")
	r := st[1].RecvRequest(t)
	is(t, r.GetId(), first.GetId())
	is(t, r.GetMode(), hubpb.RequestMode_SUBSCRIPTION)
	st[1].Send(t, partOf(r.GetId(), 1, "B", "b1", `{"topic":"t","value":4}`, nil))
	o := await(t, sub)
	is(t, o.err, nil)
	is(t, o.value.Initial.Value, 4)
	o.value.Close()
	is(t, st[1].RecvCancel(t).GetRequestId(), r.GetId()) // the hub it moved to
}

func TestInstanceStatusOnEveryStream(t *testing.T) {
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(2)
	h := [2]*fakehub.Hub{fakehub.Start(t), fakehub.Start(t)}
	app := newApp(t, Hubs(h[0].Addr(), h[1].Addr()), StatusInterval(200*ms))
	is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
		started.Done()
		<-release
		return Empty{}, nil
	}), nil)
	is(t, app.Start(context.Background()), nil)
	defer close(release)
	s1, _ := registered(t, h[0], "hub-1")
	s2, _ := registered(t, h[1], "hub-2")
	hubIDsAre(t, app, "hub-1", "hub-2")
	s1.Send(t, command("c-1", "orders.PlaceOrder", "{}"))
	s1.Send(t, command("c-2", "orders.PlaceOrder", "{}"))
	started.Wait()
	busy := time.Now()
	time.Sleep(900 * ms) // the observation window: several reports
	for _, st := range []*fakehub.Stream{s1, s2} {
		times, all := st.StatusTimes(), st.Statuses()
		var during []uint32
		for i, at := range times {
			if i > 0 {
				if gap := at.Sub(times[i-1]); gap > 260*ms {
					t.Fatalf("stream %d: a gap of %v between two InstanceStatus", st.Index, gap)
				}
			}
			if at.After(busy.Add(250 * ms)) {
				during = append(during, all[i].GetHandlersInFlight())
				ids := slices.Clone(all[i].GetConnectedHubIds())
				slices.Sort(ids)
				is(t, strings.Join(ids, ","), "hub-1,hub-2")
				is(t, len(all[i].GetProcessors()), 0)
			}
		}
		if len(during) < 3 || slices.ContainsFunc(during, func(n uint32) bool { return n != 2 }) {
			t.Fatalf("stream %d: handlers_in_flight %v while two handlers ran", st.Index, during)
		}
		if tail := time.Since(times[len(times)-1]); tail > 260*ms {
			t.Fatalf("stream %d: no InstanceStatus for %v", st.Index, tail)
		}
	}
}

func TestDNSTargets(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	var mu sync.Mutex
	answer := []string{a.Addr()}
	resolved := make(chan struct{}, 16)
	resolve := func(_ context.Context, host, port string) ([]string, error) {
		is(t, host+":"+port, "hubs.example:7000")
		mu.Lock()
		defer mu.Unlock()
		select {
		case resolved <- struct{}{}:
		default:
		}
		return slices.Clone(answer), nil
	}
	tm := scaled()
	tm.dnsRefresh = 50 * ms
	app := startApp(t, Hubs("dns:///hubs.example:7000"), withTimings(tm), withResolver(resolve), MaxConcurrent(1))
	sa, _ := registered(t, a, "hub-a")
	hubIDsAre(t, app, "hub-a")
	// A new address in the answer connects.
	mu.Lock()
	answer = []string{a.Addr(), b.Addr()}
	mu.Unlock()
	sb, _ := registered(t, b, "hub-b")
	hubIDsAre(t, app, "hub-a", "hub-b")
	// A failed resolution keeps the hubs.
	mu.Lock()
	answer = nil
	mu.Unlock()
	time.Sleep(150 * ms)
	hubIDsAre(t, app, "hub-a", "hub-b")
	// A vanished address leaves the routing at once, gets a Register with no types, and is
	// closed once nothing of the instance is in flight on it.
	held := async(func() (Empty, error) {
		return Send[Empty](ctxWithin(t, 5*s_), app, PlaceOrder{}, RoutingKey(keyForIDs(t, "hub-a", "hub-b", "hub-a")))
	})
	r := sa.RecvRequest(t) // written to hub-a, unanswered
	mu.Lock()
	answer = []string{b.Addr()}
	mu.Unlock()
	empty := sa.RecvRegister(t)
	is(t, len(empty.GetCommandTypes())+len(empty.GetQueryTypes())+len(empty.GetSharedQueryTypes()), 0)
	is(t, empty.GetServiceName(), "orders-service")
	hubIDsAre(t, app, "hub-b")
	roundTrip(t, app, sb, RoutingKey(keyForIDs(t, "hub-a", "hub-b", "hub-a"))) // routed to hub-b now
	sa.Nothing(t, 300*ms)                                                      // still open: a request of ours is unanswered on it
	isAlive(t, sa)
	sa.Send(t, replyTo(r.GetId(), "{}"))
	is(t, await(t, held).err, nil)
	sa.HalfClosed(t, 2*s_)
	a.NoStreamWithin(t, 300*ms)
}

// keyForIDs is a routing key whose rendezvous winner among id1 and id2 is winner.
func keyForIDs(t *testing.T, id1, id2, winner string) string {
	t.Helper()
	for n := range 1000 {
		key := fmt.Sprintf("order-%d", n)
		if rendezvous.Rank([]string{id1, id2}, key)[0] == winner {
			return key
		}
	}
	t.Fatalf("no key for %s", winner)
	return ""
}

func TestADNSAddressBackWhileDrainingIsKept(t *testing.T) {
	a := fakehub.Start(t)
	var mu sync.Mutex
	answer := []string{a.Addr()}
	resolve := func(context.Context, string, string) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(answer), nil
	}
	tm := scaled()
	tm.dnsRefresh = 50 * ms
	app := newApp(t, Hubs("dns:///hubs.example:7000"), withTimings(tm), withResolver(resolve), MaxConcurrent(1))
	is(t, HandleCommand(app, func(context.Context, GetOrder) (Empty, error) { return Empty{}, nil }), nil)
	is(t, app.Start(context.Background()), nil)
	sa, first := registered(t, a, "hub-a")
	is(t, strings.Join(first.GetCommandTypes(), ","), "orders.GetOrder")
	hubIDsAre(t, app, "hub-a")
	held := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*s_), app, PlaceOrder{}) })
	r := sa.RecvRequest(t) // keeps the drain waiting
	mu.Lock()
	answer = []string{"127.0.0.1:1"} // hub-a left the answer
	mu.Unlock()
	is(t, len(sa.RecvRegister(t).GetCommandTypes()), 0)
	hubIDsAre(t, app)
	mu.Lock()
	answer = []string{a.Addr()} // and is back before it is idle
	mu.Unlock()
	back := sa.RecvRegister(t) // the types announced again, on the same stream
	is(t, strings.Join(back.GetCommandTypes(), ","), "orders.GetOrder")
	hubIDsAre(t, app, "hub-a")
	sa.Send(t, replyTo(r.GetId(), "{}"))
	is(t, await(t, held).err, nil)
	sa.Nothing(t, 300*ms) // not closed
	isAlive(t, sa)
	a.NoStreamWithin(t, 200*ms)
}

func TestAResolvedAddressIsCheckedAgainstTheDNSName(t *testing.T) {
	app := newApp(t, Hubs("dns:///hubs.example:7000"), TLS(TLSConfig{}))
	c := app.newPoolForTest().newConn("10.0.0.1:7000", "hubs.example")
	is(t, c.tlsServerName(), "hubs.example")
	_, err := c.transport(nil)
	is(t, err, nil)
	named := newApp(t, Hubs("dns:///hubs.example:7000"), TLS(TLSConfig{ServerName: "hub.internal"}))
	c = named.newPoolForTest().newConn("10.0.0.1:7000", "hubs.example")
	is(t, c.tlsServerName(), "hub.internal") // the configured name wins
	static := newApp(t, Hubs("h:7000"), TLS(TLSConfig{}))
	is(t, static.newPoolForTest().all()[0].tlsServerName(), "") // the dialled host
}

// newPoolForTest is the pool of a, not started.
func (a *App) newPoolForTest() *pool { return newPool(a) }
