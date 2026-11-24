package snodra

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// ackEvery is fakehub.Ack with another heartbeat interval.
func ackEvery(hubID string, interval time.Duration) *hubpb.RegisterAck {
	a := fakehub.Ack(hubID)
	a.HeartbeatIntervalMs = uint32(interval / ms)
	return a
}

func TestEveryStreamOpensWithRegisterThenHeartbeat(t *testing.T) {
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), MaxConcurrent(8), Tags(map[string]string{"zone": "a"}))
	s := hub.Next(t)
	first := s.Recv(t).GetRegister()
	is(t, first.GetServiceName(), "orders-service")
	is(t, first.GetInstanceId(), "i-1")
	is(t, first.GetInitialPermits(), uint32(8))
	is(t, first.GetLoadFactor(), uint32(100))
	is(t, first.GetClientVersion(), "snodra-messaging-go/"+Version)
	is(t, first.GetTags()["zone"], "a")
	is(t, s.Recv(t).GetHeartbeat() != nil, true)
	s.Close(codes.Unavailable, "restart")
	s2 := hub.Next(t)
	is(t, proto.Equal(s2.Recv(t).GetRegister(), first), true)
	is(t, s2.Recv(t).GetHeartbeat() != nil, true)
}

func TestRegisteredOnlyOnTheAck(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.EchoHeartbeats(true)
	time.Sleep(150 * time.Millisecond)
	is(t, len(app.HubIDs()), 0) // echoes confirm nothing
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	is(t, app.HubIDs()[0], "hub-1")
}

func TestNoAckWithinTheHandshakeTimeoutReopens(t *testing.T) {
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.EchoHeartbeats(true)
	start := time.Now()
	hub.Next(t) // closed by the library after 300 ms (scaled), reopened after the backoff
	within(t, time.Since(start), 300*time.Millisecond, 300*time.Millisecond+250*time.Millisecond)
}

func TestHeartbeatsAtTheAckInterval(t *testing.T) {
	// 170 ms, not the 100 ms used before the ack: a library ignoring the ack is out of bounds.
	const interval = 170 * ms
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, ackEvery("hub-1", interval))
	acked := time.Now()
	time.Sleep(time.Second + interval)
	// The first heartbeat after the ack may follow the cadence before it: skip one interval.
	var beats []time.Time
	for _, b := range s.Heartbeats() {
		if b.After(acked.Add(interval)) {
			beats = append(beats, b)
		}
	}
	if len(beats) < 4 {
		t.Fatalf("%d heartbeats in 1 s at %v", len(beats), interval)
	}
	var sum time.Duration
	for i := 1; i < len(beats); i++ {
		gap := beats[i].Sub(beats[i-1])
		if gap > interval*3/2 {
			t.Fatalf("gap %v between heartbeats %d and %d (interval %v)", gap, i-1, i, interval)
		}
		sum += gap
	}
	within(t, sum/time.Duration(len(beats)-1), interval*8/10, interval*12/10)
}

func TestTheSilentHubDecision(t *testing.T) {
	const i = time.Second
	for _, c := range []struct {
		since      time.Duration // since the last echo, on the tick
		unanswered int           // heartbeats sent since it
		want       bool
	}{
		{4*i - ms, 3, true},      // the fourth tick after the last echoed heartbeat
		{3*i + ms, 3, true},      // more than three intervals, three heartbeats unanswered
		{3 * i, 3, false},        // exactly three intervals: not more
		{3*i + ms, 2, false},     // a late third tick: only two heartbeats unanswered yet
		{10 * i, 2, false},       // (the ticker skipped ticks: the next tick decides)
		{2*i + 500*ms, 3, false}, // three unanswered, but not three intervals
		{3*i + 500*ms, 0, false}, // an echo just came
	} {
		if got := hubSilent(c.since, c.unanswered, i, 3); got != c.want {
			t.Fatalf("since %v, %d unanswered: got %v, want %v", c.since, c.unanswered, got, c.want)
		}
	}
}

func TestASilentHubIsDroppedOnTheTickAfterThreeIntervals(t *testing.T) {
	const interval = 200 * ms
	const slack = 60 * ms // scheduling and transport, also under -race
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, ackEvery("hub-1", interval))
	hub.NoStreamWithin(t, 5*interval) // echoed: the stream stays up past three intervals
	s.EchoHeartbeats(false)
	at := s.Ended(t, 10*interval)
	echoed, beats := s.Echoed(), s.Heartbeats()
	if len(echoed) == 0 {
		t.Fatal("no heartbeat was echoed")
	}
	last := echoed[len(echoed)-1]
	silence := at.Sub(last)
	// Never before three full intervals without an echo (the library measures from its own,
	// later, receipt of that echo, so the fake's view is strictly above three intervals).
	if silence <= 3*interval {
		t.Fatalf("dropped %v after the last echo, want more than %v", silence, 3*interval)
	}
	// And by the next heartbeat tick after that: at most four intervals.
	within(t, silence, 3*interval, 4*interval+slack)
	// The check runs on the heartbeat tick, before that tick's heartbeat: the drop comes one
	// interval after the last heartbeat sent, which is not a second heartbeat at the same moment.
	within(t, at.Sub(beats[len(beats)-1]), interval-slack, interval+slack)
	unanswered := 0
	for _, b := range beats {
		if b.After(last) {
			unanswered++
		}
	}
	is(t, unanswered, 3)        // dropped on the tick after the third unanswered heartbeat
	hub.Next(t).RecvRegister(t) // and the library reconnects
}

// liveWithSlowTicks is an app on one hub whose ack sets a 10 s heartbeat interval: no tick
// heartbeat comes during the test, so any heartbeat is one the library wrote for another reason.
func liveWithSlowTicks(t *testing.T) (*App, *hubConn, *fakehub.Stream) {
	t.Helper()
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, ackEvery("hub-1", 10*s_))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	return app, app.pool.conns[0], s
}

// nextAfterRegister reads up to the next Register and returns it and the frame after it,
// InstanceStatus frames skipped.
func nextAfterRegister(t *testing.T, s *fakehub.Stream) (*hubpb.Register, *hubpb.ClientMessage) {
	t.Helper()
	r := s.RecvRegister(t)
	for {
		if m := s.Recv(t); m.GetStatus() == nil {
			return r, m
		}
	}
}

func TestEveryLaterRegisterIsFollowedByAHeartbeat(t *testing.T) {
	t.Run("re-registration", func(t *testing.T) {
		_, c, s := liveWithSlowTicks(t)
		c.reregister()
		r, next := nextAfterRegister(t, s)
		is(t, r.GetServiceName(), "orders-service")
		is(t, next.GetHeartbeat() != nil, true)
	})
	t.Run("the empty Register at shutdown", func(t *testing.T) {
		app, _, s := liveWithSlowTicks(t)
		done := make(chan error, 1)
		go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
		r, next := nextAfterRegister(t, s)
		is(t, len(r.GetCommandTypes())+len(r.GetQueryTypes())+len(r.GetSharedQueryTypes()), 0)
		is(t, next.GetHeartbeat() != nil, true)
		s.Ack(t, fakehub.Ack("hub-1"))
		s.HalfClosed(t, time.Second)
		s.Close(codes.OK, "")
		is(t, <-done, nil)
	})
	t.Run("the empty Register of a rollover", func(t *testing.T) {
		_, c, s := liveWithSlowTicks(t)
		c.rollReq <- c.stream()
		r, next := nextAfterRegister(t, s)
		is(t, len(r.GetCommandTypes())+len(r.GetQueryTypes())+len(r.GetSharedQueryTypes()), 0)
		is(t, next.GetHeartbeat() != nil, true)
	})
}

func TestTheHeartbeatAfterALaterRegisterCountsTowardTheSilentHub(t *testing.T) {
	// The ack resets the heartbeat ticker. Its third tick comes a little more than three
	// intervals after the ack, the last sign of life, when only two tick heartbeats were
	// written: the hub is silent then only if the heartbeat after the later Register counted.
	const interval = 500 * ms
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.EchoHeartbeats(false)
	acked := time.Now()
	s.Ack(t, ackEvery("hub-1", interval))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	app.pool.conns[0].reregister()
	at := s.Ended(t, 10*interval)
	// On the third tick (without the count, the fourth: four intervals).
	within(t, at.Sub(acked), 3*interval, 3*interval+interval/2)
	hub.Next(t).RecvRegister(t) // and the library reconnects
}

func TestTheBackoffResetsAfterAnAck(t *testing.T) {
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()))
	// Three UNAVAILABLE closes take the backoff to its 200 ms cap (50, 100, 200).
	for i := 0; i < 3; i++ {
		s := hub.Next(t)
		s.RecvRegister(t)
		s.Close(codes.Unavailable, "restart")
	}
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	s.Close(codes.Unavailable, "restart")
	start := time.Now()
	hub.Next(t)
	// Back at the 50 ms floor (at most 60 ms jittered); without the reset at least 160 ms.
	within(t, time.Since(start), 0, 140*ms)
}

func TestARefusedRegisterWaitsTheCapAndIsExposed(t *testing.T) {
	hub := fakehub.Start(t)
	logs := &logRecorder{}
	app := startApp(t, Hubs(hub.Addr()), Logger(slog.New(logs)))
	_, ok := app.Rejection()
	is(t, ok, false)
	const msg = "type orders.PlaceOrder is owned by billing-service"
	refusals := []struct {
		code codes.Code
		name string
	}{
		{codes.AlreadyExists, "ALREADY_EXISTS"},
		{codes.AlreadyExists, "ALREADY_EXISTS"}, // the same refusal again: logged at debug
		{codes.InvalidArgument, "INVALID_ARGUMENT"},
		{codes.Unauthenticated, "UNAUTHENTICATED"},
		{codes.PermissionDenied, "PERMISSION_DENIED"},
	}
	s := hub.Next(t)
	for _, r := range refusals {
		s.RecvRegister(t)
		s.Close(r.code, msg)
		start := time.Now()
		s = hub.Next(t) // Start's goroutines still run
		within(t, time.Since(start), 160*ms, 200*ms+80*ms)
		rej, ok := app.Rejection()
		is(t, ok, true)
		is(t, rej.Code, r.name)
		is(t, rej.Message, msg)
		is(t, rej.Hub, hub.Addr())
		is(t, rej.At.IsZero(), false)
	}
	is(t, logs.count(slog.LevelError, "the hub refused the registration"), 4)
	is(t, logs.count(slog.LevelDebug, "the hub refused the registration"), 1)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	rej, ok := app.Rejection() // a later acceptance does not clear it (SG-11)
	is(t, ok, true)
	is(t, rej.Code, "PERMISSION_DENIED")
}

func TestUnavailableIsAnOrdinaryClosure(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Close(codes.Unavailable, "jwks not loaded yet")
	start := time.Now()
	hub.Next(t)
	within(t, time.Since(start), 0, 140*ms) // the 50 ms floor, not the 160-200 ms of a refusal
	_, ok := app.Rejection()
	is(t, ok, false)
}

func TestShutdownLeavesTheHubInOrder(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	r := s.RecvRegister(t)
	is(t, r.GetServiceName(), "orders-service")
	is(t, r.GetInstanceId(), "i-1")
	is(t, len(r.GetCommandTypes())+len(r.GetQueryTypes())+len(r.GetSharedQueryTypes()), 0)
	s.Ack(t, fakehub.Ack("hub-1"))
	s.HalfClosed(t, time.Second)
	s.Close(codes.OK, "")
	select {
	case err := <-done:
		is(t, err, nil)
	case <-time.After(150 * ms): // the close wait is 200 ms: the hub's end must cut it short
		t.Fatal("Shutdown did not return once the hub finished its side")
	}
	is(t, len(app.HubIDs()), 0)
}

func TestShutdownDoesNotWaitOnTheEmptyRegistersAck(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	s.RecvRegister(t) // never acked
	is(t, len(app.HubIDs()), 1)
	is(t, app.HubIDs()[0], "hub-1")
	select {
	case err := <-done:
		is(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	// deregister wait (100 ms) + close wait (200 ms) + slack
	within(t, time.Since(start), 0, 100*ms+200*ms+150*ms)
}

func TestShutdownReturnsByItsDeadline(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	start := time.Now()
	err := app.Shutdown(ctxWithin(t, 50*ms)) // shorter than the 100 ms deregister wait
	is(t, errors.Is(err, context.DeadlineExceeded), true)
	// The deadline, plus at most the 50 ms grace for the stopped connections, plus slack.
	within(t, time.Since(start), 0, 50*ms+killGrace+100*ms)
	is(t, len(app.HubIDs()), 0)
}

func TestShutdownStopsAConnectionInBackoff(t *testing.T) {
	hub := fakehub.Start(t)
	hub.Down()
	logs := &logRecorder{}
	app := startApp(t, Hubs(hub.Addr()), Logger(slog.New(logs)))
	retrying := func() int {
		return logs.count(slog.LevelWarn, "the hub stream ended; reconnecting") +
			logs.count(slog.LevelDebug, "the hub stream ended; reconnecting")
	}
	waitFor(t, func() bool { return retrying() >= 2 }) // refused connects: the connection waits in its backoff
	start := time.Now()
	is(t, app.Shutdown(ctxWithin(t, 5*time.Second)), nil)
	within(t, time.Since(start), 0, 100*ms)
	after := retrying()
	time.Sleep(300 * ms) // longer than the 200 ms backoff cap
	is(t, retrying(), after)
}

func TestPickRoundRobinsOverAcknowledgedHubs(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	app := startApp(t, Hubs(a.Addr(), b.Addr()))
	sa, sb := a.Next(t), b.Next(t)
	sa.RecvRegister(t)
	sb.RecvRegister(t)
	_, err := app.pool.pick("")
	is(t, errors.Is(err, ErrNeverSent), true)
	sa.Ack(t, fakehub.Ack("hub-a"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	for i := 0; i < 3; i++ {
		c, err := app.pool.pick("")
		is(t, err, nil)
		is(t, c.ack().GetHubId(), "hub-a")
	}
	sb.Ack(t, fakehub.Ack("hub-b"))
	waitFor(t, func() bool { return len(app.HubIDs()) == 2 })
	is(t, app.HubIDs()[0]+","+app.HubIDs()[1], "hub-a,hub-b")
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		c, err := app.pool.pick("")
		is(t, err, nil)
		seen[c.ack().GetHubId()]++
	}
	is(t, seen["hub-a"], 2)
	is(t, seen["hub-b"], 2)
}

func TestStreamSendFailsOnceTheStreamIsGone(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	c, err := app.pool.pick("")
	is(t, err, nil)
	ref := c.stream()
	hb := &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Heartbeat{Heartbeat: &hubpb.Heartbeat{SentAtMs: 42}}}
	is(t, ref.send(hb), nil)
	for {
		if m := s.Recv(t); m.GetHeartbeat().GetSentAtMs() == 42 {
			break
		}
	}
	s.Close(codes.Unavailable, "restart")
	hub.Next(t)
	is(t, ref.send(hb) != nil, true) // that stream only: never written to the new one
}

// waitFor polls cond for up to a second.
func waitFor(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 1 s")
		}
		time.Sleep(5 * ms)
	}
}

func TestTheConnectionFollowsTheStateDiagram(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	c := app.pool.conns[0]
	stateIs := func(want connState) {
		t.Helper()
		waitFor(t, func() bool { return c.currentState() == want })
	}
	s := hub.Next(t)
	s.RecvRegister(t)
	stateIs(stateAwaitingAck)
	s.Close(codes.AlreadyExists, "type orders.PlaceOrder is owned by billing-service")
	stateIs(stateRejected)
	s = hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	stateIs(stateLive)
	s.Close(codes.Unavailable, "restart")
	stateIs(stateBackoff)
	s = hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	stateIs(stateLive)
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	s.RecvRegister(t)
	stateIs(stateDraining)
	is(t, <-done, nil)
	stateIs(stateStopped)
}

func TestShutdownLeavesNoGoroutineBehind(t *testing.T) {
	for name, budget := range map[string]time.Duration{"in time": 5 * time.Second, "context expired": 20 * ms} {
		t.Run(name, func(t *testing.T) { shutdownLeavesNoGoroutineBehind(t, budget) })
	}
}

func shutdownLeavesNoGoroutineBehind(t *testing.T, budget time.Duration) {
	hub := fakehub.Start(t)
	before := runtime.NumGoroutine()
	app, err := New(Service("orders-service"), Instance("i-1"), withTimings(scaled()), Hubs(hub.Addr(), "127.0.0.1:1"))
	is(t, err, nil)
	is(t, app.Start(context.Background()), nil)
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	err = app.Shutdown(ctxWithin(t, budget))
	if budget < time.Second {
		is(t, errors.Is(err, context.DeadlineExceeded), true)
	} else {
		is(t, err, nil)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			t.Fatalf("%d goroutines, %d before:\n%s", runtime.NumGoroutine(), before, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * ms)
	}
}

func TestARefusalCodeOnAnAcknowledgedStreamIsARefusal(t *testing.T) { // SG-15
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	s.Close(codes.PermissionDenied, "instance id held by another service")
	start := time.Now()
	hub.Next(t)
	within(t, time.Since(start), 160*ms, 200*ms+80*ms)
	rej, ok := app.Rejection()
	is(t, ok, true)
	is(t, rej.Code, "PERMISSION_DENIED")
}

func TestALateAckDuringShutdownIsIgnored(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	s := hub.Next(t)
	s.RecvRegister(t) // awaiting the ack when the shutdown begins
	done := make(chan error, 1)
	go func() { done <- app.Shutdown(ctxWithin(t, 5*time.Second)) }()
	waitFor(t, func() bool { return app.pool.conns[0].stopRequested() })
	s.Ack(t, fakehub.Ack("hub-1"))
	s.HalfClosed(t, time.Second)
	is(t, len(app.HubIDs()), 0) // never counted as registered
	s.Nothing(t, 100*ms, "register")
	is(t, <-done, nil)
	is(t, app.Ready(ctxWithin(t, 100*ms)) != nil, true) // the shutdown, not a registration
}

func TestEachDistinctRefusalIsLoggedAtErrorOnce(t *testing.T) {
	hub := fakehub.Start(t)
	logs := &logRecorder{}
	startApp(t, Hubs(hub.Addr()), Logger(slog.New(logs)))
	s := hub.Next(t)
	for _, msg := range []string{"reason A", "reason B", "reason A", "reason B"} {
		s.RecvRegister(t)
		s.Close(codes.AlreadyExists, msg)
		s = hub.Next(t)
	}
	is(t, logs.count(slog.LevelError, "the hub refused the registration"), 2)
	is(t, logs.count(slog.LevelDebug, "the hub refused the registration"), 2)
}
