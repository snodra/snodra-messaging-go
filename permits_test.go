package snodra

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

func TestPermitFlushRule(t *testing.T) {
	one := permitLedger{initial: 1, offered: 1}
	one.arrived()
	n, _ := one.finished()
	is(t, n, uint32(1)) // one permit: every completion
	big := permitLedger{initial: 64, offered: 64}
	for i := 0; i < 10; i++ {
		big.arrived()
	}
	n, arm := big.finished()
	is(t, n, uint32(0))
	is(t, arm, true) // 54 held > min(16, 32): batch
	for i := 0; i < 15; i++ {
		n, _ = big.finished()
	}
	is(t, n, uint32(16)) // 16 pending: flush
	small := permitLedger{initial: 8, offered: 8}
	for i := 0; i < 5; i++ {
		small.arrived()
	}
	n, _ = small.finished()
	is(t, n, uint32(1)) // held 3 <= min(16, 4): at once
}

func TestTheTimerTakesEveryPendingGrant(t *testing.T) {
	l := permitLedger{initial: 64, offered: 64}
	for i := 0; i < 3; i++ {
		l.arrived()
	}
	n, arm := l.finished()
	is(t, n, uint32(0))
	is(t, arm, true)
	_, arm = l.finished()
	is(t, arm, false) // armed once
	is(t, l.timerFired(), uint32(2))
	is(t, l.timerFired(), uint32(0))
	_, arm = l.finished()
	is(t, arm, true) // armed again after it fired
}

func TestTheLedgerSurvivesItsCountersWrapping(t *testing.T) {
	l := permitLedger{initial: 1, offered: 1, received: math.MaxUint32, returned: math.MaxUint32}
	l.arrived() // received wraps to 0
	n, _ := l.finished()
	is(t, n, uint32(1)) // still "the hub holds none": at once, not batched
}

func TestOnePermitComesBackAtOnce(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) { is(t, HandleCommand(app, place), nil) }, MaxConcurrent(1))
	var gaps []time.Duration
	for i := 0; i < 10; i++ {
		s.Send(t, command("r-"+string(rune('a'+i)), "orders.PlaceOrder", `{}`))
		s.RecvReply(t)
		replied := time.Now()
		is(t, s.RecvPermits(t).GetCount(), uint32(1))
		gaps = append(gaps, time.Since(replied))
	}
	slices.Sort(gaps)
	if median := gaps[len(gaps)/2]; median >= 35*ms {
		t.Fatalf("median Reply to Permits %v, gaps %v (want at once)", median, gaps)
	}
}

func TestPermitsAreBatchedFor50ms(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) { is(t, HandleCommand(app, place), nil) }, MaxConcurrent(64))
	s.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	s.RecvReply(t)
	replied := time.Now()
	is(t, s.RecvPermits(t).GetCount(), uint32(1))
	within(t, time.Since(replied), 40*ms, 150*ms)
}

func TestSixteenPendingGrantsGoAtOnce(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) { is(t, HandleCommand(app, place), nil) }, MaxConcurrent(64))
	for i := 0; i < 16; i++ {
		s.Send(t, command("r-"+string(rune('a'+i)), "orders.PlaceOrder", `{}`))
	}
	total, deadline := uint32(0), time.Now().Add(5*time.Second)
	for total < 16 && time.Now().Before(deadline) {
		total += s.Recv(t).GetPermits().GetCount()
	}
	is(t, total, uint32(16))
}

func TestPermitsGoBackOnTheArrivalStream(t *testing.T) {
	a, b := fakehub.Start(t), fakehub.Start(t)
	app := newApp(t, Hubs(a.Addr(), b.Addr()), MaxConcurrent(1))
	is(t, HandleCommand(app, place), nil)
	is(t, app.Start(context.Background()), nil)
	sa, sb := a.Next(t), b.Next(t)
	sa.RecvRegister(t)
	sb.RecvRegister(t)
	sa.Ack(t, fakehub.Ack("hub-a"))
	sb.Ack(t, fakehub.Ack("hub-b"))
	sb.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder", Payload: []byte(`{}`)}))
	is(t, sb.RecvReply(t).GetRequestId(), "r-1")
	is(t, sb.RecvPermits(t).GetCount(), uint32(1))
	sa.Nothing(t, 200*ms, "reply", "permits")
}

func TestTheLowWaterMarkUsesTheRegistersPermits(t *testing.T) { // SG-4
	l := permitLedger{initial: 3, offered: 3}
	is(t, l.raise(5), uint32(2)) // topped up to 5: Permits{count 2, grant 5}
	is(t, l.raise(4), uint32(0)) // never lowered
	is(t, l.raise(5), uint32(0)) // offered already
	is(t, l.offered, uint32(5))
	for range 3 {
		l.arrived()
	}
	n, arm := l.finished()
	// The hub holds 5 - 3 = 2, above the mark of the Register's 3, min(16, 3/2) = 1: the grant
	// waits for the batch, as in the reference library (the top-ups do not move the mark).
	is(t, n, uint32(0))
	is(t, arm, true)
	for range 2 {
		l.arrived()
	}
	n, _ = l.finished() // the hub holds 5 - 5 + 0 = 0: at once, with what is pending
	is(t, n, uint32(2))
}
