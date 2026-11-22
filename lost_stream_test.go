package snodra

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
)

func TestALostStreamEndsWhatArrivedOnIt(t *testing.T) {
	hub := fakehub.Start(t)
	stopped := make(chan error, 1)
	var updates *Updates[WatchOrders]
	app, s := serving(t, hub, func(app *App) {
		u, err := HandleSubscription(app, watchOrders)
		is(t, err, nil)
		updates = u
		is(t, HandleStream(app, func(ctx context.Context, q ListOrders, w *StreamWriter[OrderLine]) error {
			err := listOrders(ctx, q, w)
			<-ctx.Done()
			stopped <- ctx.Err()
			return err
		}), nil)
		is(t, HandleCommand(app, place), nil)
	}, MaxConcurrent(1), ReconnectBackoff(50*ms, 200*ms))

	// Served on stream 1: a streaming handler waiting for credit, and a subscription.
	s.Send(t, streamRequest("s#1", 1, `{"count":5}`))
	is(t, nextPart(t, s).GetSeq(), uint64(1))
	open(t, updates, s, "w#1", "t")
	is(t, app.SubscriptionsServed(), 1)

	// Sent through stream 1: a stream and a subscription, each with its first part.
	streamed := async(func() ([]OrderLine, error) {
		return collect(Stream[OrderLine](context.Background(), app, ListOrders{}))
	})
	sr := s.RecvRequest(t)
	s.Send(t, line(sr.GetId(), 1))
	sub := subscribing(app, context.Background())
	wr := s.RecvRequest(t)
	s.Send(t, partOf(wr.GetId(), 1, "B", "b1", `{"topic":"t","value":0}`, nil))
	o := await(t, sub)
	is(t, o.err, nil)
	watched := async(func() ([]OrderUpdate, error) { return collect(o.value.Updates()) })

	s.Close(codes.Unavailable, "restart")

	if err := receive(t, stopped); !errors.Is(err, context.Canceled) {
		t.Fatalf("the streaming handler's context: %v, want cancelled", err)
	}
	poll(t, func() bool { return app.SubscriptionsServed() == 0 })
	is(t, updates.Emit(topic("t"), OrderUpdate{Value: 1}), 0)
	st := await(t, streamed)
	is(t, len(st.value), 1)
	is(t, errors.Is(st.err, ErrOutcomeUnknown), true)
	w := await(t, watched)
	is(t, len(w.value), 0)
	is(t, errors.Is(w.err, ErrOutcomeUnknown), true)
	is(t, waiting(app), 0)

	// Stream 2: nothing of the lost stream's, and its only handler slot is free.
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.Ack(t, fakehub.Ack("hub-1"))
	s2.Nothing(t, 300*ms, "part", "end", "permits")
	alive(t, s2, "r-1")
}
