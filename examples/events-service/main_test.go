package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// ask sends a request through the fake hub's stream and returns its Reply, giving up after 5 s.
func ask(t *testing.T, s *fakehub.Stream, kind hubpb.RequestKind, id, typ, payload string) *hubpb.Reply {
	t.Helper()
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: id, Kind: kind, Type: typ, Payload: []byte(payload)}))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r := s.Recv(t).GetReply(); r != nil {
			if r.GetRequestId() != id {
				t.Fatalf("reply for %s, want %s", r.GetRequestId(), id)
			}
			return r
		}
	}
	t.Fatalf("no reply to %s within 5 s", id)
	return nil
}

// The command publishes the event, the relay publishes it to the event store, and the service's
// own processor handles it: the query shows the projection once the event arrived.
func TestEventsServicePublishesAndConsumes(t *testing.T) {
	hub := fakehub.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, snodra.Hubs(hub.Addr())) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("run did not return after its context ended")
		}
	}
	defer stop()

	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))

	reply := ask(t, s, hubpb.RequestKind_COMMAND, "r-1", "orders.PlaceOrder", `{"orderId":"o-1","sku":"book","cents":1250}`)
	if reply.GetError() != nil {
		t.Fatalf("PlaceOrder: %v", reply.GetError())
	}

	// The event travels the outbox, the relay and the processor asynchronously.
	deadline := time.Now().Add(10 * time.Second)
	for attempt := 0; ; attempt++ {
		got := ask(t, s, hubpb.RequestKind_QUERY, fmt.Sprintf("q-%d", attempt), "orders.GetPlacedOrder", `{"orderId":"o-1"}`)
		if got.GetError() == nil {
			var receipt Receipt
			if err := json.Unmarshal(got.GetPayload(), &receipt); err != nil {
				t.Fatalf("GetPlacedOrder: %s %v", got.GetPayload(), err)
			}
			if receipt != (Receipt{OrderID: "o-1", SKU: "book", Cents: 1250}) {
				t.Fatalf("GetPlacedOrder: %+v", receipt)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the event handler never ran: %v", got.GetError())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
