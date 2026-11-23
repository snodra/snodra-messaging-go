package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// reply forwards a request through the fake hub's stream and returns the Reply, giving up
// after 5 s.
func reply(t *testing.T, s *fakehub.Stream, kind hubpb.RequestKind, id, typ, payload string) *hubpb.Reply {
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

func TestQuickstartServes(t *testing.T) {
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
	r := s.RecvRegister(t)
	if r.GetServiceName() != "orders-service" ||
		len(r.GetCommandTypes()) != 1 || r.GetCommandTypes()[0] != "orders.PlaceOrder" ||
		len(r.GetQueryTypes()) != 1 || r.GetQueryTypes()[0] != "orders.GetOrder" {
		t.Fatalf("Register %v", r)
	}
	s.Ack(t, fakehub.Ack("hub-1"))

	got := reply(t, s, hubpb.RequestKind_COMMAND, "r-1", "orders.PlaceOrder", `{"orderId":"o-1","cents":1250}`)
	var receipt Receipt
	if err := json.Unmarshal(got.GetPayload(), &receipt); err != nil || receipt != (Receipt{OrderID: "o-1", Total: 1250}) {
		t.Fatalf("PlaceOrder: %s %v", got.GetPayload(), err)
	}
	got = reply(t, s, hubpb.RequestKind_QUERY, "r-2", "orders.GetOrder", `{"orderId":"o-1"}`)
	if err := json.Unmarshal(got.GetPayload(), &receipt); err != nil || receipt != (Receipt{OrderID: "o-1", Total: 1250}) {
		t.Fatalf("GetOrder: %s %v", got.GetPayload(), err)
	}
	got = reply(t, s, hubpb.RequestKind_QUERY, "r-3", "orders.GetOrder", `{"orderId":"nope"}`)
	if got.GetError().GetCode() != hubpb.ErrorCode_INVALID_REQUEST {
		t.Fatalf("an unknown order: %v", got)
	}
	got = reply(t, s, hubpb.RequestKind_COMMAND, "r-4", "orders.PlaceOrder", `{"cents":1}`)
	if got.GetError().GetCode() != hubpb.ErrorCode_INVALID_REQUEST {
		t.Fatalf("an order without id: %v", got)
	}
	stop()
}
