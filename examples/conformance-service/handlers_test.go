package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// started runs the service's handlers on a fake hub and returns the acknowledged stream.
func started(t *testing.T, e env) (*fakehub.Hub, *fakehub.Stream) {
	t.Helper()
	hub := fakehub.Start(t)
	e.hubs = []string{hub.Addr()}
	app, err := snodra.New(e.options()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := registerHandlers(app, e); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		_ = app.Shutdown(ctx)
	})
	s := hub.Next(t)
	r := s.RecvRegister(t)
	if got := strings.Join(r.GetCommandTypes(), ","); got != "conformance.Done,conformance.Echo,conformance.Fail,conformance.Sleep,conformance.Strict" {
		t.Fatalf("command types %s", got)
	}
	if got := strings.Join(r.GetQueryTypes(), ","); got != "conformance.EchoQuery,conformance.SleepQuery,conformance.View" {
		t.Fatalf("query types %s", got)
	}
	return hub, s
}

// call forwards a request and returns its Reply.
func call(t *testing.T, s *fakehub.Stream, kind hubpb.RequestKind, typ, payload string, headers map[string]string) *hubpb.Reply {
	t.Helper()
	id := "r-" + typ
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: id, Kind: kind, Type: typ, Payload: []byte(payload), Headers: headers}))
	for {
		m := s.Recv(t)
		if r := m.GetReply(); r != nil {
			if r.GetRequestId() != id {
				t.Fatalf("reply for %s, want %s", r.GetRequestId(), id)
			}
			return r
		}
	}
}

func decode(t *testing.T, r *hubpb.Reply) map[string]any {
	t.Helper()
	if r.GetError() != nil {
		t.Fatalf("error %v", r.GetError())
	}
	var m map[string]any
	if err := json.Unmarshal(r.GetPayload(), &m); err != nil {
		t.Fatalf("%s: %v", r.GetPayload(), err)
	}
	return m
}

func TestTheServiceAnswersAsTheContractSays(t *testing.T) {
	e := env{service: "conformance-go", instance: "i-1", capacity: 4, retry: true}
	_, s := started(t, e)
	const cmd, qry = hubpb.RequestKind_COMMAND, hubpb.RequestKind_QUERY

	// View before the ack: no hub, no rejection.
	v := decode(t, call(t, s, qry, "conformance.View", `{}`, nil))
	if v["service"] != "conformance-go" || v["instanceId"] != "i-1" || len(v["hubIds"].([]any)) != 0 ||
		v["lastRejection"] != nil || v["subscriptionsServed"] != float64(0) || v["retry"] != true {
		t.Fatalf("View %v", v)
	}
	s.Ack(t, fakehub.Ack("hub-1"))
	time.Sleep(50 * time.Millisecond)
	if v := decode(t, call(t, s, qry, "conformance.View", `{}`, nil)); v["hubIds"].([]any)[0] != "hub-1" {
		t.Fatalf("View after the ack %v", v)
	}

	for _, typ := range []string{"conformance.Echo", "conformance.EchoQuery"} {
		kind := map[bool]hubpb.RequestKind{true: qry, false: cmd}[strings.HasSuffix(typ, "Query")]
		got := call(t, s, kind, typ, `{"a":[1,2]}`, map[string]string{"x-tenant": "t-1"})
		var a EchoAnswer
		if err := json.Unmarshal(got.GetPayload(), &a); err != nil {
			t.Fatal(err)
		}
		if string(a.Payload) != `{"a":[1,2]}` || a.Headers["x-tenant"] != "t-1" || a.InstanceID != "i-1" || a.RequestID != "r-"+typ {
			t.Fatalf("%s: %s", typ, got.GetPayload())
		}
	}
	if r := call(t, s, cmd, "conformance.Done", `null`, nil); string(r.GetPayload()) != "{}" {
		t.Fatalf("Done %v", r)
	}
	for _, typ := range []string{"conformance.Sleep", "conformance.SleepQuery"} {
		kind := map[bool]hubpb.RequestKind{true: qry, false: cmd}[strings.HasSuffix(typ, "Query")]
		if m := decode(t, call(t, s, kind, typ, `{"ms":30}`, nil)); m["instanceId"] != "i-1" || m["sleptMs"].(float64) < 30 {
			t.Fatalf("%s %v", typ, m)
		}
	}
	f := call(t, s, cmd, "conformance.Fail", `{"message":"boom","details":{"a": 1}}`, nil).GetError()
	if f.GetCode() != hubpb.ErrorCode_HANDLER_ERROR || !strings.Contains(f.GetMessage(), "boom") || string(f.GetDetails()) != `{"a":1}` {
		t.Fatalf("Fail %v", f)
	}
	if m := decode(t, call(t, s, cmd, "conformance.Strict", `{"count":3}`, nil)); m["count"] != float64(3) {
		t.Fatalf("Strict %v", m)
	}
	for _, bad := range []string{`{"count":"not a number"}`, `not json`} {
		if e := call(t, s, cmd, "conformance.Strict", bad, nil).GetError(); e.GetCode() != hubpb.ErrorCode_INVALID_REQUEST {
			t.Fatalf("Strict %s: %v", bad, e)
		}
	}
}

func TestViewExposesTheLastRejection(t *testing.T) {
	hub, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Close(codes.PermissionDenied, "conformance refusal 4: permission denied")
	s = hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	v := decode(t, call(t, s, hubpb.RequestKind_QUERY, "conformance.View", `{}`, nil))
	if v["lastRejection"] != "PERMISSION_DENIED: conformance refusal 4: permission denied" || v["retry"] != false {
		t.Fatalf("View %v", v)
	}
}
