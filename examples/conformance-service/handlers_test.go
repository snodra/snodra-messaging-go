package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// started runs the service's handlers on a fake hub and returns its first stream, not
// acknowledged yet. The reconnect backoff is short (a refusal waits 160-200 ms), well inside
// the fake hub's 5 s wait for the next stream.
func started(t *testing.T, e env) (*snodra.App, *fakehub.Hub, *fakehub.Stream) {
	t.Helper()
	hub := fakehub.Start(t)
	e.hubs = []string{hub.Addr()}
	app, err := snodra.New(append(e.options(), snodra.ReconnectBackoff(50*time.Millisecond, 200*time.Millisecond))...)
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
	if got := strings.Join(r.GetCommandTypes(), ","); got != "conformance.Done,conformance.Echo,conformance.Emit,conformance.Fail,conformance.Guarded,conformance.Relay,conformance.Send,conformance.Sleep,conformance.Strict" {
		t.Fatalf("command types %s", got)
	}
	if got := strings.Join(r.GetQueryTypes(), ","); got != "conformance.EchoQuery,conformance.Outcome,conformance.SleepQuery,conformance.Stream,conformance.View,conformance.Watch" {
		t.Fatalf("query types %s", got)
	}
	if got := strings.Join(r.GetSharedQueryTypes(), ","); got != "conformance.SharedQuery" {
		t.Fatalf("shared query types %s", got)
	}
	return app, hub, s
}

// ready waits (at most 1 s) until app counts a hub as registered.
func ready(t *testing.T, app *snodra.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := app.Ready(ctx); err != nil {
		t.Fatalf("not registered: %v", err)
	}
}

// call forwards a request and returns its Reply; it fails the test after 5 s.
func call(t *testing.T, s *fakehub.Stream, kind hubpb.RequestKind, typ, payload string, headers map[string]string) *hubpb.Reply {
	t.Helper()
	id := "r-" + typ
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: id, Kind: kind, Type: typ, Payload: []byte(payload), Headers: headers}))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("no Reply to %s within 5 s", id)
		}
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
	e := env{service: "conformance-go", instance: "i-1", capacity: 4, retry: &snodra.RetryNeverRan}
	app, _, s := started(t, e)
	const cmd, qry = hubpb.RequestKind_COMMAND, hubpb.RequestKind_QUERY

	// View before the ack: no hub, no rejection.
	v := decode(t, call(t, s, qry, "conformance.View", `{}`, nil))
	if v["service"] != "conformance-go" || v["instanceId"] != "i-1" || len(v["hubIds"].([]any)) != 0 ||
		v["lastRejection"] != nil || v["subscriptionsServed"] != float64(0) || v["retry"] != true {
		t.Fatalf("View %v", v)
	}
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	if v := decode(t, call(t, s, qry, "conformance.View", `{}`, nil)); fmt.Sprint(v["hubIds"]) != "[hub-1]" {
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
	app, hub, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Close(codes.PermissionDenied, "conformance refusal 4: permission denied")
	s = hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	v := decode(t, call(t, s, hubpb.RequestKind_QUERY, "conformance.View", `{}`, nil))
	if v["lastRejection"] != "PERMISSION_DENIED: conformance refusal 4: permission denied" || v["retry"] != false {
		t.Fatalf("View %v", v)
	}
}

// nextFrame returns the next Request or Reply on s (skipping permits, heartbeats and statuses);
// it fails the test after 5 s.
func nextFrame(t *testing.T, s *fakehub.Stream) (*hubpb.Request, *hubpb.Reply) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m := s.Recv(t)
		if r := m.GetRequest(); r != nil {
			return r, nil
		}
		if r := m.GetReply(); r != nil {
			return nil, r
		}
	}
	t.Fatal("no Request or Reply within 5 s")
	return nil, nil
}

func TestSendGoesThroughTheLibrary(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	const cmd = hubpb.RequestKind_COMMAND

	// A unary command with a routing key, answered: the outcome carries the decoded payload.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-send", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"unary","target":"probe","routingKey":"rk-7","timeoutMs":1200,"payload":{"mark":"u1"}}`)}))
	probe, _ := nextFrame(t, s)
	if probe.GetType() != "conformance.runner.Probe" || probe.GetKind() != cmd || probe.GetRoutingKey() != "rk-7" ||
		probe.GetTimeoutMs() != 1200 || string(probe.GetPayload()) != `{"mark":"u1"}` {
		t.Fatalf("the library sent %v", probe)
	}
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Reply{Reply: &hubpb.Reply{
		RequestId: probe.GetId(), Result: &hubpb.Reply_Payload{Payload: []byte(`{"probe":"answer"}`)}}}})
	_, reply := nextFrame(t, s)
	out := decode(t, reply)
	if out["result"] != "ok" || fmt.Sprint(out["payload"]) != "map[probe:answer]" || out["elapsedMs"] == nil {
		t.Fatalf("outcome %v", out)
	}

	// Asynchronous, failing with the hub's NO_HANDLER: the outcome comes from conformance.Outcome.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-async", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"unary","target":"probe-query","async":true}`)}))
	var opID string
	for answered := false; opID == "" || !answered; {
		probe, reply := nextFrame(t, s)
		switch {
		case probe != nil:
			if probe.GetKind() != hubpb.RequestKind_QUERY || probe.GetType() != "conformance.runner.ProbeQuery" {
				t.Fatalf("the library sent %v", probe)
			}
			s.Send(t, fakehub.HubError(probe.GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
			answered = true
		default:
			opID, _ = decode(t, reply)["opId"].(string)
		}
	}
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-outcome", Kind: hubpb.RequestKind_QUERY, Type: "conformance.Outcome",
		Payload: []byte(`{"opId":"` + opID + `","waitMs":3000}`)}))
	_, reply = nextFrame(t, s)
	out = decode(t, reply)
	e, _ := out["error"].(map[string]any)
	if out["result"] != "error" || e["kind"] != "code" || e["code"] != "NO_HANDLER" {
		t.Fatalf("outcome %v", out)
	}
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-unknown", Kind: hubpb.RequestKind_QUERY, Type: "conformance.Outcome",
		Payload: []byte(`{"opId":"nope"}`)}))
	_, reply = nextFrame(t, s)
	if out := decode(t, reply); out["unknownOp"] != "nope" {
		t.Fatalf("outcome %v", out)
	}

	// Headers through the library's generic header option.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-headers", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"unary","target":"probe","headers":{"x-tenant":"t-1"},"payload":{"mark":"h1"}}`)}))
	probe, _ = nextFrame(t, s)
	if probe.GetHeaders()["x-tenant"] != "t-1" {
		t.Fatalf("the library sent %v", probe)
	}
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Reply{Reply: &hubpb.Reply{
		RequestId: probe.GetId(), Result: &hubpb.Reply_Payload{Payload: []byte(`{}`)}}}})
	if _, reply = nextFrame(t, s); decode(t, reply)["result"] != "ok" {
		t.Fatalf("outcome %s", reply.GetPayload())
	}
	// A header the library owns: refused, and nothing is sent (the next frame is the answer).
	for _, name := range []string{"authorization", "CORRELATIONID", "correlation-id", "TraceParent", "tracestate"} {
		s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-refused", Kind: cmd, Type: "conformance.Send",
			Payload: []byte(`{"op":"unary","target":"probe","headers":{"` + name + `":"forged"}}`)}))
		probe, reply = nextFrame(t, s)
		if probe != nil {
			t.Fatalf("%s: the library sent %v", name, probe)
		}
		out := decode(t, reply)
		if e, _ := out["error"].(map[string]any); out["result"] != "error" || e["kind"] != "refused" {
			t.Fatalf("%s: outcome %v", name, out)
		}
	}

	// An op the contract does not name.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-teleport", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"teleport","target":"probe"}`)}))
	_, reply = nextFrame(t, s)
	if out := decode(t, reply); out["result"] != "unsupported" || out["reason"] == "" {
		t.Fatalf("outcome %v", out)
	}
}

func TestSharedQueryAnswersWhoAnswered(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	if m := decode(t, call(t, s, hubpb.RequestKind_QUERY, "conformance.SharedQuery", `{}`, nil)); m["service"] != "conformance-go" || m["instanceId"] != "i-1" {
		t.Fatalf("SharedQuery %v", m)
	}
}

func TestScatterGoesThroughTheLibrarysGather(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-scatter", Kind: hubpb.RequestKind_COMMAND, Type: "conformance.Send",
		Payload: []byte(`{"op":"scatter","target":"probe-query","payload":{"k":1},"maxResults":2,"timeoutMs":3000}`)}))
	gather, _ := nextFrame(t, s)
	if gather.GetType() != "conformance.runner.ProbeQuery" || gather.GetMode() != hubpb.RequestMode_SCATTER_GATHER ||
		gather.GetKind() != hubpb.RequestKind_QUERY || gather.GetMaxResults() != 2 || gather.GetTimeoutMs() != 3000 ||
		string(gather.GetPayload()) != `{"k":1}` {
		t.Fatalf("the library sent %v", gather)
	}
	id := gather.GetId()
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Part{Part: &hubpb.ReplyPart{RequestId: id, Seq: 0,
		ResponderService: "conformance-runner", ResponderInstance: "p1", Payload: []byte(`{"target":"p1"}`)}}})
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Part{Part: &hubpb.ReplyPart{RequestId: id, Seq: 1,
		ResponderService: "conformance-runner", ResponderInstance: "p2",
		Error: &hubpb.Error{RequestId: id, Code: hubpb.ErrorCode_HANDLER_ERROR, Message: "p2 fails"}}}})
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_End{End: &hubpb.ReplyEnd{RequestId: id, Parts: 2, Failed: 1, TimedOut: 1}}})
	_, reply := nextFrame(t, s)
	var out struct {
		Result string `json:"result"`
		Parts  []struct {
			Service  string          `json:"service"`
			Instance string          `json:"instance"`
			OK       bool            `json:"ok"`
			Payload  json.RawMessage `json:"payload"`
			Error    map[string]any  `json:"error"`
		} `json:"parts"`
		Failed   int `json:"failed"`
		TimedOut int `json:"timedOut"`
	}
	if err := json.Unmarshal(reply.GetPayload(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Result != "ok" || len(out.Parts) != 2 || out.Failed != 1 || out.TimedOut != 1 {
		t.Fatalf("outcome %s", reply.GetPayload())
	}
	p1, p2 := out.Parts[0], out.Parts[1]
	if p1.Service != "conformance-runner" || p1.Instance != "p1" || !p1.OK || string(p1.Payload) != `{"target":"p1"}` || p1.Error != nil {
		t.Fatalf("part 1 %+v", p1)
	}
	if p2.Instance != "p2" || p2.OK || p2.Payload != nil || p2.Error["kind"] != "code" || p2.Error["code"] != "HANDLER_ERROR" || p2.Error["message"] != "p2 fails" {
		t.Fatalf("part 2 %+v", p2)
	}

	// No target at all: the gather fails as a whole.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-scatter-2", Kind: hubpb.RequestKind_COMMAND, Type: "conformance.Send",
		Payload: []byte(`{"op":"scatter","target":"echo-query"}`)}))
	gather, _ = nextFrame(t, s)
	s.Send(t, fakehub.HubError(gather.GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	_, reply = nextFrame(t, s)
	m := decode(t, reply)
	if e, _ := m["error"].(map[string]any); m["result"] != "error" || e["code"] != "NO_HANDLER" {
		t.Fatalf("outcome %v", m)
	}

	// A target that is not a query.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-scatter-3", Kind: hubpb.RequestKind_COMMAND, Type: "conformance.Send",
		Payload: []byte(`{"op":"scatter","target":"probe"}`)}))
	_, reply = nextFrame(t, s)
	if m := decode(t, reply); m["result"] != "unsupported" {
		t.Fatalf("outcome %v", m)
	}
}

func TestRelayFailsWithTheNestedCallsError(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-relay", Kind: hubpb.RequestKind_COMMAND, Type: "conformance.Relay",
		Payload: []byte(`{"target":"missing","payload":{"x":1}}`)}))
	nested, _ := nextFrame(t, s)
	if nested.GetType() != "conformance.runner.Missing" || string(nested.GetPayload()) != `{"x":1}` {
		t.Fatalf("the library sent %v", nested)
	}
	s.Send(t, fakehub.HubError(nested.GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	_, reply := nextFrame(t, s)
	if e := reply.GetError(); e.GetCode() != hubpb.ErrorCode_HANDLER_ERROR || !strings.Contains(e.GetMessage(), "conformance.runner.Missing") {
		t.Fatalf("Relay answered %v", reply)
	}

	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-relay-2", Kind: hubpb.RequestKind_COMMAND, Type: "conformance.Relay",
		Payload: []byte(`{"target":"probe","payload":{"reply":{"ok":true}}}`)}))
	nested, _ = nextFrame(t, s)
	if nested.GetType() != "conformance.runner.Probe" {
		t.Fatalf("the library sent %v", nested)
	}
	s.Send(t, &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Reply{Reply: &hubpb.Reply{
		RequestId: nested.GetId(), Result: &hubpb.Reply_Payload{Payload: []byte(`{"ok":true}`)}}}})
	_, reply = nextFrame(t, s)
	if out := decode(t, reply); fmt.Sprint(out["reply"]) != "map[ok:true]" {
		t.Fatalf("Relay answered %v", out)
	}
}

// token is a signed-looking token for sub; without a JWKS the service reads it unverified.
func token(sub string) string {
	seg := func(v string) string { return base64.RawURLEncoding.EncodeToString([]byte(v)) }
	return seg(`{"alg":"RS256","typ":"JWT"}`) + "." + seg(`{"sub":"`+sub+`"}`) + "." + seg("signature")
}

func TestGuardedNeedsAnAcceptedCaller(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	const cmd = hubpb.RequestKind_COMMAND
	auth := func(sub string) map[string]string { return map[string]string{"authorization": "Bearer " + token(sub)} }
	if m := decode(t, call(t, s, cmd, "conformance.Guarded", `{"count":2}`, auth("alice"))); m["count"] != float64(2) || m["user"] != "alice" {
		t.Fatalf("Guarded %v", m)
	}
	for _, c := range []struct {
		payload string
		headers map[string]string
		want    hubpb.ErrorCode
	}{
		{`{"count":2}`, nil, hubpb.ErrorCode_UNAUTHENTICATED},
		{`not json`, nil, hubpb.ErrorCode_UNAUTHENTICATED}, // decided before decoding
		{`{"count":2}`, map[string]string{"authorization": "Bearer not-a-token"}, hubpb.ErrorCode_UNAUTHENTICATED},
		{`{"count":2}`, auth("conformance-refused"), hubpb.ErrorCode_PERMISSION_DENIED},
		{`{"count":"not a number"}`, auth("alice"), hubpb.ErrorCode_INVALID_REQUEST},
	} {
		if e := call(t, s, cmd, "conformance.Guarded", c.payload, c.headers).GetError(); e.GetCode() != c.want {
			t.Fatalf("Guarded %s %v: %v, want %v", c.payload, c.headers, e, c.want)
		}
	}
}

func TestTheStreamHandlerStreamsTheCount(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "s#1", Kind: hubpb.RequestKind_QUERY, Type: "conformance.Stream",
		Mode: hubpb.RequestMode_STREAMING, InitialCredit: 2, Payload: []byte(`{"count":3,"delayMs":10}`)}))
	for n := 1; n <= 2; n++ {
		p := s.RecvPart(t)
		if p.GetRequestId() != "s#1" || p.GetSeq() != uint64(n) || string(p.GetPayload()) != fmt.Sprintf(`{"n":%d}`, n) {
			t.Fatalf("part %d: %v", n, p)
		}
	}
	s.Nothing(t, 200*time.Millisecond, "part", "end")
	s.Send(t, fakehub.Flow("s#1", 1))
	if p := s.RecvPart(t); p.GetSeq() != 3 || string(p.GetPayload()) != `{"n":3}` {
		t.Fatalf("part 3: %v", p)
	}
	if e := s.RecvEnd(t); e.GetRequestId() != "s#1" || e.GetError() != nil {
		t.Fatalf("end %v", e)
	}
}

func TestStreamGoesThroughTheLibrarysStream(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	const cmd = hubpb.RequestKind_COMMAND
	part := func(id string, seq uint64) *hubpb.ServerMessage {
		return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Part{Part: &hubpb.ReplyPart{RequestId: id, Seq: seq,
			ResponderService: "conformance-runner", ResponderInstance: "p1", Payload: []byte(fmt.Sprintf(`{"i":%d}`, seq))}}}
	}
	end := func(id string, parts uint32) *hubpb.ServerMessage {
		return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_End{End: &hubpb.ReplyEnd{RequestId: id, Parts: parts}}}
	}

	// Read to the end, under the credit asked for.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-stream", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"stream","target":"probe-stream","payload":{"k":1},"initialCredit":2,"maxResults":5,"timeoutMs":3000}`)}))
	req, _ := nextFrame(t, s)
	if req.GetType() != "conformance.runner.ProbeStream" || req.GetMode() != hubpb.RequestMode_STREAMING ||
		req.GetInitialCredit() != 2 || req.GetMaxResults() != 5 || req.GetTimeoutMs() != 3000 || string(req.GetPayload()) != `{"k":1}` {
		t.Fatalf("the library sent %v", req)
	}
	s.Send(t, part(req.GetId(), 1))
	s.Send(t, part(req.GetId(), 2))
	if f := s.RecvFlow(t); f.GetRequestId() != req.GetId() || f.GetCredits() != 1 {
		t.Fatalf("flow %v", f)
	}
	s.Send(t, end(req.GetId(), 2))
	var reply *hubpb.Reply
	for reply == nil {
		m := streamFrameOf(t, s)
		reply = m.GetReply()
	}
	out := decode(t, reply)
	if out["result"] != "ok" || out["end"] != "completed" || fmt.Sprint(out["items"]) != "[map[i:1] map[i:2]]" {
		t.Fatalf("outcome %v", out)
	}

	// Stop after read 1: the stream is cancelled.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-stream-2", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"stream","target":"stream","payload":{"count":4},"read":1}`)}))
	req, _ = nextFrame(t, s)
	if req.GetType() != "conformance.Stream" || req.GetInitialCredit() != 64 || string(req.GetPayload()) != `{"count":4,"delayMs":0}` {
		t.Fatalf("the library sent %v", req)
	}
	s.Send(t, part(req.GetId(), 1))
	if c := s.RecvCancel(t); c.GetRequestId() != req.GetId() {
		t.Fatalf("cancel %v", c)
	}
	_, reply = nextFrame(t, s)
	if out := decode(t, reply); out["result"] != "ok" || out["end"] != "stopped" || fmt.Sprint(out["items"]) != "[map[i:1]]" {
		t.Fatalf("outcome %v", out)
	}

	// A gap: lost parts, with the items read before it.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-stream-3", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"stream","target":"probe-stream"}`)}))
	req, _ = nextFrame(t, s)
	s.Send(t, part(req.GetId(), 1))
	s.Send(t, part(req.GetId(), 3))
	s.RecvCancel(t)
	_, reply = nextFrame(t, s)
	out = decode(t, reply)
	if e, _ := out["error"].(map[string]any); out["result"] != "error" || e["kind"] != "lost-parts" || fmt.Sprint(out["items"]) != "[map[i:1]]" {
		t.Fatalf("outcome %v", out)
	}

	// A target that does not stream.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-stream-4", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"stream","target":"probe-query"}`)}))
	_, reply = nextFrame(t, s)
	if m := decode(t, reply); m["result"] != "unsupported" {
		t.Fatalf("outcome %v", m)
	}
}

// streamFrameOf is the next frame on s that is not a Permits, Heartbeat or InstanceStatus.
func streamFrameOf(t *testing.T, s *fakehub.Stream) *hubpb.ClientMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := s.Recv(t); m.GetPermits() == nil && m.GetHeartbeat() == nil && m.GetStatus() == nil {
			return m
		}
	}
	t.Fatal("no frame within 5 s")
	return nil
}

// watchRequest is a forwarded subscription target id of conformance.Watch on topic.
func watchRequest(id, topic string) *hubpb.ServerMessage {
	return fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_QUERY, Type: "conformance.Watch",
		Mode: hubpb.RequestMode_SUBSCRIPTION, Payload: []byte(`{"topic":"` + topic + `"}`)})
}

// nextOf is the next frame on s that is not a Permits, Heartbeat or InstanceStatus; it fails the
// test after 5 s.
func nextOf(t *testing.T, s *fakehub.Stream) *hubpb.ClientMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := s.Recv(t); m.GetPermits() == nil && m.GetHeartbeat() == nil && m.GetStatus() == nil {
			return m
		}
	}
	t.Fatal("no frame within 5 s")
	return nil
}

func TestWatchAndEmitServeSubscriptions(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4, maxSubscriptions: 2})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	const cmd, qry = hubpb.RequestKind_COMMAND, hubpb.RequestKind_QUERY
	s.Send(t, watchRequest("w1#1", "a"))
	if e := call(t, s, qry, "conformance.Watch", `{"topic":"a"}`, nil).GetError(); e.GetCode() != hubpb.ErrorCode_INVALID_REQUEST {
		t.Fatalf("a unary request that is not the initial one: %v", e) // the library's own answer
	}
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "w1#0", Kind: qry, Type: "conformance.Watch", Payload: []byte(`{"topic":"a"}`)}))
	r := nextOf(t, s).GetReply()
	if r.GetRequestId() != "w1#0" || string(r.GetPayload()) != `{"topic":"a","value":0,"instanceId":"i-1"}` {
		t.Fatalf("initial %v", r)
	}
	if v := decode(t, call(t, s, qry, "conformance.View", `{}`, nil)); v["subscriptionsServed"] != float64(1) {
		t.Fatalf("View %v", v)
	}
	// What Emit sends goes out before its answer.
	emit := func(id, payload string) {
		t.Helper()
		s.Send(t, fakehub.Forward(&hubpb.Request{Id: id, Kind: cmd, Type: "conformance.Emit", Payload: []byte(payload)}))
	}
	answered := func(id string) {
		t.Helper()
		if r := nextOf(t, s).GetReply(); r.GetRequestId() != id || string(r.GetPayload()) != "{}" {
			t.Fatalf("Emit %s: %v", id, r)
		}
	}
	emit("e-1", `{"topic":"a","values":[1,"two"]}`)
	for seq, want := range []string{`{"topic":"a","value":1}`, `{"topic":"a","value":"two"}`} {
		p := nextOf(t, s).GetPart()
		if p.GetRequestId() != "w1#1" || p.GetSeq() != uint64(seq+1) || string(p.GetPayload()) != want {
			t.Fatalf("update %d: %v", seq+1, p)
		}
	}
	answered("e-1")
	emit("e-2", `{"topic":"a","complete":true}`)
	if e := nextOf(t, s).GetEnd(); e.GetRequestId() != "w1#1" || e.GetError() != nil || e.GetParts() != 2 {
		t.Fatalf("end %v", e)
	}
	answered("e-2")
	s.Send(t, watchRequest("w2#1", "b"))
	emit("e-3", `{"topic":"b","values":[],"fail":"feed closed"}`)
	if e := nextOf(t, s).GetEnd(); e.GetRequestId() != "w2#1" || e.GetError().GetCode() != hubpb.ErrorCode_HANDLER_ERROR ||
		e.GetError().GetMessage() != "feed closed" {
		t.Fatalf("end %v", e)
	}
	answered("e-3")
	// CONFORMANCE_MAX_SUBSCRIPTIONS 2: a third is refused.
	for _, id := range []string{"w3#1", "w4#1", "w5#1"} {
		s.Send(t, watchRequest(id, "c"))
	}
	if e := nextOf(t, s).GetEnd(); e.GetRequestId() != "w5#1" || e.GetError().GetCode() != hubpb.ErrorCode_RESOURCE_EXHAUSTED {
		t.Fatalf("end %v", e)
	}
}

func TestSubscribeGoesThroughTheLibrarysSubscribe(t *testing.T) {
	app, _, s := started(t, env{service: "conformance-go", instance: "i-1", capacity: 4})
	s.Ack(t, fakehub.Ack("hub-1"))
	ready(t, app)
	const cmd = hubpb.RequestKind_COMMAND
	part := func(id string, seq uint64) *hubpb.ServerMessage {
		return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Part{Part: &hubpb.ReplyPart{RequestId: id, Seq: seq,
			ResponderService: "conformance-runner", ResponderInstance: "p1", Payload: []byte(fmt.Sprintf(`{"i":%d}`, seq))}}}
	}
	end := func(id string, parts uint32) *hubpb.ServerMessage {
		return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_End{End: &hubpb.ReplyEnd{RequestId: id, Parts: parts}}}
	}

	// The initial result and the updates, to the end, under the credit asked for.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-sub", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"subscribe","target":"probe-watch","payload":{"k":1},"initialCredit":2,"maxResults":5,"timeoutMs":3000}`)}))
	req, _ := nextFrame(t, s)
	if req.GetType() != "conformance.runner.ProbeWatch" || req.GetMode() != hubpb.RequestMode_SUBSCRIPTION ||
		req.GetInitialCredit() != 2 || req.GetMaxResults() != 5 || req.GetTimeoutMs() != 3000 || string(req.GetPayload()) != `{"k":1}` {
		t.Fatalf("the library sent %v", req)
	}
	s.Send(t, part(req.GetId(), 1))
	s.Send(t, part(req.GetId(), 2))
	if f := s.RecvFlow(t); f.GetRequestId() != req.GetId() || f.GetCredits() != 1 {
		t.Fatalf("flow %v", f)
	}
	s.Send(t, end(req.GetId(), 2))
	var reply *hubpb.Reply
	for reply == nil {
		reply = nextOf(t, s).GetReply()
	}
	out := decode(t, reply)
	if out["result"] != "ok" || out["end"] != "completed" || fmt.Sprint(out["items"]) != "[map[i:1] map[i:2]]" {
		t.Fatalf("outcome %v", out)
	}

	// Stop after read 1: the initial result counts, and the subscription is cancelled.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-sub-2", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"subscribe","target":"watch","payload":{"topic":"x"},"read":1}`)}))
	req, _ = nextFrame(t, s)
	if req.GetType() != "conformance.Watch" || req.GetInitialCredit() != 64 || string(req.GetPayload()) != `{"topic":"x"}` {
		t.Fatalf("the library sent %v", req)
	}
	s.Send(t, part(req.GetId(), 1))
	if c := s.RecvCancel(t); c.GetRequestId() != req.GetId() {
		t.Fatalf("cancel %v", c)
	}
	_, reply = nextFrame(t, s)
	if out := decode(t, reply); out["result"] != "ok" || out["end"] != "stopped" || fmt.Sprint(out["items"]) != "[map[i:1]]" {
		t.Fatalf("outcome %v", out)
	}

	// A short end: lost parts, with the items read before it.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-sub-3", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"subscribe","target":"probe-watch"}`)}))
	req, _ = nextFrame(t, s)
	s.Send(t, part(req.GetId(), 1))
	s.Send(t, end(req.GetId(), 3))
	_, reply = nextFrame(t, s)
	out = decode(t, reply)
	if e, _ := out["error"].(map[string]any); out["result"] != "error" || e["kind"] != "lost-parts" || fmt.Sprint(out["items"]) != "[map[i:1]]" {
		t.Fatalf("outcome %v", out)
	}

	// A failed initial result, and a target that does not subscribe.
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-sub-4", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"subscribe","target":"probe-watch"}`)}))
	req, _ = nextFrame(t, s)
	s.Send(t, fakehub.HubError(req.GetId(), hubpb.ErrorCode_NO_HANDLER, "no owner"))
	_, reply = nextFrame(t, s)
	if e, _ := decode(t, reply)["error"].(map[string]any); e["kind"] != "code" || e["code"] != "NO_HANDLER" {
		t.Fatalf("outcome %v", reply)
	}
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-sub-5", Kind: cmd, Type: "conformance.Send",
		Payload: []byte(`{"op":"subscribe","target":"probe-stream"}`)}))
	_, reply = nextFrame(t, s)
	if m := decode(t, reply); m["result"] != "unsupported" {
		t.Fatalf("outcome %v", m)
	}
}
