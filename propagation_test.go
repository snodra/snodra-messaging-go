package snodra

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/traceparent"
)

const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// relaying serves orders.PlaceOrder with a handler that sends orders.CancelOrder on, as an
// application does inside a handler, and fails with that call's error.
func relaying(t *testing.T, opts ...Option) *fakehub.Stream {
	t.Helper()
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
			return Send[Empty](ctx, app, &CancelOrder{ID: "o-1"})
		}), nil)
	}, opts...)
	return s
}

// nested forwards orders.PlaceOrder with id and headers and returns the request its handler
// sent, answering it and checking the handler's reply.
func nested(t *testing.T, s *fakehub.Stream, id string, headers map[string]string) *hubpb.Request {
	t.Helper()
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: id, Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
		Payload: []byte(`{}`), Headers: headers}))
	r := s.RecvRequest(t)
	is(t, r.GetType(), "orders.CancelOrder")
	s.Send(t, replyTo(r.GetId(), `{}`))
	reply := nextReply(t, s)
	is(t, reply.GetRequestId(), id)
	is(t, reply.GetError() == nil, true)
	return r
}

func TestANestedRequestCarriesTheHandledContext(t *testing.T) {
	s := relaying(t)
	bearer := "Bearer " + readableToken(t, map[string]any{"sub": "u-1"}) // read unverified: no JWKS
	r := nested(t, s, "r-1", map[string]string{"correlationId": "c-1", "authorization": bearer, "traceparent": tp, "x-tenant": "t-1"})
	h := r.GetHeaders()
	is(t, h["correlationId"], "c-1")
	is(t, h["authorization"], bearer)
	is(t, traceparent.TraceID(h["traceparent"]), "4bf92f3577b34da6a3ce929d0e0e4736")
	_, carried := h["x-tenant"]
	is(t, carried, false) // other headers are the handled request's, not carried on
}

func TestTheHandledIDStandsInForAMissingCorrelationID(t *testing.T) {
	s := relaying(t)
	h := nested(t, s, "r-9", nil).GetHeaders()
	is(t, h["correlationId"], "r-9")
	_, auth := h["authorization"]
	is(t, auth, false)
	_, trace := h["traceparent"]
	is(t, trace, false)
}

func TestAnEmptyCorrelationIDIsReplacedByAFreshOne(t *testing.T) {
	s := relaying(t)
	h := nested(t, s, "r-1", map[string]string{"correlationId": ""}).GetHeaders()
	is(t, uuidV4.MatchString(h["correlationId"]), true) // not the handled id
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestOutsideAnyHandlingTheCorrelationIDIsFresh(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx := ctxWithin(t, 5*time.Second)
	seen := map[string]bool{}
	for range 2 {
		sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
		r := s.RecvRequest(t)
		id := r.GetHeaders()["correlationId"]
		is(t, uuidV4.MatchString(id), true)
		is(t, len(r.GetHeaders()), 1) // no authorization, no traceparent
		seen[id] = true
		s.Send(t, replyTo(r.GetId(), `{}`))
		is(t, await(t, sent).err, nil)
	}
	is(t, len(seen), 2) // a new one per send
}

func TestOnlyAValidTraceparentIsForwarded(t *testing.T) {
	is(t, traceparent.Valid(tp), true)
	invalid := []string{"", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01", "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01"}
	for _, bad := range invalid {
		is(t, traceparent.Valid(bad), false)
	}
	s := relaying(t)
	for i, bad := range invalid[1:] { // the nested request of a handled invalid one carries none
		h := nested(t, s, "r-"+string(rune('a'+i)), map[string]string{"traceparent": bad}).GetHeaders()
		_, trace := h["traceparent"]
		is(t, trace, false)
	}
}

func TestReservedHeadersAreRefusedBeforeAnythingIsSent(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	ctx := ctxWithin(t, 5*time.Second)
	for _, h := range []string{"authorization", "traceparent", "tracestate", "correlationId", "correlation-id",
		"AUTHORIZATION", "CORRELATIONID", "Correlation-Id", "TraceState"} {
		_, err := Send[Empty](ctx, app, PlaceOrder{}, Header("x-tenant", "t-1"), Header(h, "x"))
		is(t, errors.Is(err, ErrReservedHeader), true)
		is(t, errors.Is(err, ErrNeverSent), false) // a refusal, not a failure to send
	}
	s.Nothing(t, 200*time.Millisecond, "request")
	// Any other header reaches the hub, with the library's own.
	sent := async(func() (Empty, error) {
		return Send[Empty](ctx, app, PlaceOrder{}, Header("x-tenant", "t-1"), Header("x-trace-note", "n"))
	})
	r := s.RecvRequest(t)
	is(t, r.GetHeaders()["x-tenant"], "t-1")
	is(t, r.GetHeaders()["x-trace-note"], "n")
	is(t, uuidV4.MatchString(r.GetHeaders()["correlationId"]), true)
	s.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

func TestForwardJWTOff(t *testing.T) {
	s := relaying(t, ForwardJWT(false))
	bearer := "Bearer " + readableToken(t, map[string]any{"sub": "u-1"})
	h := nested(t, s, "r-1", map[string]string{"correlationId": "c-1", "authorization": bearer, "traceparent": tp}).GetHeaders()
	_, auth := h["authorization"]
	is(t, auth, false)
	is(t, h["correlationId"], "c-1") // the rest still travels
	is(t, h["traceparent"], tp)
}

func TestOutgoingHeaders(t *testing.T) {
	ctx := withHandling(context.Background(), &handling{
		info:          MessageInfo{ID: "r-1", CorrelationID: "c-1"},
		authorization: "Bearer t",
		traceparent:   tp,
	})
	own := map[string]string{"x-tenant": "t-1"}
	got := outgoingHeaders(ctx, own, true)
	is(t, len(got), 4)
	is(t, got["x-tenant"], "t-1")
	is(t, got["correlationId"], "c-1")
	is(t, got["authorization"], "Bearer t")
	is(t, got["traceparent"], tp)
	is(t, len(own), 1) // the caller's map is not changed
	is(t, len(outgoingHeaders(ctx, nil, false)), 2)
}
