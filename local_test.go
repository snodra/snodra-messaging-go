package snodra

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// AuditOrder is a command no handler of the tests' apps serves.
type AuditOrder struct {
	OrderID string `json:"orderId"`
}

func (AuditOrder) MessageType() string { return "orders.AuditOrder" }

// ShipOrder is a command whose handler, in the tests, sends on.
type ShipOrder struct {
	OrderID string `json:"orderId"`
}

func (ShipOrder) MessageType() string { return "orders.ShipOrder" }

func TestACommandThisInstanceHandlesIsAnsweredInProcess(t *testing.T) {
	hub := fakehub.Start(t)
	rec := &recorder{}
	seen := make(chan MessageInfo, 1)
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, p PlaceOrder) (Receipt, error) {
			info, _ := MessageFrom(ctx)
			seen <- info
			return Receipt{OrderID: p.OrderID, Total: 7}, nil
		}), nil)
	}, Telemetry(rec))
	r, err := Send[Receipt](context.Background(), app, PlaceOrder{OrderID: "o-1"}, Header("x-tenant", "t-1"),
		RoutingKey("o-1"), Timeout(time.Nanosecond), Priority(9))
	is(t, err, nil)
	is(t, r, Receipt{OrderID: "o-1", Total: 7})
	info := receive(t, seen)
	is(t, info.Type, "orders.PlaceOrder")
	is(t, info.ID != "", true)
	is(t, info.CorrelationID != "" && info.CorrelationID != info.ID, true) // a new correlation id
	is(t, len(info.Headers), 2)
	is(t, info.Headers["x-tenant"], "t-1")
	is(t, info.Headers["correlationId"], info.CorrelationID)
	s.Nothing(t, 200*ms, "request") // the hub saw nothing
	is(t, rec.count(func(r *recorder) int { return len(r.requests) + len(r.handling) }), 0)
}

func TestAQueryThisInstanceHandlesIsAnsweredInProcessUnlessShared(t *testing.T) {
	hub := fakehub.Start(t)
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleQuery(app, get), nil)
		is(t, HandleQuery(app, func(context.Context, ListOrders) (Receipt, error) { return Receipt{}, nil }, Shared()), nil)
	})
	r, err := Query[Receipt](context.Background(), app, GetOrder{OrderID: "o-2"})
	is(t, err, nil)
	is(t, r.OrderID, "o-2")

	// A shared query type goes through the hub, which spreads it over every owner.
	done := make(chan error, 1)
	go func() {
		_, err := Query[Receipt](context.Background(), app, ListOrders{})
		done <- err
	}()
	req := s.RecvRequest(t)
	is(t, req.GetType(), "orders.ListOrders")
	s.Send(t, replyTo(req.GetId(), `{}`))
	is(t, receive(t, done), nil)

	// A query of a command type, and a command of a query type, are not served here.
	go func() {
		_, err := Query[Receipt](context.Background(), app, AuditOrder{})
		done <- err
	}()
	req = s.RecvRequest(t)
	is(t, req.GetType(), "orders.AuditOrder")
	s.Send(t, replyTo(req.GetId(), `{}`))
	is(t, receive(t, done), nil)
}

func TestAlwaysRouteThroughHubSendsEveryRequestToTheHub(t *testing.T) {
	hub := fakehub.Start(t)
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, place), nil)
		is(t, HandleQuery(app, get), nil)
	}, AlwaysRouteThroughHub(true))
	for _, m := range []Message{PlaceOrder{OrderID: "o-1"}, GetOrder{OrderID: "o-1"}} {
		done := make(chan error, 1)
		go func() {
			_, err := Send[Receipt](context.Background(), app, m)
			done <- err
		}()
		req := s.RecvRequest(t)
		is(t, req.GetType(), m.MessageType())
		s.Send(t, replyTo(req.GetId(), `{"orderId":"o-1"}`))
		is(t, receive(t, done), nil)
	}
}

func TestALocalCallNeedsNoHub(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"))
	is(t, HandleCommand(app, place), nil)
	// Before Start: answered in-process; a type served elsewhere is never sent.
	r, err := Send[Receipt](context.Background(), app, PlaceOrder{OrderID: "o-1"})
	is(t, err, nil)
	is(t, r.Total, 42)
	_, err = Send[Receipt](context.Background(), app, AuditOrder{})
	is(t, errors.Is(err, ErrNeverSent), true)
	// Started but no hub acknowledged, and after Shutdown: still in-process.
	is(t, app.Start(context.Background()), nil)
	_, err = Send[Receipt](context.Background(), app, PlaceOrder{})
	is(t, err, nil)
	is(t, app.Shutdown(ctxWithin(t, 5*s)), nil)
	_, err = Send[Receipt](context.Background(), app, PlaceOrder{})
	is(t, err, nil)
}

func TestALocalCallActsForTheHandledRequestsCaller(t *testing.T) {
	srv, _ := testJWKS(t)
	key, _ := testKeys(t)
	token := "Bearer " + mint(t, key, "k1", userClaims("alice", time.Hour, map[string]any{"roles": []any{"shipper"}}))
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	hub := fakehub.Start(t)
	inner := make(chan MessageInfo, 1)
	callers := make(chan Caller, 1)
	app, s := serving(t, hub, func(app *App) {
		// The outer handler, reached through the hub, sends ShipOrder, which this instance handles.
		is(t, HandleCommand(app, func(ctx context.Context, p PlaceOrder) (Receipt, error) {
			return Send[Receipt](ctx, app, ShipOrder{OrderID: p.OrderID})
		}), nil)
		is(t, HandleCommand(app, func(ctx context.Context, p ShipOrder) (Receipt, error) {
			info, _ := MessageFrom(ctx)
			inner <- info
			c, _ := CallerFrom(ctx)
			callers <- c
			// Sent on from the in-process handler: the caller's token and trace go on.
			return Send[Receipt](ctx, app, AuditOrder(p))
		}, RequireRole("shipper")), nil)
	}, JWKS(srv.URL, testIssuer), MaxConcurrent(1)) // one handler slot: the local call takes none
	s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
		Payload: []byte(`{"orderId":"o-1"}`), Headers: map[string]string{"authorization": token,
			"correlationId": "c-1", "traceparent": traceparent}}))
	req := s.RecvRequest(t)
	is(t, req.GetType(), "orders.AuditOrder")
	is(t, req.GetHeaders()["authorization"], token)
	is(t, req.GetHeaders()["correlationId"], "c-1")
	is(t, req.GetHeaders()["traceparent"], traceparent)
	info := receive(t, inner)
	is(t, info.CorrelationID, "c-1")
	is(t, info.ID != "r-1", true)
	_, hasAuth := info.Headers["authorization"]
	is(t, hasAuth, false) // the in-process request's headers: the caller's own and the correlation id
	is(t, receive(t, callers).Subject, "alice")
	s.Send(t, replyTo(req.GetId(), `{"orderId":"o-1","total":3}`))
	r := nextReply(t, s)
	is(t, r.GetRequestId(), "r-1")
	is(t, string(r.GetPayload()), `{"orderId":"o-1","total":3}`)
	_ = app
}

func TestALocalCallFailsAsThroughTheHub(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"), DefaultRetry(RetryPolicy{MaxAttempts: 3, InitialBackoff: ms, Multiplier: 2, MaxBackoff: ms}),
		JWKS("https://issuer.test/jwks", testIssuer))
	calls := 0
	is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
		calls++
		return Empty{}, WithDetails(errors.New("out of stock"), []string{"sku-1"})
	}), nil)
	is(t, HandleCommand(app, func(context.Context, ShipOrder) (Empty, error) { panic("boom") }), nil)
	is(t, HandleCommand(app, func(context.Context, *CancelOrder) (Empty, error) { return Empty{}, nil },
		RequireAuthenticated()), nil)
	is(t, HandleQuery(app, func(context.Context, GetOrder) (string, error) { return "not a receipt", nil }), nil)

	var e *Error
	_, err := Send[Empty](context.Background(), app, PlaceOrder{})
	is(t, errors.As(err, &e), true)
	is(t, e.Code, CodeHandlerError)
	is(t, e.Message, "out of stock")
	is(t, string(e.Details), `["sku-1"]`)
	is(t, calls, 1) // never retried

	_, err = Send[Empty](context.Background(), app, ShipOrder{})
	is(t, errors.As(err, &e), true)
	is(t, e.Code, CodeHandlerError)
	is(t, e.Message, "handler panicked")

	_, err = Send[Empty](context.Background(), app, &CancelOrder{}) // an anonymous caller
	is(t, errors.As(err, &e), true)
	is(t, e.Code, CodeUnauthenticated)

	_, err = Query[Receipt](context.Background(), app, GetOrder{}) // the answer does not decode
	is(t, err != nil && !errors.As(err, &e), true)

	_, err = Send[Empty](context.Background(), app, PlaceOrder{}, Header("authorization", "x"))
	is(t, errors.Is(err, ErrReservedHeader), true)
	is(t, calls, 1)
}

func TestALocalCallRunsUnderTheCallersContext(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"))
	started := make(chan struct{})
	is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
		close(started)
		<-ctx.Done()
		return Empty{}, ctx.Err()
	}), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Send[Empty](ctx, app, PlaceOrder{})
		done <- err
	}()
	<-started
	cancel()
	// As a written request whose caller gave up: the context's error, and an unknown outcome.
	err := receive(t, done)
	is(t, errors.Is(err, context.Canceled), true)
	is(t, errors.Is(err, ErrOutcomeUnknown), true)

	// The handler's own *Error stays reachable with errors.As.
	started2 := make(chan struct{})
	is(t, HandleQuery(app, func(ctx context.Context, _ GetOrder) (Receipt, error) {
		close(started2)
		<-ctx.Done()
		return Receipt{}, &Error{Code: CodeInvalidRequest, Message: "refused"}
	}), nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		_, err := Query[Receipt](ctx2, app, GetOrder{})
		done2 <- err
	}()
	<-started2
	cancel2()
	err = receive(t, done2)
	is(t, errors.Is(err, context.Canceled), true)
	is(t, errors.Is(err, ErrOutcomeUnknown), true)
	var he *Error
	is(t, errors.As(err, &he), true)
	is(t, strings.Contains(he.Message, "refused"), true)

	// A context that ended before the call: the handler never runs.
	_, err = Send[Empty](ctx, app, PlaceOrder{})
	is(t, errors.Is(err, ErrNeverSent), true)
	is(t, errors.Is(err, context.Canceled), true)
}

// A local call acts for the caller the hub path would carry: the handled request's caller when
// its token is forwarded, nobody when ForwardJWT is off.
func TestALocalCallSeesTheCallerTheHubWouldCarry(t *testing.T) {
	srv, _ := testJWKS(t)
	key, _ := testKeys(t)
	token := "Bearer " + mint(t, key, "k1", userClaims("alice", time.Hour, nil))
	seen := func(t *testing.T, forward, throughHub bool) string {
		hub := fakehub.Start(t)
		callers := make(chan string, 2)
		_, s := serving(t, hub, func(app *App) {
			is(t, HandleCommand(app, func(ctx context.Context, p PlaceOrder) (Receipt, error) {
				return Send[Receipt](ctx, app, ShipOrder{OrderID: p.OrderID})
			}), nil)
			is(t, HandleCommand(app, func(ctx context.Context, _ ShipOrder) (Receipt, error) {
				c, _ := CallerFrom(ctx)
				callers <- c.Subject
				return Receipt{}, nil
			}), nil)
		}, JWKS(srv.URL, testIssuer), ForwardJWT(forward), AlwaysRouteThroughHub(throughHub))
		s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-1", Kind: hubpb.RequestKind_COMMAND, Type: "orders.PlaceOrder",
			Payload: []byte(`{}`), Headers: map[string]string{"authorization": token}}))
		if throughHub { // the hub forwards ShipOrder back to this instance, as it was sent
			r := s.RecvRequest(t)
			s.Send(t, fakehub.Forward(&hubpb.Request{Id: "r-2", Kind: r.GetKind(), Type: r.GetType(),
				Payload: r.GetPayload(), Headers: r.GetHeaders()}))
			reply := nextReply(t, s)
			is(t, reply.GetRequestId(), "r-2")
			s.Send(t, replyTo(r.GetId(), string(reply.GetPayload())))
		}
		is(t, nextReply(t, s).GetRequestId(), "r-1")
		return receive(t, callers)
	}
	for _, forward := range []bool{true, false} {
		viaHub := seen(t, forward, true)
		local := seen(t, forward, false)
		if local != viaHub {
			t.Fatalf("ForwardJWT(%v): the local handler saw %q, through the hub %q", forward, local, viaHub)
		}
	}
	is(t, seen(t, true, false), "alice")
	is(t, seen(t, false, false), "")
}

// The handler of a local call gets its own copy of the message, through the codec: what it
// changes the caller never sees, as through a hub.
func TestALocalCallGoesThroughTheCodec(t *testing.T) {
	app := newApp(t, Hubs("127.0.0.1:1"))
	is(t, HandleCommand(app, func(_ context.Context, c *CancelOrder) (Empty, error) {
		c.ID = "changed by the handler"
		return Empty{}, nil
	}), nil)
	is(t, HandleQuery(app, getOrderHandler), nil)
	msg := &CancelOrder{ID: "o-1"}
	_, err := Send[Empty](context.Background(), app, msg)
	is(t, err, nil)
	is(t, msg.ID, "o-1")
	// A query of a streaming type this instance serves: INVALID_REQUEST, as the hub path answers.
	app2 := newApp(t, Hubs("127.0.0.1:1"))
	is(t, HandleStream(app2, listOrders), nil)
	_, err = Query[Receipt](context.Background(), app2, ListOrders{})
	var e *Error
	is(t, errors.As(err, &e), true)
	is(t, e.Code, CodeInvalidRequest)
	is(t, e.Message, "orders.ListOrders is a streaming query: send it with Stream (mode STREAMING)")
}

// getOrderHandler is a query handler of another type, so that the app has more than one handler.
func getOrderHandler(_ context.Context, q GetOrder) (Receipt, error) {
	return Receipt{OrderID: q.OrderID}, nil
}
