package snodra

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/traceparent"
)

func TestRequestOutcomeLabels(t *testing.T) {
	cases := map[string]error{"ok": nil, "no_handler": &Error{Code: CodeNoHandler}, "timeout": &Error{Code: CodeTimeout},
		"handler_disconnected": &Error{Code: CodeHandlerDisconnected}, "resource_exhausted": &Error{Code: CodeResourceExhausted},
		"handler_error": &Error{Code: CodeHandlerError}, "denied": &Error{Code: CodePermissionDenied},
		"invalid_request": &Error{Code: CodeInvalidRequest}, "cancelled": &Error{Code: CodeCancelled},
		"unavailable": ErrOutcomeUnknown, "error": errors.New("x")}
	for want, err := range cases {
		is(t, requestOutcome(err), want)
	}
	is(t, requestOutcome(&Error{Code: CodeUnauthenticated}), "denied")
	is(t, requestOutcome(ErrNeverSent), "unavailable")
	is(t, requestOutcome(&Error{Code: Code(77)}), "handler_error") // an unknown code reads as HANDLER_ERROR
	is(t, requestOutcome(&undecodableReply{typ: "t", err: errors.New("x")}), "invalid_request")
	// The caller's own deadline: not "unavailable", whatever else the error wraps.
	is(t, requestOutcome(&localRefusal{ErrReservedHeader}), "invalid_request")
	is(t, requestOutcome(errors.Join(ErrOutcomeUnknown, context.DeadlineExceeded)), "timeout")
}

// recorder is an Instrumentation keeping what it is told.
type recorder struct {
	NopInstrumentation
	mu       sync.Mutex
	requests []requestRecord
	tokens   []string
	rotated  int
	refresh  int
	read     func() Gauges
	handling []Handling
	dones    []error
	inject   func(ctx context.Context, h map[string]string)
}

type requestRecord struct {
	kind, typ, outcome string
	took               time.Duration
}

func (r *recorder) RequestDone(kind, typ, outcome string, took time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, requestRecord{kind, typ, outcome, took})
}

func (r *recorder) TokenFetched(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokens = append(r.tokens, result)
}

func (r *recorder) CertificateRotated() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotated++
}

func (r *recorder) CredentialsRefreshSent() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refresh++
}

func (r *recorder) ObserveGauges(read func() Gauges) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.read = read
}

func (r *recorder) StartHandling(ctx context.Context, h Handling) (context.Context, func(error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handling = append(r.handling, h)
	return ctx, func(err error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.dones = append(r.dones, err)
	}
}

func (r *recorder) Inject(ctx context.Context, h map[string]string) {
	if r.inject != nil {
		r.inject(ctx, h)
	}
}

func (r *recorder) snapshot() (reqs []requestRecord, tokens []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]requestRecord(nil), r.requests...), append([]string(nil), r.tokens...)
}

func (r *recorder) count(f func(*recorder) int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return f(r)
}

func TestOneRequestRecordPerCall(t *testing.T) {
	rec := &recorder{}
	app, _, s := sending(t, fakehub.Ack("hub-1"), Telemetry(rec))
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * ms, Multiplier: 2, MaxBackoff: 100 * ms}
	sent := async(func() (Empty, error) {
		return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}, Retry(policy))
	})
	for range 2 { // two refusals, retried
		s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	}
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{}`))
	is(t, await(t, sent).err, nil)
	reqs, _ := rec.snapshot()
	is(t, len(reqs), 1)
	is(t, reqs[0].kind, "command")
	is(t, reqs[0].typ, "orders.PlaceOrder")
	is(t, reqs[0].outcome, "ok")
	within(t, reqs[0].took, 20*ms, 5*time.Second) // the backoffs are in it

	// A query that fails, and a call that never reaches a hub.
	q := async(func() (Receipt, error) { return Query[Receipt](ctxWithin(t, 5*time.Second), app, GetOrder{}) })
	s.Send(t, failWith(s.RecvRequest(t).GetId(), hubpb.ErrorCode_HANDLER_ERROR, "boom", ""))
	is(t, await(t, q).err != nil, true)
	reqs, _ = rec.snapshot()
	is(t, len(reqs), 2)
	is(t, reqs[1], requestRecord{"query", "orders.GetOrder", "handler_error", reqs[1].took})

	_, err := Send[Empty](context.Background(), app, PlaceOrder{}, Header("traceparent", "x"))
	is(t, errors.Is(err, ErrReservedHeader), true)
	reqs, _ = rec.snapshot()
	is(t, len(reqs), 2) // a call its options refused is no call
}

func TestAnUndecodableReplyIsInvalidRequest(t *testing.T) {
	rec := &recorder{}
	app, _, s := sending(t, fakehub.Ack("hub-1"), Telemetry(rec))
	sent := async(func() (Receipt, error) { return Send[Receipt](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{"total":"many"}`))
	is(t, await(t, sent).err != nil, true)
	reqs, _ := rec.snapshot()
	is(t, len(reqs), 1)
	is(t, reqs[0].outcome, "invalid_request")
}

func TestTokenFetchesAreCounted(t *testing.T) {
	rec := &recorder{}
	fetch := func(h http.HandlerFunc) {
		t.Helper()
		srv := httptest.NewServer(h)
		app, err := New(Service("orders-service"), Hubs("hub:7000"), Telemetry(rec),
			JWT(NewClientCredentials(ClientCredentials{TokenURL: srv.URL, ClientID: "a", ClientSecret: "b"})))
		is(t, err, nil)
		_, _ = app.cfg.tokenSource.Token(context.Background())
		srv.Close()
	}
	fetch(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"a","expires_in":60}`))
	})
	fetch(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusInternalServerError) })
	fetch(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_client"})
	})
	fetch(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"expires_in":60}`)) })
	// Refused: the endpoint is gone.
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	app, err := New(Service("orders-service"), Hubs("hub:7000"), Telemetry(rec),
		JWT(NewClientCredentials(ClientCredentials{TokenURL: url, ClientID: "a", ClientSecret: "b"})))
	is(t, err, nil)
	_, err = app.cfg.tokenSource.Token(context.Background())
	is(t, err != nil, true)
	_, tokens := rec.snapshot()
	is(t, len(tokens), 5)
	for i, want := range []string{"ok", "http_error", "oauth_error", "bad_response", "io_error"} {
		is(t, tokens[i], want)
	}
}

func TestCredentialRefreshesAndCertificateRotationsAreCounted(t *testing.T) {
	rec := &recorder{}
	path := filepath.Join(t.TempDir(), "token")
	writeFile(t, path, "t-1")
	hub := fakehub.Start(t)
	startApp(t, Hubs(hub.Addr()), TokenFile(path), Telemetry(rec))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, ackEvery("hub-1", 50*ms))
	writeFile(t, path, "t-2")
	is(t, s.RecvCredentials(t).GetToken(), "t-2")
	poll(t, func() bool { return rec.count(func(r *recorder) int { return r.refresh }) == 1 })
	is(t, rec.count(func(r *recorder) int { return r.rotated }), 0)

	// A renewed certificate pair counts one rotation, the first one none.
	ca := newTestCA(t, "operator CA")
	thub := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	dir := t.TempDir()
	certFile, keyFile := pairFiles(t, dir, ca.client(t, "orders-service", time.Now().Add(time.Hour)))
	tm := scaled()
	tm.certCheckInterval, tm.rollDelayMax = 50*ms, 0
	crec := &recorder{}
	app := newApp(t, Hubs(thub.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}),
		ClientCertificate(certFile, keyFile), withTimings(tm), Telemetry(crec))
	is(t, app.Start(context.Background()), nil)
	cs := thub.Next(t)
	cs.RecvRegister(t)
	cs.Ack(t, fakehub.Ack("hub-1"))
	poll(t, func() bool { return app.Ready(ctxWithin(t, time.Second)) == nil })
	is(t, crec.count(func(r *recorder) int { return r.rotated }), 0)
	pairFiles(t, dir, ca.client(t, "orders-service", time.Now().Add(2*time.Hour)))
	cs.RecvRegister(t) // out of the routing first
	cs.HalfClosed(t, time.Second)
	cs.Close(codes.OK, "")
	cs2 := thub.Next(t)
	cs2.RecvRegister(t)
	cs2.Ack(t, fakehub.Ack("hub-1"))
	poll(t, func() bool { return crec.count(func(r *recorder) int { return r.rotated }) == 1 })
}

func TestGaugesReadTheLiveState(t *testing.T) {
	rec := &recorder{}
	a, b := fakehub.Start(t), fakehub.Start(t)
	app := newApp(t, Hubs(a.Addr(), b.Addr()), Telemetry(rec))
	running, release := make(chan struct{}, 1), make(chan struct{})
	is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) {
		running <- struct{}{}
		<-release
		return Empty{}, nil
	}), nil)
	is(t, rec.read == nil, true) // called by Start, not before
	is(t, app.Start(context.Background()), nil)
	sa, sb := a.Next(t), b.Next(t)
	for i, s := range []*fakehub.Stream{sa, sb} {
		s.RecvRegister(t)
		s.Recv(t)
		s.Ack(t, fakehub.Ack([]string{"hub-a", "hub-b"}[i]))
	}
	rec.mu.Lock()
	read := rec.read
	rec.mu.Unlock()
	is(t, read != nil, true)
	poll(t, func() bool { return read().ConnectedHubs == 2 })
	is(t, read(), Gauges{ConnectedHubs: 2})
	sa.Send(t, command("r-1", "orders.PlaceOrder", `{}`))
	receive(t, running)
	is(t, read(), Gauges{HandlersInFlight: 1, ConnectedHubs: 2})
	close(release)
	is(t, nextReply(t, sa).GetRequestId(), "r-1")
	poll(t, func() bool { return read().HandlersInFlight == 0 })
}

func TestStartHandlingGetsTheRequestAndItsResult(t *testing.T) {
	rec := &recorder{}
	hub := fakehub.Start(t)
	_, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(_ context.Context, m PlaceOrder) (Empty, error) {
			switch m.OrderID {
			case "panic":
				panic("boom")
			case "fail":
				return Empty{}, errors.New("failed")
			}
			return Empty{}, nil
		}), nil)
		is(t, HandleQuery(app, func(context.Context, GetOrder) (Receipt, error) { return Receipt{}, nil }), nil)
	}, Telemetry(rec))
	send := func(id, typ, payload string, headers map[string]string) {
		t.Helper()
		kind := hubpb.RequestKind_COMMAND
		if typ == "orders.GetOrder" {
			kind = hubpb.RequestKind_QUERY
		}
		s.Send(t, fakehub.Forward(&hubpb.Request{Id: id, Kind: kind, Type: typ, Payload: []byte(payload), Headers: headers}))
		is(t, nextReply(t, s).GetRequestId(), id)
	}
	send("r-1", "orders.PlaceOrder", `{"orderId":"ok"}`, map[string]string{"traceparent": tp, "tracestate": "v=1"})
	send("r-2", "orders.GetOrder", `{}`, map[string]string{"traceparent": "garbage", "tracestate": "v=1"})
	send("r-3", "orders.PlaceOrder", `{"orderId":"fail"}`, nil)
	send("r-4", "orders.PlaceOrder", `{"orderId":"panic"}`, nil)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	is(t, len(rec.handling), 4)
	is(t, rec.handling[0], Handling{Type: "orders.PlaceOrder", Kind: "command", Traceparent: tp, Tracestate: "v=1"})
	is(t, rec.handling[1], Handling{Type: "orders.GetOrder", Kind: "query"}) // an invalid traceparent is none, its state too
	is(t, len(rec.dones), 4)
	is(t, rec.dones[0], nil)
	is(t, rec.dones[1], nil)
	is(t, rec.dones[2].Error(), "failed")
	is(t, rec.dones[3].Error(), "handler panicked")
}

func TestInjectMayReplaceTheForwardedTraceparent(t *testing.T) {
	const child = "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01"
	rec := &recorder{}
	s := relaying(t, Telemetry(rec))
	rec.inject = func(_ context.Context, h map[string]string) {
		is(t, h["traceparent"], tp) // what is forwarded is what it starts from
		h["traceparent"], h["tracestate"], h["authorization"] = child, "v=2", "Bearer stolen"
	}
	h := nested(t, s, "r-1", map[string]string{"traceparent": tp}).GetHeaders()
	is(t, h["traceparent"], child)
	is(t, h["tracestate"], "v=2")
	_, hijacked := h["authorization"] // only the trace context can be written
	is(t, hijacked, false)

	rec.inject = func(_ context.Context, h map[string]string) { h["traceparent"], h["tracestate"] = "not-valid", "v=3" }
	h = nested(t, s, "r-2", map[string]string{"traceparent": tp}).GetHeaders()
	is(t, h["traceparent"], tp) // an invalid one is dropped
	_, state := h["tracestate"]
	is(t, state, false)
	h = nested(t, s, "r-3", nil).GetHeaders()
	_, carried := h["traceparent"]
	is(t, carried, false)
	is(t, traceparent.Valid(child), true)
}

// panicking panics in every method.
type panicking struct{}

func (panicking) RequestDone(string, string, string, time.Duration) { panic("RequestDone") }
func (panicking) TokenFetched(string)                               { panic("TokenFetched") }
func (panicking) CertificateRotated()                               { panic("CertificateRotated") }
func (panicking) CredentialsRefreshSent()                           { panic("CredentialsRefreshSent") }
func (panicking) ObserveGauges(func() Gauges)                       { panic("ObserveGauges") }
func (panicking) StartHandling(context.Context, Handling) (context.Context, func(error)) {
	panic("StartHandling")
}
func (panicking) Inject(context.Context, map[string]string) { panic("Inject") }

func TestAPanickingImplementationIsRecoveredAndLoggedOnce(t *testing.T) {
	logs := &logRecorder{}
	hub := fakehub.Start(t)
	app, s := serving(t, hub, func(app *App) {
		is(t, HandleCommand(app, func(ctx context.Context, _ PlaceOrder) (Empty, error) {
			return Send[Empty](ctx, app, &CancelOrder{ID: "o-1"})
		}), nil)
	}, Telemetry(panicking{}), Logger(slog.New(logs)))
	nested(t, s, "r-1", map[string]string{"traceparent": tp}) // the handler ran, the nested send went out
	nested(t, s, "r-2", nil)
	app.tel.tokenFetched("ok")
	app.tel.certificateRotated()
	app.tel.credentialsRefreshSent()
	// A direct call has no peer to answer: it is sent, its record panics, the call returns.
	sent := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}) })
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{}`))
	is(t, await(t, sent).err, nil)
	is(t, logs.count(slog.LevelError, "the telemetry implementation panicked; it keeps being called, its later panics are not logged"), 1)
}

func TestWithoutTelemetryNothingIsAllocatedOrRead(t *testing.T) {
	for name, tel := range map[string]*telemetry{
		"none":      newTelemetry(nil, slog.Default()),
		"nop":       newTelemetry(NopInstrumentation{}, slog.Default()),
		"nil value": nil,
	} {
		ctx := context.Background()
		headers := map[string]string{"traceparent": tp}
		allocs := testing.AllocsPerRun(100, func() {
			started := tel.now()
			tel.requestDone("command", "orders.PlaceOrder", started, nil)
			tel.tokenFetched("ok")
			tel.certificateRotated()
			tel.credentialsRefreshSent()
			_, done := tel.startHandling(ctx, Handling{Type: "orders.PlaceOrder", Kind: "command"})
			if done != nil {
				t.Error("a handling is traced")
			}
			tel.inject(ctx, headers)
		})
		if allocs != 0 {
			t.Errorf("%s: %v allocations", name, allocs)
		}
		is(t, tel.now().IsZero(), true)
	}
	// NopInstrumentation, called directly, is a no-op too.
	var nop Instrumentation = NopInstrumentation{}
	c, done := nop.StartHandling(context.Background(), Handling{})
	done(nil)
	is(t, c, context.Background())
}

// readingInside reads the gauges inside ObserveGauges, as an implementation primed on
// registration would.
type readingInside struct {
	NopInstrumentation
	got chan Gauges
}

func (r *readingInside) ObserveGauges(read func() Gauges) {
	// Read in a goroutine and give up after a while: a read that waits for a lock the caller
	// holds fails the test at once instead of hanging it until the test timeout.
	res := make(chan Gauges, 1)
	go func() { res <- read() }()
	select {
	case g := <-res:
		r.got <- g
	case <-time.After(2 * time.Second):
		r.got <- Gauges{ConnectedHubs: -1} // sentinel: the read did not return
	}
}

func TestStartDoesNotHoldItsLockWhileTheImplementationReadsTheGauges(t *testing.T) {
	rec := &readingInside{got: make(chan Gauges, 1)}
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()), Telemetry(rec))
	started := make(chan error, 1)
	go func() { started <- app.Start(context.Background()) }()
	select {
	case err := <-started:
		is(t, err, nil)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return: ObserveGauges is called with the app's lock held")
	}
	got := <-rec.got
	if got.ConnectedHubs == -1 {
		t.Fatal("the gauges read did not return: ObserveGauges is called with the app's lock held")
	}
	is(t, got.ConnectedHubs, 0)
}

func TestAShutdownBeforeTheRegistrationRegistersNothing(t *testing.T) {
	rec := &stopping{}
	tel := newTelemetry(rec, slog.New(slog.DiscardHandler))
	tel.stopGauges() // Shutdown completed between start and observeGauges
	tel.observeGauges(func() Gauges { return Gauges{} })
	is(t, rec.read == nil, true)
	is(t, rec.stopped.Load(), int32(0))

	// The usual order: registered, then ended once.
	rec2 := &stopping{}
	tel2 := newTelemetry(rec2, slog.New(slog.DiscardHandler))
	tel2.observeGauges(func() Gauges { return Gauges{} })
	tel2.stopGauges()
	tel2.stopGauges()
	is(t, rec2.read != nil, true)
	is(t, rec2.stopped.Load(), int32(1))
}

func TestAnOlderCertificateComingBackIsNotARotation(t *testing.T) {
	rec := &recorder{}
	app := newApp(t, Hubs("127.0.0.1:1"), Telemetry(rec))
	rotations := func() int { return rec.count(func(r *recorder) int { return r.rotated }) }
	app.certPresented("aaaa") // the first pair: no rotation
	is(t, rotations(), 0)
	app.certPresented("bbbb") // renewed
	is(t, rotations(), 1)
	app.certPresented("bbbb") // a second hub rolls over to it
	app.certPresented("aaaa") // a hub reconnects with the older pair
	app.certPresented("bbbb")
	is(t, rotations(), 1)
	app.certPresented("cccc") // renewed again
	is(t, rotations(), 2)
}

func TestACallersCancellationRecordsNothing(t *testing.T) {
	rec := &recorder{}
	app, _, s := sending(t, fakehub.Ack("hub-1"), Telemetry(rec))
	ctx, cancel := context.WithCancel(context.Background())
	sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
	s.RecvRequest(t)
	cancel() // written, unanswered
	is(t, errors.Is(await(t, sent).err, context.Canceled), true)

	// Cancelled while waiting to retry after a NO_HANDLER: still the caller's cancel.
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: 2 * time.Second, Multiplier: 2, MaxBackoff: 4 * time.Second}
	ctx2, cancel2 := context.WithCancel(context.Background())
	sent2 := async(func() (Empty, error) {
		return Send[Empty](ctx2, app, PlaceOrder{}, Retry(policy), Timeout(30*time.Second))
	})
	s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	time.AfterFunc(100*ms, cancel2)
	is(t, errors.Is(await(t, sent2).err, context.Canceled), true)
	reqs, _ := rec.snapshot()
	is(t, len(reqs), 0)

	// The hub's own CANCELLED is recorded, and a deadline is a timeout.
	q := async(func() (Receipt, error) { return Query[Receipt](ctxWithin(t, 5*time.Second), app, GetOrder{}) })
	s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), hubpb.ErrorCode_CANCELLED, "x"))
	await(t, q)
	reqs, _ = rec.snapshot()
	is(t, len(reqs), 1)
	is(t, reqs[0].outcome, "cancelled")
}

// A call its options refuse is no call: nothing is recorded.
func TestALocalRefusalRecordsNothing(t *testing.T) {
	rec := &recorder{}
	app, _, _ := sending(t, fakehub.Ack("hub-1"), Telemetry(rec))
	for _, c := range []SendOption{Header("traceparent", "x"), Timeout(-time.Second), Retry(RetryPolicy{})} {
		_, err := Send[Empty](context.Background(), app, PlaceOrder{}, c)
		is(t, err != nil, true)
	}
	reqs, _ := rec.snapshot()
	is(t, len(reqs), 0)
}

// panickingCodec panics decoding.
type panickingCodec struct{}

func (panickingCodec) Marshal(any) ([]byte, error) { return []byte("{}"), nil }
func (panickingCodec) Unmarshal([]byte, any) error { panic("codec") }

func TestAPanicAfterPrepareIsNotRecordedAsOK(t *testing.T) {
	rec := &recorder{}
	app, _, s := sending(t, fakehub.Ack("hub-1"), Telemetry(rec), Codec(panickingCodec{}))
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		_, _ = Send[Receipt](ctxWithin(t, 5*time.Second), app, PlaceOrder{})
	}()
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{}`))
	is(t, <-done, any("codec")) // the panic goes on
	reqs, _ := rec.snapshot()
	for _, r := range reqs {
		is(t, r.outcome == "ok", false)
	}
}

func TestACertificateRenewalIsOneRotationWithSeveralHubs(t *testing.T) {
	ca := newTestCA(t, "operator CA")
	a := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	b := fakehub.Start(t, fakehub.WithTLS(ca.server(t, "hub.test"), ca.pool()))
	dir := t.TempDir()
	certFile, keyFile := pairFiles(t, dir, ca.client(t, "orders-service", time.Now().Add(time.Hour)))
	tm := scaled()
	tm.certCheckInterval, tm.rollDelayMax = 50*ms, 0
	rec := &recorder{}
	app := newApp(t, Hubs(a.Addr(), b.Addr()), TLS(TLSConfig{CAFile: ca.caFile(t), ServerName: "hub.test"}),
		ClientCertificate(certFile, keyFile), withTimings(tm), Telemetry(rec))
	is(t, app.Start(context.Background()), nil)
	streams := map[*fakehub.Hub]*fakehub.Stream{}
	for i, h := range []*fakehub.Hub{a, b} {
		s := h.Next(t)
		s.RecvRegister(t)
		s.Ack(t, fakehub.Ack([]string{"hub-a", "hub-b"}[i]))
		streams[h] = s
	}
	poll(t, func() bool { return len(app.pool.live()) == 2 })
	var before []string
	for _, c := range app.pool.all() {
		before = append(before, c.presented().fingerprint)
	}
	pairFiles(t, dir, ca.client(t, "orders-service", time.Now().Add(2*time.Hour)))
	for _, h := range []*fakehub.Hub{a, b} {
		s := streams[h]
		nextRegister(t, s) // out of the routing first
		s.HalfClosed(t, 2*time.Second)
		s.Close(codes.OK, "")
		s2 := h.Next(t)
		nextRegister(t, s2)
		s2.Ack(t, fakehub.Ack(map[*fakehub.Hub]string{a: "hub-a", b: "hub-b"}[h]))
	}
	poll(t, func() bool { // every connection presents the renewed pair
		for i, c := range app.pool.all() {
			if p := c.presented(); p == nil || p.fingerprint == before[i] {
				return false
			}
		}
		return true
	})
	is(t, rec.count(func(r *recorder) int { return r.rotated }), 1)
}

// minAllocs is the fewest allocations of f in five measurements: the count is process wide, and
// background goroutines of earlier tests add to one run.
func minAllocs(f func()) float64 {
	best := testing.AllocsPerRun(50, f)
	for range 4 {
		best = min(best, testing.AllocsPerRun(50, f))
	}
	return best
}

func TestNoOpTelemetryAddsNoAllocationToRequestAndRun(t *testing.T) {
	build := func(opts ...Option) *App {
		app := newApp(t, append([]Option{Hubs("hub:7000")}, opts...)...)
		is(t, HandleCommand(app, func(context.Context, PlaceOrder) (Empty, error) { return Empty{}, nil }), nil)
		return app
	}
	for name, opt := range map[string]Option{"value": Telemetry(NopInstrumentation{}), "pointer": Telemetry(&NopInstrumentation{}), "none": Telemetry(nil)} {
		app := build(opt)
		is(t, app.tel.enabled(), false)
		h := app.handlers["orders.PlaceOrder"]
		req := &hubpb.Request{Id: "r-1", Type: "orders.PlaceOrder", Kind: hubpb.RequestKind_COMMAND, Payload: []byte(`{}`)}
		scope := &handling{info: MessageInfo{ID: "r-1", Type: "orders.PlaceOrder"}}
		ctx := context.Background()
		base := minAllocs(func() { _, _ = h.invoke(withHandling(ctx, scope), app.cfg.codec, req.GetPayload(), nil) })
		run := minAllocs(func() { _, _ = app.run(ctx, h, req, scope) })
		if run > base {
			t.Errorf("%s: run allocates %v, the bare handler call %v", name, run, base)
		}
		// request: a refused call (nil message) goes through the same wrapper.
		var nilMsg Message
		fresh := minAllocs(func() { _, _ = request[Empty](ctx, app, hubpb.RequestKind_COMMAND, nilMsg, nil) })
		bare := minAllocs(func() { _, _, _ = doRequest[Empty](ctx, app, hubpb.RequestKind_COMMAND, nilMsg, nil) })
		if fresh > bare {
			t.Errorf("%s: request allocates %v, doRequest %v", name, fresh, bare)
		}
	}
}

// nextRegister returns the next Register on s, skipping the frames between (permits, heartbeats).
func nextRegister(t *testing.T, s *fakehub.Stream) *hubpb.Register {
	t.Helper()
	for range 50 {
		if r := s.Recv(t).GetRegister(); r != nil {
			return r
		}
	}
	t.Fatal("no Register")
	return nil
}

// stopping is an Instrumentation that is told when its gauges end.
type stopping struct {
	NopInstrumentation
	read    func() Gauges
	stopped atomic.Int32
}

func (s *stopping) ObserveGauges(read func() Gauges) { s.read = read }
func (s *stopping) StopObservingGauges()             { s.stopped.Add(1) }

func TestShutdownEndsTheGauges(t *testing.T) {
	rec := &stopping{}
	hub := fakehub.Start(t)
	app, _ := serving(t, hub, func(*App) {}, Telemetry(rec))
	poll(t, func() bool { return rec.read().ConnectedHubs == 1 })
	is(t, rec.stopped.Load(), int32(0))
	is(t, app.Shutdown(ctxWithin(t, 5*time.Second)), nil)
	is(t, rec.stopped.Load(), int32(1))
	is(t, rec.read(), Gauges{})
	is(t, app.Shutdown(ctxWithin(t, 5*time.Second)), nil)
	is(t, rec.stopped.Load(), int32(1)) // once
}
