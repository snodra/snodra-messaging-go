package snodra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/traceparent"
)

// Instrumentation is where the library reports what it does: request timings, token fetches,
// certificate rotations, credential refreshes, two gauges and the handling of a request, for
// metrics and traces. The default is [NopInstrumentation]; the otel module of this repository
// implements it on OpenTelemetry (see [Telemetry]).
//
// Every method runs on a hot path of the library: an implementation must return quickly and
// never block. A panic in one is recovered and logged once; the library goes on without it.
// Every label value comes from a bounded set (a kind, a registered type, an outcome), never an
// id. Embed [NopInstrumentation] to implement only some of the methods.
type Instrumentation interface {
	// RequestDone records one Send or Query call of msgType, from the call to its result with
	// every retry included; a call its caller cancels (context.Canceled) is not reported.
	// kind is "command" or "query"; outcome is "ok", "no_handler", "timeout",
	// "handler_disconnected", "resource_exhausted", "handler_error", "denied",
	// "invalid_request" (also an answer that does not decode, and a call refused locally),
	// "cancelled" (the hub's CANCELLED), "unavailable" (no hub took the request, or its
	// stream broke before the answer) or "error".
	RequestDone(kind, msgType, outcome string, took time.Duration)
	// TokenFetched records one fetch of an access token by [NewClientCredentials]: result is
	// "ok", "http_error" (a non-success status without an OAuth2 error body), "oauth_error" (an
	// OAuth2 error body), "io_error" (the request did not complete, or the secret could not be
	// read) or "bad_response" (a success answer that is no usable token response).
	TokenFetched(result string)
	// CertificateRotated records a client certificate pair presented to a hub after another one
	// before it.
	CertificateRotated()
	// CredentialsRefreshSent records a credentials refresh written to a hub stream.
	CredentialsRefreshSent()
	// ObserveGauges is called once, by [App.Start]: read returns the library's gauges as they
	// are now, from memory; call it whenever the gauges are collected. It is safe to call from
	// any goroutine.
	ObserveGauges(read func() Gauges)
	// StartHandling starts the handling of a request by a handler of this instance, before
	// the handler runs. The context it returns is the handler's (a span can live in it, and
	// the requests the handler sends carry it to Inject); the function it returns is called
	// once when the handler is done, with its error (nil on success).
	StartHandling(ctx context.Context, h Handling) (context.Context, func(err error))
	// Inject lets the implementation put the trace context of ctx on the headers of a request
	// about to be sent: it may set "traceparent" and "tracestate" in headers. Any other key,
	// and a "traceparent" that is not a valid W3C one, is ignored.
	Inject(ctx context.Context, headers map[string]string)
}

// A GaugesStopper is an Instrumentation that wants to know when the gauges it was given by
// ObserveGauges end: [App.Shutdown] calls StopObservingGauges once, as it finishes, and the
// read function it was given reads zeros from then on. One Instrumentation serves one App.
type GaugesStopper interface {
	StopObservingGauges()
}

// Gauges are the library's gauges at one moment.
type Gauges struct {
	// HandlersInFlight is the requests from the hubs whose handler runs now (or waits for a
	// handler slot).
	HandlersInFlight int64
	// ConnectedHubs is the hubs with an acknowledged stream.
	ConnectedHubs int64
}

// Handling describes a request a handler is about to handle.
type Handling struct {
	// Type is the message type, for example "orders.PlaceOrder".
	Type string
	// Kind is "command" or "query".
	Kind string
	// Traceparent is the request's traceparent header when it is a valid W3C one, else "".
	Traceparent string
	// Tracestate is the request's tracestate header, only with a valid Traceparent.
	Tracestate string
}

// NopInstrumentation is the default [Instrumentation]: it does nothing, at no cost. Embed it to
// implement only some of the methods.
type NopInstrumentation struct{}

func (NopInstrumentation) RequestDone(string, string, string, time.Duration) {}
func (NopInstrumentation) TokenFetched(string)                               {}
func (NopInstrumentation) CertificateRotated()                               {}
func (NopInstrumentation) CredentialsRefreshSent()                           {}
func (NopInstrumentation) ObserveGauges(func() Gauges)                       {}
func (NopInstrumentation) StartHandling(ctx context.Context, _ Handling) (context.Context, func(error)) {
	return ctx, func(error) {}
}
func (NopInstrumentation) Inject(context.Context, map[string]string) {}

// Telemetry reports what the library does to i, for example the one of the otel module of this
// repository. See [Instrumentation]. A nil i is the default, [NopInstrumentation].
func Telemetry(i Instrumentation) Option {
	return func(c *config) error {
		c.telemetry = i
		return nil
	}
}

// telemetry is what the library calls: the configured Instrumentation, or nothing. A nil
// *telemetry and one without an implementation do nothing, allocate nothing and read no clock.
// A panic of the implementation is recovered and logged, once.
type telemetry struct {
	i      Instrumentation // nil: nothing is reported
	log    *slog.Logger
	logged atomic.Bool

	gmu          sync.Mutex // orders ObserveGauges and StopObservingGauges
	gaugesObsd   bool       // ObserveGauges was called
	gaugesStoppd bool       // the app is shut down: no registration after this
}

func newTelemetry(i Instrumentation, log *slog.Logger) *telemetry {
	switch i.(type) {
	case NopInstrumentation, *NopInstrumentation:
		i = nil
	}
	return &telemetry{i: i, log: log}
}

// enabled is true when an implementation is configured.
func (t *telemetry) enabled() bool { return t != nil && t.i != nil }

// guard, deferred, recovers a panic of the implementation's method.
func (t *telemetry) guard(method string) {
	if r := recover(); r != nil && t.logged.CompareAndSwap(false, true) {
		t.log.Error("the telemetry implementation panicked; it keeps being called, its later panics are not logged",
			"method", method, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
	}
}

// now is the start of a timed call; the zero time when nothing is reported.
func (t *telemetry) now() time.Time {
	if !t.enabled() {
		return time.Time{}
	}
	return time.Now()
}

// requestDone reports a Send or Query of typ that began at started (from now) and ended with err.
func (t *telemetry) requestDone(kind, typ string, started time.Time, err error) {
	if !t.enabled() || started.IsZero() || errors.Is(err, context.Canceled) {
		return // the caller's own cancellation is not a result: nothing is recorded
	}
	defer t.guard("RequestDone")
	t.i.RequestDone(kind, typ, requestOutcome(err), time.Since(started))
}

func (t *telemetry) tokenFetched(result string) {
	if !t.enabled() {
		return
	}
	defer t.guard("TokenFetched")
	t.i.TokenFetched(result)
}

func (t *telemetry) certificateRotated() {
	if !t.enabled() {
		return
	}
	defer t.guard("CertificateRotated")
	t.i.CertificateRotated()
}

func (t *telemetry) credentialsRefreshSent() {
	if !t.enabled() {
		return
	}
	defer t.guard("CredentialsRefreshSent")
	t.i.CredentialsRefreshSent()
}

// observeGauges hands the implementation read; a read that panics reads zeros. After the
// app's shutdown it hands nothing: a registration made then would never be ended. The ordering
// lock is held while the implementation runs, so a Shutdown racing this call waits for it and
// then ends the registration it made; read does not take that lock.
func (t *telemetry) observeGauges(read func() Gauges) {
	if !t.enabled() {
		return
	}
	t.gmu.Lock()
	defer t.gmu.Unlock()
	if t.gaugesStoppd {
		return
	}
	t.gaugesObsd = true
	defer t.guard("ObserveGauges")
	t.i.ObserveGauges(func() Gauges {
		defer func() { _ = recover() }()
		return read()
	})
}

// stopGauges tells the implementation, when it asks to know, that the app is shut down.
func (t *telemetry) stopGauges() {
	if !t.enabled() {
		return
	}
	t.gmu.Lock()
	defer t.gmu.Unlock()
	if t.gaugesStoppd {
		return
	}
	t.gaugesStoppd = true
	if !t.gaugesObsd {
		return // never registered: nothing to end
	}
	if s, ok := t.i.(GaugesStopper); ok {
		defer t.guard("StopObservingGauges")
		s.StopObservingGauges()
	}
}

// startHandling starts the handling of a request: the handler's context, and the function to
// call when it is done. Without an implementation (or when it panics) ctx stays as it is.
func (t *telemetry) startHandling(ctx context.Context, h Handling) (hctx context.Context, done func(error)) {
	if !t.enabled() {
		return ctx, nil
	}
	hctx = ctx
	defer func() {
		if r := recover(); r != nil {
			t.guard2(r, "StartHandling")
			hctx, done = ctx, nil
		}
	}()
	c, d := t.i.StartHandling(ctx, h)
	if c == nil {
		c = ctx
	}
	if d == nil {
		return c, nil
	}
	return c, func(err error) {
		defer t.guard("StartHandling (done)")
		d(err)
	}
}

// guard2 is guard for a panic already recovered.
func (t *telemetry) guard2(r any, method string) {
	if t.logged.CompareAndSwap(false, true) {
		t.log.Error("the telemetry implementation panicked; it keeps being called, its later panics are not logged",
			"method", method, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
	}
}

// inject lets the implementation set the traceparent and tracestate of headers, the headers of a
// request about to be sent with ctx: it works on a copy of those two, and a traceparent that
// is not a valid one is dropped (headers keep what they had).
func (t *telemetry) inject(ctx context.Context, headers map[string]string) {
	if !t.enabled() {
		return
	}
	scratch := map[string]string{}
	if tp, ok := headers[traceparentHeader]; ok {
		scratch[traceparentHeader] = tp
	}
	func() {
		defer t.guard("Inject")
		t.i.Inject(ctx, scratch)
	}()
	if tp, ok := scratch[traceparentHeader]; ok && traceparent.Valid(tp) {
		headers[traceparentHeader] = tp
		if ts, ok := scratch[tracestateHeader]; ok {
			headers[tracestateHeader] = ts
		}
	}
}

const tracestateHeader = "tracestate"

// requestOutcome is the outcome label of a call that ended with err (nil: "ok"). It is the
// hub's outcome names for an [*Error], "unavailable" when no hub took the request or its stream
// broke before the answer, "invalid_request" for an answer that does not decode and for a call
// refused locally (a reserved header, an invalid option), "timeout" for the caller's own
// deadline, and "error" for anything else. The caller's own cancellation is not an outcome: a
// call it ends records nothing (see requestDone).
func requestOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	var e *Error
	if errors.As(err, &e) {
		switch e.Code {
		case CodeNoHandler:
			return "no_handler"
		case CodeTimeout:
			return "timeout"
		case CodeHandlerDisconnected:
			return "handler_disconnected"
		case CodeResourceExhausted:
			return "resource_exhausted"
		case CodeHandlerError:
			return "handler_error"
		case CodeUnauthenticated, CodePermissionDenied:
			return "denied"
		case CodeInvalidRequest:
			return "invalid_request"
		case CodeCancelled:
			return "cancelled"
		}
		return "handler_error"
	}
	var u *undecodableReply
	var r *localRefusal
	switch {
	case errors.As(err, &u):
		return "invalid_request"
	case errors.As(err, &r):
		return "invalid_request"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, ErrOutcomeUnknown), errors.Is(err, ErrNeverSent):
		return "unavailable"
	}
	return "error"
}

// localRefusal is the error of a call refused before anything was built or sent: a reserved
// header, a negative timeout, an invalid retry policy.
type localRefusal struct{ err error }

func (e *localRefusal) Error() string { return e.err.Error() }
func (e *localRefusal) Unwrap() error { return e.err }

// undecodableReply is the error of a call whose answer does not decode as the wanted type.
type undecodableReply struct {
	typ string
	err error
}

func (e *undecodableReply) Error() string {
	return fmt.Sprintf("snodra: cannot decode the answer of %s: %v", e.typ, e.err)
}
func (e *undecodableReply) Unwrap() error { return e.err }

// kindLabel is the kind label of a request: "command" or "query".
func kindLabel(commandKind bool) string {
	if commandKind {
		return "command"
	}
	return "query"
}
