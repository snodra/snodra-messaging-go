package observability_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/observability"
)

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func collect(t *testing.T, r *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		is(t, sm.Scope.Name, observability.ScopeName)
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func attrs(kv ...string) attribute.Set {
	var a []attribute.KeyValue
	for i := 0; i < len(kv); i += 2 {
		a = append(a, attribute.String(kv[i], kv[i+1]))
	}
	return attribute.NewSet(a...)
}

func TestInstrumentNamesAndLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	i := observability.New(observability.WithMeterProvider(mp))
	i.RequestDone("command", "orders.PlaceOrder", "ok", 30*time.Millisecond)
	i.RequestDone("query", "orders.GetOrder", "timeout", 2*time.Second)
	i.TokenFetched("ok")
	i.TokenFetched("ok")
	i.TokenFetched("oauth_error")
	i.CertificateRotated()
	i.CredentialsRefreshSent()
	i.CredentialsRefreshSent()
	calls := 0
	i.ObserveGauges(func() snodra.Gauges {
		calls++
		return snodra.Gauges{HandlersInFlight: 3, ConnectedHubs: 2}
	})

	got := collect(t, reader)
	is(t, calls, 1) // both gauges from one read

	h, ok := got["snodra.hub.client.request.duration"].Data.(metricdata.Histogram[float64])
	is(t, ok, true)
	is(t, got["snodra.hub.client.request.duration"].Unit, "s")
	is(t, len(h.DataPoints), 2)
	for _, p := range h.DataPoints {
		if !slices.Equal(p.Bounds, []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}) {
			t.Fatalf("bounds %v", p.Bounds)
		}
		is(t, p.Count, uint64(1))
		switch {
		case p.Attributes.Equals(&[]attribute.Set{attrs("kind", "command", "type", "orders.PlaceOrder", "outcome", "ok")}[0]):
			is(t, p.Sum, 0.03)
		case p.Attributes.Equals(&[]attribute.Set{attrs("kind", "query", "type", "orders.GetOrder", "outcome", "timeout")}[0]):
			is(t, p.Sum, 2.0)
		default:
			t.Fatalf("attributes %v", p.Attributes)
		}
	}

	gauge := func(name string) int64 {
		g, ok := got[name].Data.(metricdata.Gauge[int64])
		is(t, ok, true)
		is(t, len(g.DataPoints), 1)
		is(t, g.DataPoints[0].Attributes.Len(), 0)
		is(t, got[name].Unit, "")
		return g.DataPoints[0].Value
	}
	is(t, gauge("snodra.hub.client.handlers.in.flight"), 3)
	is(t, gauge("snodra.hub.client.connected.hubs"), 2)

	counter := func(name string) metricdata.Sum[int64] {
		s, ok := got[name].Data.(metricdata.Sum[int64])
		is(t, ok, true)
		is(t, s.IsMonotonic, true)
		return s
	}
	tokens := counter("snodra.hub.client.token.fetches")
	is(t, len(tokens.DataPoints), 5) // the others stay at 0
	for _, p := range tokens.DataPoints {
		v, _ := p.Attributes.Value("result")
		switch v.AsString() {
		case "ok":
			is(t, p.Value, 2)
		case "oauth_error":
			is(t, p.Value, 1)
		case "http_error", "io_error", "bad_response":
			is(t, p.Value, 0)
		default:
			t.Fatalf("result %v", v)
		}
	}
	is(t, counter("snodra.hub.client.certificate.rotations").DataPoints[0].Value, 1)
	is(t, counter("snodra.hub.client.credentials.refreshes.sent").DataPoints[0].Value, 2)
	is(t, got["snodra.hub.client.token.fetches"].Description, "Access token fetches by result")
}

func TestTheHandlerSpanIsAChildOfTheIncomingTraceparent(t *testing.T) {
	const traceID, parentID = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	i := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(sdkmetric.NewMeterProvider()),
		observability.WithPropagator(propagation.TraceContext{}))

	ctx, done := i.StartHandling(context.Background(), snodra.Handling{
		Type: "orders.PlaceOrder", Kind: "command", Traceparent: "00-" + traceID + "-" + parentID + "-01", Tracestate: "v=1"})
	headers := map[string]string{}
	i.Inject(ctx, headers)
	done(nil)

	spans := rec.Ended()
	is(t, len(spans), 1)
	span := spans[0]
	is(t, span.Name(), "orders.PlaceOrder")
	is(t, span.SpanKind(), trace.SpanKindServer)
	is(t, span.SpanContext().TraceID().String(), traceID)
	is(t, span.Parent().SpanID().String(), parentID)
	is(t, span.Parent().IsRemote(), true)
	is(t, headers["traceparent"], "00-"+traceID+"-"+span.SpanContext().SpanID().String()+"-01")
	is(t, headers["tracestate"], "v=1")
	is(t, len(headers), 2)
}

func TestAFailedHandlingMarksItsSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	i := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(sdkmetric.NewMeterProvider()))
	_, done := i.StartHandling(context.Background(), snodra.Handling{Type: "orders.GetOrder", Kind: "query"})
	done(context.DeadlineExceeded)
	span := rec.Ended()[0]
	is(t, span.Status().Code.String(), "Error")
	is(t, len(span.Events()), 1)
	is(t, span.Parent().IsValid(), false) // no traceparent: a new trace
}

func TestWithoutAPropagatorTheTraceContextIsStillUsed(t *testing.T) {
	// No global propagator is installed in this test binary.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	i := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(sdkmetric.NewMeterProvider()))
	ctx, done := i.StartHandling(context.Background(), snodra.Handling{
		Type: "orders.PlaceOrder", Kind: "command", Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	h := map[string]string{}
	i.Inject(ctx, h)
	done(nil)
	is(t, rec.Ended()[0].SpanContext().TraceID().String(), "4bf92f3577b34da6a3ce929d0e0e4736")
	is(t, h["traceparent"] != "", true)
}

func TestWithoutAnSDKEveryCallIsSafeAndRecordsNothing(t *testing.T) {
	i := observability.New() // no SDK installed: the API's no-op providers
	i.RequestDone("command", "t", "ok", time.Millisecond)
	i.TokenFetched("ok")
	i.CertificateRotated()
	i.CredentialsRefreshSent()
	i.ObserveGauges(func() snodra.Gauges { t.Error("a no-op meter never reads"); return snodra.Gauges{} })
	ctx, done := i.StartHandling(context.Background(), snodra.Handling{Type: "t", Kind: "query"})
	done(nil)
	i.Inject(ctx, map[string]string{})
}

func TestEveryCounterSeriesExistsAtZeroFromTheStart(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	observability.New(observability.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	got := collect(t, reader)
	tokens := got["snodra.hub.client.token.fetches"].Data.(metricdata.Sum[int64])
	seen := map[string]int64{}
	for _, p := range tokens.DataPoints {
		v, _ := p.Attributes.Value("result")
		seen[v.AsString()] = p.Value
	}
	is(t, len(seen), 5)
	for _, r := range []string{"ok", "http_error", "oauth_error", "io_error", "bad_response"} {
		v, ok := seen[r]
		is(t, ok, true)
		is(t, v, 0)
	}
	for _, name := range []string{"snodra.hub.client.certificate.rotations", "snodra.hub.client.credentials.refreshes.sent"} {
		s := got[name].Data.(metricdata.Sum[int64])
		is(t, len(s.DataPoints), 1)
		is(t, s.DataPoints[0].Value, 0)
	}
}

func TestEveryDescriptionIsRusts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	i := observability.New(observability.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	i.RequestDone("command", "t", "ok", time.Millisecond)
	i.ObserveGauges(func() snodra.Gauges { return snodra.Gauges{} })
	got := collect(t, reader)
	for name, want := range map[string]string{
		"snodra.hub.client.request.duration":           "Commands and queries sent through the hub, from the call to its result (retries included), per kind, type and outcome",
		"snodra.hub.client.handlers.in.flight":         "Incoming hub requests being handled or waiting for a handler permit",
		"snodra.hub.client.connected.hubs":             "Hubs with an acknowledged stream",
		"snodra.hub.client.token.fetches":              "Access token fetches by result",
		"snodra.hub.client.certificate.rotations":      "Client certificate pairs adopted from the files",
		"snodra.hub.client.credentials.refreshes.sent": "ClientMessage.credentials written to the hubs",
	} {
		is(t, got[name].Description, want)
	}
}

func TestTheGaugeCallbackEndsWithTheApp(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	i := observability.New(observability.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	reads := 0
	i.ObserveGauges(func() snodra.Gauges { reads++; return snodra.Gauges{ConnectedHubs: 1} })
	collect(t, reader)
	is(t, reads, 1)
	stopper, ok := i.(snodra.GaugesStopper)
	is(t, ok, true)
	stopper.StopObservingGauges()
	stopper.StopObservingGauges() // twice is harmless
	collect(t, reader)
	is(t, reads, 1) // no longer read, no longer holding the app
}
