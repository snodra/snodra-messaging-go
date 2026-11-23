package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	snodra "github.com/snodra/snodra-messaging-go"
)

// ScopeName is the instrumentation scope of the library's instruments and spans.
// TODO(spec-gap): SG-24 (the telemetry's names are not specified).
const ScopeName = "github.com/snodra/snodra-messaging-go"

// The instrument names of the library, the Rust and Kotlin libraries' ones.
const (
	requestDuration      = "snodra.hub.client.request.duration"
	handlersInFlight     = "snodra.hub.client.handlers.in.flight"
	connectedHubs        = "snodra.hub.client.connected.hubs"
	tokenFetches         = "snodra.hub.client.token.fetches"
	certificateRotations = "snodra.hub.client.certificate.rotations"
	credentialsRefreshes = "snodra.hub.client.credentials.refreshes.sent"
)

// durationBoundaries are the bucket boundaries of the request duration, in seconds: the hub's.
var durationBoundaries = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Option configures [New].
type Option func(*options)

type options struct {
	meters metric.MeterProvider
	traces trace.TracerProvider
	prop   propagation.TextMapPropagator
}

// WithMeterProvider records the metrics on mp instead of the global meter provider.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meters = mp }
}

// WithTracerProvider starts the spans on tp instead of the global tracer provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.traces = tp }
}

// WithPropagator reads and writes the trace context with p instead of the global propagator.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.prop = p }
}

// instrumentation is the snodra.Instrumentation on OpenTelemetry.
type instrumentation struct {
	snodra.NopInstrumentation // for a method added to the interface later

	meter  metric.Meter
	tracer trace.Tracer
	prop   propagation.TextMapPropagator // nil: the global one, or W3C trace context when that is none

	duration  metric.Float64Histogram
	tokens    metric.Int64Counter
	rotations metric.Int64Counter
	refreshes metric.Int64Counter
}

// New is the Instrumentation to pass to snodra.Telemetry. By default it uses the global meter
// provider, tracer provider and propagator, as they are when New runs for the providers and when
// a request is handled for the propagator; when no propagator is installed it reads and writes
// the W3C trace context (traceparent and tracestate).
func New(opts ...Option) snodra.Instrumentation {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.meters == nil {
		o.meters = otel.GetMeterProvider()
	}
	if o.traces == nil {
		o.traces = otel.GetTracerProvider()
	}
	i := &instrumentation{
		meter:  o.meters.Meter(ScopeName),
		tracer: o.traces.Tracer(ScopeName),
		prop:   o.prop,
	}
	// Instrument builders report an error only for an invalid name, and give a working no-op
	// instrument with it: there is nothing to do with one.
	i.duration, _ = i.meter.Float64Histogram(requestDuration,
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBoundaries...),
		metric.WithDescription("Commands and queries sent through the hub, from the call to its result (retries included), per kind, type and outcome"))
	i.tokens, _ = i.meter.Int64Counter(tokenFetches,
		metric.WithDescription("Access token fetches by result"))
	i.rotations, _ = i.meter.Int64Counter(certificateRotations,
		metric.WithDescription("Client certificate pairs adopted from the files"))
	i.refreshes, _ = i.meter.Int64Counter(credentialsRefreshes,
		metric.WithDescription("ClientMessage.credentials written to the hubs"))
	return i
}

// propagator is the one to use now.
func (i *instrumentation) propagator() propagation.TextMapPropagator {
	if i.prop != nil {
		return i.prop
	}
	if p := otel.GetTextMapPropagator(); len(p.Fields()) > 0 {
		return p
	}
	return propagation.TraceContext{} // none installed: the protocol's own trace context
}

func (i *instrumentation) RequestDone(kind, msgType, outcome string, took time.Duration) {
	i.duration.Record(context.Background(), took.Seconds(), metric.WithAttributeSet(attribute.NewSet(
		attribute.String("kind", kind),
		attribute.String("type", msgType),
		attribute.String("outcome", outcome))))
}

func (i *instrumentation) TokenFetched(result string) {
	i.tokens.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("result", result))))
}

func (i *instrumentation) CertificateRotated() {
	i.rotations.Add(context.Background(), 1)
}

func (i *instrumentation) CredentialsRefreshSent() {
	i.refreshes.Add(context.Background(), 1)
}

// ObserveGauges registers the two gauges, read together by one callback.
func (i *instrumentation) ObserveGauges(read func() snodra.Gauges) {
	handlers, err := i.meter.Int64ObservableGauge(handlersInFlight,
		metric.WithDescription("Incoming hub requests being handled or waiting for a handler permit"))
	if err != nil {
		otel.Handle(err)
		return
	}
	hubs, err := i.meter.Int64ObservableGauge(connectedHubs,
		metric.WithDescription("Hubs with an acknowledged stream"))
	if err != nil {
		otel.Handle(err)
		return
	}
	_, err = i.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		g := read()
		o.ObserveInt64(handlers, g.HandlersInFlight)
		o.ObserveInt64(hubs, g.ConnectedHubs)
		return nil
	}, handlers, hubs)
	if err != nil {
		otel.Handle(err)
	}
}

// StartHandling starts a server span named after the message type, a child of the request's
// traceparent.
// TODO(spec-gap): SG-24 (the reference libraries create no handler span).
func (i *instrumentation) StartHandling(ctx context.Context, h snodra.Handling) (context.Context, func(error)) {
	if h.Traceparent != "" {
		carrier := propagation.MapCarrier{"traceparent": h.Traceparent}
		if h.Tracestate != "" {
			carrier["tracestate"] = h.Tracestate
		}
		ctx = i.propagator().Extract(ctx, carrier)
	}
	ctx, span := i.tracer.Start(ctx, h.Type, trace.WithSpanKind(trace.SpanKindServer))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// Inject writes the trace context of ctx.
func (i *instrumentation) Inject(ctx context.Context, headers map[string]string) {
	i.propagator().Inject(ctx, propagation.MapCarrier(headers))
}
