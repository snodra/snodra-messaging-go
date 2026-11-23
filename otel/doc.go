// Package otel reports what snodra-messaging-go does to OpenTelemetry: the library's metrics
// and the traces of the requests its handlers serve. It implements snodra.Instrumentation on
// the OpenTelemetry API only; the application installs the SDK and its exporters.
//
//	app, err := snodra.New(
//		snodra.Service("orders-service"),
//		snodra.Telemetry(otel.New()),
//	)
//
// New uses the global meter provider, tracer provider and propagator, so install them before
// New (or pass them with WithMeterProvider, WithTracerProvider and WithPropagator). With none
// installed the API's no-op providers make every measurement free.
//
// # Metrics
//
// The instrument names, units and attributes are those of the Rust and Kotlin libraries, so a
// dashboard works for every library. A Prometheus export turns the dots into underscores, adds
// the unit (s becomes _seconds) and _total to a counter.
//
//   - snodra.hub.client.request.duration (histogram, s; kind, type, outcome): a Send or Query,
//     from the call to its result, retries included. kind is command or query; outcome is ok,
//     no_handler, timeout, handler_disconnected, resource_exhausted, handler_error, denied,
//     invalid_request, cancelled, unavailable or error.
//   - snodra.hub.client.handlers.in.flight (gauge): requests from the hubs whose handler runs.
//   - snodra.hub.client.connected.hubs (gauge): hubs with an acknowledged stream.
//   - snodra.hub.client.token.fetches (counter; result): OAuth2 access token fetches by result
//     (ok, http_error, oauth_error, io_error, bad_response).
//   - snodra.hub.client.certificate.rotations (counter): client certificate pairs presented
//     after the first.
//   - snodra.hub.client.credentials.refreshes.sent (counter): credential refreshes written to
//     the hubs.
//
// The scope of the instruments and of the spans is [ScopeName].
//
// # Traces
//
// A request a handler serves gets a server span named after the message type, a child of the
// request's valid traceparent header (with its tracestate). The requests the handler sends,
// with the context it was given, carry the span's trace context as their traceparent and
// tracestate. A request sent outside any handler carries the trace context of the context it
// is sent with.
//
// See https://snodra.damoreira.com/library-authors/messages/ (Trace context).
package otel
