# snodra-messaging-go

The official Go client library of the [Snodra](https://snodra.damoreira.com/) messaging suite. A service registers
handlers for commands, queries, streaming queries and subscriptions, and sends messages to other services, through a
Snodra hub: the library keeps the hub streams, permits, retries and timeouts, you write functions.

**Status: pre-1.0, feature level 1.** The library implements level 1 of the protocol (see
[Feature level 1](#feature-level-1-and-how-it-is-proved)) and passes the 33 checks of the conformance runner. The API
may still change before 1.0, and no version has been released yet. It needs Go 1.26 or later; the core module depends
only on gRPC and protobuf.

```sh
go get github.com/snodra/snodra-messaging-go
```

The package is named `snodra`; the examples import it as `snodra "github.com/snodra/snodra-messaging-go"`.

## Quick start

A complete service, which owns one command and one query. It is
[`examples/quickstart/main.go`](examples/quickstart/main.go), and a test runs it against a scripted hub.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	snodra "github.com/snodra/snodra-messaging-go"
)

// PlaceOrder is a command: the orders-service owns it.
type PlaceOrder struct {
	OrderID string `json:"orderId"`
	Cents   int64  `json:"cents"`
}

func (PlaceOrder) MessageType() string { return "orders.PlaceOrder" }

// Receipt answers PlaceOrder.
type Receipt struct {
	OrderID string `json:"orderId"`
	Total   int64  `json:"total"`
}

// GetOrder is a query.
type GetOrder struct {
	OrderID string `json:"orderId"`
}

func (GetOrder) MessageType() string { return "orders.GetOrder" }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// SNODRA_SERVICE_NAME, SNODRA_SERVER_ADDRESS and the other SNODRA_* variables override
	// the name given here.
	if err := run(ctx, snodra.FromEnv()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, opts ...snodra.Option) error {
	app, err := snodra.New(append([]snodra.Option{snodra.Service("orders-service")}, opts...)...)
	if err != nil {
		return err
	}

	var mu sync.Mutex
	orders := map[string]Receipt{}

	err = snodra.HandleCommand(app, func(ctx context.Context, cmd PlaceOrder) (Receipt, error) {
		if cmd.OrderID == "" {
			return Receipt{}, snodra.InvalidRequest("orderId is required")
		}
		mu.Lock()
		defer mu.Unlock()
		orders[cmd.OrderID] = Receipt{OrderID: cmd.OrderID, Total: cmd.Cents}
		return orders[cmd.OrderID], nil
	})
	if err != nil {
		return err
	}
	err = snodra.HandleQuery(app, func(ctx context.Context, q GetOrder) (Receipt, error) {
		mu.Lock()
		defer mu.Unlock()
		order, ok := orders[q.OrderID]
		if !ok {
			return Receipt{}, snodra.InvalidRequest("no such order")
		}
		return order, nil
	})
	if err != nil {
		return err
	}

	// Run connects to every hub, serves until ctx ends, then shuts down in order.
	return app.Run(ctx)
}
```

Run it against a hub with `SNODRA_SERVER_ADDRESS=localhost:7000 go run ./examples/quickstart` (the hub is the
`snodra/snodra-server` image, see [its repository](https://github.com/snodra/snodra-server)). A message is a Go type with a
`MessageType() string` method (the protocol's type name); its payload is JSON by default (see `snodra.Codec`).
Handlers are registered before `Start` (or `Run`); a registration error is returned at once and again by `Start`.

## Sending

Any service, including one that owns no handler, sends with `Send` (a command) and `Query` (a query). An error is one of
a few kinds you can test for:

```go
receipt, err := snodra.Send[Receipt](ctx, app, PlaceOrder{OrderID: "o-1", Cents: 1250},
	snodra.RoutingKey("o-1"), snodra.Timeout(3*time.Second))
if err != nil {
	var e *snodra.Error
	switch {
	case errors.As(err, &e):
		fmt.Println("the hub or the handler answered", e.Code, e.Message)
	case errors.Is(err, snodra.ErrNeverSent):
		fmt.Println("no handler saw it: sending it again is safe")
	case errors.Is(err, snodra.ErrOutcomeUnknown):
		fmt.Println("it may or may not have run")
	}
	return
}
order, err := snodra.Query[Receipt](ctx, app, GetOrder{OrderID: receipt.OrderID})
```

- A context deadline is the caller's timeout; a cancelled context returns `context.Canceled`, together with
  `ErrOutcomeUnknown` once the request was written.
- A per-call `snodra.Timeout` wins over the app's `snodra.DefaultTimeout`; with neither, the hub applies the type's own
  timeout.
- Retries are opt-in: `snodra.DefaultRetry(snodra.RetryNeverRan)` or `snodra.Retry(...)` per call. A command is retried
  only when its handler never ran; a query also after an unknown outcome. `snodra.Header` adds a header,
  `snodra.Priority` sets the request's priority.
- A request sent while a handler runs carries that request's correlation id, its caller's token (see `snodra.ForwardJWT`)
  and its trace context.
- A command, or a query that is not shared, of a type this instance handles itself is answered in-process: its handler
  runs on the caller's goroutine with no hub, no permit and no handler slot, also before `Start` and after `Shutdown`.
  It sees a fresh request id, the caller's `snodra.Header`s and the correlation id, and acts for the caller of the
  request being handled (anonymous outside a handler); routing key, timeout, priority and retry do not apply (the
  caller's context bounds it), nothing goes to the telemetry, and its errors are those a hub would have relayed.
  `snodra.AlwaysThroughHub(true)` (`SNODRA_ALWAYS_THROUGH_HUB=true`) sends every request through a hub. Streams,
  scatter-gathers and subscriptions always go through a hub.

## Streams, scatter-gather and subscriptions

```go
// Serving a stream: the items go out under the caller's credit.
_ = snodra.HandleStream(app, func(ctx context.Context, _ ListOrders, w *snodra.StreamWriter[Receipt]) error {
	for _, r := range []Receipt{{OrderID: "o-1", Total: 1250}, {OrderID: "o-2", Total: 80}} {
		if err := w.Send(ctx, r); err != nil {
			return err // the caller stopped reading, or the hub stream is gone
		}
	}
	return nil
})

// Reading one: breaking out of the loop cancels the stream at the hub.
for r, err := range snodra.Stream[Receipt](ctx, app, ListOrders{}) {
	if err != nil {
		return // the last thing the sequence yields
	}
	fmt.Println(r.OrderID, r.Total)
}

// Asking every instance of every owner of a shared query type.
gathered, err := snodra.Gather[Receipt](ctx, app, GetOrder{OrderID: "o-1"}, snodra.Timeout(2*time.Second))
if err == nil {
	fmt.Println(len(gathered.Parts), "answers from", gathered.Targets, "targets")
}
```

```go
// Serving a subscription: the initial result now, updates whenever you emit them. The type
// arguments are the query, its initial result and its updates, as Subscribe[I, U] reads them.
updates, _ := snodra.HandleSubscription[WatchOrder, OrderStatus, OrderStatus](app, func(_ context.Context, q WatchOrder) (OrderStatus, error) {
	return OrderStatus{Status: "placed"}, nil
})
updates.Emit(func(q WatchOrder) bool { return q.OrderID == "o-1" }, OrderStatus{Status: "shipped"})

// Following one.
sub, err := snodra.Subscribe[OrderStatus, OrderStatus](ctx, app, WatchOrder{OrderID: "o-1"})
if err != nil {
	return
}
defer sub.Close()
fmt.Println(sub.Initial.Status)
for u, err := range sub.Updates() {
	if err != nil {
		return
	}
	fmt.Println(u.Status)
}
```

Streams and subscription updates are `iter.Seq2[T, error]`. A stream is retried only until its first part; after that
nothing is retried and a lost hub stream ends the sequence with `ErrOutcomeUnknown`.

## Guarding a handler

```go
// The caller is checked before the payload is decoded; a guard needs snodra.JWKS.
_ = snodra.HandleCommand(app, func(ctx context.Context, cmd ArchiveOrder) (snodra.Empty, error) {
	caller, _ := snodra.CallerFrom(ctx)
	fmt.Println("archived by", caller.Subject)
	return snodra.Empty{}, nil
}, snodra.RequireRole("orders-admin"))
```

`RequireAuthenticated`, `RequireRole` and `RequireCaller` answer UNAUTHENTICATED or PERMISSION_DENIED. They need
`snodra.JWKS` (or `SNODRA_JWKS_URL` and `SNODRA_ISSUER`): the token is verified (signature, issuer, expiry) against the
issuer's keys, and registering a guarded handler without them is an error that also fails `Start` and `Run`. Without a
JWKS no token is an identity: every caller is anonymous (`CallerFrom` reports none), the token still goes on with the
requests the handler sends, and a header that is not a bearer token, or a token that is not a signed JWS, is still
answered UNAUTHENTICATED.

## Configuration

Every setting is an option (`snodra.Service`, `snodra.Hubs`, `snodra.MaxConcurrent`, ...), and `snodra.FromEnv()` reads
the same `SNODRA_*` variables the other Snodra libraries read; options given after it win. The full list, with
defaults, is in the documentation of `FromEnv` (`go doc snodra.FromEnv`) and at
<https://snodra.damoreira.com/messaging/go/configuration/>. The common ones:

| Variable | Option | Default |
|---|---|---|
| `SNODRA_SERVICE_NAME` | `Service` | required |
| `SNODRA_INSTANCE_ID` | `Instance` | a random UUID |
| `SNODRA_SERVER_ADDRESS` | `Hubs` | `static://snodra-server:7000`; a list of `host:port`, or one `dns:///name:port` |
| `SNODRA_MAX_CONCURRENT_HANDLERS` | `MaxConcurrent` | 256 |
| `SNODRA_SERVER_REQUEST_TIMEOUT_MS` | `DefaultTimeout` | none (the hub decides) |
| `SNODRA_SERVER_RETRY_ENABLED` | `DefaultRetry` | false |
| `SNODRA_SERVER_TOKEN`, `SNODRA_SERVER_TOKEN_FILE` | `Token`, `TokenFile` | none |
| `SNODRA_SERVER_OAUTH2_TOKEN_URI` (and `_CLIENT_ID`, `_CLIENT_SECRET[_FILE]`, `_SCOPE`, `_CA`) | `JWT`, `NewClientCredentials` | none |
| `SNODRA_SERVER_TLS`, `SNODRA_SERVER_TLS_CA`, `SNODRA_SERVER_TLS_SERVER_NAME` | `TLS` | false |
| `SNODRA_SERVER_TLS_CLIENT_CERT`, `SNODRA_SERVER_TLS_CLIENT_KEY` | `ClientCertificate` | none (implies TLS) |
| `SNODRA_JWKS_URL`, `SNODRA_ISSUER` | `JWKS` | none (no caller is verified; guards need it) |
| `SNODRA_ALWAYS_THROUGH_HUB` | `AlwaysThroughHub` | false (own types answered in-process) |

- An instance opens one stream to every hub it is given and registers the same types on each. A second address that
  answers a hub id already served is parked, and takes over if the first goes away. An `https://` hub address implies
  TLS with the system's roots.
- At most one kind of credential is used: a token, a JWT (renewed before it expires), or a client certificate (the pair
  is watched, and the hub connections roll over to a renewed pair).

### Credentials in logs

`AccessToken` and `ClientCredentials` hide their secrets when printed with `%v`, `%+v`, `%#v` or `%s`, and in `log/slog`.
Two things are not covered: `%p` and `%w` (fmt rejects them as bad verbs for these types and prints the struct raw, which
`go vet` flags), and `encoding/json`, whose `json.Marshal` writes the exported secret fields (`AccessToken.Value`,
`ClientCredentials.ClientSecret`), as `golang.org/x/oauth2.Token` does. Do not marshal these values.

```go
app, err := snodra.New(
	snodra.Service("orders-service"),
	snodra.Hubs("https://hub.example.test:7000"), // https:// implies TLS
	snodra.JWT(snodra.NewClientCredentials(snodra.ClientCredentials{
		TokenURL:         "https://idp.example.test/oauth2/token",
		ClientSecretFile: "/run/secrets/orders-service-secret",
	})),
)
```

## Behaviour worth knowing

- **Shutdown.** `App.Shutdown(ctx)` leaves the hubs in order (an empty registration so requests route elsewhere, then
  then every subscription it serves ends, then the handlers in flight for at most 5 s, then the stream half-close) and returns by `ctx`'s deadline.
  `App.Run` shuts down within 15 s. A handler that ignores its context outlives `Shutdown`: the library cannot stop a
  goroutine, so it stops waiting for it.
- **Cancelled requests.** When the hub cancels a request (the caller gave up, the request timed out), the handler's
  context is cancelled. A handler that ignores its context keeps its permit and its concurrency slot until it
  returns: they go back when the handler returns, not when the cancel arrives.
- **Streams that stall.** When a served stream waits longer than `snodra.StreamCreditTimeout` (10 minutes) for the
  caller's credit, the stream ends at once with TIMEOUT and the handler's context is cancelled; its permit and slot
  return only when the handler returns.
- **Panics.** A panic in a handler is recovered and answered as HANDLER_ERROR with the message `handler panicked`.
- **Errors on the wire.** A handler's error is HANDLER_ERROR unless it was made with `snodra.Unauthenticated`,
  `snodra.PermissionDenied` or `snodra.InvalidRequest`; `snodra.WithDetails` attaches machine-readable details, which only
  a HANDLER_ERROR carries.
- **Not yet specified.** Where the specification is silent or the other libraries differ from it, this library does what
  the Rust library does, except in SG-19, SG-22 and SG-23, where it departs from it (SG-22 and SG-23 as the Kotlin
  library does). [SPEC-GAPS.md](SPEC-GAPS.md) records each case with what the Rust library, the Kotlin library and this
  one do, and the question for the specification.

## Telemetry

The core only defines an interface (`snodra.Instrumentation`, installed with `snodra.Telemetry`) and costs nothing
without one. The separate module `github.com/snodra/snodra-messaging-go/otel` implements it on the OpenTelemetry API, with the
metric names and labels of the other libraries (request duration, connected hubs, handlers in flight, token fetches,
certificate rotations, credential refreshes). It also creates a span for every request a handler serves, which neither
the Rust nor the Kotlin library does, and names its instrumentation scope after this module (see SG-24 in
[SPEC-GAPS.md](SPEC-GAPS.md)). A call its caller cancels records no metric.

```go
app, err := snodra.New(
	snodra.Service("orders-service"),
	snodra.Hubs("snodra-server:7000"),
	snodra.Telemetry(otel.New()), // uses the global OpenTelemetry providers
)
```

## Feature level 1 and how it is proved

The protocol has feature levels; level 1 is the part every official library has: registration and the heartbeat,
reconnection, permits, unary requests with timeouts, retries and errors, caller identity and trace propagation,
streams, subscriptions, scatter-gather, credentials and TLS, and several hubs. The protocol and what a library must do
are specified at <https://snodra.damoreira.com/library-authors/>.

It is proved by the language-neutral conformance runner, [snodra-conformance](https://github.com/snodra/snodra-conformance):
[`examples/conformance-service`](examples/conformance-service) is this library's service for it, written on the public API
only, and the runner drives it against scripted hubs and a real one. To run the level yourself you need Docker:

```sh
scripts/conformance.sh                       # the 33 level-1 checks, a hub from the snodra/snodra-server image
scripts/conformance.sh --only L1-RETRY       # one check
SNODRA_HUB_PORT=17000 scripts/conformance.sh # when port 7000 is taken (the second hub is 100 above)
```

The script builds the service, downloads the pinned runner release into `bin/` and checks it against the sha256 pinned in
[`scripts/conformance-runner.sha256`](scripts/conformance-runner.sha256) (or uses `SNODRA_CONFORMANCE_BIN`), starts the
hub (`SNODRA_SERVER_VERSION` picks the image tag) and removes it afterwards. CI runs the same script with the pinned
runner and fails when a check fails or is skipped, and keeps the runner's report as a build artifact.

Unit tests need no Docker (`go test -race ./...`, in this module and in `otel/`); they run against an in-process scripted
hub.

## Licence

Apache-2.0, see [LICENSE](LICENSE).
