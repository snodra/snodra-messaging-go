<p align="center">
  <img alt="Snodra Messaging for Go mascot" src="docs/assets/brand/svg/snodra-messaging.svg" width="160">
</p>

<h1 align="center">Snodra Messaging (Go)</h1>

<p align="center">
  Commands, queries, streams, scatter-gathers and subscriptions — events through an outbox, processors with
  control — for Go services, routed by Snodra Server.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-messaging-go/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-messaging-go/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pkg.go.dev/github.com/snodra/snodra-messaging-go"><img alt="Go reference" src="https://pkg.go.dev/badge/github.com/snodra/snodra-messaging-go.svg"></a>
  <a href="https://github.com/snodra/snodra-messaging-go/releases"><img alt="release" src="https://img.shields.io/github/v/release/snodra/snodra-messaging-go"></a>
  <img alt="Go" src="https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white">
  <a href="https://snodra.damoreira.com/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

One package, `snodra`, that gives a Go service what it needs to send and serve requests in a Snodra system, with the
same wire protocol as services written with any Snodra library: you write functions, the library keeps the hub streams, permits,
retries and timeouts.

- **Commands and queries** through Snodra Server, with deadlines, retries and routing keys. A command or a query that
  is not shared, of a type the service handles itself, is answered in-process by its own handler, without the hub;
  `snodra.AlwaysRouteThroughHub(true)` sends it through the hub like any other request.
- **Every query mode**: point, scatter-gather, streaming and subscription; streams and updates are `iter.Seq2`.
- **Events** through an outbox: `Publish` appends the event in the transaction the context carries, and a relay in
  every instance publishes it to the event store, at least once. `Key` is the order key.
- **Event handlers** with processed-event markers: `HandleEvent` runs in the `default` processor or in a named one
  (`InProcessor`), each with one subscription and `processor:<processor>:<eventId>` markers, so a redelivery is
  skipped and one processor's failure does not hold another's.
- **Processors, status and control**: every started processor reports its state, segments, counters and dead-letter
  count to the hub. The dashboard pauses and resumes it persistently; a leased reset stops its subscription, runs the
  `HandleReset` handlers when anything replays, deletes the replayed range's markers, and replays from earliest, latest,
  timestamp or explicit positions. `DisallowReplay()` keeps a handler out of a replay, and all control runs off the hub
  read loop.
- **Sagas**: `HandleSaga` runs in the `default` processor after the event handlers (stage 2), with associations, an
  optimistic state version, parked failures, deadlines and the local `SagaCommandFailed` notice. A saga records what
  it sends; a relay dispatches each saga's commands and events after its transaction committed, in order under a
  lease. A command is sent by its wire type with `idempotency-key`/`saga-id`, the handled event's correlation and
  trace, and no caller identity; transient failures are retried by the relay, permanent ones come back as the notice.
  A deadline scheduler claims each due deadline with a lease, runs its handler and marks it fired in one transaction.
- **Idempotent external calls**: `HandleIdempotentCommand` records each call under the `idempotency-key` header in
  `external_calls`: the first insert wins, a repeat after success returns the first reply, a permanent failure is
  replayed, and a lapsed call is given up as unknown (or taken over with `ExternalIdempotent()`). `Retryable` and
  `UnknownOutcome` write the two reserved `Error.details` markers, which `IsRetryable` and `IsUnknownOutcome` read
  locally and from a hub; the saga relay owns the backoff.
- **Dead letters**: a record that exhausts its attempts is parked according to `DeadLetterMode`
  (`SNODRA_DEAD_LETTER_MODE`): `collection` keeps it in the message store's dead letters, with list, retry,
  retry-all and discard driven from the dashboard; `event-store` publishes it to the `<source>.dlt` source with the
  six `snodra-dlt-*` headers, when the store declares `native-dead-letter`; `log` logs it and moves on.
- **Add-ons**: Kafka and RabbitMQ Streams for the event store; MongoDB and SQL (PostgreSQL, MySQL) for the outbox, the
  markers, the dead letters, the processor control, the external calls and the saga records; in-memory stores for
  tests. Each add-on is a module of its own, and `snodratest` is the add-on kit.
- **The hub connection**: registration, heartbeats, reconnects, flow control, several hubs and an ordered shutdown.
- **Credentials and identity**: a token (static, or a file read again), an OAuth2 JWT (renewed) or a client
  certificate (rolled over to a renewed pair); the caller's token verified against the issuer's keys, and guards on
  handlers.
- **Telemetry** behind an interface, with an OpenTelemetry implementation in the separate `observability` module.
- **Feature level 3**, proved by the 33 level-1 checks of the conformance runner and by the level-2 and level-3
  event-store and message-store kits. The saga store ports, the level-4 store checks, the saga runtime (stage 2,
  the dispatch relay, failure notices and deadlines) and the idempotent external-call adapter with its two markers are
  implemented on `main`; level 4 is not claimed or released yet.

## Install

```sh
go get github.com/snodra/snodra-messaging-go@latest
go get github.com/snodra/snodra-messaging-go/observability@latest   # optional: OpenTelemetry

# One event store and one message store, when the service publishes or handles events:
go get github.com/snodra/snodra-messaging-go/kafka@latest           # Kafka event store
go get github.com/snodra/snodra-messaging-go/rabbitmq@latest        # RabbitMQ Streams event store
go get github.com/snodra/snodra-messaging-go/mongo@latest           # MongoDB message store
go get github.com/snodra/snodra-messaging-go/sql@latest             # SQL message store (PostgreSQL, MySQL)
```

The modules are on the Go module proxy and need Go 1.26 or later; the core depends only on gRPC and protobuf. The
modules of a release share its version. Each add-on depends on the core and on its own store client, never the reverse.

## Add-ons

| Add-on | Module | Holds |
|---|---|---|
| Kafka event store | `github.com/snodra/snodra-messaging-go/kafka` | Events, in ordered partitions |
| RabbitMQ Streams event store | `github.com/snodra/snodra-messaging-go/rabbitmq` | Events, in partitioned streams |
| MongoDB message store | `github.com/snodra/snodra-messaging-go/mongo` | Outbox, markers, dead letters and processor control, in the shared collections |
| SQL message store | `github.com/snodra/snodra-messaging-go/sql` | Outbox, markers, dead letters and processor control, in the shared V1 tables |

A service needs one event store and one message store, each passed to `snodra.New` with `WithEventStore` and
`WithStore`. The event stores follow the shared wire layouts and declare their capabilities; the message stores follow
the shared SQL V1 and MongoDB layouts. The Go event-store add-ons have no event browser, so they declare no `browse`.
The settings and guarantees are on <https://snodra.damoreira.com/docs/add-ons/>.

## A handler

```go
app, err := snodra.New(snodra.Service("orders-service"), snodra.FromEnv())
if err != nil {
	return err
}
err = snodra.HandleCommand(app, func(ctx context.Context, cmd PlaceOrder) (Receipt, error) {
	return Receipt{OrderID: cmd.OrderID, Total: cmd.Cents}, nil
})
if err != nil {
	return err
}
return app.Run(ctx) // connects to every hub, serves until ctx ends, then shuts down in order
```

A message is a Go type with a `MessageType() string` method; its payload is JSON by default (see `snodra.Codec`).
Settings are options, and `snodra.FromEnv()` reads the `SNODRA_*` variables; options given after it win.

## Documentation

Quick start, the settings map, the API, events and testing: **<https://snodra.damoreira.com/docs/messaging/go/>**, and
`go doc -all` for the package. A complete service is [`examples/quickstart`](examples/quickstart/main.go). The
library speaks the Snodra protocol (`snodra.hub.v1`). Where the specification is silent, [SPEC-GAPS.md](SPEC-GAPS.md)
records what this library does. Other languages: <https://snodra.damoreira.com/>.

- Events, the outbox, the relay and processors: [Go API, Events](https://snodra.damoreira.com/docs/messaging/go/api/#events)
- Every `SNODRA_*` variable, the core's and the add-ons': [Go configuration](https://snodra.damoreira.com/docs/messaging/go/configuration/)
- The four add-ons: [Add-ons](https://snodra.damoreira.com/docs/add-ons/)
- Testing handlers, the add-on kits and the conformance runner: [Go testing](https://snodra.damoreira.com/docs/messaging/go/testing/)

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## Releases

Versions are cut by CI from the commit types. Never push a tag. When the checks pass on a push to
`main`, the subjects since the last `v*` tag decide. Any `feat` is the next minor. Otherwise any `fix`
or `perf` is the next patch. Anything else (`docs`, `test`, `chore`, `ci`, `refactor`, `build`,
`style`) releases nothing.

The two modules share one version, and each has its own tag: `vX.Y.Z` for
`github.com/snodra/snodra-messaging-go` and `observability/vX.Y.Z` for
`github.com/snodra/snodra-messaging-go/observability`. CI commits `release: X.Y.Z` (`Version` in `version.go`,
the requirement of `observability/go.mod` on the root module with its hashes in `observability/go.sum`, and the
`go.work` replacement) and tags that commit twice. Before anything is pushed, both modules are built and tested as a
consumer gets them, from the new tag. A last job, which builds nothing, pushes the commit and both tags together or
not at all. The release workflow then has the Go proxy fetch both versions and creates the GitHub release. Pull
before you push: `main` then carries that commit.

- Below 1.0.0, any `type!:` subject and any line starting `BREAKING CHANGE:` in a message count as a
  minor, never a major. 1.0.0 is declared by hand.
- A revert (`Revert "feat: ..."`, `revert:`) is not a release reason. The reverted change stays in the
  last published version until the next `feat`, `fix` or `perf`.
- Only tags shaped `vN.N.N` count. The highest one reachable from `main` is the last release.
- The Go proxy keeps a version forever: a tag is never moved or deleted. A wrong release is fixed by the next one.
- `scripts/next-version.sh` holds the rule. `for t in scripts/*.test.sh; do sh "$t"; done` tests it.

If a release fails after the push (the tags exist, the GitHub release does not), use **Re-run failed jobs**
on that run: every step is safe to repeat. Or run the `release` workflow by hand with the existing `vX.Y.Z`
tag. **Re-run all jobs** on a run that released does nothing, because `main` has moved to its release
commit. If it fails before the push, nothing was pushed: fix the cause and re-run the failed jobs (while `main`
has not moved), or push the fix, whose run releases.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
