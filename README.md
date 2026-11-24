<p align="center">
  <img alt="Snodra Messaging for Go mascot" src="docs/assets/brand/svg/snodra-messaging.svg" width="160">
</p>

<h1 align="center">Snodra Messaging (Go)</h1>

<p align="center">
  Commands, queries, streams, scatter-gathers and subscriptions for Go services, routed by Snodra Server.
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
- **The hub connection**: registration, heartbeats, reconnects, flow control, several hubs and an ordered shutdown.
- **Credentials and identity**: a token (static, or a file read again), an OAuth2 JWT (renewed) or a client
  certificate (rolled over to a renewed pair); the caller's token verified against the issuer's keys, and guards on
  handlers.
- **Telemetry** behind an interface, with an OpenTelemetry implementation in the separate `observability` module.
- **Feature level 1**, proved by the 33 checks of the conformance runner. Events, stores, event processors and sagas
  (levels 2 to 4) are not in it yet.

## Install

```sh
go get github.com/snodra/snodra-messaging-go@latest
go get github.com/snodra/snodra-messaging-go/observability@latest   # optional: OpenTelemetry
```

Both modules are on the Go module proxy and need Go 1.26 or later; the core depends only on gRPC and protobuf. Both
modules of a release have the same version.

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

Quick start, the settings map, the API and testing: **<https://snodra.damoreira.com/docs/messaging/go/>**, and
`go doc -all` for the package. A complete service is [`examples/quickstart`](examples/quickstart/main.go). The
library speaks the Snodra protocol (`snodra.hub.v1`). Where the specification is silent, [SPEC-GAPS.md](SPEC-GAPS.md)
records what this library does. Other languages: <https://snodra.damoreira.com/>.

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
