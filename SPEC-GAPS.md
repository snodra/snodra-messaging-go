# Specification gaps

Places where the [library specification](https://snodra.damoreira.com/library-authors/) is silent, or where the
reference library departs from it. In each case this library follows the reference library, and the code carries a
`// TODO(spec-gap): SG-<n>` comment at the spot.

## Format of an entry

### SG-<n>: short title

- **Specification:** what the specification says (or that it is silent).
- **Reference library:** what the Rust library does.
- **This library:** what the Go code does.
- **Question:** the open question for the specification owner.

## Entries

### SG-1: the backoff after a stream closed with UNAVAILABLE

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/library-authors/protocol/)
  says a stream the hub closes with `UNAVAILABLE` "is an ordinary closure: reconnect from the backoff floor".
- **Reference library:** goes back to the floor (500 ms) only after an attempt that reached the `RegisterAck`; an
  `UNAVAILABLE` close before the ack keeps doubling the wait like any other failure.
- **This library:** does what the reference library does (`connection.go`).
- **Question:** does "from the backoff floor" mean a reset after every `UNAVAILABLE` close, even of a stream that was
  never acknowledged, or only that `UNAVAILABLE` is not a refusal (no wait at the cap)?

### SG-9: the bounds of a shutdown

- **Specification:** [protocol.md, Shutting down](https://snodra.damoreira.com/library-authors/protocol/) fixes one
  bound, 1 s for the acks of the empty `Register`; the handler drain and the wait for the hub to finish its side after
  the half-close are "bounded" without a value.
- **Reference library:** waits at most 1 s for the acks, then 10 s for the handlers in flight, then 2 s for the
  served subscriptions' ends, then 2 s for the hub to finish each stream after the half-close.
- **This library:** applies the same bounds, each also bounded by the context given to `Shutdown` (when it ends first
  the streams are dropped and `Shutdown` returns the context's error) (`app.go`).
- **Question:** should the specification name these values, or leave them to each library?

### SG-11: which refusal a library keeps

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/library-authors/protocol/) says
  the official libraries log a refusal once per distinct message "and keep it as the connection's last rejection".
- **Reference library:** keeps two: a per-connection last rejection that a later accepted `Register` clears, and an
  application-wide latest rejection (over every hub) that only a newer refusal replaces; the conformance service
  reports the application-wide one.
- **This library:** exposes the application-wide one, `App.Rejection`, never cleared by an acceptance (`app.go`,
  `connection.go`).
- **Question:** is the kept rejection per connection and cleared on acceptance, or the application's latest refusal?

### SG-15: a live stream closed with a refusal code

- **Specification:** describes `ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED` and `PERMISSION_DENIED` as
  the hub's answer to a `Register` instead of an ack; it says nothing of a stream closed with one of them after its
  ack (for example access revoked on a live session), apart from the client certificate's expiry, which is an
  ordinary reconnect.
- **Reference library:** treats such a close as a refusal whether or not the stream was acknowledged (kept as the
  latest rejection, the next attempt after 80 to 100 % of the backoff cap), except a certificate session's
  `UNAUTHENTICATED`.
- **This library:** treats it as a refusal whether or not the stream was acknowledged (`connection.go`); the
  certificate exception comes with client certificates.
- **Question:** is a refusal code on an acknowledged stream a refusal of the registration?
