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

### SG-2: the retry budget when the caller chose no timeout

- **Specification:** [protocol.md, Never sent or outcome unknown](https://snodra.damoreira.com/library-authors/protocol/)
  says that when a library retries, "all attempts share the caller's time budget, and each attempt sends what is left
  of it as `Request.timeout_ms`, never 0"; it does not say what the budget is when the caller chose no timeout, nor
  how little of it may be left for a retry to start.
- **Reference library:** the budget is the caller's timeout; else the hub's per-type override for the type, else
  `RegisterAck.default_timeout_ms` (from the ack of the hub the request would go to); else 5 s (no hub acknowledged
  yet). A retry starts only when at least `max(initial backoff, 250 ms)` of the budget is left after its wait;
  otherwise the call ends with the last error.
- **This library:** does the same (`retry.go`).
- **Question:** should the specification name the budget without a caller's timeout, and the least time left for a
  retry?

### SG-6: forwarded tokens without a JWKS

- **Specification:** [messages.md, Forwarded identity](https://snodra.damoreira.com/library-authors/messages/) says a
  library "must validate the token before it runs a handler that requires an authenticated caller, exactly as the
  service validates a REST call (signature against the issuer's JWKS, expiry, issuer)"; it does not say what a
  service configured without a JWKS does.
- **Reference library:** without a JWKS URL, reads the claims of a signed token without checking its signature (the
  caller's `verified` is false), logs a warning once, and lets the guards pass on those claims; it refuses an
  unsigned token (`alg: none`, or an empty signature) and an encrypted one (five segments) with `UNAUTHENTICATED`. The
  Kotlin library does the same without a `JwtDecoder`.
- **This library:** does the same: without `JWKS` (or `SNODRA_JWKS_URL`), `Caller.Verified` is false, the warning
  is logged once per `App`, and the guards pass on the unverified claims (`identity.go`, `internal/jwt`).
- **Question:** may a library run a guarded handler on an unverified token, or must a guarded handler refuse every
  caller when no JWKS is configured?

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

### SG-16: a configured reconnect backoff outside the specified range

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/library-authors/protocol/)
  says that after a refused `Register` a library "waits at least 1 s, and at most its backoff cap of at most 5 s"
  before the next attempt; [Reconnect and backoff](https://snodra.damoreira.com/library-authors/protocol/) caps the
  backoff at 5 s.
- **Reference library:** refuses only a minimum or maximum of 0 and a minimum above the maximum. A maximum above
  5 s is accepted, and a refused `Register` waits 80 to 100 % of the configured maximum, without a 1 s floor (so a
  maximum of 100 ms retries a refusal every 80 to 100 ms). The Kotlin library does the same.
- **This library:** does what the reference libraries do (`options.go`, `connection.go`); the shipped default
  (500 ms to 5 s) is inside the specified range, and `ReconnectBackoff` documents the range.
- **Question:** should a library refuse (or clamp) a configured maximum above 5 s, and floor a refusal's wait at
  1 s whatever the configuration?

### SG-17: the message of the error codes no handler produces

- **Specification:** [protocol.md, Replies and errors](https://snodra.damoreira.com/library-authors/protocol/) says a
  library must "keep `Error.message` for the codes a handler produces" (`HANDLER_ERROR`, `UNAUTHENTICATED`,
  `PERMISSION_DENIED`, `INVALID_REQUEST`); it says nothing of the message of the other codes.
- **Reference library:** keeps the hub's message for an unknown code (read as `HANDLER_ERROR`); keeps it for
  `CANCELLED` too, prefixed with the type ("'<type>': <hub's message>"); and replaces it with its own text naming the
  type for `NO_HANDLER` ("No handler registered for '<type>'"), `TIMEOUT` ("Timed out waiting for reply to
  '<type>'"), `HANDLER_DISCONNECTED` ("Handler for '<type>' disconnected") and `RESOURCE_EXHAUSTED` ("Handler for
  '<type>' is busy"). The Kotlin library does the same for these four codes (for a unary request), except that its
  `RESOURCE_EXHAUSTED` text has a suffix: "Handler for '<type>' is busy: its hub queue or session buffer is full".
- **This library:** does what the Rust library does: `Error.Message` holds those texts for those four codes, the
  prefixed message for `CANCELLED`, the hub's message otherwise (`errors.go`).
- **Question:** should a library keep the hub's message for every code (it is safe to log), or is it free to replace
  it for the codes no handler produces?

### SG-18: a forwarded token that fails, for a handler without a guard

- **Specification:** [messages.md, Forwarded identity](https://snodra.damoreira.com/library-authors/messages/) asks a
  library to validate the token "before it runs a handler that requires an authenticated caller"; it says nothing of
  a request that carries a token that fails (unreadable, expired, wrongly signed) to a handler that requires no
  caller, nor of such a request to a type the instance does not serve.
- **Reference library:** checks the token of every request first, before it looks the handler up: a token that fails
  is answered `UNAUTHENTICATED` whatever the handler (also instead of `NO_HANDLER`); a request without a token is an
  anonymous caller and runs. The Kotlin library does the same.
- **This library:** does the same (`dispatch.go`, `identity.go`).
- **Question:** is a failing token `UNAUTHENTICATED` for every handler, or only for a handler that requires an
  authenticated caller (the others running as if the request carried no token)?

### SG-19: when a silent hub is dropped

- **Specification:** [protocol.md, Heartbeats](https://snodra.damoreira.com/library-authors/protocol/) says a library
  "must drop a stream that brought no echo for three heartbeat intervals"; it does not say when the library looks, nor
  how far past three intervals the drop may come.
- **Reference library:** looks on each heartbeat tick, before sending that tick's heartbeat, and drops the stream when
  more than three intervals have passed since the last echo it received (also before the `RegisterAck`, with the
  3 s interval, where the 10 s handshake timeout comes first in practice). Since an echo normally arrives just after
  its tick, the drop normally comes on the fourth tick after the last echoed heartbeat; a tick that fires a little
  late, or an echo that is unusually fast, makes the third tick see "more than three intervals" and drop one tick
  earlier, exactly three intervals after the last echo. The Kotlin library also looks on the tick, from the
  `RegisterAck` on, and drops when the oldest unanswered heartbeat is more than three intervals old.
- **This library:** looks on each heartbeat tick from the `RegisterAck` on, with the reference library's comparison
  (more than three intervals since the last echo, or since the ack), and also waits until three heartbeats sent since
  that echo are unanswered: the drop always comes on the tick after the third unanswered heartbeat, between three and
  four intervals after the last echo (`connection.go`, `hubSilent`).
- **Question:** should the specification name the drop moment (for example "on the first heartbeat tick after three
  unanswered heartbeats"), so that every library drops a silent hub within the same window?
