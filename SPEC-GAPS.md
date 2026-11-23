# Specification gaps

Places where the [library specification](https://snodra.damoreira.com/library-authors/) is silent, or where the
reference library (the Rust library) departs from it. This library does on the wire what the reference library does,
unless the entry says otherwise: SG-19, SG-22 and SG-23 depart from it (SG-22 and SG-23 as the Kotlin library does), and
SG-24 records the telemetry names, scope and span that differ from it. Each entry says what the Rust library does,
what the Kotlin library does where it differs, and what this library does. The code carries a
`// TODO(spec-gap): SG-<n>` comment at the spot.

## Format of an entry

### SG-<n>: short title

- **Specification:** what the specification says (or that it is silent).
- **Reference library:** what the Rust library does (and the Kotlin library, where it differs).
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

### SG-3: the local guards of a stream

- **Specification:** [protocol.md, Timeouts](https://snodra.damoreira.com/library-authors/protocol/) sizes the
  client-side request guard of a request as "the hub's bound for the request + 1 s"; for a streaming query, whose
  answer is many parts, it names the hub's idle timeout ("no part within `Request.timeout_ms`", from the forward, reset
  by every part) but not how long the sender's own guard waits for the first part and between parts.
- **Reference library:** guards a stream like the hub's idle timeout: the hub's bound plus 1 s between parts, and
  twice the bound plus 1 s for the first part (the hub bounds a queue wait by the timeout, then starts its idle clock
  at the forward). When a guard fires the stream fails with `TIMEOUT` and is cancelled at the hub
  (`Cancel{reason "local guard timeout"}`), so that the handler stops holding its permit. A subscription's guard
  covers its initial result only (the bound plus 1 s).
- **This library:** does the same (`stream_client.go`, `subscribe.go`).
- **Question:** should the specification size the guards of a stream (first part and idle) as it sizes the unary one?

### SG-4: the low-water mark of a stream that was topped up

- **Specification:** [protocol.md, Flow control](https://snodra.damoreira.com/library-authors/protocol/) gives a
  finished request's permit back at once when the hub holds at most `min(16, initial_permits / 2)` of the stream's
  permits; with several hubs ([Several hubs](https://snodra.damoreira.com/library-authors/protocol/)) a stream is
  topped up with `Permits{count, grant}` when another hub leaves, and the page does not say whether the mark follows.
- **Settled:** the mark is the stream's `Register.initial_permits` alone, as the Kotlin library computes it; a top-up
  raises what the hub holds (and its cap on the session), not the mark. The Rust library computes the mark from
  everything the stream offered, top-ups included.
- **This library:** does what the Kotlin library does (`permits.go`); before, it followed the Rust library.
- **Question:** settled; the specification's `initial_permits` is the Register's.

### SG-5: when a JWT is renewed

- **Specification:** [protocol.md, Credentials](https://snodra.damoreira.com/library-authors/protocol/) says "both
  official libraries renew at 80 % of its lifetime", and the timing summary says "JWT renewal: at 80 % of the token's
  lifetime".
- **Reference library:** renews when a margin of a fifth of the lifetime, at least 5 s and at most half the lifetime,
  is left: at 80 % of a lifetime of 25 s or more, at 50 % of a 10 s token (renewed after 5 s), at 4 s of an 8 s one.
  Between two renewals it waits at least 1 s. The Kotlin library computes the same margin.
- **This library:** does what the reference libraries do (`credentials.go`, `refreshAt`).
- **Question:** should the specification state the margin (max(20 %, 5 s), at most half the lifetime) instead of a
  flat 80 %?

### SG-6: forwarded tokens without a JWKS

- **Specification:** [messages.md, Forwarded identity](https://snodra.damoreira.com/library-authors/messages/) says a
  library "must validate the token before it runs a handler that requires an authenticated caller, exactly as the
  service validates a REST call (signature against the issuer's JWKS, expiry, issuer)"; it does not say what a
  service configured without a JWKS does.
- **Settled:** a JWKS is required wherever a token is used as an identity. Registering a handler with a guard
  (authenticated caller, role, caller check) without a configured JWKS is a startup error. Without a JWKS a library
  never presents a token's claims as the caller: the caller is anonymous, and the token is only forwarded with the
  requests the handler sends. What can be judged without the keys is still refused with `UNAUTHENTICATED` before the
  handler lookup (SG-18): a header that is not a bearer token, a token that is not a JWS, an unsigned token (`alg:
  none`, an empty signature), an encrypted one (five segments). With a JWKS nothing changes.
- **This library:** does so. `RequireAuthenticated`, `RequireRole` and `RequireCaller` without `JWKS` (or
  `SNODRA_JWKS_URL`) make the registration fail, and so `Start` and `Run`; without a JWKS `CallerFrom` reports no
  caller and a warning is logged once per `App` when a token arrives (`identity.go`, `register.go`, `internal/jwt`).
  Before, it read the claims of a signed token unverified (`Caller.Verified` false, now removed) and let the guards
  pass on them, as the Rust library and the Kotlin library (without a `JwtDecoder`) still do.
- **Question:** settled.

### SG-7: a token endpoint that fails before any stream

- **Specification:** [protocol.md, Credentials](https://snodra.damoreira.com/library-authors/protocol/) says what to
  do after the hub closed a JWT session; it says nothing of an access token that cannot be had at all (the endpoint
  refuses, answers an error or cannot be reached) when a stream is about to open.
- **Reference library:** opens no stream (a token past its renewal margin is never presented), keeps the endpoint's
  error, prefixed "no access token for <service> (<instance>):", as the connection's last rejection (a per-connection
  value that a later accepted `Register` clears, not the application-wide latest refusal of a hub), logs it at error
  once per distinct message, and retries at the ordinary backoff (500 ms doubling to 5 s), not at the cap of a
  refusal. A failed renewal while a stream is registered keeps the current token and retries after 1 s, doubling to
  30 s.
- **This library:** does the same, and since it exposes only the application-wide refusal, `App.Rejection` shows the
  failure (code `UNAUTHENTICATED`, the message naming the access token) until a newer refusal replaces it
  (`connection.go`, `credentials.go`).
- **Question:** should the specification say that no stream is opened without a valid token, how that failure is
  exposed, and at which backoff it is retried?

### SG-8: the client certificate rollover

- **Specification:** [writing-a-library.md, Credentials](https://snodra.damoreira.com/library-authors/writing-a-library/)
  says the official libraries "watch the files, wait a random delay so replicas do not all leave at once, and roll
  their hubs over one at a time"; it names no period, no bound of the delay and no limit of a hub's rollover.
- **Reference library:** reads the files every 3 s; for a valid pair (past its notBefore, before its notAfter, for
  the service) that a registered connection does not present, waits once per pair a random delay uniform in
  `[0, min(30 s, a tenth of the presented certificate's remaining validity)]`, reads the files again, then rolls the
  connections in the order of the configured hubs, one at a time: a `Register` with no types, at most 10 s for the
  hub's requests (handlers running, requests written to it unanswered) to finish, the half-close, and a reconnect at
  once with the new pair; the next hub is rolled only once this one registered again, within 30 s, else the rollover
  stops until the files change again. A pair that cannot be used leaves every connection as it is (logged at error
  when it persists for a period); one not valid yet is waited for. The Kotlin library also reads the files every 3 s.
- **This library:** does the same (`tls.go`, `connection.go`).
- **Question:** should the specification name the period, the delay's bound and the limits, or leave them to each
  library?

### SG-9: the bounds of a shutdown

- **Specification:** [protocol.md, Shutting down](https://snodra.damoreira.com/library-authors/protocol/) fixes one
  bound, 1 s for the acks of the empty `Register`; the handler drain and the wait for the hub to finish its side after
  the half-close are "bounded" without a value.
- **Reference library:** waits at most 1 s for the acks, then 10 s for the handlers in flight, then 2 s for the
  served subscriptions' ends, then 2 s for the hub to finish each stream after the half-close.
- **This library:** applies the same bounds, each also bounded by the context given to `Shutdown` (when it ends first
  the streams are dropped and `Shutdown` returns the context's error) (`app.go`).
- **Question:** should the specification name these values, or leave them to each library?

### SG-10: how often an unwritten request moves to another hub

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/library-authors/protocol/) says a
  request never handed to a stream's transport "goes to the next hub with the same id"; it does not say how often,
  should the next stream be lost too before it writes the request.
- **Reference library:** moves such a request at most 8 times; the ninth loss fails the call as never sent
  ("'<type>' not written: 9 hub streams lost before it was"). A request whose stream is gone before it could even be
  queued goes to the next hub without counting as a move. Unary requests, scatter-gathers, streams and subscriptions
  move alike.
- **This library:** does the same (`send.go`, `stream_client.go`), failing with `ErrNeverSent`.
- **Also:** on a move the reference library and this one send the call's chosen timeout again as `Request.timeout_ms`
  (each hop is guarded by the full bound), while the Kotlin library sends what is left of the call's deadline.
- **Question:** should the specification bound the moves, and with which number; and should a moved request send its
  remaining time or its timeout unchanged?

### SG-11: which refusal a library keeps

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/library-authors/protocol/) says
  the official libraries log a refusal once per distinct message "and keep it as the connection's last rejection".
- **Reference library:** keeps two: a per-connection last rejection that a later accepted `Register` clears, and an
  application-wide latest rejection (over every hub) that only a newer refusal replaces; the conformance service
  reports the application-wide one.
- **This library:** exposes the application-wide one, `App.Rejection`, never cleared by an acceptance (`app.go`,
  `connection.go`).
- **Question:** is the kept rejection per connection and cleared on acceptance, or the application's latest refusal?

### SG-12: a request in a mode the handler does not serve

- **Specification:** [protocol.md, Handling a request](https://snodra.damoreira.com/library-authors/protocol/) says
  a request in the wrong mode for the handler ("a unary request to a streaming handler, for example") is
  `INVALID_REQUEST`; [Streaming](https://snodra.damoreira.com/library-authors/protocol/) also lists what the hub does
  with "a unary `Reply` to the target (a handler that does not stream)": it relays it as one part and an end, which
  reads as if a unary query handler could answer a streaming request.
- **Settled:** a library answers as the Kotlin library does; the Kotlin library's answers are the standard. After the
  token check (SG-18), whatever the handler:

  | Request | Command handler | Query handler | Streaming handler | Subscription handler | Type not served |
  |---|---|---|---|---|---|
  | unary | served (`COMMAND`); `NO_HANDLER` (`QUERY`) | served (`QUERY`); `NO_HANDLER` (`COMMAND`) | `Reply` `INVALID_REQUEST` | `Reply` `INVALID_REQUEST` (`<id>#0`: served) | `Reply` `NO_HANDLER` |
  | streaming | `ReplyEnd` `NO_HANDLER` | `ReplyEnd` `INVALID_REQUEST` | served | `ReplyEnd` `INVALID_REQUEST` | `ReplyEnd` `NO_HANDLER` |
  | subscription | `ReplyEnd` `INVALID_REQUEST` | `ReplyEnd` `INVALID_REQUEST` | `ReplyEnd` `INVALID_REQUEST` | admitted | `ReplyEnd` `INVALID_REQUEST` |

  A unary request of an unknown kind is `INVALID_REQUEST`; the kind of a streaming or a subscription request is not
  looked at (only query handlers serve those modes, and the hub refuses a command in them). The texts:
  "<type> is a streaming query: send it with Stream (mode STREAMING)", "<type> is a subscription query: send it with
  Subscribe (mode SUBSCRIPTION)", "<type> is not a streaming query handler (it answers mode UNARY)" (or
  `SUBSCRIPTION`), "<type> is not a subscription query handler (it answers mode UNARY)" (or `STREAMING`), and "no
  subscription handler for <type> on this instance". The Kotlin library names the handler's method in its "is not a
  ... handler" texts and says mode `UNARY` for every handler of another mode; this library names the mode the handler
  serves.
- **This library:** answers as the table says (`dispatch.go`, `subscription_handler.go`), pinned cell by cell by
  `TestEveryModeAgainstEveryHandler`. Before, it answered a streaming request to a subscription handler, and a
  subscription request of a command type or of a type not served, with `NO_HANDLER`, as the Rust library does.
- **Question:** settled; what remains is the specification's wording of the hub's relay of a unary `Reply`, which no
  official library's handler sends to a streaming request.

### SG-13: the initial request of a subscription and scatter-gather target ids

- **Specification:** [protocol.md, Subscription](https://snodra.damoreira.com/library-authors/protocol/) says the
  handler of a subscription type "recognises the initial request by its `#0` id and answers any other unary request
  with `ErrorCode.INVALID_REQUEST`"; [Scatter-gather](https://snodra.damoreira.com/library-authors/protocol/) numbers
  the targets `<id>#0`, `<id>#1`, ... too.
- **Reference library:** recognises the initial request by the `#0` suffix alone, as the specification says, and
  notes the collision: a scatter-gather of a subscription type would have its target `<id>#0` answered with the
  initial result (and the other targets with `INVALID_REQUEST`). Only a raw client can send one: the typed calls never
  gather a subscription type. The Kotlin library recognises it by the suffix alone too.
- **This library:** does what the reference library does (`dispatch.go`).
- **Question:** should the initial request carry something no scatter-gather target can (its own mode, or a header),
  or should the hub refuse a scatter-gather of a subscription type?

### SG-14: the http:// and https:// prefixes of a hub address

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/library-authors/protocol/) says a hub
  whose `RegisterAck.hub_id` is empty is known by its target as `host:port`, "with a `static://` prefix (and in the
  Rust library `http://` or `https://`) stripped"; it says nothing else of those two prefixes.
- **Reference library:** accepts `http://` and `https://` (after an optional `static://`) in the hub address list and
  strips them from a target that stands for a hub id. An `https://` address is reached over TLS with the system's
  roots when no TLS is configured; with a TLS configuration every hub is reached over TLS, `http://` ones too. Without
  one, every other address is plain text, so a list may mix both. `https://h:7000` and `h:7000` are two targets
  (`static://h:7000` and `http://h:7000` are the same one as `h:7000`).
- **This library:** does the same (`targets.go`, `tls.go`).
- **Question:** should the prefixes, and the TLS an `https://` address implies, be part of the specification for every
  library, or dropped from the reference library?

### SG-15: a live stream closed with a refusal code

- **Specification:** describes `ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED` and `PERMISSION_DENIED` as
  the hub's answer to a `Register` instead of an ack; it says nothing of a stream closed with one of them after its
  ack (for example access revoked on a live session), apart from the client certificate's expiry, which is an
  ordinary reconnect.
- **Reference library:** treats such a close as a refusal whether or not the stream was acknowledged (kept as the
  latest rejection, the next attempt after 80 to 100 % of the backoff cap), except a certificate session's
  `UNAUTHENTICATED`.
- **This library:** treats it as a refusal whether or not the stream was acknowledged, except an `UNAUTHENTICATED`
  close of an acknowledged stream when a client certificate is configured, which is an ordinary reconnect at the
  normal backoff, reading the certificate files again, as in the reference library (`connection.go`).
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
- **This library:** does the same (`dispatch.go`, `subscription_handler.go`, `identity.go`).
- **Question:** is a failing token `UNAUTHENTICATED` for every handler, or only for a handler that requires an
  authenticated caller (the others running as if the request carried no token)?

### SG-19: when a silent hub is dropped

- **Specification:** [protocol.md, Heartbeats](https://snodra.damoreira.com/library-authors/protocol/) says a library
  "must drop a stream that brought no echo for three heartbeat intervals"; it does not say when the library looks, nor
  how far past three intervals the drop may come.
- **Reference library:** looks on each heartbeat tick, before sending that tick's heartbeat, and drops the stream when
  more than three intervals have passed since the last echo it received. It also does so before the `RegisterAck`,
  with the 3 s interval and the stream's opening as the first sign of life: a stream that brings neither an ack nor
  an echo is dropped on its 9 s tick (which, firing a little after its deadline, sees more than 9 s), before its
  10 s handshake timeout. Since an echo normally arrives just after its tick, the drop normally comes on the fourth
  tick after the last echoed heartbeat; a tick that fires a little late, or an echo that is unusually fast, makes the
  third tick see "more than three intervals" and drop one tick earlier, exactly three intervals after the last echo.
  The Kotlin library also looks on the tick, from the
  `RegisterAck` on, and drops when the oldest unanswered heartbeat is more than three intervals old.
- **This library:** looks on each heartbeat tick from the `RegisterAck` on, with the reference library's comparison
  (more than three intervals since the last echo, or since the ack), and also waits until three heartbeats sent since
  that echo are unanswered: the drop always comes on the tick after the third unanswered heartbeat, between three and
  four intervals after the last echo (`connection.go`, `hubSilent`). Before the `RegisterAck` it does not look: a
  stream that brings neither an ack nor an echo is closed by the 10 s handshake timeout, about 1 s later than in the
  reference library (as the Kotlin library, whose ticks start at the ack).
- **Question:** should the specification name the drop moment (for example "on the first heartbeat tick after three
  unanswered heartbeats"), so that every library drops a silent hub within the same window?

### SG-20: the static token file

- **Specification:** [writing-a-library.md, Credentials](https://snodra.damoreira.com/library-authors/writing-a-library/)
  says a token file "is read again before every connect, and an empty file after a good read is a rotation caught
  half way: keep the last good token"; it says nothing of a file that cannot be read or is empty when the service
  starts, nor of a token that changes in the file while a stream is registered.
- **Reference library:** refuses to start when the file cannot be read or is empty. On every heartbeat tick of a
  registered stream it reads the file again and, when it holds another non-empty token, sends it once as
  `ClientMessage.credentials` (a rotation without a reconnect); a file that cannot be read or is empty changes
  nothing. The Kotlin library does both too.
- **This library:** does the same (`options.go`, `credentials.go`).
- **Question:** should the specification say that a token file must hold a token at startup, and that a changed
  token is sent on the live streams?

### SG-21: retrying a stream

- **Specification:** [protocol.md, Never sent or outcome unknown](https://snodra.damoreira.com/library-authors/protocol/)
  says that when a library retries, "all attempts share the caller's time budget, and each attempt sends what is left
  of it as `Request.timeout_ms`"; it says nothing of a streaming query, whose `Request.timeout_ms` is an idle timeout
  (the longest wait for the next part), nor of when a stream stops being retried.
- **Reference library:** retries a stream with the policy of a unary query, for the same errors, only until its first
  part, a clean end or an error it does not retry; a retry is a new stream with a fresh id that sends the caller's
  chosen idle timeout again, not what is left of the budget (the budget only bounds when a retry may start). After the
  first part nothing is retried. The Kotlin library does the same. A subscription is retried the same way until its
  initial result, but each attempt sends what is left of the budget, as a unary request does: there
  `Request.timeout_ms` bounds the initial result only.
- **This library:** does the same (`stream_client.go`; subscriptions in `subscribe.go`).
- **Question:** should a retried stream send the remaining budget, as a unary request does, or its idle timeout
  unchanged; and should the specification say that a stream is retried only before its first part?

### SG-22: a second target answering a hub id already served

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/library-authors/protocol/) says a later
  target answering a `hub_id` another target owns "is closed and parked [...] and is retried only once the owner is
  gone".
- **Reference library:** closes such a target for good: it is never retried, also when the owner later answers
  another id; with a `dns:///` target it is dropped once its address leaves the answer. An instance behind aliases or
  load-balanced addresses can so stay one hub short after a rolling restart of the hubs.
- **This library:** follows the specification, as the Kotlin library does (`pool.go`, `connection.go`): the
  duplicate's stream is closed and its address parked (no stream, not counted in the permit split, not in the hub ids
  or the status); once no kept connection owns that hub id any more (the owner now answers another id, or its address
  left the DNS answer and is being drained), the parked address is connected again at once, unless its own address
  left the DNS answer.
- **Question:** should the reference library park and retry a duplicate as the specification says?

### SG-23: the permit split during a client certificate rollover with several hubs

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/library-authors/protocol/) counts in N
  the hubs that are live or on their first connect attempt, and says "a hub that joins or returns gets
  `ceil(max / N)` on its new stream".
- **Reference library:** takes a hub being rolled over to a renewed client certificate out of N while another hub
  serves; the rolled hub's new `Register` therefore offers `ceil(max / (N - 1))` (5 of 5 with two hubs and a
  capacity of 5) and the other hubs are topped up meanwhile.
- **This library:** keeps counting the rolled hub, as the Kotlin library does: its new `Register` offers
  `ceil(max / N)` (3 of 5); the other hubs are topped up once its old stream has ended, as in both libraries
  (`pool.go`).
- **Question:** is a rolled hub a hub that "returns" (`ceil(max / N)`), or does it get the share of the hubs that
  stayed?

### SG-24: the names of the telemetry

- **Specification:** silent: the library-author pages define no metric, span or instrumentation scope names.
- **Reference libraries:** the Rust library and the Kotlin library emit the same instruments (`snodra.hub.client.request.duration`
  in seconds with `kind`, `type` and `outcome`, the gauges `snodra.hub.client.handlers.in.flight` and
  `snodra.hub.client.connected.hubs`, the counters `snodra.hub.client.token.fetches` with `result`,
  `snodra.hub.client.certificate.rotations` and `snodra.hub.client.credentials.refreshes.sent`), so one dashboard serves both.
  The Rust library's instrumentation scope is `snodra-messaging-rs`; Kotlin's Micrometer meters have none. Neither creates
  a span for a request its handlers serve (the Kotlin library only writes the current trace context into the requests it
  sends).
- **This library:** emits the same instrument names, units, attributes, bucket boundaries and descriptions (the
  `otel` module, `otel/otel.go`). Differences visible to a dashboard: the instrumentation scope is the Go import path
  `github.com/snodra/snodra-messaging-go`; a handled request gets a server span named after its message type, a child of its
  valid `traceparent`; the three counters are synchronous instead of observable, exported as the same monotonic sums with
  every series at 0 from the start; a call ended by the caller's own deadline (`context.DeadlineExceeded`) is `timeout`.
  A call the caller cancels records nothing, as in both libraries.
- **Question:** should the specification name the instruments and their labels as part of the library contract, and should
  every library name its scope after its package and create the handler span?
