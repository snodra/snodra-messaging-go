# Specification gaps

Places where the [library specification](https://snodra.damoreira.com/docs/library-authors/) is silent or ambiguous. Each
entry says what the specification says, what this library does where it leaves a choice, and whether the question is
settled; where an entry is settled its behaviour is the specification. The code carries a
`// TODO(spec-gap): SG-<n>` comment at the spot.

## Format of an entry

### SG-<n>: short title

- **Specification:** what the specification says (or that it is silent).
- **Settled:** what is now the specification, when the entry is settled.
- **This library:** what the Go code does.
- **Question:** the open question, or that it is settled.

## Entries

### SG-1: the backoff after a stream closed with UNAVAILABLE

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/docs/library-authors/protocol/)
  says a stream the hub closes with `UNAVAILABLE` "is an ordinary closure: reconnect from the backoff floor".
- **This library:** goes back to the floor (500 ms) only after an attempt that reached the `RegisterAck`; an
  `UNAVAILABLE` close before the ack keeps doubling the wait like any other failure (`connection.go`).
- **Question:** settled: that is the specification's reading (the floor comes back only after an acknowledged stream;
  `UNAVAILABLE` is not a refusal).

### SG-2: the retry budget when the caller chose no timeout

- **Specification:** [protocol.md, Never sent or outcome unknown](https://snodra.damoreira.com/docs/library-authors/protocol/)
  says that when a library retries, "all attempts share the caller's time budget, and each attempt sends what is left
  of it as `Request.timeout_ms`, never 0"; it does not say what the budget is when the caller chose no timeout, nor
  how little of it may be left for a retry to start.
- **This library:** the budget is the caller's timeout; else the hub's per-type override for the type, else
  `RegisterAck.default_timeout_ms` (from the ack of the hub the request would go to); else 5 s (no hub acknowledged
  yet). A retry starts only when at least `max(initial backoff, 250 ms)` of the budget is left after its wait;
  otherwise the call ends with the last error (`retry.go`).
- **Question:** settled: the budget and the least time left above are the specification (with no hub acknowledged yet,
  5 s).

### SG-3: the local guards of a stream

- **Specification:** [protocol.md, Timeouts](https://snodra.damoreira.com/docs/library-authors/protocol/) sizes the
  client-side request guard of a request as "the hub's bound for the request + 1 s"; for a streaming query, whose
  answer is many parts, it names the hub's idle timeout ("no part within `Request.timeout_ms`", from the forward, reset
  by every part) but not how long the sender's own guard waits for the first part and between parts.
- **This library:** guards a stream like the hub's idle timeout: the hub's bound plus 1 s between parts, and
  twice the bound plus 1 s for the first part (the hub bounds a queue wait by the timeout, then starts its idle clock
  at the forward). When a guard fires the stream fails with `TIMEOUT` and is cancelled at the hub
  (`Cancel{reason "local guard timeout"}`), so that the handler stops holding its permit. A subscription's guard
  covers its initial result only (the bound plus 1 s) (`stream_client.go`, `subscribe.go`).
- **Question:** settled: the guards above are the specification.

### SG-4: the low-water mark of a stream that was topped up

- **Specification:** [protocol.md, Flow control](https://snodra.damoreira.com/docs/library-authors/protocol/) gives a
  finished request's permit back at once when the hub holds at most `min(16, initial_permits / 2)` of the stream's
  permits; with several hubs ([Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/)) a stream is
  topped up with `Permits{count, grant}` when another hub leaves, and the page does not say whether the mark follows.
- **Settled:** the mark is the stream's `Register.initial_permits` alone; a top-up raises what the hub holds (and its
  cap on the session), not the mark.
- **This library:** computes the mark that way (`permits.go`).
- **Question:** settled; the specification's `initial_permits` is the Register's.

### SG-5: when a JWT is renewed

- **Specification:** [protocol.md, Credentials](https://snodra.damoreira.com/docs/library-authors/protocol/) says "both
  official libraries renew at 80 % of its lifetime", and the timing summary says "JWT renewal: at 80 % of the token's
  lifetime".
- **This library:** renews when a margin of a fifth of the lifetime, at least 5 s and at most half the lifetime,
  is left: at 80 % of a lifetime of 25 s or more, at 50 % of a 10 s token (renewed after 5 s), at 4 s of an 8 s one.
  Between two renewals it waits at least 1 s (`credentials.go`, `refreshAt`).
- **Question:** settled: the margin above is the specification.

### SG-6: forwarded tokens without a JWKS

- **Specification:** [messages.md, Forwarded identity](https://snodra.damoreira.com/docs/library-authors/messages/) says a
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
  pass on them.
- **Question:** settled.

### SG-7: a token endpoint that fails before any stream

- **Specification:** [protocol.md, Credentials](https://snodra.damoreira.com/docs/library-authors/protocol/) says what to
  do after the hub closed a JWT session; it says nothing of an access token that cannot be had at all (the endpoint
  refuses, answers an error or cannot be reached) when a stream is about to open.
- **This library:** opens no stream (a token past its renewal margin is never presented), keeps the endpoint's
  error, prefixed "no access token for <service> (<instance>):", as the connection's last rejection (a per-connection
  value that a later accepted `Register` clears, not the application-wide latest refusal of a hub), logs it at error
  once per distinct message, and retries at the ordinary backoff (500 ms doubling to 5 s), not at the cap of a
  refusal. A failed renewal while a stream is registered keeps the current token and retries after 1 s, doubling to
  30 s. It exposes only the application-wide refusal, so `App.Rejection` shows the failure (code `UNAUTHENTICATED`,
  the message naming the access token) until a newer refusal replaces it (`connection.go`, `credentials.go`; SG-11).
- **Question:** settled: the behaviour above is the specification.

### SG-8: the client certificate rollover

- **Specification:** [writing-a-library.md, Credentials](https://snodra.damoreira.com/docs/library-authors/writing-a-library/)
  says the official libraries "watch the files, wait a random delay so replicas do not all leave at once, and roll
  their hubs over one at a time"; it names no period, no bound of the delay and no limit of a hub's rollover.
- **This library:** reads the files every 3 s; for a valid pair (past its notBefore, before its notAfter, for
  the service) that a registered connection does not present, waits once per pair a random delay uniform in
  `[0, min(30 s, a tenth of the presented certificate's remaining validity)]`, reads the files again, then rolls the
  connections in the order of the configured hubs, one at a time: a `Register` with no types, at most 10 s for the
  hub's requests (handlers running, requests written to it unanswered) to finish, the half-close, and a reconnect at
  once with the new pair; the next hub is rolled only once this one registered again, within 30 s, else the rollover
  stops until the files change again. A pair that cannot be used leaves every connection as it is (logged at error
  when it persists for a period); one not valid yet is waited for (`tls.go`, `connection.go`).
- **Question:** should the specification name the period, the delay's bound and the limits, or leave them to each
  library?

### SG-9: the bounds of a shutdown

- **Specification:** [protocol.md, Shutting down](https://snodra.damoreira.com/docs/library-authors/protocol/) fixes one
  bound, 1 s for the acks of the empty `Register`; the handler drain and the wait for the hub to finish its side after
  the half-close are "bounded" without a value.
- **This library:** waits at most 1 s for the acks, then ends every subscription it serves, then waits at most 5 s for
  the handlers in flight, then half-closes each stream and waits at most 2 s for the hub to finish it. Each wait is
  also bounded by the context given to `Shutdown` (when it ends first the streams are dropped and `Shutdown` returns
  the context's error), and the subscriptions' ends are waited for at most 2 s more after the drain (`app.go`).
- **Question:** should the specification name these values, or leave them to each library?

### SG-10: how often an unwritten request moves to another hub

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/) says a
  request never handed to a stream's transport "goes to the next hub with the same id"; it does not say how often,
  should the next stream be lost too before it writes the request.
- **Settled:** such a request moves at most 8 times; the ninth loss fails the call as never sent ("'<type>' not
  written: 9 hub streams lost before it was"). A request whose stream is gone before it could even be queued goes to
  the next hub without counting as a move. Every move sends the call's chosen timeout again as `Request.timeout_ms`
  (each hop is guarded by the full bound), and so does the walk over the hubs that answer a unary request
  `NO_HANDLER`. One deadline bounds the call: when no hub can take the request it waits for one, and it starts no new
  hop, once its chosen timeout (5 s without one) has passed since it began; so a call lasts at most that plus one
  hop's guard. Unary requests, scatter-gathers, streams and subscriptions move alike. A unary request that reaches
  the deadline after a `NO_HANDLER` with a live hub still untried fails as never sent too; when every live hub
  declined, the last answer comes back, also after the deadline. A hub that did not take the request (its stream was
  gone before the request was queued, or was lost while it was queued) is not tried again in that call either, until
  the live hubs change while no other hub can take it.
- **This library:** does so (`send.go`, `stream_client.go`), failing with `ErrNeverSent`. Before, it failed at once
  when no hub was acknowledged (outside a client certificate rollover), and later each move waited for a hub anew;
  it returned the last `NO_HANDLER` at the deadline whether or not a live hub was still untried, and it skipped only
  the hubs that declined.
- **Question:** settled.

### SG-11: which refusal a library keeps

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/docs/library-authors/protocol/) says
  the official libraries log a refusal once per distinct message "and keep it as the connection's last rejection".
- **This library:** keeps two: a per-connection last rejection that a later accepted `Register` clears, and an
  application-wide latest rejection (over every hub) that only a newer refusal replaces. It exposes the
  application-wide one, `App.Rejection`, never cleared by an acceptance (`app.go`, `connection.go`).
- **Question:** is the kept rejection per connection and cleared on acceptance, or the application's latest refusal?

### SG-12: a request in a mode the handler does not serve

- **Specification:** [protocol.md, Handling a request](https://snodra.damoreira.com/docs/library-authors/protocol/) says
  a request in the wrong mode for the handler ("a unary request to a streaming handler, for example") is
  `INVALID_REQUEST`; [Streaming](https://snodra.damoreira.com/docs/library-authors/protocol/) also lists what the hub does
  with "a unary `Reply` to the target (a handler that does not stream)": it relays it as one part and an end, which
  reads as if a unary query handler could answer a streaming request.
- **Settled:** a library answers as the table says. After the token check (SG-18), whatever the handler:

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
  subscription handler for <type> on this instance". In the "is not a ... handler" texts this library names the mode
  the handler serves.
- **The hub first:** since services declare their subscription types (SG-13), the hub itself refuses with one
  `Error{INVALID_REQUEST}`, before any instance gets anything, a unary, streaming or scatter-gather request of a
  declared subscription type, and a subscription request of a query type nobody declared; and a streaming or a
  subscription request of a command type. The handler-side answers above stay as a second line of defence: a hub
  that does not check, and a request answered in-process, reach them.
- **This library:** answers as the table says (`dispatch.go`, `subscription_handler.go`), pinned cell by cell by
  `TestEveryModeAgainstEveryHandler`. Before, it answered a streaming request to a subscription handler, and a
  subscription request of a command type or of a type not served, with `NO_HANDLER`.
- **Question:** settled; what remains is the specification's wording of the hub's relay of a unary `Reply`, which no
  handler of this library sends to a streaming request.

### SG-13: the initial request of a subscription and scatter-gather target ids

- **Specification:** [protocol.md, Subscription](https://snodra.damoreira.com/docs/library-authors/protocol/) says the
  handler of a subscription type "recognises the initial request by its `#0` id and answers any other unary request
  with `ErrorCode.INVALID_REQUEST`"; [Scatter-gather](https://snodra.damoreira.com/docs/library-authors/protocol/) numbers
  the targets `<id>#0`, `<id>#1`, ... too, so a scatter-gather of a subscription type would have its target `<id>#0`
  answered with an initial result.
- **Settled:** a service declares the types its subscription handlers serve in `Register.subscription_query_types`
  (each also in `query_types` or `shared_query_types`; the registrants of a type must agree, else `ALREADY_EXISTS`).
  The hub serves a declared type in mode `SUBSCRIPTION` only: a point query, a streaming query or a scatter-gather of
  it is refused with one `Error{INVALID_REQUEST}` before any instance gets anything, and a subscription of a query
  type nobody declared likewise. A `#0` request at a subscription handler therefore always comes from the hub.
- **This library:** declares every type registered with `HandleSubscription`, shared or not, on every `Register`
  (the empty one of a shutdown, a rollover or a withdrawn address clears it with the other lists), and still
  recognises the initial request by its `#0` suffix (`app.go`, `register.go`, `dispatch.go`).
- **Question:** settled.

### SG-14: the http:// and https:// prefixes of a hub address

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/) says a hub
  whose `RegisterAck.hub_id` is empty is known by its target as `host:port`, "with a `static://` prefix (and
  `http://` or `https://`, for a library that accepts them) stripped"; it says nothing else of those two prefixes.
- **This library:** accepts `http://` and `https://` (after an optional `static://`) in the hub address list and
  strips them from a target that stands for a hub id. An `https://` address is reached over TLS with the system's
  roots when no TLS is configured; with a TLS configuration every hub is reached over TLS, `http://` ones too. Without
  one, every other address is plain text, so a list may mix both. `https://h:7000` and `h:7000` are two targets
  (`static://h:7000` and `http://h:7000` are the same one as `h:7000`) (`targets.go`, `tls.go`).
- **Question:** should the prefixes, and the TLS an `https://` address implies, be part of the specification for every
  library, or dropped?

### SG-15: a live stream closed with a refusal code

- **Specification:** describes `ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED` and `PERMISSION_DENIED` as
  the hub's answer to a `Register` instead of an ack; it says nothing of a stream closed with one of them after its
  ack (for example access revoked on a live session), apart from the client certificate's expiry, which is an
  ordinary reconnect.
- **Settled:** such a close is a refusal whether or not the stream was acknowledged (kept as the latest rejection, the
  next attempt after 80 to 100 % of the backoff cap), except the hub's close of an acknowledged certificate session
  at the certificate's end (`UNAUTHENTICATED` "certificate expired"), which reconnects from the backoff floor and
  reads the certificate files again. Only the "certificate expired" close of an acknowledged certificate session is
  that exception, not every `UNAUTHENTICATED` one.
- **This library:** does so (`connection.go`).
- **Question:** settled.

### SG-16: a configured reconnect backoff outside the specified range

- **Specification:** [protocol.md, Connect and register](https://snodra.damoreira.com/docs/library-authors/protocol/)
  says that after a refused `Register` a library "waits at least 1 s, and at most its backoff cap of at most 5 s"
  before the next attempt; [Reconnect and backoff](https://snodra.damoreira.com/docs/library-authors/protocol/) caps the
  backoff at 5 s.
- **This library:** refuses only a minimum or maximum of 0 and a minimum above the maximum. A maximum above
  5 s is accepted, and a refused `Register` waits 80 to 100 % of the configured maximum, without a 1 s floor (so a
  maximum of 100 ms retries a refusal every 80 to 100 ms) (`options.go`, `connection.go`). The shipped default
  (500 ms to 5 s) is inside the specified range, and `ReconnectBackoff` documents the range.
- **Question:** settled: the configured values are accepted as they are.

### SG-17: the message of the error codes no handler produces

- **Specification:** [protocol.md, Replies and errors](https://snodra.damoreira.com/docs/library-authors/protocol/) says a
  library must "keep `Error.message` for the codes a handler produces" (`HANDLER_ERROR`, `UNAUTHENTICATED`,
  `PERMISSION_DENIED`, `INVALID_REQUEST`); it says nothing of the message of the other codes.
- **Settled:** the caller reads these texts: "No handler registered for '<type>'" (`NO_HANDLER`),
  "Timed out waiting for reply to '<type>'" (`TIMEOUT`), "Handler for '<type>' disconnected"
  (`HANDLER_DISCONNECTED`), "Handler for '<type>' is busy: its hub queue or session buffer is full"
  (`RESOURCE_EXHAUSTED`; for a subscription the hub's own reason after the colon, a subscriber buffer overflow or an
  instance's subscription limit), the hub's message for `CANCELLED` ("cancelled by the hub" when it gave none), and
  the hub's message for an unknown code (read as `HANDLER_ERROR`). A handler side answers a type it does not serve
  with `NO_HANDLER` "no handler for type <type>". - **This library:** reads these texts (`errors.go`, `dispatch.go`). The reasons of the `Cancel` it writes are
  ("collector cancelled", "subscriber
  cancelled", "no part within the local guard", "no initial result within the local guard", "<stream or
  subscription> <type> lost <n> part(s)", "collector failed: ...", "initial result unusable: ..."), and a streaming
  handler starved of credit ends with `TIMEOUT` "no credit within <ms> ms".
- **Question:** settled.

### SG-18: a forwarded token that fails, for a handler without a guard

- **Specification:** [messages.md, Forwarded identity](https://snodra.damoreira.com/docs/library-authors/messages/) asks a
  library to validate the token "before it runs a handler that requires an authenticated caller"; it says nothing of
  a request that carries a token that fails (unreadable, expired, wrongly signed) to a handler that requires no
  caller, nor of such a request to a type the instance does not serve.
- **This library:** checks the token of every request first, before it looks the handler up: a token that fails
  is answered `UNAUTHENTICATED` whatever the handler (also instead of `NO_HANDLER`); a request without a token is an
  anonymous caller and runs (`dispatch.go`, `subscription_handler.go`, `identity.go`).
- **Question:** settled: a failing token is `UNAUTHENTICATED` before the handler lookup, for every handler and for a
  type not served.

### SG-19: when a silent hub is dropped

- **Specification:** [protocol.md, Heartbeats](https://snodra.damoreira.com/docs/library-authors/protocol/) says a library
  "must drop a stream that brought no echo for three heartbeat intervals"; it does not say when the library looks, nor
  how far past three intervals the drop may come.
- **Settled:** a library looks on each heartbeat tick from the `RegisterAck` on and drops the stream when more than
  three heartbeat intervals have passed since the last echo (or since the ack) and at least three heartbeats sent
  since that echo are unanswered: the drop comes on the tick after the third unanswered heartbeat, between three and
  four intervals after the last echo, never before three full intervals. Before the `RegisterAck` it does not look:
  the 10 s handshake timeout closes a stream that brings neither an ack nor an echo. The heartbeats counted are all
  those written since the last echo: every `Register` is followed by a heartbeat, the later ones too (a
  re-registration, the empty `Register` at shutdown, at a client certificate rollover or when an address leaves the
  DNS answer), and those count with the ticks'.
- **This library:** does so (`connection.go`, `hubSilent`), writing and counting the heartbeat after every later
  `Register`. Before, it wrote no heartbeat after a later `Register`.
- **Question:** settled.

### SG-20: the static token file

- **Specification:** [writing-a-library.md, Credentials](https://snodra.damoreira.com/docs/library-authors/writing-a-library/)
  says a token file "is read again before every connect, and an empty file after a good read is a rotation caught
  half way: keep the last good token"; it says nothing of a file that cannot be read or is empty when the service
  starts, nor of a token that changes in the file while a stream is registered.
- **This library:** refuses to start when the file cannot be read or is empty. On every heartbeat tick of a
  registered stream it reads the file again and, when it holds another non-empty token, sends it once as
  `ClientMessage.credentials` (a rotation without a reconnect); a file that cannot be read or is empty changes
  nothing (`options.go`, `credentials.go`).
- **Question:** settled: the behaviour above is the specification.

### SG-21: retrying a stream

- **Specification:** [protocol.md, Never sent or outcome unknown](https://snodra.damoreira.com/docs/library-authors/protocol/)
  says that when a library retries, "all attempts share the caller's time budget, and each attempt sends what is left
  of it as `Request.timeout_ms`"; it says nothing of a streaming query, whose `Request.timeout_ms` is an idle timeout
  (the longest wait for the next part), nor of when a stream stops being retried.
- **This library:** retries a stream with the policy of a unary query, for the same errors, only until its first
  part, a clean end or an error it does not retry; a retry is a new stream with a fresh id that sends the caller's
  chosen idle timeout again, not what is left of the budget (the budget only bounds when a retry may start). After the
  first part nothing is retried. A subscription is retried the same way until its initial result, but each attempt
  sends what is left of the budget, as a unary request does: there `Request.timeout_ms` bounds the initial result
  only (`stream_client.go`; subscriptions in `subscribe.go`).
- **Question:** settled: the behaviour above is the specification.

### SG-22: a second target answering a hub id already served

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/) says a later
  target answering a `hub_id` another target owns "is closed and parked [...] and is retried only once the owner is
  gone".
- **This library:** follows the specification (`pool.go`, `connection.go`): the
  duplicate's stream is closed and its address parked (no stream, not counted in the permit split, not in the hub ids
  or the status); once no kept connection owns that hub id any more (the owner now answers another id, or its address
  left the DNS answer and is being drained), the parked address is connected again at once, unless its own address
  left the DNS answer.
- **Question:** settled: parked and retried as above, not closed for good (an instance behind aliases would stay one
  hub short after a rolling restart of the hubs).

### SG-23: the permit split during a client certificate rollover with several hubs

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/) counts in N
  the hubs that are live or on their first connect attempt, and says "a hub that joins or returns gets
  `ceil(max / N)` on its new stream".
- **This library:** keeps counting a hub being rolled over to a renewed client certificate: its new `Register` offers
  `ceil(max / N)` (3 of 5 with two hubs and a capacity of 5); the other hubs are topped up once its old stream has
  ended (`pool.go`).
- **Question:** settled: the rolled hub stays counted (not taken out of N while another hub serves, which would offer
  it `ceil(max / (N - 1))`, 5 of 5).

### SG-24: the names of the telemetry

- **Specification:** silent: the library-author pages define no metric, span or instrumentation scope names.
- **This library:** emits the instruments `snodra.hub.client.request.duration` (a histogram in seconds with `kind`,
  `type` and `outcome`), the gauges `snodra.hub.client.handlers.in.flight` and `snodra.hub.client.connected.hubs`, and
  the counters `snodra.hub.client.token.fetches` (with `result`), `snodra.hub.client.certificate.rotations` and
  `snodra.hub.client.credentials.refreshes.sent` (the `observability` module, `observability/observability.go`). The
  instrumentation scope is the Go import path of that module, `github.com/snodra/snodra-messaging-go/observability`
  (by Go convention the scope is the instrumenting package's import path); a handled request gets a server span named
  after its message type, a child of its valid `traceparent`; the three counters are synchronous, exported as monotonic
  sums with every series at 0 from the start; a call ended by the caller's own deadline (`context.DeadlineExceeded`)
  is `timeout`. A call the caller cancels records nothing; nor does a call its options refuse (a reserved header, an
  invalid option) or one answered in-process.
- **Question:** should the specification name the instruments and their labels as part of the library contract, and should
  every library name its scope after its package and create the handler span?

### SG-25: the next hub of an unkeyed request after a hub did not take it

- **Specification:** [protocol.md, Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/) says a request
  that moves goes to the other live hubs "in the same order the request chose its first hub by (... an unkeyed one:
  round-robin order from its hub)"; with three or more hubs it does not say which hub comes next when the shared
  round-robin counter has moved meanwhile, as it does at every hop.
- **This library:** rotates over all live hubs from its next turn of the counter and takes the first one not tried yet
  (`pool.go`, `order`, and `send.go`, `writeOnce`). With hubs A, B and C and the request declined by A, it goes to B
  next unless another call moved the counter meanwhile. The protocol does not fix this order, and a caller must not
  rely on it.
- **Question:** settled: the order is the library's.

### SG-26: the segment an event without a key goes to

- **Specification:** [event-store-add-ons.md, The reference: the in-memory event store](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-reference-the-in-memory-event-store)
  says "an event without a key to the next segment in turn"; it does not say whether the turn belongs to a source or
  to the store, nor where it starts.
- **This library:** keeps one turn for the whole store, starting at segment 0 and wrapping at the store's number of
  segments; events of every source share it (`eventstore_memory.go`). The reference's in-memory store keeps one
  counter for the store too (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/eventstore/memory/MemoryEventStore.kt:107`).
- **Question:** settled: the turn is the store's, shared by its sources.

### SG-27: an acknowledgement after a segment changed hands

- **Specification:** [event-store-add-ons.md, The operations](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-operations)
  says an acknowledgement "acknowledges that delivery and every earlier one of the segment; a later subscription of the
  name resumes after it", and that what a revoked segment delivered and was not acknowledged comes again to its next
  holder; it does not say what an acknowledgement of a segment the subscription no longer holds does.
- **This library:** checks that the segment and the position exist, then drops the acknowledgement: the segment's new
  holder starts from the committed position (`eventstore_memory.go`). The reference does the same
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/eventstore/memory/MemoryEventStore.kt:268`).
- **Question:** settled: the acknowledgement is dropped.

### SG-28: redelivering a position at or after the subscription's next one

- **Specification:** [event-store-add-ons.md, The operations](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-operations)
  says redeliver "hands that delivery and the segment's later ones out again without leaving the subscription"; it does
  not say what a call with a position at or after the subscription's next position does.
- **This library:** only an earlier position moves the next position back; a position at or after it changes nothing, so
  a redelivery never skips an event (`eventstore_memory.go`). The reference guards the move the same way
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/eventstore/memory/MemoryEventStore.kt:279`).
- **Question:** settled: a position at or after the next one changes nothing.

### SG-29: appending an outbox entry whose id is stored already

- **Specification:** [message-store-add-ons.md, The stores](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  lists the outbox's operations, their guarantees and the identifiers the stores hold, but does not say what `append`
  does with an entry whose id is stored already.
- **This library:** refuses it with `ErrConflict`: nothing is appended, and the attempt is retried like a database's
  duplicate key (`store_memory.go`, `Append`). The reference's in-memory store throws a duplicate-key exception
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryStores.kt:108`),
  and the Rust store conflicts too.
- **Question:** is a second append of an id a conflict (and the attempt's retry) or a silent no-op?

### SG-30: ordering the opaque positions of a replay range in a message store

- **Specification:** [message-store-add-ons.md, The stores](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  says a reset deletes a processor's markers "in replay ranges", and the event-store port gives a range as
  `[from, until)` "in the event store's order"; the message store receives the positions as opaque strings and has no
  `EventStore.Compare`, so it does not say how a store orders them.
- **This library:** orders them as the reference's stored layout does
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryStores.kt:38`):
  a canonical decimal position is a number, ordered by value; any other position is text, ordered by its UTF-8 bytes;
  and a number and a text never match, so a range whose kind differs from a marker's deletes nothing. The SQL layout's
  numeric partitions and offsets and the MongoDB layout's numbers agree on the decimal case
  (`store_memory.go`, `memoryPosition`).
- **Question:** settled: the stored layout's order, as the reference's in-memory store implements it.

### SG-31: an abandoned transaction's write reservations

- **Specification:** [message-store-add-ons.md, The stores](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  says transactions begin, commit and abort ("abort: never fails"), and that writes outside one run in a transaction of
  their own; it does not say what a transaction the caller abandons without either does.
- **This library:** a transaction reserves every record it writes, so a second live transaction writing one of them
  fails at once with `ErrConflict`; the reservations are released by `Commit` or `Rollback`. A transaction abandoned
  without either keeps its reservations until the process ends: Go has no destructor to run (`store_memory.go`,
  `memoryTx`). The caller must always defer `Rollback`. The reference's in-memory store reserves the row the same way
  and conflicts the second writer
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryTables.kt:77`),
  releasing it only when the transaction ends
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryMessageStore.kt:143`);
  `TestAnAbandonedTransactionKeepsItsReservation` pins it.
- **Question:** should the specification require `Rollback` on every path and leave an abandoned transaction's
  reservations to the store, or name a maximum lifetime after which the store may release them?

### SG-32: the relay's send timeout and the lease it must leave free

- **Specification:** [message-store-add-ons.md, The stores](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  says a claim holds the entries "under a lease" and that a failed attempt counts, but it does not say how long one
  publish may take, how much of the lease a send must leave free, or how long a lease must be.
- **This library:** one publish gets 40 s; a send without an answer in 40 s counts as a failed send, and a batch stops
  one send timeout before its lease ends, so the lease must be longer than 40 s (`New` refuses a shorter one). The
  references make the same choice with their own values: Kotlin's timeout is 45 s
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/OutboxPublisher.kt:197`)
  and it refuses a lease not longer than that
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/config/SnodraMessagingProperties.kt:334`);
  Rust's is 40 s (`snodra-messaging-rs/src/events/publisher.rs:38`, `OUTBOX_SEND_TIMEOUT`). The values appear in the
  `SNODRA_OUTBOX_LEASE_MS` documentation and in `outbox.go`.
- **Question:** should the specification fix the send timeout and the lease's lower bound, or leave them to each
  library?

### SG-33: the attempts, the wait and the dead letter of a failed record

- **Specification:** [feature-levels.md, Level 2](https://snodra.damoreira.com/docs/library-authors/feature-levels/#level-2-events)
  says events are handled at least once with duplicates skipped, and [message-store-add-ons.md](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  says a record that exhausts its attempts is parked as a dead letter at level 3; neither page says how many handler
  invocations a record gets, how long the wait between them is, or what happens when they all fail.
- **Settled:** the level-2 gate adopted five invocations one second apart, then log and acknowledge at level 2; at
  level 3 the same five attempts end in the configured dead-letter mode: `collection` parks a `HANDLER_FAILED` letter,
  `event-store` publishes it to `<source>.dlt`, and `log` logs and acknowledges it. A payload its handler cannot decode
  is not retried and becomes a `HANDLER_FAILED` letter with error type `InvalidMessage`. No further decision is needed.
- **This library:** five invocations, one second apart (`SNODRA_CONSUMER_MAX_ATTEMPTS`); the exhaustion check is
  `consumer.go:356` and `parkFailed` (`consumer.go:369-394`) switches on `SNODRA_DEAD_LETTER_MODE` (`env.go:63`); the
  undecodable payload path is `consumer.go:351-354`. The references use the same count and wait (Kotlin
  `EventConsumer.kt`: `maxAttempts` 5 from
  `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/config/SnodraMessagingProperties.kt:314`
  and `backOffMs = 1000` at
  `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:98`;
  Rust `consumer_max_attempts: 5` at `snodra-messaging-rs/src/config.rs:259` and a 1 s backoff at
  `snodra-messaging-rs/src/app.rs:1026`).
- **Question:** settled by the level-2 gate: the specification should name the five attempts, the one-second wait and
  the mode-specific outcome above.

### SG-34: a malformed record at level 2 and level 3

- **Specification:** [messages.md](https://snodra.damoreira.com/docs/library-authors/messages/) says an event has a
  type name and its payload is a JSON object, and
  [feature-levels.md, Level 3](https://snodra.damoreira.com/docs/library-authors/feature-levels/#level-3-processors)
  lists `INVALID` as a dead-letter reason (no type, a payload that is not a JSON object); no page says what a level-2
  library does with such a record.
- **Settled:** the level-2 gate adopted log-and-acknowledge without a marker at level 2; at level 3 the record is an
  `INVALID` letter in `collection`, is published to `<source>.dlt` in `event-store`, and is logged and acknowledged in
  `log`. An `INVALID` letter blocks no key and is never retried; it can only be discarded. No further decision is needed.
- **This library:** `parkInvalid` (`consumer.go:398-413`) handles a missing type header (`consumer.go:234-238`) and a
  payload that is not a JSON object (`consumer.go:249-251`); the mode switch is `consumer.go:399-409`. The reference
  marks it `INVALID` and sends it to its sink (the type check at
  `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:359`
  and the payload check at
  `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:371`).
- **Question:** settled by the level-2 gate and the published level-3 dead-letter modes: the specification should name
  the level-2 log-and-skip and the level-3 `INVALID` letter.

### SG-35: the re-subscribe backoff of a failed event subscription

- **Specification:** [event-store-add-ons.md, The operations](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-operations)
  says an error from `next` means the subscription is broken, and the caller "closes it and subscribes again after a
  backoff"; it does not give the wait or its cap.
- **This library:** every failed subscription waits 1 s before the next, then 2, 4, 8, 16 and 30 s, the wait
  staying at 30 s; a subscription that delivered a record or lived 30 s starts it over at 1 s. The references use
  the same values and schedule: Kotlin `MIN` 1 s, `MAX` 30 s and `STABLE` 30 s
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/Resubscribe.kt:74`,
  `:77` and `:80`), Rust `RESUBSCRIBE_MIN` 1 s and `RESUBSCRIBE_MAX` 30 s, the latter also the stable threshold
  (`snodra-messaging-rs/src/events/resubscribe.rs:16` and `:18`).
- **Question:** should the specification fix the re-subscribe backoff, or leave it to each library?

### SG-36: an event published outside any trace has no traceparent

- **Specification:** [messages.md, Event headers](https://snodra.damoreira.com/docs/library-authors/messages/#event-headers)
  says every event carries seven headers and lists `traceparent` as "the W3C trace context of the work that published
  it"; [Trace context](https://snodra.damoreira.com/docs/library-authors/messages/#trace-context) says an invalid one
  is dropped, never passed on, and the message is sent without one. It does not say whether an event published outside
  any trace carries an empty `traceparent` header or none at all.
- **This library:** an event published outside any trace has six headers and no `traceparent`; inside a trace it has
  all seven, with the handled message's valid `traceparent`. The references write one only when the work carries a
  valid one, and nothing outside a trace: Kotlin
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/tracing/TraceHeaders.kt:22`)
  and Rust (`snodra-messaging-rs/src/events/outbox.rs:40`). `TestPublishOutsideAnyHandling` pins it.
- **Question:** should the page say an event outside any trace has six headers, or require an empty `traceparent`?

### SG-37: a source that does not exist when a subscription joins

- **Specification:** [event-store-add-ons.md, The operations](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-operations)
  says subscribe joins the subscription's name on its source as one more instance, and that a new subscription starts at
  the earliest position of every segment; it also says a new subscription's first event is an assignment, "an empty one
  when it holds no segment, within a bound of its joining (10 s in both libraries, `FIRST_ASSIGNMENT_WITHIN`)"; [Kafka
  wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#kafka-wire-rules) says the library
  creates no topic ("the infrastructure does"). No page says what `subscribe` does while the source's topic does not
  exist yet.
- **This library:** `Subscribe` does not ask for the topic's metadata. The consumer's group joins when the topic appears,
  and until then `Next` yields no assignment; a short caller context ends with that context's error, not a broken
  subscription. A topic created later is noticed at the consumer's next metadata refresh (the client caches metadata for
  up to 5 minutes); the test forces a refresh so it does not wait. The core has no first-assignment bound at level 2: its
  read loop just calls `Next`, so a source that never appears leaves the processor waiting with no error, and only the
  kit's joining check reports it. The reference starts a subscription the same way, by subscribing the consumer and
  polling
  (`snodra-messaging@64dde1e:snodra-messaging-kafka/src/main/kotlin/com/damoreira/snodra/messaging/kafka/KafkaSubscription.kt:175`
  and `:401`). `TestSubscribingBeforeTheTopicExistsWaitsForIt` pins it.
- **Question:** should `Subscribe` refuse a source that does not exist, or wait for it as this library does? If it waits,
  the published "first event within 10 s" rule cannot hold for a source that never appears: the rule is the kit's bound
  and the interface's promise, but the level-2 core enforces neither, so the specification should say which of the two is
  the contract.

### SG-38: comparing a position that is not an offset

- **Specification:** [event-store-add-ons.md, Segments and positions](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#segments-and-positions)
  says segments and positions are opaque strings a reader never interprets and that the core orders positions only
  through the event store's `compare`; Kafka's positions are decimal offsets. It does not say what `compare` does with
  a position that is not an offset.
- **This library:** decimal offsets compare by value; any text that is not an offset comes after every offset, by its
  bytes among themselves (`kafka/offsets.go`). Go's `Compare` returns no error, so the order is total and never fails.
  The reference throws *invalid* for a position that is not an offset
  (`snodra-messaging@64dde1e:snodra-messaging-kafka/src/main/kotlin/com/damoreira/snodra/messaging/kafka/KafkaEventStore.kt:113`);
  Rust orders text after every numeric offset.
- **Question:** is a total order the contract, or should a library be able to refuse a malformed position?

### SG-39: a transaction of another message store in a Go context

- **Specification:** [message-store-add-ons.md, The stores](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  says an add-on joins the caller's transaction and never writes beside it; the reference's SQL scope refuses a
  transaction that is not one of its own. The Go interface binds a transaction through a `context.Context`, and a
  context can carry only keys its package knows.
- **This library:** `Store.scope` refuses a context carrying another `*Store`'s transaction with `ErrInvalid`, and
  joins its own `Begin` transaction, a service's `*sql.Tx` (`WithTx`) or a service's `*sql.Conn` (`WithConn`). A
  transaction of a different `snodra.MessageStore` implementation is invisible: each implementation uses an
  unexported context key, so the call runs in a short transaction of its own instead of being refused. The same
  holds for a session: a session bound from another store's client is not detected either. The reference refuses
  one at the scope (`snodra-messaging-rs/crates/snodra-messaging-sqlx/src/db.rs:805`).
- **Question:** should the core define a shared transaction marker so one store can detect another's, or is the
  caller responsible for never mixing a context across stores?

### SG-40: the precision of an outbox entry's created time

- **Specification:** [message-store-add-ons.md, The SQL layout](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-sql-layout)
  keeps times as UTC instants at millisecond precision (`TIMESTAMPTZ(3)` and `DATETIME(3)`); the Go
  `OutboxEntry.CreatedAt` is a `time.Time`, and neither its documentation nor the kit says what happens to its
  sub-millisecond part.
- **Settled:** an outbox entry's times are at millisecond precision, what every store keeps; a store may truncate
  the value to it. The reference kit builds the times it compares truncated to milliseconds
  (`snodra-messaging@64dde1e:snodra-messaging-test/src/main/kotlin/com/damoreira/snodra/messaging/test/store/MessageStoreConformanceTest.kt:66`).
- **This library:** the Go kit's `storeEntryWithPayload` truncates the created time to milliseconds, so the appended
  and the claimed entry compare equal on every store; the SQL add-on truncates every time it binds to the layout's
  milliseconds, so the database never rounds one. `TestTheSQLMessageStorePassesTheKit` pins it on both engines.
- **Question:** settled: millisecond precision is the contract the kit and the layout share.

### SG-41: a transaction or session of another message store in a Go context

- **Specification:** [message-store-add-ons.md, The stores](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-stores)
  says an add-on joins the caller's transaction and never writes beside it; the reference's SQL scope refuses a
  transaction that is not one of its own. The Go interface binds a transaction through a `context.Context`, and a
  context can carry only keys its package knows.
- **This library:** the MongoDB store's `scope` refuses a context carrying another `*Store`'s transaction with
  `ErrInvalid`, and joins its own `Begin` transaction or a service's session (`WithSession`). A transaction of a
  different `snodra.MessageStore` implementation is invisible: each implementation uses an unexported context key,
  so the call runs on its own instead of being refused. (The session half of this limit is SG-39's.) The reference
  refuses a foreign transaction at the scope
  (`snodra-messaging-rs/crates/snodra-messaging-sqlx/src/db.rs:805`).
- **Question:** should the core define a shared transaction marker so one store can detect another's, or is the
  caller responsible for never mixing a context across stores?

### SG-42: a changed retention at schema check time

- **Specification:** [mongodb-layout.md](https://snodra.damoreira.com/docs/library-authors/mongodb-layout/) says every
  instance sharing a database needs the same retentions and that neither add-on changes an index that exists; the
  core's `StoreLifecycle` says `CheckSchema` fails unless the database has what the store needs. No page says whether
  a TTL index created with one retention is a schema failure for a store configured with another.
- **This library:** `CheckSchema` compares an index's keys, TTL, uniqueness and partial filter (regardless of field
  order) with the layout and the store's settings. A database initialized with a different retention fails it with an
  error naming the index and saying every instance needs the same retentions; `InitSchema` leaves the existing index
  as it is (MongoDB refuses an index under an existing name with other options). A retention is stored in whole
  seconds, floored as the reference stores it, so a positive sub-second retention becomes zero. The two reference
  add-ons have no schema check to compare with.
- **Question:** should a retention change be a schema failure (the index must be dropped and recreated), or is the TTL
  a setting each instance may differ on?

### SG-43: the Go stream client cannot carry `snodra-member`

- **Specification:** [RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  says a subscription's consumer carries `snodra-member=<a random id per subscription instance>`, and that the
  official add-ons "use each client's low-level client class: the high-level APIs can neither set `snodra-member` nor
  answer a consumer update with a stored offset".
- **This library:** the official Go client (`rabbitmq-stream-go-client` v1.10.0) exports no low-level client or raw
  subscribe, and its consumer's subscribe properties are built inside `declareSubscriber` from fixed fields
  (`github.com/rabbitmq/rabbitmq-stream-go-client@v1.10.0/pkg/stream/client.go:1005`), so it cannot set
  `snodra-member`. The add-on speaks the protocol's low-level commands itself
  (`rabbitmq/internal/streamwire`) and keeps the client's `pkg/amqp` for AMQP 1.0 message encoding; the add-on's
  tests use its high-level API to create streams and, in the AMQP layout test, to decode one published message.
  `TestSingleActiveConsumerAndSnodraMember` pins the property on
  a real broker, and `TestStoredOffsetIsTheNextOne` pins the consumer-update reply with the stored offset.
- **Question:** should the specification name the low-level capabilities a client must expose (and so allow a client
  replacement), or require the Go client to add custom subscribe properties?

### SG-44: a management-API consumer without `snodra-member`

- **Specification:** [RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  says a subscription's members are "the distinct `snodra-member` values of the consumers of that name on the
  source's partitions". A consumer written with another client (the official Go client cannot set the property) reads
  the same stream without one.
- **This library:** the management-API reader falls back to `<connection name>:<subscription id>` for a consumer
  without `snodra-member`, and counts it as a member, so a reset refuses. The Kotlin add-on does the same
  (`snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitManagement.kt:168`), as
  does the Rust add-on (`snodra-messaging-rs/crates/snodra-messaging-rabbitmq/src/management.rs:197`).
- **Question:** is a consumer without the property a member (a reset must refuse while it reads), or is it invisible
  to the members list?

### SG-45: reading a stream's first offset and head

- **Specification:** [event-store-add-ons.md, Segments and positions](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#segments-and-positions)
  makes positions opaque and [the RabbitMQ wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  say the position is the offset, but no page says how an add-on learns a stream's first offset and head. The stream
  protocol's statistics name chunk ids, not offsets.
- **This library:** reads the first and the last chunk on the partition's leader, as the Kotlin and Rust add-ons do
  (`snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitReads.kt:100`,
  `snodra-messaging-rs/crates/snodra-messaging-rabbitmq/src/read.rs:134`): the first chunk's first offset is the first
  position, the last chunk's first offset plus its records is the head. A stream is empty only when both reads are
  silent within 1.5 s; one silent read beside one that answered is a failure, never a shortened or empty stream.
  `TestReplayOnlyWithTheManagementAPI` pins a plan's `from`/`until` from these reads.
- **Question:** should the specification define how bounds are read (or a minimum bound), or leave it to each add-on?

### SG-46: `browse` on a library without an event browser

- **Specification:** [RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  says the RabbitMQ Streams add-on declares `browse`, `lag` and `native-dead-letter`, and `replay` with the
  management API; the same page says a library without an event browser (the Kotlin library today) may still declare
  `browse` for a store another program can read. The Go core has no event-browser interface.
- **This library:** declares `lag`, `native-dead-letter` and (with the management API) `replay`, never `browse`; its
  `EventStore` has no browser to return. A browser program with its own RabbitMQ add-on can still read the streams.
- **Question:** should the Go and Python add-on pages say `browse` is not declared when the library has no browser, or
  should they declare it for a separate browser program?

### SG-47: when the management API's metrics are off

- **Specification:** [event-store-add-ons.md, Capabilities](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#capabilities)
  says the RabbitMQ add-on declares `replay` only when its management API is configured, because only that API lists
  the instances reading a subscription. No page says what a store does when the API is configured but its metrics
  collection is off (`management.disable_stats` or `management_agent.disable_metrics_collector`): its consumer list
  then answers nothing.
- **This library:** `New` declares `replay` from the management URL without asking it; `Open` asks the overview once
  and drops `replay` when the metrics are off, and fails with `ErrFailed` when the API does not answer (a store that
  would declare `replay` cannot start while that node is down). Every reset asks the consumer list and fails, never
  "nobody reads", on an answer it cannot trust.
- **Question:** should the specification settle whether disabled metrics is a startup failure or a store without
  `replay`, and whether `Open` or `New` is the contract?

### SG-48: a reset is all or nothing over independently stored offsets

- **Specification:** [event-store-add-ons.md, The admin](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-admin)
  says `apply(ranges)` "moves each range's segment's committed position to its `from`, all or nothing". RabbitMQ
  Streams has no command that stores several partitions' offsets together: each is its own asynchronous command on
  its partition's leader, and a failure after some were stored leaves them moved.
- **This library:** validates every range and every head first, refuses while a member reads, stores each from in
  turn, then reads every one back. A store or read-back failure is reported; the offsets already stored are not
  restored. The Kotlin add-on has the same limit
  (`snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitSubscriptionAdmin.kt:54`),
  as does the Rust add-on (`snodra-messaging-rs/crates/snodra-messaging-rabbitmq/src/admin.rs:235`). A range read back
  at another offset is a subscribers-active refusal. `TestReplayOnlyWithTheManagementAPI` pins the successful path and
  the refusal while a subscription reads.
- **Question:** should a reset roll back (best effort) when a later partition's store fails, or should the
  specification say a failed apply may leave some partitions moved and the planner re-runs it?

### SG-49: a level-3 instance whose message store lacks the dead-letter or control port

- **Specification:** [protocol.md, Processor control](https://snodra.damoreira.com/docs/library-authors/protocol/#processor-control)
  says an instance reads its processors' control state at start and every 5 s, and
  [feature-levels.md, Level 3](https://snodra.damoreira.com/docs/library-authors/feature-levels/#level-3-processors)
  says a record that exhausts its attempts is parked (`collection`). The Go message-store interfaces are frozen at
  level 2, so a store may still answer `ErrUnsupported` for `DeadLetters` and `Control`; no page says what an
  instance does then.
- **This library:** starts its processors unpaused and keeps consuming when `Control` is unsupported, and logs and
  acknowledges a record it cannot park when `DeadLetters` is unsupported, as a level-2 instance did, instead of
  stalling the subscription. `pause` and `resume` then answer `CONTROL_REFUSAL_FAILED` naming the missing control
  port, and `dead_letter_list`, `dead_letter_retry`, `dead_letter_retry_all` and `dead_letter_discard` answer
  `CONTROL_REFUSAL_FAILED` naming the missing dead-letter port. A store that supports the ports behaves as the page
  says. The reference always has both ports and parks
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:489`)
  and reads its control documents at start
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/control/ProcessorControl.kt:124`).
- **Question:** should a library refuse to start at level 3 when the store lacks these ports (naming the add-on), or
  degrade to the level-2 behaviour as this one does?

### SG-50: a reset prepare that cannot stop the record in hand

- **Specification:** [protocol.md, Processor control](https://snodra.damoreira.com/docs/library-authors/protocol/#processor-control)
  says `reset_prepare` "takes the processor's lock ... and leaves the subscription"; no page bounds the wait for a
  record a handler is still processing.
- **This library:** waits at most 30 s (the reset stop limit) for the consumer to leave; when it cannot (a handler
  does not return), the prepare is refused `CONTROL_REFUSAL_FAILED` "the subscription did not stop: ..." and the
  lock stays, so a finish or the lease can release it. The reference joins its consumer for 30 s and then answers
  OK even though the reader may still be alive
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/control/ProcessorControl.kt:275`,
  `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:243`).
  An OK while the subscription still has a reader is what the execute's subscribers-active check exists to stop, so
  this library refuses instead.
- **Residual window (execute):** the extension can mark the execute lost between its last lost check and
  `Apply`. The consumer stays out until the execute returns, and `Apply` refuses with `ErrSubscribersActive`
  while any instance reads, so a takeover cannot move the positions under a reader. Closing the instant
  itself needs a cancelable apply the event-store contract does not have.
- **Question:** should a prepare answer OK (the reference's reply) or a refusal (this library) when the record in
  hand keeps the consumer in the subscription past the stop bound?

### SG-51: a reset handler whose processor has no event handler

- **Specification:** the pages say a reset handler runs when a processor resets, and that the hub ignores a
  processor an instance does not report; nothing says what a library does with a reset handler registered for a
  processor that never runs.
- **This library:** `App.Start` fails naming the processor ("the reset handler for processor X needs an event handler
  in that processor"). The reference builds its processors from the handlers that run, so a processor with no event
  handler and no saga never starts and its reset handler is dead without an error
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/LocalEventDispatcher.kt:80`,
  `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventProcessors.kt:51`).
- **Question:** refuse at startup (this library), ignore the handler, or run a processor that exists only to hold
  reset handlers?
