package snodra

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// sendConfig is what the [SendOption]s of one call set.
type sendConfig struct {
	routingKey    string
	timeout       time.Duration
	priority      int32
	hasPriority   bool
	retry         *RetryPolicy
	headers       map[string]string // the caller's own, by Header
	maxResults    uint32
	initialCredit uint32
}

// A SendOption configures one [Send], [Query], [Gather], [Stream] or [Subscribe].
type SendOption func(*sendConfig)

// RoutingKey sends the request with key (Request.routing_key): commands of one type with the
// same key go to the same instance of the owning service, typically the aggregate id. Queries
// ignore it at the hub.
func RoutingKey(key string) SendOption { return func(c *sendConfig) { c.routingKey = key } }

// Timeout is this call's timeout, sent as Request.timeout_ms; it wins over [DefaultTimeout]. A
// deadline of the call's context that comes sooner is the timeout instead. Without one the hub
// applies its per-type override or its default request timeout. A negative d fails the call.
func Timeout(d time.Duration) SendOption {
	return func(c *sendConfig) { c.timeout = d }
}

// Priority is this call's Request.priority (higher is served first while the target's queue at
// the hub is not empty); it wins over [Priorities]. The default is 0.
func Priority(p int32) SendOption {
	return func(c *sendConfig) { c.priority, c.hasPriority = p, true }
}

// Retry is this call's retry policy; it wins over [DefaultRetry]. An invalid policy fails the
// call before anything is sent.
func Retry(p RetryPolicy) SendOption { return func(c *sendConfig) { c.retry = &p } }

// NoRetry sends this call once, whatever [DefaultRetry] says.
func NoRetry() SendOption { return Retry(noRetry) }

// MaxResults ends a [Gather] after n successful parts, a [Stream] after n items, or a
// [Subscribe] after n updates, the initial result not counted (Request.max_results): the hub
// sends the end then and cancels the targets or the handler still working. 0, the default, is
// no limit. [Send] and [Query] ignore it.
func MaxResults(n uint32) SendOption { return func(c *sendConfig) { c.maxResults = n } }

// Send sends the command cmd through a hub to the instance that handles its type and returns the
// handler's answer decoded as R ([Empty] for a command without a result).
//
// A failed call is one of:
//   - an [*Error] (test with errors.As): the hub's or the handler's answer, by its [Code], or the
//     library's own TIMEOUT when no answer came within the hub's bound for the request plus 1 s
//     (a request still waiting in the library's buffer then is withdrawn and reported as TIMEOUT);
//   - [ErrNeverSent] (test with errors.Is): no hub had an acknowledged stream, or the request was
//     still in the library's buffer when the stream broke; the handler never saw it;
//   - [ErrOutcomeUnknown]: the request was written, then its stream broke before the answer; the
//     handler may or may not have run;
//   - the context's error, when ctx ended first: with [ErrOutcomeUnknown] too once the request was
//     written, else with [ErrNeverSent];
//   - [ErrReservedHeader]: a [Header] the library sets itself; nothing was sent;
//   - another error for a local failure (the message cannot be encoded, the answer not decoded,
//     an invalid option).
//
// A deadline of ctx is the caller's timeout (see [Timeout]). Sent while handling a request (with
// the handler's ctx, or one derived from it), the request carries that request's correlation id,
// its authorization header (see [ForwardJWT]) and its valid traceparent; outside any handling it
// carries a new correlation id. See
// https://snodra.damoreira.com/library-authors/protocol/ (Requests).
//
// A command of a type this instance handles itself is answered in-process, unless
// [AlwaysThroughHub] is on: its handler runs on the caller's goroutine under ctx, with no hub, no
// permit and no handler slot ([MaxConcurrent] does not count it), also before [App.Start] and
// after [App.Shutdown]. The handler sees a request with a fresh id, the call's [Header]s and the
// correlation id (that of the request ctx is handling, else a new one); it acts for the caller of
// the request ctx is handling, anonymous outside a handler, whose token and trace go on with the
// requests it sends; its guards apply. [RoutingKey], [Timeout], [Priority] and retries do not apply,
// and the telemetry records nothing. Its error is the [*Error] the hub would have relayed (a panic
// is HANDLER_ERROR "handler panicked"); when ctx ended and the handler failed, ctx's error.
func Send[R any](ctx context.Context, app *App, cmd Message, opts ...SendOption) (R, error) {
	return request[R](ctx, app, hubpb.RequestKind_COMMAND, cmd, opts)
}

// Query sends the query q through a hub to an instance that handles its type and returns the
// answer decoded as R. It fails as [Send] does; a query is also retried after an unknown outcome.
// A query of a type this instance handles itself, not [Shared], is answered in-process as [Send]
// says; a shared one goes through a hub, which spreads it over every owner, this instance
// included.
func Query[R any](ctx context.Context, app *App, q Message, opts ...SendOption) (R, error) {
	return request[R](ctx, app, hubpb.RequestKind_QUERY, q, opts)
}

// request is a call: answered in-process when this instance handles it (see local), else its
// settings, the attempts under the retry policy, the answer decoded. One record goes to the
// telemetry per call through the hubs, retries included, unless the caller's own cancellation
// ended it or the call panicked.
func request[R any](ctx context.Context, app *App, kind hubpb.RequestKind, m Message, opts []SendOption) (R, error) {
	if h, typ := app.localHandler(kind, m); h != nil {
		return local[R](ctx, app, h, kind, typ, m, opts)
	}
	var tel *telemetry // nil for a nil app: prepare fails
	if app != nil {
		tel = app.tel
	}
	started := tel.now()
	r, typ, err := doRequest[R](ctx, app, kind, m, opts)
	if typ != "" {
		tel.requestDone(kindLabel(kind == hubpb.RequestKind_COMMAND), typ, started, err)
	}
	return r, err
}

// doRequest is request without the telemetry; typ is the message type once it is known.
func doRequest[R any](ctx context.Context, app *App, kind hubpb.RequestKind, m Message, opts []SendOption) (_ R, typ string, _ error) {
	var zero R
	out, err := prepare(ctx, app, kind, hubpb.RequestMode_UNARY, m, opts)
	if out != nil {
		typ = out.typ
	}
	if err != nil {
		return zero, typ, err
	}
	answer, err := retrying(ctx, kind, out.policy, out.budget, out.chosen, func(timeout time.Duration) ([]byte, error) {
		ans, err := app.attempt(ctx, out.tmpl, timeout)
		if err != nil {
			return nil, err
		}
		return answered(out.typ, ans)
	})
	if err != nil {
		return zero, typ, err
	}
	r, err := decodeAnswer[R](app.cfg.codec, answer)
	if err != nil {
		return zero, typ, &undecodableReply{typ: out.typ, err: err}
	}
	return r, typ, nil
}

// decodeAnswer decodes an answer's payload as R: an empty payload reads as {}, and [Empty] is
// not decoded at all.
func decodeAnswer[R any](codec PayloadCodec, payload []byte) (R, error) {
	var r R
	if _, ok := any(r).(Empty); ok {
		return r, nil
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if err := codec.Unmarshal(payload, &r); err != nil {
		var zero R
		return zero, err
	}
	return r, nil
}

// outgoing is a call ready to be sent: the request every attempt copies (with its own id and
// timeout) and what the attempts share.
type outgoing struct {
	typ    string
	tmpl   *hubpb.Request
	policy RetryPolicy
	chosen time.Duration // the caller's timeout; 0: none
	budget time.Duration // the time all attempts share, when the policy retries
}

// prepare checks a call of m in mode with opts and builds what its attempts send. Nothing is
// sent when it fails; the outgoing it returns then holds the message type once that is known
// (for the telemetry), nothing else.
func prepare(ctx context.Context, app *App, kind hubpb.RequestKind, mode hubpb.RequestMode, m Message, opts []SendOption) (*outgoing, error) {
	if app == nil {
		return nil, errors.New("snodra: send on a nil App")
	}
	typ, err := messageName(m)
	if err != nil {
		return nil, err
	}
	out, err := prepareTyped(ctx, app, typ, kind, mode, m, opts)
	if err != nil {
		return &outgoing{typ: typ}, err
	}
	return out, nil
}

// prepareTyped is prepare for a message named typ.
func prepareTyped(ctx context.Context, app *App, typ string, kind hubpb.RequestKind, mode hubpb.RequestMode, m Message, opts []SendOption) (*outgoing, error) {
	cfg, policy, err := sendSettings(app, typ, opts)
	if err != nil {
		return nil, err
	}
	payload, err := app.cfg.codec.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("snodra: cannot encode %s: %w", typ, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNeverSent, err)
	}
	chosen := cfg.timeout
	if chosen == 0 {
		chosen = app.cfg.defaultTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		left := time.Until(deadline)
		if left <= 0 {
			return nil, fmt.Errorf("%w: %w", ErrNeverSent, context.DeadlineExceeded)
		}
		if chosen == 0 || left < chosen {
			chosen = left
		}
	}
	priority := app.cfg.priorities[typ]
	if cfg.hasPriority {
		priority = cfg.priority
	}
	headers := outgoingHeaders(ctx, cfg.headers, app.cfg.forwardJWT)
	app.tel.inject(ctx, headers)
	tmpl := &hubpb.Request{
		Kind:       kind,
		Type:       typ,
		RoutingKey: cfg.routingKey,
		Payload:    payload,
		Priority:   priority,
		Mode:       mode,
		Headers:    headers,
	}
	switch mode {
	case hubpb.RequestMode_SCATTER_GATHER:
		// Every target is asked: a gather goes to a hub unkeyed and carries no routing key, as in
		// the reference library.
		tmpl.RoutingKey = ""
		tmpl.MaxResults = cfg.maxResults
	case hubpb.RequestMode_STREAMING, hubpb.RequestMode_SUBSCRIPTION:
		// A stream or a subscription goes to a hub unkeyed and carries no routing key, as in the
		// reference library.
		tmpl.RoutingKey = ""
		tmpl.MaxResults = cfg.maxResults
		tmpl.InitialCredit = cfg.initialCredit
		if tmpl.InitialCredit == 0 {
			tmpl.InitialCredit = defaultCredit
		}
	}
	out := &outgoing{typ: typ, tmpl: tmpl, policy: policy, chosen: chosen}
	if policy.MaxAttempts > 1 {
		out.budget = retryBudget(app.ackFor(tmpl.GetRoutingKey()), typ, chosen)
	}
	return out, nil
}

// sendSettings are what opts set for a call of typ, and its retry policy, or why the call is
// refused before anything is sent (a reserved header, a negative timeout, an invalid policy).
func sendSettings(app *App, typ string, opts []SendOption) (sendConfig, RetryPolicy, error) {
	var cfg sendConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if err := checkHeaders(cfg.headers, typ); err != nil {
		return cfg, noRetry, &localRefusal{err}
	}
	if cfg.timeout < 0 {
		return cfg, noRetry, &localRefusal{fmt.Errorf("snodra: the Timeout of %s must not be negative, got %v", typ, cfg.timeout)}
	}
	policy := noRetry
	switch {
	case cfg.retry != nil:
		policy = *cfg.retry
	case app.cfg.retry != nil:
		policy = *app.cfg.retry
	}
	if err := policy.validate(); err != nil {
		return cfg, noRetry, &localRefusal{fmt.Errorf("%w (sending %s)", err, typ)}
	}
	return cfg, policy, nil
}

// messageName is m's wire name, or why m cannot be sent.
func messageName(m Message) (name string, err error) {
	if m == nil {
		return "", errors.New("snodra: send of a nil message")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("snodra: %T.MessageType panicked: %v", m, r)
		}
	}()
	if name = m.MessageType(); name == "" {
		return "", fmt.Errorf("snodra: %T has an empty type name (MessageType returned \"\")", m)
	}
	return name, nil
}

// requestTiming is the Request.timeout_ms and the local guard of a request of type typ to the hub
// that sent ack, when the caller chose chosen (0: none). It sends the chosen timeout (at least
// 1 ms) or 0, so that the hub applies its per-type override or its default; the guard is the
// hub's bound for the request plus 1 s: min(chosen, override or max) or, when 0 is sent, the
// override or max. See https://snodra.damoreira.com/library-authors/protocol/ (Timeouts).
func requestTiming(ack *hubpb.RegisterAck, typ string, chosen time.Duration) (timeoutMS uint32, guard time.Duration) {
	if chosen > 0 {
		timeoutMS = uint32(min(max(chosen, minTimeout).Milliseconds(), math.MaxUint32))
	}
	limit := fallbackMaxTimeout
	if n := ack.GetMaxTimeoutMs(); n > 0 {
		limit = time.Duration(n) * time.Millisecond
	}
	if n := ack.GetTypeTimeoutsMs()[typ]; n > 0 { // a zero override means none
		limit = time.Duration(n) * time.Millisecond
	}
	bound := limit
	if timeoutMS > 0 {
		bound = min(time.Duration(timeoutMS)*time.Millisecond, limit)
	}
	return timeoutMS, bound + guardMargin
}

// answer is what came for a request: a payload, or the hub's or the handler's error; for a
// scatter-gather, its parts in arrival order and its end.
type answer struct {
	payload []byte
	failure *hubpb.Error
	parts   []*hubpb.ReplyPart
	end     *hubpb.ReplyEnd
}

// call is one attempt in flight: one Request with its own id.
type call struct {
	id     string
	answer chan answer // buffered: the one answer
	// gather is a scatter-gather: answered by parts and an end, never by a Reply.
	gather bool
	// inbox is a streaming query's: its parts and its end (or failure) in arrival order, as they
	// come; answer is not used. Nil for any other request.
	inbox *streamInbox
	// parts collects a gather's parts until its end (under pendingCalls.mu); the local guard
	// bounds how long.
	parts []*hubpb.ReplyPart
	// written is set (under the stream's mu) when the stream's writer took the request.
	written bool
	// on is the stream the request was written to; nil until it was.
	on atomic.Pointer[streamRef]
}

// pendingCalls are the attempts awaiting their answer, by request id.
type pendingCalls struct {
	mu   sync.Mutex
	byID map[string]*call
}

func newPendingCalls() *pendingCalls { return &pendingCalls{byID: map[string]*call{}} }

func (p *pendingCalls) add(c *call) {
	p.mu.Lock()
	p.byID[c.id] = c
	p.mu.Unlock()
}

func (p *pendingCalls) remove(id string) {
	p.mu.Lock()
	delete(p.byID, id)
	p.mu.Unlock()
}

// writtenTo counts the calls written to ref that await their answer. Without consumers it
// leaves out the streams and subscriptions this instance reads: they end when their owners
// decide, so a certificate rollover does not wait for them (it cuts them), as in the reference
// library; the drain of an address that left the DNS answer waits for them.
func (p *pendingCalls) writtenTo(ref *streamRef, consumers bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.byID {
		if c.on.Load() == ref && (consumers || c.inbox == nil) {
			n++
		}
	}
	return n
}

// deliver hands a the call with id; false when nothing waits for it (answered, given up).
func (p *pendingCalls) deliver(id string, a answer) bool {
	p.mu.Lock()
	c := p.byID[id]
	delete(p.byID, id)
	p.mu.Unlock()
	if c == nil {
		return false
	}
	c.finish(a)
	return true
}

// finish hands a, the end of the call, to its caller.
func (c *call) finish(a answer) {
	if c.inbox != nil {
		c.inbox.push(streamEvent{failure: a.failure, end: a.end})
		return
	}
	c.answer <- a
}

// reply hands a Reply's a to the unary call with id; a gather or a stream is never answered by
// a Reply, so one for it is ignored.
func (p *pendingCalls) reply(id string, a answer) {
	p.mu.Lock()
	c := p.byID[id]
	if c == nil || c.gather || c.inbox != nil {
		p.mu.Unlock()
		return
	}
	delete(p.byID, id)
	p.mu.Unlock()
	c.answer <- a
}

// part adds a ReplyPart to the gather it belongs to, or passes it on to the stream; ignored for
// an unknown or unary request.
func (p *pendingCalls) part(rp *hubpb.ReplyPart) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch c := p.byID[rp.GetRequestId()]; {
	case c == nil:
	case c.gather:
		c.parts = append(c.parts, rp)
	case c.inbox != nil:
		c.inbox.push(streamEvent{part: rp})
	}
}

// end completes the gather a ReplyEnd belongs to with the parts it collected, or the stream;
// ignored for an unknown or unary request.
func (p *pendingCalls) end(e *hubpb.ReplyEnd) {
	p.mu.Lock()
	c := p.byID[e.GetRequestId()]
	if c == nil || (!c.gather && c.inbox == nil) {
		p.mu.Unlock()
		return
	}
	delete(p.byID, e.GetRequestId())
	parts := c.parts
	c.parts = nil
	p.mu.Unlock()
	c.finish(answer{parts: parts, end: e})
}

// attempt sends tmpl once, with a fresh id and timeout chosen, and waits for its answer (a
// gather's: its parts and end), the local guard, the end of its stream or the end of ctx. The
// answer may carry the hub's or the handler's error; the error is the attempt's own failure.
//
// Within the attempt the request keeps its id while it goes from hub to hub
// (https://snodra.damoreira.com/library-authors/protocol/, Several hubs): when the stream it
// was queued on is lost before the request was written, it goes to the next hub, at most
// maxUnwrittenMoves times; and a unary request a hub answers with NO_HANDLER (never delivered)
// goes to each other live hub in turn, at most once each, within what is left of chosen; when
// every hub declined, the last answer is the attempt's.
func (a *App) attempt(ctx context.Context, tmpl *hubpb.Request, chosen time.Duration) (answer, error) {
	typ := tmpl.GetType()
	id := uuid.New()
	defer a.calls.remove(id)
	unary := tmpl.GetMode() == hubpb.RequestMode_UNARY
	started := time.Now()
	moves := 0
	var declined []*hubConn // the hubs that answered NO_HANDLER
	var noHandler *answer   // the last of those answers
	for {
		timeout := chosen
		if noHandler != nil && chosen > 0 {
			left := chosen - time.Since(started)
			if left <= 0 {
				return *noHandler, nil
			}
			timeout = max(left, minTimeout)
		}
		c := &call{id: id, answer: make(chan answer, 1), gather: tmpl.GetMode() == hubpb.RequestMode_SCATTER_GATHER}
		ref, guard, conn, err := a.write(ctx, tmpl, timeout, c, declined)
		if err != nil {
			if noHandler != nil && ctx.Err() == nil {
				return *noHandler, nil // no other hub left to try
			}
			return answer{}, err
		}
		ans, err := a.await(ctx, typ, c, ref, guard)
		switch {
		case errors.Is(err, errNotWritten):
			// TODO(spec-gap): SG-10 (an unwritten request moves to another hub at most 8 times).
			moves++
			if moves > maxUnwrittenMoves {
				return answer{}, fmt.Errorf("%w: '%s' not written: %d hub streams lost before it was", ErrNeverSent, typ, moves)
			}
			a.log.Debug("the hub stream was lost before the request was written; trying the next hub", "type", typ, "request", id)
		case err != nil:
			return answer{}, err
		case unary && ans.failure.GetCode() == hubpb.ErrorCode_NO_HANDLER:
			a.log.Debug("no handler on this hub; trying the others", "type", typ, "request", id, "hub", conn.address)
			declined = append(declined, conn)
			noHandler = &ans
		default:
			return ans, nil
		}
	}
}

// errNotWritten is the end of a request whose stream was lost before the request was written:
// it may go to another hub with the same id.
var errNotWritten = errors.New("snodra: the hub stream was lost before the request was written")

// await waits for the answer of c, written (or being written) to ref: the answer, the local
// guard, the end of ref (errNotWritten when the request was never written) or the end of ctx.
func (a *App) await(ctx context.Context, typ string, c *call, ref *streamRef, guard time.Duration) (answer, error) {
	timer := time.NewTimer(guard)
	defer timer.Stop()
	select {
	case ans := <-c.answer:
		return ans, nil
	case <-ref.gone:
		select {
		case ans := <-c.answer: // answered just before the stream ended
			return ans, nil
		default:
		}
		if ref.withdraw(c) {
			return answer{}, errNotWritten
		}
		return answer{}, fmt.Errorf("%w: the hub stream closed before the reply to %s", ErrOutcomeUnknown, typ)
	case <-timer.C:
		// The hub answers TIMEOUT itself; the guard covers a hub that never answers.
		ref.withdraw(c)
		return answer{}, &Error{Code: CodeTimeout, Message: timeoutText(typ)}
	case <-ctx.Done():
		if ref.withdraw(c) {
			return answer{}, fmt.Errorf("%w: %w", ErrNeverSent, ctx.Err())
		}
		return answer{}, fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
	}
}

// write queues tmpl as the request of c (with c's id and the timeout chosen) for the first hub
// in the routing order of its key that takes it, skipping the hubs in skip, c recorded as
// pending, and returns that stream, the request's local guard (the hub's bound plus 1 s) and
// the hub. A hub whose stream is gone before the request is queued is passed over for the next
// one (nothing was written). When it fails, nothing was sent and c is not pending.
func (a *App) write(ctx context.Context, tmpl *hubpb.Request, chosen time.Duration, c *call, skip []*hubConn) (*streamRef, time.Duration, *hubConn, error) {
	typ := tmpl.GetType()
	a.mu.Lock()
	p := a.pool
	a.mu.Unlock()
	if p == nil {
		return nil, 0, nil, fmt.Errorf("%w: the app is not started (sending %s)", ErrNeverSent, typ)
	}
	var until time.Time // the end of a wait for a hub out of the routing for a rollover
	for {
		ref, guard, conn, err := a.writeOnce(ctx, p, tmpl, chosen, c, skip)
		if err == nil || ctx.Err() != nil || len(skip) > 0 || !p.rolling() {
			return ref, guard, conn, err
		}
		// No hub took it only because a client certificate rollover has the hub(s) out of the
		// routing (a single hub, reconnecting with the renewed pair): wait for one, within the
		// caller's timeout and at most rollReconnectLimit (when the rollover gives up anyway), as
		// the reference library does.
		if until.IsZero() {
			limit := a.cfg.timings.rollReconnectLimit
			if chosen > 0 {
				limit = min(limit, chosen)
			}
			until = time.Now().Add(limit)
		}
		left := time.Until(until)
		if left <= 0 {
			return ref, guard, conn, err
		}
		timer := time.NewTimer(min(left, rollPoll))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, 0, nil, fmt.Errorf("%w: %w", ErrNeverSent, ctx.Err())
		case <-a.stopped:
			timer.Stop()
			return ref, guard, conn, err
		}
	}
}

// writeOnce is one try of write over the hubs routable now.
func (a *App) writeOnce(ctx context.Context, p *pool, tmpl *hubpb.Request, chosen time.Duration, c *call, skip []*hubConn) (*streamRef, time.Duration, *hubConn, error) {
	typ := tmpl.GetType()
	ranked := p.ranked(tmpl.GetRoutingKey())
	if len(ranked) == 0 {
		return nil, 0, nil, fmt.Errorf("%w: no hub has acknowledged the registration (sending %s)", ErrNeverSent, typ)
	}
	tried := false
	for _, conn := range ranked {
		if slices.Contains(skip, conn) {
			continue
		}
		ref, ack := conn.live()
		if ref == nil {
			continue
		}
		tried = true
		timeoutMS, guard := requestTiming(ack, typ, chosen)
		req := &hubpb.Request{
			Id:            c.id,
			Kind:          tmpl.GetKind(),
			Type:          typ,
			RoutingKey:    tmpl.GetRoutingKey(),
			Payload:       tmpl.GetPayload(),
			Headers:       tmpl.GetHeaders(),
			TimeoutMs:     timeoutMS,
			Priority:      tmpl.GetPriority(),
			Mode:          tmpl.GetMode(),
			MaxResults:    tmpl.GetMaxResults(),
			InitialCredit: tmpl.GetInitialCredit(),
		}
		a.calls.add(c)
		if err := ref.sendRequest(ctx, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Request{Request: req}}, c); err != nil {
			a.calls.remove(c.id)
			if cerr := ctx.Err(); cerr != nil {
				return nil, 0, nil, fmt.Errorf("%w: %w", ErrNeverSent, cerr)
			}
			a.log.Debug("not written, trying the next hub", "type", typ, "request", c.id, "hub", conn.address)
			continue
		}
		return ref, guard, conn, nil
	}
	if !tried && len(skip) > 0 {
		return nil, 0, nil, fmt.Errorf("%w: no other hub to send %s to", ErrNeverSent, typ)
	}
	return nil, 0, nil, fmt.Errorf("%w: the hub stream closed before %s was written", ErrNeverSent, typ)
}

// answered is the payload of ans, or its error as the caller sees it.
func answered(typ string, ans answer) ([]byte, error) {
	if ans.failure != nil {
		return nil, errorFromWire(typ, ans.failure)
	}
	return ans.payload, nil
}

// ackFor is the RegisterAck of the hub a request with key would go to now; nil when none.
func (a *App) ackFor(key string) *hubpb.RegisterAck {
	a.mu.Lock()
	p := a.pool
	a.mu.Unlock()
	if p == nil {
		return nil
	}
	return p.ackFor(key)
}
