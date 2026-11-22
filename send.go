package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// sendConfig is what the [SendOption]s of one call set.
type sendConfig struct {
	routingKey  string
	timeout     time.Duration
	priority    int32
	hasPriority bool
	retry       *RetryPolicy
	headers     map[string]string // the caller's own, by Header
}

// A SendOption configures one [Send] or [Query].
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
func Send[R any](ctx context.Context, app *App, cmd Message, opts ...SendOption) (R, error) {
	return request[R](ctx, app, hubpb.RequestKind_COMMAND, cmd, opts)
}

// Query sends the query q through a hub to an instance that handles its type and returns the
// answer decoded as R. It fails as [Send] does; a query is also retried after an unknown outcome.
func Query[R any](ctx context.Context, app *App, q Message, opts ...SendOption) (R, error) {
	return request[R](ctx, app, hubpb.RequestKind_QUERY, q, opts)
}

// request is a call: its settings, the attempts under the retry policy, the answer decoded.
func request[R any](ctx context.Context, app *App, kind hubpb.RequestKind, m Message, opts []SendOption) (R, error) {
	var zero R
	if app == nil {
		return zero, errors.New("snodra: send on a nil App")
	}
	typ, err := messageName(m)
	if err != nil {
		return zero, err
	}
	var cfg sendConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if err := checkHeaders(cfg.headers, typ); err != nil {
		return zero, err
	}
	if cfg.timeout < 0 {
		return zero, fmt.Errorf("snodra: the Timeout of %s must not be negative, got %v", typ, cfg.timeout)
	}
	policy := noRetry
	switch {
	case cfg.retry != nil:
		policy = *cfg.retry
	case app.cfg.retry != nil:
		policy = *app.cfg.retry
	}
	if err := policy.validate(); err != nil {
		return zero, fmt.Errorf("%w (sending %s)", err, typ)
	}
	payload, err := app.cfg.codec.Marshal(m)
	if err != nil {
		return zero, fmt.Errorf("snodra: cannot encode %s: %w", typ, err)
	}
	if err := ctx.Err(); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrNeverSent, err)
	}
	chosen := cfg.timeout
	if chosen == 0 {
		chosen = app.cfg.defaultTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		left := time.Until(deadline)
		if left <= 0 {
			return zero, fmt.Errorf("%w: %w", ErrNeverSent, context.DeadlineExceeded)
		}
		if chosen == 0 || left < chosen {
			chosen = left
		}
	}
	priority := app.cfg.priorities[typ]
	if cfg.hasPriority {
		priority = cfg.priority
	}
	tmpl := &hubpb.Request{
		Kind:       kind,
		Type:       typ,
		RoutingKey: cfg.routingKey,
		Payload:    payload,
		Priority:   priority,
		Mode:       hubpb.RequestMode_UNARY,
		Headers:    outgoingHeaders(ctx, cfg.headers, app.cfg.forwardJWT),
	}
	var budget time.Duration
	if policy.MaxAttempts > 1 {
		budget = retryBudget(app.ackFor(cfg.routingKey), typ, chosen)
	}
	answer, err := retrying(ctx, kind, policy, budget, chosen, func(timeout time.Duration) ([]byte, error) {
		return app.attempt(ctx, tmpl, timeout)
	})
	if err != nil {
		return zero, err
	}
	if _, ok := any(zero).(Empty); ok {
		return zero, nil
	}
	if len(answer) == 0 {
		answer = []byte("{}")
	}
	var r R
	if err := app.cfg.codec.Unmarshal(answer, &r); err != nil {
		return zero, fmt.Errorf("snodra: cannot decode the answer of %s: %w", typ, err)
	}
	return r, nil
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

// answer is what came for a request: a payload, or the hub's or the handler's error.
type answer struct {
	payload []byte
	failure *hubpb.Error
}

// call is one attempt in flight: one Request with its own id.
type call struct {
	id     string
	answer chan answer // buffered: the one answer
	// written is set (under the stream's mu) when the stream's writer took the request.
	written bool
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

// deliver hands a the call with id; false when nothing waits for it (answered, given up).
func (p *pendingCalls) deliver(id string, a answer) bool {
	p.mu.Lock()
	c := p.byID[id]
	delete(p.byID, id)
	p.mu.Unlock()
	if c == nil {
		return false
	}
	c.answer <- a
	return true
}

// attempt sends tmpl once, with a fresh id and timeout chosen, to an acknowledged hub and waits
// for its answer, the local guard, the end of its stream or the end of ctx.
func (a *App) attempt(ctx context.Context, tmpl *hubpb.Request, chosen time.Duration) ([]byte, error) {
	typ := tmpl.GetType()
	a.mu.Lock()
	p := a.pool
	a.mu.Unlock()
	if p == nil {
		return nil, fmt.Errorf("%w: the app is not started (sending %s)", ErrNeverSent, typ)
	}
	conn, err := p.pick(tmpl.GetRoutingKey())
	if err != nil {
		return nil, fmt.Errorf("%w (sending %s)", err, typ)
	}
	ref, ack := conn.live()
	if ref == nil {
		return nil, fmt.Errorf("%w: the hub %s has no acknowledged stream (sending %s)", ErrNeverSent, conn.address, typ)
	}
	timeoutMS, guard := requestTiming(ack, typ, chosen)
	c := &call{id: uuid.New(), answer: make(chan answer, 1)}
	req := &hubpb.Request{
		Id:         c.id,
		Kind:       tmpl.GetKind(),
		Type:       typ,
		RoutingKey: tmpl.GetRoutingKey(),
		Payload:    tmpl.GetPayload(),
		Headers:    tmpl.GetHeaders(),
		TimeoutMs:  timeoutMS,
		Priority:   tmpl.GetPriority(),
		Mode:       tmpl.GetMode(),
	}
	a.calls.add(c)
	defer a.calls.remove(c.id)
	if err := ref.sendRequest(ctx, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Request{Request: req}}, c); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, fmt.Errorf("%w: %w", ErrNeverSent, cerr)
		}
		return nil, fmt.Errorf("%w: the hub stream closed before %s was written", ErrNeverSent, typ)
	}
	timer := time.NewTimer(guard)
	defer timer.Stop()
	select {
	case ans := <-c.answer:
		return answered(typ, ans)
	case <-ref.gone:
		select {
		case ans := <-c.answer: // answered just before the stream ended
			return answered(typ, ans)
		default:
		}
		if ref.withdraw(c) {
			return nil, fmt.Errorf("%w: the hub stream closed before %s was written", ErrNeverSent, typ)
		}
		return nil, fmt.Errorf("%w: the hub stream closed before the reply to %s", ErrOutcomeUnknown, typ)
	case <-timer.C:
		// The hub answers TIMEOUT itself; the guard covers a hub that never answers.
		ref.withdraw(c)
		return nil, &Error{Code: CodeTimeout, Message: timeoutText(typ)}
	case <-ctx.Done():
		if ref.withdraw(c) {
			return nil, fmt.Errorf("%w: %w", ErrNeverSent, ctx.Err())
		}
		return nil, fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
	}
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
