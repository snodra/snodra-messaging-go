package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// send runs a conformance.Send: at once, or (async) in the background, answering its operation
// id for conformance.Outcome.
func (s *service) send(ctx context.Context, m Send) (json.RawMessage, error) {
	if !m.Async {
		return json.Marshal(s.run(ctx, m))
	}
	id := s.ops.start(func() map[string]any {
		// The handler's context ends with its answer: the call runs on its own.
		return s.run(context.WithoutCancel(ctx), m)
	})
	return json.Marshal(map[string]string{"opId": id})
}

// outcome answers conformance.Outcome: the operation's outcome, {pending: true} when it is not
// over within waitMs, {unknownOp} for an id it never gave.
func (s *service) outcome(ctx context.Context, m Outcome) (json.RawMessage, error) {
	out, known := s.ops.wait(ctx, m.OpID, time.Duration(m.WaitMs)*time.Millisecond)
	switch {
	case !known:
		return json.Marshal(map[string]string{"unknownOp": m.OpID})
	case out == nil:
		return json.Marshal(map[string]bool{"pending": true})
	}
	return json.Marshal(out)
}

// run makes the call m asks for and reports its outcome in the contract's shape.
func (s *service) run(ctx context.Context, m Send) map[string]any {
	start := time.Now()
	var out map[string]any
	switch m.Op {
	case "unary", "":
		out = s.unary(ctx, m)
	case "scatter":
		out = s.scatter(ctx, m)
	default:
		out = unsupported("op " + m.Op + " is not supported yet")
	}
	out["elapsedMs"] = time.Since(start).Milliseconds()
	return out
}

func unsupported(reason string) map[string]any {
	return map[string]any{"result": "unsupported", "reason": reason}
}

// options are the library's send options m names.
func options(m Send) []snodra.SendOption {
	var opts []snodra.SendOption
	for name, value := range m.Headers {
		opts = append(opts, snodra.Header(name, value))
	}
	if m.RoutingKey != nil {
		opts = append(opts, snodra.RoutingKey(*m.RoutingKey))
	}
	if m.TimeoutMs != nil {
		opts = append(opts, snodra.Timeout(time.Duration(*m.TimeoutMs)*time.Millisecond))
	}
	if m.Retry != nil {
		p := snodra.RetryNeverRan
		p.MaxAttempts = m.Retry.MaxAttempts
		if m.Retry.InitialBackoffMs != nil {
			p.InitialBackoff = time.Duration(*m.Retry.InitialBackoffMs) * time.Millisecond
		}
		opts = append(opts, snodra.Retry(p))
	}
	if m.MaxResults != nil {
		opts = append(opts, snodra.MaxResults(uint32(min(max(*m.MaxResults, 0), math.MaxUint32))))
	}
	return opts
}

// unary sends m's target with the library's Send or Query and the options m names.
func (s *service) unary(ctx context.Context, m Send) map[string]any {
	opts := options(m)
	var (
		reply json.RawMessage
		err   error
	)
	switch m.Target {
	case "probe":
		reply, err = snodra.Send[json.RawMessage](ctx, s.app, Probe{m.Payload}, opts...)
	case "probe-query":
		reply, err = snodra.Query[json.RawMessage](ctx, s.app, ProbeQuery{m.Payload}, opts...)
	case "missing":
		reply, err = snodra.Send[json.RawMessage](ctx, s.app, Missing{m.Payload}, opts...)
	case "echo":
		reply, err = snodra.Send[json.RawMessage](ctx, s.app, Echo{m.Payload}, opts...)
	case "echo-query":
		reply, err = snodra.Query[json.RawMessage](ctx, s.app, EchoQuery{m.Payload}, opts...)
	case "shared-query":
		reply, err = snodra.Query[json.RawMessage](ctx, s.app, SharedQuery{m.Payload}, opts...)
	default:
		return unsupported("target " + m.Target + " is not a unary target")
	}
	if err != nil {
		return map[string]any{"result": "error", "error": classify(err)}
	}
	return map[string]any{"result": "ok", "payload": reply}
}

// scatter gathers m's query target with the library's Gather and the options m names: one
// part per answer, in arrival order, with the counts.
func (s *service) scatter(ctx context.Context, m Send) map[string]any {
	opts := options(m)
	var (
		g   *snodra.Gathered[json.RawMessage]
		err error
	)
	switch m.Target {
	case "probe-query":
		g, err = snodra.Gather[json.RawMessage](ctx, s.app, ProbeQuery{m.Payload}, opts...)
	case "echo-query":
		g, err = snodra.Gather[json.RawMessage](ctx, s.app, EchoQuery{m.Payload}, opts...)
	case "shared-query":
		g, err = snodra.Gather[json.RawMessage](ctx, s.app, SharedQuery{m.Payload}, opts...)
	default:
		return unsupported("target " + m.Target + " is not a query target")
	}
	if err != nil {
		return map[string]any{"result": "error", "error": classify(err)}
	}
	parts := make([]map[string]any, 0, len(g.Parts))
	for _, p := range g.Parts {
		part := map[string]any{"service": p.Service, "instance": p.Instance, "ok": p.Err == nil}
		if p.Err != nil {
			part["error"] = classify(p.Err)
		} else {
			part["payload"] = p.Value
		}
		parts = append(parts, part)
	}
	return map[string]any{"result": "ok", "parts": parts, "failed": g.Failed, "timedOut": g.TimedOut}
}

// classify is the contract's error of a failed call, by the library's typed errors.
func classify(err error) map[string]any {
	out := map[string]any{"kind": "local", "code": nil, "message": err.Error(), "details": nil}
	var e *snodra.Error
	switch {
	case errors.Is(err, snodra.ErrReservedHeader):
		out["kind"] = "refused"
	case errors.Is(err, snodra.ErrNeverSent):
		out["kind"] = "never-sent"
	case errors.Is(err, snodra.ErrOutcomeUnknown):
		out["kind"] = "outcome-unknown"
	case errors.Is(err, snodra.ErrLostParts):
		out["kind"] = "lost-parts"
	case errors.As(err, &e):
		out["kind"], out["code"], out["message"] = "code", e.Code.String(), e.Message
		if len(e.Details) > 0 {
			out["details"] = string(e.Details)
		}
	}
	return out
}

// operations are the asynchronous conformance.Send calls, by id.
type operations struct {
	mu   sync.Mutex
	byID map[string]*operation
}

type operation struct {
	done    chan struct{} // closed when outcome is set
	outcome map[string]any
}

func newOperations() *operations { return &operations{byID: map[string]*operation{}} }

// start runs call in the background and returns its id.
func (o *operations) start(call func() map[string]any) string {
	id := rand.Text()
	op := &operation{done: make(chan struct{})}
	o.mu.Lock()
	o.byID[id] = op
	o.mu.Unlock()
	go func() {
		op.outcome = call()
		close(op.done)
	}()
	return id
}

// wait is the outcome of operation id, waiting at most d (nil while it runs); known is false
// for an id start never gave.
func (o *operations) wait(ctx context.Context, id string, d time.Duration) (out map[string]any, known bool) {
	o.mu.Lock()
	op := o.byID[id]
	o.mu.Unlock()
	if op == nil {
		return nil, false
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-op.done:
		return op.outcome, true
	case <-timer.C:
	case <-ctx.Done():
	}
	select {
	case <-op.done:
		return op.outcome, true
	default:
		return nil, true
	}
}
