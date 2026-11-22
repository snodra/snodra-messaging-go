package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"iter"
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
	case "stream":
		out = s.stream(ctx, m)
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
		opts = append(opts, snodra.MaxResults(clampUint32(*m.MaxResults)))
	}
	if m.InitialCredit != nil {
		opts = append(opts, snodra.InitialCredit(clampUint32(*m.InitialCredit)))
	}
	return opts
}

func clampUint32(n int64) uint32 { return uint32(min(max(n, 0), math.MaxUint32)) }

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

// stream streams m's target with the library's Stream and the options m names, reading every
// item (each after readDelayMs), or only read of them before it drops the stream, which cancels
// it, as the reference service does.
func (s *service) stream(ctx context.Context, m Send) map[string]any {
	opts := options(m)
	var items iter.Seq2[json.RawMessage, error]
	switch m.Target {
	case "probe-stream":
		items = snodra.Stream[json.RawMessage](ctx, s.app, ProbeStream{m.Payload}, opts...)
	case "stream":
		var q Stream
		if len(m.Payload) > 0 {
			if err := json.Unmarshal(m.Payload, &q); err != nil {
				return unsupported("payload of conformance.Stream: " + err.Error())
			}
		}
		items = snodra.Stream[json.RawMessage](ctx, s.app, q, opts...)
	default:
		return unsupported("target " + m.Target + " is not a streaming target")
	}
	return readItems(ctx, items, m, nil)
}

// readItems ranges over items after got (a subscription's initial result), waiting readDelayMs
// before taking each, until they end or read items were taken (got counted): then it stops
// ranging, which cancels the stream or subscription. With read 0 and nothing taken yet, a stream
// is opened and dropped at its first item. A context that ends during a wait ends the read.
func readItems(ctx context.Context, items iter.Seq2[json.RawMessage, error], m Send, got []json.RawMessage) map[string]any {
	if got == nil {
		got = []json.RawMessage{}
	}
	enough := func() bool { return m.Read != nil && int64(len(got)) >= *m.Read }
	if len(got) > 0 && enough() {
		return map[string]any{"result": "ok", "items": got, "end": "stopped"}
	}
	delay := time.Duration(0)
	if m.ReadDelayMs != nil {
		delay = time.Duration(*m.ReadDelayMs) * time.Millisecond
	}
	for item, err := range items {
		if err != nil {
			return map[string]any{"result": "error", "error": classify(err), "items": got}
		}
		if enough() {
			break // read 0: opened, then dropped
		}
		if delay > 0 && !pause(ctx, delay) {
			return map[string]any{"result": "error", "error": classify(ctx.Err()), "items": got}
		}
		got = append(got, item)
		if enough() {
			break
		}
	}
	if enough() {
		return map[string]any{"result": "ok", "items": got, "end": "stopped"}
	}
	return map[string]any{"result": "ok", "items": got, "end": "completed"}
}

// pause waits d; false when ctx ended first.
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
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
