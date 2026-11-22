package main

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// service holds what the handlers answer with.
type service struct {
	app *snodra.App
	env env
	ops *operations // the asynchronous conformance.Send calls
}

// registerHandlers registers every type of the conformance contract the service handles so
// far, as an application would.
func registerHandlers(app *snodra.App, e env) error {
	s := &service{app: app, env: e, ops: newOperations()}
	return errors.Join(
		snodra.HandleCommand(app, func(ctx context.Context, m Echo) (EchoAnswer, error) { return s.echo(ctx, m.RawMessage), nil }),
		snodra.HandleQuery(app, func(ctx context.Context, m EchoQuery) (EchoAnswer, error) { return s.echo(ctx, m.RawMessage), nil }),
		snodra.HandleCommand(app, func(context.Context, Done) (snodra.Empty, error) { return snodra.Empty{}, nil }),
		snodra.HandleCommand(app, func(ctx context.Context, m Sleep) (SleepAnswer, error) { return s.sleep(ctx, m.Ms) }),
		snodra.HandleQuery(app, func(ctx context.Context, m SleepQuery) (SleepAnswer, error) { return s.sleep(ctx, m.Ms) }),
		snodra.HandleCommand(app, s.fail),
		snodra.HandleCommand(app, func(_ context.Context, m Strict) (StrictAnswer, error) { return StrictAnswer(m), nil }),
		// An authenticated caller, and not the subject the contract names as refused.
		snodra.HandleCommand(app, guarded, snodra.RequireCaller(func(c snodra.Caller) bool { return c.Subject != "conformance-refused" })),
		snodra.HandleCommand(app, s.relay),
		snodra.HandleCommand(app, func(ctx context.Context, m Send) (json.RawMessage, error) { return s.send(ctx, m) }),
		snodra.HandleQuery(app, func(ctx context.Context, m Outcome) (json.RawMessage, error) { return s.outcome(ctx, m) }),
		snodra.HandleQuery(app, s.view),
	)
}

func (s *service) echo(ctx context.Context, payload []byte) EchoAnswer {
	a := EchoAnswer{Payload: payload, Headers: map[string]string{}, InstanceID: s.env.instance}
	if info, ok := snodra.MessageFrom(ctx); ok {
		maps.Copy(a.Headers, info.Headers)
		a.RequestID = info.ID
	}
	return a
}

// guarded answers the count and the caller's subject; its guard ran before the payload was
// decoded.
func guarded(ctx context.Context, m Guarded) (GuardedAnswer, error) {
	c, _ := snodra.CallerFrom(ctx)
	return GuardedAnswer{Count: m.Count, User: c.Subject}, nil
}

func (s *service) sleep(ctx context.Context, ms int64) (SleepAnswer, error) {
	start := time.Now()
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return SleepAnswer{}, ctx.Err()
	}
	return SleepAnswer{InstanceID: s.env.instance, SleptMs: time.Since(start).Milliseconds()}, nil
}

// fail fails as an application does: an error, with the details attached.
func (s *service) fail(_ context.Context, m Fail) (snodra.Empty, error) {
	err := errors.New(m.Message)
	if len(m.Details) > 0 {
		err = snodra.WithDetails(err, m.Details)
	}
	return snodra.Empty{}, err
}

// relay sends the payload on, as an application does inside a handler: to
// conformance.runner.Probe or conformance.runner.Missing with default options, failing with the
// library's error as it is.
func (s *service) relay(ctx context.Context, m Relay) (RelayAnswer, error) {
	var (
		reply json.RawMessage
		err   error
	)
	if m.Target == "missing" {
		reply, err = snodra.Send[json.RawMessage](ctx, s.app, Missing{m.Payload})
	} else {
		reply, err = snodra.Send[json.RawMessage](ctx, s.app, Probe{m.Payload})
	}
	if err != nil {
		return RelayAnswer{}, err
	}
	return RelayAnswer{Reply: reply}, nil
}

func (s *service) view(context.Context, View) (ViewAnswer, error) {
	a := ViewAnswer{
		Service:    s.env.service,
		InstanceID: s.env.instance,
		HubIDs:     s.app.HubIDs(),
		Retry:      s.env.retry != nil,
	}
	if a.HubIDs == nil {
		a.HubIDs = []string{}
	}
	if r, ok := s.app.Rejection(); ok {
		last := r.Code + ": " + r.Message
		a.LastRejection = &last
	}
	return a, nil
}
