package main

import (
	"context"
	"errors"
	"maps"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// service holds what the handlers answer with.
type service struct {
	app *snodra.App
	env env
}

// registerHandlers registers every type of the conformance contract the service handles so
// far, as an application would.
func registerHandlers(app *snodra.App, e env) error {
	s := &service{app: app, env: e}
	return errors.Join(
		snodra.HandleCommand(app, func(ctx context.Context, m Echo) (EchoAnswer, error) { return s.echo(ctx, m.RawMessage), nil }),
		snodra.HandleQuery(app, func(ctx context.Context, m EchoQuery) (EchoAnswer, error) { return s.echo(ctx, m.RawMessage), nil }),
		snodra.HandleCommand(app, func(context.Context, Done) (snodra.Empty, error) { return snodra.Empty{}, nil }),
		snodra.HandleCommand(app, func(ctx context.Context, m Sleep) (SleepAnswer, error) { return s.sleep(ctx, m.Ms) }),
		snodra.HandleQuery(app, func(ctx context.Context, m SleepQuery) (SleepAnswer, error) { return s.sleep(ctx, m.Ms) }),
		snodra.HandleCommand(app, s.fail),
		snodra.HandleCommand(app, func(_ context.Context, m Strict) (StrictAnswer, error) { return StrictAnswer(m), nil }),
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

func (s *service) view(context.Context, View) (ViewAnswer, error) {
	a := ViewAnswer{
		Service:    s.env.service,
		InstanceID: s.env.instance,
		HubIDs:     s.app.HubIDs(),
		Retry:      s.env.retry,
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
