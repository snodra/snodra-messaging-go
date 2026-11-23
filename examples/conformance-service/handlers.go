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
	app     *snodra.App
	env     env
	ops     *operations            // the asynchronous conformance.Send calls
	watches *snodra.Updates[Watch] // the conformance.Watch subscriptions served here
}

// registerHandlers registers every type of the conformance contract the service handles so
// far, as an application would.
func registerHandlers(app *snodra.App, e env) error {
	s := &service{app: app, env: e, ops: newOperations()}
	watches, err := snodra.HandleSubscription(app, func(_ context.Context, m Watch) (WatchInitial, error) {
		return WatchInitial{Topic: m.Topic, Value: 0, InstanceID: e.instance}, nil
	})
	s.watches = watches
	return errors.Join(
		err,
		snodra.HandleCommand(app, s.emit),
		snodra.HandleCommand(app, func(ctx context.Context, m Echo) (EchoAnswer, error) { return s.echo(ctx, m.RawMessage), nil }),
		snodra.HandleQuery(app, func(ctx context.Context, m EchoQuery) (EchoAnswer, error) { return s.echo(ctx, m.RawMessage), nil }),
		snodra.HandleCommand(app, func(context.Context, Done) (snodra.Empty, error) { return snodra.Empty{}, nil }),
		snodra.HandleCommand(app, func(ctx context.Context, m Sleep) (SleepAnswer, error) { return s.sleep(ctx, m.Ms) }),
		snodra.HandleQuery(app, func(ctx context.Context, m SleepQuery) (SleepAnswer, error) { return s.sleep(ctx, m.Ms) }),
		// Shared: other services may own it too.
		snodra.HandleQuery(app, func(context.Context, SharedQuery) (SharedQueryAnswer, error) {
			return SharedQueryAnswer{Service: e.service, InstanceID: e.instance}, nil
		}, snodra.Shared()),
		snodra.HandleCommand(app, s.fail),
		snodra.HandleCommand(app, func(_ context.Context, m Strict) (StrictAnswer, error) { return StrictAnswer(m), nil }),
		registerGuarded(app, e),
		snodra.HandleCommand(app, s.relay),
		snodra.HandleCommand(app, func(ctx context.Context, m Send) (json.RawMessage, error) { return s.send(ctx, m) }),
		snodra.HandleQuery(app, func(ctx context.Context, m Outcome) (json.RawMessage, error) { return s.outcome(ctx, m) }),
		snodra.HandleQuery(app, s.view),
		snodra.HandleStream(app, stream),
	)
}

// stream streams m.Count items {n}, n = 1, 2, ..., each after m.DelayMs, under the caller's
// credit.
func stream(ctx context.Context, m Stream, w *snodra.StreamWriter[StreamItem]) error {
	delay := time.Duration(m.DelayMs) * time.Millisecond
	for n := int64(1); n <= m.Count; n++ {
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		if err := w.Send(ctx, StreamItem{N: n}); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) echo(ctx context.Context, payload []byte) EchoAnswer {
	a := EchoAnswer{Payload: payload, Headers: map[string]string{}, InstanceID: s.env.instance}
	if info, ok := snodra.MessageFrom(ctx); ok {
		maps.Copy(a.Headers, info.Headers)
		a.RequestID = info.ID
	}
	return a
}

// registerGuarded registers conformance.Guarded: for an authenticated caller that is not the
// subject the contract names as refused. A guard needs the issuer's keys; without them (the
// runner gives them only to the checks that call Guarded) no caller can be verified, so the
// handler refuses every one itself.
func registerGuarded(app *snodra.App, e env) error {
	if e.jwksURL == "" {
		return snodra.HandleCommand(app, func(context.Context, Guarded) (GuardedAnswer, error) {
			return GuardedAnswer{}, snodra.Unauthenticated("handler requires authentication")
		})
	}
	return snodra.HandleCommand(app, guarded, snodra.RequireCaller(func(c snodra.Caller) bool { return c.Subject != "conformance-refused" }))
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

// emit emits each value to the Watch subscriptions of the topic, then completes or fails them.
func (s *service) emit(_ context.Context, m Emit) (snodra.Empty, error) {
	ofTopic := func(w Watch) bool { return w.Topic == m.Topic }
	for _, v := range m.Values {
		s.watches.Emit(ofTopic, WatchUpdate{Topic: m.Topic, Value: v})
	}
	switch {
	case m.Fail != nil:
		s.watches.Fail(ofTopic, errors.New(*m.Fail))
	case m.Complete:
		s.watches.Complete(ofTopic)
	}
	return snodra.Empty{}, nil
}

func (s *service) view(context.Context, View) (ViewAnswer, error) {
	a := ViewAnswer{
		Service:             s.env.service,
		InstanceID:          s.env.instance,
		HubIDs:              s.app.HubIDs(),
		SubscriptionsServed: s.app.SubscriptionsServed(),
		Retry:               s.env.retry != nil,
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
