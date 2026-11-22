package snodramessaging

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
)

func TestRunReturnsNilOnCancel(t *testing.T) {
	hub := fakehub.Start(t)
	app := newApp(t, Hubs(hub.Addr()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	cancel()
	r := s.RecvRegister(t)
	is(t, len(r.GetCommandTypes())+len(r.GetQueryTypes())+len(r.GetSharedQueryTypes()), 0)
	s.HalfClosed(t, time.Second)
	select {
	case err := <-done:
		is(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestNewRefusesBadSettings(t *testing.T) {
	const hub = "127.0.0.1:7000"
	refused := func(want string, opts ...Option) {
		t.Helper()
		_, err := New(opts...)
		if err == nil {
			t.Fatalf("New accepted settings that should fail with %q", want)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
	refused("SNODRA_SERVICE_NAME", Hubs(hub))
	refused("MaxConcurrent", Service("orders-service"), Hubs(hub), MaxConcurrent(0))
	refused("ReconnectBackoff", Service("orders-service"), Hubs(hub), ReconnectBackoff(0, time.Second))
	refused("ReconnectBackoff", Service("orders-service"), Hubs(hub), ReconnectBackoff(2*s, time.Second))
	refused("SNODRA_SERVER_ADDRESS", Service("orders-service"))
	refused("SNODRA_SERVER_ADDRESS", Service("orders-service"), Hubs(""))
	refused("SNODRA_SERVER_ADDRESS", Service("orders-service"), Hubs("no-port"))
	refused("Instance", Service("orders-service"), Hubs(hub), Instance(""))
}

func TestNewDefaults(t *testing.T) {
	app, err := New(Service("orders-service"), Hubs("static://hub-1:7000", "hub-2:7000", "hub-1:7000"))
	is(t, err, nil)
	is(t, len(app.cfg.instance), 36) // a fresh UUID
	is(t, strings.Join(app.cfg.hubs, ","), "hub-1:7000,hub-2:7000")
	is(t, app.cfg.maxConcurrent, 256)
	is(t, app.cfg.loadFactor, uint32(100))
	other, err := New(Service("orders-service"), Hubs("hub-1:7000"))
	is(t, err, nil)
	is(t, other.cfg.instance != app.cfg.instance, true)
}

func TestStartTwiceAndAfterShutdownIsRefused(t *testing.T) {
	hub := fakehub.Start(t)
	app := startApp(t, Hubs(hub.Addr()))
	is(t, app.Start(context.Background()) != nil, true)
	is(t, app.Shutdown(ctxWithin(t, 5*time.Second)), nil)
	is(t, app.Shutdown(ctxWithin(t, 5*time.Second)), nil) // a second Shutdown is a no-op
	is(t, app.Start(context.Background()) != nil, true)
	is(t, app.Ready(ctxWithin(t, time.Second)) != nil, true) // never ready, and not waiting for it
}
