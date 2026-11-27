package snodra

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// l3Serving starts a started app on hub with the handlers register adds and one acknowledged
// stream; the in-memory stores, the consumer settings and the shortened processor clocks are
// already set.
func l3Serving(t *testing.T, hub *fakehub.Hub, register func(*App), opts ...Option) (*App, *fakehub.Stream) {
	t.Helper()
	base := []Option{
		Hubs(hub.Addr()), WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false),
	}
	app := newApp(t, append(base, opts...)...)
	register(app)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Recv(t)
	s.Ack(t, fakehub.Ack("hub-1"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)
	return app, s
}

// controlRequest builds the hub's frame for req.
func controlRequest(req *hubpb.ControlRequest) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Control{Control: req}}
}

// pauseControl is a pause of processor with control id id.
func pauseControl(id, processor string) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_Pause{Pause: &hubpb.ProcessorPause{}},
	})
}

// resumeControl is a resume of processor with control id id.
func resumeControl(id, processor string) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_Resume{Resume: &hubpb.ProcessorResume{}},
	})
}

// gateControl is a processor-control store whose SetPaused blocks until release (after the first
// call signalled entered).
type gateControl struct {
	ProcessorControlStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu       sync.Mutex
	sequence []bool
}

func (c *gateControl) SetPaused(ctx context.Context, processor string, paused bool, by string, now time.Time) error {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	c.mu.Lock()
	c.sequence = append(c.sequence, paused)
	c.mu.Unlock()
	return c.ProcessorControlStore.SetPaused(ctx, processor, paused, by, now)
}

func (c *gateControl) calls() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.sequence...)
}

// gatedStore hands out control through a gateControl.
type gatedStore struct {
	MessageStore
	control *gateControl
}

func (s *gatedStore) Control() ProcessorControlStore { return s.control }

// slowControl is a control store whose SetPaused takes delay and counts how many run at once per
// processor.
type slowControl struct {
	ProcessorControlStore
	delay time.Duration
	mu    sync.Mutex
	now   map[string]int
	most  map[string]int
}

func (c *slowControl) SetPaused(ctx context.Context, processor string, paused bool, by string, now time.Time) error {
	c.mu.Lock()
	if c.now == nil {
		c.now, c.most = map[string]int{}, map[string]int{}
	}
	c.now[processor]++
	if c.now[processor] > c.most[processor] {
		c.most[processor] = c.now[processor]
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.now[processor]--
		c.mu.Unlock()
	}()
	time.Sleep(c.delay)
	return c.ProcessorControlStore.SetPaused(ctx, processor, paused, by, now)
}

// mostAtOnce is how many SetPaused calls of processor ran at once, at most.
func (c *slowControl) mostAtOnce(processor string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.most[processor]
}

// slowStore hands out control through a slowControl.
type slowStore struct {
	MessageStore
	control *slowControl
}

func (s *slowStore) Control() ProcessorControlStore { return s.control }

// L3-CONTROL: a control request is handled off the read loop (requests and heartbeats go on
// while it waits), answered with exactly one reply carrying its control_id, on its arrival
// stream, and logged at INFO.
func TestControlRunsOffTheReadLoopWithOneReplyAndAnInfoLine(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryMessageStore()
	control := &gateControl{ProcessorControlStore: inner.Control(), entered: make(chan struct{}), release: make(chan struct{})}
	store := &gatedStore{MessageStore: inner, control: control}
	recorder := &logRecorder{}
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleCommand(app, place); err != nil {
			t.Fatal(err)
		}
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), Logger(slog.New(recorder)))

	s.Send(t, pauseControl("control-1", DefaultProcessor))
	<-control.entered
	// The pause is still blocked in the store, and the stream keeps serving.
	s.Send(t, command("cmd-1", "orders.PlaceOrder", `{"orderId":"o-1"}`))
	reply := nextReply(t, s)
	is(t, reply.GetRequestId(), "cmd-1")
	if got := control.calls(); len(got) != 0 {
		t.Fatalf("the pause completed while it was blocked: %v", got)
	}
	close(control.release)
	answer := s.RecvControlReply(t)
	is(t, answer.GetControlId(), "control-1")
	if answer.GetOk() == nil {
		t.Fatalf("pause: %v", answer)
	}
	s.Nothing(t, 100*ms, "control_reply")
	if got := control.calls(); len(got) != 1 || !got[0] {
		t.Fatalf("the store saw %v, want one pause", got)
	}
	state, err := inner.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || !state.Paused || state.PausedBy != "control-1" {
		t.Fatalf("the pause was not persisted: %+v, %v", state, err)
	}

	var info slog.Record
	recorder.mu.Lock()
	for _, record := range recorder.records {
		if record.Level == slog.LevelInfo && record.Message == "processor control" {
			info = record
		}
	}
	recorder.mu.Unlock()
	if info.Message == "" {
		t.Fatal("the control request was not logged at INFO")
	}
	attrs := map[string]string{}
	info.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	is(t, attrs["control"], "control-1")
	is(t, attrs["processor"], DefaultProcessor)
	is(t, attrs["action"], "pause")
	if attrs["result"] == "" {
		t.Fatal("the log line does not name the result")
	}
	if app.processorStatuses()[0].GetState() != hubpb.ProcessorState_PROCESSOR_PAUSED {
		t.Fatalf("the processor is not paused: %v", app.processorStatuses()[0].GetState())
	}
}

// L3-CONTROL: one request per processor at a time: a slow action delays the next one of the same
// processor but no other processor's.
func TestControlRunsOneRequestPerProcessorAtATime(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryMessageStore()
	control := &slowControl{ProcessorControlStore: inner.Control(), delay: 20 * ms}
	store := &slowStore{MessageStore: inner, control: control}
	_, s := l3Serving(t, hub, func(app *App) {
		for _, name := range []string{"mail", "billing"} {
			name := name
			if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }, InProcessor(name)); err != nil {
				t.Fatal(err)
			}
		}
	}, WithStore(store))

	s.Send(t, pauseControl("p-1", "mail"))
	s.Send(t, resumeControl("p-2", "mail"))
	s.Send(t, pauseControl("p-3", "billing"))
	seen := map[string]*hubpb.ControlReply{}
	for len(seen) < 3 {
		answer := s.RecvControlReply(t)
		seen[answer.GetControlId()] = answer
	}
	for _, id := range []string{"p-1", "p-2", "p-3"} {
		if seen[id] == nil {
			t.Fatalf("no reply for %s: %v", id, seen)
		}
	}
	is(t, control.mostAtOnce("mail"), 1)
	state, err := inner.Control().Get(context.Background(), "mail")
	if err != nil || state.Paused {
		t.Fatalf("mail after pause and resume: %+v, %v", state, err)
	}
	state, err = inner.Control().Get(context.Background(), "billing")
	if err != nil || !state.Paused {
		t.Fatalf("billing after pause: %+v, %v", state, err)
	}
}

// L3-CONTROL: a processor this instance does not run is refused UNKNOWN_PROCESSOR, once.
func TestControlRefusesAnUnknownProcessor(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	s.Send(t, pauseControl("control-1", "nobody"))
	answer := s.RecvControlReply(t)
	is(t, answer.GetControlId(), "control-1")
	if answer.GetRefused() == nil {
		t.Fatalf("unknown processor: %v", answer)
	}
	is(t, answer.GetRefused().GetCode(), hubpb.ControlRefusal_CONTROL_REFUSAL_UNKNOWN_PROCESSOR)
	s.Nothing(t, 50*ms, "control_reply")
}

// L3-CONTROL: a control request without an action is refused INVALID, never UNSPECIFIED.
func TestControlWithoutAnActionIsInvalid(t *testing.T) {
	hub := fakehub.Start(t)
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	s.Send(t, controlRequest(&hubpb.ControlRequest{ControlId: "control-1", Processor: DefaultProcessor}))
	answer := s.RecvControlReply(t)
	is(t, answer.GetControlId(), "control-1")
	if answer.GetRefused() == nil {
		t.Fatalf("no action: %v", answer)
	}
	is(t, answer.GetRefused().GetCode(), hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID)
	if answer.GetRefused().GetCode() == hubpb.ControlRefusal_CONTROL_REFUSAL_UNSPECIFIED {
		t.Fatal("a library never answers UNSPECIFIED")
	}
}

// L3-CONTROL: the reply goes back on the stream the request arrived on, not on another hub's.
func TestControlRepliesOnItsArrivalStream(t *testing.T) {
	first, second := fakehub.Start(t), fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := newApp(t, Hubs(first.Addr(), second.Addr()), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	a := first.Next(t)
	b := second.Next(t)
	for _, stream := range []*fakehub.Stream{a, b} {
		stream.RecvRegister(t)
		stream.Recv(t)
	}
	a.Ack(t, fakehub.Ack("hub-1"))
	b.Ack(t, fakehub.Ack("hub-2"))
	is(t, app.Ready(ctxWithin(t, time.Second)), nil)

	b.Send(t, pauseControl("control-2", DefaultProcessor))
	answer := b.RecvControlReply(t)
	is(t, answer.GetControlId(), "control-2")
	a.Nothing(t, 100*ms, "control_reply")
}
