package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// resetPrepareControl is a reset_prepare request.
func resetPrepareControl(id, processor, operation string, leaseMs int64) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_ResetPrepare{ResetPrepare: &hubpb.ResetPrepare{
			OperationId: operation, LeaseMs: leaseMs,
		}},
	})
}

// resetExecuteControl is a reset_execute request.
func resetExecuteControl(id, processor, operation string, target *hubpb.ResetTarget) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_ResetExecute{ResetExecute: &hubpb.ResetExecute{
			OperationId: operation, Target: target,
		}},
	})
}

// resetFinishControl is a reset_finish request.
func resetFinishControl(id, processor, operation string, aborted bool) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_ResetFinish{ResetFinish: &hubpb.ResetFinish{
			OperationId: operation, Aborted: aborted,
		}},
	})
}

// targetEarliest is the earliest reset target.
func targetEarliest() *hubpb.ResetTarget {
	return &hubpb.ResetTarget{To: &hubpb.ResetTarget_Earliest{Earliest: true}}
}

// targetLatest is the latest reset target.
func targetLatest() *hubpb.ResetTarget {
	return &hubpb.ResetTarget{To: &hubpb.ResetTarget_Latest{Latest: true}}
}

// targetTimestamp is a timestamp reset target.
func targetTimestamp(milliseconds int64) *hubpb.ResetTarget {
	return &hubpb.ResetTarget{To: &hubpb.ResetTarget_TimestampMs{TimestampMs: milliseconds}}
}

// targetPositions is an explicit-positions reset target.
func targetPositions(pairs ...SegmentPosition) *hubpb.ResetTarget {
	listed := make([]*hubpb.SegmentPosition, 0, len(pairs))
	for _, pair := range pairs {
		listed = append(listed, &hubpb.SegmentPosition{Segment: pair.Segment, Position: pair.Position})
	}
	return &hubpb.ResetTarget{To: &hubpb.ResetTarget_Positions{
		Positions: &hubpb.SegmentPositions{Positions: listed},
	}}
}

// sendControl sends msg and returns its one reply.
func sendControl(t *testing.T, s *fakehub.Stream, msg *hubpb.ServerMessage) *hubpb.ControlReply {
	t.Helper()
	s.Send(t, msg)
	return s.RecvControlReply(t)
}

// l3ServingAt is l3Serving on a given application clock, set before Start so the store and the
// processors stamp and compare the same times.
func l3ServingAt(t *testing.T, hub *fakehub.Hub, now func() time.Time, register func(*App), opts ...Option) (*App, *fakehub.Stream) {
	t.Helper()
	base := []Option{
		Hubs(hub.Addr()), WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false),
	}
	app := newApp(t, append(base, opts...)...)
	app.now = now
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

// waitSubscriptionMembers waits until the subscription has want members.
func waitSubscriptionMembers(t *testing.T, events EventStore, spec SubscriptionSpec, want int) {
	t.Helper()
	admin, err := events.Admin(spec)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, fmt.Sprintf("%d subscription members", want), func() bool {
		members, err := admin.Members(context.Background())
		return err == nil && len(members) == want
	})
}

// okReply fails unless reply is OK.
func okReply(t *testing.T, reply *hubpb.ControlReply, what string) {
	t.Helper()
	if reply.GetOk() == nil {
		t.Fatalf("%s: %v", what, reply)
	}
}

// refusedReply fails unless reply is a refusal with code.
func refusedReply(t *testing.T, reply *hubpb.ControlReply, code hubpb.ControlRefusal, what string) *hubpb.ControlRefused {
	t.Helper()
	if reply.GetRefused() == nil || reply.GetRefused().GetCode() != code {
		t.Fatalf("%s: got %v, want refusal %s", what, reply, code)
	}
	return reply.GetRefused()
}

// seenCounts counts the event handler's calls by event id.
type seenCounts struct {
	mu   sync.Mutex
	seen map[string]int
}

func newSeenCounts() *seenCounts { return &seenCounts{seen: map[string]int{}} }

func (s *seenCounts) add(id string) {
	s.mu.Lock()
	s.seen[id]++
	s.mu.Unlock()
}

func (s *seenCounts) get(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id]
}

func (s *seenCounts) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, count := range s.seen {
		n += count
	}
	return n
}

// resetContexts records the reset handlers' calls.
type resetContexts struct {
	mu      sync.Mutex
	context []ResetContext
}

func (r *resetContexts) add(context ResetContext) {
	r.mu.Lock()
	r.context = append(r.context, context)
	r.mu.Unlock()
}

func (r *resetContexts) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.context)
}

func (r *resetContexts) at(i int) ResetContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.context[i]
}

// L3-RESET: prepare leaves the subscription; execute to earliest replays what was acknowledged,
// runs the reset handlers before deleting the range's markers, and finish consumes again and the
// replay range closes when the committed positions reach it.
func TestAResetToEarliestReplaysHistoryWithItsHandlers(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(2)
	if err != nil {
		t.Fatal(err)
	}
	counts := newSeenCounts()
	resets := &resetContexts{}
	ids := []string{"evt-1", "evt-2", "evt-3"}
	var markersAtHandler atomic.Bool
	var handlerRan atomic.Int64
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(ctx context.Context, reset ResetContext) error {
			handlerRan.Add(1)
			resets.add(reset)
			all := true
			for _, id := range ids {
				exists, err := store.Processed().Exists(ctx, ProcessorMarker(DefaultProcessor, id))
				if err != nil || !exists {
					all = false
				}
			}
			markersAtHandler.Store(all)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))

	for i, id := range ids {
		key := fmt.Sprintf("k%d", i)
		publishTestEvent(t, events, id, "orders.OrderPlaced", &key, nil)
	}
	waitUntil(t, "the three records", func() bool { return counts.total() == 3 })
	// A marker without a position (a local dispatch) is not the reset's to delete.
	if _, err := store.Processed().Record(context.Background(), ProcessedMarker{
		ID: "local:1", Type: "orders.OrderPlaced", ProcessedAt: time.Now(), Processor: DefaultProcessor,
	}); err != nil {
		t.Fatal(err)
	}

	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	waitUntil(t, "the processor stopped for the reset", func() bool {
		status := processorStatusOf(t, app, DefaultProcessor)
		return status.GetState() == hubpb.ProcessorState_PROCESSOR_STOPPED && status.GetOperationId() == "op-1"
	})
	waitSubscriptionMembers(t, events, SubscriptionSpec{
		Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource,
	}, 0)

	reply := sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest()))
	outcome := reply.GetReset_()
	if outcome == nil {
		t.Fatalf("execute: %v", reply)
	}
	if len(outcome.GetRanges()) == 0 {
		t.Fatal("earliest replays a range")
	}
	is(t, outcome.GetMarkersDeleted(), uint64(3))
	is(t, outcome.GetResetHandlers(), uint32(1))
	is(t, handlerRan.Load(), int64(1))
	if !markersAtHandler.Load() {
		t.Fatal("a marker was deleted before the reset handler ran")
	}
	reset := resets.at(0)
	is(t, reset.Processor, DefaultProcessor)
	is(t, reset.OperationID, "op-1")
	if _, earliest := reset.Target.(ResetEarliest); !earliest {
		t.Fatalf("the reset context's target: %T", reset.Target)
	}
	if len(reset.Ranges) != len(outcome.GetRanges()) {
		t.Fatalf("the reset context's ranges: %v, outcome: %v", reset.Ranges, outcome.GetRanges())
	}
	for _, id := range ids {
		if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, id)); err != nil || exists {
			t.Fatalf("the range's marker of %s: %v, %v", id, exists, err)
		}
	}
	if exists, err := store.Processed().Exists(context.Background(), "local:1"); err != nil || !exists {
		t.Fatalf("a marker without a position was deleted: %v, %v", exists, err)
	}

	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	waitUntil(t, "the replayed history", func() bool {
		for _, id := range ids {
			if counts.get(id) != 2 {
				return false
			}
		}
		return true
	})
	waitUntil(t, "the replay range to close", func() bool {
		state, err := store.Control().Get(context.Background(), DefaultProcessor)
		return err == nil && len(state.Replay) == 0
	})
}

// L3-RESET: a reset to latest moves the positions without replaying or running reset handlers.
func TestAResetToLatestMovesWithoutReplay(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	counts := newSeenCounts()
	resets := &resetContexts{}
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(ctx context.Context, reset ResetContext) error {
			resets.add(reset)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("k%d", i)
		publishTestEvent(t, events, fmt.Sprintf("evt-%d", i), "orders.OrderPlaced", &key, nil)
	}
	waitUntil(t, "the three records", func() bool { return counts.total() == 3 })

	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	reply := sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetLatest()))
	outcome := reply.GetReset_()
	if outcome == nil || len(outcome.GetRanges()) != 0 ||
		outcome.GetMarkersDeleted() != 0 || outcome.GetResetHandlers() != 0 {
		t.Fatalf("latest: %v", reply)
	}
	is(t, resets.count(), 0)
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	neverWithin(t, 100*ms, "a replay of latest", func() bool { return counts.total() > 3 })
	publishTestEvent(t, events, "evt-4", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the record after latest", func() bool { return counts.get("evt-4") == 1 })
}

// L3-RESET: refusals change nothing: another operation's lock, a wrong operation, an ended
// operation, a bad lease and a missing target.
func TestResetRefusalsChangeNothing(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	counts := newSeenCounts()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the record", func() bool { return counts.total() == 1 })
	marker := ProcessorMarker(DefaultProcessor, "evt-1")

	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	refused := refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-2", DefaultProcessor, "op-2", 30000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_LOCKED, "another operation's lock")
	if !strings.Contains(refused.GetMessage(), "op-1") {
		t.Fatalf("the LOCKED message names the holder: %q", refused.GetMessage())
	}
	refusedReply(t, sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-2", targetEarliest())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED, "execute of an unprepared operation")
	refusedReply(t, sendControl(t, s, resetFinishControl("finish-2", DefaultProcessor, "op-2", true)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED, "finish of another operation")
	if exists, err := store.Processed().Exists(context.Background(), marker); err != nil || !exists {
		t.Fatalf("a refusal moved the markers: %v, %v", exists, err)
	}
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || state.Operation == nil || state.Operation.ID != "op-1" || len(state.Replay) != 0 {
		t.Fatalf("a refusal changed the lock or the ranges: %+v, %v", state, err)
	}

	// The same operation again renews the lease.
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-3", DefaultProcessor, "op-1", 30000)), "renewal")
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-4", DefaultProcessor, "op-1", 30000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED, "a prepare after the operation ended")
	refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-5", DefaultProcessor, "op-3", 999)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "a lease under 1000 ms")
	refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-6", DefaultProcessor, "", 30000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "no operation id")
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-7", DefaultProcessor, "op-4", 30000)), "prepare op-4")
	refusedReply(t, sendControl(t, s, resetExecuteControl("execute-2", DefaultProcessor, "op-4", nil)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "no target")
	refusedReply(t, sendControl(t, s, resetExecuteControl("execute-3", DefaultProcessor, "op-4", targetPositions())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "an empty positions target")
	okReply(t, sendControl(t, s, resetFinishControl("finish-4", DefaultProcessor, "op-4", true)), "finish op-4")
	if exists, err := store.Processed().Exists(context.Background(), marker); err != nil || !exists {
		t.Fatalf("the refused resets moved markers: %v, %v", exists, err)
	}
}

// L3-RESET: a failed reset handler stops the reset before the markers and positions; the finish
// releases the lock and the record is not replayed.
func TestAResetHandlerFailureStopsBeforeTheMarkers(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	counts := newSeenCounts()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(context.Context, ResetContext) error {
			return errText("the projection cannot be cleared")
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the record", func() bool { return counts.total() == 1 })
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	refused := refusedReply(t, sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_RESET_HANDLER_FAILED, "a failed reset handler")
	if !strings.Contains(refused.GetMessage(), "the projection cannot be cleared") ||
		!strings.Contains(refused.GetMessage(), "positions not moved") {
		t.Fatalf("the handler refusal: %q", refused.GetMessage())
	}
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1")); err != nil || !exists {
		t.Fatalf("a failed reset handler deleted the marker: %v, %v", exists, err)
	}
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || len(state.Replay) != 0 {
		t.Fatalf("a failed reset handler wrote ranges: %+v, %v", state, err)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
	neverWithin(t, 100*ms, "a replay after a failed handler", func() bool { return counts.total() > 1 })
}

// L3-RESET: a passed lease releases the lock and resumes by itself; an ended operation refuses a
// late prepare; a renewal keeps the lease.
func TestAResetLeaseRenewalAndExpiry(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	clock := newStoreClock()
	var handled atomic.Int64
	app, s := l3ServingAt(t, hub, clock.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			handled.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 1000)), "prepare")
	clock.Advance(400 * time.Millisecond)
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-2", DefaultProcessor, "op-1", 1000)), "renewal")
	clock.Advance(700 * time.Millisecond) // past the first lease, inside the renewed one
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || state.Operation == nil || state.Operation.ID != "op-1" {
		t.Fatalf("the renewed lease was released too early: %+v, %v", state, err)
	}
	clock.Advance(400 * time.Millisecond) // past the renewed lease too
	waitUntil(t, "the lease to pass and the processor to run again", func() bool {
		state, err := store.Control().Get(context.Background(), DefaultProcessor)
		return err == nil && state.Operation == nil &&
			processorStatusOf(t, app, DefaultProcessor).GetState() == hubpb.ProcessorState_PROCESSOR_RUNNING
	})
	publishTestEvent(t, events, "evt-after-expiry", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "a record after the lease passed", func() bool { return handled.Load() == 1 })
	refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-3", DefaultProcessor, "op-1", 1000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED, "a late prepare after the lease passed")
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-4", DefaultProcessor, "op-2", 30000)), "prepare op-2")
	okReply(t, sendControl(t, s, resetFinishControl("finish-2", DefaultProcessor, "op-2", false)), "finish op-2")
}

// L3-RESET: an instance starting under a held lock does not join the subscription; a finish for
// that operation releases it and consuming starts.
func TestAnInstanceStartingUnderAHeldLockDoesNotJoin(t *testing.T) {
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	at := time.Now()
	if outcome, err := store.Control().Lock(context.Background(), DefaultProcessor, "op-9", at.Add(time.Minute), at); err != nil || !outcome.Taken {
		t.Fatalf("the pre-held lock: %+v, %v", outcome, err)
	}
	hub := fakehub.Start(t)
	counts := newSeenCounts()
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	neverWithin(t, 100*ms, "a record under a held lock", func() bool { return counts.total() > 0 })
	status := processorStatusOf(t, app, DefaultProcessor)
	is(t, status.GetState(), hubpb.ProcessorState_PROCESSOR_STOPPED)
	is(t, status.GetOperationId(), "op-9")
	waitSubscriptionMembers(t, events, SubscriptionSpec{
		Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource,
	}, 0)
	okReply(t, sendControl(t, s, resetFinishControl("finish-9", DefaultProcessor, "op-9", false)), "finish the foreign operation")
	waitUntil(t, "the record after the lock left", func() bool { return counts.total() == 1 })
}

// L3-RESET: execute refuses while another instance still reads the subscription, and moves
// nothing.
func TestAResetWithSubscribersActiveMovesNothing(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	counts := newSeenCounts()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the record", func() bool { return counts.total() == 1 })
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")

	second, err := events.Subscribe(context.Background(), SubscriptionSpec{
		Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	refused := refusedReply(t, sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE, "subscribers active")
	if refused.GetMessage() == "" {
		t.Fatal("the refusal names the subscribers")
	}
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1")); err != nil || !exists {
		t.Fatalf("a subscribers-active refusal deleted the marker: %v, %v", exists, err)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitSubscriptionMembers(t, events, SubscriptionSpec{
		Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource,
	}, 0)
	reply := sendControl(t, s, resetExecuteControl("execute-2", DefaultProcessor, "op-1", targetEarliest()))
	if reply.GetReset_() == nil {
		t.Fatalf("execute after the subscribers left: %v", reply)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
}

// noReplayStore hides the replay capability from an event store.
type noReplayStore struct {
	EventStore
}

func (s noReplayStore) Capabilities() []Capability {
	var kept []Capability
	for _, capability := range s.EventStore.Capabilities() {
		if capability != CapabilityReplay {
			kept = append(kept, capability)
		}
	}
	return kept
}

func (s noReplayStore) Admin(SubscriptionSpec) (SubscriptionAdmin, error) {
	return nil, &UnsupportedError{Capability: CapabilityReplay}
}

// L3-RESET: an event store without replay refuses a reset UNSUPPORTED before anything changes.
func TestAResetWithoutReplayIsUnsupported(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	inner := NewMemoryEventStore()
	events := noReplayStore{EventStore: inner}
	counts := newSeenCounts()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, inner, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the record without replay", func() bool { return counts.total() == 1 })
	refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_UNSUPPORTED, "prepare without replay")
	refusedReply(t, sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_UNSUPPORTED, "execute without replay")
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || state.Operation != nil {
		t.Fatalf("a reset without replay took the lock: %+v, %v", state, err)
	}
	publishTestEvent(t, inner, "evt-2", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the processor after the refusals", func() bool { return counts.total() == 2 })
}

// L3-REPLAY: a handler marked DisallowReplay does not run again inside a replay range; the
// others do, and the range closes.
func TestReplaySkipsDisallowReplayHandlers(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	normal := newSeenCounts()
	noReplay := newSeenCounts()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			normal.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			noReplay.add(event.OrderID)
			return nil
		}, DisallowReplay()); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "both handlers on both records", func() bool {
		return normal.total() == 2 && noReplay.total() == 2
	})
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	if reply := sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())); reply.GetReset_() == nil {
		t.Fatalf("execute: %v", reply)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	waitUntil(t, "the normal handler replayed", func() bool {
		return normal.get("evt-1") == 2 && normal.get("evt-2") == 2
	})
	is(t, noReplay.get("evt-1"), 1)
	is(t, noReplay.get("evt-2"), 1)
	waitUntil(t, "the replay range's close", func() bool {
		state, err := store.Control().Get(context.Background(), DefaultProcessor)
		return err == nil && len(state.Replay) == 0
	})
}

// L3-RESET/L3-REPLAY: a timestamp target replays from the first record at or after it, and an
// explicit position from that position on.
func TestResetTimestampAndPositionsTargets(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	counts := newSeenCounts()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	key := "k1"
	for i := 0; i < 3; i++ {
		publishTestEvent(t, events, fmt.Sprintf("evt-%d", i), "orders.OrderPlaced", &key, nil)
		time.Sleep(3 * time.Millisecond)
	}
	waitUntil(t, "the three records", func() bool { return counts.total() == 3 })
	records := events.Events(DefaultEventsSource)
	if len(records) != 3 {
		t.Fatalf("records: %d", len(records))
	}
	second := records[1]

	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	reply := sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetTimestamp(second.TimestampMs)))
	outcome := reply.GetReset_()
	if outcome == nil || len(outcome.GetRanges()) != 1 || outcome.GetRanges()[0].GetFrom() != second.Position {
		t.Fatalf("timestamp target: %v", reply)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	waitUntil(t, "the events from the timestamp", func() bool {
		return counts.get("evt-1") == 2 && counts.get("evt-2") == 2
	})
	is(t, counts.get("evt-0"), 1)

	okReply(t, sendControl(t, s, resetPrepareControl("prepare-2", DefaultProcessor, "op-2", 30000)), "prepare op-2")
	reply = sendControl(t, s, resetExecuteControl("execute-2", DefaultProcessor, "op-2",
		targetPositions(SegmentPosition{Segment: second.Segment, Position: second.Position})))
	outcome = reply.GetReset_()
	if outcome == nil || len(outcome.GetRanges()) != 1 ||
		outcome.GetRanges()[0].GetFrom() != second.Position ||
		outcome.GetRanges()[0].GetSegment() != second.Segment {
		t.Fatalf("positions target: %v", reply)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-2", DefaultProcessor, "op-2", false)), "finish op-2")
	waitUntil(t, "the events from the position", func() bool {
		return counts.get("evt-1") == 3 && counts.get("evt-2") == 3
	})
	is(t, counts.get("evt-0"), 1)
}

// A reset handler needs an event handler in its processor, and is registered before Start.
func TestResetHandlerRegistration(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	app := newApp(t, Hubs(hub.Addr()), WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := HandleReset(app, "", func(context.Context, ResetContext) error { return nil }); err == nil {
		t.Fatal("an empty processor name was accepted")
	}
	if err := HandleReset(app, "Default", func(context.Context, ResetContext) error { return nil }); err == nil {
		t.Fatal("a processor name with capitals was accepted")
	}
	if err := HandleReset(app, "mail", func(context.Context, ResetContext) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "mail") {
		t.Fatalf("a reset handler without its processor: %v", err)
	}

	if err := HandleReset(app, DefaultProcessor, nil); err == nil {
		t.Fatal("a nil reset handler was accepted")
	}
	app2 := newApp(t, Hubs("127.0.0.1:1"), WithStore(NewMemoryMessageStore()), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app2, func(context.Context, OrderPlaced) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := HandleReset(app2, DefaultProcessor, func(context.Context, ResetContext) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := app2.Start(context.Background()); err != nil {
		t.Fatalf("Start with a reset handler: %v", err)
	}
	if err := HandleReset(app2, DefaultProcessor, func(context.Context, ResetContext) error { return nil }); !errors.Is(err, ErrRegistrationClosed) {
		t.Fatalf("registration after Start: %v", err)
	}
}

// L3-RESET: an executing reset extends its own lease, so a slow reset handler does not lose it.
func TestAResetExtendsItsLeaseWhileExecuting(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	clock := newStoreClock()
	entered := make(chan struct{})
	release := make(chan struct{})
	var renewed atomic.Bool
	var initial time.Time
	timings := controlTimings()
	timings.resetLeaseRenew = time.Millisecond
	_, s := l3ServingAt(t, hub, clock.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(ctx context.Context, reset ResetContext) error {
			close(entered)
			<-release
			state, err := store.Control().Get(ctx, DefaultProcessor)
			if err == nil && state.Operation != nil && state.Operation.ID == reset.OperationID &&
				state.Operation.LeaseUntil.After(initial) {
				renewed.Store(true)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withTimings(timings))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 1000)), "prepare")
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || state.Operation == nil {
		t.Fatalf("the prepared lock: %+v, %v", state, err)
	}
	initial = state.Operation.LeaseUntil
	clock.Advance(500 * time.Millisecond)
	s.Send(t, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest()))
	<-entered
	waitUntil(t, "the execute to extend its lease", func() bool {
		state, err := store.Control().Get(context.Background(), DefaultProcessor)
		return err == nil && state.Operation != nil && state.Operation.LeaseUntil.After(initial)
	})
	close(release)
	if reply := s.RecvControlReply(t); reply.GetReset_() == nil {
		t.Fatalf("a slow reset with a renewed lease: %v", reply)
	}
	if !renewed.Load() {
		t.Fatal("the execute did not extend its lease")
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
}

// L3-RESET: a lease lost after the reset handlers stops the reset before the markers and says
// what changed.
func TestAResetLeaseLostAfterTheHandlers(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(ctx context.Context, reset ResetContext) error {
			_, err := store.Control().Release(ctx, DefaultProcessor, reset.OperationID, time.Now())
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	refused := refusedReply(t, sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, "a lost lease")
	if !strings.Contains(refused.GetMessage(), "lease lost") ||
		!strings.Contains(refused.GetMessage(), "reset handlers ran (1)") ||
		!strings.Contains(refused.GetMessage(), "positions not moved") {
		t.Fatalf("the lost-lease refusal: %q", refused.GetMessage())
	}
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1")); err != nil || !exists {
		t.Fatalf("a lost lease deleted the marker: %v, %v", exists, err)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
}

// L3-RESET: a finish consumes again, still paused if the processor was paused, and the records
// published meanwhile are delivered only after the resume.
func TestFinishKeepsAPausedProcessorPaused(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	counts := newSeenCounts()
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			counts.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the first record", func() bool { return counts.total() == 1 })
	okReply(t, sendControl(t, s, pauseControl("pause-1", DefaultProcessor)), "pause")
	waitUntil(t, "the paused processor", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetState() == hubpb.ProcessorState_PROCESSOR_PAUSED
	})
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	if reply := sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetLatest())); reply.GetReset_() == nil {
		t.Fatalf("execute latest while paused: %v", reply)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	waitUntil(t, "paused again after the finish", func() bool {
		status := processorStatusOf(t, app, DefaultProcessor)
		return status.GetState() == hubpb.ProcessorState_PROCESSOR_PAUSED && status.GetOperationId() == ""
	})
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", nil, nil)
	neverWithin(t, 100*ms, "a delivery after a paused finish", func() bool { return counts.get("evt-2") > 0 })
	okReply(t, sendControl(t, s, resumeControl("resume-1", DefaultProcessor)), "resume")
	waitUntil(t, "the record held by the pause and the reset", func() bool { return counts.get("evt-2") == 1 })
}

// L3-RESET: the replay ranges extend a still-open range of the segment and replace it in the
// store; a latest reset replays nothing.
func TestResetRangesExtendAndMerge(t *testing.T) {
	compare := func(_ string, a, b string) int { return SegmentOrder(a, b) }
	open := []ReplayRange{
		{Source: "s", Segment: "0", From: "0", Until: "5"},
		{Source: "s", Segment: "1", From: "1", Until: "2"},
	}
	plan := []ReplayRange{
		{Source: "s", Segment: "0", From: "2", Until: "4"},
		{Source: "s", Segment: "2", From: "0", Until: "3"},
	}
	ranges := replayingRanges(plan, ResetEarliest{}, open, compare)
	if len(ranges) != 2 {
		t.Fatalf("replaying ranges: %v", ranges)
	}
	if ranges[0].Segment != "0" || ranges[0].From != "2" || ranges[0].Until != "5" {
		t.Fatalf("a still-open range's end extends the new one: %v", ranges)
	}
	if ranges[1].Segment != "2" || ranges[1].Until != "3" {
		t.Fatalf("a new segment's range: %v", ranges)
	}
	if got := replayingRanges(plan, ResetLatest{}, open, compare); len(got) != 0 {
		t.Fatalf("latest replays nothing: %v", got)
	}
	if got := replayingRanges([]ReplayRange{{Source: "s", Segment: "0", From: "4", Until: "4"}}, ResetEarliest{}, nil, compare); len(got) != 0 {
		t.Fatalf("an empty move is no replay: %v", got)
	}
	merged := mergeReplayRanges(open, ranges, compare)
	if len(merged) != 3 || merged[0].Segment != "0" || merged[0].From != "2" ||
		merged[1].Segment != "1" || merged[1].From != "1" ||
		merged[2].Segment != "2" || merged[2].From != "0" {
		t.Fatalf("the merged ranges: %v", merged)
	}
}

// The hub's reset targets become the library's targets; a missing, empty or duplicated positions
// target is INVALID.
func TestResetTargetFromWire(t *testing.T) {
	request := &hubpb.ControlRequest{}
	if _, reply := resetTarget(request, nil); reply.GetRefused() == nil ||
		reply.GetRefused().GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID {
		t.Fatalf("no target: %v", reply)
	}
	if target, reply := resetTarget(request, targetLatest()); reply != nil {
		t.Fatalf("latest: %v", reply)
	} else if _, ok := target.(ResetLatest); !ok {
		t.Fatalf("latest target: %T", target)
	}
	if target, reply := resetTarget(request, targetTimestamp(42)); reply != nil {
		t.Fatalf("timestamp: %v", reply)
	} else if timestamp, ok := target.(ResetTimestamp); !ok || timestamp.TimestampMs != 42 {
		t.Fatalf("timestamp target: %#v", target)
	}
	if _, reply := resetTarget(request, targetPositions()); reply.GetRefused() == nil {
		t.Fatalf("empty positions: %v", reply)
	}
	if _, reply := resetTarget(request, targetPositions(
		SegmentPosition{Segment: "0", Position: "1"},
		SegmentPosition{Segment: "0", Position: "2"},
	)); reply.GetRefused() == nil {
		t.Fatalf("a segment twice: %v", reply)
	}
	if target, reply := resetTarget(request, targetPositions(SegmentPosition{Segment: "0", Position: "3"})); reply != nil {
		t.Fatalf("positions: %v", reply)
	} else if positions, ok := target.(ResetPositions); !ok || len(positions.Positions) != 1 ||
		positions.Positions[0] != (SegmentPosition{Segment: "0", Position: "3"}) {
		t.Fatalf("positions target: %#v", target)
	}
}

// L3-RESET: a prepare whose record in hand does not finish within the stop limit is refused; the
// lock stays and a later finish releases it.
func TestAPrepareWhoseHandlerDoesNotFinishIsRefused(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	entered := make(chan struct{})
	gate := make(chan struct{})
	timings := controlTimings()
	timings.resetStopLimit = 50 * time.Millisecond
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			close(entered)
			<-gate
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withTimings(timings))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	<-entered
	refused := refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, "a prepare over a running handler")
	if !strings.Contains(refused.GetMessage(), "did not stop") {
		t.Fatalf("the refusal says the subscription did not stop: %q", refused.GetMessage())
	}
	state, err := store.Control().Get(context.Background(), DefaultProcessor)
	if err != nil || state.Operation == nil || state.Operation.ID != "op-1" {
		t.Fatalf("the lock after the refused prepare: %+v, %v", state, err)
	}
	close(gate)
	waitUntil(t, "the record in hand to finish", func() bool {
		status := processorStatusOf(t, app, DefaultProcessor)
		return status.GetState() == hubpb.ProcessorState_PROCESSOR_STOPPED && status.GetOperationId() == "op-1"
	})
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
}

// L3-REPLAY: a dead letter parked during a replay is retried as a replay: the DisallowReplay
// handler stays out of both.
func TestARetriedDeadLetterFromAReplaySkipsDisallowReplay(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	normal := newSeenCounts()
	noReplay := newSeenCounts()
	var fail atomic.Bool
	settings := consumerSettings()
	settings.consumerTries = 1
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			if fail.Load() && event.OrderID == "evt-1" {
				return errText("the projection failed in the replay")
			}
			normal.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			noReplay.add(event.OrderID)
			return nil
		}, DisallowReplay()); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the first handling", func() bool {
		return normal.get("evt-1") == 1 && noReplay.get("evt-1") == 1
	})
	fail.Store(true)
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	if reply := sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())); reply.GetReset_() == nil {
		t.Fatalf("execute: %v", reply)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false)), "finish")
	waitUntil(t, "the replay's dead letter", func() bool {
		return len(lettersOf(t, store, DefaultProcessor)) == 1
	})
	letter := lettersOf(t, store, DefaultProcessor)[0]
	if !letter.Replay {
		t.Fatal("a letter parked during a replay is marked as a replay")
	}
	is(t, normal.get("evt-1"), 1)
	is(t, noReplay.get("evt-1"), 1)

	fail.Store(false)
	s.Send(t, deadLetterRetryControl("retry-1", DefaultProcessor, letter.ID))
	if outcome := s.RecvControlReply(t).GetRetry(); outcome == nil || outcome.GetResult() != retryRetried {
		t.Fatalf("the replayed letter's retry: %v", outcome)
	}
	is(t, normal.get("evt-1"), 2)
	is(t, noReplay.get("evt-1"), 1)
}

// L3-RESET: an execute that outlives its limit lets the lease lapse and says the handlers ran;
// the extension stops at the deadline.
func TestAResetThatOutlivesItsLimitLosesTheLease(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	clock := newStoreClock()
	entered := make(chan struct{})
	release := make(chan struct{})
	timings := controlTimings()
	timings.resetExecuteLimit = 100 * time.Millisecond
	_, s := l3ServingAt(t, hub, clock.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(context.Context, ResetContext) error {
			close(entered)
			<-release
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withTimings(timings))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 1000)), "prepare")
	s.Send(t, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest()))
	<-entered
	clock.Advance(200 * time.Millisecond) // past the 100 ms execute limit
	close(release)
	refused := refusedReply(t, s.RecvControlReply(t),
		hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, "an execute past its limit")
	if !strings.Contains(refused.GetMessage(), "ran longer than") ||
		!strings.Contains(refused.GetMessage(), "reset handlers ran (1)") {
		t.Fatalf("the outlived execute: %q", refused.GetMessage())
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
}

// L3-RESET: a finish whose lease lapsed and whose lock another operation took over is
// NOT_PREPARED and leaves the other lock; a finish for that operation releases it.
func TestAFinishAfterATakeoverIsNotPrepared(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	timings := controlTimings()
	timings.controlPoll = time.Second // keep the poller from clearing the held operation first
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withTimings(timings))
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	now := time.Now()
	if released, err := store.Control().Release(context.Background(), DefaultProcessor, "op-1", now); err != nil || !released {
		t.Fatalf("releasing op-1 for the takeover: %v, %v", released, err)
	}
	if outcome, err := store.Control().Lock(context.Background(), DefaultProcessor, "op-2", now.Add(30*time.Second), now); err != nil || !outcome.Taken {
		t.Fatalf("taking over with op-2: %+v, %v", outcome, err)
	}
	refused := refusedReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED, "a finish after a takeover")
	if !strings.Contains(refused.GetMessage(), "op-2") {
		t.Fatalf("the refusal names the new holder: %q", refused.GetMessage())
	}
	waitUntil(t, "the new operation's status", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetOperationId() == "op-2"
	})
	refusedReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)),
		hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED, "the ended operation is remembered")
	okReply(t, sendControl(t, s, resetFinishControl("finish-2", DefaultProcessor, "op-2", false)), "finish op-2")
	waitUntil(t, "the consumer after the takeover", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetOperationId() == ""
	})
}

// extendFailingControl fails every lease extension while fail is set.
type extendFailingControl struct {
	ProcessorControlStore
	fail  atomic.Bool
	calls atomic.Int64
	gets  atomic.Int64
}

func (c *extendFailingControl) Get(ctx context.Context, processor string) (ProcessorControlState, error) {
	c.gets.Add(1)
	return c.ProcessorControlStore.Get(ctx, processor)
}

func (c *extendFailingControl) Extend(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (bool, error) {
	c.calls.Add(1)
	if c.fail.Load() {
		return false, errText("the control store is down")
	}
	return c.ProcessorControlStore.Extend(ctx, processor, operation, leaseUntil, now)
}

type extendFailingStore struct {
	MessageStore
	control *extendFailingControl
}

func (s *extendFailingStore) Control() ProcessorControlStore { return s.control }

// L3-RESET: while an execute runs without a renewable lease, the expired persisted lease never
// lets the consumer rejoin before the execute refuses; nothing moves after the loss.
func TestAnExecuteThatLostItsLeaseKeepsTheConsumerOut(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryMessageStore()
	control := &extendFailingControl{ProcessorControlStore: inner.Control()}
	store := &extendFailingStore{MessageStore: inner, control: control}
	events := NewMemoryEventStore()
	clock := newStoreClock()
	entered := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Int64
	timings := controlTimings()
	timings.resetLeaseRenew = time.Millisecond
	app, s := l3ServingAt(t, hub, clock.Now, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			handled.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleReset(app, DefaultProcessor, func(context.Context, ResetContext) error {
			close(entered)
			<-release
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withTimings(timings))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the record before the reset", func() bool { return handled.Load() == 1 })
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 1000)), "prepare")
	control.fail.Store(true)
	s.Send(t, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest()))
	<-entered
	waitUntil(t, "the failed lease extension", func() bool { return control.calls.Load() > 0 })
	// The persisted lease passes while the reset handler still runs.
	clock.Advance(2 * time.Second)
	before := control.gets.Load()
	waitUntil(t, "polls after the lease passed", func() bool { return control.gets.Load() >= before+3 })
	status := processorStatusOf(t, app, DefaultProcessor)
	if status.GetState() != hubpb.ProcessorState_PROCESSOR_STOPPED || status.GetOperationId() != "op-1" {
		t.Fatalf("the executing processor left its stop: %v %v", status.GetState(), status.GetOperationId())
	}
	waitSubscriptionMembers(t, events, SubscriptionSpec{
		Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource,
	}, 0)
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", nil, nil)
	neverWithin(t, 100*time.Millisecond, "a record under a lost reset lock", func() bool {
		return handled.Load() > 1
	})
	close(release)
	refused := refusedReply(t, s.RecvControlReply(t),
		hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, "the execute reply after the loss")
	if !strings.Contains(refused.GetMessage(), "lease lost") {
		t.Fatalf("the lost execute: %q", refused.GetMessage())
	}
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-1")); err != nil || !exists {
		t.Fatalf("a lost execute deleted the marker: %v, %v", exists, err)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
	waitUntil(t, "the record after the finish", func() bool { return handled.Load() == 2 })
}

// L3-RESET: a non-executor applies the persisted replay ranges and pause on its finish before it
// consumes again: the replayed records skip DisallowReplay and stay paused.
func TestANonExecutorFinishAppliesTheReplayRangesAndPause(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	timings := controlTimings()
	timings.controlPoll = 5 * time.Second // the finish must apply the state, not a poll
	normal := newSeenCounts()
	noReplay := newSeenCounts()
	// The non-executor starts first, so it holds the single segment.
	app2, s2 := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			normal.add(event.OrderID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			noReplay.add(event.OrderID)
			return nil
		}, DisallowReplay()); err != nil {
			t.Fatal(err)
		}
	}, Instance("i-2"), WithStore(store), WithEventStore(events), withTimings(timings))
	app1, s1 := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, Instance("i-1"), WithStore(store), WithEventStore(events), withTimings(timings))
	publishTestEvent(t, events, "evt-1", "orders.OrderPlaced", nil, nil)
	publishTestEvent(t, events, "evt-2", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "both live handlers on both records", func() bool {
		return normal.total() == 2 && noReplay.total() == 2
	})

	okReply(t, sendControl(t, s2, resetPrepareControl("prepare-2", DefaultProcessor, "op-1", 30000)), "prepare the non-executor")
	okReply(t, sendControl(t, s1, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare the executor")
	if reply := sendControl(t, s1, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())); reply.GetReset_() == nil {
		t.Fatalf("execute: %v", reply)
	}
	// The executor leaves; the non-executor still holds the prepared operation.
	if err := app1.Shutdown(ctxWithin(t, 5*s)); err != nil {
		t.Fatalf("shutting the executor down: %v", err)
	}
	if err := store.Control().SetPaused(context.Background(), DefaultProcessor, true, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	okReply(t, sendControl(t, s2, resetFinishControl("finish-2", DefaultProcessor, "op-1", false)), "finish the non-executor")
	waitUntil(t, "paused with the replay ranges after the finish", func() bool {
		status := processorStatusOf(t, app2, DefaultProcessor)
		return status.GetState() == hubpb.ProcessorState_PROCESSOR_PAUSED && status.GetOperationId() == ""
	})
	neverWithin(t, 100*time.Millisecond, "a replay before the resume", func() bool { return normal.total() > 2 })
	okReply(t, sendControl(t, s2, resumeControl("resume-2", DefaultProcessor)), "resume")
	waitUntil(t, "the replayed records", func() bool { return normal.total() == 4 })
	is(t, noReplay.total(), 2)
}

// flipMembersAdmin answers no member on its first call and one on its second: a consumer that
// rejoined between the execute's plan and the move.
type flipMembersAdmin struct {
	SubscriptionAdmin
	calls   atomic.Int64
	applied atomic.Int64
}

func (a *flipMembersAdmin) Members(context.Context) ([]string, error) {
	if a.calls.Add(1) == 1 {
		return nil, nil
	}
	return []string{"a-reader"}, nil
}

func (a *flipMembersAdmin) Apply(ctx context.Context, ranges []ReplayRange) error {
	a.applied.Add(1)
	return a.SubscriptionAdmin.Apply(ctx, ranges)
}

type flipMembersStore struct {
	EventStore
	admin *flipMembersAdmin
}

func (s *flipMembersStore) Admin(spec SubscriptionSpec) (SubscriptionAdmin, error) {
	inner, err := s.EventStore.Admin(spec)
	if err != nil {
		return nil, err
	}
	s.admin = &flipMembersAdmin{SubscriptionAdmin: inner}
	return s.admin, nil
}

// L3-RESET: a reader that reappears between the plan and the move is refused immediately before
// Apply, and the positions do not move.
func TestAResetRechecksTheMembersBeforeMoving(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryEventStore()
	events := &flipMembersStore{EventStore: inner}
	store := NewMemoryMessageStore()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	publishTestEvent(t, inner, "evt-1", "orders.OrderPlaced", nil, nil)
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	refusedReply(t, sendControl(t, s, resetExecuteControl("execute-1", DefaultProcessor, "op-1", targetEarliest())),
		hubpb.ControlRefusal_CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE, "a reader that reappeared")
	if events.admin == nil || events.admin.calls.Load() < 2 {
		t.Fatalf("members rechecked before apply: %+v", events.admin)
	}
	if applied := events.admin.applied.Load(); applied != 0 {
		t.Fatalf("the positions moved with a reader present: %d applies", applied)
	}
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
}

// staleReadControl blocks one control-state read after its state was taken, so a test can write
// the store before the poll applies the old read.
type staleReadControl struct {
	ProcessorControlStore
	block    atomic.Bool
	captured chan struct{}
	release  chan struct{}
	once     sync.Once
	gets     atomic.Int64
}

func (c *staleReadControl) Get(ctx context.Context, processor string) (ProcessorControlState, error) {
	c.gets.Add(1)
	state, err := c.ProcessorControlStore.Get(ctx, processor)
	if c.block.Load() {
		c.once.Do(func() {
			close(c.captured)
			<-c.release
		})
	}
	return state, err
}

type staleReadStore struct {
	MessageStore
	control *staleReadControl
}

func (s *staleReadStore) Control() ProcessorControlStore { return s.control }

// L3-RESET: a poll whose read predates a prepare does not clear the lock or wake the consumer.
func TestAStalePollDoesNotUndoAPrepare(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryMessageStore()
	control := &staleReadControl{
		ProcessorControlStore: inner.Control(),
		captured:              make(chan struct{}),
		release:               make(chan struct{}),
	}
	store := &staleReadStore{MessageStore: inner, control: control}
	events := NewMemoryEventStore()
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	control.block.Store(true)
	<-control.captured
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	close(control.release)
	waitUntil(t, "polls after the stale read", func() bool { return control.gets.Load() >= 5 })
	status := processorStatusOf(t, app, DefaultProcessor)
	if status.GetState() != hubpb.ProcessorState_PROCESSOR_STOPPED || status.GetOperationId() != "op-1" {
		t.Fatalf("the stale poll undid the prepare: %v %v", status.GetState(), status.GetOperationId())
	}
	waitSubscriptionMembers(t, events, SubscriptionSpec{
		Name: ProcessorSubscription("orders-service", DefaultProcessor), Source: DefaultEventsSource,
	}, 0)
	okReply(t, sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", true)), "finish aborted")
}

// getFailingControl fails the control-state read while fail is set.
type getFailingControl struct {
	ProcessorControlStore
	fail atomic.Bool
}

func (c *getFailingControl) Get(ctx context.Context, processor string) (ProcessorControlState, error) {
	if c.fail.Load() {
		return ProcessorControlState{}, errText("the control store is down")
	}
	return c.ProcessorControlStore.Get(ctx, processor)
}

type getFailingStore struct {
	MessageStore
	control *getFailingControl
}

func (s *getFailingStore) Control() ProcessorControlStore { return s.control }

// L3-RESET: a finish whose lock cannot be read back says so instead of claiming it consumes.
func TestAFinishWhoseLockCannotBeReadSaysSo(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryMessageStore()
	control := &getFailingControl{ProcessorControlStore: inner.Control()}
	store := &getFailingStore{MessageStore: inner, control: control}
	events := NewMemoryEventStore()
	app, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	okReply(t, sendControl(t, s, resetPrepareControl("prepare-1", DefaultProcessor, "op-1", 30000)), "prepare")
	control.fail.Store(true)
	answer := sendControl(t, s, resetFinishControl("finish-1", DefaultProcessor, "op-1", false))
	if answer.GetOk() == nil || !strings.Contains(answer.GetOk().GetDetail(), "once the lock is read back") {
		t.Fatalf("a finish that cannot read the lock back: %v", answer)
	}
	control.fail.Store(false)
	waitUntil(t, "the consumer after the control store answers", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetState() == hubpb.ProcessorState_PROCESSOR_RUNNING
	})
}

// noRemoveControl reports that another instance removed a passed replay range.
type noRemoveControl struct {
	ProcessorControlStore
	removed atomic.Int64
}

func (c *noRemoveControl) RemoveRange(context.Context, string, ReplayRange, time.Time) (bool, error) {
	c.removed.Add(1)
	return false, nil
}

type noRemoveStore struct {
	MessageStore
	control *noRemoveControl
}

func (s *noRemoveStore) Control() ProcessorControlStore { return s.control }

// L3-REPLAY: a range another instance removed is dropped locally too.
func TestAPassedRangeRemovedElsewhereIsDroppedLocally(t *testing.T) {
	store := &noRemoveStore{
		MessageStore: NewMemoryMessageStore(),
		control:      &noRemoveControl{ProcessorControlStore: NewMemoryMessageStore().Control()},
	}
	app := eventsApp(t, WithStore(store), WithEventStore(NewMemoryEventStore()),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	p := newProcessor("orders-service", DefaultProcessor, DefaultEventsSource, map[string][]*eventHandler{})
	sub := &fixedSubscription{shared: []SegmentStatus{{Segment: "0", Position: "5", Committed: "5"}}}
	p.run.markStarted()
	p.run.attach(sub)
	p.run.setReplay([]ReplayRange{{Source: DefaultEventsSource, Segment: "0", From: "0", Until: "3"}})
	app.closePassedReplayRanges(context.Background(), p, app.now())
	if ranges := p.run.replayRanges(); len(ranges) != 0 {
		t.Fatalf("the local replay range was kept: %v", ranges)
	}
	if store.control.removed.Load() == 0 {
		t.Fatal("the range was not submitted for removal")
	}
}
