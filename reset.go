package snodra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// The reset state machine's clocks: the lease a prepare must ask for at least, how often an
// executing reset renews its lease, and how long an operation that ended on this instance is
// remembered so a late renewal is refused.
const (
	resetMinLease        = time.Second
	resetLeaseExtension  = 10 * time.Second
	resetFinishedKept    = 60 * time.Second
	resetFinishedKeptMax = 24 * time.Hour
)

// ResetContext is what a reset handler is told: the processor, the reset's operation id, its
// target and the replay ranges it will delete markers in (never empty). A handler runs before any
// marker or position moves; a failure stops the reset with
// ControlRefusal.CONTROL_REFUSAL_RESET_HANDLER_FAILED.
type ResetContext struct {
	Processor   string
	OperationID string
	Target      ResetTarget
	Ranges      []ReplayRange
}

// HandleReset registers h as a reset handler of processor (the [DefaultProcessor] or a named
// one): every reset of that processor runs its handlers, in registration order, when anything
// replays, before any marker or position moves. Reset handlers are registered before [App.Start],
// like event handlers, and a processor with a reset handler needs an event handler in it.
//
//	A reset handler clears the projections a replay will rebuild, so the replayed events apply
//	to empty state; it sees the target and the replay ranges of the reset.
func HandleReset(app *App, processor string, h func(context.Context, ResetContext) error) error {
	if app == nil {
		return errors.New("snodra: registration on a nil App")
	}
	name := strings.TrimSpace(processor)
	switch {
	case name == "":
		return errors.New("snodra: HandleReset needs a processor name")
	case name != DefaultProcessor && !processorName.MatchString(name):
		return fmt.Errorf("snodra: HandleReset(%q): a processor name matches [a-z0-9][a-z0-9-]{0,47}", name)
	case h == nil:
		return fmt.Errorf("snodra: HandleReset(%s): nil reset handler", name)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.started || app.stopping {
		return fmt.Errorf("%w: the reset handler of processor %s registered after Start", ErrRegistrationClosed, name)
	}
	label := fmt.Sprintf("reset handler %d of processor %s", len(app.resetHandlers[name])+1, name)
	app.resetHandlers[name] = append(app.resetHandlers[name], &resetHandler{
		processor: name,
		label:     label,
		invoke:    h,
	})
	return nil
}

// resetHandler is one handler of one processor's resets.
type resetHandler struct {
	processor string
	label     string
	invoke    func(context.Context, ResetContext) error
}

// call runs the handler and turns a panic into an error so the reset stops with this handler's
// failure, never a crashed control queue.
func (h *resetHandler) call(ctx context.Context, reset ResetContext) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	return h.invoke(ctx, reset)
}

// checkResetStartup refuses a reset handler whose processor has no event handler: no reset of it
// could ever run.
func (a *App) checkResetStartup() error {
	names := make([]string, 0, len(a.resetHandlers))
	for name := range a.resetHandlers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if len(a.eventHandlers[name]) == 0 {
			// TODO(spec-gap): SG-51 (the reference accepts a reset handler whose processor never
			// runs; it is dead there).
			return fmt.Errorf("snodra: the reset handler for processor %s needs an event handler in that processor", name)
		}
	}
	return nil
}

// resetHandlersFor is the reset handlers of processor, in registration order.
func (a *App) resetHandlersFor(processor string) []*resetHandler {
	return a.resetHandlers[processor]
}

// resetPrepare takes the processor's lock for the operation, stops the consumer and leaves the
// subscription; the same operation again renews the lease. See
// https://snodra.damoreira.com/docs/library-authors/protocol/ (Processor control).
func (c *controlPlane) resetPrepare(ctx context.Context, req *hubpb.ControlRequest, prepare *hubpb.ResetPrepare) *hubpb.ControlReply {
	a := c.app
	op := strings.TrimSpace(prepare.GetOperationId())
	lease := time.Duration(prepare.GetLeaseMs()) * time.Millisecond
	if op == "" || lease < resetMinLease {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
			"a prepare needs an operation id and a lease of at least 1000 ms")
	}
	if refusal := c.resetCapability(req); refusal != nil {
		return refusal
	}
	p := a.processorByName(req.GetProcessor())
	now := a.now()
	run := p.run
	if run.hasEnded(op, now) {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED,
			"operation "+op+" already finished on this instance")
	}
	control := a.cfg.store.Control()
	if held := run.heldOperation(); held != nil && held.id == op {
		extended, err := control.Extend(ctx, p.name, op, now.Add(lease), now)
		if err != nil {
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
		}
		if !extended {
			run.clearHeld()
			run.rememberEnded(op, lease, now)
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED,
				"lock lost: operation "+op+" no longer holds the lock of processor "+p.name)
		}
		run.updateHeldLease(op, lease)
		if err := a.stopForReset(ctx, p); err != nil {
			// TODO(spec-gap): SG-50 (a prepare that cannot stop within its bound refuses where the
			// reference answers OK with the reader possibly still alive).
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
				"the subscription did not stop: "+err.Error())
		}
		return controlOK(req, "lease renewed")
	}
	outcome, err := control.Lock(ctx, p.name, op, now.Add(lease), now)
	if err != nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
	}
	if !outcome.Taken {
		holder := outcome.Holder
		if holder.ID != "" {
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_LOCKED,
				fmt.Sprintf("operation %s holds the lock of processor %s until %s",
					holder.ID, p.name, holder.LeaseUntil.UTC().Format(time.RFC3339)))
		}
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_LOCKED,
			"another operation holds the lock of processor "+p.name)
	}
	run.setHeld(op, lease)
	if err := a.stopForReset(ctx, p); err != nil {
		// TODO(spec-gap): SG-50 (as for a renewal that cannot stop).
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
			"the subscription did not stop: "+err.Error())
	}
	a.log.Info("the processor stopped for a reset", "processor", p.name, "operation", op,
		"subscription", p.subscription.Name)
	return controlOK(req, "stopped, left the subscription "+p.subscription.Name)
}

// resetExecute plans and runs one reset of a processor this instance prepared: the event store
// plans the target, nobody may read the subscription, the reset handlers run when anything
// replays, the processor's markers in the ranges are deleted, the ranges are stored and the
// positions move. A refusal after the reset handlers says what did or did not move.
func (c *controlPlane) resetExecute(ctx context.Context, req *hubpb.ControlRequest, execute *hubpb.ResetExecute) *hubpb.ControlReply {
	a := c.app
	op := strings.TrimSpace(execute.GetOperationId())
	if op == "" {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "a reset execute needs an operation id")
	}
	if refusal := c.resetCapability(req); refusal != nil {
		return refusal
	}
	p := a.processorByName(req.GetProcessor())
	held := p.run.heldOperation()
	if held == nil || held.id != op {
		return c.notPrepared(ctx, req, p, op)
	}
	target, refusal := resetTarget(req, execute.GetTarget())
	if refusal != nil {
		return refusal
	}
	p.run.setExecuting(op)
	defer p.run.setExecuting("")
	deadline := a.now().Add(a.cfg.timings.resetExecuteLimit)
	stopExtension := c.extendResetLease(ctx, p, op, held.lease, deadline)
	defer stopExtension()

	changed := &resetChange{}
	reply := c.runReset(ctx, req, p, op, held.lease, target, deadline, changed)
	// A whole lease for the finish, unless the lease was lost already.
	if a.now().Before(deadline) {
		now := a.now()
		_, _ = a.cfg.store.Control().Extend(ctx, p.name, op, now.Add(held.lease), now)
	}
	return reply
}

// extendResetLease renews the operation's lease while its execute runs, never past the execute's
// deadline (a handler that outlives it lets the lease lapse so another instance can recover); the
// returned function stops it.
func (c *controlPlane) extendResetLease(ctx context.Context, p *processor, op string, lease time.Duration, deadline time.Time) func() {
	period := resetLeaseExtension
	if third := lease / 3; third < period {
		period = third
	}
	if period < 50*time.Millisecond {
		period = 50 * time.Millisecond
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := c.app.now()
				if !now.Before(deadline) {
					return // the execute outlived its limit: let the lease lapse
				}
				if _, err := c.app.cfg.store.Control().Extend(ctx, p.name, op, now.Add(lease), now); err != nil {
					return
				}
			}
		}
	}()
	return func() { close(done) }
}

// resetChange is what an execute changed before it stopped.
type resetChange struct {
	handlers int
	markers  uint64
	ranges   bool
}

// runReset is the execute's work, answering with the reset's outcome or with a refusal that
// names what changed.
func (c *controlPlane) runReset(ctx context.Context, req *hubpb.ControlRequest, p *processor, op string, lease time.Duration, target ResetTarget, deadline time.Time, changed *resetChange) *hubpb.ControlReply {
	a := c.app
	admin, err := a.cfg.eventStore.Admin(p.subscription)
	if err != nil {
		return c.resetStoreRefusal(req, err, changed)
	}
	plan, err := admin.Plan(ctx, target)
	if err != nil {
		return c.resetStoreRefusal(req, err, changed)
	}
	members, err := admin.Members(ctx)
	if err != nil {
		return c.resetStoreRefusal(req, err, changed)
	}
	if len(members) > 0 {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE,
			fmt.Sprintf("subscription %s still has %d subscriber(s): %s",
				p.subscription.Name, len(members), strings.Join(members, ", ")))
	}
	state, err := a.cfg.store.Control().Get(ctx, p.name)
	if err != nil {
		return c.resetStoreRefusal(req, err, changed)
	}
	open := state.Replay
	compare := func(segment, left, right string) int {
		return a.cfg.eventStore.Compare(segment, left, right)
	}
	ranges := replayingRanges(plan, target, open, compare)
	resetContext := ResetContext{
		Processor:   p.name,
		OperationID: op,
		Target:      target,
		Ranges:      append([]ReplayRange(nil), ranges...),
	}
	if len(ranges) > 0 {
		for _, handler := range a.resetHandlersFor(p.name) {
			changed.handlers++
			if err := handler.call(ctx, resetContext); err != nil {
				a.log.Warn("a reset handler failed; the reset stops before the markers",
					"processor", p.name, "operation", op, "handler", handler.label, "error", err)
				return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_RESET_HANDLER_FAILED,
					handler.label+" failed: "+err.Error(), changed)
			}
		}
	}
	if lost := c.resetLeaseLost(ctx, p, op, lease, deadline); lost != "" {
		return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, lost, changed)
	}
	if len(ranges) > 0 {
		deleted, err := a.cfg.store.Processed().DeleteMarkers(ctx, p.name, ranges)
		if deleted > 0 {
			changed.markers = uint64(deleted)
		}
		if err != nil {
			return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
				"deleting the replay range's markers: "+err.Error(), changed)
		}
		merged := mergeReplayRanges(open, ranges, compare)
		now := a.now()
		if err := a.cfg.store.Control().SetReplay(ctx, p.name, merged, now); err != nil {
			return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
				"storing the replay ranges: "+err.Error(), changed)
		}
		changed.ranges = true
		p.run.setReplay(merged)
	}
	if lost := c.resetLeaseLost(ctx, p, op, lease, deadline); lost != "" {
		return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, lost, changed)
	}
	if err := admin.Apply(ctx, plan); err != nil {
		return c.resetStoreRefusal(req, err, changed)
	}
	a.log.Info("a processor was reset", "processor", p.name, "operation", op,
		"ranges", len(ranges), "markers", changed.markers, "handlers", changed.handlers)
	out := &hubpb.ResetOutcome{
		MarkersDeleted: changed.markers,
		ResetHandlers:  uint32(changed.handlers),
	}
	out.Ranges = wireRanges(ranges)
	return &hubpb.ControlReply{
		ControlId: req.GetControlId(),
		Result:    &hubpb.ControlReply_Reset_{Reset_: out},
	}
}

// resetLeaseLost extends the lease before a step and returns why it cannot go on, empty when it
// can.
func (c *controlPlane) resetLeaseLost(ctx context.Context, p *processor, op string, lease time.Duration, deadline time.Time) string {
	now := c.app.now()
	if !now.Before(deadline) {
		return fmt.Sprintf("lease lost: the reset ran longer than %s", c.app.cfg.timings.resetExecuteLimit)
	}
	extended, err := c.app.cfg.store.Control().Extend(ctx, p.name, op, now.Add(lease), now)
	if err != nil {
		return "lease lost: cannot extend it: " + err.Error()
	}
	if !extended {
		return "lease lost: operation " + op + " no longer holds the lock of processor " + p.name
	}
	return ""
}

// resetStoreRefusal is the refusal of what the event store refused or failed at, naming what the
// execute changed before it.
func (c *controlPlane) resetStoreRefusal(req *hubpb.ControlRequest, err error, changed *resetChange) *hubpb.ControlReply {
	switch {
	case errors.Is(err, ErrUnsupported):
		var unsupported *UnsupportedError
		capability := ""
		if errors.As(err, &unsupported) {
			capability = unsupported.Capability.String()
		}
		message := "the event store lacks the capability replay: the processor cannot be reset"
		if capability != "" {
			message = "the event store lacks the capability " + capability + ": the processor cannot be reset"
		}
		return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_UNSUPPORTED, message, changed)
	case errors.Is(err, ErrInvalid):
		return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, err.Error(), changed)
	case errors.Is(err, ErrSubscribersActive):
		return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE, err.Error(), changed)
	default:
		c.app.log.Warn("a reset's event-store call failed", "processor", req.GetProcessor(), "error", err)
		return c.resetChangedRefusal(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error(), changed)
	}
}

// resetChangedRefusal is a refusal that adds what the execute changed when anything did.
func (c *controlPlane) resetChangedRefusal(req *hubpb.ControlRequest, code hubpb.ControlRefusal, cause string, changed *resetChange) *hubpb.ControlReply {
	if changed.handlers == 0 && changed.markers == 0 && !changed.ranges {
		return controlRefused(req, code, cause)
	}
	deleted := "no markers deleted"
	if changed.markers > 0 {
		deleted = fmt.Sprintf("%d markers deleted", changed.markers)
	}
	written := ""
	if changed.ranges {
		written = ", replay ranges written"
	}
	message := fmt.Sprintf("%s; reset handlers ran (%d), %s%s, positions not moved: retry the reset",
		cause, changed.handlers, deleted, written)
	return controlRefused(req, code, message)
}

// resetFinish releases the operation's lock and consumes again, still paused if it was. A finish
// for nothing held is OK; another operation's lock is NOT_PREPARED.
func (c *controlPlane) resetFinish(ctx context.Context, req *hubpb.ControlRequest, finish *hubpb.ResetFinish) *hubpb.ControlReply {
	a := c.app
	op := strings.TrimSpace(finish.GetOperationId())
	if op == "" {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "a reset finish needs an operation id")
	}
	p := a.processorByName(req.GetProcessor())
	now := a.now()
	control := a.cfg.store.Control()
	if held := p.run.heldOperation(); held != nil && held.id == op {
		p.run.rememberEnded(op, held.lease, now)
		released, err := control.Release(ctx, p.name, op, now)
		if err != nil {
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
		}
		p.run.clearHeld()
		if !released {
			// The lease lapsed and another operation took the lock before this finish.
			if state, err := control.Get(ctx, p.name); err == nil && state.Operation != nil &&
				state.Operation.HoldsAt(now) && state.Operation.ID != op {
				p.run.setLockState(state.Operation.ID, true)
				_ = a.stopForReset(ctx, p)
				return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED,
					fmt.Sprintf("operation %s holds the lock of processor %s, not %s",
						state.Operation.ID, p.name, op))
			}
		}
		c.settleAfterFinish(ctx, p, now)
		a.log.Info("a processor's reset was finished", "processor", p.name, "operation", op,
			"aborted", finish.GetAborted())
		return controlOK(req, "released, consuming again")
	}
	state, err := control.Get(ctx, p.name)
	if err != nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
	}
	switch {
	case state.Operation == nil || !state.Operation.HoldsAt(now):
		if state.Operation != nil && state.Operation.ID == op {
			_, _ = control.Release(ctx, p.name, op, now)
		}
		p.run.rememberEnded(op, 0, now)
		c.settleAfterFinish(ctx, p, now)
		return controlOK(req, "no operation holds the lock")
	case state.Operation.ID == op:
		released, err := control.Release(ctx, p.name, op, now)
		if err != nil {
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
		}
		p.run.clearHeld()
		p.run.rememberEnded(op, 0, now)
		if !released {
			if current, err := control.Get(ctx, p.name); err == nil && current.Operation != nil &&
				current.Operation.HoldsAt(now) && current.Operation.ID != op {
				p.run.setLockState(current.Operation.ID, true)
				_ = a.stopForReset(ctx, p)
				return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED,
					fmt.Sprintf("operation %s holds the lock of processor %s, not %s",
						current.Operation.ID, p.name, op))
			}
		}
		c.settleAfterFinish(ctx, p, now)
		return controlOK(req, "released, consuming again")
	default:
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED,
			fmt.Sprintf("operation %s holds the lock of processor %s, not %s",
				state.Operation.ID, p.name, op))
	}
}

// settleAfterFinish records the lock state after a finish and starts or keeps the consumer
// stopped accordingly.
func (c *controlPlane) settleAfterFinish(ctx context.Context, p *processor, now time.Time) {
	state, err := c.app.cfg.store.Control().Get(ctx, p.name)
	if err != nil {
		// The store cannot prove the lock gone: keep the consumer out until a poll can.
		p.run.setLockState("", true)
		_ = c.app.stopForReset(ctx, p)
		return
	}
	if state.Operation != nil && state.Operation.HoldsAt(now) {
		p.run.setLockState(state.Operation.ID, true)
		_ = c.app.stopForReset(ctx, p)
		return
	}
	p.run.setLockState("", false)
	p.run.signalWake()
}

// stopForReset waits for the consumer to leave its subscription, at most the reset stop limit.
func (a *App) stopForReset(ctx context.Context, p *processor) error {
	limit := a.cfg.timings.resetStopLimit
	if limit < time.Millisecond {
		limit = time.Millisecond
	}
	stopCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	return p.run.stopReading(stopCtx)
}

// notPrepared is the NOT_PREPARED refusal, naming the operation that holds the lock when one
// does.
func (c *controlPlane) notPrepared(ctx context.Context, req *hubpb.ControlRequest, p *processor, op string) *hubpb.ControlReply {
	holder := ""
	if state, err := c.app.cfg.store.Control().Get(ctx, p.name); err == nil &&
		state.Operation != nil && state.Operation.HoldsAt(c.app.now()) {
		holder = fmt.Sprintf("; operation %s holds the lock", state.Operation.ID)
	}
	return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_PREPARED,
		"operation "+op+" is not prepared on this instance"+holder)
}

// resetCapability refuses a reset the event store cannot do before anything changes.
func (c *controlPlane) resetCapability(req *hubpb.ControlRequest) *hubpb.ControlReply {
	if c.app.cfg.store == nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
			"the instance has no message store for the processor's lock")
	}
	if c.app.cfg.eventStore == nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
			"the instance has no event store to reset")
	}
	if !Supports(c.app.cfg.eventStore, CapabilityReplay) {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_UNSUPPORTED,
			"the event store "+c.app.cfg.eventStore.Name()+" lacks the capability replay: processor "+
				req.GetProcessor()+" cannot be reset")
	}
	return nil
}

// resetTarget reads the hub's reset target.
func resetTarget(req *hubpb.ControlRequest, target *hubpb.ResetTarget) (ResetTarget, *hubpb.ControlReply) {
	switch to := target.GetTo().(type) {
	case *hubpb.ResetTarget_Earliest:
		return ResetEarliest{}, nil
	case *hubpb.ResetTarget_Latest:
		return ResetLatest{}, nil
	case *hubpb.ResetTarget_TimestampMs:
		return ResetTimestamp{TimestampMs: to.TimestampMs}, nil
	case *hubpb.ResetTarget_Positions:
		listed := to.Positions.GetPositions()
		if len(listed) == 0 {
			return nil, controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
				"the positions target lists no segment")
		}
		seen := make(map[string]bool, len(listed))
		positions := make([]SegmentPosition, 0, len(listed))
		for _, position := range listed {
			if seen[position.GetSegment()] {
				return nil, controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
					"the positions target names a segment twice")
			}
			seen[position.GetSegment()] = true
			positions = append(positions, SegmentPosition{
				Segment:  position.GetSegment(),
				Position: position.GetPosition(),
			})
		}
		return ResetPositions{Positions: positions}, nil
	default:
		return nil, controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
			"the reset has no target")
	}
}

// replayingRanges is the replay ranges of a reset whose plan is moves over the still-open ranges:
// only the moves that replay (From before Until), each with the farthest end of a still-open range
// of its segment (that range's records past Until were not replayed yet and their markers are
// gone). A latest reset replays nothing.
func replayingRanges(moves []ReplayRange, target ResetTarget, open []ReplayRange, compare func(segment, left, right string) int) []ReplayRange {
	if _, latest := target.(ResetLatest); latest {
		return nil
	}
	ranges := make([]ReplayRange, 0, len(moves))
	for _, move := range moves {
		if move.Until == "" || compare(move.Segment, move.From, move.Until) >= 0 {
			continue
		}
		replay := move
		for _, still := range open {
			if still.Source != move.Source || still.Segment != move.Segment || still.Until == "" {
				continue
			}
			if compare(move.Segment, replay.Until, still.Until) < 0 {
				replay.Until = still.Until
			}
		}
		ranges = append(ranges, replay)
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Source != ranges[j].Source {
			return ranges[i].Source < ranges[j].Source
		}
		return SegmentOrder(ranges[i].Segment, ranges[j].Segment) < 0
	})
	return ranges
}

// mergeReplayRanges is the open ranges after a reset: those of the segments that got a new range
// are replaced by it; sorted by source, segment and From.
func mergeReplayRanges(open, replaying []ReplayRange, compare func(segment, left, right string) int) []ReplayRange {
	merged := make([]ReplayRange, 0, len(open)+len(replaying))
	for _, still := range open {
		replaced := false
		for _, replay := range replaying {
			if still.Source == replay.Source && still.Segment == replay.Segment {
				replaced = true
				break
			}
		}
		if !replaced {
			merged = append(merged, still)
		}
	}
	merged = append(merged, replaying...)
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Source != merged[j].Source {
			return merged[i].Source < merged[j].Source
		}
		if c := SegmentOrder(merged[i].Segment, merged[j].Segment); c != 0 {
			return c < 0
		}
		return compare(merged[i].Segment, merged[i].From, merged[j].From) < 0
	})
	return merged
}

// wireRanges is the reply's ranges.
func wireRanges(ranges []ReplayRange) []*hubpb.ReplayRange {
	out := make([]*hubpb.ReplayRange, 0, len(ranges))
	for _, replay := range ranges {
		out = append(out, &hubpb.ReplayRange{Segment: replay.Segment, From: replay.From, Until: replay.Until})
	}
	return out
}
