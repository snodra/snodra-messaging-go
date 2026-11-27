package snodra

import (
	"context"
	"errors"
	"time"
)

// pollProcessorControl reads every processor's control state once at start and then every
// controlPoll: a pause or resume persisted by any instance (or by the hub's control request)
// stops or restarts consumption, and a reset lock it reads stops the consumer or lets it join
// again. The state is kept in the message store, so an instance that missed a control request
// converges here.
// See https://snodra.damoreira.com/docs/library-authors/protocol/ (Processor control).
func (a *App) pollProcessorControl(ctx context.Context) error {
	interval := a.cfg.timings.controlPoll
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		a.readProcessorControl(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// readProcessorControl is one pass over every processor's control state. A failed read keeps the
// processor as it is (and a consumer that has not started waits for the state); a store without
// the control port starts its processors unpaused.
func (a *App) readProcessorControl(ctx context.Context) {
	if a.cfg.store == nil {
		return
	}
	control := a.cfg.store.Control()
	now := a.now()
	for _, p := range a.processors {
		if p.run.controlIsUnsupported() {
			continue
		}
		readAt := p.run.changeCount()
		state, err := control.Get(ctx, p.name)
		if err != nil {
			if errors.Is(err, ErrUnsupported) {
				// TODO(spec-gap): SG-49 (a store without the control port runs unpaused).
				p.run.markControlUnsupported()
				a.log.Debug("the message store has no processor control port; the processor runs unpaused",
					"processor", p.name)
				continue
			}
			if ctx.Err() == nil {
				a.log.Warn("could not read a processor's control state; keeping the processor as it is",
					"processor", p.name, "error", err)
			}
			continue
		}
		p.run.applyControl(ctx, state, readAt, now)
		a.reconcileResetHold(ctx, p, state, now)
		a.closePassedReplayRanges(ctx, p, now)
		if p.run.holdsLock() {
			if err := a.stopForReset(ctx, p); err != nil && ctx.Err() == nil {
				a.log.Warn("the processor did not leave its subscription for the reset lock",
					"processor", p.name, "error", err)
			}
		} else {
			p.run.signalWake()
		}
	}
}

// reconcileResetHold lets go of the operation this instance held when another instance released
// it, and releases a lease that passed without a renewal, so the consumer joins again.
func (a *App) reconcileResetHold(ctx context.Context, p *processor, state ProcessorControlState, now time.Time) {
	held := p.run.heldOperation()
	if held == nil {
		return
	}
	if state.Operation == nil || state.Operation.ID != held.id {
		p.run.clearHeld()
		p.run.rememberEnded(held.id, held.lease, now)
		a.log.Info("the processor's reset operation ended elsewhere; consuming again",
			"processor", p.name, "operation", held.id)
		return
	}
	if state.Operation.HoldsAt(now) || p.run.isExecuting(held.id) {
		return
	}
	changed, err := a.cfg.store.Control().Expire(ctx, p.name, held.id, now)
	if err != nil {
		if ctx.Err() == nil {
			a.log.Warn("could not expire a passed reset lease", "processor", p.name, "error", err)
		}
		return
	}
	if !changed {
		return // renewed meanwhile: the next poll reads it
	}
	p.run.clearHeld()
	p.run.rememberEnded(held.id, held.lease, now)
	p.run.setLockState("", false)
	a.log.Warn("the lease of a reset operation passed without a renewal; released it, consuming again",
		"processor", p.name, "operation", held.id)
}

// closePassedReplayRanges removes the replay ranges this instance's subscription has reached the
// end of: the committed position (the next one while nothing is committed) is at or past Until,
// in the event store's order. A range of a segment this instance does not hold is left to the one
// that does.
func (a *App) closePassedReplayRanges(ctx context.Context, p *processor, now time.Time) {
	ranges := p.run.replayRanges()
	if len(ranges) == 0 {
		return
	}
	segments := p.run.segments()
	if len(segments) == 0 {
		return
	}
	for _, replay := range ranges {
		if replay.Source != p.subscription.Source || replay.Until == "" {
			continue
		}
		progress := ""
		for _, segment := range segments {
			if segment.Segment != replay.Segment {
				continue
			}
			progress = segment.Committed
			if progress == "" {
				progress = segment.Position
			}
			break
		}
		if progress == "" || a.cfg.eventStore.Compare(replay.Segment, progress, replay.Until) < 0 {
			continue
		}
		removed, err := a.cfg.store.Control().RemoveRange(ctx, p.name, replay, now)
		if err != nil {
			if ctx.Err() == nil {
				a.log.Warn("could not close a replay range", "processor", p.name, "segment", replay.Segment, "error", err)
			}
			continue
		}
		if removed {
			p.run.removeReplay(replay)
			a.log.Info("the processor replayed a segment to its range's end",
				"processor", p.name, "segment", replay.Segment, "source", replay.Source, "until", replay.Until)
		}
	}
}
