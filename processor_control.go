package snodra

import (
	"context"
	"errors"
	"time"
)

// pollProcessorControl reads every processor's control state once at start and then every
// controlPoll: a pause or resume persisted by any instance (or by the hub's control request)
// stops or restarts consumption. The state is kept in the message store, so an instance that
// missed a control request converges here.
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
		p.run.applyControl(state, readAt, now)
	}
}
