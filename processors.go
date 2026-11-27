package snodra

import (
	"context"
	"sync"
	"time"
)

// processorRuntime is one processor's in-memory state: what its status reports, the pause it
// applies to its subscription, and the control state it last read. The mutex is held only for
// memory; nothing here calls the message store or the event subscription while holding it.
type processorRuntime struct {
	mu sync.Mutex
	// started is true while the processor's consumer loop runs.
	started bool
	// paused stops delivery without leaving the subscription.
	paused bool
	// assigned is set by the first Assigned; rebalancing is set between a Revoked and the
	// Assigned that closes the change.
	assigned    bool
	rebalancing bool
	// failingSince is when the last exhausted record or reader error happened.
	failingSince time.Time
	lastError    string
	lastErrorAt  time.Time
	// sub is the subscription being read; nil between subscriptions.
	sub EventSubscription
	// controlKnown is set once the processor's control state was read (or written) once: until
	// then the consumer does not subscribe, so a persisted pause is never missed.
	controlKnown bool
	// controlUnsupported is set when the message store has no control port: the processor runs
	// and its pause cannot persist.
	controlUnsupported bool
	controlReady       chan struct{}
	// changes counts the changes this instance made to the control state, so a poll that read
	// the store before a change does not apply its stale copy.
	changes uint64
	// operationID is the reset operation holding the processor's lock; reset support fills it.
	operationID string
	// replay is the open replay ranges of the processor's source.
	replay []ReplayRange
}

// processorSnapshot is a processor's runtime state at one moment.
type processorSnapshot struct {
	started      bool
	paused       bool
	assigned     bool
	rebalancing  bool
	failingSince time.Time
	lastError    string
	lastErrorAt  time.Time
	sub          EventSubscription
	operationID  string
	replay       []ReplayRange
}

func newProcessorRuntime() *processorRuntime {
	return &processorRuntime{controlReady: make(chan struct{})}
}

// markStarted is called when the processor's consumer loop begins.
func (r *processorRuntime) markStarted() {
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
}

// markStopped is called when the consumer loop returns (shutdown).
func (r *processorRuntime) markStopped() {
	r.mu.Lock()
	r.started = false
	r.assigned = false
	r.rebalancing = false
	r.sub = nil
	r.mu.Unlock()
}

// markAssigned ends a first assignment or a reassignment.
func (r *processorRuntime) markAssigned() {
	r.mu.Lock()
	r.assigned = true
	r.rebalancing = false
	r.mu.Unlock()
}

// markRevoked begins a reassignment.
func (r *processorRuntime) markRevoked() {
	r.mu.Lock()
	r.rebalancing = true
	r.mu.Unlock()
}

// markFailed is the latest failure: it becomes last_error, and the processor is FAILING for the
// failing window.
func (r *processorRuntime) markFailed(err error, now time.Time) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.lastError = err.Error()
	r.lastErrorAt = now
	r.failingSince = now
	r.mu.Unlock()
}

// attach makes sub the subscription being read and applies the pause the processor already has.
func (r *processorRuntime) attach(sub EventSubscription) {
	r.mu.Lock()
	r.sub = sub
	paused := r.paused
	r.mu.Unlock()
	if paused {
		sub.Pause()
	}
}

// detach forgets sub, when it is still the current one.
func (r *processorRuntime) detach(sub EventSubscription) {
	r.mu.Lock()
	if r.sub == sub {
		r.sub = nil
	}
	r.mu.Unlock()
}

// pausedNow reports whether delivery is paused.
func (r *processorRuntime) pausedNow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.paused
}

// waitControl waits until the processor's control state was read once (or the store has no
// control port), and false when ctx ends first.
func (r *processorRuntime) waitControl(ctx context.Context) bool {
	r.mu.Lock()
	ready := r.controlReady
	known := r.controlKnown
	r.mu.Unlock()
	if known {
		return true
	}
	select {
	case <-ready:
		return true
	case <-ctx.Done():
		return false
	}
}

// markControlKnownLocked closes the control-ready channel once.
func (r *processorRuntime) markControlKnownLocked() {
	if r.controlKnown {
		return
	}
	r.controlKnown = true
	close(r.controlReady)
}

// markControlUnsupported records that the message store has no control port: a processor of such
// a store starts unpaused and its pauses cannot persist.
func (r *processorRuntime) markControlUnsupported() {
	r.mu.Lock()
	r.controlUnsupported = true
	r.markControlKnownLocked()
	r.mu.Unlock()
}

// controlIsUnsupported reports whether the store has no control port.
func (r *processorRuntime) controlIsUnsupported() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.controlUnsupported
}

// changeCount is how many changes this instance made to the control state.
func (r *processorRuntime) changeCount() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.changes
}

// applyPaused applies a pause or resume made on this instance (a control request): the store was
// written already, so the state is known.
func (r *processorRuntime) applyPaused(paused bool) {
	r.mu.Lock()
	changed := r.paused != paused
	r.paused = paused
	r.changes++
	r.markControlKnownLocked()
	sub := r.sub
	r.mu.Unlock()
	if changed && sub != nil {
		if paused {
			sub.Pause()
		} else {
			sub.Resume()
		}
	}
}

// applyControl applies a control state read from the store at readAt. A state read before a
// change made here since is left to the next poll, which reads what that change wrote; the read
// still proves the store reachable, so the consumer may start.
func (r *processorRuntime) applyControl(state ProcessorControlState, readAt uint64, now time.Time) {
	r.mu.Lock()
	if r.controlKnown && r.changes != readAt {
		r.markControlKnownLocked()
		r.mu.Unlock()
		return
	}
	changed := r.paused != state.Paused
	r.paused = state.Paused
	r.replay = append([]ReplayRange(nil), state.Replay...)
	// The reset lock's effects (leaving the subscription, replaying) arrive with the reset
	// support; the status names the operation that holds it meanwhile.
	r.operationID = ""
	if state.Operation != nil && state.Operation.HoldsAt(now) {
		r.operationID = state.Operation.ID
	}
	r.changes++
	r.markControlKnownLocked()
	sub := r.sub
	r.mu.Unlock()
	if changed && sub != nil {
		if state.Paused {
			sub.Pause()
		} else {
			sub.Resume()
		}
	}
}

// snapshot is the processor's runtime state now.
func (r *processorRuntime) snapshot() processorSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return processorSnapshot{
		started:      r.started,
		paused:       r.paused,
		assigned:     r.assigned,
		rebalancing:  r.rebalancing,
		failingSince: r.failingSince,
		lastError:    r.lastError,
		lastErrorAt:  r.lastErrorAt,
		sub:          r.sub,
		operationID:  r.operationID,
		replay:       append([]ReplayRange(nil), r.replay...),
	}
}

// takeSubscriptionError asks the subscription for the trouble it recovered from by itself and
// records it as the latest failure.
func (r *processorRuntime) takeSubscriptionError(now time.Time) {
	r.mu.Lock()
	sub := r.sub
	r.mu.Unlock()
	if sub == nil {
		return
	}
	if message := sub.TakeError(); message != "" {
		r.markFailed(errText(message), now)
	}
}

// errText is a plain error from text (the subscription's recovered trouble).
type errText string

func (e errText) Error() string { return string(e) }
