package snodra

import (
	"context"
	"sync"
	"time"
)

// processorRuntime is one processor's in-memory state: what its status reports, the pause it
// applies to its subscription, and the control state it last read. mu is held only for memory;
// subMu is held across a subscription's use (attach, detach and Close, Pause, Resume, Segments)
// so a closed subscription is never called.
type processorRuntime struct {
	mu sync.Mutex
	// subMu serializes the subscription's life against Pause and Resume and the status's
	// Segments read: a subscription that was detached and closed is never called again.
	subMu sync.Mutex
	// started is true while the processor's consumer loop runs; wasStarted stays true once it
	// ran, so the status still reports a stopped processor.
	started    bool
	wasStarted bool
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
	// changes counts the control-state changes applied here (a control action, or a poll's read
	// it applied), so a poll whose read predates one of them is left to the next poll.
	changes uint64
	// operationID is the reset operation holding the processor's lock, as the control poller
	// last read it; empty when none.
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
	r.wasStarted = true
	r.mu.Unlock()
}

// markStopped is called when the consumer loop returns (shutdown).
func (r *processorRuntime) markStopped() {
	r.subMu.Lock()
	r.mu.Lock()
	r.started = false
	r.assigned = false
	r.rebalancing = false
	r.sub = nil
	r.mu.Unlock()
	r.subMu.Unlock()
}

// hasStarted reports whether the consumer loop ran at least once.
func (r *processorRuntime) hasStarted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wasStarted
}

// operation is the reset operation holding the processor's lock, empty when none.
func (r *processorRuntime) operation() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.operationID
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
	r.subMu.Lock()
	defer r.subMu.Unlock()
	r.mu.Lock()
	r.sub = sub
	paused := r.paused
	r.mu.Unlock()
	if paused {
		sub.Pause()
	}
}

// detachAndClose forgets sub and closes it under the subscription lock: a pause, a resume or a
// status read waits until the close is over and then sees no subscription, never a closed one.
func (r *processorRuntime) detachAndClose(sub EventSubscription, closeSub func()) {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	r.mu.Lock()
	if r.sub == sub {
		r.sub = nil
	}
	r.mu.Unlock()
	closeSub()
}

// segments is the subscription's assignment, read under the subscription lock so a concurrent
// close is never raced. It is nil when nothing is being read.
func (r *processorRuntime) segments() []SegmentStatus {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	r.mu.Lock()
	sub, started := r.sub, r.started
	r.mu.Unlock()
	if sub == nil || !started {
		return nil
	}
	return sub.Segments()
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
	r.subMu.Lock()
	defer r.subMu.Unlock()
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
	r.subMu.Lock()
	defer r.subMu.Unlock()
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
		operationID:  r.operationID,
		replay:       append([]ReplayRange(nil), r.replay...),
	}
}

// takeSubscriptionError asks the subscription for the trouble it recovered from by itself and
// records it as the latest failure.
func (r *processorRuntime) takeSubscriptionError(now time.Time) {
	r.subMu.Lock()
	r.mu.Lock()
	sub := r.sub
	r.mu.Unlock()
	message := ""
	if sub != nil {
		message = sub.TakeError()
	}
	r.subMu.Unlock()
	if message != "" {
		r.markFailed(errText(message), now)
	}
}

// errText is a plain error from text (the subscription's recovered trouble).
type errText string

func (e errText) Error() string { return string(e) }
