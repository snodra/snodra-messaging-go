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
	// lockHolds is true while an unexpired reset operation holds the lock: the consumer leaves
	// or stays out of the subscription until it is released or its lease passes.
	lockHolds bool
	// replay is the open replay ranges of the processor's source.
	replay []ReplayRange
	// held is the reset operation this instance prepared; nil when none.
	held *heldReset
	// executing is the operation whose execute runs now; executingLost says its lease was lost
	// while it ran, so it must not renew or move anything more.
	executing     string
	executingLost bool
	// finished remembers the operations that ended on this instance, with the time each is
	// forgotten (a late renewal is NOT_PREPARED).
	finished map[string]time.Time
	// readDone is closed when the consumer finished its current subscription attempt; subCancel
	// stops the subscription it is reading (a reset prepare leaves the subscription).
	readDone  chan struct{}
	subCancel context.CancelFunc
	// wake tells the consumer to look at the lock again.
	wake chan struct{}
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
	lockHolds    bool
	replay       []ReplayRange
}

func newProcessorRuntime() *processorRuntime {
	return &processorRuntime{
		controlReady: make(chan struct{}),
		finished:     map[string]time.Time{},
		wake:         make(chan struct{}, 1),
	}
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

// replayRanges is the open replay ranges of the processor's source.
func (r *processorRuntime) replayRanges() []ReplayRange {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ReplayRange(nil), r.replay...)
}

// setReplay replaces the open replay ranges.
func (r *processorRuntime) setReplay(ranges []ReplayRange) {
	r.mu.Lock()
	r.replay = append([]ReplayRange(nil), ranges...)
	r.changes++
	r.mu.Unlock()
}

// removeReplay drops one open replay range, when it is still there.
func (r *processorRuntime) removeReplay(replay ReplayRange) {
	r.mu.Lock()
	for i, open := range r.replay {
		if open == replay {
			r.replay = append(r.replay[:i], r.replay[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
}

// holdsLock reports whether an unexpired reset operation holds the processor's lock.
func (r *processorRuntime) holdsLock() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lockHolds
}

// signalWake tells the consumer to look at the lock and at the control state again.
func (r *processorRuntime) signalWake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// runnableNow reports whether no reset operation holds the lock now.
func (r *processorRuntime) runnableNow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.lockHolds
}

// waitRunnable waits until no reset operation holds the lock, and false when ctx ends first.
func (r *processorRuntime) waitRunnable(ctx context.Context) bool {
	for {
		r.mu.Lock()
		runnable := !r.lockHolds
		r.mu.Unlock()
		if runnable {
			return true
		}
		select {
		case <-r.wake:
		case <-ctx.Done():
			return false
		}
	}
}

// beginRead marks a subscription attempt in flight and returns the channel endRead closes.
func (r *processorRuntime) beginRead(cancel context.CancelFunc) chan struct{} {
	r.mu.Lock()
	done := make(chan struct{})
	r.readDone = done
	r.subCancel = cancel
	r.mu.Unlock()
	return done
}

// endRead closes a subscription attempt: the consumer has attached nothing (or closed what it
// attached), so a stop waiting for it may go on.
func (r *processorRuntime) endRead(done chan struct{}) {
	r.mu.Lock()
	if r.readDone == done {
		r.readDone = nil
		r.subCancel = nil
	}
	r.mu.Unlock()
	close(done)
}

// stopReading stops the subscription being read (or the attempt to build one) and waits until
// the consumer left it. It returns ctx's error when the consumer did not finish in time.
func (r *processorRuntime) stopReading(ctx context.Context) error {
	r.mu.Lock()
	cancel, done := r.subCancel, r.readDone
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	r.mu.Lock()
	changed := r.paused != paused
	r.paused = paused
	r.changes++
	r.markControlKnownLocked()
	r.mu.Unlock()
	if changed {
		r.pauseSubscription(paused)
	}
}

// pauseSubscription applies the pause to the subscription being read, if any. It takes the
// subscription lock, never the state lock across a subscription call.
func (r *processorRuntime) pauseSubscription(paused bool) {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	r.mu.Lock()
	sub := r.sub
	r.mu.Unlock()
	if sub == nil {
		return
	}
	if paused {
		sub.Pause()
	} else {
		sub.Resume()
	}
}

// applyControl applies a control state read from the store at readAt and reports whether it
// applied (a state read before a change made here since is left to the next poll, which reads
// what that change wrote) and the change count after its own apply. A poll's reconcile step uses
// that count so a reset write between the two invalidates the read too. While this instance
// executes a reset, the lock stays reported held even if the persisted lease lapsed: the
// execute must finish (and refuse) before the consumer can join.
func (r *processorRuntime) applyControl(ctx context.Context, state ProcessorControlState, readAt uint64, now time.Time) (applied bool, after uint64) {
	r.mu.Lock()
	if r.controlKnown && r.changes != readAt {
		r.markControlKnownLocked()
		after = r.changes
		r.mu.Unlock()
		return false, after
	}
	changed := r.paused != state.Paused
	r.paused = state.Paused
	r.replay = append([]ReplayRange(nil), state.Replay...)
	r.operationID = ""
	r.lockHolds = false
	if r.executing != "" {
		// The execute keeps the consumer out until it returns, whether or not the persisted lease
		// still names it (it can be cleared by a poll between the execute's check and this read).
		r.operationID = r.executing
		r.lockHolds = true
		if state.Operation == nil || state.Operation.ID != r.executing || !state.Operation.HoldsAt(now) {
			r.executingLost = true
		}
	} else if state.Operation != nil && state.Operation.HoldsAt(now) {
		r.operationID = state.Operation.ID
		r.lockHolds = true
	}
	r.changes++
	after = r.changes
	r.markControlKnownLocked()
	r.mu.Unlock()
	if changed {
		r.pauseSubscription(state.Paused)
	}
	return true, after
}

// heldReset is the reset operation this instance prepared.
type heldReset struct {
	id    string
	lease time.Duration
}

// setHeld records the operation this instance prepared and its lease.
func (r *processorRuntime) setHeld(id string, lease time.Duration) {
	r.mu.Lock()
	r.held = &heldReset{id: id, lease: lease}
	r.operationID = id
	r.lockHolds = true
	r.changes++
	r.mu.Unlock()
}

// heldOperation is the operation this instance prepared, nil when none.
func (r *processorRuntime) heldOperation() *heldReset {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held == nil {
		return nil
	}
	copied := *r.held
	return &copied
}

// updateHeldLease records a renewed lease for the operation held here; false when another
// operation is held now.
func (r *processorRuntime) updateHeldLease(id string, lease time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held == nil || r.held.id != id {
		return false
	}
	r.held.lease = lease
	r.changes++
	return true
}

// clearHeld forgets the operation held here and returns it, nil when none.
func (r *processorRuntime) clearHeld() *heldReset {
	r.mu.Lock()
	defer r.mu.Unlock()
	held := r.held
	r.held = nil
	r.changes++
	return held
}

// setLockState records what the control poller read: an unexpired operation holds the lock (ours
// or another instance's) and is named in the status.
func (r *processorRuntime) setLockState(operationID string, holds bool) {
	r.mu.Lock()
	r.operationID = operationID
	r.lockHolds = holds
	r.changes++
	r.mu.Unlock()
}

// setExecuting records the operation whose execute runs here.
func (r *processorRuntime) setExecuting(id string) {
	r.mu.Lock()
	r.executing = id
	r.executingLost = false
	if id != "" {
		// From here until the execute returns the consumer stays out, even if a poll clears the
		// prepared operation concurrently.
		r.operationID = id
		r.lockHolds = true
	}
	r.mu.Unlock()
}

// isExecuting reports whether id's execute runs here.
func (r *processorRuntime) isExecuting(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.executing == id
}

// markExecutingLost records that the execute of id lost its lease: it may not renew or move
// anything more, and the consumer stays out until the execute returns and a poll proves the lock
// gone.
func (r *processorRuntime) markExecutingLost(id string) {
	r.mu.Lock()
	if r.executing == id {
		r.executingLost = true
		r.changes++
	}
	r.mu.Unlock()
}

// executingIsLost reports whether the execute of id lost its lease.
func (r *processorRuntime) executingIsLost(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.executing == id && r.executingLost
}

// rememberEnded remembers an operation that ended on this instance for max(2*lease, 60s), at
// most a day; a late renewal is refused NOT_PREPARED while it is remembered.
func (r *processorRuntime) rememberEnded(id string, lease time.Duration, now time.Time) {
	if id == "" {
		return
	}
	keep := 2 * lease
	if keep < resetFinishedKept {
		keep = resetFinishedKept
	}
	if keep > resetFinishedKeptMax {
		keep = resetFinishedKeptMax
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for ended, until := range r.finished {
		if !until.After(now) {
			delete(r.finished, ended)
		}
	}
	r.finished[id] = now.Add(keep)
}

// hasEnded reports whether id ended on this instance and is still remembered.
func (r *processorRuntime) hasEnded(id string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.finished[id]
	if ok && !until.After(now) {
		delete(r.finished, id)
		return false
	}
	return ok
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
		lockHolds:    r.lockHolds,
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
