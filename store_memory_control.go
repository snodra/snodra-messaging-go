package snodra

import (
	"context"
	"time"
)

// memoryControl is MemoryMessageStore's processor-control store: one state per processor, the
// source of truth of its pause, reset lock and open replay ranges. Every operation is a write of
// its own, outside any transaction; the times are the caller's clock.
type memoryControl struct {
	store *MemoryMessageStore
}

// Get is the state of processor; the zero state (not paused, nothing held) when the store has
// none.
func (m memoryControl) Get(ctx context.Context, processor string) (ProcessorControlState, error) {
	if err := ctx.Err(); err != nil {
		return ProcessorControlState{}, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state, ok := m.store.data.control[processor]
	if !ok {
		return ProcessorControlState{Processor: processor}, nil
	}
	return copyControlState(*state), nil
}

// SetPaused pauses (by the control id, or "local") or resumes processor, idempotently. A resume
// clears PausedAt and PausedBy.
func (m memoryControl) SetPaused(ctx context.Context, processor string, paused bool, by string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state := m.upsertLocked(processor)
	state.Paused = paused
	if paused {
		state.PausedAt = now
		state.PausedBy = by
	} else {
		state.PausedAt = time.Time{}
		state.PausedBy = ""
	}
	state.UpdatedAt = now
	return nil
}

// Lock takes the lock of processor for operation until leaseUntil, atomically: when nothing holds
// it or the holder's lease has passed; a renewal (the operation holds it already) moves the lease
// only. The outcome names the holder when another holds it.
func (m memoryControl) Lock(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (LockOutcome, error) {
	if err := ctx.Err(); err != nil {
		return LockOutcome{}, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state := m.upsertLocked(processor)
	switch held := state.Operation; {
	case held != nil && held.ID == operation:
		held.LeaseUntil = leaseUntil
		state.UpdatedAt = now
		return LockOutcome{Taken: true}, nil
	case held != nil && held.HoldsAt(now):
		return LockOutcome{Holder: *held}, nil
	default:
		state.Operation = &ControlOperation{
			ID:         operation,
			Kind:       ResetKind,
			LeaseUntil: leaseUntil,
			PreparedAt: now,
		}
		state.UpdatedAt = now
		return LockOutcome{Taken: true}, nil
	}
}

// Extend moves the lease of operation; false when it does not hold the lock.
func (m memoryControl) Extend(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state, ok := m.store.data.control[processor]
	if !ok || state.Operation == nil || state.Operation.ID != operation {
		return false, nil
	}
	state.Operation.LeaseUntil = leaseUntil
	state.UpdatedAt = now
	return true, nil
}

// Release clears the lock when operation holds it (never another operation's); false when it did
// not.
func (m memoryControl) Release(ctx context.Context, processor, operation string, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state, ok := m.store.data.control[processor]
	if !ok || state.Operation == nil || state.Operation.ID != operation {
		return false, nil
	}
	state.Operation = nil
	state.UpdatedAt = now
	return true, nil
}

// Expire clears the lock when operation holds it and its lease has passed at now (a renewal that
// came meanwhile keeps it); false otherwise.
func (m memoryControl) Expire(ctx context.Context, processor, operation string, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state, ok := m.store.data.control[processor]
	if !ok || state.Operation == nil || state.Operation.ID != operation ||
		state.Operation.HoldsAt(now) {
		return false, nil
	}
	state.Operation = nil
	state.UpdatedAt = now
	return true, nil
}

// SetReplay replaces the replay ranges of processor.
func (m memoryControl) SetReplay(ctx context.Context, processor string, ranges []ReplayRange, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state := m.upsertLocked(processor)
	state.Replay = append([]ReplayRange(nil), ranges...)
	state.UpdatedAt = now
	return nil
}

// RemoveRange removes that exact range from the replay ranges of processor; false when it was not
// there.
func (m memoryControl) RemoveRange(ctx context.Context, processor string, replay ReplayRange, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.store.mu.Lock()
	defer m.store.mu.Unlock()
	m.store.initLocked()
	state, ok := m.store.data.control[processor]
	if !ok {
		return false, nil
	}
	for i, open := range state.Replay {
		if open == replay {
			state.Replay = append(state.Replay[:i], state.Replay[i+1:]...)
			state.UpdatedAt = now
			return true, nil
		}
	}
	return false, nil
}

// upsertLocked is the state of processor, made when absent.
func (m memoryControl) upsertLocked(processor string) *ProcessorControlState {
	state := m.store.data.control[processor]
	if state == nil {
		state = &ProcessorControlState{Processor: processor}
		m.store.data.control[processor] = state
	}
	return state
}

// copyControlState copies the operation and replay ranges a caller could change behind the
// store's back.
func copyControlState(state ProcessorControlState) ProcessorControlState {
	copied := state
	if state.Operation != nil {
		operation := *state.Operation
		copied.Operation = &operation
	}
	copied.Replay = append([]ReplayRange(nil), state.Replay...)
	return copied
}
