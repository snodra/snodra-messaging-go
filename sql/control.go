package sql

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// sqlControl is the snodra.ProcessorControlStore on snodra_processor_control. Every write is a
// transaction of its own, outside the transaction the context may carry: a pause, a lock or a
// replay range commits even when the service's transaction rolls back (the shared layout's rule).
type sqlControl struct {
	store *Store
}

var _ snodra.ProcessorControlStore = sqlControl{}

// replayRangeJSON is a replay range as the replay column holds it.
type replayRangeJSON struct {
	Source  string `json:"source"`
	Segment string `json:"segment"`
	From    string `json:"from"`
	Until   string `json:"until"`
}

// Get is the state of processor; the zero state (not paused, nothing held) when the store has
// none.
func (c sqlControl) Get(ctx context.Context, processor string) (snodra.ProcessorControlState, error) {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return snodra.ProcessorControlState{}, err
	}
	return c.store.readControl(ctx, processor)
}

// readControl reads one row; the zero state when it is absent.
func (s *Store) readControl(ctx context.Context, processor string) (snodra.ProcessorControlState, error) {
	query := s.bind(fmt.Sprintf(
		"SELECT processor, paused, paused_at, paused_by, op_id, op_kind, op_lease_until, op_prepared_at, "+
			"CAST(replay AS %s), updated_at FROM snodra_processor_control WHERE processor = ?", s.textType()))
	var (
		state                            snodra.ProcessorControlState
		pausedAt, leaseUntil, preparedAt stdSQL.NullTime
		updatedAt                        stdSQL.NullTime
		pausedBy, opID, opKind           stdSQL.NullString
		replayText                       string
	)
	err := s.db.QueryRowContext(ctx, query, processor).Scan(
		&state.Processor, &state.Paused, &pausedAt, &pausedBy, &opID, &opKind, &leaseUntil, &preparedAt, &replayText, &updatedAt)
	if errors.Is(err, stdSQL.ErrNoRows) {
		return snodra.ProcessorControlState{Processor: processor}, nil
	}
	if err != nil {
		return snodra.ProcessorControlState{}, storeError(err)
	}
	if pausedAt.Valid {
		state.PausedAt = pausedAt.Time
	}
	if pausedBy.Valid {
		state.PausedBy = pausedBy.String
	}
	if updatedAt.Valid {
		state.UpdatedAt = updatedAt.Time
	}
	if opID.Valid {
		operation := snodra.ControlOperation{ID: opID.String, Kind: snodra.ResetKind}
		if opKind.Valid && opKind.String != "" {
			operation.Kind = opKind.String
		}
		if leaseUntil.Valid {
			operation.LeaseUntil = leaseUntil.Time
		}
		if preparedAt.Valid {
			operation.PreparedAt = preparedAt.Time
		}
		state.Operation = &operation
	}
	ranges, err := decodeReplay(replayText)
	if err != nil {
		return snodra.ProcessorControlState{}, err
	}
	state.Replay = ranges
	return state, nil
}

// decodeReplay reads a replay column.
func decodeReplay(text string) ([]snodra.ReplayRange, error) {
	if text == "" || text == "null" {
		return nil, nil
	}
	var stored []replayRangeJSON
	if err := json.Unmarshal([]byte(text), &stored); err != nil {
		return nil, fmt.Errorf("the SQL message store cannot read a replay column: %w", err)
	}
	ranges := make([]snodra.ReplayRange, 0, len(stored))
	for _, r := range stored {
		ranges = append(ranges, snodra.ReplayRange{Source: r.Source, Segment: r.Segment, From: r.From, Until: r.Until})
	}
	return ranges, nil
}

// encodeReplay writes replay ranges as the replay column holds them.
func encodeReplay(ranges []snodra.ReplayRange) (string, error) {
	stored := make([]replayRangeJSON, 0, len(ranges))
	for _, r := range ranges {
		if err := checkOptionalText("a replay range's source", r.Source, maxSource); err != nil {
			return "", err
		}
		if err := checkOptionalText("a replay range's segment", r.Segment, maxSegment); err != nil {
			return "", err
		}
		if err := checkOptionalText("a replay range's from position", r.From, maxPosition); err != nil {
			return "", err
		}
		if err := checkOptionalText("a replay range's until position", r.Until, maxPosition); err != nil {
			return "", err
		}
		stored = append(stored, replayRangeJSON{Source: r.Source, Segment: r.Segment, From: r.From, Until: r.Until})
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		return "", invalidf("cannot encode replay ranges: %v", err)
	}
	return string(encoded), nil
}

// ensureControl creates processor's row (not paused, no operation, no replay) unless it exists.
func (s *Store) ensureControl(ctx context.Context, tx *stdSQL.Tx, processor string) error {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT INTO snodra_processor_control (processor, paused, replay) VALUES (?, FALSE, CAST(? AS %s))", s.jsonType())
	if s.driver == Postgres {
		query += " ON CONFLICT (processor) DO NOTHING"
	} else {
		query += " ON DUPLICATE KEY UPDATE processor = snodra_processor_control.processor"
	}
	if _, err := tx.ExecContext(ctx, s.bind(query), processor, "[]"); err != nil {
		return storeError(err)
	}
	return nil
}

// ownTx runs work in a transaction of its own and commits it.
func (s *Store) ownTx(ctx context.Context, work func(*stdSQL.Tx) error) error {
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return err
	}
	if err := work(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return storeError(tx.Commit())
}

// SetPaused pauses (by the control id, or "local") or resumes processor, idempotently.
func (c sqlControl) SetPaused(ctx context.Context, processor string, paused bool, by string, now time.Time) error {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return err
	}
	if err := checkText("a pause's control id", by, -1); err != nil {
		return err
	}
	s := c.store
	return s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		if err := s.ensureControl(ctx, tx, processor); err != nil {
			return err
		}
		var pausedAt, pausedBy any
		if paused {
			pausedAt = millis(now)
			pausedBy = by
		}
		query := s.bind("UPDATE snodra_processor_control SET paused = ?, paused_at = ?, paused_by = ?, updated_at = ? WHERE processor = ?")
		_, err := tx.ExecContext(ctx, query, paused, pausedAt, pausedBy, millis(now), processor)
		return storeError(err)
	})
}

// Lock takes the lock of processor for operation until leaseUntil, atomically: when nothing holds
// it or the holder's lease has passed; a renewal moves the lease only.
func (c sqlControl) Lock(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (snodra.LockOutcome, error) {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return snodra.LockOutcome{}, err
	}
	if err := checkText("a reset's operation id", operation, -1); err != nil {
		return snodra.LockOutcome{}, err
	}
	s := c.store
	var outcome snodra.LockOutcome
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		if err := s.ensureControl(ctx, tx, processor); err != nil {
			return err
		}
		renew := s.bind("UPDATE snodra_processor_control SET op_lease_until = ?, updated_at = ? WHERE processor = ? AND op_id = ?")
		result, err := tx.ExecContext(ctx, renew, millis(leaseUntil), millis(now), processor, operation)
		if err != nil {
			return storeError(err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return storeError(err)
		} else if affected == 1 {
			outcome = snodra.LockOutcome{Taken: true}
			return nil
		}
		take := s.bind("UPDATE snodra_processor_control SET op_id = ?, op_kind = ?, op_lease_until = ?, op_prepared_at = ?, updated_at = ? " +
			"WHERE processor = ? AND (op_id IS NULL OR op_lease_until <= ?)")
		result, err = tx.ExecContext(ctx, take, operation, snodra.ResetKind, millis(leaseUntil), millis(now), millis(now), processor, millis(now))
		if err != nil {
			return storeError(err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return storeError(err)
		} else if affected == 1 {
			outcome = snodra.LockOutcome{Taken: true}
			return nil
		}
		state, err := s.readControlTx(ctx, tx, processor)
		if err != nil {
			return err
		}
		outcome = snodra.LockOutcome{}
		if state.Operation != nil {
			outcome.Holder = *state.Operation
		}
		return nil
	})
	return outcome, err
}

// readControlTx reads one row in tx; the zero state when it is absent.
func (s *Store) readControlTx(ctx context.Context, tx *stdSQL.Tx, processor string) (snodra.ProcessorControlState, error) {
	query := s.bind(fmt.Sprintf(
		"SELECT processor, paused, paused_at, paused_by, op_id, op_kind, op_lease_until, op_prepared_at, "+
			"CAST(replay AS %s), updated_at FROM snodra_processor_control WHERE processor = ?", s.textType()))
	var (
		state                            snodra.ProcessorControlState
		pausedAt, leaseUntil, preparedAt stdSQL.NullTime
		updatedAt                        stdSQL.NullTime
		pausedBy, opID, opKind           stdSQL.NullString
		replayText                       string
	)
	err := tx.QueryRowContext(ctx, query, processor).Scan(
		&state.Processor, &state.Paused, &pausedAt, &pausedBy, &opID, &opKind, &leaseUntil, &preparedAt, &replayText, &updatedAt)
	if errors.Is(err, stdSQL.ErrNoRows) {
		return snodra.ProcessorControlState{Processor: processor}, nil
	}
	if err != nil {
		return snodra.ProcessorControlState{}, storeError(err)
	}
	if pausedAt.Valid {
		state.PausedAt = pausedAt.Time
	}
	if pausedBy.Valid {
		state.PausedBy = pausedBy.String
	}
	if updatedAt.Valid {
		state.UpdatedAt = updatedAt.Time
	}
	if opID.Valid {
		operation := snodra.ControlOperation{ID: opID.String, Kind: snodra.ResetKind}
		if opKind.Valid && opKind.String != "" {
			operation.Kind = opKind.String
		}
		if leaseUntil.Valid {
			operation.LeaseUntil = leaseUntil.Time
		}
		if preparedAt.Valid {
			operation.PreparedAt = preparedAt.Time
		}
		state.Operation = &operation
	}
	ranges, err := decodeReplay(replayText)
	if err != nil {
		return snodra.ProcessorControlState{}, err
	}
	state.Replay = ranges
	return state, nil
}

// Extend moves the lease of operation; false when it does not hold the lock.
func (c sqlControl) Extend(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (bool, error) {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return false, err
	}
	if err := checkText("a reset's operation id", operation, -1); err != nil {
		return false, err
	}
	s := c.store
	var extended bool
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		result, err := tx.ExecContext(ctx, s.bind("UPDATE snodra_processor_control SET op_lease_until = ?, updated_at = ? WHERE processor = ? AND op_id = ?"),
			millis(leaseUntil), millis(now), processor, operation)
		if err != nil {
			return storeError(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return storeError(err)
		}
		extended = affected == 1
		return nil
	})
	return extended, err
}

// Release clears the lock when operation holds it.
func (c sqlControl) Release(ctx context.Context, processor, operation string, now time.Time) (bool, error) {
	return c.clearLock(ctx, processor, operation, now, false)
}

// Expire clears the lock when operation holds it and its lease has passed at now.
func (c sqlControl) Expire(ctx context.Context, processor, operation string, now time.Time) (bool, error) {
	return c.clearLock(ctx, processor, operation, now, true)
}

// clearLock is Release and Expire.
func (c sqlControl) clearLock(ctx context.Context, processor, operation string, now time.Time, expiredOnly bool) (bool, error) {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return false, err
	}
	if err := checkText("a reset's operation id", operation, -1); err != nil {
		return false, err
	}
	s := c.store
	query := "UPDATE snodra_processor_control SET op_id = NULL, op_kind = NULL, op_lease_until = NULL, op_prepared_at = NULL, updated_at = ? " +
		"WHERE processor = ? AND op_id = ?"
	if expiredOnly {
		query += " AND op_lease_until <= ?"
	}
	var cleared bool
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		args := []any{millis(now), processor, operation}
		if expiredOnly {
			args = append(args, millis(now))
		}
		result, err := tx.ExecContext(ctx, s.bind(query), args...)
		if err != nil {
			return storeError(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return storeError(err)
		}
		cleared = affected == 1
		return nil
	})
	return cleared, err
}

// SetReplay replaces the replay ranges of processor.
func (c sqlControl) SetReplay(ctx context.Context, processor string, ranges []snodra.ReplayRange, now time.Time) error {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return err
	}
	encoded, err := encodeReplay(ranges)
	if err != nil {
		return err
	}
	s := c.store
	return s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		if err := s.ensureControl(ctx, tx, processor); err != nil {
			return err
		}
		query := fmt.Sprintf("UPDATE snodra_processor_control SET replay = CAST(? AS %s), updated_at = ? WHERE processor = ?", s.jsonType())
		_, err := tx.ExecContext(ctx, s.bind(query), encoded, millis(now), processor)
		return storeError(err)
	})
}

// RemoveRange removes that exact range from the replay ranges of processor; false when it was
// not there.
func (c sqlControl) RemoveRange(ctx context.Context, processor string, replay snodra.ReplayRange, now time.Time) (bool, error) {
	if err := checkText("a processor's name", processor, maxProcessor); err != nil {
		return false, err
	}
	if _, err := encodeReplay([]snodra.ReplayRange{replay}); err != nil {
		return false, err
	}
	s := c.store
	var removed bool
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		var replayText string
		query := s.bind(fmt.Sprintf("SELECT CAST(replay AS %s) FROM snodra_processor_control WHERE processor = ? FOR UPDATE", s.textType()))
		err := tx.QueryRowContext(ctx, query, processor).Scan(&replayText)
		if errors.Is(err, stdSQL.ErrNoRows) {
			return nil
		}
		if err != nil {
			return storeError(err)
		}
		ranges, err := decodeReplay(replayText)
		if err != nil {
			return err
		}
		found := -1
		for i, open := range ranges {
			if open == replay {
				found = i
				break
			}
		}
		if found < 0 {
			return nil
		}
		remaining := append(ranges[:found], ranges[found+1:]...)
		encoded, err := encodeReplay(remaining)
		if err != nil {
			return err
		}
		update := fmt.Sprintf("UPDATE snodra_processor_control SET replay = CAST(? AS %s), updated_at = ? WHERE processor = ?", s.jsonType())
		if _, err := tx.ExecContext(ctx, s.bind(update), encoded, millis(now), processor); err != nil {
			return storeError(err)
		}
		removed = true
		return nil
	})
	return removed, err
}
