package sql

import (
	"context"
	stdSQL "database/sql"
	"errors"
	"fmt"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// sqlExternalCalls is the snodra.ExternalCallStore on snodra_external_calls. Every write is one
// statement of its own, outside the caller's transaction: a call that happened is never forgotten
// by a rolled-back handler.
type sqlExternalCalls struct {
	store *Store
}

var _ snodra.ExternalCallStore = sqlExternalCalls{}

// Insert inserts call; false when a record of its key exists.
func (c sqlExternalCalls) Insert(ctx context.Context, call snodra.ExternalCall) (bool, error) {
	if err := checkText("an external call's key", call.Key, -1); err != nil {
		return false, err
	}
	s := c.store
	query := s.bind("INSERT INTO snodra_external_calls (id_hash, id, status, reply, error, attempts, updated_at, lease_until, owner) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if s.driver == Postgres {
		query += " ON CONFLICT (id_hash) DO NOTHING"
	}
	var reply, errText, owner any
	var leaseUntil any
	if call.Reply != "" {
		reply = call.Reply
	}
	if call.Error != "" {
		errText = call.Error
	}
	if call.Owner != "" {
		owner = call.Owner
	}
	if !call.LeaseUntil.IsZero() {
		leaseUntil = millis(call.LeaseUntil)
	}
	result, err := s.db.ExecContext(ctx, query, sha256Hex(call.Key), call.Key, string(call.Status),
		reply, errText, call.Attempts, millis(call.UpdatedAt), leaseUntil, owner)
	if err != nil {
		if s.driver == MySQL && isUniqueViolation(err) {
			return false, nil
		}
		return false, storeError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, storeError(err)
	}
	return affected == 1, nil
}

// Find returns the record of key, or nil when there is none.
func (c sqlExternalCalls) Find(ctx context.Context, key string) (*snodra.ExternalCall, error) {
	if err := checkText("an external call's key", key, -1); err != nil {
		return nil, err
	}
	query := c.store.bind("SELECT id, status, reply, error, attempts, updated_at, lease_until, owner " +
		"FROM snodra_external_calls WHERE id_hash = ?")
	row := c.store.db.QueryRowContext(ctx, query, sha256Hex(key))
	call, err := scanExternalCall(row)
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &call, nil
}

// scanExternalCall reads one external-call row.
func scanExternalCall(row rowScanner) (snodra.ExternalCall, error) {
	var (
		call                  snodra.ExternalCall
		status                string
		reply, errText, owner stdSQL.NullString
		leaseUntil            stdSQL.NullTime
		updatedAt             time.Time
	)
	if err := row.Scan(&call.Key, &status, &reply, &errText, &call.Attempts, &updatedAt, &leaseUntil, &owner); err != nil {
		return snodra.ExternalCall{}, err
	}
	call.Status = snodra.ExternalCallStatus(status)
	call.Reply = reply.String
	call.Error = errText.String
	call.Owner = owner.String
	call.UpdatedAt = updatedAt
	if leaseUntil.Valid {
		call.LeaseUntil = leaseUntil.Time
	}
	return call, nil
}

// TakeOver takes the lapsed call seen over for owner.
func (c sqlExternalCalls) TakeOver(ctx context.Context, seen snodra.ExternalCall, owner string, leaseUntil, now time.Time) (*snodra.ExternalCall, error) {
	if err := checkText("an external call's key", seen.Key, -1); err != nil {
		return nil, err
	}
	s := c.store
	condition, args := ownedSeen(seen)
	query := s.bind(fmt.Sprintf(
		"UPDATE snodra_external_calls SET owner = ?, lease_until = ?, updated_at = ?, attempts = attempts + 1 "+
			"WHERE id_hash = ? AND status = ? AND %s", condition))
	args = append([]any{owner, millis(leaseUntil), millis(now), sha256Hex(seen.Key), string(snodra.ExternalCallInProgress)}, args...)
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, storeError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, storeError(err)
	}
	if affected == 0 {
		return nil, nil
	}
	return s.findCall(ctx, seen.Key)
}

// Expire gives the lapsed call seen up as UNKNOWN.
func (c sqlExternalCalls) Expire(ctx context.Context, seen snodra.ExternalCall, now time.Time) (bool, error) {
	s := c.store
	condition, args := ownedSeen(seen)
	query := s.bind(fmt.Sprintf(
		"UPDATE snodra_external_calls SET status = ?, owner = NULL, lease_until = NULL, updated_at = ? "+
			"WHERE id_hash = ? AND status = ? AND %s", condition))
	args = append([]any{string(snodra.ExternalCallUnknown), millis(now), sha256Hex(seen.Key), string(snodra.ExternalCallInProgress)}, args...)
	return s.affected(ctx, query, args...)
}

// Succeed settles the record owner owns as SUCCEEDED with reply.
func (c sqlExternalCalls) Succeed(ctx context.Context, key, owner, reply string, now time.Time) (bool, error) {
	query := c.store.bind("UPDATE snodra_external_calls SET status = ?, reply = ?, error = NULL, owner = NULL, lease_until = NULL, updated_at = ? " +
		"WHERE id_hash = ? AND status = ? AND owner = ?")
	return c.store.affected(ctx, query, string(snodra.ExternalCallSucceeded), reply, millis(now),
		sha256Hex(key), string(snodra.ExternalCallInProgress), owner)
}

// Fail settles the record owner owns as FAILED with errText.
func (c sqlExternalCalls) Fail(ctx context.Context, key, owner, errText string, now time.Time) (bool, error) {
	query := c.store.bind("UPDATE snodra_external_calls SET status = ?, reply = NULL, error = ?, owner = NULL, lease_until = NULL, updated_at = ? " +
		"WHERE id_hash = ? AND status = ? AND owner = ?")
	return c.store.affected(ctx, query, string(snodra.ExternalCallFailed), errText, millis(now),
		sha256Hex(key), string(snodra.ExternalCallInProgress), owner)
}

// MarkUnknown settles the record owner owns as UNKNOWN.
func (c sqlExternalCalls) MarkUnknown(ctx context.Context, key, owner string, now time.Time) (bool, error) {
	query := c.store.bind("UPDATE snodra_external_calls SET status = ?, reply = NULL, error = NULL, owner = NULL, lease_until = NULL, updated_at = ? " +
		"WHERE id_hash = ? AND status = ? AND owner = ?")
	return c.store.affected(ctx, query, string(snodra.ExternalCallUnknown), millis(now),
		sha256Hex(key), string(snodra.ExternalCallInProgress), owner)
}

// Reset deletes the record owner owns.
func (c sqlExternalCalls) Reset(ctx context.Context, key, owner string) (bool, error) {
	query := c.store.bind("DELETE FROM snodra_external_calls WHERE id_hash = ? AND status = ? AND owner = ?")
	return c.store.affected(ctx, query, sha256Hex(key), string(snodra.ExternalCallInProgress), owner)
}

// ownedSeen is the SQL condition and arguments matching the record a lapsed call read: its owner
// and lease.
func ownedSeen(seen snodra.ExternalCall) (string, []any) {
	if seen.LeaseUntil.IsZero() {
		return "owner = ? AND lease_until IS NULL", []any{seen.Owner}
	}
	return "owner = ? AND lease_until = ?", []any{seen.Owner, millis(seen.LeaseUntil)}
}

// findCall reads a committed record.
func (s *Store) findCall(ctx context.Context, key string) (*snodra.ExternalCall, error) {
	query := s.bind("SELECT id, status, reply, error, attempts, updated_at, lease_until, owner " +
		"FROM snodra_external_calls WHERE id_hash = ?")
	call, err := scanExternalCall(s.db.QueryRowContext(ctx, query, sha256Hex(key)))
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	return &call, nil
}

// affected runs one own statement and reports whether it changed a row.
func (s *Store) affected(ctx context.Context, query string, args ...any) (bool, error) {
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, storeError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, storeError(err)
	}
	return affected > 0, nil
}
