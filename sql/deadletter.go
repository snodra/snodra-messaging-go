package sql

import (
	"bytes"
	"context"
	stdSQL "database/sql"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	snodra "github.com/snodra/snodra-messaging-go"
)

// sqlDeadLetters is the snodra.DeadLetterStore on snodra_dead_letters. Every write but DeleteIn is
// a transaction of its own, outside the transaction the context may carry.
type sqlDeadLetters struct {
	store *Store
}

var _ snodra.DeadLetterStore = sqlDeadLetters{}

// maxDeadLetterID is the schema's VARCHAR(578).
const maxDeadLetterID = 578

// deadLettersLive is the condition every read and write but Park applies: a letter whose expiry
// passed is gone at once.
const deadLettersLive = "expire_at > ?"

// Park inserts letter; ParkAlready when a letter of that id exists (a redelivery).
func (d sqlDeadLetters) Park(ctx context.Context, letter snodra.DeadLetter) (snodra.ParkResult, error) {
	if err := checkText("a dead letter's id", letter.ID, maxDeadLetterID); err != nil {
		return 0, err
	}
	if err := checkOptionalText("a dead letter's processor", letter.Processor, maxProcessor); err != nil {
		return 0, err
	}
	if err := checkOptionalText("a dead letter's source", letter.Source, maxSource); err != nil {
		return 0, err
	}
	if err := checkOptionalText("a dead letter's segment", letter.Segment, maxSegment); err != nil {
		return 0, err
	}
	if err := checkOptionalText("a dead letter's position", letter.Position, maxPosition); err != nil {
		return 0, err
	}
	if letter.Key != nil {
		if err := checkText("a dead letter's key", *letter.Key, -1); err != nil {
			return 0, err
		}
	}
	headers, err := headersJSON(letter.Headers)
	if err != nil {
		return 0, err
	}
	payloadText, payloadBinary := deadLetterPayload(letter.Payload)
	if err := checkText("a dead letter's error type", cutFreeText(letter.Error.Type), -1); err != nil {
		return 0, err
	}
	if err := checkText("a dead letter's error message", cutFreeText(letter.Error.Message), -1); err != nil {
		return 0, err
	}
	if err := checkText("a dead letter's blocked-by id", letter.BlockedBy, -1); err != nil {
		return 0, err
	}
	now := millis(d.store.now())
	s := d.store
	var result snodra.ParkResult
	err = s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		// An expired letter of the same id no longer exists: its id is free again.
		if _, err := tx.ExecContext(ctx, s.bind("DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at <= ?"), letter.ID, now); err != nil {
			return storeError(err)
		}
		var key, eventID, eventType, blockedBy any
		var keyHash any
		if letter.Key != nil {
			key = *letter.Key
			keyHash = sha256Hex(*letter.Key)
		}
		if letter.EventID != "" {
			eventID = letter.EventID
		}
		if letter.EventType != "" {
			eventType = letter.EventType
		}
		if letter.BlockedBy != "" {
			blockedBy = letter.BlockedBy
		}
		var positionNum any
		if number, ok := positionNumber(letter.Position); ok {
			positionNum = number
		}
		query := fmt.Sprintf(
			"INSERT INTO snodra_dead_letters (id, processor, source, segment, position, position_num, event_key, event_key_hash, "+
				"event_id, event_type, headers, payload, payload_binary, reason, error_type, error_message, blocked_by, attempts, "+
				"parked_at, last_attempt_at, expire_at, replay) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS %s), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			s.jsonType())
		if s.driver == Postgres {
			query += " ON CONFLICT (id) DO NOTHING"
		}
		execResult, err := tx.ExecContext(ctx, s.bind(query),
			letter.ID, letter.Processor, letter.Source, letter.Segment, letter.Position, positionNum, key, keyHash,
			eventID, eventType, headers, payloadText, payloadBinary, string(letter.Reason),
			cutFreeText(letter.Error.Type), cutFreeText(letter.Error.Message), blockedBy, letter.Attempts,
			millis(letter.ParkedAt), millis(letter.LastAttemptAt), millis(letter.ExpireAt), letter.Replay)
		if err != nil {
			if s.driver == MySQL && isUniqueViolation(err) {
				result = snodra.ParkAlready
				return nil
			}
			return storeError(err)
		}
		affected, err := execResult.RowsAffected()
		if err != nil {
			return storeError(err)
		}
		if affected == 1 {
			result = snodra.ParkNew
		} else {
			result = snodra.ParkAlready
		}
		return nil
	})
	return result, err
}

// deadLetterPayload chooses the text or binary column a payload needs: text only when it is UTF-8
// without U+0000, the rule every text column of the layout follows.
func deadLetterPayload(payload []byte) (text any, binary any) {
	if len(payload) == 0 {
		return "", nil
	}
	if utf8.Valid(payload) && !bytes.ContainsRune(payload, 0) {
		return string(payload), nil
	}
	return nil, payload
}

// Head is the id of the head letter of (processor, key): the earliest position among its letters
// that are not INVALID, then the lowest id.
func (d sqlDeadLetters) Head(ctx context.Context, processor, key string) (string, bool, error) {
	return d.first(ctx, processor, key, "", nil)
}

// Earlier is the id of the earliest letter of (processor, key) at a position before position that
// is not INVALID.
func (d sqlDeadLetters) Earlier(ctx context.Context, processor, key, position string) (string, bool, error) {
	bound, arg := positionBound("<", position)
	return d.first(ctx, processor, key, " AND "+bound, arg)
}

// first is Head and Earlier: the first blocking letter of a key, optionally before a position.
func (d sqlDeadLetters) first(ctx context.Context, processor, key, condition string, arg any) (string, bool, error) {
	if err := checkOptionalText("a dead letter's processor", processor, maxProcessor); err != nil {
		return "", false, err
	}
	if err := checkText("a dead letter's key", key, -1); err != nil {
		return "", false, err
	}
	s := d.store
	query := s.bind("SELECT id FROM snodra_dead_letters WHERE processor = ? AND event_key_hash = ? AND reason <> 'INVALID' AND expire_at > ?" +
		condition + " ORDER BY position_num IS NULL, position_num, position, id LIMIT 1")
	args := []any{processor, sha256Hex(key), millis(s.now())}
	if arg != nil {
		args = append(args, arg)
	}
	var id string
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, stdSQL.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, storeError(err)
	}
	return id, true, nil
}

// ParkedKeys is the keys with a letter (not INVALID) of processor on the given segments of
// source.
func (d sqlDeadLetters) ParkedKeys(ctx context.Context, processor, source string, segments []string) ([]string, error) {
	if len(segments) == 0 {
		return nil, nil
	}
	if err := checkOptionalText("a dead letter's processor", processor, maxProcessor); err != nil {
		return nil, err
	}
	if err := checkOptionalText("a dead letter's source", source, maxSource); err != nil {
		return nil, err
	}
	for _, segment := range segments {
		if err := checkText("a dead letter's segment", segment, maxSegment); err != nil {
			return nil, err
		}
	}
	s := d.store
	args := []any{processor, source}
	for _, segment := range segments {
		args = append(args, segment)
	}
	args = append(args, millis(s.now()))
	query := s.bind(fmt.Sprintf(
		"SELECT DISTINCT event_key FROM snodra_dead_letters WHERE processor = ? AND source = ? AND segment IN (%s) "+
			"AND event_key IS NOT NULL AND reason <> 'INVALID' AND "+deadLettersLive, placeholders(len(segments))))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, storeError(err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return keys, nil
}

// Get returns the letter, or nil when there is none.
func (d sqlDeadLetters) Get(ctx context.Context, id string) (*snodra.DeadLetter, error) {
	if err := checkText("a dead letter's id", id, maxDeadLetterID); err != nil {
		return nil, err
	}
	s := d.store
	query := s.bind(fmt.Sprintf("SELECT %s FROM snodra_dead_letters WHERE id = ? AND expire_at > ?", s.deadLetterColumns()))
	letter, err := scanDeadLetter(s.db.QueryRowContext(ctx, query, id, millis(s.now())))
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &letter, nil
}

// List returns letters by ParkedAt, then id, every processor's when processor is empty, at most
// limit when limit > 0.
func (d sqlDeadLetters) List(ctx context.Context, processor string, limit int) ([]snodra.DeadLetter, error) {
	if err := checkOptionalText("a dead letter's processor", processor, maxProcessor); err != nil {
		return nil, err
	}
	s := d.store
	query := fmt.Sprintf("SELECT %s FROM snodra_dead_letters WHERE expire_at > ?", s.deadLetterColumns())
	args := []any{millis(s.now())}
	if processor != "" {
		query += " AND processor = ?"
		args = append(args, processor)
	}
	query += " ORDER BY parked_at, id"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, s.bind(query), args...)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	letters := make([]snodra.DeadLetter, 0)
	for rows.Next() {
		letter, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}
		letters = append(letters, letter)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return letters, nil
}

// Summaries returns every letter of processor, without its record, by position then id.
func (d sqlDeadLetters) Summaries(ctx context.Context, processor string) ([]snodra.DeadLetterSummary, error) {
	if err := checkOptionalText("a dead letter's processor", processor, maxProcessor); err != nil {
		return nil, err
	}
	s := d.store
	query := s.bind("SELECT id, event_key, position, reason, parked_at FROM snodra_dead_letters WHERE processor = ? AND expire_at > ? " +
		"ORDER BY position_num IS NULL, position_num, position, id")
	rows, err := s.db.QueryContext(ctx, query, processor, millis(s.now()))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	summaries := make([]snodra.DeadLetterSummary, 0)
	for rows.Next() {
		var (
			summary  snodra.DeadLetterSummary
			key      stdSQL.NullString
			reason   string
			parkedAt time.Time
		)
		if err := rows.Scan(&summary.ID, &key, &summary.Position, &reason, &parkedAt); err != nil {
			return nil, storeError(err)
		}
		if key.Valid {
			value := key.String
			summary.Key = &value
		}
		summary.Reason = snodra.DeadLetterReason(reason)
		summary.ParkedAt = parkedAt
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return summaries, nil
}

// Count is how many letters there are; every processor's when processor is empty.
func (d sqlDeadLetters) Count(ctx context.Context, processor string) (int64, error) {
	if err := checkOptionalText("a dead letter's processor", processor, maxProcessor); err != nil {
		return 0, err
	}
	s := d.store
	query := "SELECT COUNT(*) FROM snodra_dead_letters WHERE expire_at > ?"
	args := []any{millis(s.now())}
	if processor != "" {
		query += " AND processor = ?"
		args = append(args, processor)
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, s.bind(query), args...).Scan(&count); err != nil {
		return 0, storeError(err)
	}
	return count, nil
}

// Delete deletes the letter; false when there was none.
func (d sqlDeadLetters) Delete(ctx context.Context, id string) (bool, error) {
	return d.delete(ctx, id, false)
}

// DeleteIn deletes the letter in the transaction carried by ctx.
func (d sqlDeadLetters) DeleteIn(ctx context.Context, id string) (bool, error) {
	return d.delete(ctx, id, true)
}

// delete is Delete and DeleteIn: with joined the delete runs in the context's transaction, else
// in one of its own.
func (d sqlDeadLetters) delete(ctx context.Context, id string, joined bool) (bool, error) {
	if err := checkText("a dead letter's id", id, maxDeadLetterID); err != nil {
		return false, err
	}
	s := d.store
	query := s.bind("DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at > ?")
	run := func(exec executor) (bool, error) {
		result, err := exec.ExecContext(ctx, query, id, millis(s.now()))
		if err != nil {
			return false, storeError(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return false, storeError(err)
		}
		return affected > 0, nil
	}
	if joined {
		exec, finish, err := s.scope(ctx)
		if err != nil {
			return false, err
		}
		deleted, err := run(exec)
		return deleted, finish(err)
	}
	var deleted bool
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		var err error
		deleted, err = run(tx)
		return err
	})
	return deleted, err
}

// RecordFailure records that a retry failed again: Attempts + 1, Error, LastAttemptAt = now,
// reason HANDLER_FAILED, no BlockedBy any more.
func (d sqlDeadLetters) RecordFailure(ctx context.Context, id string, failure snodra.DeadLetterError, now time.Time) (bool, error) {
	if err := checkText("a dead letter's id", id, maxDeadLetterID); err != nil {
		return false, err
	}
	if err := checkText("a dead letter's error type", cutFreeText(failure.Type), -1); err != nil {
		return false, err
	}
	if err := checkText("a dead letter's error message", cutFreeText(failure.Message), -1); err != nil {
		return false, err
	}
	s := d.store
	query := s.bind("UPDATE snodra_dead_letters SET attempts = attempts + 1, error_type = ?, error_message = ?, " +
		"last_attempt_at = ?, reason = 'HANDLER_FAILED', blocked_by = NULL WHERE id = ? AND expire_at > ?")
	var changed bool
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		result, err := tx.ExecContext(ctx, query, cutFreeText(failure.Type), cutFreeText(failure.Message), millis(now), id, millis(s.now()))
		if err != nil {
			return storeError(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return storeError(err)
		}
		changed = affected > 0
		return nil
	})
	return changed, err
}

// deadLetterColumns is the SELECT list scanDeadLetter reads.
func (s *Store) deadLetterColumns() string {
	return "id, processor, source, segment, position, event_key, event_id, event_type, " +
		"CAST(headers AS " + s.textType() + "), payload, payload_binary, reason, error_type, error_message, blocked_by, " +
		"attempts, parked_at, last_attempt_at, expire_at, replay"
}

// scanDeadLetter reads one row of deadLetterColumns.
func scanDeadLetter(row rowScanner) (snodra.DeadLetter, error) {
	var (
		letter                            snodra.DeadLetter
		key, eventID, eventType, blocked  stdSQL.NullString
		headersText, reason, errorType    string
		errorMessage                      string
		payload                           stdSQL.NullString
		payloadBinary                     []byte
		parkedAt, lastAttemptAt, expireAt time.Time
	)
	if err := row.Scan(&letter.ID, &letter.Processor, &letter.Source, &letter.Segment, &letter.Position,
		&key, &eventID, &eventType, &headersText, &payload, &payloadBinary, &reason, &errorType, &errorMessage,
		&blocked, &letter.Attempts, &parkedAt, &lastAttemptAt, &expireAt, &letter.Replay); err != nil {
		return snodra.DeadLetter{}, storeError(err)
	}
	if key.Valid {
		value := key.String
		letter.Key = &value
	}
	letter.EventID = eventID.String
	letter.EventType = eventType.String
	letter.BlockedBy = blocked.String
	headers, err := headersFromJSON(headersText)
	if err != nil {
		return snodra.DeadLetter{}, err
	}
	letter.Headers = headers
	if payload.Valid {
		letter.Payload = []byte(payload.String)
	} else {
		letter.Payload = payloadBinary
	}
	letter.Reason = snodra.DeadLetterReason(reason)
	letter.Error = snodra.DeadLetterError{Type: errorType, Message: errorMessage}
	letter.ParkedAt = parkedAt
	letter.LastAttemptAt = lastAttemptAt
	letter.ExpireAt = expireAt
	return letter, nil
}
