package sql

import (
	"context"
	stdSQL "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// sqlOutbox is the snodra.OutboxStore on snodra_outbox.
type sqlOutbox struct {
	store *Store
}

var _ snodra.OutboxStore = sqlOutbox{}

// Append appends entry in the transaction carried by ctx, or in a short transaction of its own.
// On PostgreSQL a notifying store also sends pg_notify in the same transaction, so a rollback
// notifies nobody and a commit wakes the relays.
func (o sqlOutbox) Append(ctx context.Context, entry snodra.OutboxEntry) error {
	headers, err := checkOutboxEntry(entry)
	if err != nil {
		return err
	}
	s := o.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	var key any
	if entry.Key != nil {
		key = *entry.Key
	}
	query := s.bind(fmt.Sprintf(
		"INSERT INTO snodra_outbox (id, type, event_key, headers, payload, created_at, seq, attempts) "+
			"VALUES (?, ?, ?, CAST(? AS %s), CAST(? AS %s), ?, ?, ?)",
		s.jsonType(), s.jsonType()))
	if _, err := exec.ExecContext(ctx, query, entry.ID, entry.Type, key, headers, string(entry.Payload), millis(entry.CreatedAt), entry.Seq, entry.Attempts); err != nil {
		return finish(storeError(err))
	}
	if s.driver == Postgres && s.notifier != nil {
		if _, err := exec.ExecContext(ctx, "SELECT pg_notify('snodra_outbox', '')"); err != nil {
			return finish(storeError(err))
		}
	}
	return finish(nil)
}

// Claim claims up to limit pending entries for owner until lease from now, in (created_at, seq,
// id) order. It runs in a transaction of its own at READ COMMITTED: SELECT ... FOR UPDATE SKIP
// LOCKED picks the entries free, already the owner's or whose lease passed, and skips the rows a
// concurrent claim holds.
func (o sqlOutbox) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	if owner == "" {
		return nil, invalidf("needs a non-empty claim owner")
	}
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := o.store
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := s.claim(ctx, tx, owner, lease, limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, storeError(err)
	}
	return entries, nil
}

// claim is Claim's work in tx.
func (s *Store) claim(ctx context.Context, tx *stdSQL.Tx, owner string, lease time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	now := millis(s.now())
	until := millis(now.Add(lease))
	query := s.bind(fmt.Sprintf(
		"SELECT id, type, event_key, CAST(headers AS %s) AS headers, CAST(payload AS %s) AS payload, created_at, seq, attempts "+
			"FROM snodra_outbox "+
			"WHERE sent_at IS NULL AND failed_at IS NULL AND (owner IS NULL OR owner = ? OR lease_until <= ?) "+
			"ORDER BY created_at, seq, id LIMIT ? FOR UPDATE SKIP LOCKED",
		s.textType(), s.textType()))
	rows, err := tx.QueryContext(ctx, query, owner, now, int64(limit))
	if err != nil {
		return nil, storeError(err)
	}
	entries := make([]snodra.OutboxEntry, 0, limit)
	for rows.Next() {
		entry, err := scanOutboxEntry(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, storeError(err)
	}
	_ = rows.Close()
	if len(entries) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(entries)+2)
	args = append(args, owner, until)
	ids := make([]string, len(entries))
	for i, entry := range entries {
		ids[i] = entry.ID
		args = append(args, entry.ID)
	}
	update := s.bind(fmt.Sprintf("UPDATE snodra_outbox SET owner = ?, lease_until = ? WHERE id IN (%s)", placeholders(len(ids))))
	if _, err := tx.ExecContext(ctx, update, args...); err != nil {
		return nil, storeError(err)
	}
	return entries, nil
}

// scanOutboxEntry reads one row of the claimed columns.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanOutboxEntry(row rowScanner) (snodra.OutboxEntry, error) {
	var entry snodra.OutboxEntry
	var key stdSQL.NullString
	var headersText, payloadText string
	var createdAt time.Time
	var seq int64
	var attempts int
	if err := row.Scan(&entry.ID, &entry.Type, &key, &headersText, &payloadText, &createdAt, &seq, &attempts); err != nil {
		return snodra.OutboxEntry{}, storeError(err)
	}
	entry.Key = nil
	if key.Valid {
		value := key.String
		entry.Key = &value
	}
	headers, err := headersFromJSON(headersText)
	if err != nil {
		return snodra.OutboxEntry{}, err
	}
	entry.Headers = headers
	entry.Payload = []byte(payloadText)
	entry.CreatedAt = createdAt
	entry.Seq = seq
	entry.Attempts = attempts
	return entry, nil
}

// MarkSent marks the entries sent; the store stamps the time from the application's clock.
func (o sqlOutbox) MarkSent(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	args := append([]any{millis(o.store.now())}, stringsToAny(ids)...)
	query := fmt.Sprintf("UPDATE snodra_outbox SET sent_at = ? WHERE id IN (%s)", placeholders(len(ids)))
	return o.store.ownUpdate(ctx, query, args...)
}

// MarkAttemptFailed counts a failed send that will be retried.
func (o sqlOutbox) MarkAttemptFailed(ctx context.Context, id, cause string) error {
	return o.store.ownUpdate(ctx, "UPDATE snodra_outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?", cutFreeText(cause), id)
}

// MarkFailed gives the entry up.
func (o sqlOutbox) MarkFailed(ctx context.Context, id, cause string) error {
	return o.store.ownUpdate(ctx, "UPDATE snodra_outbox SET attempts = attempts + 1, last_error = ?, failed_at = ? WHERE id = ?", cutFreeText(cause), millis(o.store.now()), id)
}

// Purge deletes the entries sent before sentBefore, in batches; how many.
func (o sqlOutbox) Purge(ctx context.Context, sentBefore time.Time) (int64, error) {
	return o.store.purgeTable(ctx, "snodra_outbox", "id", "sent_at < ?", millis(sentBefore))
}

// Pending counts the entries neither sent nor failed.
func (o sqlOutbox) Pending(ctx context.Context) (int64, error) {
	var pending int64
	if err := o.store.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM snodra_outbox WHERE sent_at IS NULL AND failed_at IS NULL").Scan(&pending); err != nil {
		return 0, storeError(err)
	}
	return pending, nil
}

// OldestPending is the CreatedAt of the oldest pending entry, held or not.
func (o sqlOutbox) OldestPending(ctx context.Context) (time.Time, bool, error) {
	var oldest time.Time
	err := o.store.db.QueryRowContext(ctx,
		"SELECT created_at FROM snodra_outbox WHERE sent_at IS NULL AND failed_at IS NULL ORDER BY created_at, seq LIMIT 1").Scan(&oldest)
	if errors.Is(err, stdSQL.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, storeError(err)
	}
	return oldest, true, nil
}

// OnAppend registers wake for every append that commits. On PostgreSQL with notifications on the
// store listens on snodra_outbox and wakes the relays; on MySQL, and with notifications off, it
// registers nothing and the relay polls.
func (o sqlOutbox) OnAppend(wake func()) (cancel func()) {
	if wake == nil || o.store.notifier == nil {
		return func() {}
	}
	return o.store.notifier.register(wake)
}

// sqlProcessed is the snodra.ProcessedStore on snodra_processed_events.
type sqlProcessed struct {
	store *Store
}

var _ snodra.ProcessedStore = sqlProcessed{}

// Record records marker in the transaction carried by ctx, or in a short transaction of its own;
// false when its id is recorded already.
func (p sqlProcessed) Record(ctx context.Context, marker snodra.ProcessedMarker) (bool, error) {
	if err := checkText("a marker's id", marker.ID, -1); err != nil {
		return false, err
	}
	if err := checkText("a marker's type", marker.Type, -1); err != nil {
		return false, err
	}
	if err := checkOptionalText("a marker's processor", marker.Processor, maxProcessor); err != nil {
		return false, err
	}
	if err := checkOptionalText("a marker's source", marker.Source, maxSource); err != nil {
		return false, err
	}
	if err := checkOptionalText("a marker's segment", marker.Segment, maxSegment); err != nil {
		return false, err
	}
	if err := checkOptionalText("a marker's position", marker.Position, maxPosition); err != nil {
		return false, err
	}
	s := p.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	var processor, source, segment, position, positionNum any
	if marker.Processor != "" {
		processor = marker.Processor
	}
	if marker.Source != "" {
		source = marker.Source
	}
	if marker.Segment != "" {
		segment = marker.Segment
	}
	if marker.Position != "" {
		position = marker.Position
		if number, ok := positionNumber(marker.Position); ok {
			positionNum = number
		}
	}
	columns := "(id_hash, id, type, processed_at, processor, source, segment, position, position_num) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	if s.driver == Postgres {
		columns += " ON CONFLICT (id_hash) DO NOTHING"
	}
	query := s.bind("INSERT INTO snodra_processed_events " + columns)
	result, err := exec.ExecContext(ctx, query, sha256Hex(marker.ID), marker.ID, marker.Type, millis(marker.ProcessedAt), processor, source, segment, position, positionNum)
	if err != nil {
		if s.driver == MySQL && isUniqueViolation(err) {
			return false, finish(nil)
		}
		return false, finish(storeError(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, finish(storeError(err))
	}
	return affected == 1, finish(nil)
}

// Exists reports whether a marker with id is committed (never one recorded in ctx's open
// transaction).
func (p sqlProcessed) Exists(ctx context.Context, id string) (bool, error) {
	var count int64
	if err := p.store.db.QueryRowContext(ctx,
		p.store.bind("SELECT COUNT(*) FROM snodra_processed_events WHERE id_hash = ?"), sha256Hex(id)).Scan(&count); err != nil {
		return false, storeError(err)
	}
	return count > 0, nil
}

// DeleteMarkers deletes the stage-1 markers of processor in ranges, and nothing else; how many.
func (p sqlProcessed) DeleteMarkers(ctx context.Context, processor string, ranges []snodra.ReplayRange) (int64, error) {
	if len(ranges) == 0 {
		return 0, nil
	}
	if err := checkOptionalText("a reset's processor", processor, maxProcessor); err != nil {
		return 0, err
	}
	s := p.store
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, replay := range ranges {
		if err := checkOptionalText("a reset range's source", replay.Source, maxSource); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		if err := checkOptionalText("a reset range's segment", replay.Segment, maxSegment); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		if err := checkOptionalText("a reset range's from position", replay.From, maxPosition); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		if err := checkOptionalText("a reset range's until position", replay.Until, maxPosition); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		from, fromArg := positionBound(">=", replay.From)
		until, untilArg := positionBound("<", replay.Until)
		query := s.bind(fmt.Sprintf(
			"DELETE FROM snodra_processed_events WHERE processor = ? AND source = ? AND segment = ? AND %s AND %s",
			from, until))
		result, err := tx.ExecContext(ctx, query, processor, replay.Source, replay.Segment, fromArg, untilArg)
		if err != nil {
			_ = tx.Rollback()
			return 0, storeError(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return 0, storeError(err)
		}
		deleted += affected
	}
	if err := tx.Commit(); err != nil {
		return 0, storeError(err)
	}
	return deleted, nil
}

// Purge deletes the markers processed before before, in batches; how many.
func (p sqlProcessed) Purge(ctx context.Context, before time.Time) (int64, error) {
	return p.store.purgeTable(ctx, "snodra_processed_events", "id_hash", "processed_at < ?", millis(before))
}

// positionBound is the SQL fragment and its argument for a position bound: a canonical decimal
// position compares through position_num by value; any other compares by its bytes among the
// non-numeric positions.
func positionBound(op, position string) (string, any) {
	if number, ok := positionNumber(position); ok {
		return "position_num " + op + " ?", number
	}
	return "position_num IS NULL AND position " + op + " ?", position
}

// placeholders is "?, ?, ..." for n parameters.
func placeholders(n int) string {
	if n < 1 {
		return "?"
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// stringsToAny is ids as bind arguments.
func stringsToAny(values []string) []any {
	args := make([]any, len(values))
	for i, value := range values {
		args[i] = value
	}
	return args
}
