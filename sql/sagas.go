package sql

import (
	"context"
	stdSQL "database/sql"
	"errors"
	"fmt"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The saga ports on the shared V1 tables. What the runtime calls joins the caller's transaction;
// the relays' and the scheduler's claims (notices, leases, deadlines) commit on their own.

// sqlSagaRepository is the snodra.SagaRepository on snodra_sagas and snodra_saga_associations.
type sqlSagaRepository struct {
	store *Store
}

var _ snodra.SagaRepository = sqlSagaRepository{}

// FindByID returns the instance, or nil when there is none.
func (r sqlSagaRepository) FindByID(ctx context.Context, id string) (*snodra.Saga, error) {
	if err := checkText("a saga's id", id, maxID); err != nil {
		return nil, err
	}
	s := r.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := s.bind("SELECT id, type, state, start_key, ended, ended_at, version, created_at, updated_at FROM snodra_sagas WHERE id = ?")
	saga, err := scanSaga(exec.QueryRowContext(ctx, query, id))
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, finish(nil)
	}
	if err != nil {
		return nil, finish(err)
	}
	associations, err := readAssociations(ctx, exec, s, []string{id})
	if err != nil {
		return nil, finish(err)
	}
	saga.Associations = associations[id]
	return &saga, finish(nil)
}

// FindLive returns the live instances of typeName associated with key = value.
func (r sqlSagaRepository) FindLive(ctx context.Context, typeName, key, value string) ([]snodra.Saga, error) {
	if err := checkOptionalText("a saga's type", typeName, maxName); err != nil {
		return nil, err
	}
	s := r.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := s.bind("SELECT s.id, s.type, s.state, s.start_key, s.ended, s.ended_at, s.version, s.created_at, s.updated_at " +
		"FROM snodra_sagas s WHERE s.type = ? AND s.ended = FALSE AND EXISTS (" +
		"SELECT 1 FROM snodra_saga_associations a WHERE a.saga_id = s.id AND a.assoc_hash = ? AND a.assoc_key = ? AND a.assoc_value = ?) " +
		"ORDER BY s.created_at, s.id")
	rows, err := exec.QueryContext(ctx, query, typeName, associationHash(key, value), key, value)
	if err != nil {
		return nil, finish(storeError(err))
	}
	sagas, err := scanSagas(rows)
	if err != nil {
		return nil, finish(err)
	}
	ids := make([]string, len(sagas))
	for i, saga := range sagas {
		ids[i] = saga.ID
	}
	associations, err := readAssociations(ctx, exec, s, ids)
	if err != nil {
		return nil, finish(err)
	}
	for i := range sagas {
		sagas[i].Associations = associations[sagas[i].ID]
	}
	return sagas, finish(nil)
}

// Insert inserts a new instance; ErrConflict when its id exists or a live instance holds its
// StartKey.
func (r sqlSagaRepository) Insert(ctx context.Context, saga snodra.Saga) error {
	if err := checkSaga(saga); err != nil {
		return err
	}
	s := r.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	live := saga.StartKey != nil && !saga.Ended
	if live && s.driver == Postgres {
		got, err := tryStartLock(ctx, exec, s, *saga.StartKey)
		if err != nil {
			return finish(err)
		}
		if !got {
			return finish(fmt.Errorf("%w: another transaction is starting a live saga with the start key", snodra.ErrConflict))
		}
	}
	insert := func() error {
		query := s.bind("INSERT INTO snodra_sagas (id, type, state, start_key, start_key_hash, ended, ended_at, version, created_at, updated_at) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		var startKey, startHash any
		if saga.StartKey != nil {
			startKey = *saga.StartKey
			startHash = sha256Hex(*saga.StartKey)
		}
		var endedAt any
		if !saga.EndedAt.IsZero() {
			endedAt = millis(saga.EndedAt)
		}
		_, err := exec.ExecContext(ctx, query, saga.ID, saga.Type, saga.State, startKey, startHash,
			saga.Ended, endedAt, saga.Version, millis(saga.CreatedAt), millis(saga.UpdatedAt))
		return err
	}
	if live && s.driver == MySQL {
		err = s.waitsAtMost(ctx, exec, 1, insert)
	} else {
		err = insert()
	}
	if err != nil {
		return finish(storeError(err))
	}
	if err := insertAssociations(ctx, exec, s, saga); err != nil {
		return finish(err)
	}
	return finish(nil)
}

// Update replaces the stored instance when its Version is expectedVersion.
func (r sqlSagaRepository) Update(ctx context.Context, saga snodra.Saga, expectedVersion int64) (bool, error) {
	if err := checkSaga(saga); err != nil {
		return false, err
	}
	s := r.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	var stored int64
	query := s.bind("SELECT version FROM snodra_sagas WHERE id = ? FOR UPDATE NOWAIT")
	err = exec.QueryRowContext(ctx, query, saga.ID).Scan(&stored)
	if errors.Is(err, stdSQL.ErrNoRows) {
		return false, finish(nil)
	}
	if err != nil {
		return false, finish(storeError(err))
	}
	if stored != expectedVersion {
		return false, finish(nil)
	}
	var startKey, startHash any
	if saga.StartKey != nil {
		startKey = *saga.StartKey
		startHash = sha256Hex(*saga.StartKey)
	}
	var endedAt any
	if !saga.EndedAt.IsZero() {
		endedAt = millis(saga.EndedAt)
	}
	update := s.bind("UPDATE snodra_sagas SET type = ?, state = ?, start_key = ?, start_key_hash = ?, ended = ?, ended_at = ?, " +
		"version = ?, created_at = ?, updated_at = ? WHERE id = ? AND version = ?")
	if _, err := exec.ExecContext(ctx, update, saga.Type, saga.State, startKey, startHash, saga.Ended, endedAt,
		saga.Version, millis(saga.CreatedAt), millis(saga.UpdatedAt), saga.ID, expectedVersion); err != nil {
		return false, finish(storeError(err))
	}
	deleteQuery := s.bind("DELETE FROM snodra_saga_associations WHERE saga_id = ?")
	if _, err := exec.ExecContext(ctx, deleteQuery, saga.ID); err != nil {
		return false, finish(storeError(err))
	}
	if err := insertAssociations(ctx, exec, s, saga); err != nil {
		return false, finish(err)
	}
	return true, finish(nil)
}

// CountLiveByType is how many live instances there are per saga type, outside any transaction.
func (r sqlSagaRepository) CountLiveByType(ctx context.Context) (map[string]int64, error) {
	rows, err := r.store.db.QueryContext(ctx, "SELECT type, COUNT(*) FROM snodra_sagas WHERE ended = FALSE GROUP BY type")
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	counts := map[string]int64{}
	for rows.Next() {
		var typeName string
		var count int64
		if err := rows.Scan(&typeName, &count); err != nil {
			return nil, storeError(err)
		}
		counts[typeName] = count
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return counts, nil
}

// tryStartLock takes the PostgreSQL transaction-scoped advisory lock of a start key, failing at
// once when another live transaction holds it.
func tryStartLock(ctx context.Context, exec executor, s *Store, startKey string) (bool, error) {
	var got bool
	if err := exec.QueryRowContext(ctx, s.bind("SELECT pg_try_advisory_xact_lock(?, hashtext(?))"),
		int64(1397638226), sha256Hex(startKey)).Scan(&got); err != nil {
		return false, storeError(err)
	}
	return got, nil
}

// waitsAtMost runs work with MySQL's lock wait at most seconds, then restores it; a lock timeout
// is the retryable conflict.
func (s *Store) waitsAtMost(ctx context.Context, exec executor, seconds int, work func() error) error {
	var previous int64
	if err := exec.QueryRowContext(ctx, "SELECT @@SESSION.innodb_lock_wait_timeout").Scan(&previous); err != nil {
		return storeError(err)
	}
	if _, err := exec.ExecContext(ctx, fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", seconds)); err != nil {
		return storeError(err)
	}
	workErr := work()
	if _, err := exec.ExecContext(ctx, fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", previous)); err != nil && workErr == nil {
		return storeError(err)
	}
	return workErr
}

// checkSaga validates an instance's bounded fields.
func checkSaga(saga snodra.Saga) error {
	if err := checkText("a saga's id", saga.ID, maxID); err != nil {
		return err
	}
	if err := checkText("a saga's type", saga.Type, maxName); err != nil {
		return err
	}
	if err := checkText("a saga's state", saga.State, -1); err != nil {
		return err
	}
	if saga.StartKey != nil {
		if err := checkText("a saga's start key", *saga.StartKey, -1); err != nil {
			return err
		}
	}
	for _, association := range saga.Associations {
		if err := checkText("a saga association's key", association.Key, -1); err != nil {
			return err
		}
		if err := checkText("a saga association's value", association.Value, -1); err != nil {
			return err
		}
	}
	return nil
}

// associationHash is SHA-256 of key, a NUL byte and value, the layout's assoc_hash.
func associationHash(key, value string) string { return sha256Hex(key + "\x00" + value) }

// scanSaga reads one saga row.
func scanSaga(row rowScanner) (snodra.Saga, error) {
	var (
		saga               snodra.Saga
		startKey           stdSQL.NullString
		endedAt            stdSQL.NullTime
		createdAt, updated time.Time
	)
	if err := row.Scan(&saga.ID, &saga.Type, &saga.State, &startKey, &saga.Ended, &endedAt, &saga.Version, &createdAt, &updated); err != nil {
		return snodra.Saga{}, err
	}
	if startKey.Valid {
		value := startKey.String
		saga.StartKey = &value
	}
	if endedAt.Valid {
		saga.EndedAt = endedAt.Time
	}
	saga.CreatedAt = createdAt
	saga.UpdatedAt = updated
	return saga, nil
}

// scanSagas reads saga rows.
func scanSagas(rows *stdSQL.Rows) ([]snodra.Saga, error) {
	defer func() { _ = rows.Close() }()
	var sagas []snodra.Saga
	for rows.Next() {
		saga, err := scanSaga(rows)
		if err != nil {
			return nil, storeError(err)
		}
		sagas = append(sagas, saga)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return sagas, nil
}

// readAssociations reads the associations of ids, in idx order.
func readAssociations(ctx context.Context, exec executor, s *Store, ids []string) (map[string][]snodra.SagaAssociation, error) {
	if len(ids) == 0 {
		return map[string][]snodra.SagaAssociation{}, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	query := "SELECT saga_id, assoc_key, assoc_value FROM snodra_saga_associations WHERE saga_id IN (" + placeholders(len(ids)) + ") ORDER BY saga_id, idx"
	rows, err := exec.QueryContext(ctx, s.bind(query), args...)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	associations := map[string][]snodra.SagaAssociation{}
	for rows.Next() {
		var sagaID, key, value string
		if err := rows.Scan(&sagaID, &key, &value); err != nil {
			return nil, storeError(err)
		}
		associations[sagaID] = append(associations[sagaID], snodra.SagaAssociation{Key: key, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return associations, nil
}

// insertAssociations inserts a saga's associations in order.
func insertAssociations(ctx context.Context, exec executor, s *Store, saga snodra.Saga) error {
	if len(saga.Associations) == 0 {
		return nil
	}
	query := s.bind("INSERT INTO snodra_saga_associations (saga_id, idx, assoc_key, assoc_value, assoc_hash) VALUES (?, ?, ?, ?, ?)")
	for i, association := range saga.Associations {
		if _, err := exec.ExecContext(ctx, query, saga.ID, i, association.Key, association.Value, associationHash(association.Key, association.Value)); err != nil {
			return storeError(err)
		}
	}
	return nil
}

// sqlSagaDispatch is the snodra.SagaDispatchStore on snodra_saga_dispatch and its leases.
type sqlSagaDispatch struct {
	store *Store
}

var _ snodra.SagaDispatchStore = sqlSagaDispatch{}

// Record records entries in the transaction carried by ctx.
func (q sqlSagaDispatch) Record(ctx context.Context, entries []snodra.SagaDispatch) error {
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if err := checkText("a saga-dispatch entry's id", entry.ID, maxID); err != nil {
			return err
		}
		if err := checkText("a saga-dispatch entry's saga id", entry.SagaID, maxID); err != nil {
			return err
		}
		if err := checkText("a saga-dispatch entry's type", entry.Type, maxName); err != nil {
			return err
		}
		if err := checkText("a saga-dispatch entry's payload", entry.Payload, -1); err != nil {
			return err
		}
		if entry.Kind == snodra.DispatchEvent {
			if err := snodra.CheckOutboxPayload(entry.Type, []byte(entry.Payload)); err != nil {
				return err
			}
		}
	}
	s := q.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	query := s.bind(fmt.Sprintf("INSERT INTO snodra_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, routing_key, created_at, "+
		"dispatched_at, attempts, next_attempt_at, failed_at, failure_reason, failure_message, notice_pending, notice_attempts, notice_next_at) "+
		"VALUES (?, ?, ?, ?, ?, ?, CAST(? AS %s), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", s.jsonType()))
	for _, entry := range entries {
		headers, err := headersJSON(entry.Headers)
		if err != nil {
			return finish(err)
		}
		var routingKey, dispatchedAt, nextAttempt, failedAt, reason, message, noticeAttempts, noticeNext any
		if entry.RoutingKey != nil {
			routingKey = *entry.RoutingKey
		}
		if !entry.DispatchedAt.IsZero() {
			dispatchedAt = millis(entry.DispatchedAt)
		}
		if !entry.NextAttemptAt.IsZero() {
			nextAttempt = millis(entry.NextAttemptAt)
		}
		if !entry.FailedAt.IsZero() {
			failedAt = millis(entry.FailedAt)
		}
		if entry.FailureReason != "" {
			reason = string(entry.FailureReason)
		}
		if entry.FailureMessage != "" {
			message = entry.FailureMessage
		}
		if entry.NoticePending {
			noticeAttempts = entry.NoticeAttempts
		}
		if !entry.NoticeNextAt.IsZero() {
			noticeNext = millis(entry.NoticeNextAt)
		}
		if _, err := exec.ExecContext(ctx, query, entry.ID, entry.SagaID, entry.Seq, string(entry.Kind), entry.Type,
			entry.Payload, headers, routingKey, millis(entry.CreatedAt), dispatchedAt, entry.Attempts, nextAttempt,
			failedAt, reason, message, entry.NoticePending, noticeAttempts, noticeNext); err != nil {
			return finish(storeError(err))
		}
	}
	return finish(nil)
}

// DueSagas returns the ids of the sagas whose first pending entry is due, oldest first.
func (q sqlSagaDispatch) DueSagas(ctx context.Context, now time.Time, except []string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	s := q.store
	excluded := ""
	var args []any
	if len(except) > 0 {
		excluded = "AND d.saga_id NOT IN (" + placeholders(len(except)) + ") "
		for _, id := range except {
			args = append(args, id)
		}
	}
	args = append(args, millis(now), limit)
	query := s.bind("SELECT d.saga_id FROM snodra_saga_dispatch d " +
		"WHERE d.dispatched_at IS NULL AND d.failed_at IS NULL " + excluded +
		"AND d.seq = (SELECT MIN(x.seq) FROM snodra_saga_dispatch x WHERE x.saga_id = d.saga_id AND x.dispatched_at IS NULL AND x.failed_at IS NULL) " +
		"AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?) ORDER BY d.created_at, d.saga_id LIMIT ?")
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storeError(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return ids, nil
}

// NextPending returns the first pending entry of sagaID.
func (q sqlSagaDispatch) NextPending(ctx context.Context, sagaID string) (*snodra.SagaDispatch, error) {
	s := q.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := s.bind("SELECT " + s.dispatchColumns() + " FROM snodra_saga_dispatch WHERE saga_id = ? AND dispatched_at IS NULL AND failed_at IS NULL ORDER BY seq LIMIT 1")
	entry, err := scanDispatch(exec.QueryRowContext(ctx, query, sagaID))
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, finish(nil)
	}
	if err != nil {
		return nil, finish(storeError(err))
	}
	return &entry, finish(nil)
}

// MarkDispatched records the entry dispatched at now, in the transaction carried by ctx.
func (q sqlSagaDispatch) MarkDispatched(ctx context.Context, id string, now time.Time) error {
	s := q.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	result, err := exec.ExecContext(ctx, s.bind("UPDATE snodra_saga_dispatch SET dispatched_at = ? WHERE id = ?"), millis(now), id)
	if err != nil {
		return finish(storeError(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return finish(storeError(err))
	}
	if affected == 0 {
		return finish(fmt.Errorf("%w: saga-dispatch entry %q does not exist", snodra.ErrInvalid, id))
	}
	return finish(nil)
}

// ScheduleRetry records a transient failure.
func (q sqlSagaDispatch) ScheduleRetry(ctx context.Context, id string, attempts int, nextAttempt time.Time) error {
	return q.store.ownUpdate(ctx, "UPDATE snodra_saga_dispatch SET attempts = ?, next_attempt_at = ? WHERE id = ?", attempts, millis(nextAttempt), id)
}

// MarkFailed gives the entry up.
func (q sqlSagaDispatch) MarkFailed(ctx context.Context, id string, failure snodra.DispatchFailure) error {
	attempts := "attempts"
	args := []any{}
	if failure.Attempts != nil {
		attempts = "?"
		args = append(args, *failure.Attempts)
	}
	var noticeAt any
	pending := false
	if failure.NoticeAt != nil {
		pending = true
		noticeAt = millis(*failure.NoticeAt)
	}
	query := "UPDATE snodra_saga_dispatch SET attempts = " + attempts + ", failed_at = ?, failure_reason = ?, failure_message = ?, " +
		"notice_pending = ?, notice_attempts = 0, notice_next_at = ? WHERE id = ?"
	args = append(args, millis(failure.FailedAt), string(failure.Reason), cutFreeText(failure.Message), pending, noticeAt, id)
	return q.store.ownUpdate(ctx, query, args...)
}

// ClaimDueNotice claims the pending failure notice due first.
func (q sqlSagaDispatch) ClaimDueNotice(ctx context.Context, now, reserveUntil time.Time) (*snodra.SagaDispatch, error) {
	s := q.store
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return nil, err
	}
	selectQuery := s.bind("SELECT " + s.dispatchColumns() + " FROM snodra_saga_dispatch " +
		"WHERE notice_pending = TRUE AND (notice_next_at IS NULL OR notice_next_at <= ?) " +
		"ORDER BY CASE WHEN notice_next_at IS NULL THEN 0 ELSE 1 END, notice_next_at, id LIMIT 1 FOR UPDATE SKIP LOCKED")
	entry, err := scanDispatch(tx.QueryRowContext(ctx, selectQuery, millis(now)))
	if errors.Is(err, stdSQL.ErrNoRows) {
		_ = tx.Rollback()
		return nil, nil
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, s.bind("UPDATE snodra_saga_dispatch SET notice_next_at = ? WHERE id = ?"), millis(reserveUntil), entry.ID); err != nil {
		_ = tx.Rollback()
		return nil, storeError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, storeError(err)
	}
	entry.NoticeNextAt = millis(reserveUntil)
	return &entry, nil
}

// ClearNotice records that the failure notice was delivered.
func (q sqlSagaDispatch) ClearNotice(ctx context.Context, id string) error {
	return q.store.ownUpdate(ctx, "UPDATE snodra_saga_dispatch SET notice_pending = NULL, notice_attempts = NULL, notice_next_at = NULL WHERE id = ?", id)
}

// ScheduleNoticeRetry records that a delivery of the notice failed.
func (q sqlSagaDispatch) ScheduleNoticeRetry(ctx context.Context, id string, noticeAttempts int, next time.Time) error {
	return q.store.ownUpdate(ctx, "UPDATE snodra_saga_dispatch SET notice_attempts = ?, notice_next_at = ? WHERE id = ?", noticeAttempts, millis(next), id)
}

// ClaimLease takes the dispatch lease of sagaID for owner until until.
func (q sqlSagaDispatch) ClaimLease(ctx context.Context, sagaID, owner string, now, until time.Time) (bool, error) {
	s := q.store
	var claimed bool
	err := s.ownTx(ctx, func(tx *stdSQL.Tx) error {
		if s.driver == Postgres {
			query := s.bind("INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES (?, ?, ?) " +
				"ON CONFLICT (saga_id) DO UPDATE SET owner = EXCLUDED.owner, lease_until = EXCLUDED.lease_until " +
				"WHERE snodra_saga_dispatch_leases.lease_until < ?")
			result, err := tx.ExecContext(ctx, query, sagaID, owner, millis(until), millis(now))
			if err != nil {
				return storeError(err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return storeError(err)
			}
			claimed = affected == 1
			return nil
		}
		result, err := tx.ExecContext(ctx, s.bind("UPDATE snodra_saga_dispatch_leases SET owner = ?, lease_until = ? WHERE saga_id = ? AND lease_until < ?"),
			owner, millis(until), sagaID, millis(now))
		if err != nil {
			return storeError(err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return storeError(err)
		} else if affected == 1 {
			claimed = true
			return nil
		}
		_, err = tx.ExecContext(ctx, s.bind("INSERT INTO snodra_saga_dispatch_leases (saga_id, owner, lease_until) VALUES (?, ?, ?)"),
			sagaID, owner, millis(until))
		if err != nil {
			if isUniqueViolation(err) {
				return nil
			}
			return storeError(err)
		}
		claimed = true
		return nil
	})
	return claimed, err
}

// RenewLease moves the lease owner holds to until.
func (q sqlSagaDispatch) RenewLease(ctx context.Context, sagaID, owner string, until time.Time) (bool, error) {
	return q.store.affected(ctx, q.store.bind("UPDATE snodra_saga_dispatch_leases SET lease_until = ? WHERE saga_id = ? AND owner = ?"),
		millis(until), sagaID, owner)
}

// ReleaseLease lets go of the lease when owner holds it.
func (q sqlSagaDispatch) ReleaseLease(ctx context.Context, sagaID, owner string) error {
	return q.store.ownUpdate(ctx, "DELETE FROM snodra_saga_dispatch_leases WHERE saga_id = ? AND owner = ?", sagaID, owner)
}

// Pending is how many entries are pending.
func (q sqlSagaDispatch) Pending(ctx context.Context) (int64, error) {
	var pending int64
	if err := q.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM snodra_saga_dispatch WHERE dispatched_at IS NULL AND failed_at IS NULL").Scan(&pending); err != nil {
		return 0, storeError(err)
	}
	return pending, nil
}

// dispatchColumns is the SELECT list scanDispatch reads.
func (s *Store) dispatchColumns() string {
	return "id, saga_id, seq, kind, type, payload, CAST(headers AS " + s.textType() + "), routing_key, created_at, dispatched_at, " +
		"attempts, next_attempt_at, failed_at, failure_reason, failure_message, notice_pending, notice_attempts, notice_next_at"
}

// scanDispatch reads one dispatch row.
func scanDispatch(row rowScanner) (snodra.SagaDispatch, error) {
	var (
		entry                       snodra.SagaDispatch
		kind, headersText           string
		routingKey, reason, message stdSQL.NullString
		createdAt                   time.Time
		dispatchedAt, nextAttempt   stdSQL.NullTime
		failedAt, noticeNext        stdSQL.NullTime
		noticePending               stdSQL.NullBool
		noticeAttempts              stdSQL.NullInt64
	)
	// The caller binds the CAST of the headers column through dispatchQuery.
	if err := row.Scan(&entry.ID, &entry.SagaID, &entry.Seq, &kind, &entry.Type, &entry.Payload, &headersText,
		&routingKey, &createdAt, &dispatchedAt, &entry.Attempts, &nextAttempt, &failedAt, &reason, &message,
		&noticePending, &noticeAttempts, &noticeNext); err != nil {
		return snodra.SagaDispatch{}, err
	}
	entry.Kind = snodra.DispatchKind(kind)
	headers, err := headersFromJSON(headersText)
	if err != nil {
		return snodra.SagaDispatch{}, err
	}
	entry.Headers = headers
	if routingKey.Valid {
		value := routingKey.String
		entry.RoutingKey = &value
	}
	entry.CreatedAt = createdAt
	if dispatchedAt.Valid {
		entry.DispatchedAt = dispatchedAt.Time
	}
	if nextAttempt.Valid {
		entry.NextAttemptAt = nextAttempt.Time
	}
	if failedAt.Valid {
		entry.FailedAt = failedAt.Time
	}
	entry.FailureReason = snodra.FailureReason(reason.String)
	entry.FailureMessage = message.String
	if noticePending.Valid {
		entry.NoticePending = noticePending.Bool
	}
	if noticeAttempts.Valid {
		entry.NoticeAttempts = int(noticeAttempts.Int64)
	}
	if noticeNext.Valid {
		entry.NoticeNextAt = noticeNext.Time
	}
	return entry, nil
}

// sqlSagaDeadlines is the snodra.SagaDeadlineStore on snodra_deadlines.
type sqlSagaDeadlines struct {
	store *Store
}

var _ snodra.SagaDeadlineStore = sqlSagaDeadlines{}

// Insert inserts deadline.
func (d sqlSagaDeadlines) Insert(ctx context.Context, deadline snodra.SagaDeadline) error {
	if err := checkDeadline(deadline); err != nil {
		return err
	}
	s := d.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	var payload, payloadType, claimedBy, claimedUntil, finishedAt any
	if deadline.Payload != nil {
		payload = *deadline.Payload
	}
	if deadline.PayloadType != nil {
		payloadType = *deadline.PayloadType
	}
	if deadline.ClaimedBy != "" {
		claimedBy = deadline.ClaimedBy
	}
	if !deadline.ClaimedUntil.IsZero() {
		claimedUntil = millis(deadline.ClaimedUntil)
	}
	if !deadline.FinishedAt.IsZero() {
		finishedAt = millis(deadline.FinishedAt)
	}
	query := s.bind("INSERT INTO snodra_deadlines (id, saga_type, saga_id, name, payload, payload_type, due_at, correlation_id, status, " +
		"claimed_by, claimed_until, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if _, err := exec.ExecContext(ctx, query, deadline.ID, deadline.SagaType, deadline.SagaID, deadline.Name,
		payload, payloadType, millis(deadline.DueAt), deadline.CorrelationID, string(deadline.Status),
		claimedBy, claimedUntil, finishedAt); err != nil {
		return finish(storeError(err))
	}
	return finish(nil)
}

// FindScheduled returns the deadline when it is still SCHEDULED.
func (d sqlSagaDeadlines) FindScheduled(ctx context.Context, id string) (*snodra.SagaDeadline, error) {
	s := d.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := s.bind("SELECT " + s.deadlineColumns() + " FROM snodra_deadlines WHERE id = ? AND status = ?")
	deadline, err := scanDeadline(exec.QueryRowContext(ctx, query, id, string(snodra.DeadlineScheduled)))
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, finish(nil)
	}
	if err != nil {
		return nil, finish(storeError(err))
	}
	return &deadline, finish(nil)
}

// Cancel cancels the SCHEDULED deadline deadlineID of sagaID.
func (d sqlSagaDeadlines) Cancel(ctx context.Context, sagaID, deadlineID string, now time.Time) error {
	return d.cancelWhere(ctx, "id = ? AND saga_id = ?", now, deadlineID, sagaID)
}

// CancelByName cancels the SCHEDULED deadlines of sagaID named name.
func (d sqlSagaDeadlines) CancelByName(ctx context.Context, sagaID, name string, now time.Time) error {
	return d.cancelWhere(ctx, "saga_id = ? AND name = ?", now, sagaID, name)
}

// CancelAll cancels every SCHEDULED deadline of sagaID.
func (d sqlSagaDeadlines) CancelAll(ctx context.Context, sagaID string, now time.Time) error {
	return d.cancelWhere(ctx, "saga_id = ?", now, sagaID)
}

// cancelWhere cancels the SCHEDULED deadlines matching condition.
func (d sqlSagaDeadlines) cancelWhere(ctx context.Context, condition string, now time.Time, args ...any) error {
	s := d.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	query := s.bind("UPDATE snodra_deadlines SET status = ?, finished_at = ? WHERE " + condition + " AND status = ?")
	args = append([]any{string(snodra.DeadlineCancelled), millis(now)}, args...)
	args = append(args, string(snodra.DeadlineScheduled))
	if _, err := exec.ExecContext(ctx, query, args...); err != nil {
		return finish(storeError(err))
	}
	return finish(nil)
}

// MarkFired sets the SCHEDULED deadline id to FIRED.
func (d sqlSagaDeadlines) MarkFired(ctx context.Context, id string, now time.Time) (bool, error) {
	s := d.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := exec.ExecContext(ctx, s.bind("UPDATE snodra_deadlines SET status = ?, finished_at = ? WHERE id = ? AND status = ?"),
		string(snodra.DeadlineFired), millis(now), id, string(snodra.DeadlineScheduled))
	if err != nil {
		return false, finish(storeError(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, finish(storeError(err))
	}
	return affected == 1, finish(nil)
}

// ClaimDue claims the SCHEDULED deadline due first that nobody holds.
func (d sqlSagaDeadlines) ClaimDue(ctx context.Context, owner string, now, claimedUntil time.Time) (*snodra.SagaDeadline, error) {
	s := d.store
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return nil, err
	}
	query := s.bind("SELECT " + s.deadlineColumns() + " FROM snodra_deadlines WHERE status = ? AND due_at <= ? " +
		"AND (claimed_until IS NULL OR claimed_until < ?) ORDER BY due_at, id LIMIT 1 FOR UPDATE SKIP LOCKED")
	deadline, err := scanDeadline(tx.QueryRowContext(ctx, query, string(snodra.DeadlineScheduled), millis(now), millis(now)))
	if errors.Is(err, stdSQL.ErrNoRows) {
		_ = tx.Rollback()
		return nil, nil
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, s.bind("UPDATE snodra_deadlines SET claimed_by = ?, claimed_until = ? WHERE id = ?"),
		owner, millis(claimedUntil), deadline.ID); err != nil {
		_ = tx.Rollback()
		return nil, storeError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, storeError(err)
	}
	deadline.ClaimedBy = owner
	deadline.ClaimedUntil = millis(claimedUntil)
	return &deadline, nil
}

// CountOverdueByName is how many SCHEDULED deadlines are due per name.
func (d sqlSagaDeadlines) CountOverdueByName(ctx context.Context, now time.Time) (map[string]int64, error) {
	rows, err := d.store.db.QueryContext(ctx, d.store.bind("SELECT name, COUNT(*) FROM snodra_deadlines WHERE status = ? AND due_at <= ? GROUP BY name"),
		string(snodra.DeadlineScheduled), millis(now))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = rows.Close() }()
	counts := map[string]int64{}
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			return nil, storeError(err)
		}
		counts[name] = count
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return counts, nil
}

// deadlineColumns is the SELECT list scanDeadline reads.
func (s *Store) deadlineColumns() string {
	return "id, saga_type, saga_id, name, payload, payload_type, due_at, correlation_id, status, claimed_by, claimed_until, finished_at"
}

// checkDeadline validates a deadline's bounded fields.
func checkDeadline(deadline snodra.SagaDeadline) error {
	if err := checkText("a deadline's id", deadline.ID, maxID); err != nil {
		return err
	}
	if err := checkText("a deadline's saga type", deadline.SagaType, maxName); err != nil {
		return err
	}
	if err := checkText("a deadline's saga id", deadline.SagaID, maxID); err != nil {
		return err
	}
	if err := checkText("a deadline's name", deadline.Name, maxName); err != nil {
		return err
	}
	if deadline.CorrelationID != "" {
		if err := checkText("a deadline's correlation id", deadline.CorrelationID, -1); err != nil {
			return err
		}
	}
	return nil
}

// scanDeadline reads one deadline row.
func scanDeadline(row rowScanner) (snodra.SagaDeadline, error) {
	var (
		deadline                          snodra.SagaDeadline
		payload, payloadType, correlation stdSQL.NullString
		claimedBy                         stdSQL.NullString
		status                            string
		dueAt                             time.Time
		claimedUntil, finishedAt          stdSQL.NullTime
	)
	if err := row.Scan(&deadline.ID, &deadline.SagaType, &deadline.SagaID, &deadline.Name, &payload, &payloadType,
		&dueAt, &correlation, &status, &claimedBy, &claimedUntil, &finishedAt); err != nil {
		return snodra.SagaDeadline{}, err
	}
	if payload.Valid {
		value := payload.String
		deadline.Payload = &value
	}
	if payloadType.Valid {
		value := payloadType.String
		deadline.PayloadType = &value
	}
	deadline.DueAt = dueAt
	if correlation.Valid {
		deadline.CorrelationID = correlation.String
	}
	deadline.Status = snodra.DeadlineStatus(status)
	deadline.ClaimedBy = claimedBy.String
	if claimedUntil.Valid {
		deadline.ClaimedUntil = claimedUntil.Time
	}
	if finishedAt.Valid {
		deadline.FinishedAt = finishedAt.Time
	}
	return deadline, nil
}

// sqlSagaFailures is the snodra.SagaFailureStore on snodra_saga_failures.
type sqlSagaFailures struct {
	store *Store
}

var _ snodra.SagaFailureStore = sqlSagaFailures{}

// Find returns the failure, or nil when there is none.
func (f sqlSagaFailures) Find(ctx context.Context, id string) (*snodra.SagaFailure, error) {
	s := f.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := s.bind("SELECT " + s.failureColumns() + " FROM snodra_saga_failures WHERE id_hash = ?")
	failure, err := scanFailure(exec.QueryRowContext(ctx, query, sha256Hex(id)))
	if errors.Is(err, stdSQL.ErrNoRows) {
		return nil, finish(nil)
	}
	if err != nil {
		return nil, finish(storeError(err))
	}
	return &failure, finish(nil)
}

// Save inserts or replaces the failure with that id.
func (f sqlSagaFailures) Save(ctx context.Context, failure snodra.SagaFailure) error {
	if err := checkText("a saga failure's saga type", failure.SagaType, maxName); err != nil {
		return err
	}
	if err := checkText("a saga failure's id", failure.ID, -1); err != nil {
		return err
	}
	headers, err := headersJSON(failure.Headers)
	if err != nil {
		return err
	}
	s := f.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO snodra_saga_failures (id_hash, id, saga_type, event_id, event_type, payload, headers, error, "+
		"attempts, first_failed_at, last_failed_at) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS %s), ?, ?, ?, ?)", s.jsonType())
	set := "id = ?, saga_type = ?, event_id = ?, event_type = ?, payload = ?, headers = CAST(? AS " + s.jsonType() + "), error = ?, attempts = ?, first_failed_at = ?, last_failed_at = ?"
	switch s.driver {
	case Postgres:
		query += " ON CONFLICT (id_hash) DO UPDATE SET " + set
	case MySQL:
		query += " ON DUPLICATE KEY UPDATE " + mysqlFailureSet
	}
	args := []any{sha256Hex(failure.ID), failure.ID, failure.SagaType, failure.EventID, failure.EventType,
		failure.Payload, headers, cutFreeText(failure.Error), failure.Attempts, millis(failure.FirstFailedAt), millis(failure.LastFailedAt)}
	if s.driver == Postgres {
		args = append(args, failure.ID, failure.SagaType, failure.EventID, failure.EventType, failure.Payload, headers,
			cutFreeText(failure.Error), failure.Attempts, millis(failure.FirstFailedAt), millis(failure.LastFailedAt))
	}
	if _, err := exec.ExecContext(ctx, s.bind(query), args...); err != nil {
		return finish(storeError(err))
	}
	return finish(nil)
}

// mysqlFailureSet updates every column from the row MySQL tried to insert.
const mysqlFailureSet = "id = VALUES(id), saga_type = VALUES(saga_type), event_id = VALUES(event_id), event_type = VALUES(event_type), " +
	"payload = VALUES(payload), headers = VALUES(headers), error = VALUES(error), attempts = VALUES(attempts), " +
	"first_failed_at = VALUES(first_failed_at), last_failed_at = VALUES(last_failed_at)"

// Delete deletes the failure.
func (f sqlSagaFailures) Delete(ctx context.Context, id string) (bool, error) {
	s := f.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := exec.ExecContext(ctx, s.bind("DELETE FROM snodra_saga_failures WHERE id_hash = ?"), sha256Hex(id))
	if err != nil {
		return false, finish(storeError(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, finish(storeError(err))
	}
	return affected == 1, finish(nil)
}

// FailedAgain records that a retry of failure id failed again.
func (f sqlSagaFailures) FailedAgain(ctx context.Context, id, errText string, attempts int, now time.Time) (bool, error) {
	s := f.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := exec.ExecContext(ctx, s.bind("UPDATE snodra_saga_failures SET attempts = attempts + ?, error = ?, last_failed_at = ? WHERE id_hash = ?"),
		attempts, cutFreeText(errText), millis(now), sha256Hex(id))
	if err != nil {
		return false, finish(storeError(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, finish(storeError(err))
	}
	return affected == 1, finish(nil)
}

// List returns the failures of sagaType (every type when empty), oldest first.
func (f sqlSagaFailures) List(ctx context.Context, sagaType string) ([]snodra.SagaFailure, error) {
	s := f.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := "SELECT " + s.failureColumns() + " FROM snodra_saga_failures"
	args := []any{}
	if sagaType != "" {
		query += " WHERE saga_type = ?"
		args = append(args, sagaType)
	}
	query += " ORDER BY last_failed_at, id_hash"
	rows, err := exec.QueryContext(ctx, s.bind(query), args...)
	if err != nil {
		return nil, finish(storeError(err))
	}
	defer func() { _ = rows.Close() }()
	var failures []snodra.SagaFailure
	for rows.Next() {
		failure, err := scanFailure(rows)
		if err != nil {
			return nil, finish(storeError(err))
		}
		failures = append(failures, failure)
	}
	if err := rows.Err(); err != nil {
		return nil, finish(storeError(err))
	}
	return failures, finish(nil)
}

// failureColumns is the SELECT list scanFailure reads.
func (s *Store) failureColumns() string {
	return "id, saga_type, event_id, event_type, payload, CAST(headers AS " + s.textType() + "), error, attempts, first_failed_at, last_failed_at"
}

// scanFailure reads one failure row.
func scanFailure(row rowScanner) (snodra.SagaFailure, error) {
	var (
		failure     snodra.SagaFailure
		headersText string
		first, last time.Time
	)
	if err := row.Scan(&failure.ID, &failure.SagaType, &failure.EventID, &failure.EventType, &failure.Payload,
		&headersText, &failure.Error, &failure.Attempts, &first, &last); err != nil {
		return snodra.SagaFailure{}, err
	}
	headers, err := headersFromJSON(headersText)
	if err != nil {
		return snodra.SagaFailure{}, err
	}
	failure.Headers = headers
	failure.FirstFailedAt = first
	failure.LastFailedAt = last
	return failure, nil
}

// sqlSagaMarkers is the snodra.SagaMarkerStore in snodra_processed_events.
type sqlSagaMarkers struct {
	store *Store
}

var _ snodra.SagaMarkerStore = sqlSagaMarkers{}

// Exists reports whether the marker is recorded.
func (m sqlSagaMarkers) Exists(ctx context.Context, id string) (bool, error) {
	s := m.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return false, err
	}
	var count int64
	if err := exec.QueryRowContext(ctx, s.bind("SELECT COUNT(*) FROM snodra_processed_events WHERE id_hash = ?"), sha256Hex(id)).Scan(&count); err != nil {
		return false, finish(storeError(err))
	}
	return count > 0, finish(nil)
}

// Insert inserts the marker; ErrConflict when it exists.
func (m sqlSagaMarkers) Insert(ctx context.Context, id, typeName string, now time.Time) error {
	s := m.store
	exec, finish, err := s.scope(ctx)
	if err != nil {
		return err
	}
	query := s.bind("INSERT INTO snodra_processed_events (id_hash, id, type, processed_at) VALUES (?, ?, ?, ?)")
	if s.driver == Postgres {
		query += " ON CONFLICT (id_hash) DO NOTHING"
	}
	result, err := exec.ExecContext(ctx, query, sha256Hex(id), id, typeName, millis(now))
	if err != nil {
		if s.driver == MySQL && isUniqueViolation(err) {
			return finish(fmt.Errorf("%w: marker %q exists", snodra.ErrConflict, id))
		}
		return finish(storeError(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return finish(storeError(err))
	}
	if affected == 0 {
		return finish(fmt.Errorf("%w: marker %q exists", snodra.ErrConflict, id))
	}
	return finish(nil)
}
