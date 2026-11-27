package snodra

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// memorySagaLease is one saga's dispatch lease.
type memorySagaLease struct {
	owner string
	until time.Time
}

// memoryExternalCalls is MemoryMessageStore's ExternalCallStore. Every write commits on its own:
// a call that happened is never forgotten by a rolled-back handler.
type memoryExternalCalls struct {
	store *MemoryMessageStore
}

var _ ExternalCallStore = memoryExternalCalls{}

func (c memoryExternalCalls) direct(run func(*memoryData) error) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.initLocked()
	return run(&c.store.data)
}

// Insert inserts call; false when a record of its key exists.
func (c memoryExternalCalls) Insert(ctx context.Context, call ExternalCall) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	inserted := false
	err := c.direct(func(data *memoryData) error {
		if _, exists := data.external.get(nil, call.Key); exists {
			return nil
		}
		copied := copyExternalCall(call)
		data.external.set(nil, data, call.Key, &copied)
		inserted = true
		return nil
	})
	return inserted, err
}

// Find returns the record of key, or nil when there is none.
func (c memoryExternalCalls) Find(ctx context.Context, key string) (*ExternalCall, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var found *ExternalCall
	err := c.direct(func(data *memoryData) error {
		if call, ok := data.external.get(nil, key); ok {
			copied := copyExternalCall(*call)
			found = &copied
		}
		return nil
	})
	return found, err
}

// TakeOver takes the lapsed call seen over for owner.
func (c memoryExternalCalls) TakeOver(ctx context.Context, seen ExternalCall, owner string, leaseUntil, now time.Time) (*ExternalCall, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var taken *ExternalCall
	err := c.direct(func(data *memoryData) error {
		stored, ok := data.external.get(nil, seen.Key)
		if !ok || !sameCallRecord(*stored, seen) || stored.Status != ExternalCallInProgress {
			return nil
		}
		call := copyExternalCall(*stored)
		call.Owner = owner
		call.LeaseUntil = leaseUntil
		call.UpdatedAt = now
		call.Attempts++
		data.external.set(nil, data, seen.Key, &call)
		taken = &call
		return nil
	})
	return taken, err
}

// Expire gives the lapsed call seen up as UNKNOWN.
func (c memoryExternalCalls) Expire(ctx context.Context, seen ExternalCall, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	expired := false
	err := c.direct(func(data *memoryData) error {
		stored, ok := data.external.get(nil, seen.Key)
		if !ok || !sameCallRecord(*stored, seen) || stored.Status != ExternalCallInProgress {
			return nil
		}
		call := copyExternalCall(*stored)
		call.Status = ExternalCallUnknown
		call.Owner = ""
		call.LeaseUntil = time.Time{}
		call.UpdatedAt = now
		data.external.set(nil, data, seen.Key, &call)
		expired = true
		return nil
	})
	return expired, err
}

// Succeed settles the record owner owns as SUCCEEDED with reply.
func (c memoryExternalCalls) Succeed(ctx context.Context, key, owner, reply string, now time.Time) (bool, error) {
	return c.settle(ctx, key, owner, func(call *ExternalCall) {
		call.Status = ExternalCallSucceeded
		call.Reply = reply
		call.Error = ""
		call.Owner = ""
		call.LeaseUntil = time.Time{}
		call.UpdatedAt = now
	})
}

// Fail settles the record owner owns as FAILED with errText.
func (c memoryExternalCalls) Fail(ctx context.Context, key, owner, errText string, now time.Time) (bool, error) {
	return c.settle(ctx, key, owner, func(call *ExternalCall) {
		call.Status = ExternalCallFailed
		call.Error = errText
		call.Reply = ""
		call.Owner = ""
		call.LeaseUntil = time.Time{}
		call.UpdatedAt = now
	})
}

// MarkUnknown settles the record owner owns as UNKNOWN.
func (c memoryExternalCalls) MarkUnknown(ctx context.Context, key, owner string, now time.Time) (bool, error) {
	return c.settle(ctx, key, owner, func(call *ExternalCall) {
		call.Status = ExternalCallUnknown
		call.Reply = ""
		call.Error = ""
		call.Owner = ""
		call.LeaseUntil = time.Time{}
		call.UpdatedAt = now
	})
}

// Reset deletes the record owner owns.
func (c memoryExternalCalls) Reset(ctx context.Context, key, owner string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	removed := false
	err := c.direct(func(data *memoryData) error {
		call, ok := data.external.get(nil, key)
		if !ok || call.Status != ExternalCallInProgress || call.Owner != owner {
			return nil
		}
		data.external.set(nil, data, key, nil)
		removed = true
		return nil
	})
	return removed, err
}

// settle is Succeed, Fail and MarkUnknown: the owner's record only.
func (c memoryExternalCalls) settle(ctx context.Context, key, owner string, change func(*ExternalCall)) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	changed := false
	err := c.direct(func(data *memoryData) error {
		stored, ok := data.external.get(nil, key)
		if !ok || stored.Status != ExternalCallInProgress || stored.Owner != owner {
			return nil
		}
		call := copyExternalCall(*stored)
		change(&call)
		data.external.set(nil, data, key, &call)
		changed = true
		return nil
	})
	return changed, err
}

// sameCallRecord is the record a lapsed call's write must still see.
func sameCallRecord(stored, seen ExternalCall) bool {
	return stored.Key == seen.Key && stored.Owner == seen.Owner &&
		stored.LeaseUntil.Equal(seen.LeaseUntil)
}

// copyExternalCall copies a record.
func copyExternalCall(call ExternalCall) ExternalCall { return call }

// memorySagas is MemoryMessageStore's SagaRepository.
type memorySagas struct {
	store *MemoryMessageStore
}

var _ SagaRepository = memorySagas{}

// FindByID returns the instance, or nil when there is none.
func (r memorySagas) FindByID(ctx context.Context, id string) (*Saga, error) {
	var found *Saga
	_, err := r.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[Saga]
		if tx != nil {
			overlay = &tx.sagas
		}
		if saga, ok := data.sagas.get(overlay, id); ok {
			copied := copySaga(*saga)
			found = &copied
		}
		return nil
	})
	return found, err
}

// FindLive returns the live instances of typeName associated with key = value.
func (r memorySagas) FindLive(ctx context.Context, typeName, key, value string) ([]Saga, error) {
	var found []Saga
	_, err := r.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[Saga]
		if tx != nil {
			overlay = &tx.sagas
		}
		for _, saga := range data.sagas.snapshot(overlay) {
			if saga.Ended || saga.Type != typeName || !hasAssociation(saga, key, value) {
				continue
			}
			found = append(found, copySaga(saga))
		}
		sort.Slice(found, func(i, j int) bool {
			if !found[i].CreatedAt.Equal(found[j].CreatedAt) {
				return found[i].CreatedAt.Before(found[j].CreatedAt)
			}
			return found[i].ID < found[j].ID
		})
		return nil
	})
	return found, err
}

// Insert inserts a new instance; ErrConflict when its id exists or a live instance holds its
// StartKey.
func (r memorySagas) Insert(ctx context.Context, saga Saga) error {
	_, err := r.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[Saga]
		if tx != nil {
			overlay = &tx.sagas
		}
		if _, exists := data.sagas.get(overlay, saga.ID); exists {
			return fmt.Errorf("%w: saga %q exists", ErrConflict, saga.ID)
		}
		if saga.StartKey != nil && !saga.Ended {
			for _, other := range data.sagas.snapshot(overlay) {
				if !other.Ended && other.StartKey != nil && *other.StartKey == *saga.StartKey {
					return fmt.Errorf("%w: a live saga holds the start key", ErrConflict)
				}
			}
		}
		copied := copySaga(saga)
		data.sagas.set(overlay, data, saga.ID, &copied)
		return nil
	})
	return err
}

// Update replaces the stored instance when its Version is expectedVersion.
func (r memorySagas) Update(ctx context.Context, saga Saga, expectedVersion int64) (bool, error) {
	updated := false
	_, err := r.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[Saga]
		if tx != nil {
			overlay = &tx.sagas
		}
		stored, ok := data.sagas.get(overlay, saga.ID)
		if !ok || stored.Version != expectedVersion {
			return nil
		}
		copied := copySaga(saga)
		data.sagas.set(overlay, data, saga.ID, &copied)
		updated = true
		return nil
	})
	return updated, err
}

// CountLiveByType is how many live instances there are per saga type, outside any transaction.
func (r memorySagas) CountLiveByType(ctx context.Context) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	r.store.initLocked()
	counts := map[string]int64{}
	for _, saga := range r.store.data.sagas.rows {
		if !saga.value.Ended {
			counts[saga.value.Type]++
		}
	}
	return counts, nil
}

// memorySagaDispatch is MemoryMessageStore's SagaDispatchStore.
type memorySagaDispatch struct {
	store *MemoryMessageStore
}

var _ SagaDispatchStore = memorySagaDispatch{}

// Record records entries in the transaction carried by ctx.
func (q memorySagaDispatch) Record(ctx context.Context, entries []SagaDispatch) error {
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if entry.Kind == DispatchEvent {
			if err := CheckOutboxPayload(entry.Type, []byte(entry.Payload)); err != nil {
				return err
			}
		}
	}
	_, err := q.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDispatch]
		if tx != nil {
			overlay = &tx.dispatch
		}
		for _, entry := range entries {
			if _, exists := data.dispatch.get(overlay, entry.ID); exists {
				return fmt.Errorf("%w: saga-dispatch entry %q exists", ErrConflict, entry.ID)
			}
			copied := copyDispatch(entry)
			data.dispatch.set(overlay, data, entry.ID, &copied)
		}
		return nil
	})
	return err
}

// DueSagas returns the ids of the sagas whose first pending entry is due, oldest first.
func (q memorySagaDispatch) DueSagas(ctx context.Context, now time.Time, except []string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	excluded := make(map[string]bool, len(except))
	for _, id := range except {
		excluded[id] = true
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	type first struct {
		entry SagaDispatch
	}
	firsts := map[string]SagaDispatch{}
	for _, row := range q.store.data.dispatch.rows {
		entry := row.value
		if entry.DispatchedAt.IsZero() == false || entry.FailedAt.IsZero() == false {
			continue
		}
		current, seen := firsts[entry.SagaID]
		if !seen || entry.Seq < current.Seq {
			firsts[entry.SagaID] = entry
		}
	}
	candidates := make([]SagaDispatch, 0, len(firsts))
	for id, entry := range firsts {
		if excluded[id] {
			continue
		}
		if !entry.NextAttemptAt.IsZero() && entry.NextAttemptAt.After(now) {
			continue
		}
		candidates = append(candidates, entry)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].SagaID < candidates[j].SagaID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	ids := make([]string, len(candidates))
	for i, entry := range candidates {
		ids[i] = entry.SagaID
	}
	return ids, nil
}

// NextPending returns the first pending entry of sagaID.
func (q memorySagaDispatch) NextPending(ctx context.Context, sagaID string) (*SagaDispatch, error) {
	var found *SagaDispatch
	_, err := q.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDispatch]
		if tx != nil {
			overlay = &tx.dispatch
		}
		var best *SagaDispatch
		for _, entry := range data.dispatch.snapshot(overlay) {
			if entry.SagaID != sagaID || !entry.DispatchedAt.IsZero() || !entry.FailedAt.IsZero() {
				continue
			}
			if best == nil || entry.Seq < best.Seq {
				copied := copyDispatch(entry)
				best = &copied
			}
		}
		found = best
		return nil
	})
	return found, err
}

// MarkDispatched records the entry dispatched at now, in the transaction carried by ctx.
func (q memorySagaDispatch) MarkDispatched(ctx context.Context, id string, now time.Time) error {
	_, err := q.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDispatch]
		if tx != nil {
			overlay = &tx.dispatch
		}
		stored, ok := data.dispatch.get(overlay, id)
		if !ok {
			return fmt.Errorf("%w: saga-dispatch entry %q does not exist", ErrInvalid, id)
		}
		entry := copyDispatch(*stored)
		entry.DispatchedAt = now
		data.dispatch.set(overlay, data, id, &entry)
		return nil
	})
	return err
}

// modifyDispatch changes one committed entry, outside any transaction.
func (q memorySagaDispatch) modifyDispatch(ctx context.Context, id string, change func(*SagaDispatch)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	stored, ok := q.store.data.dispatch.get(nil, id)
	if !ok {
		return fmt.Errorf("%w: saga-dispatch entry %q does not exist", ErrInvalid, id)
	}
	entry := copyDispatch(*stored)
	change(&entry)
	q.store.data.dispatch.set(nil, &q.store.data, id, &entry)
	return nil
}

// ScheduleRetry records a transient failure.
func (q memorySagaDispatch) ScheduleRetry(ctx context.Context, id string, attempts int, nextAttempt time.Time) error {
	return q.modifyDispatch(ctx, id, func(entry *SagaDispatch) {
		entry.Attempts = attempts
		entry.NextAttemptAt = nextAttempt
	})
}

// MarkFailed gives the entry up.
func (q memorySagaDispatch) MarkFailed(ctx context.Context, id string, failure DispatchFailure) error {
	return q.modifyDispatch(ctx, id, func(entry *SagaDispatch) {
		if failure.Attempts != nil {
			entry.Attempts = *failure.Attempts
		}
		entry.FailedAt = failure.FailedAt
		entry.FailureReason = failure.Reason
		entry.FailureMessage = failure.Message
		if failure.NoticeAt != nil {
			entry.NoticePending = true
			entry.NoticeAttempts = 0
			entry.NoticeNextAt = *failure.NoticeAt
		} else {
			entry.NoticePending = false
			entry.NoticeAttempts = 0
			entry.NoticeNextAt = time.Time{}
		}
	})
}

// ClaimDueNotice claims the pending failure notice due first.
func (q memorySagaDispatch) ClaimDueNotice(ctx context.Context, now, reserveUntil time.Time) (*SagaDispatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	var chosen *SagaDispatch
	for _, row := range q.store.data.dispatch.rows {
		entry := row.value
		if !entry.NoticePending {
			continue
		}
		if !entry.NoticeNextAt.IsZero() && entry.NoticeNextAt.After(now) {
			continue
		}
		if chosen == nil || earlierNotice(entry, *chosen) {
			copied := entry
			chosen = &copied
		}
	}
	if chosen == nil {
		return nil, nil
	}
	chosen.NoticeNextAt = reserveUntil
	copied := copyDispatch(*chosen)
	q.store.data.dispatch.set(nil, &q.store.data, chosen.ID, &copied)
	return &copied, nil
}

// earlierNotice orders notices: one with no next time first, then by next time, then id.
func earlierNotice(a, b SagaDispatch) bool {
	if a.NoticeNextAt.IsZero() != b.NoticeNextAt.IsZero() {
		return a.NoticeNextAt.IsZero()
	}
	if !a.NoticeNextAt.Equal(b.NoticeNextAt) {
		return a.NoticeNextAt.Before(b.NoticeNextAt)
	}
	return a.ID < b.ID
}

// ClearNotice records that the failure notice was delivered.
func (q memorySagaDispatch) ClearNotice(ctx context.Context, id string) error {
	return q.modifyDispatch(ctx, id, func(entry *SagaDispatch) {
		entry.NoticePending = false
		entry.NoticeAttempts = 0
		entry.NoticeNextAt = time.Time{}
	})
}

// ScheduleNoticeRetry records that a delivery of the notice failed.
func (q memorySagaDispatch) ScheduleNoticeRetry(ctx context.Context, id string, noticeAttempts int, next time.Time) error {
	return q.modifyDispatch(ctx, id, func(entry *SagaDispatch) {
		entry.NoticeAttempts = noticeAttempts
		entry.NoticeNextAt = next
	})
}

// ClaimLease takes the dispatch lease of sagaID for owner until until.
func (q memorySagaDispatch) ClaimLease(ctx context.Context, sagaID, owner string, now, until time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	if lease, ok := q.store.data.leases.get(nil, sagaID); ok && !lease.until.Before(now) {
		return false, nil
	}
	lease := memorySagaLease{owner: owner, until: until}
	q.store.data.leases.set(nil, &q.store.data, sagaID, &lease)
	return true, nil
}

// RenewLease moves the lease owner holds to until.
func (q memorySagaDispatch) RenewLease(ctx context.Context, sagaID, owner string, until time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	lease, ok := q.store.data.leases.get(nil, sagaID)
	if !ok || lease.owner != owner {
		return false, nil
	}
	lease.until = until
	copied := *lease
	q.store.data.leases.set(nil, &q.store.data, sagaID, &copied)
	return true, nil
}

// ReleaseLease lets go of the lease when owner holds it.
func (q memorySagaDispatch) ReleaseLease(ctx context.Context, sagaID, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	lease, ok := q.store.data.leases.get(nil, sagaID)
	if ok && lease.owner == owner {
		q.store.data.leases.set(nil, &q.store.data, sagaID, nil)
	}
	return nil
}

// Pending is how many entries are pending.
func (q memorySagaDispatch) Pending(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	q.store.initLocked()
	var count int64
	for _, row := range q.store.data.dispatch.rows {
		if row.value.DispatchedAt.IsZero() && row.value.FailedAt.IsZero() {
			count++
		}
	}
	return count, nil
}

// memorySagaDeadlines is MemoryMessageStore's SagaDeadlineStore.
type memorySagaDeadlines struct {
	store *MemoryMessageStore
}

var _ SagaDeadlineStore = memorySagaDeadlines{}

// Insert inserts deadline.
func (d memorySagaDeadlines) Insert(ctx context.Context, deadline SagaDeadline) error {
	_, err := d.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDeadline]
		if tx != nil {
			overlay = &tx.deadlines
		}
		if _, exists := data.deadlines.get(overlay, deadline.ID); exists {
			return fmt.Errorf("%w: deadline %q exists", ErrConflict, deadline.ID)
		}
		copied := copyDeadline(deadline)
		data.deadlines.set(overlay, data, deadline.ID, &copied)
		return nil
	})
	return err
}

// FindScheduled returns the deadline when it is still SCHEDULED.
func (d memorySagaDeadlines) FindScheduled(ctx context.Context, id string) (*SagaDeadline, error) {
	var found *SagaDeadline
	_, err := d.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDeadline]
		if tx != nil {
			overlay = &tx.deadlines
		}
		if deadline, ok := data.deadlines.get(overlay, id); ok && deadline.Status == DeadlineScheduled {
			copied := copyDeadline(*deadline)
			found = &copied
		}
		return nil
	})
	return found, err
}

// Cancel cancels the SCHEDULED deadline deadlineID of sagaID.
func (d memorySagaDeadlines) Cancel(ctx context.Context, sagaID, deadlineID string, now time.Time) error {
	return d.cancelWhere(ctx, now, func(deadline SagaDeadline) bool {
		return deadline.SagaID == sagaID && deadline.ID == deadlineID
	})
}

// CancelByName cancels the SCHEDULED deadlines of sagaID named name.
func (d memorySagaDeadlines) CancelByName(ctx context.Context, sagaID, name string, now time.Time) error {
	return d.cancelWhere(ctx, now, func(deadline SagaDeadline) bool {
		return deadline.SagaID == sagaID && deadline.Name == name
	})
}

// CancelAll cancels every SCHEDULED deadline of sagaID.
func (d memorySagaDeadlines) CancelAll(ctx context.Context, sagaID string, now time.Time) error {
	return d.cancelWhere(ctx, now, func(deadline SagaDeadline) bool {
		return deadline.SagaID == sagaID
	})
}

// cancelWhere cancels the scheduled deadlines matching match.
func (d memorySagaDeadlines) cancelWhere(ctx context.Context, now time.Time, match func(SagaDeadline) bool) error {
	_, err := d.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDeadline]
		if tx != nil {
			overlay = &tx.deadlines
		}
		for _, deadline := range data.deadlines.snapshot(overlay) {
			if deadline.Status != DeadlineScheduled || !match(deadline) {
				continue
			}
			deadline.Status = DeadlineCancelled
			deadline.FinishedAt = now
			copied := copyDeadline(deadline)
			data.deadlines.set(overlay, data, deadline.ID, &copied)
		}
		return nil
	})
	return err
}

// MarkFired sets the SCHEDULED deadline id to FIRED.
func (d memorySagaDeadlines) MarkFired(ctx context.Context, id string, now time.Time) (bool, error) {
	fired := false
	_, err := d.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaDeadline]
		if tx != nil {
			overlay = &tx.deadlines
		}
		stored, ok := data.deadlines.get(overlay, id)
		if !ok || stored.Status != DeadlineScheduled {
			return nil
		}
		deadline := copyDeadline(*stored)
		deadline.Status = DeadlineFired
		deadline.FinishedAt = now
		data.deadlines.set(overlay, data, id, &deadline)
		fired = true
		return nil
	})
	return fired, err
}

// ClaimDue claims the SCHEDULED deadline due first that nobody holds.
func (d memorySagaDeadlines) ClaimDue(ctx context.Context, owner string, now, claimedUntil time.Time) (*SagaDeadline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.store.mu.Lock()
	defer d.store.mu.Unlock()
	d.store.initLocked()
	var chosen *SagaDeadline
	for _, row := range d.store.data.deadlines.rows {
		deadline := row.value
		if deadline.Status != DeadlineScheduled || deadline.DueAt.After(now) {
			continue
		}
		if !deadline.ClaimedUntil.IsZero() && !deadline.ClaimedUntil.Before(now) {
			continue
		}
		if chosen == nil || deadline.DueAt.Before(chosen.DueAt) ||
			(deadline.DueAt.Equal(chosen.DueAt) && deadline.ID < chosen.ID) {
			copied := deadline
			chosen = &copied
		}
	}
	if chosen == nil {
		return nil, nil
	}
	chosen.ClaimedBy = owner
	chosen.ClaimedUntil = claimedUntil
	copied := copyDeadline(*chosen)
	d.store.data.deadlines.set(nil, &d.store.data, chosen.ID, &copied)
	return &copied, nil
}

// CountOverdueByName is how many SCHEDULED deadlines are due per name.
func (d memorySagaDeadlines) CountOverdueByName(ctx context.Context, now time.Time) (map[string]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.store.mu.Lock()
	defer d.store.mu.Unlock()
	d.store.initLocked()
	counts := map[string]int64{}
	for _, row := range d.store.data.deadlines.rows {
		if row.value.Status == DeadlineScheduled && !row.value.DueAt.After(now) {
			counts[row.value.Name]++
		}
	}
	return counts, nil
}

// memorySagaFailures is MemoryMessageStore's SagaFailureStore.
type memorySagaFailures struct {
	store *MemoryMessageStore
}

var _ SagaFailureStore = memorySagaFailures{}

// Find returns the failure, or nil when there is none.
func (f memorySagaFailures) Find(ctx context.Context, id string) (*SagaFailure, error) {
	var found *SagaFailure
	_, err := f.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaFailure]
		if tx != nil {
			overlay = &tx.failures
		}
		if failure, ok := data.failures.get(overlay, id); ok {
			copied := copyFailure(*failure)
			found = &copied
		}
		return nil
	})
	return found, err
}

// Save inserts or replaces the failure with that id.
func (f memorySagaFailures) Save(ctx context.Context, failure SagaFailure) error {
	_, err := f.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaFailure]
		if tx != nil {
			overlay = &tx.failures
		}
		copied := copyFailure(failure)
		data.failures.set(overlay, data, failure.ID, &copied)
		return nil
	})
	return err
}

// Delete deletes the failure.
func (f memorySagaFailures) Delete(ctx context.Context, id string) (bool, error) {
	deleted := false
	_, err := f.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaFailure]
		if tx != nil {
			overlay = &tx.failures
		}
		if _, ok := data.failures.get(overlay, id); !ok {
			return nil
		}
		data.failures.set(overlay, data, id, nil)
		deleted = true
		return nil
	})
	return deleted, err
}

// FailedAgain records that a retry of failure id failed again.
func (f memorySagaFailures) FailedAgain(ctx context.Context, id, errText string, attempts int, now time.Time) (bool, error) {
	changed := false
	_, err := f.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaFailure]
		if tx != nil {
			overlay = &tx.failures
		}
		stored, ok := data.failures.get(overlay, id)
		if !ok {
			return nil
		}
		failure := copyFailure(*stored)
		failure.Attempts += attempts
		failure.Error = errText
		failure.LastFailedAt = now
		data.failures.set(overlay, data, id, &failure)
		changed = true
		return nil
	})
	return changed, err
}

// List returns the failures of sagaType (every type when empty), oldest first.
func (f memorySagaFailures) List(ctx context.Context, sagaType string) ([]SagaFailure, error) {
	var found []SagaFailure
	_, err := f.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[SagaFailure]
		if tx != nil {
			overlay = &tx.failures
		}
		for _, failure := range data.failures.snapshot(overlay) {
			if sagaType == "" || failure.SagaType == sagaType {
				found = append(found, copyFailure(failure))
			}
		}
		sort.Slice(found, func(i, j int) bool {
			if !found[i].LastFailedAt.Equal(found[j].LastFailedAt) {
				return found[i].LastFailedAt.Before(found[j].LastFailedAt)
			}
			return found[i].ID < found[j].ID
		})
		return nil
	})
	return found, err
}

// memorySagaMarkers is MemoryMessageStore's SagaMarkerStore, in the processed-event table.
type memorySagaMarkers struct {
	store *MemoryMessageStore
}

var _ SagaMarkerStore = memorySagaMarkers{}

// Exists reports whether the marker is recorded.
func (m memorySagaMarkers) Exists(ctx context.Context, id string) (bool, error) {
	exists := false
	_, err := m.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[ProcessedMarker]
		if tx != nil {
			overlay = &tx.processed
		}
		_, exists = data.processed.get(overlay, id)
		return nil
	})
	return exists, err
}

// Insert inserts the marker; ErrConflict when it exists.
func (m memorySagaMarkers) Insert(ctx context.Context, id, typeName string, now time.Time) error {
	_, err := m.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[ProcessedMarker]
		if tx != nil {
			overlay = &tx.processed
		}
		if _, exists := data.processed.get(overlay, id); exists {
			return fmt.Errorf("%w: marker %q exists", ErrConflict, id)
		}
		marker := ProcessedMarker{ID: id, Type: typeName, ProcessedAt: now}
		data.processed.set(overlay, data, id, &marker)
		return nil
	})
	return err
}

// hasAssociation reports whether saga has key = value.
func hasAssociation(saga Saga, key, value string) bool {
	for _, association := range saga.Associations {
		if association.Key == key && association.Value == value {
			return true
		}
	}
	return false
}

// copySaga copies the parts a caller could change behind the store's back.
func copySaga(saga Saga) Saga {
	copied := saga
	if saga.StartKey != nil {
		value := *saga.StartKey
		copied.StartKey = &value
	}
	copied.Associations = append([]SagaAssociation(nil), saga.Associations...)
	return copied
}

// copyDispatch copies a dispatch entry.
func copyDispatch(entry SagaDispatch) SagaDispatch {
	copied := entry
	if entry.RoutingKey != nil {
		value := *entry.RoutingKey
		copied.RoutingKey = &value
	}
	if entry.Headers != nil {
		copied.Headers = make(map[string]string, len(entry.Headers))
		for name, value := range entry.Headers {
			copied.Headers[name] = value
		}
	}
	return copied
}

// copyDeadline copies a deadline.
func copyDeadline(deadline SagaDeadline) SagaDeadline {
	copied := deadline
	if deadline.Payload != nil {
		value := *deadline.Payload
		copied.Payload = &value
	}
	if deadline.PayloadType != nil {
		value := *deadline.PayloadType
		copied.PayloadType = &value
	}
	return copied
}

// copyFailure copies a failure.
func copyFailure(failure SagaFailure) SagaFailure {
	copied := failure
	if failure.Headers != nil {
		copied.Headers = make(map[string]string, len(failure.Headers))
		for name, value := range failure.Headers {
			copied.Headers[name] = value
		}
	}
	return copied
}
