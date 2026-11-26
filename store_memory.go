package snodra

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	// MemoryMessageStoreName is the in-memory message store's name (MessageStore.Name).
	MemoryMessageStoreName = "memory"
)

// MemoryMessageStore is the in-memory message store: the whole message-store contract inside
// one process, for tests, local runs and single-process demos, with no database and no
// infrastructure. It implements the outbox and the processed-event markers; the dead letters,
// processor control, external calls and saga ports of this release return ErrUnsupported (the
// official add-ons implement every store by level 4, and this store follows).
//
//   - Real transactions: Begin hands out a transaction that collects its writes; other readers
//     see them only once it commits, and a rollback leaves nothing. Reads in the transaction see
//     its own writes. A transaction reserves every record it writes: a second live transaction
//     writing one of them fails at once with ErrConflict, and a commit fails with ErrConflict
//     when a record it wrote changed meanwhile (a write outside any transaction). What runs
//     outside a transaction commits at once. A transaction must always end with Commit or
//     Rollback: one abandoned without either keeps its reservations until the process ends.
//   - Claims and leases are atomic: of two claimers, one wins. The times the store stamps itself
//     are the application's clock (UseClock); until it is set, the wall clock.
//   - A committed outbox append calls every OnAppend waker, so the relay publishes at once
//     rather than at its next poll.
//   - It has no schema and nothing in the background: StoreLifecycle's InitSchema and CheckSchema
//     do nothing, and Background waits for the app to end.
//   - Nothing is ever deleted but what the explicit Purge and DeleteMarkers calls remove: it keeps
//     everything else it is given until the process ends. It is not durable: one process,
//     everything lost when it ends. A service builder warns at startup that it is not durable.
//
// Every reference to one MemoryMessageStore is that store. Do not copy the struct; use the
// pointer the constructor returns.
//
// A MemoryMessageStore is safe for concurrent use.
type MemoryMessageStore struct {
	mu    sync.Mutex
	data  memoryData
	clock func() time.Time

	nextTx    uint64
	nextWaker uint64
	wakers    map[uint64]func()
}

// NewMemoryMessageStore returns an empty in-memory message store.
func NewMemoryMessageStore() *MemoryMessageStore {
	store := &MemoryMessageStore{}
	store.initLocked()
	return store
}

// Name returns "memory".
func (s *MemoryMessageStore) Name() string { return MemoryMessageStoreName }

// UseClock sets the application's clock: what the store stamps the times it sets itself with (an
// outbox entry's sending, a claim's lease). A nil clock restores the wall clock. The clock must
// not call the store.
func (s *MemoryMessageStore) UseClock(now func() time.Time) {
	s.mu.Lock()
	if now == nil {
		now = time.Now
	}
	s.clock = now
	s.mu.Unlock()
}

// InitSchema does nothing: the in-memory store needs no schema.
func (s *MemoryMessageStore) InitSchema(context.Context) error { return nil }

// CheckSchema does nothing: the in-memory store needs no schema.
func (s *MemoryMessageStore) CheckSchema(context.Context) error { return nil }

// Background runs nothing: it waits until ctx is done, as the lifecycle's background work lives
// as long as the app.
func (s *MemoryMessageStore) Background(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Begin starts a transaction and returns a context that carries it.
func (s *MemoryMessageStore) Begin(ctx context.Context) (context.Context, Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	s.initLocked()
	s.nextTx++
	tx := &memoryTx{store: s, id: s.nextTx}
	s.mu.Unlock()
	return context.WithValue(ctx, memoryTxKey{}, tx), tx, nil
}

// Outbox returns the outbox.
func (s *MemoryMessageStore) Outbox() OutboxStore { return memoryOutbox{store: s} }

// Processed returns the processed-event markers.
func (s *MemoryMessageStore) Processed() ProcessedStore { return memoryProcessed{store: s} }

// DeadLetters returns the dead-letter port, which this store does not implement yet: every
// operation fails with ErrUnsupported.
func (s *MemoryMessageStore) DeadLetters() DeadLetterStore { return memoryDeadLetters{} }

// Control returns the processor-control port, which this store does not implement yet: every
// operation fails with ErrUnsupported.
func (s *MemoryMessageStore) Control() ProcessorControlStore { return memoryControl{} }

// ExternalCalls returns the external-call port, which this store does not implement yet: every
// operation fails with ErrUnsupported.
func (s *MemoryMessageStore) ExternalCalls() ExternalCallStore { return memoryExternalCalls{} }

// Sagas returns the saga ports, which this store does not implement yet: every operation fails
// with ErrUnsupported.
func (s *MemoryMessageStore) Sagas() SagaStores {
	return SagaStores{
		Sagas:     memorySagas{},
		Dispatch:  memorySagaDispatch{},
		Deadlines: memorySagaDeadlines{},
		Failures:  memorySagaFailures{},
		Markers:   memorySagaMarkers{},
	}
}

// initLocked gives a zero value the maps of NewMemoryMessageStore, so a zero MemoryMessageStore
// is usable too.
func (s *MemoryMessageStore) initLocked() {
	if s.data.outbox.rows == nil {
		s.data.outbox.rows = make(map[string]*memoryTableRow[memoryOutboxRow])
	}
	if s.data.processed.rows == nil {
		s.data.processed.rows = make(map[string]*memoryTableRow[ProcessedMarker])
	}
	if s.data.reserved == nil {
		s.data.reserved = make(map[memoryRecordKey]uint64)
	}
	if s.wakers == nil {
		s.wakers = make(map[uint64]func())
	}
}

// nowLocked is the application's clock.
func (s *MemoryMessageStore) nowLocked() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// wake calls every OnAppend waker, outside the data lock.
func (s *MemoryMessageStore) wake() {
	s.mu.Lock()
	wakers := make([]func(), 0, len(s.wakers))
	for _, wake := range s.wakers {
		wakers = append(wakers, wake)
	}
	s.mu.Unlock()
	for _, wake := range wakers {
		wake()
	}
}

// unsupportedMemory is the refusal of a store this release does not implement.
func unsupportedMemory(what string) error {
	return fmt.Errorf("%w: the in-memory message store does not implement %s", ErrUnsupported, what)
}

// ---- tables and transactions -------------------------------------------------------------------

// memoryTable is one collection, each record with the version of its last committed write.
type memoryTable[T any] struct {
	rows map[string]*memoryTableRow[T]
}

type memoryTableRow[T any] struct {
	version uint64
	value   T
}

// memoryOverlay is a transaction's writes to one table: per id, the version it saw (0: absent)
// and what it wrote (nil: deleted).
type memoryOverlay[T any] struct {
	writes map[string]memoryWrite[T]
}

type memoryWrite[T any] struct {
	base  uint64
	value *T
}

// get returns the record id as overlay sees it (the committed one without).
func (t *memoryTable[T]) get(overlay *memoryOverlay[T], id string) (*T, bool) {
	if overlay != nil {
		if write, ok := overlay.writes[id]; ok {
			if write.value == nil {
				return nil, false
			}
			return write.value, true
		}
	}
	if row, ok := t.rows[id]; ok {
		return &row.value, true
	}
	return nil, false
}

// set writes value (nil: deletes) as id: into overlay, or committed at once.
func (t *memoryTable[T]) set(overlay *memoryOverlay[T], data *memoryData, id string, value *T) {
	if overlay == nil {
		data.version++
		if value == nil {
			delete(t.rows, id)
			return
		}
		t.rows[id] = &memoryTableRow[T]{version: data.version, value: *value}
		return
	}
	if overlay.writes == nil {
		overlay.writes = make(map[string]memoryWrite[T])
	}
	write, ok := overlay.writes[id]
	if !ok {
		if row, committed := t.rows[id]; committed {
			write.base = row.version
		}
	}
	write.value = value
	overlay.writes[id] = write
}

// stale is the first write of overlay whose record changed since the transaction saw it.
func (t *memoryTable[T]) stale(overlay *memoryOverlay[T]) (string, bool) {
	for id, write := range overlay.writes {
		var version uint64
		if row, ok := t.rows[id]; ok {
			version = row.version
		}
		if version != write.base {
			return id, true
		}
	}
	return "", false
}

// apply commits the writes of overlay.
func (t *memoryTable[T]) apply(overlay *memoryOverlay[T], data *memoryData) {
	for id, write := range overlay.writes {
		t.set(nil, data, id, write.value)
	}
}

// memoryOutboxRow is one outbox entry and the relay's marks on it.
type memoryOutboxRow struct {
	entry      OutboxEntry
	sentAt     time.Time
	failedAt   time.Time
	owner      string
	leaseUntil time.Time
}

// pending reports whether the entry is neither sent nor failed.
func (r memoryOutboxRow) pending() bool { return r.sentAt.IsZero() && r.failedAt.IsZero() }

// memoryRecordKey names one record across the tables, for a transaction's reservations.
type memoryRecordKey struct {
	table string
	id    string
}

// memoryData is everything the store holds.
type memoryData struct {
	// version counts every committed write: a record's version is the count at its last write.
	version   uint64
	outbox    memoryTable[memoryOutboxRow]
	processed memoryTable[ProcessedMarker]
	// reserved maps a record a live transaction wrote to that transaction's id.
	reserved map[memoryRecordKey]uint64
}

// memoryTx is a transaction of a MemoryMessageStore, held in a Tx: its writes, kept apart until
// the commit.
type memoryTx struct {
	store *MemoryMessageStore
	id    uint64
	ended bool
	// doomed: a write met a record another live transaction reserved, so the commit fails.
	doomed bool
	// reserved are the records this transaction reserved, for their release.
	reserved  []memoryRecordKey
	outbox    memoryOverlay[memoryOutboxRow]
	processed memoryOverlay[ProcessedMarker]
}

// memoryTxKey is the context key of a transaction.
type memoryTxKey struct{}

// memoryTxOf returns the MemoryMessageStore transaction the context carries, if any.
func memoryTxOf(ctx context.Context) *memoryTx {
	tx, _ := ctx.Value(memoryTxKey{}).(*memoryTx)
	return tx
}

// written is the (table, id) of every record the transaction wrote.
func (tx *memoryTx) written() []memoryRecordKey {
	keys := make([]memoryRecordKey, 0, len(tx.outbox.writes)+len(tx.processed.writes))
	for id := range tx.outbox.writes {
		keys = append(keys, memoryRecordKey{"outbox", id})
	}
	for id := range tx.processed.writes {
		keys = append(keys, memoryRecordKey{"processed_events", id})
	}
	return keys
}

// clear forgets the writes (a rollback).
func (tx *memoryTx) clear() {
	tx.outbox = memoryOverlay[memoryOutboxRow]{}
	tx.processed = memoryOverlay[ProcessedMarker]{}
}

// reserve reserves for tx every record it wrote; a conflict when another live transaction holds
// one.
// TODO(spec-gap): SG-31 (an abandoned transaction keeps its reservations until the process ends;
// the reference reserves the row until its transaction ends too, and conflicts the second writer,
// snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryTables.kt:77).
func (d *memoryData) reserve(tx *memoryTx) error {
	for _, key := range tx.written() {
		holder, held := d.reserved[key]
		if !held {
			d.reserved[key] = tx.id
			tx.reserved = append(tx.reserved, key)
			continue
		}
		if holder == tx.id {
			continue
		}
		return fmt.Errorf("%w: another transaction writes %s record %q",
			ErrConflict, key.table, key.id)
	}
	return nil
}

// release lets go of tx's reservations.
func (d *memoryData) release(tx *memoryTx) {
	for _, key := range tx.reserved {
		if d.reserved[key] == tx.id {
			delete(d.reserved, key)
		}
	}
	tx.reserved = nil
}

// commit writes tx's writes, all or nothing: a conflict when a record it wrote changed since it
// saw it.
func (d *memoryData) commit(tx *memoryTx) error {
	if id, stale := d.outbox.stale(&tx.outbox); stale {
		return fmt.Errorf("%w: outbox record %q changed since the transaction read it",
			ErrConflict, id)
	}
	if id, stale := d.processed.stale(&tx.processed); stale {
		return fmt.Errorf("%w: processed_events record %q changed since the transaction read it",
			ErrConflict, id)
	}
	d.outbox.apply(&tx.outbox, d)
	d.processed.apply(&tx.processed, d)
	return nil
}

// withTx runs run on the data, and on the transaction the context carries when it is one of this
// store's live transactions, then reserves the records run wrote. With no transaction (or an
// ended one) the writes commit at once. committed reports that case.
func (s *MemoryMessageStore) withTx(ctx context.Context, run func(*memoryData, *memoryTx) error) (committed bool, err error) {
	tx := memoryTxOf(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if tx == nil || tx.ended {
		return true, run(&s.data, nil)
	}
	if tx.store != s {
		return false, fmt.Errorf("%w: the context carries another message store's transaction",
			ErrInvalid)
	}
	err = run(&s.data, tx)
	if reserved := s.data.reserve(tx); reserved != nil {
		tx.doomed = true
		return false, reserved
	}
	return false, err
}

// Commit writes the transaction's writes, all or nothing.
func (tx *memoryTx) Commit(ctx context.Context) error {
	store := tx.store
	store.mu.Lock()
	if tx.ended {
		store.mu.Unlock()
		return nil
	}
	tx.ended = true
	var err error
	if tx.doomed {
		err = fmt.Errorf("%w: the transaction met a record another one was writing",
			ErrConflict)
	} else {
		err = store.data.commit(tx)
	}
	store.data.release(tx)
	appended := err == nil && len(tx.outbox.writes) > 0
	tx.clear()
	store.mu.Unlock()
	if appended {
		store.wake()
	}
	return err
}

// Rollback abandons the transaction's writes; it never fails.
func (tx *memoryTx) Rollback(ctx context.Context) error {
	store := tx.store
	store.mu.Lock()
	if !tx.ended {
		tx.ended = true
		tx.clear()
		store.data.release(tx)
	}
	store.mu.Unlock()
	return nil
}

// ---- outbox ------------------------------------------------------------------------------------

// memoryOutbox is MemoryMessageStore's outbox.
type memoryOutbox struct {
	store *MemoryMessageStore
}

// Append appends entry in the context's transaction, or on its own.
func (o memoryOutbox) Append(ctx context.Context, entry OutboxEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := CheckOutboxPayload(entry.Type, entry.Payload); err != nil {
		return err
	}
	copied := copyOutboxEntry(entry)
	committed, err := o.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[memoryOutboxRow]
		if tx != nil {
			overlay = &tx.outbox
		}
		if _, exists := data.outbox.get(overlay, entry.ID); exists {
			// TODO(spec-gap): SG-29 (a duplicate outbox id is a conflict, never an overwrite).
			return fmt.Errorf("%w: an outbox entry %q exists", ErrConflict, entry.ID)
		}
		row := memoryOutboxRow{entry: copied}
		data.outbox.set(overlay, data, entry.ID, &row)
		return nil
	})
	if err == nil && committed {
		o.store.wake()
	}
	return err
}

// Claim claims up to limit pending entries for owner under a lease. An empty owner is refused
// with ErrInvalid: the store keeps "" for an entry nobody holds.
func (o memoryOutbox) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("%w: a claim needs a non-empty owner", ErrInvalid)
	}
	if limit <= 0 {
		return nil, nil
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	now := store.nowLocked()
	until := now.Add(lease)
	type candidate struct {
		createdAt time.Time
		seq       int64
		id        string
	}
	candidates := make([]candidate, 0, len(store.data.outbox.rows))
	for id, row := range store.data.outbox.rows {
		if !row.value.pending() {
			continue
		}
		if row.value.owner != "" && row.value.owner != owner && row.value.leaseUntil.After(now) {
			continue
		}
		candidates = append(candidates, candidate{row.value.entry.CreatedAt, row.value.entry.Seq, id})
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.Before(b.createdAt)
		}
		if a.seq != b.seq {
			return a.seq < b.seq
		}
		return a.id < b.id
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	claimed := make([]OutboxEntry, 0, len(candidates))
	for _, candidate := range candidates {
		row := store.data.outbox.rows[candidate.id]
		row.value.owner = owner
		row.value.leaseUntil = until
		claimed = append(claimed, copyOutboxEntry(row.value.entry))
	}
	return claimed, nil
}

// MarkSent marks the entries sent now.
func (o memoryOutbox) MarkSent(ctx context.Context, ids []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	now := store.nowLocked()
	for _, id := range ids {
		if row := store.data.outbox.rows[id]; row != nil {
			row.value.sentAt = now
		}
	}
	return nil
}

// MarkAttemptFailed counts a failed send that will be retried.
func (o memoryOutbox) MarkAttemptFailed(ctx context.Context, id, cause string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	if row := store.data.outbox.rows[id]; row != nil {
		row.value.entry.Attempts++
	}
	return nil
}

// MarkFailed gives the entry up.
func (o memoryOutbox) MarkFailed(ctx context.Context, id, cause string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	if row := store.data.outbox.rows[id]; row != nil {
		row.value.entry.Attempts++
		row.value.failedAt = store.nowLocked()
	}
	return nil
}

// Purge removes the entries sent before sentBefore.
func (o memoryOutbox) Purge(ctx context.Context, sentBefore time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	var purged int64
	for id, row := range store.data.outbox.rows {
		if !row.value.sentAt.IsZero() && row.value.sentAt.Before(sentBefore) {
			delete(store.data.outbox.rows, id)
			purged++
		}
	}
	return purged, nil
}

// Pending counts the entries neither sent nor failed.
func (o memoryOutbox) Pending(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	var pending int64
	for _, row := range store.data.outbox.rows {
		if row.value.pending() {
			pending++
		}
	}
	return pending, nil
}

// OldestPending is the CreatedAt of the oldest pending entry.
func (o memoryOutbox) OldestPending(ctx context.Context) (time.Time, bool, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	store := o.store
	store.mu.Lock()
	defer store.mu.Unlock()
	store.initLocked()
	var oldest time.Time
	found := false
	for _, row := range store.data.outbox.rows {
		if !row.value.pending() {
			continue
		}
		if !found || row.value.entry.CreatedAt.Before(oldest) {
			oldest = row.value.entry.CreatedAt
			found = true
		}
	}
	return oldest, found, nil
}

// OnAppend registers wake, called after every committed append.
func (o memoryOutbox) OnAppend(wake func()) func() {
	if wake == nil {
		return func() {}
	}
	store := o.store
	store.mu.Lock()
	store.initLocked()
	store.nextWaker++
	id := store.nextWaker
	store.wakers[id] = wake
	store.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			store.mu.Lock()
			delete(store.wakers, id)
			store.mu.Unlock()
		})
	}
}

// copyOutboxEntry copies everything a caller could change behind the store's back.
func copyOutboxEntry(entry OutboxEntry) OutboxEntry {
	copied := entry
	copied.Key = copyStringPointer(entry.Key)
	copied.Headers = copyStringMap(entry.Headers)
	copied.Payload = append([]byte(nil), entry.Payload...)
	return copied
}

// ---- processed events --------------------------------------------------------------------------

// memoryProcessed is MemoryMessageStore's processed-event store.
type memoryProcessed struct {
	store *MemoryMessageStore
}

// Record records marker in the context's transaction (or on its own); false when its id is
// recorded already.
func (p memoryProcessed) Record(ctx context.Context, marker ProcessedMarker) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	recorded := false
	_, err := p.store.withTx(ctx, func(data *memoryData, tx *memoryTx) error {
		var overlay *memoryOverlay[ProcessedMarker]
		if tx != nil {
			overlay = &tx.processed
		}
		if _, exists := data.processed.get(overlay, marker.ID); exists {
			return nil
		}
		copied := marker
		data.processed.set(overlay, data, marker.ID, &copied)
		recorded = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return recorded, nil
}

// Exists reports whether a marker with id is committed.
func (p memoryProcessed) Exists(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	p.store.initLocked()
	_, exists := p.store.data.processed.get(nil, id)
	return exists, nil
}

// DeleteMarkers deletes the stage-1 markers of processor in ranges.
func (p memoryProcessed) DeleteMarkers(ctx context.Context, processor string, ranges []ReplayRange) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	p.store.initLocked()
	var deleted int64
	for _, replay := range ranges {
		from := memoryPositionOf(replay.From)
		until := memoryPositionOf(replay.Until)
		for id, row := range p.store.data.processed.rows {
			marker := row.value
			if marker.Processor != processor || marker.Source != replay.Source ||
				marker.Segment != replay.Segment || marker.Position == "" {
				continue
			}
			if !memoryPositionOf(marker.Position).within(from, until) {
				continue
			}
			delete(p.store.data.processed.rows, id)
			deleted++
		}
	}
	return deleted, nil
}

// Purge removes the markers processed before before.
func (p memoryProcessed) Purge(ctx context.Context, before time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	p.store.initLocked()
	var purged int64
	for id, row := range p.store.data.processed.rows {
		if row.value.ProcessedAt.Before(before) {
			delete(p.store.data.processed.rows, id)
			purged++
		}
	}
	return purged, nil
}

// memoryPosition is a replay position as the document stores keep it: a number when it is a
// canonical decimal int64, else its text; a number and a text never compare with each other.
type memoryPosition struct {
	number int64
	text   string
	isText bool
}

// memoryPositionOf reads a position.
func memoryPositionOf(position string) memoryPosition {
	if number, err := strconv.ParseInt(position, 10, 64); err == nil &&
		strconv.FormatInt(number, 10) == position {
		return memoryPosition{number: number}
	}
	return memoryPosition{text: position, isText: true}
}

// within reports whether the position is in [from, until), each comparison within one kind.
func (p memoryPosition) within(from, until memoryPosition) bool {
	if p.isText || from.isText || until.isText {
		return p.isText && from.isText && until.isText &&
			from.text <= p.text && p.text < until.text
	}
	return from.number <= p.number && p.number < until.number
}

// ---- the ports this release does not implement -------------------------------------------------

// memoryDeadLetters is the dead-letter port until it is implemented.
type memoryDeadLetters struct{}

func (memoryDeadLetters) Park(context.Context, DeadLetter) (ParkResult, error) {
	return 0, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) Head(context.Context, string, string) (string, bool, error) {
	return "", false, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) Earlier(context.Context, string, string, string) (string, bool, error) {
	return "", false, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) ParkedKeys(context.Context, string, string, []string) ([]string, error) {
	return nil, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) Get(context.Context, string) (*DeadLetter, error) {
	return nil, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) List(context.Context, string, int) ([]DeadLetter, error) {
	return nil, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) Summaries(context.Context, string) ([]DeadLetterSummary, error) {
	return nil, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) Count(context.Context, string) (int64, error) {
	return 0, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) Delete(context.Context, string) (bool, error) {
	return false, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) DeleteIn(context.Context, string) (bool, error) {
	return false, unsupportedMemory("dead letters")
}

func (memoryDeadLetters) RecordFailure(context.Context, string, DeadLetterError, time.Time) (bool, error) {
	return false, unsupportedMemory("dead letters")
}

// memoryControl is the processor-control port until it is implemented.
type memoryControl struct{}

func (memoryControl) Get(context.Context, string) (ProcessorControlState, error) {
	return ProcessorControlState{}, unsupportedMemory("processor control")
}

func (memoryControl) SetPaused(context.Context, string, bool, string, time.Time) error {
	return unsupportedMemory("processor control")
}

func (memoryControl) Lock(context.Context, string, string, time.Time, time.Time) (LockOutcome, error) {
	return LockOutcome{}, unsupportedMemory("processor control")
}

func (memoryControl) Extend(context.Context, string, string, time.Time, time.Time) (bool, error) {
	return false, unsupportedMemory("processor control")
}

func (memoryControl) Release(context.Context, string, string, time.Time) (bool, error) {
	return false, unsupportedMemory("processor control")
}

func (memoryControl) Expire(context.Context, string, string, time.Time) (bool, error) {
	return false, unsupportedMemory("processor control")
}

func (memoryControl) SetReplay(context.Context, string, []ReplayRange, time.Time) error {
	return unsupportedMemory("processor control")
}

func (memoryControl) RemoveRange(context.Context, string, ReplayRange, time.Time) (bool, error) {
	return false, unsupportedMemory("processor control")
}

// memoryExternalCalls is the external-call port until it is implemented.
type memoryExternalCalls struct{}

func (memoryExternalCalls) Insert(context.Context, ExternalCall) (bool, error) {
	return false, unsupportedMemory("external calls")
}

func (memoryExternalCalls) Find(context.Context, string) (*ExternalCall, error) {
	return nil, unsupportedMemory("external calls")
}

func (memoryExternalCalls) TakeOver(context.Context, ExternalCall, string, time.Time, time.Time) (*ExternalCall, error) {
	return nil, unsupportedMemory("external calls")
}

func (memoryExternalCalls) Expire(context.Context, ExternalCall, time.Time) (bool, error) {
	return false, unsupportedMemory("external calls")
}

func (memoryExternalCalls) Succeed(context.Context, string, string, string, time.Time) (bool, error) {
	return false, unsupportedMemory("external calls")
}

func (memoryExternalCalls) Fail(context.Context, string, string, string, time.Time) (bool, error) {
	return false, unsupportedMemory("external calls")
}

func (memoryExternalCalls) MarkUnknown(context.Context, string, string, time.Time) (bool, error) {
	return false, unsupportedMemory("external calls")
}

func (memoryExternalCalls) Reset(context.Context, string, string) (bool, error) {
	return false, unsupportedMemory("external calls")
}

// memorySagas is the saga repository until it is implemented.
type memorySagas struct{}

func (memorySagas) FindByID(context.Context, string) (*Saga, error) {
	return nil, unsupportedMemory("sagas")
}

func (memorySagas) FindLive(context.Context, string, string, string) ([]Saga, error) {
	return nil, unsupportedMemory("sagas")
}

func (memorySagas) Insert(context.Context, Saga) error {
	return unsupportedMemory("sagas")
}

func (memorySagas) Update(context.Context, Saga, int64) (bool, error) {
	return false, unsupportedMemory("sagas")
}

func (memorySagas) CountLiveByType(context.Context) (map[string]int64, error) {
	return nil, unsupportedMemory("sagas")
}

// memorySagaDispatch is the saga dispatch port until it is implemented.
type memorySagaDispatch struct{}

func (memorySagaDispatch) Record(context.Context, []SagaDispatch) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) DueSagas(context.Context, time.Time, []string, int) ([]string, error) {
	return nil, unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) NextPending(context.Context, string) (*SagaDispatch, error) {
	return nil, unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) MarkDispatched(context.Context, string, time.Time) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) ScheduleRetry(context.Context, string, int, time.Time) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) MarkFailed(context.Context, string, DispatchFailure) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) ClaimDueNotice(context.Context, time.Time, time.Time) (*SagaDispatch, error) {
	return nil, unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) ClearNotice(context.Context, string) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) ScheduleNoticeRetry(context.Context, string, int, time.Time) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) ClaimLease(context.Context, string, string, time.Time, time.Time) (bool, error) {
	return false, unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) RenewLease(context.Context, string, string, time.Time) (bool, error) {
	return false, unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) ReleaseLease(context.Context, string, string) error {
	return unsupportedMemory("saga dispatch")
}

func (memorySagaDispatch) Pending(context.Context) (int64, error) {
	return 0, unsupportedMemory("saga dispatch")
}

// memorySagaDeadlines is the deadline port until it is implemented.
type memorySagaDeadlines struct{}

func (memorySagaDeadlines) Insert(context.Context, SagaDeadline) error {
	return unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) FindScheduled(context.Context, string) (*SagaDeadline, error) {
	return nil, unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) Cancel(context.Context, string, string, time.Time) error {
	return unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) CancelByName(context.Context, string, string, time.Time) error {
	return unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) CancelAll(context.Context, string, time.Time) error {
	return unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) MarkFired(context.Context, string, time.Time) (bool, error) {
	return false, unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) ClaimDue(context.Context, string, time.Time, time.Time) (*SagaDeadline, error) {
	return nil, unsupportedMemory("saga deadlines")
}

func (memorySagaDeadlines) CountOverdueByName(context.Context, time.Time) (map[string]int64, error) {
	return nil, unsupportedMemory("saga deadlines")
}

// memorySagaFailures is the failure port until it is implemented.
type memorySagaFailures struct{}

func (memorySagaFailures) Find(context.Context, string) (*SagaFailure, error) {
	return nil, unsupportedMemory("saga failures")
}

func (memorySagaFailures) Save(context.Context, SagaFailure) error {
	return unsupportedMemory("saga failures")
}

func (memorySagaFailures) Delete(context.Context, string) (bool, error) {
	return false, unsupportedMemory("saga failures")
}

func (memorySagaFailures) FailedAgain(context.Context, string, string, int, time.Time) (bool, error) {
	return false, unsupportedMemory("saga failures")
}

func (memorySagaFailures) List(context.Context, string) ([]SagaFailure, error) {
	return nil, unsupportedMemory("saga failures")
}

// memorySagaMarkers is the saga marker port until it is implemented.
type memorySagaMarkers struct{}

func (memorySagaMarkers) Exists(context.Context, string) (bool, error) {
	return false, unsupportedMemory("saga markers")
}

func (memorySagaMarkers) Insert(context.Context, string, string, time.Time) error {
	return unsupportedMemory("saga markers")
}

var (
	_ MessageStore          = (*MemoryMessageStore)(nil)
	_ StoreLifecycle        = (*MemoryMessageStore)(nil)
	_ OutboxStore           = memoryOutbox{}
	_ ProcessedStore        = memoryProcessed{}
	_ ProcessedEvents       = memoryProcessed{}
	_ DeadLetterStore       = memoryDeadLetters{}
	_ ProcessorControlStore = memoryControl{}
	_ ExternalCallStore     = memoryExternalCalls{}
	_ SagaRepository        = memorySagas{}
	_ SagaDispatchStore     = memorySagaDispatch{}
	_ SagaDeadlineStore     = memorySagaDeadlines{}
	_ SagaFailureStore      = memorySagaFailures{}
	_ SagaMarkerStore       = memorySagaMarkers{}
)
