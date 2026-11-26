package main

import (
	"context"
	"strings"
	"sync"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// markerObservation is one stage-1 processed-event marker record the app's consumer made: the
// event it delivered to this instance, where, and whether the marker was new (the handler ran) or
// existed already (a duplicate).
type markerObservation struct {
	eventID   string
	segment   string
	position  string
	duplicate bool
}

// handledObservation is one event the probe's handler ran for.
type handledObservation struct {
	id        string
	run       string
	publisher string
	family    string
	key       string
	seq       int64
}

// txRecord collects what one transaction writes to the ledger. The library's consumer opens the
// transaction before the stage-1 marker and commits it after the handler; only a commit moves the
// record into the ledger, so State never reports a delivery the transaction did not commit.
type txRecord struct {
	markers []markerObservation
	handled map[string]handledObservation
}

func newTxRecord() *txRecord {
	return &txRecord{handled: map[string]handledObservation{}}
}

// txRecordKey is where Begin puts the transaction's record: the consumer passes the same context
// to the marker record and, through it, to the handlers.
type txRecordKey struct{}

// txRecordFrom returns the record of the transaction ctx carries, or nil.
func txRecordFrom(ctx context.Context) *txRecord {
	record, _ := ctx.Value(txRecordKey{}).(*txRecord)
	return record
}

// ledger is what the probe's consumer did, in memory: the stage-1 marker records (from the store
// wrapper) and the events whose handler ran. Both languages' drivers read it through State; the
// real state is the stores.
type ledger struct {
	mu      sync.Mutex
	markers []markerObservation
	handled map[string]handledObservation
}

func newLedger() *ledger {
	return &ledger{handled: map[string]handledObservation{}}
}

// observeMarker records one processed port call. A new marker belongs to the transaction ctx
// carries, and reaches the ledger only when that transaction commits; a duplicate is not
// committed (the consumer rolls back) and is recorded at once, as the delivery the other language
// skipped.
func (l *ledger) observeMarker(family string, marker snodra.ProcessedMarker, recorded bool, tx *txRecord) {
	prefix := "processor:" + processorName(family) + ":"
	if !strings.HasPrefix(marker.ID, prefix) {
		return
	}
	observation := markerObservation{
		eventID:   strings.TrimPrefix(marker.ID, prefix),
		segment:   marker.Segment,
		position:  marker.Position,
		duplicate: !recorded,
	}
	if recorded && tx != nil {
		tx.markers = append(tx.markers, observation)
		return
	}
	l.appendMarker(observation)
}

// appendMarker adds a marker the ledger may show now.
func (l *ledger) appendMarker(observation markerObservation) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.markers = append(l.markers, observation)
}

// observeHandled records one event handler run: in the transaction ctx carries, or at once when
// the handler runs outside one.
func (l *ledger) observeHandled(event OrderPlaced, tx *txRecord) {
	observation := handledObservation{
		id:        event.ID,
		run:       event.Run,
		publisher: event.Publisher,
		family:    event.Family,
		key:       event.Key,
		seq:       event.Seq,
	}
	if tx != nil {
		tx.handled[event.ID] = observation
		return
	}
	l.appendHandled(observation)
}

// appendHandled adds a handler's record the ledger may show now.
func (l *ledger) appendHandled(observation handledObservation) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handled[observation.id] = observation
}

// commit moves a committed transaction's records into the ledger.
func (l *ledger) commit(tx *txRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.markers = append(l.markers, tx.markers...)
	for id, observation := range tx.handled {
		l.handled[id] = observation
	}
}

// snapshot returns the deliveries as the driver reads them, with the handler's details joined in,
// and the counts of handled and duplicate deliveries.
func (l *ledger) snapshot() ([]Delivery, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	deliveries := make([]Delivery, 0, len(l.markers))
	handled, duplicates := 0, 0
	for _, marker := range l.markers {
		delivery := Delivery{
			ID:       marker.eventID,
			Segment:  marker.segment,
			Position: marker.position,
		}
		if marker.duplicate {
			delivery.Outcome = "duplicate"
			duplicates++
		} else {
			delivery.Outcome = "handled"
			if observed, ok := l.handled[marker.eventID]; ok {
				delivery.Publisher = observed.publisher
				delivery.Key = observed.key
			}
			handled++
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, handled, duplicates
}

// observedStore is the message store the app is built with: every port is the add-on's own, and
// only the processed port is wrapped so the probe can tell which stage-1 marker the consumer
// recorded and which it found. Its lifecycle calls go to the add-on so the schema and background
// work stay the add-on's.
type observedStore struct {
	snodra.MessageStore
	inner  snodra.MessageStore
	family string
	ledger *ledger
}

var _ snodra.MessageStore = (*observedStore)(nil)
var _ snodra.StoreLifecycle = (*observedStore)(nil)

func newObservedStore(inner snodra.MessageStore, family string, ledger *ledger) *observedStore {
	return &observedStore{MessageStore: inner, inner: inner, family: family, ledger: ledger}
}

// Begin starts the add-on's transaction and carries the ledger record the consumer's marker and
// handlers write; the ledger sees them only when the transaction commits.
func (s *observedStore) Begin(ctx context.Context) (context.Context, snodra.Tx, error) {
	innerCtx, tx, err := s.inner.Begin(ctx)
	if err != nil {
		return innerCtx, tx, err
	}
	record := newTxRecord()
	return context.WithValue(innerCtx, txRecordKey{}, record), &observedTx{inner: tx, ledger: s.ledger, record: record}, nil
}

// Processed is the add-on's processed store, observed.
func (s *observedStore) Processed() snodra.ProcessedStore {
	return &observedProcessed{inner: s.inner.Processed(), family: s.family, ledger: s.ledger}
}

// observedTx is the add-on's transaction, committing its ledger record after the add-on committed.
type observedTx struct {
	inner  snodra.Tx
	ledger *ledger
	record *txRecord
}

var _ snodra.Tx = (*observedTx)(nil)

func (t *observedTx) Commit(ctx context.Context) error {
	if err := t.inner.Commit(ctx); err != nil {
		return err
	}
	t.ledger.commit(t.record)
	return nil
}

func (t *observedTx) Rollback(ctx context.Context) error {
	return t.inner.Rollback(ctx)
}

// UseClock hands the add-on its application clock, when it takes one.
func (s *observedStore) UseClock(now func() time.Time) {
	if lifecycle, ok := s.inner.(snodra.StoreLifecycle); ok {
		lifecycle.UseClock(now)
	}
}

// InitSchema prepares the add-on's schema.
func (s *observedStore) InitSchema(ctx context.Context, config *snodra.Config) error {
	if lifecycle, ok := s.inner.(snodra.StoreLifecycle); ok {
		return lifecycle.InitSchema(ctx, config)
	}
	return nil
}

// CheckSchema checks the add-on's schema.
func (s *observedStore) CheckSchema(ctx context.Context) error {
	if lifecycle, ok := s.inner.(snodra.StoreLifecycle); ok {
		return lifecycle.CheckSchema(ctx)
	}
	return nil
}

// Background runs the add-on's background work, or waits until the app ends.
func (s *observedStore) Background(ctx context.Context) error {
	if lifecycle, ok := s.inner.(snodra.StoreLifecycle); ok {
		return lifecycle.Background(ctx)
	}
	<-ctx.Done()
	return nil
}

// observedProcessed delegates the processed port and records the stage-1 markers the consumer
// records through it.
type observedProcessed struct {
	inner  snodra.ProcessedStore
	family string
	ledger *ledger
}

var _ snodra.ProcessedStore = (*observedProcessed)(nil)

func (p *observedProcessed) Record(ctx context.Context, marker snodra.ProcessedMarker) (bool, error) {
	recorded, err := p.inner.Record(ctx, marker)
	if err == nil {
		p.ledger.observeMarker(p.family, marker, recorded, txRecordFrom(ctx))
	}
	return recorded, err
}

func (p *observedProcessed) Exists(ctx context.Context, id string) (bool, error) {
	return p.inner.Exists(ctx, id)
}

func (p *observedProcessed) DeleteMarkers(ctx context.Context, processor string, ranges []snodra.ReplayRange) (int64, error) {
	return p.inner.DeleteMarkers(ctx, processor, ranges)
}

func (p *observedProcessed) Purge(ctx context.Context, before time.Time) (int64, error) {
	return p.inner.Purge(ctx, before)
}
