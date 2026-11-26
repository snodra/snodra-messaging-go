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

// observeMarker records one processed port call.
func (l *ledger) observeMarker(family string, marker snodra.ProcessedMarker, recorded bool) {
	prefix := "processor:" + processorName(family) + ":"
	if !strings.HasPrefix(marker.ID, prefix) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.markers = append(l.markers, markerObservation{
		eventID:   strings.TrimPrefix(marker.ID, prefix),
		segment:   marker.Segment,
		position:  marker.Position,
		duplicate: !recorded,
	})
}

// observeHandled records one event handler run.
func (l *ledger) observeHandled(event OrderPlaced) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handled[event.ID] = handledObservation{
		id:        event.ID,
		run:       event.Run,
		publisher: event.Publisher,
		family:    event.Family,
		key:       event.Key,
		seq:       event.Seq,
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

// Processed is the add-on's processed store, observed.
func (s *observedStore) Processed() snodra.ProcessedStore {
	return &observedProcessed{inner: s.inner.Processed(), family: s.family, ledger: s.ledger}
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
		p.ledger.observeMarker(p.family, marker, recorded)
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
