package snodratest_test

import (
	"context"
	"sync"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
)

// markers is the processed-event store the kit's at-least-once check records in.
type markers struct {
	mu  sync.Mutex
	ids map[string]bool
}

func newMarkers() *markers { return &markers{ids: make(map[string]bool)} }

func (m *markers) Record(ctx context.Context, marker snodra.ProcessedMarker) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ids[marker.ID] {
		return false, nil
	}
	m.ids[marker.ID] = true
	return true, nil
}

// The nine level-2 event-store checks on the in-memory event store.
func TestMemoryEventStorePassesTheKit(t *testing.T) {
	store := snodra.NewMemoryEventStore()
	snodratest.EventStore(t, "conformance.events",
		func() snodra.EventStore { return store },
		newMarkers())
}
