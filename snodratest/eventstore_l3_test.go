package snodratest_test

import (
	"context"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
)

// withoutReplay hides the replay capability and refuses the admin side.
type withoutReplay struct {
	snodra.EventStore
}

func (s withoutReplay) Capabilities() []snodra.Capability {
	var kept []snodra.Capability
	for _, capability := range s.EventStore.Capabilities() {
		if capability != snodra.CapabilityReplay {
			kept = append(kept, capability)
		}
	}
	return kept
}

func (s withoutReplay) Admin(snodra.SubscriptionSpec) (snodra.SubscriptionAdmin, error) {
	return nil, &snodra.UnsupportedError{Capability: snodra.CapabilityReplay}
}

// withoutLag hides the lag capability and reports no segment's lag.
type withoutLag struct {
	snodra.EventStore
}

func (s withoutLag) Capabilities() []snodra.Capability {
	var kept []snodra.Capability
	for _, capability := range s.EventStore.Capabilities() {
		if capability != snodra.CapabilityLag {
			kept = append(kept, capability)
		}
	}
	return kept
}

func (s withoutLag) Subscribe(ctx context.Context, spec snodra.SubscriptionSpec) (snodra.EventSubscription, error) {
	sub, err := s.EventStore.Subscribe(ctx, spec)
	if err != nil {
		return nil, err
	}
	return noLagSubscription{EventSubscription: sub}, nil
}

// noLagSubscription reports every segment's lag as unknown.
type noLagSubscription struct {
	snodra.EventSubscription
}

func (s noLagSubscription) Segments() []snodra.SegmentStatus {
	statuses := s.EventSubscription.Segments()
	for i := range statuses {
		statuses[i].Lag = -1
	}
	return statuses
}

// The level-3 absent-capability checks on stores that hide replay and lag, and the no-browser
// check on a store without browse.
func TestTheAbsentCapabilityChecks(t *testing.T) {
	store := snodra.NewMemoryEventStore()
	t.Run("admin_without_replay_is_unsupported", func(t *testing.T) {
		kit := &snodratest.EventStoreKit{
			Source:  "conformance.l3-absent",
			Open:    func() snodra.EventStore { return withoutReplay{EventStore: store} },
			Markers: newMarkers(),
		}
		snodratest.AdminWithoutReplayIsUnsupported(t, kit)
	})
	t.Run("lag_unknown_without_capability", func(t *testing.T) {
		kit := &snodratest.EventStoreKit{
			Source:  "conformance.l3-absent",
			Open:    func() snodra.EventStore { return withoutLag{EventStore: store} },
			Markers: newMarkers(),
		}
		snodratest.LagUnknownWithoutCapability(t, kit)
	})
	t.Run("no_browser_without_capability", func(t *testing.T) {
		kit := &snodratest.EventStoreKit{
			Source:  "conformance.l3-absent",
			Open:    func() snodra.EventStore { return store },
			Markers: newMarkers(),
		}
		snodratest.NoBrowserWithoutCapability(t, kit)
	})
}
