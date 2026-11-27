package snodratest_test

import (
	"context"
	"fmt"
	"strings"
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

// withBrowse declares the browse capability.
type withBrowse struct {
	snodra.EventStore
}

func (s withBrowse) Capabilities() []snodra.Capability {
	return append(s.EventStore.Capabilities(), snodra.CapabilityBrowse)
}

// skipRecorder records a check's Skip instead of ending the test, so a skip can be asserted.
type skipRecorder struct {
	*testing.T
	skipped bool
}

func (s *skipRecorder) Skip(...any) { s.skipped = true }

// The no-browser check does not report a pass for a browse store: it skips, since the Go kit
// cannot run the browse checks.
func TestNoBrowserCheckSkipsABrowseStore(t *testing.T) {
	store := snodra.NewMemoryEventStore()
	kit := &snodratest.EventStoreKit{
		Source:  "conformance.l3-absent",
		Open:    func() snodra.EventStore { return withBrowse{EventStore: store} },
		Markers: newMarkers(),
	}
	recorder := &skipRecorder{T: t}
	snodratest.NoBrowserWithoutCapability(recorder, kit)
	if !recorder.skipped {
		t.Fatal("a store declaring browse must skip the no-browser check, not pass it")
	}
}

// wrongCapabilityStore refuses the admin for another capability.
type wrongCapabilityStore struct {
	snodra.EventStore
}

func (s wrongCapabilityStore) Admin(snodra.SubscriptionSpec) (snodra.SubscriptionAdmin, error) {
	return nil, &snodra.UnsupportedError{Capability: snodra.CapabilityLag}
}

// failRecorder records a check's Fatalf instead of ending the test.
type failRecorder struct {
	*testing.T
	failed string
}

func (f *failRecorder) Fatalf(format string, args ...any) {
	f.failed = fmt.Sprintf(format, args...)
}

// The admin-without-replay check names the replay capability, not just any Unsupported error.
func TestAdminWithoutReplayCheckNamesTheCapability(t *testing.T) {
	store := snodra.NewMemoryEventStore()
	kit := &snodratest.EventStoreKit{
		Source:  "conformance.l3-absent",
		Open:    func() snodra.EventStore { return wrongCapabilityStore{EventStore: store} },
		Markers: newMarkers(),
	}
	recorder := &failRecorder{T: t}
	snodratest.AdminWithoutReplayIsUnsupported(recorder, kit)
	if !strings.Contains(recorder.failed, "lag") || !strings.Contains(recorder.failed, "want replay") {
		t.Fatalf("the check accepted a wrong capability: %q", recorder.failed)
	}
}
