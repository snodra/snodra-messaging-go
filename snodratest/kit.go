package snodratest

import (
	"context"
	"crypto/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

const (
	// wait is the longest a check waits for what the event store does in the background.
	wait = 60 * time.Second
	// quiet is how long a check watches a subscription that must deliver nothing.
	quiet = 3 * time.Second
	// slice is one turn of reading a single subscription. It bounds a call that never answers
	// and keeps a check responsive.
	slice = 250 * time.Millisecond
	// parallelSlice is one turn of reading many subscriptions one after the other. It is
	// shorter than slice so a round over every instance of a check stays short.
	parallelSlice = 10 * time.Millisecond
	// joinWindow is how long a check waits for the assignment change a joining instance may
	// cause.
	joinWindow = 6 * time.Second
	// eventType is the type of the kit's events.
	eventType = "conformance.Event"
)

// EventStoreKit is what every check runs on. It is exported so a failing check can be run by
// itself.
type EventStoreKit struct {
	// Source is the prepared source (see the package documentation).
	Source string
	// Open is one service instance per call.
	Open func() snodra.EventStore
	// Markers records the processed-event marker of the at-least-once check.
	Markers snodra.ProcessedEvents
}

// instance makes one service instance.
func (k *EventStoreKit) instance(tb testing.TB) snodra.EventStore {
	tb.Helper()
	if k.Open == nil {
		tb.Fatal("snodratest: EventStoreKit.Open is nil")
	}
	store := k.Open()
	if store == nil {
		tb.Fatal("snodratest: EventStoreKit.Open returned a nil event store")
	}
	return store
}

// processSuffix makes this process's subscription names distinct from those of an earlier run
// against a persistent store, so a new name starts at the earliest position instead of a stored
// committed one. It is one suffix per process.
var processSuffix = strings.ToLower(rand.Text())

// spec is a subscription name nothing has used.
func (k *EventStoreKit) spec() snodra.SubscriptionSpec {
	return snodra.SubscriptionSpec{
		Name:   "conformance." + processSuffix + "-" + strconv.FormatUint(nextNumber(), 10),
		Source: k.Source,
	}
}

// EventStore runs the level-2 event-store checks and the level-3 capability checks in the Rust
// kit's order, each as a subtest named exactly as the Rust kit names it. A capability check runs
// in one of its two forms: the capability's check when the store declares it, and the
// absent-capability check when it does not. The Go kit has no event browser, so the four browse
// checks are omitted as a kit difference (a store declaring browse skips the no-browser check).
// See the package documentation for the arguments.
func EventStore(t *testing.T, source string, open func() snodra.EventStore, markers snodra.ProcessedEvents) {
	t.Helper()
	if source == "" {
		t.Fatal("snodratest: the source is empty")
	}
	if open == nil {
		t.Fatal("snodratest: open is nil")
	}
	if markers == nil {
		t.Fatal("snodratest: markers is nil; pass the message store's processed-event store")
	}
	kit := &EventStoreKit{Source: source, Open: open, Markers: markers}
	events := kit.instance(t)
	name := events.Name()
	run := func(id string, check func(testing.TB, *EventStoreKit)) {
		t.Helper()
		if !t.Run(id, func(t *testing.T) { check(t, kit) }) {
			t.Fatalf("event store conformance check %q failed on event store %q", id, name)
		}
	}
	for _, check := range eventStoreChecks {
		run(check.id, check.run)
	}
	if snodra.Supports(events, snodra.CapabilityReplay) {
		run("replay_from_earliest_redelivers_history", ReplayFromEarliestRedeliversHistory)
	} else {
		run("admin_without_replay_is_unsupported", AdminWithoutReplayIsUnsupported)
	}
	if snodra.Supports(events, snodra.CapabilityLag) {
		run("lag_counts_unacked", LagCountsUnacked)
	} else {
		run("lag_unknown_without_capability", LagUnknownWithoutCapability)
	}
	if snodra.Supports(events, snodra.CapabilityNativeDeadLetter) {
		run("dead_letter_source_receives", DeadLetterSourceReceives)
	}
	// A store that declares browse skips this check itself (the Go kit has no event browser); one
	// that does not declares it by passing. The four browse checks are a kit difference.
	run("no_browser_without_capability", NoBrowserWithoutCapability)
}

// CheckIDs returns the ids the event-store kit may run: the level-2 checks and the level-3
// capability checks, in the Rust kit's order (each capability's two forms both appear; a run
// uses one of them). The four browse checks are not listed: the Go kit has no event browser.
func CheckIDs() []string {
	ids := make([]string, 0, len(eventStoreChecks)+len(eventStoreL3Checks))
	for _, check := range eventStoreChecks {
		ids = append(ids, check.id)
	}
	for _, check := range eventStoreL3Checks {
		ids = append(ids, check.id)
	}
	return ids
}

// eventStoreChecks is the level-2 set, in the Rust kit's order. checks.txt is the published copy
// of these ids, and a test pins it to a literal copy of the Rust list.
var eventStoreChecks = []struct {
	id  string
	run func(testing.TB, *EventStoreKit)
}{
	{"headers_round_trip", HeadersRoundTrip},
	{"same_key_arrives_in_publish_order", SameKeyArrivesInPublishOrder},
	{"every_processor_gets_every_event", EveryProcessorGetsEveryEvent},
	{"instances_share_a_processor", InstancesShareAProcessor},
	{"a_joining_instance_announces_its_first_assignment", AJoiningInstanceAnnouncesItsFirstAssignment},
	{"an_instance_left_without_segments_closes_its_change", AnInstanceLeftWithoutSegmentsClosesItsChange},
	{"unacked_delivery_is_redelivered", UnackedDeliveryIsRedelivered},
	{"redelivered_after_commit_is_skipped", RedeliveredAfterCommitIsSkipped},
	{"paused_subscription_receives_nothing_and_resumes_in_place", PausedSubscriptionReceivesNothingAndResumesInPlace},
}

// eventStoreL3Checks is the level-3 capability list, in the Rust kit's order minus the four
// browse checks (a kit difference: the Go library has no event browser). A capability's two
// forms both appear; EventStore runs one of them.
var eventStoreL3Checks = []struct {
	id  string
	run func(testing.TB, *EventStoreKit)
}{
	{"replay_from_earliest_redelivers_history", ReplayFromEarliestRedeliversHistory},
	{"admin_without_replay_is_unsupported", AdminWithoutReplayIsUnsupported},
	{"lag_counts_unacked", LagCountsUnacked},
	{"lag_unknown_without_capability", LagUnknownWithoutCapability},
	{"dead_letter_source_receives", DeadLetterSourceReceives},
	{"no_browser_without_capability", NoBrowserWithoutCapability},
}

var freshNumber atomic.Uint64

// nextNumber is a number no other name, id or header of the kit's uses.
func nextNumber() uint64 { return freshNumber.Add(1) }

// fresh is a name nothing has used.
func fresh(what string) string {
	return what + "-" + strconv.FormatUint(nextNumber(), 10)
}

// bounded runs call with the kit's wait and fails the check naming step when it fails or does
// not answer. The call must return when ctx is done: Go cannot interrupt a call that ignores
// it, so a store that does not is a hanging check, not a failure this helper can report.
func bounded(tb testing.TB, step string, call func(context.Context) error) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := call(ctx); err != nil {
		tb.Fatalf("%s: %v", step, err)
	}
}

// subscribe joins store as one more instance of spec, or fails the check.
func subscribe(tb testing.TB, store snodra.EventStore, spec snodra.SubscriptionSpec) snodra.EventSubscription {
	tb.Helper()
	var subscription snodra.EventSubscription
	bounded(tb, "subscribe "+spec.Name, func(ctx context.Context) error {
		created, err := store.Subscribe(ctx, spec)
		subscription = created
		return err
	})
	if subscription == nil {
		tb.Fatalf("subscribe %s: a nil subscription", spec.Name)
	}
	return subscription
}

// publishAll publishes sent in order, or fails the check naming the event.
func publishAll(tb testing.TB, store snodra.EventStore, sent []snodra.OutgoingEvent) {
	tb.Helper()
	for _, event := range sent {
		event := event
		bounded(tb, "publish "+event.ID, func(ctx context.Context) error {
			return store.Publish(ctx, event)
		})
	}
}

// recordMarker records marker, or fails the check.
func recordMarker(tb testing.TB, markers snodra.ProcessedEvents, marker snodra.ProcessedMarker) bool {
	tb.Helper()
	var recorded bool
	bounded(tb, "record marker "+marker.ID, func(ctx context.Context) error {
		var err error
		recorded, err = markers.Record(ctx, marker)
		return err
	})
	return recorded
}
