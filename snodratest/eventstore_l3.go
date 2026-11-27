package snodratest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// adminOf joins the reset side of spec, or fails the check when the store refuses it.
func adminOf(tb testing.TB, store snodra.EventStore, spec snodra.SubscriptionSpec) snodra.SubscriptionAdmin {
	tb.Helper()
	var admin snodra.SubscriptionAdmin
	bounded(tb, "admin "+spec.Name, func(ctx context.Context) error {
		created, err := store.Admin(spec)
		admin = created
		return err
	})
	if admin == nil {
		tb.Fatalf("admin %s: a nil admin", spec.Name)
	}
	return admin
}

// adminMembers reads the subscription's members, or fails the check.
func adminMembers(tb testing.TB, admin snodra.SubscriptionAdmin) []string {
	tb.Helper()
	var members []string
	bounded(tb, "members", func(ctx context.Context) error {
		found, err := admin.Members(ctx)
		members = found
		return err
	})
	return members
}

// adminPlan plans target, or fails the check.
func adminPlan(tb testing.TB, admin snodra.SubscriptionAdmin, target snodra.ResetTarget) []snodra.ReplayRange {
	tb.Helper()
	var ranges []snodra.ReplayRange
	bounded(tb, fmt.Sprintf("plan %T", target), func(ctx context.Context) error {
		planned, err := admin.Plan(ctx, target)
		ranges = planned
		return err
	})
	return ranges
}

// planError plans target, returning its error rather than failing the check.
func planError(tb testing.TB, admin snodra.SubscriptionAdmin, target snodra.ResetTarget) error {
	tb.Helper()
	var failure error
	bounded(tb, fmt.Sprintf("plan %T", target), func(ctx context.Context) error {
		_, failure = admin.Plan(ctx, target)
		return nil
	})
	return failure
}

// applyError applies ranges, returning its error rather than failing the check.
func applyError(tb testing.TB, admin snodra.SubscriptionAdmin, ranges []snodra.ReplayRange) error {
	tb.Helper()
	var failure error
	bounded(tb, "apply", func(ctx context.Context) error {
		failure = admin.Apply(ctx, ranges)
		return nil
	})
	return failure
}

// apply applies ranges, or fails the check.
func apply(tb testing.TB, admin snodra.SubscriptionAdmin, ranges []snodra.ReplayRange) {
	tb.Helper()
	bounded(tb, "apply", func(ctx context.Context) error { return admin.Apply(ctx, ranges) })
}

// waitMembers waits until the subscription has want members; a reader is pumped while waiting so
// an active subscription keeps its membership.
func waitMembers(tb testing.TB, admin snodra.SubscriptionAdmin, want int, reader *eventReader) {
	tb.Helper()
	deadline := time.Now().Add(wait)
	for {
		members := adminMembers(tb, admin)
		if len(members) == want {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("the subscription has %d members, want %d (%v)", len(members), want, members)
		}
		if reader != nil {
			reader.pump(tb, slice)
		} else {
			time.Sleep(slice)
		}
	}
}

// inRange reports whether position is inside replay's [from, until) by events' order.
func inRange(events snodra.EventStore, replay snodra.ReplayRange, position string) bool {
	return replay.Until != "" &&
		events.Compare(replay.Segment, replay.From, position) <= 0 &&
		events.Compare(replay.Segment, position, replay.Until) < 0
}

// L3 cap: replay_from_earliest_redelivers_history. While an instance reads the subscription its
// positions do not move; with none, a reset to earliest replays what it had acknowledged, and the
// plan of each target moves the segments as documented.
func ReplayFromEarliestRedeliversHistory(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	spec := kit.spec()
	var sent []snodra.OutgoingEvent
	for i := 0; i < 3; i++ {
		key := fresh("key")
		sent = append(sent, event(kit.Source, &key))
	}
	all := eventIDs(sent)
	publishAll(tb, events, sent)

	first := join(tb, events, spec)
	first.until(tb, "the events delivered", func(r *eventReader) bool { return r.hasAll(all) })
	delivered := first.mine(all)
	admin := adminOf(tb, events, spec)
	waitMembers(tb, admin, 1, first)

	whileActive := adminPlan(tb, admin, snodra.ResetEarliest{})
	if err := applyError(tb, admin, whileActive); !errors.Is(err, snodra.ErrSubscribersActive) {
		tb.Fatalf("no move while an instance reads: %v", err)
	}
	first.close(tb)
	waitMembers(tb, admin, 0, nil)
	// The plan after the subscription left sees the positions it committed; the one taken while
	// it read may predate the acknowledgements.
	earliest := adminPlan(tb, admin, snodra.ResetEarliest{})

	for _, delivery := range delivered {
		found := false
		for _, replay := range earliest {
			if replay.Segment == delivery.Segment && replay.Source == kit.Source &&
				inRange(events, replay, delivery.Position) {
				found = true
				break
			}
		}
		if !found {
			tb.Fatalf("earliest replays %s of segment %s: %v", delivery.Position, delivery.Segment, earliest)
		}
	}
	latest := adminPlan(tb, admin, snodra.ResetLatest{})
	for _, delivery := range delivered {
		found := false
		for _, replay := range latest {
			if replay.Segment == delivery.Segment {
				found = true
			}
		}
		if !found {
			tb.Fatalf("latest moves segment %s: %v", delivery.Segment, latest)
		}
	}
	for _, replay := range latest {
		if replay.Until != "" && events.Compare(replay.Segment, replay.From, replay.Until) < 0 {
			tb.Fatalf("latest replays nothing: %v", latest)
		}
	}
	oldest := delivered[0]
	at := adminPlan(tb, admin, snodra.ResetTimestamp{TimestampMs: oldest.TimestampMs})
	found := false
	for _, replay := range at {
		if replay.Segment == oldest.Segment && events.Compare(replay.Segment, oldest.Position, replay.From) >= 0 {
			found = true
		}
	}
	if !found {
		tb.Fatalf("a time moves to the first position at or after it: %v", at)
	}
	newest := oldest.TimestampMs
	for _, delivery := range delivered {
		if delivery.TimestampMs > newest {
			newest = delivery.TimestampMs
		}
	}
	past := adminPlan(tb, admin, snodra.ResetTimestamp{TimestampMs: newest + 1})
	for _, delivery := range delivered {
		found = false
		for _, replay := range past {
			if replay.Segment == delivery.Segment &&
				events.Compare(replay.Segment, delivery.Position, replay.From) < 0 {
				found = true
			}
		}
		if !found {
			tb.Fatalf("a time after every event of segment %s moves past %s: %v",
				delivery.Segment, delivery.Position, past)
		}
	}
	positions := adminPlan(tb, admin, snodra.ResetPositions{Positions: []snodra.SegmentPosition{
		{Segment: oldest.Segment, Position: oldest.Position},
	}})
	if len(positions) != 1 || positions[0].Segment != oldest.Segment || positions[0].From != oldest.Position {
		tb.Fatalf("positions move the listed segments only: %v", positions)
	}
	for _, target := range []snodra.ResetTarget{
		snodra.ResetPositions{Positions: []snodra.SegmentPosition{
			{Segment: "conformance-no-such-segment", Position: oldest.Position},
		}},
		snodra.ResetPositions{Positions: []snodra.SegmentPosition{
			{Segment: oldest.Segment, Position: "9223372036854775807"},
		}},
	} {
		if err := planError(tb, admin, target); !errors.Is(err, snodra.ErrInvalid) {
			tb.Fatalf("%#v is invalid: %v", target, err)
		}
	}
	apply(tb, admin, earliest)
	restarted := kit.instance(tb) // a new instance starts at the reset positions
	second := join(tb, restarted, spec)
	second.until(tb, "the acknowledged events replayed", func(r *eventReader) bool { return r.hasAll(all) })
	second.close(tb)
}

// L3 cap: admin_without_replay_is_unsupported. Without the capability, the reset side of a
// subscription is refused as unsupported.
func AdminWithoutReplayIsUnsupported(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	spec := kit.spec()
	var failure error
	bounded(tb, "admin "+spec.Name, func(context.Context) error {
		_, failure = events.Admin(spec)
		return nil
	})
	var unsupported *snodra.UnsupportedError
	if !errors.As(failure, &unsupported) || unsupported.Capability != snodra.CapabilityReplay {
		tb.Fatalf("admin without replay is unsupported for replay: %v", failure)
	}
}

// L3 cap: lag_counts_unacked. Each segment's lag counts what the subscription has not
// acknowledged; passes on an empty source because it primes every assigned segment first.
func LagCountsUnacked(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	reader := join(tb, events, kit.spec())
	reader.until(tb, "segments assigned", func(r *eventReader) bool { return len(r.assigned) > 0 })

	primed := map[string]bool{}
	deadline := time.Now().Add(wait)
	for !subset(primed, reader.assigned) {
		if time.Now().After(deadline) {
			tb.Fatalf("an acknowledged event on every segment within %v (primed %v of %v)",
				wait, sortedKeys(primed), reader.assignedList())
		}
		key := fresh("key")
		prime := event(kit.Source, &key)
		publishAll(tb, events, []snodra.OutgoingEvent{prime})
		reader.until(tb, "the priming event delivered", func(r *eventReader) bool { return r.has(prime.ID) })
		primed[reader.firstDelivery(tb, prime.ID).Segment] = true
	}
	deadline = time.Now().Add(wait)
	for {
		reader.until(tb, "everything acknowledged, lag 0", func(r *eventReader) bool {
			lag, known := knownLag(r, r.assignedList())
			return known && lag == 0
		})
		count := len(reader.seen)
		reader.pump(tb, quiet)
		if len(reader.seen) == count {
			lag, known := knownLag(reader, reader.assignedList())
			if known && lag == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			tb.Fatalf("the source drained within %v", wait)
		}
	}
	listed := segmentNames(reader.segmentStatuses(tb))
	ordered := append([]string(nil), listed...)
	sort.Slice(ordered, func(i, j int) bool { return snodra.SegmentOrder(ordered[i], ordered[j]) < 0 })
	if !sameStrings(listed, ordered) {
		tb.Fatalf("segments() lists them in segment order: %v", listed)
	}
	assigned := reader.assignedList()
	if !sameStrings(listed, assigned) {
		tb.Fatalf("segments() lists the assignment: %v, want %v", listed, assigned)
	}

	var sent []snodra.OutgoingEvent
	for i := 0; i < 5; i++ {
		key := fresh("key")
		sent = append(sent, event(kit.Source, &key))
	}
	all := eventIDs(sent)
	reader.hold(all)
	publishAll(tb, events, sent)
	reader.until(tb, "the held events delivered", func(r *eventReader) bool { return r.hasAll(all) })
	held := map[string]bool{}
	for _, delivery := range reader.mine(all) {
		held[delivery.Segment] = true
	}
	reader.until(tb, "the lag counts the five unacknowledged events", func(r *eventReader) bool {
		lag, known := knownLag(r, sortedKeys(held))
		return known && lag == 5
	})
	for _, delivery := range append([]snodra.Delivery(nil), reader.mine(all)...) {
		reader.ack(tb, delivery)
	}
	reader.until(tb, "the lag back to 0 once acknowledged", func(r *eventReader) bool {
		lag, known := knownLag(r, sortedKeys(held))
		return known && lag == 0
	})
	reader.close(tb)
}

// L3 cap: lag_unknown_without_capability. Without lag, no segment reports a lag.
func LagUnknownWithoutCapability(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	key := fresh("key")
	sent := event(kit.Source, &key)
	publishAll(tb, events, []snodra.OutgoingEvent{sent})
	reader := join(tb, events, kit.spec())
	reader.until(tb, "the event delivered", func(r *eventReader) bool { return r.has(sent.ID) })
	statuses := reader.segmentStatuses(tb)
	if len(statuses) == 0 {
		tb.Fatal("segments() lists the assignment")
	}
	for _, segment := range statuses {
		if segment.Lag != -1 {
			tb.Fatalf("no lag without the capability: %v", statuses)
		}
	}
	reader.close(tb)
}

// L3 cap: dead_letter_source_receives. The dead-letter source takes a letter, its snodra-dlt-*
// headers included, and delivers it as published.
func DeadLetterSourceReceives(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	deadLetterSource := snodra.DeadLetterSource(kit.Source)
	key := fresh("key")
	letter := event(deadLetterSource, &key)
	letter.Headers["snodra-dlt-processor"] = "conformance"
	letter.Headers["snodra-dlt-reason"] = "FAILED"
	letter.Headers["snodra-dlt-error"] = "the handler failed"
	publishAll(tb, events, []snodra.OutgoingEvent{letter})
	spec := kit.spec()
	spec.Source = deadLetterSource
	reader := join(tb, events, spec)
	reader.until(tb, "the letter delivered from the dead-letter source", func(r *eventReader) bool {
		return r.has(letter.ID)
	})
	got := reader.firstDelivery(tb, letter.ID)
	for name, value := range letter.Headers {
		if got.Headers[name] != value {
			tb.Fatalf("header %s: got %q, want %q", name, got.Headers[name], value)
		}
	}
	if !sameKey(got.Key, letter.Key) {
		tb.Fatalf("the key: got %v, want %v", shownKey(got.Key), shownKey(letter.Key))
	}
	if string(got.Payload) != string(letter.Payload) {
		tb.Fatalf("the payload: got %q, want %q", got.Payload, letter.Payload)
	}
	reader.close(tb)
}

// L3 cap: no_browser_without_capability. The Go library has no event browser; a store that does
// not declare browse proves that, and one that does has the four browse checks omitted as a kit
// difference.
func NoBrowserWithoutCapability(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	if snodra.Supports(kit.instance(tb), snodra.CapabilityBrowse) {
		// The check is vacuous for such a store, and the Go kit cannot run the four browse checks
		// that would verify it: skip rather than report a pass.
		tb.Skip("the store declares browse; the Go kit has no event browser, so no_browser_without_capability is not applicable (a kit difference)")
	}
}

// segmentStatuses is the subscription's Segments(), or a failure.
func (r *eventReader) segmentStatuses(tb testing.TB) []snodra.SegmentStatus {
	tb.Helper()
	if r.subscription == nil {
		tb.Fatalf("%s: segments of a closed subscription", r.name)
	}
	return r.subscription.Segments()
}

// segmentNames is the segment names of statuses.
func segmentNames(statuses []snodra.SegmentStatus) []string {
	names := make([]string, len(statuses))
	for i, status := range statuses {
		names[i] = status.Segment
	}
	return names
}

// knownLag sums the lags of the segments with a known one, None when a required segment has no
// known lag or none is known.
func knownLag(reader *eventReader, required []string) (int64, bool) {
	if reader.subscription == nil {
		return 0, false
	}
	statuses := reader.subscription.Segments()
	for _, needed := range required {
		found := false
		for _, status := range statuses {
			if status.Segment == needed && status.Lag >= 0 {
				found = true
			}
		}
		if !found {
			return 0, false
		}
	}
	var sum int64
	known := false
	for _, status := range statuses {
		if status.Lag >= 0 {
			sum += status.Lag
			known = true
		}
	}
	return sum, known
}

// subset reports whether every element of want is in have.
func subset(have, want map[string]bool) bool {
	for key := range want {
		if !have[key] {
			return false
		}
	}
	return true
}

// sameStrings reports whether two string slices are equal, in order.
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
