package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The reset side plans and moves a group's offsets: while an instance reads the subscription
// it refuses, and with the group empty it moves the positions, so the group replays.
func TestTheResetSidePlansAndMovesTheGroupsOffsets(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("reset")
	createTopic(t, broker, topic, 2, nil)
	store := newTestStore(t, broker)
	var sentIDs []string
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("order-%d", i)
		id := fmt.Sprintf("reset-%d", i)
		if err := store.Publish(t.Context(), event(topic, id, &key)); err != nil {
			t.Fatalf("publishing %s: %v", id, err)
		}
		sentIDs = append(sentIDs, id)
	}
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "reset"), Source: topic}
	sub := subscribe(t, store, spec)
	waitForAssignment(t, sub, 10*time.Second)
	firstRun := readDeliveries(t, sub, len(sentIDs), 30*time.Second)
	committed := committedOf(t, firstRun)
	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	members, err := admin.Members(t.Context())
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("members %v, want the one instance", members)
	}

	earliest, err := admin.Plan(t.Context(), snodra.ResetEarliest{})
	if err != nil {
		t.Fatalf("plan earliest: %v", err)
	}
	if len(earliest) != 2 {
		t.Fatalf("the plan moves %d segments, want 2", len(earliest))
	}
	for _, replay := range earliest {
		if replay.Source != topic {
			t.Fatalf("range of %q, want %q", replay.Source, topic)
		}
		if replay.From != "0" {
			t.Fatalf("earliest moves %s to %q, want 0", replay.Segment, replay.From)
		}
		if want, ok := committed[replay.Segment]; ok && replay.Until != want {
			t.Fatalf("until of %s is %q, want %q", replay.Segment, replay.Until, want)
		}
	}
	if err := admin.Apply(t.Context(), earliest); !errors.Is(err, snodra.ErrSubscribersActive) {
		t.Fatalf("apply while an instance reads: %v, want ErrSubscribersActive", err)
	}
	if err := sub.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
	waitNoMembers(t, admin, 10*time.Second)
	if err := admin.Apply(t.Context(), earliest); err != nil {
		t.Fatalf("apply earliest: %v", err)
	}

	replayed := subscribe(t, store, spec)
	again := readDeliveries(t, replayed, len(sentIDs), 30*time.Second)
	if got := ids(again); !reflect.DeepEqual(sortedCopy(got), sortedCopy(sentIDs)) {
		t.Fatalf("the replay delivered %v, want %v", got, sentIDs)
	}
	for _, delivery := range again {
		if delivery.Position == "" {
			t.Fatalf("the replay has no position: %+v", delivery)
		}
	}

	latest, err := admin.Plan(t.Context(), snodra.ResetLatest{})
	if err != nil {
		t.Fatalf("plan latest: %v", err)
	}
	for _, replay := range latest {
		if replay.From == "" || replay.From == "0" {
			t.Fatalf("latest moves %s to %q", replay.Segment, replay.From)
		}
	}
	if err := replayed.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
	waitNoMembers(t, admin, 10*time.Second)
	if err := admin.Apply(t.Context(), latest); err != nil {
		t.Fatalf("apply latest: %v", err)
	}
	atHead := subscribe(t, store, spec)
	waitForAssignment(t, atHead, 10*time.Second)
	expectNoDeliveries(t, atHead, 500*time.Millisecond)
	// A target position the topic does not have is invalid.
	if _, err := admin.Plan(t.Context(), snodra.ResetPositions{Positions: []snodra.SegmentPosition{
		{Segment: "99", Position: "0"},
	}}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("an unknown segment: %v, want ErrInvalid", err)
	}
}

// The segments report the next position, the committed one, the head and the lag.
func TestSegmentsReportPositionsAndLag(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("segments")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)
	key := "order-42"
	for i := 0; i < 3; i++ {
		if err := store.Publish(t.Context(), event(topic, fmt.Sprintf("segment-%d", i), &key)); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "segments"), Source: topic}
	sub := subscribe(t, store, spec)
	waitForAssignment(t, sub, 10*time.Second)
	delivery := readDeliveries(t, sub, 1, 30*time.Second)[0]
	if delivery.Segment != "0" || delivery.Position != "0" {
		t.Fatalf("the first delivery is %s/%s", delivery.Segment, delivery.Position)
	}
	waitSegment(t, sub, 10*time.Second, func(status snodra.SegmentStatus) bool {
		return status.Segment == "0" && status.Position == "1" && status.Committed == "1" &&
			status.Head == "3" && status.Lag == 2
	})
}

// A group that never existed has no members and no committed offsets, and its plan moves
// nothing to an offset the topic does not have.
func TestTheResetSideOfAGroupThatNeverExisted(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("empty-group")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "never"), Source: topic}
	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	members, err := admin.Members(t.Context())
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("a group that never existed has members %v", members)
	}
	plan, err := admin.Plan(t.Context(), snodra.ResetEarliest{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(plan) != 1 || plan[0].From != "0" || plan[0].Until != "" {
		t.Fatalf("the plan of a group with no commit: %+v", plan)
	}
	if err := admin.Apply(t.Context(), plan); err != nil {
		t.Fatalf("apply: %v", err)
	}
	plan, err = admin.Plan(t.Context(), snodra.ResetPositions{Positions: []snodra.SegmentPosition{
		{Segment: "0", Position: "0"},
	}})
	if err != nil {
		t.Fatalf("plan a position the empty topic has: %v", err)
	}
	if plan[0].From != "0" || plan[0].Until != "0" {
		t.Fatalf("the plan of a committed empty group: %+v", plan)
	}
	if _, err := admin.Plan(t.Context(), snodra.ResetPositions{Positions: []snodra.SegmentPosition{
		{Segment: "0", Position: "1"},
	}}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("a position past the head: %v, want ErrInvalid", err)
	}
}

// committedOf is the committed offset after each delivered segment: one after its last delivery.
func committedOf(t *testing.T, deliveries []snodra.Delivery) map[string]string {
	t.Helper()
	committed := make(map[string]int64)
	for _, delivery := range deliveries {
		offset, err := strconv.ParseInt(delivery.Position, 10, 64)
		if err != nil {
			t.Fatalf("position %q: %v", delivery.Position, err)
		}
		if next := offset + 1; next > committed[delivery.Segment] {
			committed[delivery.Segment] = next
		}
	}
	formatted := make(map[string]string, len(committed))
	for segment, offset := range committed {
		formatted[segment] = strconv.FormatInt(offset, 10)
	}
	return formatted
}

// expectNoDeliveries reads for the window and fails on any delivery.
func expectNoDeliveries(t *testing.T, sub *subscription, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline))
		event, err := sub.Next(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			t.Fatalf("next: %v", err)
		}
		if delivery, ok := event.(snodra.Delivery); ok {
			t.Fatalf("a delivery at the head: %+v", delivery)
		}
	}
}

func sortedCopy(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return sorted
}
