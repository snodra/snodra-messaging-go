package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// keyForPartition finds a key the stream clients' hash routing places on partition.
func keyForPartition(t *testing.T, partition, partitions int) string {
	t.Helper()
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("plan-key-%d", i)
		if partitionIndex(key, partitions) == partition {
			return key
		}
	}
	t.Fatalf("no key for partition %d of %d", partition, partitions)
	return ""
}

// Plan on a real super stream: earliest, latest, timestamp and positions, each segment with its
// stored offset as until; duplicate, unknown and past-head positions are invalid.
func TestAdminPlanTargets(t *testing.T) {
	broker := rabbitBroker(t)
	const partitions = 3
	source := broker.superStream(t, partitions)
	store := broker.store(t)
	keys := make([]string, partitions)
	for i := range keys {
		keys[i] = keyForPartition(t, i, partitions)
	}
	for round := 0; round < 2; round++ {
		for partition := 0; partition < partitions; partition++ {
			key := keys[partition]
			if err := store.Publish(t.Context(), event(source, fmt.Sprintf("plan-%d-%d", round, partition), &key)); err != nil {
				t.Fatalf("publishing round %d partition %d: %v", round, partition, err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	spec := snodra.SubscriptionSpec{Name: "orders-service.plan", Source: source}
	sub := subscribe(t, store, spec)
	deliveries := readDeliveries(t, sub, partitions*2, 30*time.Second)
	closeSubscription(t, sub)
	bySegment := make(map[string][]snodra.Delivery)
	for _, delivery := range deliveries {
		bySegment[delivery.Segment] = append(bySegment[delivery.Segment], delivery)
	}
	if len(bySegment) != partitions {
		t.Fatalf("deliveries of %d segments, want %d: %v", len(bySegment), partitions, bySegment)
	}
	segments := make([]string, 0, partitions)
	head := make(map[string]uint64)
	secondRoundTarget := int64(1<<62 - 1)
	for segment, found := range bySegment {
		if len(found) != 2 {
			t.Fatalf("segment %s: %d deliveries, want 2", segment, len(found))
		}
		segments = append(segments, segment)
		head[segment] = mustOffset(t, found[1].Position) + 1
		if found[1].TimestampMs < secondRoundTarget {
			secondRoundTarget = found[1].TimestampMs
		}
	}
	sort.Slice(segments, func(i, j int) bool { return snodra.SegmentOrder(segments[i], segments[j]) < 0 })
	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}

	rangeOf := func(segment, from string) snodra.ReplayRange {
		return snodra.ReplayRange{Source: source, Segment: segment, From: from, Until: fmt.Sprintf("%d", head[segment])}
	}
	sortRanges := func(ranges []snodra.ReplayRange) {
		sort.Slice(ranges, func(i, j int) bool { return snodra.SegmentOrder(ranges[i].Segment, ranges[j].Segment) < 0 })
	}
	var earliest, latest, positions, timestamp []snodra.ReplayRange
	var positionInputs []snodra.SegmentPosition
	for _, segment := range segments {
		earliest = append(earliest, rangeOf(segment, "0"))
		latest = append(latest, rangeOf(segment, fmt.Sprintf("%d", head[segment])))
		positions = append(positions, rangeOf(segment, "1"))
		positionInputs = append(positionInputs, snodra.SegmentPosition{Segment: segment, Position: "1"})
		from := ""
		for _, delivery := range bySegment[segment] {
			if delivery.TimestampMs >= secondRoundTarget {
				from = delivery.Position
				break
			}
		}
		if from == "" {
			from = fmt.Sprintf("%d", head[segment])
		}
		timestamp = append(timestamp, rangeOf(segment, from))
	}
	sortRanges(timestamp)
	cases := []struct {
		name   string
		target snodra.ResetTarget
		want   []snodra.ReplayRange
	}{
		{"earliest", snodra.ResetEarliest{}, earliest},
		{"latest", snodra.ResetLatest{}, latest},
		{"timestamp", snodra.ResetTimestamp{TimestampMs: secondRoundTarget}, timestamp},
		{"positions", snodra.ResetPositions{Positions: positionInputs}, positions},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := admin.Plan(t.Context(), c.target)
			if err != nil {
				t.Fatalf("Plan(%s): %v", c.name, err)
			}
			sortRanges(got)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("Plan(%s): got %+v, want %+v", c.name, got, c.want)
			}
		})
	}

	invalid := []struct {
		name   string
		target snodra.ResetTarget
	}{
		{"duplicate", snodra.ResetPositions{Positions: []snodra.SegmentPosition{
			{Segment: segments[0], Position: "0"}, {Segment: segments[0], Position: "1"},
		}}},
		{"unknown", snodra.ResetPositions{Positions: []snodra.SegmentPosition{{Segment: "99", Position: "0"}}}},
		{"past the head", snodra.ResetPositions{Positions: []snodra.SegmentPosition{
			{Segment: segments[0], Position: fmt.Sprintf("%d", head[segments[0]]+1)},
		}}},
		{"not an offset", snodra.ResetPositions{Positions: []snodra.SegmentPosition{{Segment: segments[0], Position: "abc"}}}},
	}
	for _, c := range invalid {
		t.Run("invalid "+c.name, func(t *testing.T) {
			if _, err := admin.Plan(t.Context(), c.target); !errors.Is(err, snodra.ErrInvalid) {
				t.Fatalf("Plan(%s): %v, want ErrInvalid", c.name, err)
			}
		})
	}
}

func TestApplyMovesEveryPartitionAndReadsItBack(t *testing.T) {
	broker := rabbitBroker(t)
	const partitions = 3
	source := broker.superStream(t, partitions)
	store := broker.store(t)
	keys := make([]string, partitions)
	for i := range keys {
		keys[i] = keyForPartition(t, i, partitions)
	}
	for partition := 0; partition < partitions; partition++ {
		key := keys[partition]
		if err := store.Publish(t.Context(), event(source, fmt.Sprintf("apply-%d", partition), &key)); err != nil {
			t.Fatalf("publishing partition %d: %v", partition, err)
		}
	}
	spec := snodra.SubscriptionSpec{Name: "orders-service.apply", Source: source}
	sub := subscribe(t, store, spec)
	readDeliveries(t, sub, partitions, 30*time.Second)
	closeSubscription(t, sub)
	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	waitNoMembers(t, admin)
	ranges, err := admin.Plan(t.Context(), snodra.ResetEarliest{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(ranges) != partitions {
		t.Fatalf("ranges: %d, want %d: %+v", len(ranges), partitions, ranges)
	}
	if err := admin.Apply(t.Context(), ranges); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for partition := 0; partition < partitions; partition++ {
		stream := fmt.Sprintf("%s-%d", source, partition)
		stored, found := storedOffset(t, store, spec.Name, stream)
		if !found || stored != 0 {
			t.Fatalf("%s: stored offset %d (found %t), want 0", stream, stored, found)
		}
	}
	again := subscribe(t, store, spec)
	redelivered := readDeliveries(t, again, partitions, 30*time.Second)
	if len(redelivered) != partitions {
		t.Fatalf("the reset replays every partition: %d deliveries", len(redelivered))
	}
	closeSubscription(t, again)
}

func waitNoMembers(t *testing.T, admin snodra.SubscriptionAdmin) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		members, err := admin.Members(context.Background())
		if err != nil {
			t.Fatalf("members: %v", err)
		}
		if len(members) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the subscription still lists members: %v", members)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func mustOffset(t *testing.T, position string) uint64 {
	t.Helper()
	offset, err := parseOffset(position)
	if err != nil {
		t.Fatal(err)
	}
	return offset
}
