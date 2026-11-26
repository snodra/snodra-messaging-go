package snodra

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSegmentOrderMatchesTheHub(t *testing.T) {
	names := []string{
		"b", "+5", "7", "99999999999999999999999", "007", "18446744073709551616",
		"10", "a", "0", "", "00", "5",
	}
	slices.SortFunc(names, SegmentOrder)
	want := []string{
		"0", "00", "5", "007", "7", "10", "18446744073709551616",
		"99999999999999999999999", "", "+5", "a", "b",
	}
	if !slices.Equal(names, want) {
		t.Fatalf("segment order: got %v, want %v", names, want)
	}
}

func TestProtocolNames(t *testing.T) {
	want := []string{"replay", "browse", "lag", "native-dead-letter"}
	for i, capability := range AllCapabilities {
		if got := capability.String(); got != want[i] {
			t.Errorf("capability %d: got %q, want %q", i, got, want[i])
		}
	}
	if got := DeadLetterSource("snodra.events"); got != "snodra.events.dlt" {
		t.Errorf("dead-letter source: got %q", got)
	}
	if len(EventHeaders) != 7 {
		t.Errorf("the seven event headers: %v", EventHeaders)
	}
}

func TestMemoryCompareOrdersPositions(t *testing.T) {
	store := NewMemoryEventStore()
	for _, test := range []struct {
		a, b string
		want int
	}{
		{"2", "10", -1},
		{"10", "2", 1},
		{"5", "5", 0},
		{"x", "10", 1},
		{"10", "x", -1},
		{"007", "7", -1},
	} {
		if got := store.Compare("0", test.a, test.b); got != test.want {
			t.Errorf("Compare(%q, %q) = %d, want %d", test.a, test.b, got, test.want)
		}
	}
}

func TestMemoryEventStoreNameAndCapabilities(t *testing.T) {
	store := NewMemoryEventStore()
	if got := store.Name(); got != "memory" {
		t.Fatalf("name: got %q, want memory", got)
	}
	want := []Capability{CapabilityReplay, CapabilityLag}
	if got := store.Capabilities(); !slices.Equal(got, want) {
		t.Fatalf("capabilities: got %v, want %v", got, want)
	}
	if !Supports(store, CapabilityReplay) || !Supports(store, CapabilityLag) {
		t.Fatal("the store declares replay and lag")
	}
	if Supports(store, CapabilityBrowse) || Supports(store, CapabilityNativeDeadLetter) {
		t.Fatal("the store declares no browse and no native dead letter")
	}
}

func TestMemoryEventStoreSegmentBounds(t *testing.T) {
	for _, segments := range []int{-1, 0, MaxMemoryEventStoreSegments + 1} {
		if _, err := NewMemoryEventStoreWithSegments(segments); !errors.Is(err, ErrInvalid) {
			t.Errorf("%d segments: got %v, want ErrInvalid", segments, err)
		}
	}
	for _, segments := range []int{1, DefaultMemoryEventStoreSegments, MaxMemoryEventStoreSegments} {
		if _, err := NewMemoryEventStoreWithSegments(segments); err != nil {
			t.Errorf("%d segments: %v", segments, err)
		}
	}
}

func TestMemoryZeroValueIsUsable(t *testing.T) {
	var store MemoryEventStore
	if err := store.Publish(context.Background(), eventFor("zero.events", nil, "zero")); err != nil {
		t.Fatal(err)
	}
	spec := SubscriptionSpec{Name: "zero.processor", Source: "zero.events"}
	subscription, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, subscription)
	if got := readAndAck(t, subscription, 1); len(got) != 1 {
		t.Fatalf("deliveries: %d", len(got))
	}
	if got := len(store.Events("zero.events")); got != 1 {
		t.Fatalf("events: %d", got)
	}
}

func TestMemoryKeyHashVectors(t *testing.T) {
	if got := fnv1a64(""); got != 0xcbf29ce484222325 {
		t.Fatalf("fnv1a64(%q) = %#x", "", got)
	}
	if got := fnv1a64("a"); got != 0xaf63dc4c8601ec8c {
		t.Fatalf("fnv1a64(%q) = %#x", "a", got)
	}
	keys := []string{"k", "order-1", "order-2", "é", ""}
	wantFour := []int{2, 1, 0, 1, 1}
	wantSix := []int{2, 3, 0, 1, 5}
	for i, key := range keys {
		if got := memorySegment(key, 4); got != wantFour[i] {
			t.Errorf("%q mod 4 = %d, want %d", key, got, wantFour[i])
		}
		if got := memorySegment(key, 6); got != wantSix[i] {
			t.Errorf("%q mod 6 = %d, want %d", key, got, wantSix[i])
		}
	}
}

func TestMemoryKeyedAndUnkeyedSegments(t *testing.T) {
	store := NewMemoryEventStore()
	keys := []string{"k", "order-1", "order-2", "é"}
	for _, key := range keys {
		key := key
		if err := store.Publish(context.Background(), eventFor("keyed.events", &key, "keyed-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	segmentOf := make(map[string]string)
	for _, delivery := range store.Events("keyed.events") {
		segmentOf[delivery.Headers[HeaderID]] = delivery.Segment
	}
	for i, key := range keys {
		want := []string{"2", "1", "0", "1"}[i]
		if got := segmentOf["keyed-"+key]; got != want {
			t.Errorf("key %q: segment %q, want %q", key, got, want)
		}
	}
	for i := 0; i < 3; i++ {
		key := "same-key"
		if err := store.Publish(context.Background(), eventFor("same.events", &key, "same")); err != nil {
			t.Fatal(err)
		}
	}
	segments := make(map[string]bool)
	for _, delivery := range store.Events("same.events") {
		segments[delivery.Segment] = true
	}
	if len(segments) != 1 {
		t.Fatalf("one key stays on one segment: %v", segments)
	}

	// The turn for unkeyed events belongs to the store, shared by its sources.
	unkeyed := NewMemoryEventStore()
	for _, source := range []string{"first.events", "second.events"} {
		for i := 0; i < 2; i++ {
			if err := unkeyed.Publish(context.Background(), eventFor(source, nil, source)); err != nil {
				t.Fatal(err)
			}
		}
	}
	first := unkeyed.Events("first.events")
	second := unkeyed.Events("second.events")
	if got := []string{first[0].Segment, first[1].Segment, second[0].Segment, second[1].Segment}; !slices.Equal(got, []string{"0", "1", "2", "3"}) {
		t.Fatalf("unkeyed events go round the store: got %v, want [0 1 2 3]", got)
	}
}

func TestMemoryKeepsItsOwnCopies(t *testing.T) {
	store := NewMemoryEventStore()
	key := "original"
	event := eventFor("copied.events", &key, "copied")
	event.Headers["x-extra"] = "original"
	if err := store.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	key = "changed"
	event.Headers["x-extra"] = "changed"
	event.Payload[0] = 'X'

	deliveries := store.Events("copied.events")
	if len(deliveries) != 1 {
		t.Fatalf("deliveries: %d", len(deliveries))
	}
	got := deliveries[0]
	if got.Key == nil || *got.Key != "original" {
		t.Fatalf("the key was not copied: %v", got.Key)
	}
	if got.Headers["x-extra"] != "original" {
		t.Fatalf("the headers were not copied: %q", got.Headers["x-extra"])
	}
	if string(got.Payload[:1]) != "{" {
		t.Fatalf("the payload was not copied: %q", got.Payload)
	}
	// A delivery is a copy too: changing it changes nothing in the store.
	got.Headers["x-extra"] = "changed"
	got.Payload[0] = 'Y'
	again := store.Events("copied.events")[0]
	if again.Headers["x-extra"] != "original" || string(again.Payload[:1]) != "{" {
		t.Fatal("a delivery must be a copy")
	}
}

func TestMemoryFirstAssignmentWithin(t *testing.T) {
	store := NewMemoryEventStore()
	spec := SubscriptionSpec{Name: "first.assignment", Source: "first.events"}
	subscription, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, subscription)
	ctx, cancel := context.WithTimeout(context.Background(), FirstAssignmentWithin)
	defer cancel()
	started := time.Now()
	event, err := subscription.Next(ctx)
	if err != nil {
		t.Fatalf("the first assignment: %v", err)
	}
	if elapsed := time.Since(started); elapsed > FirstAssignmentWithin {
		t.Fatalf("the first assignment took %v, want within %v", elapsed, FirstAssignmentWithin)
	}
	assigned, ok := event.(Assigned)
	if !ok {
		t.Fatalf("the first event is an Assigned, got %T", event)
	}
	if len(assigned.Segments) != DefaultMemoryEventStoreSegments {
		t.Fatalf("a lone instance holds every segment: %v", assigned.Segments)
	}
}

func TestMemoryMisuseIsAnError(t *testing.T) {
	store := NewMemoryEventStore()
	if err := store.Publish(context.Background(), eventFor("", nil, "no-source")); !errors.Is(err, ErrRejected) {
		t.Fatalf("an event without a source: %v", err)
	}
	for _, spec := range []SubscriptionSpec{{Name: "", Source: "s"}, {Name: "p", Source: ""}} {
		if _, err := store.Subscribe(context.Background(), spec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("subscribe %+v: %v", spec, err)
		}
	}
	spec := SubscriptionSpec{Name: "misuse", Source: "misuse.events"}
	subscription, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if event, err := subscription.Next(ctx); err != nil {
		t.Fatal(err)
	} else if _, ok := event.(Assigned); !ok {
		t.Fatalf("assigned first: %T", event)
	}
	for _, place := range [][2]string{{"9", "0"}, {"x", "0"}, {"0", "x"}, {"0", "5"}} {
		segment, position := place[0], place[1]
		if err := subscription.Ack(segment, position); !errors.Is(err, ErrInvalid) {
			t.Errorf("ack %s %s: %v", segment, position, err)
		}
		if err := subscription.Redeliver(segment, position); !errors.Is(err, ErrInvalid) {
			t.Errorf("redeliver %s %s: %v", segment, position, err)
		}
	}
	if got := store.Compare("0", "x", "10"); got <= 0 {
		t.Fatalf("Compare(x, 10) = %d, want greater", got)
	}
	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Apply(ctx, []ReplayRange{{
		Source: "another.source", Segment: "0", From: "0",
	}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a range of another source: %v", err)
	}
	for _, target := range []ResetTarget{
		ResetPositions{Positions: []SegmentPosition{{Segment: "9", Position: "0"}}},
		ResetPositions{Positions: []SegmentPosition{{Segment: "0", Position: "x"}}},
		ResetPositions{Positions: []SegmentPosition{{Segment: "0", Position: "1"}}},
	} {
		if _, err := admin.Plan(ctx, target); !errors.Is(err, ErrInvalid) {
			t.Errorf("plan %+v: %v", target, err)
		}
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatalf("close twice: %v", err)
	}
	if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("next after close: %v", err)
	}
	if err := subscription.Ack("0", "0"); !errors.Is(err, ErrClosed) {
		t.Fatalf("ack after close: %v", err)
	}
	if statuses := subscription.Segments(); statuses != nil {
		t.Fatalf("segments after close: %v", statuses)
	}
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := store.Publish(cancelled, eventFor("s", nil, "cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled publish: %v", err)
	}
}

func TestMemoryReplay(t *testing.T) {
	store := NewMemoryEventStore()
	source := "replay.events"
	key := "replay-key"
	for i := 0; i < 3; i++ {
		if err := store.Publish(context.Background(), eventFor(source, &key, "replay")); err != nil {
			t.Fatal(err)
		}
	}
	spec := SubscriptionSpec{Name: "replay.processor", Source: source}
	segment := firstDeliverySegment(store.Events(source))
	first, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	deliveries := readAndAck(t, first, 3)
	if len(deliveries) != 3 {
		t.Fatalf("deliveries: %d", len(deliveries))
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	members, err := admin.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 0 {
		t.Fatalf("no members after close: %v", members)
	}
	earliest, err := admin.Plan(ctx, ResetEarliest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(earliest) != DefaultMemoryEventStoreSegments {
		t.Fatalf("earliest moves every segment: %v", earliest)
	}
	found := false
	for _, replay := range earliest {
		if replay.Segment == segment {
			found = true
			if replay.From != "0" || replay.Until != "3" {
				t.Fatalf("earliest replays %s: %+v", segment, replay)
			}
		}
	}
	if !found {
		t.Fatalf("the segment %s is planned: %v", segment, earliest)
	}

	// While an instance reads the subscription, nothing moves.
	active, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	waitForAssignment(t, active)
	if err := admin.Apply(ctx, earliest); !errors.Is(err, ErrSubscribersActive) {
		t.Fatalf("apply while a member reads: %v", err)
	}
	if members, err := admin.Members(ctx); err != nil || len(members) != 1 || !strings.HasPrefix(members[0], "memory-") {
		t.Fatalf("members while one reads: %v, %v", members, err)
	}
	if err := active.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := admin.Apply(ctx, earliest); err != nil {
		t.Fatalf("apply earliest: %v", err)
	}
	replayed, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAndAck(t, replayed, 3); len(got) != 3 {
		t.Fatalf("the acknowledged events replayed: %d", len(got))
	}
	if err := replayed.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	latest, err := admin.Plan(ctx, ResetLatest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, replay := range latest {
		if replay.Segment != segment {
			continue
		}
		if replay.From != "3" || replay.Until != "3" {
			t.Fatalf("latest: %+v", replay)
		}
		if store.Compare(segment, replay.From, replay.Until) < 0 {
			t.Fatalf("latest replays nothing: %+v", replay)
		}
	}
	positions, err := admin.Plan(ctx, ResetPositions{Positions: []SegmentPosition{{Segment: segment, Position: "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 || positions[0].Segment != segment || positions[0].From != "1" || positions[0].Until != "3" {
		t.Fatalf("positions move the listed segments only: %v", positions)
	}
}

func TestMemorySubscriptionSegments(t *testing.T) {
	store := NewMemoryEventStore()
	source := "segments.events"
	key := "segments-key"
	for i := 0; i < 2; i++ {
		if err := store.Publish(context.Background(), eventFor(source, &key, "segments")); err != nil {
			t.Fatal(err)
		}
	}
	segment := firstDeliverySegment(store.Events(source))
	spec := SubscriptionSpec{Name: "segments.processor", Source: source}
	subscription, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, subscription)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		event, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if assigned, ok := event.(Assigned); ok && len(assigned.Segments) == DefaultMemoryEventStoreSegments {
			break
		}
	}
	var deliveries []Delivery
	for len(deliveries) < 2 {
		event, err := subscription.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		delivery, ok := event.(Delivery)
		if !ok {
			continue
		}
		deliveries = append(deliveries, delivery)
	}
	if err := subscription.Ack(deliveries[0].Segment, deliveries[0].Position); err != nil {
		t.Fatal(err)
	}
	statuses := subscription.Segments()
	want := SegmentStatus{}
	for _, status := range statuses {
		if status.Segment == segment {
			want = status
		}
	}
	if want.Segment == "" {
		t.Fatalf("the segment %s is listed: %v", segment, statuses)
	}
	if want.Position != "2" || want.Committed != "1" || want.Head != "2" {
		t.Fatalf("positions: %+v", want)
	}
	if want.Lag != 1 {
		t.Fatalf("lag counts the unacknowledged event: %+v", want)
	}
	if !sortedBySegment(statuses) {
		t.Fatalf("segments are listed in segment order: %v", statuses)
	}
	if err := subscription.Ack(deliveries[1].Segment, deliveries[1].Position); err != nil {
		t.Fatal(err)
	}
	for _, status := range subscription.Segments() {
		if status.Segment == segment && status.Lag != 0 {
			t.Fatalf("lag after the acknowledgement: %+v", status)
		}
	}
}

func TestMemoryEventStoreConfig(t *testing.T) {
	store, err := NewMemoryEventStoreWithSegments(7)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(store.Events("empty")); got != 0 {
		t.Fatalf("an empty source: %d events", got)
	}
	subscription, err := store.Subscribe(context.Background(), SubscriptionSpec{Name: "p", Source: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, subscription)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assigned := event.(Assigned)
	if len(assigned.Segments) != 7 {
		t.Fatalf("seven segments: %v", assigned.Segments)
	}
}

// SG-28: redelivering a position at or after the subscription's next one changes nothing, so it
// never skips an event.
func TestMemoryRedeliverForwardDoesNotSkip(t *testing.T) {
	store := NewMemoryEventStore()
	source := "redeliver.forward.events"
	key := "order-1" // segment "1" of the four, by the pinned hash vector
	var sent []OutgoingEvent
	for i := 0; i < 3; i++ {
		sent = append(sent, eventFor(source, &key, "redeliver-forward-"+strconv.Itoa(i)))
	}
	for _, event := range sent {
		if err := store.Publish(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	spec := SubscriptionSpec{Name: "redeliver.forward", Source: source}
	subscription, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, subscription)

	first := nextDelivery(t, subscription)
	segment := first.Segment
	if segment != "1" || first.Position != "0" {
		t.Fatalf("first delivery: segment %s position %s, want 1 0", segment, first.Position)
	}
	status := statusOf(t, subscription, segment)
	if status.Position != "1" || status.Head != "3" {
		t.Fatalf("after one of three: position %q, head %q, want 1 and 3", status.Position, status.Head)
	}
	if err := subscription.Redeliver(segment, "2"); err != nil {
		t.Fatalf("redeliver 2: %v", err)
	}
	second := nextDelivery(t, subscription)
	if second.Segment != segment || second.Position != "1" {
		t.Fatalf("a redelivery at or after the next position changes nothing: got %s %s, want %s 1",
			second.Segment, second.Position, segment)
	}
	if got := second.Headers[HeaderID]; got != sent[1].ID {
		t.Fatalf("the next event is still the second one: got %q, want %q", got, sent[1].ID)
	}
}

// SG-27: an acknowledgement of a segment that changed hands is dropped, so its new holder still
// starts at the committed position and receives the unacknowledged event.
func TestMemoryLateAckAfterHandoverIsDropped(t *testing.T) {
	store := NewMemoryEventStore()
	source := "late.ack.events"
	key := "order-1" // segment "1" of the four, by the pinned hash vector
	handled := eventFor(source, &key, "late-ack")
	if err := store.Publish(context.Background(), handled); err != nil {
		t.Fatal(err)
	}
	spec := SubscriptionSpec{Name: "late.ack", Source: source}
	first, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, first)
	delivery := nextDelivery(t, first)
	if delivery.Segment != "1" || delivery.Position != "0" {
		t.Fatalf("first delivery: segment %s position %s, want 1 0", delivery.Segment, delivery.Position)
	}

	second, err := store.Subscribe(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription(t, second)

	// The first instance must see the segment revoked before it acknowledges it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	revoked := false
	for !revoked {
		event, err := first.Next(ctx)
		if err != nil {
			t.Fatalf("first instance: next: %v", err)
		}
		switch event := event.(type) {
		case Revoked:
			for _, segment := range event.Segments {
				if segment == delivery.Segment {
					revoked = true
				}
			}
		case Assigned:
			// A closing assignment may come before or after the revocation.
		case Delivery:
			t.Fatalf("first instance: an unexpected delivery after its event: %+v", event)
		default:
			t.Fatalf("first instance: unknown subscription event %T", event)
		}
	}
	if err := first.Ack(delivery.Segment, delivery.Position); err != nil {
		t.Fatalf("an acknowledgement of a segment that changed hands: %v", err)
	}

	again := nextDelivery(t, second)
	if again.Segment != delivery.Segment || again.Position != "0" || again.Headers[HeaderID] != handled.ID {
		t.Fatalf("the new holder receives the unacknowledged event: got %s %s %q, want %s 0 %q",
			again.Segment, again.Position, again.Headers[HeaderID], delivery.Segment, handled.ID)
	}
	status := statusOf(t, second, delivery.Segment)
	if status.Committed != "" {
		t.Fatalf("the late acknowledgement moved the committed position: %+v", status)
	}
}

// nextDelivery is the next delivery of subscription, skipping assignment changes.
func nextDelivery(t *testing.T, subscription EventSubscription) Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		event, err := subscription.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		switch event := event.(type) {
		case Delivery:
			return event
		case Assigned, Revoked:
			continue
		default:
			t.Fatalf("unknown subscription event %T", event)
		}
	}
}

// statusOf is the status of segment in the subscription's current assignment.
func statusOf(t *testing.T, subscription EventSubscription, segment string) SegmentStatus {
	t.Helper()
	for _, status := range subscription.Segments() {
		if status.Segment == segment {
			return status
		}
	}
	t.Fatalf("segment %s is not assigned", segment)
	return SegmentStatus{}
}

// eventFor is one event of these tests: headers id/type, an optional key and a JSON object
// payload.
func eventFor(source string, key *string, id string) OutgoingEvent {
	headers := map[string]string{
		HeaderType: "test.Event",
		HeaderID:   id,
	}
	return OutgoingEvent{
		Source:  source,
		ID:      id,
		Type:    "test.Event",
		Key:     key,
		Headers: headers,
		Payload: []byte(`{"ok":true}`),
	}
}

func firstDeliverySegment(deliveries []Delivery) string {
	if len(deliveries) == 0 {
		return ""
	}
	return deliveries[0].Segment
}

func readAndAck(t *testing.T, subscription EventSubscription, count int) []Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var deliveries []Delivery
	for len(deliveries) < count {
		event, err := subscription.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		delivery, ok := event.(Delivery)
		if !ok {
			continue
		}
		if err := subscription.Ack(delivery.Segment, delivery.Position); err != nil {
			t.Fatalf("ack: %v", err)
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries
}

func waitForAssignment(t *testing.T, subscription EventSubscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if _, ok := event.(Assigned); !ok {
		t.Fatalf("assigned first: %T", event)
	}
}

func closeSubscription(t *testing.T, subscription EventSubscription) {
	t.Helper()
	if err := subscription.Close(context.Background()); err != nil {
		t.Errorf("close: %v", err)
	}
}

func sortedBySegment(statuses []SegmentStatus) bool {
	for i := 1; i < len(statuses); i++ {
		if SegmentOrder(statuses[i-1].Segment, statuses[i].Segment) >= 0 {
			return false
		}
	}
	return true
}
