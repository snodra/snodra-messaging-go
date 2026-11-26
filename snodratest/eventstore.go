package snodratest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// HeadersRoundTrip checks that the seven headers, any other header, the key (or none) and the
// payload arrive as published.
func HeadersRoundTrip(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	key := fresh("key")
	keyed := event(kit.Source, &key)
	keyed.Headers["x-conformance"] = "kept as given"
	unkeyed := event(kit.Source, nil)
	sent := []snodra.OutgoingEvent{keyed, unkeyed}
	publishAll(tb, events, sent)

	reader := join(tb, events, kit.spec())
	ids := eventIDs(sent)
	reader.until(tb, "both events delivered", func(r *eventReader) bool { return r.hasAll(ids) })
	for _, want := range sent {
		got := reader.firstDelivery(tb, want.ID)
		for _, name := range snodra.EventHeaders {
			if _, ok := got.Headers[name]; !ok {
				tb.Fatalf("header %s delivered", name)
			}
		}
		for name, value := range want.Headers {
			if got.Headers[name] != value {
				tb.Fatalf("header %s: got %q, want %q", name, got.Headers[name], value)
			}
		}
		if !sameKey(got.Key, want.Key) {
			tb.Fatalf("the key: got %v, want %v", shownKey(got.Key), shownKey(want.Key))
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			tb.Fatalf("the payload: got %q, want %q", got.Payload, want.Payload)
		}
		if got.TimestampMs <= 0 {
			tb.Fatalf("an append time: %d", got.TimestampMs)
		}
	}
	reader.close(tb)
}

// SameKeyArrivesInPublishOrder checks that events of one key arrive in publish order, all from
// one segment, each once; the positions of a segment's deliveries increase by Compare.
func SameKeyArrivesInPublishOrder(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	keys := []string{fresh("key"), fresh("key"), fresh("key"), fresh("key")}
	var sent []snodra.OutgoingEvent
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			key := key
			sent = append(sent, event(kit.Source, &key))
		}
	}
	publishAll(tb, events, sent)

	reader := join(tb, events, kit.spec())
	all := eventIDs(sent)
	reader.until(tb, "every event delivered", func(r *eventReader) bool { return r.hasAll(all) })
	got := reader.mine(all)
	if len(got) != len(sent) {
		tb.Fatalf("each event once: got %d deliveries of %d events", len(got), len(sent))
	}
	for _, key := range keys {
		var published []string
		for _, want := range sent {
			if sameKeyValue(want.Key, key) {
				published = append(published, want.ID)
			}
		}
		var delivered []string
		segments := make(map[string]bool)
		for _, delivery := range got {
			if sameKeyValue(delivery.Key, key) {
				delivered = append(delivered, idOf(delivery))
				segments[delivery.Segment] = true
			}
		}
		if !slices.Equal(delivered, published) {
			tb.Fatalf("key %s: publish order, got %v, want %v", key, delivered, published)
		}
		if len(segments) != 1 {
			tb.Fatalf("key %s: one segment, got %v", key, sortedKeys(segments))
		}
	}
	last := make(map[string]string)
	for _, delivery := range got {
		if before, ok := last[delivery.Segment]; ok {
			if events.Compare(delivery.Segment, before, delivery.Position) >= 0 {
				tb.Fatalf("segment %s: %s before %s", delivery.Segment, before, delivery.Position)
			}
		}
		last[delivery.Segment] = delivery.Position
	}
	reader.close(tb)
}

// EveryProcessorGetsEveryEvent checks that two subscription names (two processors) each receive
// every event.
func EveryProcessorGetsEveryEvent(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	var sent []snodra.OutgoingEvent
	for i := 0; i < 10; i++ {
		key := fresh("key")
		sent = append(sent, event(kit.Source, &key))
	}
	publishAll(tb, events, sent)

	first := join(tb, events, kit.spec())
	second := join(tb, events, kit.spec())
	all := eventIDs(sent)
	allUntil(tb, []*eventReader{first, second}, "both processors get every event", func(readers []*eventReader) bool {
		return readers[0].hasAll(all) && readers[1].hasAll(all)
	})
	first.close(tb)
	second.close(tb)
}

// InstancesShareAProcessor checks that two instances of one subscription split its segments
// (adding the second spreads them, every revocation closed by an assignment) and share its
// events: each event goes to one of them, and both get some.
func InstancesShareAProcessor(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	firstStore := kit.instance(tb)
	secondStore := kit.instance(tb)
	spec := kit.spec()

	first := join(tb, firstStore, spec)
	first.until(tb, "the first instance holds segments", func(r *eventReader) bool {
		return len(r.assigned) > 0
	})
	second := join(tb, secondStore, spec)
	allUntil(tb, []*eventReader{first, second},
		"both instances hold segments, none held by both, every revocation closed by an assignment",
		func(readers []*eventReader) bool {
			a, b := readers[0], readers[1]
			return len(a.assigned) > 0 && len(b.assigned) > 0 &&
				disjoint(a.assigned, b.assigned) && !a.changing && !b.changing
		})

	var sent []snodra.OutgoingEvent
	for i := 0; i < 40; i++ {
		key := fresh("key")
		sent = append(sent, event(kit.Source, &key))
	}
	publishAll(tb, firstStore, sent)
	all := eventIDs(sent)
	allUntil(tb, []*eventReader{first, second}, "every event delivered", func(readers []*eventReader) bool {
		for _, id := range all {
			if !readers[0].has(id) && !readers[1].has(id) {
				return false
			}
		}
		return true
	})
	toFirst := deliveryIDs(first.mine(all))
	toSecond := deliveryIDs(second.mine(all))
	if len(toFirst) == 0 || len(toSecond) == 0 {
		tb.Fatalf("both instances get events: %d and %d", len(toFirst), len(toSecond))
	}
	for id := range toFirst {
		if toSecond[id] {
			tb.Fatalf("delivered to both instances: %s", id)
		}
	}
	first.close(tb)
	second.close(tb)
}

// AJoiningInstanceAnnouncesItsFirstAssignment checks that instances join one subscription one at
// a time, one more than its segments, each reading the whole time: every newcomer's first event is
// an Assigned (an empty one when it holds nothing, as the last one may), within
// FirstAssignmentWithin of joining.
func AJoiningInstanceAnnouncesItsFirstAssignment(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	spec := kit.spec()
	firstStore := kit.instance(tb)
	readers := []*eventReader{join(tb, firstStore, spec)}
	readers[0].until(tb, "the first instance holds segments", func(r *eventReader) bool {
		return len(r.assigned) > 0
	})
	readers[0].pump(tb, quiet)
	segments := len(readers[0].assigned)
	for len(readers) <= segments {
		store := kit.instance(tb)
		readers = append(readers, join(tb, store, spec))
		newcomer := len(readers) - 1
		deadline := time.Now().Add(snodra.FirstAssignmentWithin)
		for readers[newcomer].first == "" {
			if time.Now().After(deadline) {
				tb.Fatalf("instance %d of %d (assigned %v): its first assignment, possibly empty, within %v of joining",
					newcomer+1, segments+1, assignedLists(readers), snodra.FirstAssignmentWithin)
			}
			pumpAll(tb, readers, parallelSlice)
		}
		if readers[newcomer].first != "Assigned" {
			tb.Fatalf("instance %d: the first event of a new subscription is an Assigned, got %s",
				newcomer+1, readers[newcomer].first)
		}
	}
	for _, reader := range readers {
		reader.close(tb)
	}
}

// AnInstanceLeftWithoutSegmentsClosesItsChange checks that more instances than segments join one
// at a time; after each join the segments are spread (none held twice, every one held, and once
// there are two instances or more no former holder keeps every segment) and every instance's last
// assignment change closed (a Revoked followed by an Assigned, an empty one for an instance left
// without segments).
//
// The instances join until one that held segments holds none, or there are twice as many instances
// as segments, two more; a run that reaches that cap without such an instance says so on the log,
// and the check then proved less. How the segments spread beyond that is the event store's.
func AnInstanceLeftWithoutSegmentsClosesItsChange(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	spec := kit.spec()
	firstStore := kit.instance(tb)
	readers := []*eventReader{join(tb, firstStore, spec)}
	readers[0].until(tb, "the first instance holds segments", func(r *eventReader) bool {
		return len(r.assigned) > 0
	})
	// Alone, it holds every segment (in time: an assignment may come in parts).
	readers[0].pump(tb, quiet)
	all := make(map[string]bool, len(readers[0].assigned))
	for segment := range readers[0].assigned {
		all[segment] = true
	}
	cap := 2*len(all) + 2
	emptied := false
	for !emptied && len(readers) < cap {
		var former []int
		for i, reader := range readers {
			if len(reader.assigned) > 0 {
				former = append(former, i)
			}
		}
		revokedBefore := revokedCount(readers)
		store := kit.instance(tb)
		readers = append(readers, join(tb, store, spec))
		newcomer := len(readers) - 1
		deadline := time.Now().Add(joinWindow)
		for revokedCount(readers) == revokedBefore && !time.Now().After(deadline) {
			taken := pumpAll(tb, readers, parallelSlice)
			if taken == 0 && readers[newcomer].changes > 0 {
				break
			}
		}
		allUntil(tb, readers, "the segments spread, every revocation closed by an assignment",
			func(readers []*eventReader) bool { return settled(readers, former, all) })
		for _, reader := range readers {
			if len(reader.assigned) == 0 && reader.revoked > 0 {
				emptied = true
			}
		}
	}
	if !emptied {
		tb.Logf("event store conformance check `an_instance_left_without_segments_closes_its_change`: %d instances and none that held segments was left without any (assigned %v): the empty assignment that closes such a change was not seen",
			cap, assignedLists(readers))
	}
	anyEmpty := false
	for _, reader := range readers {
		if len(reader.assigned) == 0 {
			anyEmpty = true
		}
	}
	if !anyEmpty {
		tb.Fatal("more instances than segments leave one without segments")
	}
	for _, reader := range readers {
		reader.close(tb)
	}
}

// UnackedDeliveryIsRedelivered checks that what a subscription did not acknowledge comes again:
// to the next subscription of the name after a close, and on a live one after Redeliver; what it
// acknowledged does not.
func UnackedDeliveryIsRedelivered(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	spec := kit.spec()
	key := fresh("key")
	k := key
	var sent []snodra.OutgoingEvent
	for i := 0; i < 3; i++ {
		sent = append(sent, event(kit.Source, &k))
	}
	all := eventIDs(sent)
	unacked := all[1:]
	publishAll(tb, events, sent)

	first := join(tb, events, spec)
	first.hold(unacked)
	first.until(tb, "the three events delivered", func(r *eventReader) bool { return r.hasAll(all) })
	first.close(tb)

	restarted := kit.instance(tb)
	second := join(tb, restarted, spec)
	second.hold(unacked)
	second.until(tb, "the unacknowledged events again", func(r *eventReader) bool { return r.hasAll(unacked) })
	if got := second.idsInOrder(all); !slices.Equal(got, unacked) {
		tb.Fatalf("after a close: the unacknowledged ones, in order, not the acknowledged one: got %v, want %v",
			got, unacked)
	}

	again := second.firstDelivery(tb, unacked[0])
	second.redeliver(tb, again)
	second.until(tb, "the unacknowledged events once more", func(r *eventReader) bool {
		return len(r.mine(unacked)) == 4
	})
	want := append(append([]string(nil), unacked...), unacked...)
	if got := second.idsInOrder(all); !slices.Equal(got, want) {
		tb.Fatalf("after redeliver: from the redelivered one on, in order: got %v, want %v", got, want)
	}
	last := second.firstDelivery(tb, unacked[1])
	second.ack(tb, last)
	second.close(tb)
}

// RedeliveredAfterCommitIsSkipped checks that a crash after the handler's transaction committed
// and before the acknowledgement makes the event come again, that its processed-event marker
// (recorded in that transaction, its id built from the delivered id header as the core builds it)
// makes it a duplicate, and that once acknowledged it does not come again.
func RedeliveredAfterCommitIsSkipped(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	if kit.Markers == nil {
		tb.Fatal("the check needs a processed-event store; pass the message store's processed-event store")
	}
	events := kit.instance(tb)
	spec := kit.spec()
	key := fresh("key")
	k := key
	handled := event(kit.Source, &k)
	publishAll(tb, events, []snodra.OutgoingEvent{handled})
	only := []string{handled.ID}
	marker := func(delivery snodra.Delivery) snodra.ProcessedMarker {
		return snodra.ProcessedMarker{
			ID:          "processor:" + spec.Name + ":" + idOf(delivery),
			Type:        eventType,
			ProcessedAt: time.Now(),
			Processor:   spec.Name,
		}
	}

	first := join(tb, events, spec)
	first.hold(only)
	first.until(tb, "the event delivered", func(r *eventReader) bool { return r.hasAll(only) })
	if recorded := recordMarker(tb, kit.Markers, marker(first.firstDelivery(tb, handled.ID))); !recorded {
		tb.Fatal("the first handling records its marker")
	}
	first.close(tb) // the crash: committed, never acknowledged

	restarted := kit.instance(tb)
	second := join(tb, restarted, spec)
	second.hold(only)
	second.until(tb, "the event again after the crash", func(r *eventReader) bool { return r.hasAll(only) })
	again := second.firstDelivery(tb, handled.ID)
	if recorded := recordMarker(tb, kit.Markers, marker(again)); recorded {
		tb.Fatal("the redelivered event records a duplicate marker")
	}
	second.ack(tb, again)
	second.close(tb)

	fence := event(kit.Source, &k)
	publishAll(tb, events, []snodra.OutgoingEvent{fence})
	after := kit.instance(tb)
	third := join(tb, after, spec)
	third.until(tb, "the next event of the key", func(r *eventReader) bool { return r.has(fence.ID) })
	if third.has(handled.ID) {
		tb.Fatal("the skipped event, acknowledged, does not come again")
	}
	third.close(tb)
}

// PausedSubscriptionReceivesNothingAndResumesInPlace checks that a paused subscription delivers
// nothing (paused before its first Next too) while its assignment goes on; resumed, it delivers
// from where it stopped, each event once, in order.
func PausedSubscriptionReceivesNothingAndResumesInPlace(tb testing.TB, kit *EventStoreKit) {
	tb.Helper()
	events := kit.instance(tb)
	key := fresh("key")
	k := key
	var before, during []snodra.OutgoingEvent
	for i := 0; i < 3; i++ {
		before = append(before, event(kit.Source, &k))
	}
	for i := 0; i < 5; i++ {
		during = append(during, event(kit.Source, &k))
	}
	beforeIDs := eventIDs(before)
	duringIDs := eventIDs(during)
	publishAll(tb, events, before)

	reader := join(tb, events, kit.spec())
	reader.pause()
	reader.pump(tb, quiet)
	if len(reader.seen) != 0 {
		tb.Fatalf("paused from the start: nothing delivered, got %d", len(reader.seen))
	}
	reader.resume()
	reader.until(tb, "the events before the pause", func(r *eventReader) bool { return r.hasAll(beforeIDs) })

	reader.pause()
	delivered := len(reader.seen)
	publishAll(tb, events, during)
	reader.pump(tb, quiet)
	if len(reader.seen) != delivered {
		tb.Fatalf("nothing delivered while paused: %d deliveries before, %d after",
			delivered, len(reader.seen))
	}
	reader.resume()
	reader.until(tb, "the events published while paused", func(r *eventReader) bool {
		return r.hasAll(duringIDs)
	})
	all := append(append([]string(nil), beforeIDs...), duringIDs...)
	if got := reader.idsInOrder(all); !slices.Equal(got, all) {
		tb.Fatalf("each once, in order: got %v, want %v", got, all)
	}
	reader.close(tb)
}

// reader is one subscription and what it gave: its assignment and its deliveries, in order.
// Every delivery is acknowledged as it comes but the held ones.
type eventReader struct {
	name         string
	subscription snodra.EventSubscription
	assigned     map[string]bool
	seen         []snodra.Delivery
	held         map[string]bool
	revoked      int
	changes      int
	first        string
	changing     bool
}

// join subscribes to spec as one instance.
func join(tb testing.TB, store snodra.EventStore, spec snodra.SubscriptionSpec) *eventReader {
	tb.Helper()
	return &eventReader{
		name:         spec.Name,
		subscription: subscribe(tb, store, spec),
		assigned:     make(map[string]bool),
		held:         make(map[string]bool),
	}
}

// hold keeps the events with these ids unacknowledged as they come.
func (r *eventReader) hold(ids []string) {
	for _, id := range ids {
		r.held[id] = true
	}
}

func (r *eventReader) pause()  { r.subscription.Pause() }
func (r *eventReader) resume() { r.subscription.Resume() }

func (r *eventReader) ack(tb testing.TB, delivery snodra.Delivery) {
	tb.Helper()
	if err := r.subscription.Ack(delivery.Segment, delivery.Position); err != nil {
		tb.Fatalf("%s: ack %s %s: %v", r.name, delivery.Segment, delivery.Position, err)
	}
}

func (r *eventReader) redeliver(tb testing.TB, delivery snodra.Delivery) {
	tb.Helper()
	if err := r.subscription.Redeliver(delivery.Segment, delivery.Position); err != nil {
		tb.Fatalf("%s: redeliver %s %s: %v", r.name, delivery.Segment, delivery.Position, err)
	}
}

// pump takes what the subscription gives for window; how many events it gave.
func (r *eventReader) pump(tb testing.TB, window time.Duration) int {
	tb.Helper()
	deadline := time.Now().Add(window)
	taken := 0
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return taken
		}
		ctx, cancel := context.WithTimeout(context.Background(), left)
		event, err := r.subscription.Next(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return taken
			}
			tb.Fatalf("%s: next: %v", r.name, err)
		}
		if event == nil {
			tb.Fatalf("%s: next returned a nil event and no error", r.name)
		}
		taken++
		r.take(tb, event)
	}
}

func (r *eventReader) take(tb testing.TB, event snodra.SubscriptionEvent) {
	tb.Helper()
	switch event := event.(type) {
	case snodra.Assigned:
		if r.first == "" {
			r.first = "Assigned"
		}
		r.changes++
		r.changing = false
		for _, segment := range event.Segments {
			checkSegment(tb, r.name, segment)
			r.assigned[segment] = true
		}
	case snodra.Revoked:
		if r.first == "" {
			r.first = "Revoked"
		}
		r.changes++
		r.revoked++
		r.changing = true
		for _, segment := range event.Segments {
			checkSegment(tb, r.name, segment)
			delete(r.assigned, segment)
		}
	case snodra.Delivery:
		if r.first == "" {
			r.first = "Delivery"
		}
		if !r.assigned[event.Segment] {
			tb.Fatalf("%s: a delivery of segment %s it does not hold (%v)",
				r.name, event.Segment, r.assignedList())
		}
		checkPosition(tb, r.name, event.Position)
		if !r.held[idOf(event)] {
			r.ack(tb, event)
		}
		r.seen = append(r.seen, event)
	default:
		tb.Fatalf("%s: unknown subscription event %T", r.name, event)
	}
}

// until reads until done, at most wait.
func (r *eventReader) until(tb testing.TB, what string, done func(*eventReader) bool) {
	tb.Helper()
	deadline := time.Now().Add(wait)
	for !done(r) {
		if time.Now().After(deadline) {
			tb.Fatalf("%s: %s within %v (assigned %v, %d deliveries)",
				r.name, what, wait, r.assignedList(), len(r.seen))
		}
		r.pump(tb, slice)
	}
}

// allUntil reads every reader until done, at most wait.
func allUntil(tb testing.TB, readers []*eventReader, what string, done func([]*eventReader) bool) {
	tb.Helper()
	deadline := time.Now().Add(wait)
	for !done(readers) {
		if time.Now().After(deadline) {
			tb.Fatalf("%s within %v (assigned %v, revocations %v)",
				what, wait, assignedLists(readers), revokedCounts(readers))
		}
		pumpAll(tb, readers, parallelSlice)
	}
}

// pumpAll takes what every reader gives for window, one after the other.
func pumpAll(tb testing.TB, readers []*eventReader, window time.Duration) int {
	tb.Helper()
	taken := 0
	for _, reader := range readers {
		taken += reader.pump(tb, window)
	}
	return taken
}

func (r *eventReader) has(id string) bool {
	for _, delivery := range r.seen {
		if idOf(delivery) == id {
			return true
		}
	}
	return false
}

func (r *eventReader) hasAll(ids []string) bool {
	for _, id := range ids {
		if !r.has(id) {
			return false
		}
	}
	return true
}

// mine is the deliveries of the events with these ids, in delivery order.
func (r *eventReader) mine(ids []string) []snodra.Delivery {
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}
	var deliveries []snodra.Delivery
	for _, delivery := range r.seen {
		if wanted[idOf(delivery)] {
			deliveries = append(deliveries, delivery)
		}
	}
	return deliveries
}

func (r *eventReader) idsInOrder(ids []string) []string {
	deliveries := r.mine(ids)
	got := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		got[i] = idOf(delivery)
	}
	return got
}

// firstDelivery is the first delivery of the event id.
func (r *eventReader) firstDelivery(tb testing.TB, id string) snodra.Delivery {
	tb.Helper()
	for _, delivery := range r.seen {
		if idOf(delivery) == id {
			return delivery
		}
	}
	tb.Fatalf("%s: %s delivered", r.name, id)
	return snodra.Delivery{}
}

func (r *eventReader) assignedList() []string {
	list := make([]string, 0, len(r.assigned))
	for segment := range r.assigned {
		list = append(list, segment)
	}
	sort.Slice(list, func(i, j int) bool {
		return snodra.SegmentOrder(list[i], list[j]) < 0
	})
	return list
}

// close leaves the subscription; it is idempotent.
func (r *eventReader) close(tb testing.TB) {
	tb.Helper()
	if r.subscription == nil {
		return
	}
	subscription := r.subscription
	r.subscription = nil
	bounded(tb, "close "+r.name, subscription.Close)
}

// settled says the segments are spread as the joining check requires.
func settled(readers []*eventReader, former []int, all map[string]bool) bool {
	held := 0
	unique := make(map[string]bool)
	for _, reader := range readers {
		for segment := range reader.assigned {
			held++
			unique[segment] = true
		}
	}
	if held != len(unique) || len(unique) != len(all) {
		return false
	}
	if len(readers) >= 2 {
		for _, index := range former {
			if len(readers[index].assigned) >= len(all) {
				return false
			}
		}
	}
	for _, reader := range readers {
		if reader.changing {
			return false
		}
	}
	return true
}

func revokedCount(readers []*eventReader) int {
	count := 0
	for _, reader := range readers {
		count += reader.revoked
	}
	return count
}

func revokedCounts(readers []*eventReader) []int {
	counts := make([]int, len(readers))
	for i, reader := range readers {
		counts[i] = reader.revoked
	}
	return counts
}

func assignedLists(readers []*eventReader) [][]string {
	lists := make([][]string, len(readers))
	for i, reader := range readers {
		lists[i] = reader.assignedList()
	}
	return lists
}

// event is an event of the kit to source: the seven headers, a fresh id, a JSON object payload.
func event(source string, key *string) snodra.OutgoingEvent {
	number := nextNumber()
	id := fmt.Sprintf("event-%d", number)
	headers := map[string]string{
		snodra.HeaderType:          eventType,
		snodra.HeaderID:            id,
		snodra.HeaderCorrelationID: fmt.Sprintf("correlation-%d", number),
		snodra.HeaderTimestamp:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		snodra.HeaderSource:        "conformance",
		snodra.HeaderUserID:        "conformance-user",
		snodra.HeaderTraceparent:   fmt.Sprintf("00-%032x-%016x-01", number, number),
	}
	return snodra.OutgoingEvent{
		Source:  source,
		ID:      id,
		Type:    eventType,
		Key:     key,
		Headers: headers,
		Payload: []byte(fmt.Sprintf(`{"id":"%s","amount":42}`, id)),
	}
}

func eventIDs(sent []snodra.OutgoingEvent) []string {
	ids := make([]string, len(sent))
	for i, event := range sent {
		ids[i] = event.ID
	}
	return ids
}

func idOf(delivery snodra.Delivery) string { return delivery.Headers[snodra.HeaderID] }

func sameKey(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameKeyValue(key *string, value string) bool {
	return key != nil && *key == value
}

func shownKey(key *string) any {
	if key == nil {
		return nil
	}
	return *key
}

func deliveryIDs(deliveries []snodra.Delivery) map[string]bool {
	ids := make(map[string]bool, len(deliveries))
	for _, delivery := range deliveries {
		ids[idOf(delivery)] = true
	}
	return ids
}

func disjoint(a, b map[string]bool) bool {
	for segment := range a {
		if b[segment] {
			return false
		}
	}
	return true
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Strings(keys)
	return keys
}

func checkSegment(tb testing.TB, name, segment string) {
	tb.Helper()
	if segment == "" || len(segment) > snodra.MaxSegmentBytes {
		tb.Fatalf("%s: a segment name of 1 to %d bytes: %q", name, snodra.MaxSegmentBytes, segment)
	}
}

func checkPosition(tb testing.TB, name, position string) {
	tb.Helper()
	if position == "" || len(position) > snodra.MaxPositionBytes {
		tb.Fatalf("%s: a position of 1 to %d bytes: %q", name, snodra.MaxPositionBytes, position)
	}
}
