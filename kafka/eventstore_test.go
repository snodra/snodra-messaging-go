package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The level-2 and level-3 event-store checks on a real broker.
func TestKafkaEventStorePassesTheKit(t *testing.T) {
	broker := kafkaBroker(t)
	source := uniqueTopic("snodra.events")
	createTopic(t, broker, source, 4, nil)
	createTopic(t, broker, source+".dlt", 1, nil)
	store := newTestStore(t, broker)
	markers := snodra.NewMemoryMessageStore().Processed()
	snodratest.EventStore(t, source,
		func() snodra.EventStore { return store },
		markers)
}

// A key lands on the partition the Java client's default partitioner gives it (murmur2, masked
// positive, modulo the partitions), so every publisher of one key writes to one partition and its
// events keep their order there.
func TestAKeyLandsOnTheJavaClientsPartition(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("partitions")
	createTopic(t, broker, topic, 6, nil)
	store := newTestStore(t, broker)

	raw, err := os.ReadFile("testdata/partitions.json")
	if err != nil {
		t.Fatalf("the pinned partitions: %v", err)
	}
	var want map[string]int32
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("the pinned partitions: %v", err)
	}
	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		key := key
		if err := store.Publish(t.Context(), event(topic, fmt.Sprintf("partition-%d", i), &key)); err != nil {
			t.Fatalf("publishing key %q: %v", key, err)
		}
	}
	if err := store.Publish(t.Context(), event(topic, "partition-unkeyed", nil)); err != nil {
		t.Fatalf("publishing an unkeyed event: %v", err)
	}

	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "partitioner"), Source: topic}
	sub := subscribe(t, store, spec)
	deliveries := readDeliveries(t, sub, len(keys)+1, 30*time.Second)
	got := make(map[string]int32, len(keys))
	for _, delivery := range deliveries {
		if delivery.Key == nil {
			continue
		}
		partition, err := strconv.ParseInt(delivery.Segment, 10, 32)
		if err != nil {
			t.Fatalf("segment %q is not a partition: %v", delivery.Segment, err)
		}
		got[*delivery.Key] = int32(partition)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the Java client's partitions: got %v, want %v", got, want)
	}
}

// Every header travels as a record header and is read by its name, never by its place; the key
// (or none) and the payload arrive as published.
func TestHeadersTravelByNameAndTheKeyAndPayloadAsPublished(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("headers")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)

	key := "order-42"
	keyed := event(topic, "headers-keyed", &key)
	keyed.Headers["x-empty"] = ""
	keyed.Headers["x-utf8"] = "café"
	keyed.Headers["x-order"] = "the order is the store's; a reader looks a header up by its name"
	unkeyed := event(topic, "headers-unkeyed", nil)
	for _, outgoing := range []snodra.OutgoingEvent{keyed, unkeyed} {
		if err := store.Publish(t.Context(), outgoing); err != nil {
			t.Fatalf("publishing %s: %v", outgoing.ID, err)
		}
	}

	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "headers"), Source: topic}
	sub := subscribe(t, store, spec)
	deliveries := readDeliveries(t, sub, 2, 30*time.Second)
	byID := make(map[string]snodra.Delivery, len(deliveries))
	for _, delivery := range deliveries {
		byID[delivery.Headers[snodra.HeaderID]] = delivery
	}
	for _, sent := range []snodra.OutgoingEvent{keyed, unkeyed} {
		got, ok := byID[sent.ID]
		if !ok {
			t.Fatalf("%s delivered", sent.ID)
		}
		for name, value := range sent.Headers {
			if got.Headers[name] != value {
				t.Fatalf("%s: header %s: got %q, want %q", sent.ID, name, got.Headers[name], value)
			}
		}
		if (got.Key == nil) != (sent.Key == nil) {
			t.Fatalf("%s: key %v, want %v", sent.ID, got.Key, sent.Key)
		}
		if got.Key != nil && *got.Key != *sent.Key {
			t.Fatalf("%s: key %q, want %q", sent.ID, *got.Key, *sent.Key)
		}
		if !bytes.Equal(got.Payload, sent.Payload) {
			t.Fatalf("%s: payload %q, want %q", sent.ID, got.Payload, sent.Payload)
		}
		if got.Segment != "0" {
			t.Fatalf("%s: segment %q on a one-partition topic", sent.ID, got.Segment)
		}
		if _, err := strconv.ParseInt(got.Position, 10, 64); err != nil {
			t.Fatalf("%s: position %q is not an offset", sent.ID, got.Position)
		}
		if got.TimestampMs <= 0 {
			t.Fatalf("%s: the record's timestamp is %d", sent.ID, got.TimestampMs)
		}
	}
}

// A consumer with no compression codec reads what the store publishes: the producer's default is
// no compression, the codec every consumer decodes without an optional library.
func TestAPublishedRecordNeedsNoCodecToRead(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("no-codec")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)

	key := "order-42"
	outgoing := event(topic, "no-codec-1", &key)
	// Repeated text compresses well: the client's default snappy preference would compress this
	// batch, and a consumer without the codec would fail to read it.
	outgoing.Payload = []byte(`{"padding":"` + strings.Repeat("compressible ", 1024) + `"}`)
	if err := store.Publish(t.Context(), outgoing); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker.bootstrap),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.WithDecompressor(noCodecDecompressor{}),
	)
	if err != nil {
		t.Fatalf("the raw consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(ctx)
	if errors := fetches.Errors(); len(errors) > 0 {
		t.Fatalf("reading without a codec: %v", errors)
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("%d records delivered, want 1", len(records))
	}
	record := records[0]
	if got := record.Attrs.CompressionType(); got != uint8(kgo.CodecNone) {
		t.Fatalf("the record's compression is %d, want %d (none)", got, kgo.CodecNone)
	}
	if !bytes.Equal(record.Value, outgoing.Payload) {
		t.Fatalf("the payload %q, want %q", record.Value, outgoing.Payload)
	}
	if record.Key == nil || string(record.Key) != key {
		t.Fatalf("the key %q, want %q", record.Key, key)
	}
}

// noCodecDecompressor is a consumer with no optional codec installed: it reads an uncompressed
// batch and refuses every compressed one.
type noCodecDecompressor struct{}

func (noCodecDecompressor) Decompress(src []byte, codecType kgo.CompressionCodecType) ([]byte, error) {
	if codecType != kgo.CodecNone {
		return nil, fmt.Errorf("the consumer has no compression codec for %d", codecType)
	}
	return src, nil
}

// Subscribing before the source's topic exists is not an error: the subscription waits for the
// topic, next reports only the caller's context expiring, and once the topic appears the group
// joins and the subscription reads what is published there.
func TestSubscribingBeforeTheTopicExistsWaitsForIt(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("late-topic")
	store := newTestStore(t, broker)
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "late-topic"), Source: topic}
	sub := subscribe(t, store, spec)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	subscriptionEvent, err := sub.Next(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("next before the topic exists: %v (event %T), want context.DeadlineExceeded", err, subscriptionEvent)
	}

	createTopic(t, broker, topic, 1, nil)
	// Wake the client's metadata refresh rather than waiting out its ordinary metadata age:
	// the group can only join once the topic's partitions are known.
	sub.client.ForceMetadataRefresh()
	waitForAssignment(t, sub, 15*time.Second)

	key := "order-42"
	if err := store.Publish(t.Context(), event(topic, "late-topic-1", &key)); err != nil {
		t.Fatalf("publishing to the new topic: %v", err)
	}
	delivered := readDeliveries(t, sub, 1, 30*time.Second)[0]
	if delivered.Headers[snodra.HeaderID] != "late-topic-1" {
		t.Fatalf("the event after the topic appeared: %s", delivered.Headers[snodra.HeaderID])
	}
	// Close before the topic cleanup deletes it, so the subscription does not see it vanish.
	if err := sub.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A subscription's name is its consumer group, `<service>.<processor>`, on the classic protocol
// with the range assignor: the instances of every official library meet in one group.
func TestASubscriptionJoinsTheGroupNamedAfterTheProcessor(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("group")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "audit"), Source: topic}
	sub := subscribe(t, store, spec)
	waitForAssignment(t, sub, 10*time.Second)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	groups, err := broker.admin.DescribeGroups(ctx, spec.Name)
	if err != nil {
		t.Fatalf("describing group %s: %v", spec.Name, err)
	}
	group, ok := groups[spec.Name]
	if !ok || group.Err != nil {
		t.Fatalf("the group %s: %+v", spec.Name, group)
	}
	if group.ProtocolType != "consumer" {
		t.Fatalf("the group protocol type is %q, want consumer", group.ProtocolType)
	}
	if group.State != "Stable" {
		t.Fatalf("the group state is %q, want Stable", group.State)
	}
	if group.Protocol != "range" {
		t.Fatalf("the assignor is %q, want range", group.Protocol)
	}
	if len(group.Members) != 1 {
		t.Fatalf("the group has %d members, want 1", len(group.Members))
	}
	if group.Members[0].ClientID == "" {
		t.Fatal("the member has no client id")
	}

	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	members, err := admin.Members(ctx)
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members) != 1 || members[0] == "" {
		t.Fatalf("members %v, want one non-empty id", members)
	}

	if err := sub.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, snodra.ErrClosed) {
		t.Fatalf("next after close: %v, want ErrClosed", err)
	}
	waitNoMembers(t, admin, 10*time.Second)
}

// Acknowledging the record at offset N commits N+1, and a new subscription of the group starts
// there; an acknowledgement that was not made is not skipped.
func TestTheGroupCommitsTheNextOffset(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("offsets")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)
	key := "order-42"
	var sent []string
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("offset-%d", i)
		if err := store.Publish(t.Context(), event(topic, id, &key)); err != nil {
			t.Fatalf("publishing %s: %v", id, err)
		}
		sent = append(sent, id)
	}
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "offsets"), Source: topic}

	first := subscribe(t, store, spec)
	waitForAssignment(t, first, 10*time.Second)
	handled := readDeliveries(t, first, 1, 30*time.Second)
	if handled[0].Position != "0" {
		t.Fatalf("the first position is %q, want 0", handled[0].Position)
	}
	waitCommitted(t, broker, spec.Name, topic, 0, 1)
	waitSegment(t, first, 10*time.Second, func(status snodra.SegmentStatus) bool {
		return status.Segment == "0" && status.Position == "1" && status.Committed == "1" &&
			status.Head == "3" && status.Lag == 2
	})
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := subscribe(t, store, spec)
	rest := readDeliveries(t, second, 2, 30*time.Second)
	if len(rest) != 2 || rest[0].Position != "1" || rest[1].Position != "2" {
		t.Fatalf("a new subscription resumes at the committed offset: %v", positions(rest))
	}
	if got := ids(rest); !reflect.DeepEqual(got, sent[1:]) {
		t.Fatalf("the unacknowledged records came again: got %v, want %v", got, sent[1:])
	}
	waitCommitted(t, broker, spec.Name, topic, 0, 3)
}

// A record the broker refuses for good is ErrRejected; the outbox relay gives it up instead of
// retrying it forever.
func TestARecordTheBrokerRefusesIsGivenUp(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("small-records")
	createTopic(t, broker, topic, 1, map[string]*string{"max.message.bytes": stringPointer("1024")})
	store := newTestStore(t, broker)
	key := "order-42"
	outgoing := event(topic, "too-large", &key)
	// max.message.bytes bounds the batch the producer sends, and the producer sends it
	// uncompressed: the payload stays over the topic's 1 KiB.
	padding := make([]byte, 8192)
	if _, err := rand.Read(padding); err != nil {
		t.Fatalf("random padding: %v", err)
	}
	outgoing.Payload = []byte(`{"padding":"` + hex.EncodeToString(padding) + `"}`)
	for attempt := 0; attempt < 2; attempt++ {
		err := store.Publish(t.Context(), outgoing)
		if !errors.Is(err, snodra.ErrRejected) {
			t.Fatalf("attempt %d: %v, want ErrRejected", attempt+1, err)
		}
	}
}

// The dead-letter source is `<source>.dlt`: a letter with its original key, payload and headers
// and the six snodra-dlt-* headers is published and read there.
func TestADeadLetterGoesToTheDeadLetterTopic(t *testing.T) {
	broker := kafkaBroker(t)
	source := uniqueTopic("snodra.events")
	deadLetterTopic := snodra.DeadLetterSource(source)
	if deadLetterTopic != source+".dlt" {
		t.Fatalf("the dead-letter source is %q", deadLetterTopic)
	}
	createTopic(t, broker, source, 1, nil)
	createTopic(t, broker, deadLetterTopic, 1, nil)
	store := newTestStore(t, broker)
	if !snodra.Supports(store, snodra.CapabilityNativeDeadLetter) {
		t.Fatal("the store declares native-dead-letter")
	}
	key := "order-42"
	letter := event(deadLetterTopic, "dead-letter-1", &key)
	letter.Headers["snodra-dlt-processor"] = "default"
	letter.Headers["snodra-dlt-reason"] = "HANDLER_FAILED"
	letter.Headers["snodra-dlt-error"] = "the projection refused the order"
	letter.Headers["snodra-dlt-original-topic"] = source
	letter.Headers["snodra-dlt-original-partition"] = "3"
	letter.Headers["snodra-dlt-original-offset"] = "42"
	if err := store.Publish(t.Context(), letter); err != nil {
		t.Fatalf("publishing the letter: %v", err)
	}

	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "dead-letters"), Source: deadLetterTopic}
	sub := subscribe(t, store, spec)
	delivered := readDeliveries(t, sub, 1, 30*time.Second)[0]
	if delivered.Key == nil || *delivered.Key != key {
		t.Fatalf("the letter's key: %v", delivered.Key)
	}
	if !bytes.Equal(delivered.Payload, letter.Payload) {
		t.Fatalf("the letter's payload: %q", delivered.Payload)
	}
	for _, name := range []string{
		"snodra-dlt-processor",
		"snodra-dlt-reason",
		"snodra-dlt-error",
		"snodra-dlt-original-topic",
		"snodra-dlt-original-partition",
		"snodra-dlt-original-offset",
	} {
		if delivered.Headers[name] != letter.Headers[name] {
			t.Fatalf("the letter's header %s: got %q, want %q",
				name, delivered.Headers[name], letter.Headers[name])
		}
	}
}

// An instance abandoned without its Close leaves its unacknowledged records uncommitted; the
// next instance of the group reads them again, and not the acknowledged ones.
func TestUnackedRecordsComeAgainAfterAnAbandonedInstance(t *testing.T) {
	broker := kafkaBroker(t)
	topic := uniqueTopic("abandoned")
	createTopic(t, broker, topic, 1, nil)
	store := newTestStore(t, broker)
	key := "order-42"
	sentIDs := []string{"abandoned-0", "abandoned-1", "abandoned-2"}
	for _, id := range sentIDs {
		if err := store.Publish(t.Context(), event(topic, id, &key)); err != nil {
			t.Fatalf("publishing %s: %v", id, err)
		}
	}
	spec := snodra.SubscriptionSpec{Name: uniqueGroup("orders-service", "abandoned"), Source: topic}

	first := subscribe(t, store, spec)
	waitForAssignment(t, first, 10*time.Second)
	handled := readDeliveries(t, first, 1, 30*time.Second)[0]
	if handled.Headers[snodra.HeaderID] != sentIDs[0] {
		t.Fatalf("the first record is %s, want %s", handled.Headers[snodra.HeaderID], sentIDs[0])
	}
	hold := map[string]bool{sentIDs[1]: true, sentIDs[2]: true}
	unacked := readDeliveriesHolding(t, first, 2, 30*time.Second, hold)
	if got := ids(unacked); !reflect.DeepEqual(got, sentIDs[1:]) {
		t.Fatalf("the two unacknowledged records: got %v, want %v", got, sentIDs[1:])
	}
	waitCommitted(t, broker, spec.Name, topic, 0, 1)

	// The instance is abandoned without its Close. Its client leaves the group gracefully, so
	// the test does not wait for a session timeout; nothing is flushed, and the group's
	// committed offset is still 1.
	abandonSubscription(first)

	second := subscribe(t, store, spec)
	again := readDeliveries(t, second, 2, 30*time.Second)
	if got := ids(again); !reflect.DeepEqual(got, sentIDs[1:]) {
		t.Fatalf("after the abandoned instance: got %v, want %v", got, sentIDs[1:])
	}
	if again[0].Position != "1" || again[1].Position != "2" {
		t.Fatalf("after the abandoned instance: positions %v, want [1 2]", positions(again))
	}
}

// abandonSubscription drops a subscription without its Close: its client leaves the group
// gracefully and nothing is flushed, so only the offsets acknowledged before it remain committed.
func abandonSubscription(sub *subscription) {
	sub.mu.Lock()
	sub.closed = true
	sub.signal()
	sub.mu.Unlock()
	if sub.stop != nil {
		sub.stop()
	}
	sub.client.Close()
}

// waitCommitted waits until the group's committed offset of a partition is want.
func waitCommitted(t *testing.T, broker *testBroker, group, topic string, partition int32, want int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		responses, err := broker.admin.FetchOffsetsForTopics(ctx, group, topic)
		cancel()
		if err == nil {
			if response, ok := responses.Lookup(topic, partition); ok && response.At == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %s: the committed offset of %s/%d did not become %d (last error %v)",
				group, topic, partition, want, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// waitSegment waits until a segment's status matches, as the subscription's refresher updates it.
func waitSegment(t *testing.T, sub *subscription, within time.Duration, matches func(snodra.SegmentStatus) bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		for _, status := range sub.Segments() {
			if matches(status) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the segment status did not match within %v: %+v", within, sub.Segments())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitNoMembers waits until the group has no members.
func waitNoMembers(t *testing.T, admin snodra.SubscriptionAdmin, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		members, err := admin.Members(ctx)
		cancel()
		if err == nil && len(members) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the group still has members %v within %v", members, within)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func ids(deliveries []snodra.Delivery) []string {
	result := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		result[i] = delivery.Headers[snodra.HeaderID]
	}
	return result
}

func positions(deliveries []snodra.Delivery) []string {
	result := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		result[i] = delivery.Position
	}
	return result
}
