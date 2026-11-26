package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
)

// The nine level-2 event-store checks on a real broker (with the management API, so every
// capability is declared).
func TestRabbitMQEventStorePassesTheKit(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.superStream(t, 4)
	broker.streamNamed(t, source+".dlt")
	store := broker.store(t)
	markers := snodra.NewMemoryMessageStore().Processed()
	snodratest.EventStore(t, source,
		func() snodra.EventStore { return store },
		markers)
}

// A key lands on the partition the stream clients' hash routing gives it (murmur3 x86_32, seed
// 104729, unsigned, modulo the partitions), so every publisher of one key writes to one
// partition whatever the library is; an unkeyed event routes by its id.
func TestKeysLandOnTheStreamClientsPartition(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.superStream(t, 6)
	store := broker.store(t)

	pinned := []struct {
		id        string
		key       *string
		partition string
	}{
		{"pinned-order-1", strPtr("order-1"), "0"},
		{"pinned-order-2", strPtr("order-2"), "2"},
		{"pinned-customer-123", strPtr("customer-123"), "5"},
		{"pinned-order-7", strPtr("order-7"), "3"},
		{"pinned-empty", strPtr(""), "4"},
		{"order-7", nil, "3"},
	}
	for _, pinned := range pinned {
		if err := store.Publish(t.Context(), event(source, pinned.id, pinned.key)); err != nil {
			t.Fatalf("publishing %s: %v", pinned.id, err)
		}
	}
	spec := snodra.SubscriptionSpec{Name: "orders-service.partitioner", Source: source}
	sub := subscribe(t, store, spec)
	defer closeSubscription(t, sub)
	deliveries := readDeliveries(t, sub, len(pinned), 30*time.Second)
	got := make(map[string]string, len(deliveries))
	for _, delivery := range deliveries {
		got[delivery.Headers[snodra.HeaderID]] = delivery.Segment
	}
	for _, pinned := range pinned {
		if got[pinned.id] != pinned.partition {
			t.Fatalf("%s: got partition %q, want %q (all: %v)", pinned.id, got[pinned.id], pinned.partition, got)
		}
	}
}

// On the broker, the message carries the AMQP layout every Snodra library writes: the official
// client's decoder reads the string message-id, the application/json symbol, the x-routing-key
// annotation, the application properties and exactly one data section.
func TestTheMessageLayoutOnTheBroker(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.stream(t)
	store := broker.store(t)
	key := "order-42"
	sent := event(source, "layout-1", &key)
	sent.Headers["x-extra"] = "kept"
	if err := store.Publish(t.Context(), sent); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	messages := make(chan *amqp.Message, 1)
	consumer, err := broker.env.NewConsumer(source, func(_ stream.ConsumerContext, message *amqp.Message) {
		select {
		case messages <- message:
		default:
		}
	}, stream.NewConsumerOptions().
		SetOffset(stream.OffsetSpecification{}.First()).
		SetCRCCheck(false))
	if err != nil {
		t.Fatalf("the official client's consumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()
	select {
	case message := <-messages:
		if message.Properties == nil {
			t.Fatal("properties")
		}
		if id, ok := message.Properties.MessageID.(string); !ok || id != sent.ID {
			t.Fatalf("message-id: %#v, want the string %q", message.Properties.MessageID, sent.ID)
		}
		if message.Properties.ContentType != "application/json" {
			t.Fatalf("content-type: %q", message.Properties.ContentType)
		}
		if got, _ := message.Annotations[keyAnnotation].(string); got != key {
			t.Fatalf("the x-routing-key annotation: %#v, want %q", message.Annotations[keyAnnotation], key)
		}
		for name, want := range sent.Headers {
			got, _ := message.ApplicationProperties[name].(string)
			if got != want {
				t.Fatalf("application property %s: %q, want %q", name, got, want)
			}
		}
		if len(message.Data) != 1 || string(message.Data[0]) != string(sent.Payload) {
			t.Fatalf("one data section with the payload: %d sections", len(message.Data))
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the official client did not deliver the event within 30 s")
	}
}

// The subscription is a single active consumer named <service>.<processor> on every partition,
// with super-stream and a distinct snodra-member per instance; the management API lists them,
// and a reset from another store sees them as members.
func TestSingleActiveConsumerAndSnodraMember(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.superStream(t, 3)
	first := broker.store(t)
	second := broker.store(t)
	spec := snodra.SubscriptionSpec{Name: "orders-service.projector", Source: source}
	firstSub := subscribe(t, first, spec)
	defer closeSubscription(t, firstSub)
	secondSub := subscribe(t, second, spec)
	defer closeSubscription(t, secondSub)

	members := broker.membersOf(t, spec.Name, 2)
	if len(members) != 2 {
		t.Fatalf("two distinct snodra-member values: %v", members)
	}
	for _, consumer := range broker.consumers(t) {
		properties, _ := consumer["properties"].(map[string]any)
		if properties["name"] != spec.Name {
			continue
		}
		if properties["single-active-consumer"] != "true" {
			t.Fatalf("single-active-consumer: %#v", properties["single-active-consumer"])
		}
		if properties["super-stream"] != source {
			t.Fatalf("super-stream: %#v, want %q", properties["super-stream"], source)
		}
		if properties[memberProperty] == "" {
			t.Fatalf("%s is missing: %#v", memberProperty, properties)
		}
	}
	admin, err := broker.store(t).Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		listed, err := admin.Members(t.Context())
		if err != nil {
			t.Fatalf("members: %v", err)
		}
		if len(listed) >= 2 {
			sort.Strings(listed)
			got := append([]string(nil), members...)
			sort.Strings(got)
			if fmt.Sprint(listed) != fmt.Sprint(got) {
				t.Fatalf("the admin's members: %v, want the management API's %v", listed, got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the admin sees two members within 30 s, got %v", listed)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// The stored offset is the next offset to read: acknowledging offsets 0 and 1 stores 2, and a
// new instance of the name starts at 2 (the third event again).
func TestStoredOffsetIsTheNextOne(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.stream(t)
	store := broker.store(t)
	name := "orders-service.next-offset"
	key := "order-1"
	var sent []snodra.OutgoingEvent
	for i := 0; i < 3; i++ {
		sent = append(sent, event(source, fmt.Sprintf("next-%d", i), &key))
		if err := store.Publish(t.Context(), sent[i]); err != nil {
			t.Fatalf("publishing %s: %v", sent[i].ID, err)
		}
	}
	spec := snodra.SubscriptionSpec{Name: name, Source: source}
	first := subscribe(t, store, spec)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var accepted []snodra.Delivery
	for len(accepted) < 2 {
		event, err := first.Next(ctx)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		delivery, ok := event.(snodra.Delivery)
		if !ok {
			continue
		}
		if err := first.Ack(delivery.Segment, delivery.Position); err != nil {
			t.Fatalf("ack: %v", err)
		}
		accepted = append(accepted, delivery)
	}
	closeSubscription(t, first)
	stored, found := storedOffset(t, store, name, source)
	if !found || stored != 2 {
		t.Fatalf("the stored offset: got %d (found %t), want the next offset 2", stored, found)
	}

	second := subscribe(t, store, spec)
	defer closeSubscription(t, second)
	deliveries := readDeliveries(t, second, 1, 30*time.Second)
	if got := deliveries[0].Headers[snodra.HeaderID]; got != sent[2].ID {
		t.Fatalf("a new instance resumes at the stored offset: got %s, want %s", got, sent[2].ID)
	}
	statuses := second.Segments()
	if len(statuses) != 1 {
		t.Fatalf("one segment: %v", statuses)
	}
	if statuses[0].Committed != "3" {
		t.Fatalf("committed after the ack: %q, want 3", statuses[0].Committed)
	}
	if statuses[0].Head != "3" {
		t.Fatalf("head: %q, want 3", statuses[0].Head)
	}
	if statuses[0].Lag != 0 {
		t.Fatalf("lag: %d, want 0", statuses[0].Lag)
	}
}

// `replay` is declared only with the management API: without it Admin is unsupported; with it a
// reset to earliest moves the stored offset and the acknowledged history comes again.
func TestReplayOnlyWithTheManagementAPI(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.stream(t)
	without := broker.storeWithoutManagement(t)
	for _, capability := range without.Capabilities() {
		if capability == snodra.CapabilityReplay {
			t.Fatal("a store without the management API declares replay")
		}
	}
	if _, err := without.Admin(snodra.SubscriptionSpec{Name: "orders-service.replay", Source: source}); !errors.Is(err, snodra.ErrUnsupported) {
		t.Fatalf("Admin without replay: %v, want ErrUnsupported", err)
	}

	store := broker.store(t)
	if !snodra.Supports(store, snodra.CapabilityReplay) {
		t.Fatal("a store with the management API declares replay")
	}
	spec := snodra.SubscriptionSpec{Name: "orders-service.replay", Source: source}
	key := "order-1"
	var sent []snodra.OutgoingEvent
	for i := 0; i < 2; i++ {
		sent = append(sent, event(source, fmt.Sprintf("replay-%d", i), &key))
		if err := store.Publish(t.Context(), sent[i]); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}
	sub := subscribe(t, store, spec)
	admin, err := store.Admin(spec)
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	// While this process reads the subscription, a reset is refused even before the
	// management API has collected the consumer.
	if _, err := admin.Plan(t.Context(), snodra.ResetEarliest{}); err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := admin.Apply(t.Context(), []snodra.ReplayRange{{Source: source, Segment: "0", From: "0"}}); !errors.Is(err, snodra.ErrSubscribersActive) {
		t.Fatalf("apply while a subscription reads: %v, want ErrSubscribersActive", err)
	}
	readDeliveries(t, sub, 2, 30*time.Second)
	closeSubscription(t, sub)

	// The management API lists a consumer until its next collection (about 5 s): wait until it
	// shows none before the reset.
	deadline := time.Now().Add(30 * time.Second)
	for {
		members, err := admin.Members(t.Context())
		if err != nil {
			t.Fatalf("members: %v", err)
		}
		if len(members) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the closed subscription still lists members: %v", members)
		}
		time.Sleep(250 * time.Millisecond)
	}
	ranges, err := admin.Plan(t.Context(), snodra.ResetEarliest{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(ranges) != 1 || ranges[0].From != "0" || ranges[0].Until != "2" {
		t.Fatalf("the plan to earliest: %+v", ranges)
	}
	if err := admin.Apply(t.Context(), ranges); err != nil {
		t.Fatalf("apply: %v", err)
	}
	stored, found := storedOffset(t, store, spec.Name, source)
	if !found || stored != 0 {
		t.Fatalf("the stored offset after the reset: got %d (found %t), want 0", stored, found)
	}
	again := subscribe(t, store, spec)
	defer closeSubscription(t, again)
	redelivered := readDeliveries(t, again, 2, 30*time.Second)
	ids := []string{redelivered[0].Headers[snodra.HeaderID], redelivered[1].Headers[snodra.HeaderID]}
	if ids[0] != sent[0].ID || ids[1] != sent[1].ID {
		t.Fatalf("the reset replays the acknowledged history: got %v, want %v", ids, []string{sent[0].ID, sent[1].ID})
	}
}

// Every test creates its own streams (one run's suffix and a counter) and deletes what it
// created; a deleted stream is gone from the broker's metadata.
func TestEachTestGetsItsOwnStreamsAndCleansUp(t *testing.T) {
	broker := rabbitBroker(t)
	first := broker.superStream(t, 2)
	second := broker.superStream(t, 2)
	if first == second {
		t.Fatalf("two tests got the same stream %q", first)
	}
	name := uniqueName("cleanup")
	if err := broker.env.DeclareStream(name, stream.NewStreamOptions()); err != nil {
		t.Fatalf("creating %s: %v", name, err)
	}
	store := broker.store(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seed, err := store.dialAny(ctx, storeConnection)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seed.Close() }()
	metadata, err := seed.Metadata(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Streams[name].ResponseCode != 0x01 {
		t.Fatalf("the stream exists before cleanup: %+v", metadata.Streams[name])
	}
	if err := broker.env.DeleteStream(name); err != nil {
		t.Fatalf("deleting %s: %v", name, err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		metadata, err = seed.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if metadata.Streams[name].ResponseCode == 0x02 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stream %s survives its cleanup: %+v", name, metadata.Streams[name])
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The native dead-letter source is the plain stream <source>.dlt; a letter keeps its key,
// payload and headers (the six snodra-dlt-* ones included) and comes back as published.
func TestTheNativeDeadLetterStreamTakesALetter(t *testing.T) {
	broker := rabbitBroker(t)
	source := broker.superStream(t, 2)
	dlt := broker.streamNamed(t, snodra.DeadLetterSource(source))
	store := broker.store(t)
	key := "order-1"
	letter := event(dlt, "dead-letter-1", &key)
	letter.Headers["snodra-dlt-processor"] = "orders-service.projector"
	letter.Headers["snodra-dlt-reason"] = "HANDLER_FAILED"
	if err := store.Publish(t.Context(), letter); err != nil {
		t.Fatalf("publishing the dead letter: %v", err)
	}
	sub := subscribe(t, store, snodra.SubscriptionSpec{Name: "orders-service.dlt", Source: dlt})
	defer closeSubscription(t, sub)
	deliveries := readDeliveries(t, sub, 1, 30*time.Second)
	if got := deliveries[0]; got.Segment != "0" {
		t.Fatalf("a plain stream is one segment, got %q", got.Segment)
	}
	for name, want := range letter.Headers {
		if deliveries[0].Headers[name] != want {
			t.Fatalf("dead letter header %s: got %q, want %q", name, deliveries[0].Headers[name], want)
		}
	}
	if string(deliveries[0].Payload) != string(letter.Payload) {
		t.Fatalf("the dead letter payload: %q", deliveries[0].Payload)
	}
}

func strPtr(value string) *string { return &value }

func closeSubscription(t *testing.T, sub snodra.EventSubscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := sub.Close(ctx); err != nil {
		t.Fatalf("closing the subscription: %v", err)
	}
}
