package kafka

import (
	"errors"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestTheCommittedOffsetIsTheNextOne(t *testing.T) {
	cases := []struct {
		position string
		want     int64
	}{
		{"0", 1},
		{"1", 2},
		{"41", 42},
		{"007", 8},
	}
	for _, c := range cases {
		got, err := committedOffset(c.position)
		if err != nil {
			t.Fatalf("position %q: %v", c.position, err)
		}
		if got != c.want {
			t.Fatalf("position %q commits %d, want %d", c.position, got, c.want)
		}
	}
}

func TestBadPositionsAreInvalid(t *testing.T) {
	for _, position := range []string{"", "-1", "1.5", "x", "9223372036854775808"} {
		if _, err := committedOffset(position); !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("position %q: %v, want ErrInvalid", position, err)
		}
	}
}

func TestSegmentsArePartitionNumbers(t *testing.T) {
	cases := []struct {
		segment string
		want    int32
	}{
		{"0", 0},
		{"3", 3},
		{"007", 7},
	}
	for _, c := range cases {
		got, err := parseSegment(c.segment)
		if err != nil {
			t.Fatalf("segment %q: %v", c.segment, err)
		}
		if got != c.want {
			t.Fatalf("segment %q is %d, want %d", c.segment, got, c.want)
		}
	}
	for _, segment := range []string{"", "-1", "1.5", "x", "2147483648"} {
		if _, err := parseSegment(segment); !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("segment %q: %v, want ErrInvalid", segment, err)
		}
	}
}

func TestCompareOrdersOffsetsByValueAndTextAfterThem(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"9", "10", -1},
		{"10", "9", 1},
		{"7", "7", 0},
		{"07", "7", 0},
		{"7", "x", -1},
		{"x", "7", 1},
		{"a", "b", -1},
	}
	for _, c := range cases {
		got := comparePositions(c.a, c.b)
		if sign(got) != c.want {
			t.Fatalf("compare(%q, %q) = %d, want sign %d", c.a, c.b, got, c.want)
		}
	}
}

func sign(value int) int {
	switch {
	case value < 0:
		return -1
	case value > 0:
		return 1
	default:
		return 0
	}
}

func TestTheStoreIsNamedAndDeclaresReplayLagAndDeadLetters(t *testing.T) {
	store, err := New(Config{BootstrapServers: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = store.Close() }()
	if got := store.Name(); got != "kafka" {
		t.Fatalf("name %q", got)
	}
	want := []snodra.Capability{
		snodra.CapabilityReplay,
		snodra.CapabilityLag,
		snodra.CapabilityNativeDeadLetter,
	}
	got := store.Capabilities()
	if len(got) != len(want) {
		t.Fatalf("capabilities %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("capabilities %v, want %v", got, want)
		}
	}
	if snodra.Supports(store, snodra.CapabilityBrowse) {
		t.Fatal("the store declares no browse: the Go library has no event browser")
	}
}

func TestNewRefusesBadSettings(t *testing.T) {
	cases := []Config{
		{},
		{BootstrapServers: " "},
		{BootstrapServers: "h:1", MaxPollRecords: -1},
		{BootstrapServers: "h:1", StatusRefresh: -1},
		{BootstrapServers: "h:1", AdminTimeout: -1},
	}
	for _, config := range cases {
		store, err := New(config)
		if err == nil {
			_ = store.Close()
			t.Fatalf("config %+v was accepted", config)
		}
		if !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("config %+v: %v, want ErrInvalid", config, err)
		}
	}
}

func TestFromEnvReadsTheBootstrapServers(t *testing.T) {
	t.Setenv("SNODRA_KAFKA_BOOTSTRAP", "k1:9092,k2:9092")
	if got := FromEnv().BootstrapServers; got != "k1:9092,k2:9092" {
		t.Fatalf("bootstrap %q", got)
	}
	t.Setenv("SNODRA_KAFKA_BOOTSTRAP", "")
	if got := FromEnv().BootstrapServers; got != "kafka:19092" {
		t.Fatalf("default bootstrap %q", got)
	}
}

func TestARecordHeaderIsLookedUpByName(t *testing.T) {
	headers := map[string]string{
		"source":        "orders-service",
		"userId":        "u-1",
		"timestamp":     "2026-01-02T03:04:05.000Z",
		"correlationId": "c-1",
		"id":            "e-1",
		"type":          "orders.Placed",
		"traceparent":   "00-trace",
	}
	record := recordHeaders(headers)
	want := []string{
		"correlationId",
		"traceparent",
		"userId",
		"type",
		"id",
		"timestamp",
		"source",
	}
	if len(record) != len(want) {
		t.Fatalf("headers %v, want %v", record, want)
	}
	for i, header := range record {
		if header.Key != want[i] {
			t.Fatalf("header %d is %q, want %q", i, header.Key, want[i])
		}
		if string(header.Value) != headers[header.Key] {
			t.Fatalf("header %q is %q, want %q", header.Key, header.Value, headers[header.Key])
		}
	}
	if recordHeaders(nil) != nil {
		t.Fatal("no headers, no record headers")
	}
}

func TestAnEmptyKeyIsAKey(t *testing.T) {
	if eventKey(nil) != nil {
		t.Fatal("no key, no record key")
	}
	empty := ""
	key := eventKey(&empty)
	if key == nil || len(key) != 0 {
		t.Fatalf("an empty key is a key, got %v", key)
	}
	value := "order-1"
	if got := string(eventKey(&value)); got != "order-1" {
		t.Fatalf("key %q", got)
	}
}

func TestPublishFailuresAreRejectedOrFailed(t *testing.T) {
	rejected := []error{
		kerr.MessageTooLarge,
		kerr.InvalidRecord,
		kerr.InvalidTopicException,
		kerr.RecordListTooLarge,
		kerr.TopicAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.InvalidRequest,
		kerr.UnsupportedForMessageFormat,
		kerr.InvalidTimestamp,
		kerr.PolicyViolation,
	}
	for _, err := range rejected {
		if got := publishError(err); !errors.Is(got, snodra.ErrRejected) {
			t.Fatalf("%v: %v, want ErrRejected", err, got)
		}
	}
	failed := []error{
		kerr.RequestTimedOut,
		kerr.BrokerNotAvailable,
		kerr.UnknownTopicOrPartition,
		kerr.NotEnoughReplicas,
		errors.New("a transport failure"),
	}
	for _, err := range failed {
		if got := publishError(err); !errors.Is(got, snodra.ErrFailed) {
			t.Fatalf("%v: %v, want ErrFailed", err, got)
		}
	}
}

func TestAPublishWithoutASourceIsRejected(t *testing.T) {
	store, err := New(Config{BootstrapServers: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Publish(t.Context(), snodra.OutgoingEvent{Source: " "}); !errors.Is(err, snodra.ErrRejected) {
		t.Fatalf("publish without a source: %v, want ErrRejected", err)
	}
}

func TestSubscribeAndAdminRefuseAnEmptyNameOrSource(t *testing.T) {
	store, err := New(Config{BootstrapServers: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.Subscribe(t.Context(), snodra.SubscriptionSpec{}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("subscribe: %v, want ErrInvalid", err)
	}
	if _, err := store.Admin(snodra.SubscriptionSpec{Name: "orders-service.default"}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("admin: %v, want ErrInvalid", err)
	}
}

func TestTakeErrorReturnsTheLatestTroubleOnce(t *testing.T) {
	sub := &subscription{}
	if got := sub.TakeError(); got != "" {
		t.Fatalf("a subscription with no trouble: %q", got)
	}
	sub.setError("kafka: the first trouble")
	sub.setError("kafka: the latest trouble")
	if got, want := sub.TakeError(), "kafka: the latest trouble"; got != want {
		t.Fatalf("TakeError: %q, want %q", got, want)
	}
	if got := sub.TakeError(); got != "" {
		t.Fatalf("TakeError after a read: %q, want empty", got)
	}
}
