package rabbitmq

import (
	"testing"
)

// The stream clients' hash routing is murmur3 x86_32 of the routing key with seed 104729, taken
// unsigned, modulo the partitions. A signed modulo gives another partition for order-7.
func TestKeysHashToTheStreamClientsPartition(t *testing.T) {
	for _, pinned := range []struct {
		key       string
		hash      uint32
		partition int
	}{
		{"order-1", 0x3163ba7a, 0},
		{"order-2", 0x4aa7bf62, 2},
		{"customer-123", 0x6fabe837, 5},
		{"order-7", 0x813d1adb, 3},
		{"", 0xc6757d56, 4},
	} {
		if got := murmur3([]byte(pinned.key)); got != pinned.hash {
			t.Fatalf("the hash of %q: got 0x%08x, want 0x%08x", pinned.key, got, pinned.hash)
		}
		if got := partitionIndex(pinned.key, 6); got != pinned.partition {
			t.Fatalf("the partition of %q among 6: got %d, want %d", pinned.key, got, pinned.partition)
		}
	}
	var signedHash uint32 = 0x813d1adb
	if got := int(int32(signedHash) % 6); got == 3 {
		t.Fatalf("the signed modulo no longer differs from the pinned partition (%d)", got)
	} else if want := partitionIndex("order-7", 6); want != 3 {
		t.Fatalf("order-7: got %d, want 3", want)
	}
}

func TestAnUnkeyedEventRoutesByItsID(t *testing.T) {
	key := "order-42"
	if got := routingKey(&key, "e-1"); got != "order-42" {
		t.Fatalf("a keyed event routes by its key, got %q", got)
	}
	if got := routingKey(nil, "e-1"); got != "e-1" {
		t.Fatalf("an unkeyed event routes by its id, got %q", got)
	}
}

func TestSegmentsArePartitionSuffixes(t *testing.T) {
	for _, pinned := range []struct {
		source string
		stream string
		want   string
	}{
		{"snodra.events", "snodra.events-0", "0"},
		{"snodra.events", "snodra.events-11", "11"},
		{"snodra.events", "snodra.events-eu", "eu"},
		{"snodra.events", "other-3", "other-3"},
		{"snodra.events", "snodra.events-", "snodra.events-"},
	} {
		if got := segmentOf(pinned.source, pinned.stream); got != pinned.want {
			t.Fatalf("segmentOf(%q, %q): got %q, want %q", pinned.source, pinned.stream, got, pinned.want)
		}
	}
}

func TestPositionsAreOffsets(t *testing.T) {
	for _, pinned := range []struct {
		a, b string
		want int
	}{
		{"1", "2", -1},
		{"2", "10", -1},
		{"10", "2", 1},
		{"7", "7", 0},
		{"", "0", 1},
		{"x", "0", 1},
		{"x", "y", -1},
	} {
		got := comparePositions(pinned.a, pinned.b)
		if (got < 0) != (pinned.want < 0) || (got > 0) != (pinned.want > 0) {
			t.Fatalf("comparePositions(%q, %q): got %d, want sign %d", pinned.a, pinned.b, got, pinned.want)
		}
	}
}
