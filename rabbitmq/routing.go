package rabbitmq

import (
	"strconv"
	"strings"
)

// hashSeed is the seed of the stream clients' hash routing: murmur3 x86_32 with seed 104729.
const hashSeed uint32 = 104729

// murmur3 is the murmur3 x86_32 hash the stream clients route with (the Java, .NET and Go
// clients, and the Rust add-on's pinned routing). Its result is used unsigned.
func murmur3(data []byte) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
	)
	h := hashSeed
	n := len(data) &^ 3
	for i := 0; i < n; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= c1
		k = k<<15 | k>>(32-15)
		k *= c2
		h ^= k
		h = h<<13 | h>>(32-13)
		h = h*5 + 0xe6546b64
	}
	var k uint32
	switch len(data) & 3 {
	case 3:
		k ^= uint32(data[n+2]) << 16
		fallthrough
	case 2:
		k ^= uint32(data[n+1]) << 8
		fallthrough
	case 1:
		k ^= uint32(data[n])
		k *= c1
		k = k<<15 | k>>(32-15)
		k *= c2
		h ^= k
	}
	h ^= uint32(len(data))
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// partitionIndex is the partition of a routing key among partitions, in the order the broker
// lists them: the hash as an unsigned number modulo the count. What every stream client's hash
// routing computes, so one key lands on one partition whatever the publishing library is.
func partitionIndex(routingKey string, partitions int) int {
	if partitions < 1 {
		return 0
	}
	return int(murmur3([]byte(routingKey)) % uint32(partitions))
}

// routingKey is the event's key, or its id when it has none: an unkeyed event spreads over the
// partitions by its id.
func routingKey(key *string, id string) string {
	if key != nil {
		return *key
	}
	return id
}

// segmentOf is the segment of partition stream stream of source: the suffix after
// "<source>-" ("snodra.events-3" is "3"), else the stream's whole name.
func segmentOf(source, stream string) string {
	rest, ok := strings.CutPrefix(stream, source+"-")
	if ok && rest != "" {
		return rest
	}
	return stream
}

// singleSegment is the segment of a source that is one plain stream (a dead-letter stream).
const singleSegment = "0"

// comparePositions orders two offsets: decimal offsets by value; any text that is not an offset
// comes after every offset, by its bytes among themselves. Compare has no error to report for a
// position that is not an offset.
func comparePositions(a, b string) int {
	x, xErr := strconv.ParseInt(a, 10, 64)
	y, yErr := strconv.ParseInt(b, 10, 64)
	switch {
	case xErr == nil && yErr == nil:
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		default:
			return 0
		}
	case xErr == nil:
		return -1
	case yErr == nil:
		return 1
	default:
		return strings.Compare(a, b)
	}
}
