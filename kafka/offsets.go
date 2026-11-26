package kafka

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	snodra "github.com/snodra/snodra-messaging-go"
)

// Kafka's offsets are its positions: a record's offset is the position the subscription reports,
// and the offset committed for a group is the next one to read. Acknowledging the record at
// position N therefore stores N+1, and a new subscription of the group starts at the stored
// value. Nothing else in the add-on stores "the last one done".
//
// Segment and position are decimal strings, as they are on the wire and in every library: a
// partition number and an offset. The parsing below accepts the forms Kafka clients accept (a
// leading sign and leading zeros, as the Java client's parseInt/Long.parseLong do), and refuses
// anything else as snodra.ErrInvalid.

// parseSegment reads a partition number: a decimal int32, not negative.
func parseSegment(segment string) (int32, error) {
	partition, err := strconv.ParseInt(segment, 10, 32)
	if err != nil || partition < 0 {
		return 0, fmt.Errorf("%w: segment %q is not a partition", snodra.ErrInvalid, segment)
	}
	return int32(partition), nil
}

// parsePosition reads an offset: a decimal int64, not negative.
func parsePosition(position string) (int64, error) {
	offset, err := strconv.ParseInt(position, 10, 64)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: position %q is not an offset", snodra.ErrInvalid, position)
	}
	return offset, nil
}

// committedOffset is the offset a group commits for an acknowledgement of position: the next
// offset to read, never the acknowledged one.
func committedOffset(position string) (int64, error) {
	offset, err := parsePosition(position)
	if err != nil {
		return 0, err
	}
	if offset == math.MaxInt64 {
		return 0, fmt.Errorf("%w: position %q has no next offset", snodra.ErrInvalid, position)
	}
	return offset + 1, nil
}

// positionOf formats an offset as a position.
func positionOf(offset int64) string { return strconv.FormatInt(offset, 10) }

// comparePositions orders two positions of Kafka: decimal offsets by value, and any text that is
// not an offset after every offset (by its bytes among themselves). The core orders positions
// only through the event store's Compare, and the comparison must be total: Compare has no
// error to report for a position that is not an offset.
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

// segmentNames formats partition numbers as segment names, in segment order.
func segmentNames(partitions []int32) []string {
	names := make([]string, len(partitions))
	for i, partition := range partitions {
		names[i] = strconv.FormatInt(int64(partition), 10)
	}
	return names
}
