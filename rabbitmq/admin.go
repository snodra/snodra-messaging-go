package rabbitmq

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/rabbitmq/internal/streamwire"
)

const (
	// adminBudget is the longest one admin call reads the streams for.
	adminBudget = 20 * time.Second
	// quiet is how long a stream may stay silent before it is taken as empty.
	quiet = 1500 * time.Millisecond
)

// admin is the reset side of one subscription (EventStore.Admin). Every call blocks: it talks
// to the event store.
type admin struct {
	store *EventStore
	spec  snodra.SubscriptionSpec
}

// nowPart is one partition with its offsets as they are now.
type nowPart struct {
	part      sourcePart
	stored    uint64
	hasStored bool
	first     uint64
	head      uint64
	lastTime  int64
	empty     bool
}

// Members is the instances reading the subscription now: the management API's consumers with
// the subscription's name on its partitions (distinct `snodra-member` values), plus this
// process's own open subscriptions of the name.
func (a *admin) Members(ctx context.Context) ([]string, error) {
	streams, err := a.streams(ctx)
	if err != nil {
		return nil, err
	}
	members, err := a.store.management.members(ctx, a.spec.Name, streams)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(members))
	for _, member := range members {
		seen[member] = true
	}
	for _, member := range a.store.live.members(a.spec.Source, a.spec.Name) {
		if !seen[member] {
			seen[member] = true
			members = append(members, member)
		}
	}
	sort.Strings(members)
	return members, nil
}

// streams is the partition streams of the source.
func (a *admin) streams(ctx context.Context) ([]string, error) {
	seed, err := a.store.dialAny(ctx, storeConnection)
	if err != nil {
		return nil, err
	}
	defer func() { _ = seed.Close() }()
	layout, err := layoutOf(ctx, seed, a.spec.Source)
	if err != nil {
		return nil, err
	}
	streams := make([]string, 0, len(layout.parts))
	for _, part := range layout.parts {
		streams = append(streams, part.stream)
	}
	return streams, nil
}

// now reads each partition's stored offset and bounds.
func (a *admin) now(ctx context.Context) ([]nowPart, error) {
	ctx, cancel := context.WithTimeout(ctx, adminBudget)
	defer cancel()
	seed, err := a.store.dialAny(ctx, storeConnection)
	if err != nil {
		return nil, err
	}
	layout, err := layoutOf(ctx, seed, a.spec.Source)
	if err != nil {
		_ = seed.Close()
		return nil, err
	}
	all := make([]nowPart, 0, len(layout.parts))
	for _, part := range layout.parts {
		address, err := leaderOf(ctx, seed, part.stream)
		if err != nil {
			_ = seed.Close()
			return nil, err
		}
		connection, err := a.store.dial(ctx, address, storeConnection)
		if err != nil {
			_ = seed.Close()
			return nil, err
		}
		stored, found, err := connection.QueryOffset(ctx, a.spec.Name, part.stream)
		if err != nil {
			_ = connection.Close()
			_ = seed.Close()
			return nil, classify("query offset", err)
		}
		first, head, lastTime, empty, err := bounds(ctx, a.store, address, part.stream)
		if err != nil {
			_ = connection.Close()
			_ = seed.Close()
			return nil, err
		}
		if found && stored > head && !empty {
			_ = connection.Close()
			_ = seed.Close()
			return nil, fmt.Errorf("%w: the offset stored for %s on %s (%d) is past the head read (%d)",
				snodra.ErrFailed, a.spec.Name, part.stream, stored, head)
		}
		_ = connection.Close()
		all = append(all, nowPart{
			part:      part,
			stored:    stored,
			hasStored: found,
			first:     first,
			head:      head,
			lastTime:  lastTime,
			empty:     empty,
		})
	}
	_ = seed.Close()
	sort.Slice(all, func(i, j int) bool {
		return snodra.SegmentOrder(all[i].part.segment, all[j].part.segment) < 0
	})
	return all, nil
}

// Plan says where target moves each segment, from the subscription's current offsets.
func (a *admin) Plan(ctx context.Context, target snodra.ResetTarget) ([]snodra.ReplayRange, error) {
	all, err := a.now(ctx)
	if err != nil {
		return nil, err
	}
	rangeOf := func(part nowPart, from uint64) snodra.ReplayRange {
		r := snodra.ReplayRange{Source: a.spec.Source, Segment: part.part.segment, From: strconv.FormatUint(from, 10)}
		if part.hasStored {
			r.Until = strconv.FormatUint(part.stored, 10)
		}
		return r
	}
	switch target := target.(type) {
	case snodra.ResetEarliest:
		ranges := make([]snodra.ReplayRange, 0, len(all))
		for _, part := range all {
			ranges = append(ranges, rangeOf(part, part.first))
		}
		return ranges, nil
	case snodra.ResetLatest:
		ranges := make([]snodra.ReplayRange, 0, len(all))
		for _, part := range all {
			ranges = append(ranges, rangeOf(part, part.head))
		}
		return ranges, nil
	case snodra.ResetTimestamp:
		ranges := make([]snodra.ReplayRange, 0, len(all))
		for _, part := range all {
			from, err := offsetAt(ctx, a.store, part, target.TimestampMs)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, rangeOf(part, from))
		}
		return ranges, nil
	case snodra.ResetPositions:
		seen := make(map[string]bool, len(target.Positions))
		ranges := make([]snodra.ReplayRange, 0, len(target.Positions))
		for _, requested := range target.Positions {
			if seen[requested.Segment] {
				return nil, fmt.Errorf("%w: segment %s is listed twice", snodra.ErrInvalid, requested.Segment)
			}
			seen[requested.Segment] = true
			var found *nowPart
			for i := range all {
				if all[i].part.segment == requested.Segment {
					found = &all[i]
					break
				}
			}
			if found == nil {
				return nil, fmt.Errorf("%w: the source %s has no segment %s", snodra.ErrInvalid, a.spec.Source, requested.Segment)
			}
			from, err := strconv.ParseUint(requested.Position, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: position %q is not an offset", snodra.ErrInvalid, requested.Position)
			}
			if from > found.head {
				return nil, fmt.Errorf("%w: position %d is past the head (%d) of segment %s", snodra.ErrInvalid, from, found.head, requested.Segment)
			}
			ranges = append(ranges, rangeOf(*found, from))
		}
		sort.Slice(ranges, func(i, j int) bool {
			return snodra.SegmentOrder(ranges[i].Segment, ranges[j].Segment) < 0
		})
		return ranges, nil
	default:
		return nil, fmt.Errorf("%w: unknown reset target %T", snodra.ErrInvalid, target)
	}
}

// Apply moves each range's segment committed position to its From, all or nothing. It refuses
// while an instance reads the subscription.
//
// TODO(spec-gap): SG-48 (RabbitMQ stores each partition's offset separately, so a failure
// mid-way cannot undo the earlier ones).
func (a *admin) Apply(ctx context.Context, ranges []snodra.ReplayRange) error {
	members, err := a.Members(ctx)
	if err != nil {
		return err
	}
	if len(members) > 0 {
		return &snodra.SubscribersActiveError{Members: members}
	}
	all, err := a.now(ctx)
	if err != nil {
		return err
	}
	type move struct {
		part *nowPart
		from uint64
	}
	moves := make([]move, 0, len(ranges))
	for _, requested := range ranges {
		if requested.Source != a.spec.Source {
			return fmt.Errorf("%w: the range's source %q is not the subscription's %q", snodra.ErrInvalid, requested.Source, a.spec.Source)
		}
		var found *nowPart
		for i := range all {
			if all[i].part.segment == requested.Segment {
				found = &all[i]
				break
			}
		}
		if found == nil {
			return fmt.Errorf("%w: the source %s has no segment %s", snodra.ErrInvalid, a.spec.Source, requested.Segment)
		}
		from, err := strconv.ParseUint(requested.From, 10, 64)
		if err != nil {
			return fmt.Errorf("%w: position %q is not an offset", snodra.ErrInvalid, requested.From)
		}
		if from > found.head {
			return fmt.Errorf("%w: position %d is past the head (%d) of segment %s", snodra.ErrInvalid, from, found.head, requested.Segment)
		}
		moves = append(moves, move{part: found, from: from})
	}
	for _, move := range moves {
		address, err := a.leader(ctx, move.part.part.stream)
		if err != nil {
			return err
		}
		connection, err := a.store.dial(ctx, address, storeConnection)
		if err != nil {
			return err
		}
		err = connection.StoreOffset(a.spec.Name, move.part.part.stream, move.from)
		_ = connection.Close()
		if err != nil {
			return classify("store offset", err)
		}
	}
	// Read back once all are stored: an instance that joined meanwhile and stored its own
	// position undoes the reset, which then fails.
	for _, move := range moves {
		address, err := a.leader(ctx, move.part.part.stream)
		if err != nil {
			return err
		}
		connection, err := a.store.dial(ctx, address, storeConnection)
		if err != nil {
			return err
		}
		stored, found, err := connection.QueryOffset(ctx, a.spec.Name, move.part.part.stream)
		_ = connection.Close()
		if err != nil {
			return classify("query offset", err)
		}
		if !found || stored != move.from {
			return &snodra.SubscribersActiveError{Members: members}
		}
	}
	return nil
}

func (a *admin) leader(ctx context.Context, stream string) (string, error) {
	seed, err := a.store.dialAny(ctx, storeConnection)
	if err != nil {
		return "", err
	}
	defer func() { _ = seed.Close() }()
	return leaderOf(ctx, seed, stream)
}

// bounds is a stream's first offset, its head and its last chunk's time; empty when both reads
// are silent. One silent read beside one that answered is a failure, never an empty stream.
//
// TODO(spec-gap): SG-45 (no page says how an add-on reads a stream's first offset and head).
func bounds(ctx context.Context, store *EventStore, address, stream string) (first, head uint64, lastTime int64, empty bool, err error) {
	type read struct {
		header streamwire.ChunkHeader
		ok     bool
		err    error
	}
	reads := make(chan read, 2)
	go func() {
		header, ok, err := readChunk(ctx, store, address, stream, streamwire.First())
		reads <- read{header: header, ok: ok, err: err}
	}()
	go func() {
		header, ok, err := readChunk(ctx, store, address, stream, streamwire.Last())
		reads <- read{header: header, ok: ok, err: err}
	}()
	answers := [2]read{}
	for i := 0; i < 2; i++ {
		answer := <-reads
		if answer.err != nil {
			return 0, 0, 0, false, answer.err
		}
		if i == 0 {
			answers[0] = answer
		} else {
			answers[1] = answer
		}
	}
	// The reads come in any order; tell the first from the last by the offsets they found.
	firstRead, lastRead := answers[0], answers[1]
	if firstRead.ok && lastRead.ok && firstRead.header.FirstOffset > lastRead.header.FirstOffset {
		firstRead, lastRead = lastRead, firstRead
	}
	switch {
	case !firstRead.ok && !lastRead.ok:
		return 0, 0, 0, true, nil
	case !firstRead.ok || !lastRead.ok:
		return 0, 0, 0, false, fmt.Errorf("%w: the bounds of %s were not read: one read answered, the other did not within %v",
			snodra.ErrFailed, stream, quiet)
	}
	first = firstRead.header.FirstOffset
	head = lastRead.header.FirstOffset + uint64(lastRead.header.Records)
	if head < first {
		first = head
	}
	return first, head, lastRead.header.TimestampMs, false, nil
}

// readChunk reads one chunk at spec, or reports silence within quiet.
func readChunk(ctx context.Context, store *EventStore, address, stream string, spec streamwire.OffsetSpec) (streamwire.ChunkHeader, bool, error) {
	connection, err := store.dial(ctx, address, storeConnection)
	if err != nil {
		return streamwire.ChunkHeader{}, false, err
	}
	defer func() { _ = connection.Close() }()
	frames := make(chan streamwire.Frame, 1)
	connection.SetDeliverHandler(func(frame streamwire.Frame) {
		select {
		case frames <- frame:
		default:
		}
	})
	if err := connection.Subscribe(ctx, 1, stream, spec, 1, nil); err != nil {
		return streamwire.ChunkHeader{}, false, classify("subscribe", err)
	}
	timer := time.NewTimer(quiet)
	defer timer.Stop()
	select {
	case frame := <-frames:
		header, err := streamwire.ParseChunkHeader(frame)
		if err != nil {
			return streamwire.ChunkHeader{}, false, fmt.Errorf("%w: %w", snodra.ErrFailed, err)
		}
		return header, true, nil
	case <-timer.C:
		return streamwire.ChunkHeader{}, false, nil
	case <-ctx.Done():
		return streamwire.ChunkHeader{}, false, ctx.Err()
	}
}

// offsetAt is the first offset of a stream written at or after timestamp_ms, else its head.
func offsetAt(ctx context.Context, store *EventStore, part nowPart, timestampMs int64) (uint64, error) {
	if part.empty || part.lastTime < timestampMs {
		return part.head, nil
	}
	address, err := part.leader(ctx, store)
	if err != nil {
		return 0, err
	}
	connection, err := store.dial(ctx, address, storeConnection)
	if err != nil {
		return 0, err
	}
	defer func() { _ = connection.Close() }()
	frames := make(chan streamwire.Frame, 16)
	connection.SetDeliverHandler(func(frame streamwire.Frame) {
		select {
		case frames <- frame:
		default:
		}
	})
	if err := connection.Subscribe(ctx, 1, part.part.stream, streamwire.Timestamp(timestampMs), 16, nil); err != nil {
		return 0, classify("subscribe", err)
	}
	deadline := time.Now().Add(adminBudget)
	for {
		if time.Now().After(deadline) {
			return part.head, nil
		}
		select {
		case frame := <-frames:
			header, err := streamwire.ParseChunkHeader(frame)
			if err != nil {
				return 0, fmt.Errorf("%w: %w", snodra.ErrFailed, err)
			}
			if header.TimestampMs >= timestampMs {
				first := header.FirstOffset
				if first < part.first {
					first = part.first
				}
				return first, nil
			}
			if header.FirstOffset+uint64(header.Records) >= part.head {
				return part.head, nil
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (p nowPart) leader(ctx context.Context, store *EventStore) (string, error) {
	seed, err := store.dialAny(ctx, storeConnection)
	if err != nil {
		return "", err
	}
	defer func() { _ = seed.Close() }()
	return leaderOf(ctx, seed, p.part.stream)
}
