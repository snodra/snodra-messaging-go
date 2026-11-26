package snodra

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultMemoryEventStoreSegments is how many segments a MemoryEventStore has per source
	// unless configured otherwise.
	DefaultMemoryEventStoreSegments = 4
	// MaxMemoryEventStoreSegments is the most segments per source a MemoryEventStore takes.
	MaxMemoryEventStoreSegments = 1024
	// memoryEventStoreName is the store's name (EventPublisher.Name).
	memoryEventStoreName = "memory"
)

// MemoryEventStore is the in-memory event store: the whole event-store contract inside one
// process, for tests, local runs and single-process demos, with no infrastructure.
//
//   - Every source is a log of a fixed number of segments (DefaultMemoryEventStoreSegments by
//     default), named "0" to "n-1". An event's key goes to segment FNV-1a 64 of the key's UTF-8
//     bytes, unsigned, modulo the segments; an event without a key goes round the segments from
//     "0". A position is the decimal index of a segment's event, from "0".
//   - A subscription per name is shared by every Subscribe of that name in the process
//     (competing instances): its segments are spread over its members, and a segment moves to
//     another member only after the one holding it saw it revoked (its next Next), was closed
//     or was dropped. The committed positions are kept per name in memory.
//   - Capabilities are CapabilityReplay and CapabilityLag; not CapabilityNativeDeadLetter (the
//     dead letters stay in the message store) and not CapabilityBrowse (this library has no
//     event browser).
//   - It is not durable: it keeps every event it is given until the process ends, and loses
//     them all then. Bounding what it holds is the caller's concern (a test, a demo); the
//     service's builder warns at startup that it is not durable.
//   - Every reference to one MemoryEventStore is that event store: hand the same one to each
//     service instance of a test to make them compete. Use the pointer the constructor returns;
//     do not copy the struct.
//
// A MemoryEventStore is safe for concurrent use.
type MemoryEventStore struct {
	mu            sync.Mutex
	segments      int
	sources       map[string]*memorySource
	groups        map[SubscriptionSpec]*memoryGroup
	subscriptions []*memorySubscription
	nextMember    uint64
	spread        int
	appended      uint64
}

// NewMemoryEventStore returns an in-memory event store with DefaultMemoryEventStoreSegments
// segments per source.
func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		segments: DefaultMemoryEventStoreSegments,
		sources:  make(map[string]*memorySource),
		groups:   make(map[SubscriptionSpec]*memoryGroup),
	}
}

// NewMemoryEventStoreWithSegments returns an in-memory event store with segments per source,
// from 1 to MaxMemoryEventStoreSegments. It fails with ErrInvalid for a number out of bounds.
func NewMemoryEventStoreWithSegments(segments int) (*MemoryEventStore, error) {
	if segments < 1 || segments > MaxMemoryEventStoreSegments {
		return nil, fmt.Errorf("%w: the in-memory event store takes 1 to %d segments, not %d",
			ErrInvalid, MaxMemoryEventStoreSegments, segments)
	}
	store := NewMemoryEventStore()
	store.segments = segments
	return store, nil
}

// A memorySource is one source: the events of each of its segments, in order.
type memorySource struct {
	segments [][]*memoryRecord
}

// A memoryRecord is one appended event. It is immutable after Publish returns its copy.
type memoryRecord struct {
	key         *string
	headers     map[string]string
	payload     []byte
	timestampMs int64
	sequence    uint64
}

// A memoryGroup is one subscription name's committed positions, its members in joining order
// and which member holds each segment now.
type memoryGroup struct {
	committed map[int]uint64
	members   []uint64
	holders   map[int]uint64
}

func newMemoryGroup() *memoryGroup {
	return &memoryGroup{
		committed: make(map[int]uint64),
		holders:   make(map[int]uint64),
	}
}

// target is the member a segment should belong to: the members share the segments round robin
// in joining order.
func (g *memoryGroup) target(segment int) (uint64, bool) {
	if len(g.members) == 0 {
		return 0, false
	}
	return g.members[segment%len(g.members)], true
}

// leave removes member: its segments are free for the others.
func (g *memoryGroup) leave(member uint64) {
	members := g.members[:0]
	for _, m := range g.members {
		if m != member {
			members = append(members, m)
		}
	}
	g.members = members
	for segment, holder := range g.holders {
		if holder == member {
			delete(g.holders, segment)
		}
	}
}

// Name returns "memory".
func (s *MemoryEventStore) Name() string { return memoryEventStoreName }

// Capabilities returns the capabilities the store declares: CapabilityReplay and
// CapabilityLag. A fresh slice is returned on every call.
func (s *MemoryEventStore) Capabilities() []Capability {
	return []Capability{CapabilityReplay, CapabilityLag}
}

// Publish appends event at once. An event without a source fails with ErrRejected. The store
// keeps its own copies of the key, headers and payload.
func (s *MemoryEventStore) Publish(ctx context.Context, event OutgoingEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.Source == "" {
		return fmt.Errorf("%w: an event needs a source", ErrRejected)
	}
	s.mu.Lock()
	s.initLocked()
	source := s.sourceLocked(event.Source)
	segment := 0
	if event.Key != nil {
		segment = memorySegment(*event.Key, s.segments)
	} else {
		// TODO(spec-gap): SG-26 (the turn is the store's, shared by its sources).
		segment = s.spread
		s.spread = (s.spread + 1) % s.segments
	}
	record := &memoryRecord{
		key:         copyStringPointer(event.Key),
		headers:     copyStringMap(event.Headers),
		payload:     append([]byte(nil), event.Payload...),
		timestampMs: time.Now().UnixMilli(),
		sequence:    s.appended,
	}
	s.appended++
	source.segments[segment] = append(source.segments[segment], record)
	s.mu.Unlock()
	s.wakeAll()
	return nil
}

// Subscribe joins spec at once. It fails with ErrInvalid for an empty name or source.
func (s *MemoryEventStore) Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spec.Name == "" || spec.Source == "" {
		return nil, fmt.Errorf("%w: a subscription needs a name and a source", ErrInvalid)
	}
	s.mu.Lock()
	s.initLocked()
	s.sourceLocked(spec.Source)
	s.nextMember++
	member := s.nextMember
	group := s.groupLocked(spec)
	group.members = append(group.members, member)
	subscription := &memorySubscription{
		store:   s,
		spec:    spec,
		member:  member,
		cursors: make(map[int]uint64),
		wake:    make(chan struct{}, 1),
	}
	s.subscriptions = append(s.subscriptions, subscription)
	s.mu.Unlock()
	s.wakeAll()
	return subscription, nil
}

// Admin returns the reset side of spec. The in-memory store declares CapabilityReplay, so it
// always offers one.
func (s *MemoryEventStore) Admin(spec SubscriptionSpec) (SubscriptionAdmin, error) {
	if spec.Name == "" || spec.Source == "" {
		return nil, fmt.Errorf("%w: a subscription needs a name and a source", ErrInvalid)
	}
	s.mu.Lock()
	s.initLocked()
	s.mu.Unlock()
	return &memoryAdmin{store: s, spec: spec}, nil
}

// Compare orders two positions of one segment. Positions are decimal indexes; anything else is
// ordered as a segment name would be, so Compare is total and never panics.
func (s *MemoryEventStore) Compare(segment, a, b string) int { return SegmentOrder(a, b) }

// Events returns every event of source so far, as a subscription would deliver it, in append
// order: what a test reads back. It is empty for a source nothing was published to. It is not
// part of the EventStore contract.
func (s *MemoryEventStore) Events(source string) []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	sourceLog := s.sources[source]
	if sourceLog == nil {
		return nil
	}
	type placed struct {
		record  *memoryRecord
		segment int
		at      int
	}
	var all []placed
	for segment, records := range sourceLog.segments {
		for at, record := range records {
			all = append(all, placed{record: record, segment: segment, at: at})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].record.sequence != all[j].record.sequence {
			return all[i].record.sequence < all[j].record.sequence
		}
		if all[i].segment != all[j].segment {
			return all[i].segment < all[j].segment
		}
		return all[i].at < all[j].at
	})
	deliveries := make([]Delivery, len(all))
	for i, item := range all {
		deliveries[i] = Delivery{
			Segment:     strconv.Itoa(item.segment),
			Position:    strconv.Itoa(item.at),
			Key:         copyStringPointer(item.record.key),
			Headers:     copyStringMap(item.record.headers),
			Payload:     append([]byte(nil), item.record.payload...),
			TimestampMs: item.record.timestampMs,
		}
	}
	return deliveries
}

// initLocked gives a zero value the defaults of NewMemoryEventStore, so a zero MemoryEventStore
// is usable too.
func (s *MemoryEventStore) initLocked() {
	if s.segments == 0 {
		s.segments = DefaultMemoryEventStoreSegments
	}
	if s.sources == nil {
		s.sources = make(map[string]*memorySource)
	}
	if s.groups == nil {
		s.groups = make(map[SubscriptionSpec]*memoryGroup)
	}
}

func (s *MemoryEventStore) sourceLocked(source string) *memorySource {
	if sourceLog := s.sources[source]; sourceLog != nil {
		return sourceLog
	}
	sourceLog := &memorySource{segments: make([][]*memoryRecord, s.segments)}
	s.sources[source] = sourceLog
	return sourceLog
}

func (s *MemoryEventStore) groupLocked(spec SubscriptionSpec) *memoryGroup {
	if group := s.groups[spec]; group != nil {
		return group
	}
	group := newMemoryGroup()
	s.groups[spec] = group
	return group
}

// segmentLocked is the index of segment, which must be one of the store's names.
func (s *MemoryEventStore) segmentLocked(segment string) (int, error) {
	index, ok := parseMemoryIndex(segment)
	if !ok || index >= s.segments {
		return 0, fmt.Errorf("%w: no segment %s (segments 0 to %d)",
			ErrInvalid, memoryShown(segment), s.segments-1)
	}
	return index, nil
}

// headLocked is the position of the next event appended to a segment of source.
func (s *MemoryEventStore) headLocked(source string, segment int) uint64 {
	sourceLog := s.sources[source]
	if sourceLog == nil || segment < 0 || segment >= len(sourceLog.segments) {
		return 0
	}
	return uint64(len(sourceLog.segments[segment]))
}

// wakeAll wakes every subscription, so one waiting in Next looks again.
func (s *MemoryEventStore) wakeAll() {
	s.mu.Lock()
	subscriptions := append([]*memorySubscription(nil), s.subscriptions...)
	s.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.wakeUp()
	}
}

// memorySubscription is one instance's subscription of the in-memory event store.
type memorySubscription struct {
	store *MemoryEventStore
	spec  SubscriptionSpec
	// member is this subscription's id within the store.
	member uint64
	// cursors are the segments this subscription holds, with the next position it delivers.
	cursors map[int]uint64
	paused  bool
	// turn is where the next delivery is looked for first, so a busy segment cannot starve
	// the others.
	turn int
	left bool
	// changing is set when a Revoked was given and no Assigned since: the next step closes
	// the change.
	changing bool
	// announced is set when an Assigned was given: a new subscription's first event is one,
	// empty when it holds nothing.
	announced bool
	wake      chan struct{}
}

func (s *memorySubscription) wakeUp() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Next waits for the next assignment change or delivery. It is driven by the caller: a step
// that gives nothing waits for a change of the store, never polls.
func (s *memorySubscription) Next(ctx context.Context) (SubscriptionEvent, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.store.mu.Lock()
		if s.left {
			s.store.mu.Unlock()
			return nil, ErrClosed
		}
		event := s.stepLocked()
		s.store.mu.Unlock()
		if event != nil {
			if _, revoked := event.(Revoked); revoked {
				// The freed segments may go to another member now.
				s.store.wakeAll()
			}
			return event, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.wake:
		}
	}
}

// stepLocked is what Next gives now, if anything; never waits.
func (s *memorySubscription) stepLocked() SubscriptionEvent {
	group := s.store.groups[s.spec]
	if group == nil {
		return nil
	}
	held := s.heldLocked()
	revoked := make([]int, 0, len(held))
	for _, segment := range held {
		target, ok := group.target(segment)
		if !ok || target != s.member {
			revoked = append(revoked, segment)
		}
	}
	if len(revoked) > 0 {
		for _, segment := range revoked {
			delete(s.cursors, segment)
			if group.holders[segment] == s.member {
				delete(group.holders, segment)
			}
		}
		s.changing = true
		return Revoked{Segments: memorySegmentNames(revoked)}
	}
	gained := make([]int, 0, len(held))
	for segment := 0; segment < s.store.segments; segment++ {
		target, ok := group.target(segment)
		if !ok || target != s.member {
			continue
		}
		if _, has := s.cursors[segment]; has {
			continue
		}
		if holder, held := group.holders[segment]; held && holder != s.member {
			continue
		}
		gained = append(gained, segment)
	}
	if len(gained) > 0 {
		for _, segment := range gained {
			group.holders[segment] = s.member
			s.cursors[segment] = group.committed[segment]
		}
		s.changing = false
		s.announced = true
		return Assigned{Segments: memorySegmentNames(gained)}
	}
	// Every revocation is closed by an assignment, an empty one when nothing new came; and a
	// new subscription that holds nothing says so (a member, idle).
	if s.changing || !s.announced {
		s.changing = false
		s.announced = true
		return Assigned{}
	}
	if s.paused || len(s.cursors) == 0 {
		return nil
	}
	source := s.store.sources[s.spec.Source]
	if source == nil {
		return nil
	}
	held = s.heldLocked()
	for i := 0; i < len(held); i++ {
		segment := held[(s.turn+i)%len(held)]
		at := s.cursors[segment]
		records := source.segments[segment]
		if at >= uint64(len(records)) {
			continue
		}
		record := records[at]
		s.cursors[segment] = at + 1
		s.turn = (s.turn + i + 1) % len(held)
		return Delivery{
			Segment:     strconv.Itoa(segment),
			Position:    strconv.FormatUint(at, 10),
			Key:         copyStringPointer(record.key),
			Headers:     copyStringMap(record.headers),
			Payload:     append([]byte(nil), record.payload...),
			TimestampMs: record.timestampMs,
		}
	}
	return nil
}

// heldLocked is the segments this subscription holds, in segment order.
func (s *memorySubscription) heldLocked() []int {
	held := make([]int, 0, len(s.cursors))
	for segment := range s.cursors {
		held = append(held, segment)
	}
	sort.Ints(held)
	return held
}

// placeLocked reads segment and position and checks the position against the segment's head.
// It returns the segment index and the position; whether the subscription still holds the
// segment is the caller's to decide.
func (s *memorySubscription) placeLocked(segment, position string) (int, uint64, error) {
	index, err := s.store.segmentLocked(segment)
	if err != nil {
		return 0, 0, err
	}
	at, ok := parseMemoryIndex(position)
	if !ok {
		return 0, 0, fmt.Errorf("%w: %s is not a position", ErrInvalid, memoryShown(position))
	}
	head := s.store.headLocked(s.spec.Source, index)
	if uint64(at) >= head {
		return 0, 0, fmt.Errorf("%w: segment %d holds no event at %d (head %d)",
			ErrInvalid, index, at, head)
	}
	return index, uint64(at), nil
}

// Ack commits the position and every earlier one of the segment. Commits at once. An
// acknowledgement of a segment this subscription no longer holds is dropped.
func (s *memorySubscription) Ack(segment, position string) error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.left {
		return ErrClosed
	}
	index, at, err := s.placeLocked(segment, position)
	if err != nil {
		return err
	}
	if _, held := s.cursors[index]; !held {
		// TODO(spec-gap): SG-27 (a late acknowledgement of a segment that changed hands).
		return nil
	}
	group := s.store.groups[s.spec]
	if group == nil {
		return nil
	}
	if committed := at + 1; committed > group.committed[index] {
		group.committed[index] = committed
	}
	return nil
}

// Redeliver hands the delivery again, without leaving the subscription. A position at or after
// the subscription's next one changes nothing, so a redelivery never skips an event.
func (s *memorySubscription) Redeliver(segment, position string) error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.left {
		return ErrClosed
	}
	index, at, err := s.placeLocked(segment, position)
	if err != nil {
		return err
	}
	current, held := s.cursors[index]
	if !held {
		return nil
	}
	if at < current {
		// TODO(spec-gap): SG-28 (only an earlier position moves the next one back).
		s.cursors[index] = at
	}
	s.wakeUp()
	return nil
}

// Pause stops delivering; assignment changes still come.
func (s *memorySubscription) Pause() {
	s.store.mu.Lock()
	s.paused = true
	s.store.mu.Unlock()
}

// Resume delivers again from where the pause stopped.
func (s *memorySubscription) Resume() {
	s.store.mu.Lock()
	s.paused = false
	s.store.mu.Unlock()
	s.wakeUp()
}

// Segments is the segments assigned now, in segment order, with their positions and lag.
func (s *memorySubscription) Segments() []SegmentStatus {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.left {
		return nil
	}
	group := s.store.groups[s.spec]
	held := s.heldLocked()
	statuses := make([]SegmentStatus, 0, len(held))
	for _, segment := range held {
		head := s.store.headLocked(s.spec.Source, segment)
		position := s.cursors[segment]
		lag := int64(head - position)
		status := SegmentStatus{
			Segment:  strconv.Itoa(segment),
			Position: strconv.FormatUint(position, 10),
			Head:     strconv.FormatUint(head, 10),
		}
		if group != nil {
			if committed, ok := group.committed[segment]; ok {
				status.Committed = strconv.FormatUint(committed, 10)
				lag = int64(head - committed)
			}
		}
		if lag < 0 {
			lag = 0
		}
		status.Lag = lag
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		return SegmentOrder(statuses[i].Segment, statuses[j].Segment) < 0
	})
	return statuses
}

// TakeError returns "" : the in-memory store has no recovered trouble to report.
func (s *memorySubscription) TakeError() string { return "" }

// Close leaves the subscription; its segments go to the other subscriptions of the name.
func (s *memorySubscription) Close(ctx context.Context) error {
	s.store.mu.Lock()
	if s.left {
		s.store.mu.Unlock()
		return nil
	}
	s.left = true
	if group := s.store.groups[s.spec]; group != nil {
		group.leave(s.member)
	}
	for i, subscription := range s.store.subscriptions {
		if subscription == s {
			s.store.subscriptions = append(s.store.subscriptions[:i], s.store.subscriptions[i+1:]...)
			break
		}
	}
	s.store.mu.Unlock()
	s.wakeUp()
	s.store.wakeAll()
	return nil
}

// memoryAdmin is the reset side of one subscription name: every call answers at once.
type memoryAdmin struct {
	store *MemoryEventStore
	spec  SubscriptionSpec
}

// Members returns the instances reading the subscription now.
func (a *memoryAdmin) Members(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	a.store.initLocked()
	group := a.store.groups[a.spec]
	if group == nil {
		return []string{}, nil
	}
	members := make([]string, len(group.members))
	for i, member := range group.members {
		members[i] = fmt.Sprintf("memory-%d", member)
	}
	return members, nil
}

// Plan says where target moves each segment, from the committed positions now.
func (a *memoryAdmin) Plan(ctx context.Context, target ResetTarget) ([]ReplayRange, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	a.store.initLocked()
	source := a.spec.Source
	committed := func(segment int) string {
		if group := a.store.groups[a.spec]; group != nil {
			if position, ok := group.committed[segment]; ok {
				return strconv.FormatUint(position, 10)
			}
		}
		return ""
	}
	rangeOf := func(segment int, from uint64) ReplayRange {
		return ReplayRange{
			Source:  source,
			Segment: strconv.Itoa(segment),
			From:    strconv.FormatUint(from, 10),
			Until:   committed(segment),
		}
	}
	var ranges []ReplayRange
	switch target := target.(type) {
	case ResetEarliest:
		for segment := 0; segment < a.store.segments; segment++ {
			ranges = append(ranges, rangeOf(segment, 0))
		}
	case ResetLatest:
		for segment := 0; segment < a.store.segments; segment++ {
			ranges = append(ranges, rangeOf(segment, a.store.headLocked(source, segment)))
		}
	case ResetTimestamp:
		for segment := 0; segment < a.store.segments; segment++ {
			var at uint64
			if sourceLog := a.store.sources[source]; sourceLog != nil {
				records := sourceLog.segments[segment]
				at = uint64(len(records))
				for i, record := range records {
					if record.timestampMs >= target.TimestampMs {
						at = uint64(i)
						break
					}
				}
			}
			ranges = append(ranges, rangeOf(segment, at))
		}
	case ResetPositions:
		ranges = make([]ReplayRange, 0, len(target.Positions))
		for _, position := range target.Positions {
			segment, err := a.store.segmentLocked(position.Segment)
			if err != nil {
				return nil, err
			}
			from, ok := parseMemoryIndex(position.Position)
			if !ok {
				return nil, fmt.Errorf("%w: %s is not a position", ErrInvalid,
					memoryShown(position.Position))
			}
			if head := a.store.headLocked(source, segment); uint64(from) > head {
				return nil, fmt.Errorf("%w: position %d is past the head of segment %d (head %d)",
					ErrInvalid, from, segment, head)
			}
			ranges = append(ranges, rangeOf(segment, uint64(from)))
		}
	default:
		return nil, fmt.Errorf("%w: unknown reset target %T", ErrInvalid, target)
	}
	sort.Slice(ranges, func(i, j int) bool {
		return SegmentOrder(ranges[i].Segment, ranges[j].Segment) < 0
	})
	return ranges, nil
}

// Apply moves the committed position of each range's segment to its From, all or nothing. It
// fails with ErrSubscribersActive while an instance reads the subscription.
func (a *memoryAdmin) Apply(ctx context.Context, ranges []ReplayRange) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.store.mu.Lock()
	a.store.initLocked()
	moves := make(map[int]uint64, len(ranges))
	for _, replay := range ranges {
		if replay.Source != a.spec.Source {
			a.store.mu.Unlock()
			return fmt.Errorf("%w: a range of source %s, not of %s", ErrInvalid,
				memoryShown(replay.Source), memoryShown(a.spec.Source))
		}
		segment, err := a.store.segmentLocked(replay.Segment)
		if err != nil {
			a.store.mu.Unlock()
			return err
		}
		from, ok := parseMemoryIndex(replay.From)
		if !ok {
			a.store.mu.Unlock()
			return fmt.Errorf("%w: %s is not a position", ErrInvalid,
				memoryShown(replay.From))
		}
		if head := a.store.headLocked(a.spec.Source, segment); uint64(from) > head {
			a.store.mu.Unlock()
			return fmt.Errorf("%w: position %d is past the head of segment %d (head %d)",
				ErrInvalid, from, segment, head)
		}
		moves[segment] = uint64(from)
	}
	group := a.store.groups[a.spec]
	if group != nil && len(group.members) > 0 {
		members := make([]string, len(group.members))
		for i, member := range group.members {
			members[i] = fmt.Sprintf("memory-%d", member)
		}
		a.store.mu.Unlock()
		return &SubscribersActiveError{Members: members}
	}
	if group == nil {
		group = newMemoryGroup()
		a.store.groups[a.spec] = group
	}
	for segment, from := range moves {
		group.committed[segment] = from
	}
	a.store.mu.Unlock()
	return nil
}

// memorySegment is the segment a key goes to.
func memorySegment(key string, segments int) int {
	return int(fnv1a64(key) % uint64(segments))
}

// fnv1a64 is FNV-1a 64 of text's bytes, unsigned: the key's segment (modulo the number of
// segments) is the same in every run, and the same as the specification's reference in-memory
// event store.
func fnv1a64(text string) uint64 {
	hash := uint64(0xcbf29ce484222325)
	for i := 0; i < len(text); i++ {
		hash ^= uint64(text[i])
		hash *= 0x100000001b3
	}
	return hash
}

// parseMemoryIndex reads a canonical decimal index: digits only, no sign, no leading zero, no
// overflow.
func parseMemoryIndex(text string) (int, bool) {
	if text == "" || (len(text) > 1 && text[0] == '0') {
		return 0, false
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(text)
	if err != nil {
		return 0, false
	}
	return index, true
}

// memoryShown quotes text for an error message, cut so a bad argument cannot make the message
// long.
func memoryShown(text string) string {
	if len(text) > 64 {
		text = text[:64] + "..."
	}
	return strconv.Quote(text)
}

func memorySegmentNames(segments []int) []string {
	names := make([]string, len(segments))
	for i, segment := range segments {
		names[i] = strconv.Itoa(segment)
	}
	return names
}

func copyStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func copyStringMap(value map[string]string) map[string]string {
	if value == nil {
		return nil
	}
	copied := make(map[string]string, len(value))
	for name, content := range value {
		copied[name] = content
	}
	return copied
}

var (
	_ EventStore        = (*MemoryEventStore)(nil)
	_ EventSubscription = (*memorySubscription)(nil)
	_ SubscriptionAdmin = (*memoryAdmin)(nil)
)
