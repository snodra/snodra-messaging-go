package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/rabbitmq/internal/streamwire"
)

const (
	// readyLimit is the deliveries a partition buffers before it stops asking for chunks.
	readyLimit = 1_000
	// firstSettle is how long a subscription active on no partition waits before announcing its
	// first, empty assignment (the broker activates a lone or first consumer at once).
	firstSettle = 1 * time.Second
	// storeWait is how long a stored offset may take to be read back.
	storeWait = 5 * time.Second
)

// subscription is one instance's subscription of a processor's name on a source.
type subscription struct {
	store  *EventStore
	spec   snodra.SubscriptionSpec
	member string

	mu        sync.Mutex
	parts     []*part
	changes   []snodra.SubscriptionEvent
	paused    bool
	closed    bool
	broken    error
	errorText string
	announced bool
	turn      int

	notify chan struct{}
	done   chan struct{}
	once   sync.Once
	live   *liveGuard
}

type part struct {
	segment string
	stream  string
	conn    *streamwire.Conn
	subID   uint8

	active    bool
	next      uint64
	hasNext   bool
	acked     uint64
	hasAcked  bool
	stored    uint64
	hasStored bool
	// handed is one past the last offset handed out since the partition became active.
	handed    uint64
	hasHanded bool
	ready     []snodra.Delivery
	inflight  []snodra.Delivery
	head      uint64
	hasHead   bool
	owed      bool
	unstored  uint64
	lastStore time.Time
}

// memberID is a random id per subscription instance, the `snodra-member` subscribe property.
func memberID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(raw[:])
}

func newSubscription(ctx context.Context, store *EventStore, spec snodra.SubscriptionSpec) (*subscription, error) {
	member := memberID()
	seed, err := store.dialAny(ctx, storeConnection)
	if err != nil {
		return nil, err
	}
	defer func() { _ = seed.Close() }()
	layout, err := layoutOf(ctx, seed, spec.Source)
	if err != nil {
		return nil, joinError(ctx, err)
	}
	if len(layout.parts) > 254 {
		return nil, fmt.Errorf("%w: the source %s has %d partitions, over 254", snodra.ErrInvalid, spec.Source, len(layout.parts))
	}
	sort.Slice(layout.parts, func(i, j int) bool {
		return snodra.SegmentOrder(layout.parts[i].segment, layout.parts[j].segment) < 0
	})
	s := &subscription{
		store:  store,
		spec:   spec,
		member: member,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	s.live = store.live.enter(spec.Source, spec.Name, member)
	joined := false
	defer func() {
		if !joined {
			s.abandon()
		}
	}()
	for index, source := range layout.parts {
		address, err := leaderOf(ctx, seed, source.stream)
		if err != nil {
			return nil, joinError(ctx, err)
		}
		target, err := store.dial(ctx, address, storeConnection)
		if err != nil {
			return nil, joinError(ctx, err)
		}
		p := &part{
			segment:   source.segment,
			stream:    source.stream,
			conn:      target,
			subID:     uint8(index + 1),
			lastStore: time.Now(),
		}
		properties := map[string]string{
			"single-active-consumer": "true",
			"name":                   spec.Name,
			memberProperty:           member,
		}
		if layout.super {
			properties["super-stream"] = spec.Source
		}
		target.SetDeliverHandler(func(frame streamwire.Frame) { s.handleDeliver(p, frame) })
		target.SetUpdateHandler(func(subscriptionID uint8, active bool, correlationID uint32) {
			if subscriptionID != p.subID {
				return
			}
			go s.handleUpdate(p, active, correlationID)
		})
		target.SetMetadataUpdateHandler(func(stream string, code uint16) {
			if stream == p.stream {
				s.fail(fmt.Errorf("%w: the stream %s changed (deleted or moved)", snodra.ErrFailed, stream))
			}
		})
		target.SetCloseHandler(func(err error) {
			if err == nil {
				s.fail(fmt.Errorf("%w: the connection to %s closed", snodra.ErrFailed, p.stream))
				return
			}
			s.fail(fmt.Errorf("%w: the connection to %s closed: %w", snodra.ErrFailed, p.stream, err))
		})
		if err := target.Subscribe(ctx, p.subID, p.stream, streamwire.Next(), 1, properties); err != nil {
			_ = target.Close()
			return nil, joinError(ctx, classify("subscribe", err))
		}
		s.parts = append(s.parts, p)
	}
	// A new subscription's first event is an assignment, empty when it holds no partition,
	// within the bound of its joining.
	go func() {
		timer := time.NewTimer(firstSettle)
		defer timer.Stop()
		select {
		case <-s.done:
		case <-timer.C:
			s.mu.Lock()
			if !s.announced && !s.closed {
				s.announced = true
				s.changes = append(s.changes, snodra.Assigned{})
			}
			s.mu.Unlock()
			s.signal()
		}
	}()
	go s.storeLoop()
	joined = true
	return s, nil
}

// joinError keeps a done context as-is.
func joinError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// abandon closes what a failed join opened.
func (s *subscription) abandon() {
	s.live.leave()
	for _, p := range s.parts {
		_ = p.conn.Close()
	}
}

func (s *subscription) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *subscription) storeLoop() {
	interval := s.store.config.OffsetStoreInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.mu.Lock()
			for _, p := range s.parts {
				if p.active && p.hasAcked && p.unstored > 0 {
					acked := p.acked
					p.unstored = 0
					p.lastStore = time.Now()
					go func() {
						if err := p.conn.StoreOffset(s.spec.Name, p.stream, acked); err != nil {
							s.recordError(fmt.Sprintf("storing the offset of %s: %v", p.stream, err))
						}
					}()
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *subscription) handleDeliver(p *part, frame streamwire.Frame) {
	chunk, err := streamwire.ParseDeliver(frame)
	if err != nil {
		s.fail(fmt.Errorf("%w: %w", snodra.ErrFailed, err))
		return
	}
	deliveries := make([]snodra.Delivery, 0, len(chunk.Messages))
	for _, message := range chunk.Messages {
		key, headers, payload, err := decodeMessage(message.Data)
		if err != nil {
			s.fail(fmt.Errorf("%w: %w", snodra.ErrFailed, err))
			return
		}
		deliveries = append(deliveries, snodra.Delivery{
			Segment:     p.segment,
			Position:    strconv.FormatUint(message.Offset, 10),
			Key:         key,
			Headers:     headers,
			Payload:     payload,
			TimestampMs: chunk.TimestampMs,
		})
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	active := p.active
	start := p.next
	if !p.hasNext {
		start = chunk.FirstOffset
	}
	for _, delivery := range deliveries {
		offset, _ := strconv.ParseUint(delivery.Position, 10, 64)
		if offset < start {
			continue
		}
		if !active {
			continue
		}
		p.ready = append(p.ready, delivery)
		p.next = offset + 1
		p.hasNext = true
		if !p.hasHead || p.next > p.head {
			p.head = p.next
			p.hasHead = true
		}
	}
	if active {
		end := chunk.FirstOffset + uint64(chunk.Records)
		if !p.hasHead || end > p.head {
			p.head = end
			p.hasHead = true
		}
	}
	credit := !s.paused && len(p.ready)+len(p.inflight) < readyLimit
	if !credit {
		p.owed = true
	}
	s.mu.Unlock()
	s.signal()
	if credit {
		if err := p.conn.Credit(p.subID, 1); err != nil {
			s.fail(fmt.Errorf("%w: asking for the next chunk of %s: %w", snodra.ErrFailed, p.stream, err))
		}
	}
}

// handleUpdate answers one consumer update. It runs off the connection's read loop: reading the
// stored offset on the same connection would deadlock there.
func (s *subscription) handleUpdate(p *part, active bool, correlationID uint32) {
	if active {
		s.activate(p, correlationID)
		return
	}
	s.deactivate(p, correlationID)
}

func (s *subscription) activate(p *part, correlationID uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), s.store.config.RequestTimeout)
	stored, found, err := p.conn.QueryOffset(ctx, s.spec.Name, p.stream)
	cancel()
	if err != nil {
		// Never from the first offset on a failed read (the whole partition again): the
		// subscription ends and the core subscribes again.
		s.fail(fmt.Errorf("%w: %s: the stored offset was not read on activation: %w", snodra.ErrFailed, p.stream, err))
		_ = p.conn.ConsumerUpdateResponse(correlationID, streamwire.Next())
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = p.conn.ConsumerUpdateResponse(correlationID, streamwire.Next())
		return
	}
	p.active = true
	p.next = 0
	p.hasNext = true
	p.hasStored = false
	p.stored = 0
	if found {
		p.next = stored
		p.stored = stored
		p.hasStored = true
	}
	p.acked = 0
	p.hasAcked = false
	p.handed = 0
	p.hasHanded = false
	p.ready = nil
	p.inflight = nil
	// The broker holds no chunk for this subscription after an activation until it is
	// credited; while paused, the credit waits for Resume.
	credit := !s.paused
	p.owed = s.paused
	if !p.hasHead || p.next > p.head {
		p.head = p.next
		p.hasHead = true
	}
	s.announced = true
	s.changes = append(s.changes, snodra.Assigned{Segments: []string{p.segment}})
	s.mu.Unlock()
	s.signal()
	if found {
		_ = p.conn.ConsumerUpdateResponse(correlationID, streamwire.At(stored))
	} else {
		_ = p.conn.ConsumerUpdateResponse(correlationID, streamwire.First())
	}
	if credit {
		_ = p.conn.Credit(p.subID, 1)
	}
}

func (s *subscription) deactivate(p *part, correlationID uint32) {
	s.mu.Lock()
	wasActive := p.active
	var acked uint64
	hasAcked := p.hasAcked && wasActive
	if hasAcked {
		acked = p.acked
	}
	if wasActive {
		p.active = false
		p.acked = 0
		p.hasAcked = false
		p.handed = 0
		p.hasHanded = false
		p.ready = nil
		p.inflight = nil
		s.changes = append(s.changes, snodra.Revoked{Segments: []string{p.segment}})
		s.changes = append(s.changes, snodra.Assigned{})
	}
	s.mu.Unlock()
	s.signal()
	if hasAcked {
		s.storeAndVerify(p, acked)
	}
	_ = p.conn.ConsumerUpdateResponse(correlationID, streamwire.Next())
}

// storeAndVerify stores acked as the next offset and waits until the broker reads it back.
func (s *subscription) storeAndVerify(p *part, acked uint64) {
	if err := p.conn.StoreOffset(s.spec.Name, p.stream, acked); err != nil {
		s.recordError(fmt.Sprintf("storing the offset of %s: %v", p.stream, err))
		return
	}
	deadline := time.Now().Add(storeWait)
	for {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		stored, found, err := p.conn.QueryOffset(ctx, s.spec.Name, p.stream)
		cancel()
		if err == nil && found && stored == acked {
			s.mu.Lock()
			p.stored = stored
			p.hasStored = true
			s.mu.Unlock()
			return
		}
		if time.Now().After(deadline) {
			s.recordError(fmt.Sprintf("the offset of %s was stored as %d but did not read back: %v", p.stream, acked, err))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *subscription) recordError(text string) {
	s.mu.Lock()
	s.errorText = text
	s.mu.Unlock()
}

func (s *subscription) fail(err error) {
	s.mu.Lock()
	if !s.closed && s.broken == nil {
		s.broken = err
		s.errorText = err.Error()
	}
	s.mu.Unlock()
	s.signal()
}

// Next waits for the next assignment change or delivery.
func (s *subscription) Next(ctx context.Context) (snodra.SubscriptionEvent, error) {
	for {
		s.mu.Lock()
		if len(s.changes) > 0 {
			change := s.changes[0]
			s.changes = s.changes[1:]
			s.mu.Unlock()
			return change, nil
		}
		if !s.paused && !s.closed {
			n := len(s.parts)
			for k := 0; k < n; k++ {
				i := (s.turn + k) % n
				p := s.parts[i]
				if !p.active || len(p.ready) == 0 {
					continue
				}
				delivery := p.ready[0]
				p.ready = p.ready[1:]
				p.inflight = append(p.inflight, delivery)
				offset, _ := strconv.ParseUint(delivery.Position, 10, 64)
				if !p.hasHanded || offset+1 > p.handed {
					p.handed = offset + 1
					p.hasHanded = true
				}
				s.turn = i + 1
				credit := p.owed && len(p.ready) < readyLimit
				p.owed = false
				s.mu.Unlock()
				if credit {
					_ = p.conn.Credit(p.subID, 1)
				}
				return delivery, nil
			}
		}
		if s.broken != nil {
			err := s.broken
			s.mu.Unlock()
			return nil, err
		}
		if s.closed {
			s.mu.Unlock()
			return nil, snodra.ErrClosed
		}
		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-s.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Ack acknowledges the delivery at position and every earlier one of the segment.
func (s *subscription) Ack(segment, position string) error {
	offset, err := parseOffset(position)
	if err != nil {
		return err
	}
	s.mu.Lock()
	p := s.part(segment)
	if p == nil || !p.active {
		s.mu.Unlock()
		return nil
	}
	next := offset + 1
	if p.hasAcked && p.acked >= next {
		s.mu.Unlock()
		return nil
	}
	if p.hasHanded && offset >= p.handed {
		s.mu.Unlock()
		return nil
	}
	if p.hasStored && offset < p.stored {
		s.mu.Unlock()
		return nil
	}
	p.acked = next
	p.hasAcked = true
	kept := p.inflight[:0]
	for _, delivery := range p.inflight {
		delivered, _ := strconv.ParseUint(delivery.Position, 10, 64)
		if delivered > offset {
			kept = append(kept, delivery)
		}
	}
	p.inflight = kept
	p.unstored++
	store := p.unstored >= s.store.config.OffsetStoreMessages || time.Since(p.lastStore) >= s.store.config.OffsetStoreInterval
	if store {
		p.unstored = 0
		p.lastStore = time.Now()
	}
	s.mu.Unlock()
	if store {
		if err := p.conn.StoreOffset(s.spec.Name, p.stream, next); err != nil {
			s.recordError(fmt.Sprintf("storing the offset of %s: %v", p.stream, err))
		}
	}
	return nil
}

// Redeliver hands out again the deliveries from position on, ahead of the later ones.
func (s *subscription) Redeliver(segment, position string) error {
	from, err := parseOffset(position)
	if err != nil {
		return err
	}
	s.mu.Lock()
	p := s.part(segment)
	if p == nil || !p.active {
		s.mu.Unlock()
		return nil
	}
	var again []snodra.Delivery
	kept := p.inflight[:0]
	for _, delivery := range p.inflight {
		offset, _ := strconv.ParseUint(delivery.Position, 10, 64)
		if offset >= from {
			again = append(again, delivery)
		} else {
			kept = append(kept, delivery)
		}
	}
	p.inflight = kept
	p.ready = append(again, p.ready...)
	s.mu.Unlock()
	s.signal()
	return nil
}

// Pause stops delivering without losing the place.
func (s *subscription) Pause() {
	s.mu.Lock()
	s.paused = true
	s.mu.Unlock()
}

// Resume delivers again from where the pause stopped.
func (s *subscription) Resume() {
	s.mu.Lock()
	s.paused = false
	var credits []*part
	for _, p := range s.parts {
		if p.active && p.owed && len(p.ready) < readyLimit {
			p.owed = false
			credits = append(credits, p)
		}
	}
	s.mu.Unlock()
	s.signal()
	for _, p := range credits {
		_ = p.conn.Credit(p.subID, 1)
	}
}

// Segments is the assigned segments, as last known.
func (s *subscription) Segments() []snodra.SegmentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	segments := make([]snodra.SegmentStatus, 0, len(s.parts))
	for _, p := range s.parts {
		if !p.active {
			continue
		}
		position := p.next
		if len(p.ready) > 0 {
			position, _ = strconv.ParseUint(p.ready[0].Position, 10, 64)
		}
		status := snodra.SegmentStatus{
			Segment:  p.segment,
			Position: strconv.FormatUint(position, 10),
			Lag:      int64(len(p.ready) + len(p.inflight)),
		}
		switch {
		case p.hasAcked:
			status.Committed = strconv.FormatUint(p.acked, 10)
		case p.hasStored:
			status.Committed = strconv.FormatUint(p.stored, 10)
		}
		if p.hasHead {
			status.Head = strconv.FormatUint(p.head, 10)
			if p.head > p.next {
				status.Lag += int64(p.head - p.next)
			}
		}
		segments = append(segments, status)
	}
	sort.Slice(segments, func(i, j int) bool {
		return snodra.SegmentOrder(segments[i].Segment, segments[j].Segment) < 0
	})
	return segments
}

// TakeError is the latest trouble the subscription recovered from by itself.
func (s *subscription) TakeError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	text := s.errorText
	s.errorText = ""
	return text
}

// Close flushes the acknowledged offsets and leaves the subscription.
func (s *subscription) Close(ctx context.Context) error {
	s.once.Do(func() {
		go s.close()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	s.closed = true
	type flush struct {
		part  *part
		acked uint64
		has   bool
	}
	var flushes []flush
	for _, p := range s.parts {
		flushes = append(flushes, flush{part: p, acked: p.acked, has: p.hasAcked})
	}
	s.mu.Unlock()
	s.signal()
	closeCtx, cancel := context.WithTimeout(context.Background(), storeWait*2)
	defer cancel()
	for _, f := range flushes {
		if f.has {
			s.storeAndVerify(f.part, f.acked)
		}
		unsubscribeCtx, unsubscribeCancel := context.WithTimeout(closeCtx, storeWait)
		if !f.part.conn.IsClosed() {
			_ = f.part.conn.Unsubscribe(unsubscribeCtx, f.part.subID)
		}
		unsubscribeCancel()
	}
	for _, p := range s.parts {
		_ = p.conn.Close()
	}
	s.live.leave()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	close(s.done)
}

func (s *subscription) part(segment string) *part {
	for _, p := range s.parts {
		if p.segment == segment {
			return p
		}
	}
	return nil
}

func parseOffset(position string) (uint64, error) {
	offset, err := strconv.ParseUint(position, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: position %q is not an offset", snodra.ErrInvalid, position)
	}
	return offset, nil
}
