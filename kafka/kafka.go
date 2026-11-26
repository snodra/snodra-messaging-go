// Package kafka is the Kafka event store add-on: the core's snodra.EventStore on Apache Kafka.
//
// On Kafka a source is a topic, a segment one of its partitions (the partition number in
// decimal) and a position an offset (in decimal); a subscription's name is its consumer group,
// `<service>.<processor>`. The layout on the wire is the one every official Snodra library
// writes (https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#kafka-wire-rules):
//
//   - one record per event: the value is the event's JSON payload, the key the event's key (an
//     empty key is a key; an event without one has no record key), and every header a record
//     header whose name and UTF-8 value are as given;
//   - the producer partitions a keyed record as the Java client's default partitioner does:
//     murmur2 of the key's bytes (seed 0x9747b28c), masked positive and taken modulo the
//     partitions. One key therefore lands on one partition whatever the publishing library is,
//     and its events keep their order there;
//   - a processor reads in the consumer group `<service>.<processor>`, on the classic group
//     protocol with the `range` assignor, so the members of every official library meet in one
//     group;
//   - the group's committed offset is the next one to read: acknowledging the record at offset N
//     stores N+1, and a new group starts at the earliest offset;
//   - a dead letter goes to `<source>.dlt`.
//
// The store declares the capabilities replay, lag and native-dead-letter. It has no event
// browser and declares no browse. It never creates or changes topics: the infrastructure does.
//
// The type is safe for concurrent use. Every subscription owns a Kafka consumer; Close releases
// it. EventStore.Close releases the producer and the admin client and does not close the
// subscriptions (the caller closes them).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Name is the name this add-on reports as the processors' event store.
const Name = "kafka"

// The add-on's own bounds and timeouts. They are the values the other official Kafka add-ons
// use, and a Config may replace each of them.
const (
	// DefaultMaxPollRecords is the most records one poll hands a subscription.
	DefaultMaxPollRecords = 100
	// DefaultStatusRefresh is how often a subscription refreshes the end offsets it counts
	// its lag against.
	DefaultStatusRefresh = time.Second
	// DefaultAdminTimeout is the most one admin call, commit or offset read waits.
	DefaultAdminTimeout = 10 * time.Second
	// sendTimeout is the most one publish takes before it fails; a relay's lease must be
	// longer.
	sendTimeout = 30 * time.Second
	// requestTimeout is how long a broker has to answer a produce request.
	requestTimeout = 20 * time.Second
	// sessionTimeout is how long a member may go without a heartbeat before the group
	// removes it. Short, so a crashed instance is replaced quickly.
	sessionTimeout = 10 * time.Second
	// rebalanceTimeout is how long a member has to rejoin a group after a rebalance began.
	rebalanceTimeout = 30 * time.Second
	// fetchMaxWait bounds one poll with no records; a change cancels the poll, so this only
	// bounds how long an idle fetch waits at the broker.
	fetchMaxWait = 250 * time.Millisecond
	// fetchMaxBytes bounds one fetch, and fetchMaxPartitionBytes one partition of it.
	fetchMaxBytes          = 1 << 20
	fetchMaxPartitionBytes = 1 << 20
)

// Config is how the store reaches Kafka.
type Config struct {
	// BootstrapServers is the broker list as `host:port` entries, comma separated. It is
	// required.
	BootstrapServers string
	// Options are extra franz-go options for every client the store creates (TLS, SASL, a
	// logger, ...). Use them for the connection: the store sets the group, the consume topic,
	// the reset offset, the group protocol, the partitioner and the commit behavior itself.
	Options []kgo.Opt
	// MaxPollRecords is the most records one poll hands a subscription (default 100, at
	// least 1).
	MaxPollRecords int
	// StatusRefresh is how often a subscription refreshes the end offsets and the group's
	// committed offsets that SegmentStatus reports (default 1 s).
	StatusRefresh time.Duration
	// AdminTimeout is the most one admin call, commit or offset read waits (default 10 s).
	AdminTimeout time.Duration
}

// FromEnv returns a Config from the environment: SNODRA_KAFKA_BOOTSTRAP (a `host:port` list,
// default `kafka:19092`). The other fields keep their defaults; set them on the returned value.
func FromEnv() Config {
	bootstrap := strings.TrimSpace(os.Getenv("SNODRA_KAFKA_BOOTSTRAP"))
	if bootstrap == "" {
		bootstrap = "kafka:19092"
	}
	return Config{BootstrapServers: bootstrap}
}

// EventStore is the Kafka event store. Construct it with New.
type EventStore struct {
	bootstrap      string
	options        []kgo.Opt
	maxPollRecords int
	statusRefresh  time.Duration
	adminTimeout   time.Duration
	producer       *kgo.Client
	adminClient    *kgo.Client
	admin          *kadm.Client
	closed         atomic.Bool
	instances      atomic.Uint64
}

// New creates the store. It does not connect: the producer, the admin client and every
// subscription connect on their first call.
//
// An empty BootstrapServers, a MaxPollRecords below 1, a StatusRefresh or AdminTimeout that is
// not positive, or options kgo refuses fail with snodra.ErrInvalid.
func New(config Config) (*EventStore, error) {
	bootstrap := strings.TrimSpace(config.BootstrapServers)
	if bootstrap == "" {
		return nil, fmt.Errorf("%w: the Kafka bootstrap servers are empty", snodra.ErrInvalid)
	}
	maxPollRecords := config.MaxPollRecords
	if maxPollRecords == 0 {
		maxPollRecords = DefaultMaxPollRecords
	}
	if maxPollRecords < 1 {
		return nil, fmt.Errorf("%w: the Kafka max poll records must be at least 1, was %d",
			snodra.ErrInvalid, maxPollRecords)
	}
	statusRefresh := config.StatusRefresh
	if statusRefresh == 0 {
		statusRefresh = DefaultStatusRefresh
	}
	if statusRefresh < 0 {
		return nil, fmt.Errorf("%w: the Kafka status refresh must be positive, was %v",
			snodra.ErrInvalid, statusRefresh)
	}
	adminTimeout := config.AdminTimeout
	if adminTimeout == 0 {
		adminTimeout = DefaultAdminTimeout
	}
	if adminTimeout < 0 {
		return nil, fmt.Errorf("%w: the Kafka admin timeout must be positive, was %v",
			snodra.ErrInvalid, adminTimeout)
	}

	options := append([]kgo.Opt(nil), config.Options...)
	producer, err := kgo.NewClient(append(options,
		kgo.SeedBrokers(bootstrap),
		// The Java client's default partitioner for keyed records: murmur2 masked positive,
		// modulo the partitions. An unkeyed record has no placement rule.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(sendTimeout),
		kgo.ProduceRequestTimeout(requestTimeout),
	)...)
	if err != nil {
		return nil, fmt.Errorf("%w: the Kafka producer: %w", snodra.ErrInvalid, err)
	}
	adminClient, err := kgo.NewClient(append(options,
		kgo.SeedBrokers(bootstrap),
		kgo.ClientID("snodra-messaging-admin"),
	)...)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("%w: the Kafka admin client: %w", snodra.ErrInvalid, err)
	}
	return &EventStore{
		bootstrap:      bootstrap,
		options:        options,
		maxPollRecords: maxPollRecords,
		statusRefresh:  statusRefresh,
		adminTimeout:   adminTimeout,
		producer:       producer,
		adminClient:    adminClient,
		admin:          kadm.NewClient(adminClient),
	}, nil
}

// Name returns "kafka".
func (s *EventStore) Name() string { return Name }

// Capabilities returns replay, lag and native-dead-letter, in protocol order. The Go library
// has no event browser, so browse is not declared.
func (s *EventStore) Capabilities() []snodra.Capability {
	return []snodra.Capability{
		snodra.CapabilityReplay,
		snodra.CapabilityLag,
		snodra.CapabilityNativeDeadLetter,
	}
}

// Publish appends event to the topic named by its source and returns once the brokers
// acknowledged it (acks=all; idempotent unless Config.Options disables it). A blank source, an
// oversized record, an invalid topic or a refused authorization is snodra.ErrRejected (the outbox
// relay gives it up); any other failure is snodra.ErrFailed (another try may succeed).
func (s *EventStore) Publish(ctx context.Context, event snodra.OutgoingEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return snodra.ErrClosed
	}
	if strings.TrimSpace(event.Source) == "" {
		return fmt.Errorf("%w: an event needs a source", snodra.ErrRejected)
	}
	record := &kgo.Record{
		Topic:   event.Source,
		Key:     eventKey(event.Key),
		Value:   event.Payload,
		Headers: recordHeaders(event.Headers),
	}
	results := s.producer.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return publishError(err)
	}
	return nil
}

// Subscribe joins spec as one more instance of its subscription: a consumer of the group
// spec.Name reading the topic spec.Source. The group joins in the background; the first Next
// yields the first assignment (an empty one when the instance holds no partition), within
// snodra.FirstAssignmentWithin of joining when the source exists.
func (s *EventStore) Subscribe(ctx context.Context, spec snodra.SubscriptionSpec) (snodra.EventSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, snodra.ErrClosed
	}
	if spec.Name == "" || spec.Source == "" {
		return nil, fmt.Errorf("%w: a subscription needs a name and a source", snodra.ErrInvalid)
	}
	// A source that does not exist yet is not refused: the consumer waits for the topic's
	// metadata and joins the group when the topic appears, as the reference subscription's
	// poll does. Until then Next yields no assignment. The core's level-2 read loop has no
	// first-assignment bound, so a source that never appears leaves the processor waiting with
	// no error, and only the kit's joining check reports it.
	//
	// TODO(spec-gap): SG-37 (a source that does not exist at Subscribe; the published 10 s
	// first-assignment rule cannot hold for a source that never appears).
	subscription := &subscription{
		store:      s,
		spec:       spec,
		topic:      spec.Source,
		group:      spec.Name,
		partitions: make(map[int32]*partitionState),
		rewind:     make(map[int32]int64),
		wake:       make(chan struct{}, 1),
		refresh:    make(chan struct{}, 1),
	}
	subscription.clientID = fmt.Sprintf("snodra-%s-%d", spec.Name, s.instances.Add(1))
	if len(subscription.clientID) > 255 {
		subscription.clientID = subscription.clientID[:255]
	}
	options := append([]kgo.Opt(nil), s.options...)
	client, err := kgo.NewClient(append(options,
		kgo.SeedBrokers(s.bootstrap),
		kgo.ClientID(subscription.clientID),
		kgo.ConsumerGroup(spec.Name),
		kgo.ConsumeTopics(spec.Source),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// The classic group protocol with the range assignor: the members of every official
		// library meet in one group.
		kgo.GroupProtocol("consumer"),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(sessionTimeout),
		kgo.RebalanceTimeout(rebalanceTimeout),
		kgo.FetchMaxWait(fetchMaxWait),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxPartitionBytes(fetchMaxPartitionBytes),
		kgo.OnPartitionsAssigned(subscription.assigned),
		kgo.OnPartitionsRevoked(subscription.revoked),
		kgo.OnPartitionsLost(subscription.lost),
		kgo.WithHooks(subscription),
	)...)
	if err != nil {
		return nil, fmt.Errorf("%w: the Kafka consumer of %s: %w", snodra.ErrInvalid, spec.Name, err)
	}
	subscription.client = client
	ctx, subscription.stop = context.WithCancel(context.Background())
	go subscription.refreshLoop(ctx)
	return subscription, nil
}

// Admin returns the reset side of spec (capability replay).
func (s *EventStore) Admin(spec snodra.SubscriptionSpec) (snodra.SubscriptionAdmin, error) {
	if s.closed.Load() {
		return nil, snodra.ErrClosed
	}
	if spec.Name == "" || spec.Source == "" {
		return nil, fmt.Errorf("%w: a subscription needs a name and a source", snodra.ErrInvalid)
	}
	return &admin{store: s, group: spec.Name, topic: spec.Source}, nil
}

// Compare orders two offsets: decimal offsets by value, text that is not an offset after every
// offset (by its bytes among themselves). Compare has no error to report for a position that is
// not an offset.
//
// TODO(spec-gap): SG-38 (a position that is not an offset).
func (s *EventStore) Compare(segment, a, b string) int { return comparePositions(a, b) }

// Close releases the producer and the admin client. It does not close subscriptions: the caller
// closes each one. Close is idempotent.
func (s *EventStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.producer.Close()
	s.adminClient.Close()
	return nil
}

// eventKey is the record key of a publish: the event's key as bytes, or no key at all. An empty
// key is a key (murmur2 hashes it like any other), not the absence of one.
func eventKey(key *string) []byte {
	if key == nil {
		return nil
	}
	return []byte(*key)
}

// recordHeaders is the record's headers, in a deterministic order: the other headers sorted by
// name, then type, id, timestamp and source. The wire page says a reader looks a header up by its
// name, never by its place.
func recordHeaders(headers map[string]string) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	trailing := [...]string{
		snodra.HeaderType,
		snodra.HeaderID,
		snodra.HeaderTimestamp,
		snodra.HeaderSource,
	}
	isTrailing := func(name string) bool {
		for _, trailingName := range trailing {
			if name == trailingName {
				return true
			}
		}
		return false
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		if !isTrailing(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range trailing {
		if _, ok := headers[name]; ok {
			names = append(names, name)
		}
	}
	record := make([]kgo.RecordHeader, 0, len(names))
	for _, name := range names {
		record = append(record, kgo.RecordHeader{Key: name, Value: []byte(headers[name])})
	}
	return record
}

// publishError classifies a produce failure: a failure no retry can fix is ErrRejected (the
// record is given up), anything else ErrFailed.
func publishError(err error) error {
	permanent := [...]error{
		kerr.MessageTooLarge,
		kerr.InvalidRecord,
		kerr.InvalidTopicException,
		kerr.RecordListTooLarge,
		kerr.InvalidRequiredAcks,
		kerr.TopicAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.InvalidRequest,
		kerr.UnsupportedForMessageFormat,
		kerr.InvalidTimestamp,
		kerr.PolicyViolation,
		kerr.UnsupportedCompressionType,
	}
	for _, refused := range permanent {
		if errors.Is(err, refused) {
			return fmt.Errorf("%w: %w", snodra.ErrRejected, err)
		}
	}
	return fmt.Errorf("%w: %w", snodra.ErrFailed, err)
}

// A partitionState is what a subscription tracks of one assigned partition.
type partitionState struct {
	// position is the next offset to deliver; -1 until known.
	position int64
	// acked is the offset after the last acknowledgement, what this subscription commits;
	// -1 when it acknowledged nothing.
	acked int64
	// committed is the group's committed offset, as last read from the broker; -1 when none.
	committed int64
	// head is the offset of the next record appended, as last read; -1 when unknown.
	head int64
	// leaderEpoch is the leader epoch of the last delivered record, for a seek.
	leaderEpoch int32
}

func newPartitionState() *partitionState {
	return &partitionState{position: -1, acked: -1, committed: -1, head: -1, leaderEpoch: -1}
}

// A pendingRecord is one fetched record, copied out of franz-go's fetch buffers so it stays
// valid while the subscription waits.
type pendingRecord struct {
	partition   int32
	offset      int64
	leaderEpoch int32
	key         *string
	headers     map[string]string
	payload     []byte
	timestampMs int64
}

// subscription is one instance's subscription: a consumer group member, the partitions it holds
// and the records it fetched but did not hand out yet.
type subscription struct {
	store    *EventStore
	spec     snodra.SubscriptionSpec
	topic    string
	group    string
	clientID string
	client   *kgo.Client
	stop     context.CancelFunc

	mu         sync.Mutex
	partitions map[int32]*partitionState
	pending    []pendingRecord
	// rewind is where a redelivery seeks a partition back to, until that record comes again.
	rewind  map[int32]int64
	changes []snodra.SubscriptionEvent
	paused  bool
	closed  bool
	// lastError is the latest trouble recovered from, until TakeError.
	lastError string
	wake      chan struct{}
	refresh   chan struct{}
	// pollCancel cancels the poll Next is waiting in when a change must be surfaced; a poll
	// with no records waits until a record arrives, so a callback cannot otherwise wake it.
	pollCancel context.CancelFunc
}

// assigned is the consumer's OnPartitionsAssigned callback: it announces the partitions, an
// empty assignment included.
func (s *subscription) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	parts := sortedPartitions(assigned[s.topic])
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	for _, partition := range parts {
		if s.partitions[partition] == nil {
			s.partitions[partition] = newPartitionState()
		}
	}
	s.changes = append(s.changes, snodra.Assigned{Segments: segmentNames(parts)})
	s.signal()
	s.mu.Unlock()
	s.signalRefresh()
}

// revoked is the consumer's OnPartitionsRevoked callback: it commits what was acknowledged of
// the partitions and gives them up before announcing the revocation. An empty revocation (the
// end of a session) is not a change.
func (s *subscription) revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	parts := sortedPartitions(revoked[s.topic])
	if len(parts) == 0 {
		return
	}
	commits := s.giveUp(parts)
	if len(commits) > 0 {
		_ = s.commit(commits)
	}
}

// lost is the consumer's OnPartitionsLost callback: the partitions are gone without a chance to
// commit; what was not acknowledged comes again.
func (s *subscription) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	parts := sortedPartitions(lost[s.topic])
	if len(parts) == 0 {
		return
	}
	s.giveUp(parts)
}

// giveUp removes the partitions from the subscription, drops what was fetched for them and
// announces the revocation. It returns the offsets to commit for them.
func (s *subscription) giveUp(parts []int32) map[int32]kgo.EpochOffset {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	commits := make(map[int32]kgo.EpochOffset, len(parts))
	revoked := make(map[int32]bool, len(parts))
	for _, partition := range parts {
		revoked[partition] = true
		if state := s.partitions[partition]; state != nil && state.acked >= 0 {
			commits[partition] = kgo.EpochOffset{Offset: state.acked, Epoch: state.leaderEpoch}
		}
		delete(s.partitions, partition)
		delete(s.rewind, partition)
	}
	if len(s.pending) > 0 {
		kept := s.pending[:0]
		for _, record := range s.pending {
			if !revoked[record.partition] {
				kept = append(kept, record)
			}
		}
		s.pending = kept
	}
	if len(parts) > 0 {
		s.changes = append(s.changes, snodra.Revoked{Segments: segmentNames(parts)})
		s.signal()
	}
	s.mu.Unlock()
	return commits
}

// OnGroupManageError keeps the latest group error for the processor's status; franz-go retries
// the membership by itself.
func (s *subscription) OnGroupManageError(err error) {
	s.setError("kafka group: " + err.Error())
}

// Next waits for the next assignment change or delivery.
func (s *subscription) Next(ctx context.Context) (snodra.SubscriptionEvent, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, snodra.ErrClosed
		}
		if len(s.changes) > 0 {
			event := s.changes[0]
			s.changes = s.changes[1:]
			s.mu.Unlock()
			return event, nil
		}
		if !s.paused {
			record, delivered, seekPartition, seekOffset := s.takePendingLocked()
			if delivered {
				s.mu.Unlock()
				return record.delivery(), nil
			}
			if seekPartition >= 0 {
				s.mu.Unlock()
				s.seek(seekPartition, seekOffset)
				continue
			}
		}
		waiting := s.paused || len(s.pending) > 0
		var pollCtx context.Context
		var cancelPoll context.CancelFunc
		if !waiting {
			pollCtx, cancelPoll = context.WithCancel(ctx)
			s.pollCancel = cancelPoll
		}
		s.mu.Unlock()
		if !waiting {
			fetches := s.client.PollRecords(pollCtx, s.store.maxPollRecords)
			cancelPoll()
			s.mu.Lock()
			s.pollCancel = nil
			s.mu.Unlock()
			s.absorb(fetches)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if fetches.IsClientClosed() {
				return nil, snodra.ErrClosed
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.wake:
		}
	}
}

// takePendingLocked takes the next record to deliver, dropping records a rebalance or a
// redelivery made stale. A partition sought back to a redelivered record reports the seek for
// Next to make outside the lock.
func (s *subscription) takePendingLocked() (pendingRecord, bool, int32, int64) {
	for len(s.pending) > 0 {
		record := s.pending[0]
		s.pending = s.pending[1:]
		state := s.partitions[record.partition]
		if state == nil {
			continue // the partition changed hands after the fetch
		}
		if target, rewound := s.rewind[record.partition]; rewound {
			switch {
			case record.offset < target:
				continue // fetched before the seek took effect
			case record.offset > target:
				// An old fetch overtook the sought record: seek again and drop it.
				return pendingRecord{}, false, record.partition, target
			default:
				delete(s.rewind, record.partition)
			}
		}
		if state.position >= 0 && record.offset < state.position {
			continue // a duplicate from an old fetch
		}
		state.position = record.offset + 1
		if record.leaderEpoch >= 0 {
			state.leaderEpoch = record.leaderEpoch
		}
		return record, true, -1, 0
	}
	return pendingRecord{}, false, -1, 0
}

// absorb copies the records of a poll out of franz-go's buffers and keeps the recoverable
// errors for the status. The caller checks the context and the client's closure.
func (s *subscription) absorb(fetches kgo.Fetches) {
	var problems []string
	for _, fetchError := range fetches.Errors() {
		if errors.Is(fetchError.Err, context.Canceled) ||
			errors.Is(fetchError.Err, context.DeadlineExceeded) ||
			errors.Is(fetchError.Err, kgo.ErrClientClosed) {
			continue
		}
		problems = append(problems,
			fmt.Sprintf("%s/%d: %v", fetchError.Topic, fetchError.Partition, fetchError.Err))
	}
	var records []pendingRecord
	fetches.EachRecord(func(record *kgo.Record) {
		records = append(records, copyRecord(record))
	})
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if len(records) > 0 {
		s.pending = append(s.pending, records...)
		s.signal()
	}
	if len(problems) > 0 {
		s.lastError = "kafka: " + strings.Join(problems, "; ")
	}
	s.mu.Unlock()
}

// Ack acknowledges the delivery at position of segment and every earlier one: the group commits
// the offset after position. An acknowledgement of a segment this subscription no longer holds
// is dropped (SG-27).
func (s *subscription) Ack(segment, position string) error {
	partition, err := parseSegment(segment)
	if err != nil {
		return err
	}
	offset, err := committedOffset(position)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return snodra.ErrClosed
	}
	state := s.partitions[partition]
	if state == nil {
		s.mu.Unlock()
		return nil
	}
	if state.acked >= offset {
		s.mu.Unlock()
		return nil
	}
	state.acked = offset
	epoch := state.leaderEpoch
	s.mu.Unlock()
	return s.commit(map[int32]kgo.EpochOffset{partition: {Offset: offset, Epoch: epoch}})
}

// Redeliver hands the delivery at position of segment and the segment's later records out again
// without leaving the subscription. A position at or after the subscription's next one changes
// nothing (SG-28).
func (s *subscription) Redeliver(segment, position string) error {
	partition, err := parseSegment(segment)
	if err != nil {
		return err
	}
	offset, err := parsePosition(position)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return snodra.ErrClosed
	}
	state := s.partitions[partition]
	if state == nil {
		s.mu.Unlock()
		return nil
	}
	if state.position >= 0 && offset >= state.position {
		s.mu.Unlock()
		return nil
	}
	state.position = offset
	s.rewind[partition] = offset
	if len(s.pending) > 0 {
		kept := s.pending[:0]
		for _, record := range s.pending {
			if record.partition != partition || record.offset < offset {
				kept = append(kept, record)
			}
		}
		s.pending = kept
	}
	s.signal()
	s.mu.Unlock()
	s.seek(partition, offset)
	return nil
}

// seek moves a partition's fetch position back. It is called from the goroutine that drives the
// subscription, as franz-go asks.
func (s *subscription) seek(partition int32, offset int64) {
	s.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		s.topic: {partition: {Offset: offset, Epoch: -1}},
	})
}

// Pause stops delivering without losing the place: Next gives assignment changes only, and a
// partition assigned while paused is not fetched. It is idempotent.
func (s *subscription) Pause() {
	s.mu.Lock()
	s.paused = true
	s.signal()
	s.mu.Unlock()
}

// Resume delivers again from where the pause stopped. It is idempotent.
func (s *subscription) Resume() {
	s.mu.Lock()
	s.paused = false
	s.signal()
	s.mu.Unlock()
}

// Segments is the assigned segments, in segment order, with their positions, committed offsets
// and lag as last known. It never blocks.
func (s *subscription) Segments() []snodra.SegmentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	partitions := make([]int32, 0, len(s.partitions))
	for partition := range s.partitions {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	statuses := make([]snodra.SegmentStatus, 0, len(partitions))
	for _, partition := range partitions {
		state := s.partitions[partition]
		status := snodra.SegmentStatus{Segment: positionOf(int64(partition)), Lag: -1}
		position := state.position
		if position < 0 {
			position = state.committed
		}
		if position >= 0 {
			status.Position = positionOf(position)
		}
		committed := state.acked
		if state.committed > committed {
			committed = state.committed
		}
		base := committed
		if base < 0 {
			base = state.position
		}
		if committed >= 0 {
			status.Committed = positionOf(committed)
		}
		if state.head >= 0 {
			status.Head = positionOf(state.head)
			if base >= 0 {
				status.Lag = state.head - base
				if status.Lag < 0 {
					status.Lag = 0
				}
			}
		}
		statuses = append(statuses, status)
	}
	return statuses
}

// TakeError returns the latest trouble the subscription recovered from by itself, and clears it.
func (s *subscription) TakeError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	text := s.lastError
	s.lastError = ""
	return text
}

// Close leaves the group after flushing what was acknowledged. It is idempotent.
func (s *subscription) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	commits := make(map[int32]kgo.EpochOffset, len(s.partitions))
	for partition, state := range s.partitions {
		if state.acked >= 0 {
			commits[partition] = kgo.EpochOffset{Offset: state.acked, Epoch: state.leaderEpoch}
		}
	}
	s.signal()
	s.mu.Unlock()
	if s.stop != nil {
		s.stop()
	}
	var err error
	if len(commits) > 0 {
		// Commit with the caller's bound, but leave the group even when it expired.
		err = s.commitContext(ctx, commits)
	}
	s.client.Close()
	return err
}

// commit commits the acknowledged offsets with the store's admin timeout.
func (s *subscription) commit(offsets map[int32]kgo.EpochOffset) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.store.adminTimeout)
	defer cancel()
	return s.commitContext(ctx, offsets)
}

// commitContext commits the offsets and waits for the brokers. A failure is the processor's
// status; what was not committed comes again.
func (s *subscription) commitContext(ctx context.Context, offsets map[int32]kgo.EpochOffset) error {
	if len(offsets) == 0 {
		return nil
	}
	results := make(chan error, 1)
	s.client.CommitOffsetsSync(ctx,
		map[string]map[int32]kgo.EpochOffset{s.topic: offsets},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
			if err == nil && response != nil {
				for _, topic := range response.Topics {
					for _, partition := range topic.Partitions {
						if code := kerr.ErrorForCode(partition.ErrorCode); code != nil {
							err = code
						}
					}
				}
			}
			results <- err
		})
	select {
	case err := <-results:
		if err != nil {
			s.setError("kafka commit: " + err.Error())
			return fmt.Errorf("%w: the Kafka commit: %w", snodra.ErrFailed, err)
		}
		return nil
	default:
		// CommitOffsetsSync waits for the callback; this is only a safety net.
		return nil
	}
}

// refreshLoop refreshes what Segments reports until the subscription closes.
func (s *subscription) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(s.store.statusRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.refresh:
		}
		s.refreshNow(ctx)
	}
}

// refreshNow reads the end offsets and the group's committed offsets, without blocking the
// subscription, and updates what Segments reports.
func (s *subscription) refreshNow(parent context.Context) {
	s.mu.Lock()
	if s.closed || len(s.partitions) == 0 {
		s.mu.Unlock()
		return
	}
	partitions := make([]int32, 0, len(s.partitions))
	for partition := range s.partitions {
		partitions = append(partitions, partition)
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(parent, s.store.adminTimeout)
	defer cancel()
	if ends, err := s.store.admin.ListEndOffsets(ctx, s.topic); err == nil {
		s.mu.Lock()
		for _, partition := range partitions {
			if state := s.partitions[partition]; state != nil {
				if end, ok := listedOffset(ends, s.topic, partition); ok {
					state.head = end
				}
			}
		}
		s.mu.Unlock()
	} else if parent.Err() == nil {
		s.setError("kafka end offsets: " + err.Error())
	}
	committed, err := s.store.admin.FetchOffsetsForTopics(ctx, s.group, s.topic)
	if err != nil {
		if !errors.Is(err, kerr.GroupIDNotFound) && parent.Err() == nil {
			s.setError("kafka committed offsets: " + err.Error())
		}
		return
	}
	s.mu.Lock()
	for _, partition := range partitions {
		if state := s.partitions[partition]; state != nil {
			if response, ok := committed.Lookup(s.topic, partition); ok && response.Err == nil && response.At >= 0 {
				state.committed = response.At
			}
		}
	}
	s.mu.Unlock()
}

// signalRefresh asks the refresher to read now.
func (s *subscription) signalRefresh() {
	select {
	case s.refresh <- struct{}{}:
	default:
	}
}

// signal wakes a Next waiting on the subscription, including one waiting in a poll with no
// records: a callback cannot otherwise surface its change until a record arrives.
func (s *subscription) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
	if s.pollCancel != nil {
		s.pollCancel()
	}
}

// setError keeps the latest recovered trouble for TakeError.
func (s *subscription) setError(text string) {
	s.mu.Lock()
	if !s.closed {
		s.lastError = text
	}
	s.mu.Unlock()
}

// delivery is the record as the core reads it.
func (r pendingRecord) delivery() snodra.Delivery {
	return snodra.Delivery{
		Segment:     positionOf(int64(r.partition)),
		Position:    positionOf(r.offset),
		Key:         r.key,
		Headers:     r.headers,
		Payload:     r.payload,
		TimestampMs: r.timestampMs,
	}
}

// copyRecord copies a fetched record out of franz-go's buffer.
func copyRecord(record *kgo.Record) pendingRecord {
	var key *string
	if record.Key != nil {
		value := string(record.Key)
		key = &value
	}
	var headers map[string]string
	if len(record.Headers) > 0 {
		headers = make(map[string]string, len(record.Headers))
		for _, header := range record.Headers {
			headers[header.Key] = string(header.Value)
		}
	}
	timestampMs := int64(0)
	if !record.Timestamp.IsZero() {
		timestampMs = record.Timestamp.UnixMilli()
	}
	return pendingRecord{
		partition:   record.Partition,
		offset:      record.Offset,
		leaderEpoch: record.LeaderEpoch,
		key:         key,
		headers:     headers,
		payload:     append([]byte(nil), record.Value...),
		timestampMs: timestampMs,
	}
}

// sortedPartitions sorts partition numbers.
func sortedPartitions(partitions []int32) []int32 {
	sorted := append([]int32(nil), partitions...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

// admin is the reset side of one subscription's group.
type admin struct {
	store *EventStore
	group string
	topic string
}

// Members returns the group's instances, by client id. A group that never existed has none.
func (a *admin) Members(ctx context.Context) ([]string, error) {
	ctx, cancel := a.bound(ctx)
	defer cancel()
	return a.members(ctx)
}

// members is Members without a new timeout.
func (a *admin) members(ctx context.Context) ([]string, error) {
	groups, err := a.store.admin.DescribeGroups(ctx, a.group)
	if err != nil {
		return nil, fmt.Errorf("%w: describing group %s: %w", snodra.ErrFailed, a.group, err)
	}
	described, ok := groups[a.group]
	if !ok {
		return []string{}, nil
	}
	if described.Err != nil {
		if errors.Is(described.Err, kerr.GroupIDNotFound) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("%w: describing group %s: %w", snodra.ErrFailed, a.group, described.Err)
	}
	members := make([]string, 0, len(described.Members))
	for _, member := range described.Members {
		id := member.ClientID
		if id == "" && member.InstanceID != nil {
			id = *member.InstanceID
		}
		if id == "" {
			id = member.MemberID
		}
		if id != "" {
			members = append(members, id)
		}
	}
	return members, nil
}

// Plan says where target moves each partition, from the topic's offsets and the group's
// committed ones, without moving anything.
func (a *admin) Plan(ctx context.Context, target snodra.ResetTarget) ([]snodra.ReplayRange, error) {
	ctx, cancel := a.bound(ctx)
	defer cancel()
	partitions, err := a.partitions(ctx)
	if err != nil {
		return nil, err
	}
	ends, err := a.ends(ctx, partitions)
	if err != nil {
		return nil, err
	}
	targets, err := a.targets(ctx, partitions, ends, target)
	if err != nil {
		return nil, err
	}
	committed, err := a.committed(ctx, partitions)
	if err != nil {
		return nil, err
	}
	ranges := make([]snodra.ReplayRange, 0, len(targets))
	for _, move := range targets {
		replay := snodra.ReplayRange{
			Source:  a.topic,
			Segment: positionOf(int64(move.partition)),
			From:    positionOf(move.offset),
		}
		if before, ok := committed[move.partition]; ok {
			replay.Until = positionOf(before)
		}
		ranges = append(ranges, replay)
	}
	sort.Slice(ranges, func(i, j int) bool {
		return snodra.SegmentOrder(ranges[i].Segment, ranges[j].Segment) < 0
	})
	return ranges, nil
}

// Apply moves each range's segment to its From, all or nothing. It refuses while an instance
// reads the subscription.
func (a *admin) Apply(ctx context.Context, ranges []snodra.ReplayRange) error {
	if len(ranges) == 0 {
		return nil
	}
	ctx, cancel := a.bound(ctx)
	defer cancel()
	partitions, err := a.partitions(ctx)
	if err != nil {
		return err
	}
	known := make(map[int32]bool, len(partitions))
	for _, partition := range partitions {
		known[partition] = true
	}
	ends, err := a.ends(ctx, partitions)
	if err != nil {
		return err
	}
	moves := make(map[int32]int64, len(ranges))
	for _, replay := range ranges {
		if replay.Source != a.topic {
			return fmt.Errorf("%w: a range of %s for the subscription of %s",
				snodra.ErrInvalid, replay.Source, a.topic)
		}
		partition, err := parseSegment(replay.Segment)
		if err != nil {
			return err
		}
		if !known[partition] {
			return fmt.Errorf("%w: %s has no segment %s", snodra.ErrInvalid, a.topic, replay.Segment)
		}
		offset, err := parsePosition(replay.From)
		if err != nil {
			return err
		}
		if end, ok := ends[partition]; ok && offset > end {
			return fmt.Errorf("%w: position %d is past the head of segment %d (%d)",
				snodra.ErrInvalid, offset, partition, end)
		}
		if _, duplicate := moves[partition]; duplicate {
			return fmt.Errorf("%w: segment %d is moved twice", snodra.ErrInvalid, partition)
		}
		moves[partition] = offset
	}
	members, err := a.members(ctx)
	if err != nil {
		return err
	}
	if len(members) > 0 {
		return &snodra.SubscribersActiveError{Members: members}
	}
	offsets := make(kadm.Offsets, 1)
	for partition, offset := range moves {
		offsets.Add(kadm.Offset{Topic: a.topic, Partition: partition, At: offset, LeaderEpoch: -1})
	}
	responses, err := a.store.admin.CommitOffsets(ctx, a.group, offsets)
	if err != nil {
		return adminCommitError(a.group, err)
	}
	for _, response := range responses {
		for _, partition := range response {
			if partition.Err != nil {
				return adminCommitError(a.group, partition.Err)
			}
		}
	}
	return nil
}

// partitions lists the topic's partition numbers, sorted, or fails with ErrInvalid when the
// topic does not exist.
func (a *admin) partitions(ctx context.Context) ([]int32, error) {
	topics, err := a.store.admin.ListTopics(ctx, a.topic)
	if err != nil {
		return nil, fmt.Errorf("%w: describing %s: %w", snodra.ErrFailed, a.topic, err)
	}
	detail, ok := topics[a.topic]
	if !ok || detail.Err != nil {
		return nil, fmt.Errorf("%w: %s does not exist", snodra.ErrInvalid, a.topic)
	}
	partitions := make([]int32, 0, len(detail.Partitions))
	for partition := range detail.Partitions {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	if len(partitions) == 0 {
		return nil, fmt.Errorf("%w: %s has no partitions", snodra.ErrInvalid, a.topic)
	}
	return partitions, nil
}

// ends reads the next offset appended to each partition.
func (a *admin) ends(ctx context.Context, partitions []int32) (map[int32]int64, error) {
	listed, err := a.store.admin.ListEndOffsets(ctx, a.topic)
	if err != nil {
		return nil, fmt.Errorf("%w: the end offsets of %s: %w", snodra.ErrFailed, a.topic, err)
	}
	ends := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		offset, ok, err := listedOffsetChecked(listed, a.topic, partition)
		if err != nil {
			return nil, fmt.Errorf("%w: the end offset of %s/%d: %w",
				snodra.ErrFailed, a.topic, partition, err)
		}
		if ok {
			ends[partition] = offset
		}
	}
	return ends, nil
}

// starts reads the first offset each partition still has.
func (a *admin) starts(ctx context.Context, partitions []int32) (map[int32]int64, error) {
	listed, err := a.store.admin.ListStartOffsets(ctx, a.topic)
	if err != nil {
		return nil, fmt.Errorf("%w: the start offsets of %s: %w", snodra.ErrFailed, a.topic, err)
	}
	starts := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		offset, ok, err := listedOffsetChecked(listed, a.topic, partition)
		if err != nil {
			return nil, fmt.Errorf("%w: the start offset of %s/%d: %w",
				snodra.ErrFailed, a.topic, partition, err)
		}
		if ok {
			starts[partition] = offset
		}
	}
	return starts, nil
}

// after reads the first offset at or after millisecond; a partition with no such record keeps
// the end offset the caller has.
func (a *admin) after(ctx context.Context, millisecond int64) (map[int32]int64, error) {
	listed, err := a.store.admin.ListOffsetsAfterMilli(ctx, millisecond, a.topic)
	if err != nil {
		return nil, fmt.Errorf("%w: the offsets of %s after %d: %w",
			snodra.ErrFailed, a.topic, millisecond, err)
	}
	offsets := make(map[int32]int64, len(listed[a.topic]))
	for partition := range listed[a.topic] {
		offset, ok, err := listedOffsetChecked(listed, a.topic, partition)
		if err != nil {
			return nil, fmt.Errorf("%w: the offset of %s/%d after %d: %w",
				snodra.ErrFailed, a.topic, partition, millisecond, err)
		}
		if ok {
			offsets[partition] = offset
		}
	}
	return offsets, nil
}

// committed reads the group's committed positions, ignoring a group that never committed.
func (a *admin) committed(ctx context.Context, partitions []int32) (map[int32]int64, error) {
	responses, err := a.store.admin.FetchOffsetsForTopics(ctx, a.group, a.topic)
	if err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			return map[int32]int64{}, nil
		}
		return nil, fmt.Errorf("%w: the committed offsets of %s: %w",
			snodra.ErrFailed, a.group, err)
	}
	committed := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		response, ok := responses.Lookup(a.topic, partition)
		if !ok || response.Err != nil || response.At < 0 {
			continue
		}
		committed[partition] = response.At
	}
	return committed, nil
}

// A segmentTarget is one partition and the offset a reset moves it to.
type segmentTarget struct {
	partition int32
	offset    int64
}

// targets is where target moves each partition, sorted by segment.
func (a *admin) targets(ctx context.Context, partitions []int32, ends map[int32]int64,
	target snodra.ResetTarget) ([]segmentTarget, error) {
	targets := make([]segmentTarget, 0, len(partitions))
	switch target := target.(type) {
	case snodra.ResetEarliest:
		starts, err := a.starts(ctx, partitions)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			offset, ok := starts[partition]
			if !ok {
				return nil, fmt.Errorf("%w: %s/%d has no start offset",
					snodra.ErrInvalid, a.topic, partition)
			}
			targets = append(targets, segmentTarget{partition, offset})
		}
	case snodra.ResetLatest:
		for _, partition := range partitions {
			offset, ok := ends[partition]
			if !ok {
				return nil, fmt.Errorf("%w: %s/%d has no end offset",
					snodra.ErrInvalid, a.topic, partition)
			}
			targets = append(targets, segmentTarget{partition, offset})
		}
	case snodra.ResetTimestamp:
		after, err := a.after(ctx, target.TimestampMs)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			offset, ok := after[partition]
			if !ok {
				offset, ok = ends[partition]
			}
			if !ok {
				return nil, fmt.Errorf("%w: %s/%d has no offset",
					snodra.ErrInvalid, a.topic, partition)
			}
			targets = append(targets, segmentTarget{partition, offset})
		}
	case snodra.ResetPositions:
		if len(target.Positions) == 0 {
			return nil, fmt.Errorf("%w: the target lists no segment", snodra.ErrInvalid)
		}
		known := make(map[int32]bool, len(partitions))
		for _, partition := range partitions {
			known[partition] = true
		}
		wanted := make(map[int32]bool, len(target.Positions))
		for _, position := range target.Positions {
			partition, err := parseSegment(position.Segment)
			if err != nil {
				return nil, err
			}
			if !known[partition] {
				return nil, fmt.Errorf("%w: %s has no segment %s",
					snodra.ErrInvalid, a.topic, position.Segment)
			}
			offset, err := parsePosition(position.Position)
			if err != nil {
				return nil, err
			}
			if end, ok := ends[partition]; ok && offset > end {
				return nil, fmt.Errorf("%w: position %d is past the head of segment %d (%d)",
					snodra.ErrInvalid, offset, partition, end)
			}
			if wanted[partition] {
				return nil, fmt.Errorf("%w: segment %d is given twice", snodra.ErrInvalid, partition)
			}
			wanted[partition] = true
			targets = append(targets, segmentTarget{partition, offset})
		}
	default:
		return nil, fmt.Errorf("%w: unknown reset target %T", snodra.ErrInvalid, target)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].partition < targets[j].partition })
	return targets, nil
}

// bound adds the admin timeout to ctx.
func (a *admin) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.store.adminTimeout)
}

// adminCommitError classifies a refused offset commit: a group that has members is subscribers
// active, anything else a failure.
func adminCommitError(group string, err error) error {
	switch {
	case errors.Is(err, kerr.UnknownMemberID),
		errors.Is(err, kerr.IllegalGeneration),
		errors.Is(err, kerr.RebalanceInProgress),
		errors.Is(err, kerr.NonEmptyGroup):
		return &snodra.SubscribersActiveError{}
	}
	return fmt.Errorf("%w: the offsets of %s: %w", snodra.ErrFailed, group, err)
}

// listedOffset is one partition's offset from a listed answer, when it has one.
func listedOffset(listed kadm.ListedOffsets, topic string, partition int32) (int64, bool) {
	offset, ok := listed.Lookup(topic, partition)
	if !ok || offset.Err != nil || offset.Offset < 0 {
		return 0, false
	}
	return offset.Offset, true
}

// listedOffsetChecked is listedOffset with the partition's error.
func listedOffsetChecked(listed kadm.ListedOffsets, topic string, partition int32) (int64, bool, error) {
	offset, ok := listed.Lookup(topic, partition)
	if !ok || offset.Offset < 0 {
		return 0, false, offset.Err
	}
	if offset.Err != nil {
		return 0, false, offset.Err
	}
	return offset.Offset, true, nil
}

var (
	_ snodra.EventStore        = (*EventStore)(nil)
	_ snodra.EventSubscription = (*subscription)(nil)
	_ snodra.SubscriptionAdmin = (*admin)(nil)
)
