package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Capability is what an event store offers beyond the required contract. It is reported to the
// hub as ProcessorStatus.capabilities.
type Capability string

// The capabilities the protocol names. An add-on may report others next to them.
const (
	// CapabilityReplay: reset to earliest, latest, a time or explicit positions, with reset
	// handlers and replay ranges (SubscriptionAdmin).
	CapabilityReplay Capability = "replay"
	// CapabilityBrowse: an event browser can read the store. The Go library has no event
	// browser, so its add-ons do not offer one; the capability still says another program
	// may read the store.
	CapabilityBrowse Capability = "browse"
	// CapabilityLag: how far a subscription is behind, per segment (SegmentStatus.Lag).
	CapabilityLag Capability = "lag"
	// CapabilityNativeDeadLetter: a dead-letter source (DeadLetterSource) for the
	// dead-letter mode that sends what a processor cannot handle to the event store.
	CapabilityNativeDeadLetter Capability = "native-dead-letter"
)

// AllCapabilities lists the capabilities the protocol names, in protocol order.
var AllCapabilities = [...]Capability{
	CapabilityReplay,
	CapabilityBrowse,
	CapabilityLag,
	CapabilityNativeDeadLetter,
}

// String returns the capability's protocol name ("replay", ...).
func (c Capability) String() string { return string(c) }

// The headers every event carries, as the event store's native metadata.
const (
	// HeaderType is the event's type name.
	HeaderType = "type"
	// HeaderID is the event's id.
	HeaderID = "id"
	// HeaderCorrelationID ties the event to the request that caused it.
	HeaderCorrelationID = "correlationId"
	// HeaderTimestamp is when the event was recorded, RFC 3339 in UTC with milliseconds.
	HeaderTimestamp = "timestamp"
	// HeaderSource is the publishing service's name, not the event store's source.
	HeaderSource = "source"
	// HeaderUserID is the caller's user id; empty when none.
	HeaderUserID = "userId"
	// HeaderTraceparent is the W3C trace context of the message that caused the event.
	HeaderTraceparent = "traceparent"
)

// EventHeaders lists the seven headers, in the order the specification gives them.
var EventHeaders = [...]string{
	HeaderType,
	HeaderID,
	HeaderCorrelationID,
	HeaderTimestamp,
	HeaderSource,
	HeaderUserID,
	HeaderTraceparent,
}

const (
	// MaxSegmentBytes is the most bytes of a segment's name; the hub drops a longer one.
	MaxSegmentBytes = 128
	// MaxPositionBytes is the most bytes of a position; the hub reads a longer one as
	// unknown.
	MaxPositionBytes = 128
)

// FirstAssignmentWithin is the longest a new event subscription takes to yield its first
// event, an Assigned (empty when it holds no segment).
const FirstAssignmentWithin = 10 * time.Second

// SegmentOrder orders segment names as the hub lists them: names of ASCII digits only by their
// value (any length, no overflow; equal values by their text), before every other name, which
// go by their bytes. It returns a negative number when a comes first, zero when they are equal,
// and a positive number when b comes first.
func SegmentOrder(a, b string) int {
	aDecimal := decimalName(a)
	bDecimal := decimalName(b)
	switch {
	case aDecimal && bDecimal:
		x := strings.TrimLeft(a, "0")
		y := strings.TrimLeft(b, "0")
		if c := compareInts(len(x), len(y)); c != 0 {
			return c
		}
		if c := strings.Compare(x, y); c != 0 {
			return c
		}
		return strings.Compare(a, b)
	case aDecimal:
		return -1
	case bDecimal:
		return 1
	default:
		return strings.Compare(a, b)
	}
}

func decimalName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func compareInts(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// DeadLetterSource is the dead-letter source of source ("<source>.dlt"): where the dead-letter
// mode event-store sends what a processor cannot handle, on an event store declaring
// CapabilityNativeDeadLetter.
func DeadLetterSource(source string) string { return source + ".dlt" }

// OutgoingEvent is one event an outbox relay hands an event store (EventPublisher.Publish).
type OutgoingEvent struct {
	// Source is where the event goes: the service's events source, or its dead-letter source
	// (DeadLetterSource).
	Source string
	// ID is the event's id, also in header HeaderID.
	ID string
	// Type is the event's type name, also in header HeaderType.
	Type string
	// Key is the order key: events with one key reach a processor in publish order. An empty
	// key is a key; nil spreads the event as the event store does.
	Key *string
	// Headers travel as given, as the event store's native metadata: the seven
	// EventHeaders and any other (for example a dead letter's snodra-dlt-* headers).
	// Headers are UTF-8 text, one value per name.
	Headers map[string]string
	// Payload is JSON bytes, carried as given.
	Payload []byte
}

// Delivery is one event a subscription delivers. Segment and Position are opaque strings of at
// most MaxSegmentBytes and MaxPositionBytes bytes that only the event store reads (decimal
// numbers on the stores shipped today); the core orders two positions of one segment only
// through EventStore.Compare.
type Delivery struct {
	// Segment is the segment of the source it was read from: a unit of order per key.
	Segment string
	// Position is its place in Segment; EventSubscription.Ack takes it.
	Position string
	// Key is the order key as published; nil for an event without one.
	Key *string
	// Headers are the headers as published, as UTF-8 text with one value per name.
	Headers map[string]string
	// Payload is the payload as published.
	Payload []byte
	// TimestampMs is when the event store appended it, epoch milliseconds (its own clock).
	TimestampMs int64
}

// SubscriptionEvent is what EventSubscription.Next gives: an Assigned, a Revoked or a Delivery.
// The set is closed; an add-on constructs one of the three types and never implements the
// interface itself.
type SubscriptionEvent interface {
	subscriptionEvent()
}

// Assigned says these segments became this subscription's, in addition to the ones it has. A
// new subscription's first event is an Assigned, empty when it holds nothing.
type Assigned struct {
	Segments []string
}

func (Assigned) subscriptionEvent() {}

// Revoked says these segments are this subscription's no longer. What it delivered of them and
// did not acknowledge comes again to whichever subscription gets them. A Revoked is always
// followed by an Assigned (possibly empty), which ends the change.
type Revoked struct {
	Segments []string
}

func (Revoked) subscriptionEvent() {}

func (Delivery) subscriptionEvent() {}

// SubscriptionSpec names a processor's subscription. Every subscription of one Name shares the
// events of Source (competing instances), and each Name receives every event (one subscription
// per processor).
type SubscriptionSpec struct {
	// Name is "<service>.<processor>": the name the event store knows the subscription by.
	Name string
	// Source is the source read.
	Source string
}

// SegmentStatus is one segment assigned to a subscription, as EventSubscription.Segments
// reports it. Positions are as in Delivery; an empty Position, Committed or Head means unknown
// or none. Lag is -1 when unknown, never negative otherwise.
type SegmentStatus struct {
	// Segment is the segment's name.
	Segment string
	// Position is the next position this subscription delivers.
	Position string
	// Committed is the subscription's committed position, the one after its last
	// acknowledgement.
	Committed string
	// Head is the position of the next event appended to the segment.
	Head string
	// Lag is how many events lie between Committed and Head (between Position and Head when
	// nothing is committed): what is not acknowledged yet. It is -1 when unknown, and always
	// -1 without CapabilityLag.
	Lag int64
}

// ProcessedMarker is the idempotency marker of a handled event: its id
// ("processor:<name>:<eventId>", "saga:<type>:<eventId>", ...), the event's type and when it
// was handled. The stage-1 marker of a delivered event also carries its processor, source,
// segment and position, so a reset deletes exactly the markers of its replay range; every other
// marker leaves them empty. An empty Processor, Source, Segment or Position means none.
type ProcessedMarker struct {
	ID          string
	Type        string
	ProcessedAt time.Time
	Processor   string
	Source      string
	Segment     string
	Position    string
}

// ProcessedEvents records processed-event markers. It is the processed-event port of a message
// store, frozen at level 2 because the event-store kit records a marker in its at-least-once
// check; the rest of the message-store interface comes with the message store.
//
// Record records marker in the caller's transaction and reports whether it was new: false means
// the id is recorded already (a duplicate), and the caller skips the handlers. A message store
// whose transaction is bound through the context records it in that transaction; with no
// transaction in the context, the store records it in a transaction of its own.
type ProcessedEvents interface {
	Record(ctx context.Context, marker ProcessedMarker) (bool, error)
}

// EventPublisher is what an outbox relay needs of an event store: its name and the append of
// one event. EventStore embeds it.
type EventPublisher interface {
	// Name is the add-on's name ("kafka", "rabbitmq-streams", "memory" or a third party's),
	// reported to the hub as the processors' event store.
	Name() string
	// Publish appends event to event.Source and returns once the event store accepted it. Two
	// publishes of one key, the second called after the first returned, are delivered in that
	// order. It fails with ErrRejected when the event store will never take the event (the
	// relay gives it up at once), and with ErrFailed when another try may succeed. A context
	// that is done is returned as-is.
	Publish(ctx context.Context, event OutgoingEvent) error
}

// EventStore is the event log a service publishes to and its event processors read from, one
// add-on per event store. The snodratest package checks the contract below; every operation
// keeps its meaning across the stores.
//
//   - One subscription per processor: every SubscriptionSpec.Name receives every event of its
//     source, whatever other processors and services do.
//   - Competing instances: the EventSubscriptions of one name share its segments; adding one
//     spreads them.
//   - Order per key: events with the same key go to one segment and reach a processor in
//     publish order.
//   - At-least-once with acknowledgement: the core acknowledges after the handler and the
//     processed-event marker committed; what was not acknowledged comes again, and the marker
//     skips it.
//   - Pause and resume: a subscription stops delivering without losing its place.
//   - The headers: EventHeaders, and any other, travel as the event store's native metadata.
//
// A new subscription name starts at the earliest position of every segment. Segments and
// positions are opaque strings of at most MaxSegmentBytes and MaxPositionBytes bytes; Compare
// is the only way the core orders two positions of one segment. Payloads are JSON bytes, carried
// as given.
type EventStore interface {
	EventPublisher
	// Capabilities is what the store offers beyond the required contract.
	Capabilities() []Capability
	// Subscribe joins spec as one more instance of its subscription. A context that is done is
	// returned as-is.
	Subscribe(ctx context.Context, spec SubscriptionSpec) (EventSubscription, error)
	// Admin is the reset side of spec, for a reset run while no instance reads it. It fails
	// with ErrUnsupported (an *UnsupportedError naming CapabilityReplay) when the store lacks
	// CapabilityReplay.
	Admin(spec SubscriptionSpec) (SubscriptionAdmin, error)
	// Compare orders two positions a and b of segment: a negative number when a comes first,
	// zero when they are equal, and a positive number when b comes first.
	Compare(segment, a, b string) int
}

// Supports reports whether store declares capability.
func Supports(store EventStore, capability Capability) bool {
	for _, c := range store.Capabilities() {
		if c == capability {
			return true
		}
	}
	return false
}

// EventSubscription is one instance's subscription of a processor (EventStore.Subscribe). It is
// not the hub subscription query the root package's Subscription type represents.
//
//   - Next drives it: nothing is delivered until Next is called, and the core keeps calling it
//     while paused to keep the membership alive and report assignment changes.
//   - Order per segment: a segment's events come in their order, each after the Assigned naming
//     its segment and before any Revoked of it.
//   - The first event is an Assigned (an empty one when the subscription holds no segment)
//     within FirstAssignmentWithin of joining: an instance that joined and holds nothing is a
//     member, idle, which the core reports running, never starting.
//   - Every Revoked is followed by an Assigned (an empty one when nothing new comes): the
//     assignment change is over only then.
//   - At-least-once: an event is acknowledged with Ack after its handling committed; one not
//     acknowledged comes again to the next subscription of the same name that gets its segment,
//     or to this one after Redeliver.
//   - A failure ends it: an error from Next means the subscription is broken; the caller closes
//     it and subscribes again after a backoff.
//   - The core serializes Pause, Resume, Segments and TakeError with Close: once Close returns,
//     none of them is called again, and Close never runs beside one of them.
type EventSubscription interface {
	// Next waits for the next assignment change or delivery. It returns when ctx is done
	// without losing anything: dropping a pending Next loses nothing.
	Next(ctx context.Context) (SubscriptionEvent, error)
	// Ack acknowledges the delivery at position of segment and every earlier one of that
	// segment: a later subscription of this name resumes after it. It may reach the event
	// store after the call returns; Close flushes it.
	Ack(segment, position string) error
	// Redeliver hands the delivery at position of segment out again, without leaving the
	// subscription; Next delivers it and that segment's later events again. Other segments are
	// unaffected.
	Redeliver(segment, position string) error
	// Pause stops delivering without losing the place: Next gives assignment changes only, and
	// a segment assigned while paused is paused at once. It is idempotent; called before the
	// first Next, nothing is ever delivered until Resume.
	Pause()
	// Resume delivers again from where the pause stopped. It is idempotent.
	Resume()
	// Segments is the segments assigned now, in SegmentOrder, with their positions and lag as
	// last known. It never blocks.
	Segments() []SegmentStatus
	// TakeError is the latest trouble the subscription recovered from by itself since the
	// last call, as text for the processor's status; it is empty when there was none. It never
	// blocks.
	TakeError() string
	// Close leaves the subscription after flushing the acknowledgements: its segments go to
	// the other subscriptions of the name. It is idempotent.
	Close(ctx context.Context) error
}

// ResetTarget is where SubscriptionAdmin.Plan moves a subscription. The set is closed; an
// add-on switches on ResetEarliest, ResetLatest, ResetTimestamp and ResetPositions.
type ResetTarget interface {
	resetTarget()
}

// ResetEarliest moves every segment to its first position: everything the event store still has
// is a replay.
type ResetEarliest struct{}

func (ResetEarliest) resetTarget() {}

// ResetLatest moves every segment to its head: nothing is replayed.
type ResetLatest struct{}

func (ResetLatest) resetTarget() {}

// ResetTimestamp moves every segment to the first position whose event was appended at or after
// TimestampMs (epoch milliseconds, the event store's append time), else its head.
type ResetTimestamp struct {
	TimestampMs int64
}

func (ResetTimestamp) resetTarget() {}

// ResetPositions moves the listed segments to their positions. A segment not listed keeps its
// committed position.
type ResetPositions struct {
	Positions []SegmentPosition
}

func (ResetPositions) resetTarget() {}

// SegmentPosition is one segment and one position of it, for ResetPositions.
type SegmentPosition struct {
	Segment  string
	Position string
}

// ReplayRange says events of Source's Segment at positions from From (inclusive) to Until
// (exclusive) are a replay. Until is the subscription's committed position before the plan,
// empty when it had none; only a range with Until set and From before it, by
// EventStore.Compare, replays anything. The others are moves without a replay.
type ReplayRange struct {
	Source  string
	Segment string
	From    string
	Until   string
}

// SubscriptionAdmin is the reset side of one subscription (EventStore.Admin). Every call blocks
// (it talks to the event store); callers run them off their event loop, and they return when
// ctx is done.
type SubscriptionAdmin interface {
	// Members is the instances reading the subscription now, the event store's ids for them;
	// empty when none does.
	Members(ctx context.Context) ([]string, error)
	// Plan says where target moves each segment, from the event store's current positions;
	// nothing moves yet. It returns one range per segment the target addresses, sorted by
	// segment. A target position the segment does not have (a segment the source lacks, a
	// position past the head) fails with ErrInvalid.
	Plan(ctx context.Context, target ResetTarget) ([]ReplayRange, error)
	// Apply moves the committed position of each range's segment to its From (Until is not
	// read), all or nothing. It fails with ErrSubscribersActive while an instance reads the
	// subscription.
	Apply(ctx context.Context, ranges []ReplayRange) error
}

// Errors store operations report, to be tested with errors.Is. The message-store ports use
// ErrNotOwner and ErrLeaseLost for their conditional writes and leases.
var (
	// ErrUnsupported: the store does not offer the operation or the capability. An
	// *UnsupportedError names the capability.
	ErrUnsupported = errors.New("snodra: the operation is not supported")
	// ErrInvalid: a bad argument, such as an unknown segment or a position the segment does
	// not have.
	ErrInvalid = errors.New("snodra: invalid argument")
	// ErrSubscribersActive: another instance still reads the subscription, so its positions
	// cannot move. A *SubscribersActiveError names the members.
	ErrSubscribersActive = errors.New("snodra: the subscription has active subscribers")
	// ErrRejected: the event store refused the event for good (too large, not authorized, a
	// destination it cannot have). Publishing it again cannot succeed, so the outbox relay
	// gives it up at once.
	ErrRejected = errors.New("snodra: the event store rejected the event")
	// ErrFailed: the event store failed or cannot be reached; trying again may succeed.
	ErrFailed = errors.New("snodra: the event store failed")
	// ErrNotOwner: a conditional write found the claim held by another owner. The whole
	// operation is retried or given up; the caller never overwrites the owner's state.
	ErrNotOwner = errors.New("snodra: the claim is held by another owner")
	// ErrLeaseLost: the lease of a claim expired before the operation committed. What the
	// operation wrote is not the caller's any more.
	ErrLeaseLost = errors.New("snodra: the lease was lost")
	// ErrClosed: the event subscription was closed.
	ErrClosed = errors.New("snodra: the subscription is closed")
)

// UnsupportedError is the ErrUnsupported an add-on returns for a missing capability. It names
// the capability, and errors.Is finds ErrUnsupported.
type UnsupportedError struct {
	Capability Capability
}

// Error returns the unsupported message, naming the capability when there is one.
func (e *UnsupportedError) Error() string {
	if e.Capability == "" {
		return ErrUnsupported.Error()
	}
	return fmt.Sprintf("%s: %s", ErrUnsupported, e.Capability)
}

// Is reports the error as ErrUnsupported.
func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

// SubscribersActiveError is the ErrSubscribersActive a reset returns while instances read the
// subscription. It names the members, and errors.Is finds ErrSubscribersActive.
type SubscribersActiveError struct {
	Members []string
}

// Error returns the subscribers-active message, naming the members when there are some.
func (e *SubscribersActiveError) Error() string {
	if len(e.Members) == 0 {
		return ErrSubscribersActive.Error()
	}
	return ErrSubscribersActive.Error() + ": " + strings.Join(e.Members, ", ")
}

// Is reports the error as ErrSubscribersActive.
func (e *SubscribersActiveError) Is(target error) bool { return target == ErrSubscribersActive }
