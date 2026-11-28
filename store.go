package snodra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// A message store is where a service keeps what the library must not lose: the events it
// publishes (the outbox), the processed-event markers that make delivery at-least-once with
// exactly-once effects, its dead letters, the processors' control state, the records of
// idempotent external calls and the sagas. It is the service's own database, reached through one
// add-on that implements every store below, so the outbox entry and the processed-event marker
// commit in the same transaction as the service's own writes.
//
// One store per service, the service's own database. A shared database holds the instances of
// one logical service: markers, processor control, dead-letter key blocking and the claims of
// deadlines and notices carry no service name.
//
// Payloads cross these interfaces as JSON bytes (or JSON text where the stored contract is
// text), never as a database's own document format; times as time.Time, epoch milliseconds on
// the wire. A store stamps the times it sets itself (an entry's sending, a lease) with the
// application's clock, never the database's, so a lease means the same on every instance
// whatever the database's clock says.
//
// The interfaces are frozen at level 2 so a third-party add-on compiles once. An operation a
// store does not implement returns ErrUnsupported; the official add-ons implement every store by
// level 4.

// MessageStore is the add-on: every store the library uses in the service's own database.
type MessageStore interface {
	// Name is the add-on's name ("mongo", "postgres", "mysql", "memory" or a third party's),
	// for logs and the startup report.
	Name() string
	// Begin starts a transaction. The returned context carries it: Outbox.Append and
	// Processed.Record called with it join the transaction, and so do the service's own writes
	// through the add-on's own transaction helper. Commit writes everything or nothing;
	// Rollback discards it. A context that carries no transaction (context.Background(), say) is
	// the plain case: the writes run in a short transaction of their own.
	Begin(ctx context.Context) (context.Context, Tx, error)
	// Outbox is the events appended in the service's transaction and relayed afterwards.
	Outbox() OutboxStore
	// Processed is the processed-event markers, the stage-1 marker included.
	Processed() ProcessedStore
	// DeadLetters is what exhausted its attempts (the collection mode).
	DeadLetters() DeadLetterStore
	// Control is the processors' pause, reset lock and replay ranges.
	Control() ProcessorControlStore
	// ExternalCalls is the idempotency records of external calls.
	ExternalCalls() ExternalCallStore
	// Sagas is the six saga ports.
	Sagas() SagaStores
}

// StoreLifecycle is what a message store may implement besides MessageStore: the builder calls
// these methods around the store's life, and the six store ports stay as they are, so a
// third-party add-on compiles against MessageStore alone. A store that does not implement it is
// simply used as it is.
//
// [App.Start] hands the store the application's clock (UseClock), prepares its schema
// (InitSchema, with the application's [Config]), or checks a schema the service applies itself
// (CheckSchema), and then starts what the store runs in the background (Background), if it
// implements this interface. A failing InitSchema or CheckSchema fails Start; an error from
// Background while the app runs is logged.
type StoreLifecycle interface {
	// UseClock sets the application's clock: what the store stamps the times it sets itself
	// with (an outbox entry's sending, a claim's lease). The clock must not call the store.
	UseClock(now func() time.Time)
	// InitSchema prepares what the store needs in its database, idempotently: indexes (MongoDB,
	// with its retentions or TTLs), tables (SQL), nothing (memory). config is the application's
	// read-only settings. It runs when the service builds the store with schema initialization
	// on (the default).
	InitSchema(ctx context.Context, config *Config) error
	// CheckSchema fails unless the database has what the store needs, for example an SQL schema
	// at the version it reads or later. It runs instead of InitSchema when the service applies
	// the schema itself.
	CheckSchema(ctx context.Context) error
	// Background is what the store runs on its own while the app runs (for example the SQL
	// add-on's purge of what the retentions let go), until ctx is done. A store with nothing to
	// run may return at once or wait for ctx.
	Background(ctx context.Context) error
}

// Config is what a message store's [StoreLifecycle] sees of the application's configuration: the
// logical service name and the event source its processors read. It is read-only: a store copies
// what it needs and never changes it. Fields are added as settings a store uses appear (the
// retentions, for example); a store must not depend on the struct's size.
type Config struct {
	// Service is the logical service name ([Service], SNODRA_SERVICE_NAME).
	Service string
	// EventsSource is the event store's source the service publishes to and its processors read
	// ([EventsSource], SNODRA_EVENTS_SOURCE).
	EventsSource string
}

// Tx is one transaction of the service's own database. A transaction must end with Commit or
// Rollback; an operation that fails with ErrConflict is retried as a whole, in a new
// transaction.
//
// Commit writes what the transaction collected, all or nothing. It fails with ErrConflict when
// another writer got to one of its records first, and with the store's own error when the
// database failed; after a commit returns, the transaction has ended either way. Rollback
// abandons everything and never fails; a transaction that ended already is a no-op.
//
// TODO(spec-gap): SG-31 (a transaction abandoned without Commit or Rollback keeps its write
// reservations until the process ends: Go has no destructor to release them; the reference's
// in-memory store reserves a row the same way until its transaction ends,
// snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryTables.kt:77).
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// OutboxEntry is one event in the outbox.
type OutboxEntry struct {
	// ID is the event's id (its "id" header).
	ID string
	// Type is the event's wire type name.
	Type string
	// Key is the event's order key; nil for none.
	Key *string
	// Headers are the headers the entry keeps in the outbox: the correlationId, the userId and,
	// when the work that published the event carried one, the traceparent. The relay adds type,
	// id, timestamp and source from the entry, so a store never holds a stale copy of them.
	Headers map[string]string
	// Payload is the event as JSON: a JSON object whose integers fit in a signed 64-bit
	// integer, at most MaxPayloadDepth deep. A store refuses every other payload with an error
	// wrapping ErrInvalid.
	Payload []byte
	// CreatedAt is when the publishing instance created the entry (the application's clock).
	CreatedAt time.Time
	// Seq is the appending instance's counter: it breaks CreatedAt ties, so per-key order
	// holds for what one instance publishes.
	Seq int64
	// Attempts counts the relay's failed sends so far.
	Attempts int
}

// OutboxStore is the outbox: events appended in the service's transaction, relayed by whichever
// instance claims them. An entry is pending until it is sent or failed. A claim holds the
// entries under a lease, so two instances relaying one outbox never both hold an entry, and a
// crashed instance's entries are claimed again once its lease has passed.
type OutboxStore interface {
	// Append appends entry in the transaction carried by ctx, or on its own when ctx carries
	// none. An entry appended in a rolled-back transaction never reaches the relay. A payload
	// that is not a JSON object with integers that fit in a signed 64-bit integer is refused
	// (ErrInvalid) and nothing is appended; an id that exists is a conflict (ErrConflict).
	Append(ctx context.Context, entry OutboxEntry) error
	// Claim returns at most limit pending entries, now held by owner until lease from the
	// application's clock: those nobody holds, those whose holder's lease has passed, and those
	// owner holds already (its lease renewed); never an entry another owner holds. owner must
	// not be empty: an empty owner is the store's mark for "nobody holds this entry", so a
	// claimer cannot be told from it. The only order is (CreatedAt, Seq, ID): an entry owner
	// still holds comes back in its place, so an entry whose send failed is retried before the
	// later ones.
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxEntry, error)
	// MarkSent marks the entries sent: never claimed again, purged after the retention.
	MarkSent(ctx context.Context, ids []string) error
	// MarkAttemptFailed counts a failed send that will be retried: Attempts + 1; the entry
	// stays pending and held by its owner until the lease passes. cause is for diagnosis; a
	// store may keep it or not.
	MarkAttemptFailed(ctx context.Context, id, cause string) error
	// MarkFailed gives the entry up (out of attempts, or a failure no retry cures): Attempts +
	// 1, never claimed again, never purged. cause is for diagnosis; a store may keep it or not.
	MarkFailed(ctx context.Context, id, cause string) error
	// Purge removes the entries sent before sentBefore; how many.
	Purge(ctx context.Context, sentBefore time.Time) (int64, error)
	// Pending is how many entries are pending (neither sent nor failed), held or not.
	Pending(ctx context.Context) (int64, error)
	// OldestPending is the CreatedAt of the oldest pending entry (held or not); ok is false
	// when none is pending.
	OldestPending(ctx context.Context) (oldest time.Time, ok bool, err error)
	// OnAppend registers wake, called each time an append commits (on its own, or with its
	// transaction), so the relay publishes at once instead of at its next poll. The returned
	// function unregisters it and is safe to call more than once. A store that cannot tell
	// registers nothing and returns a function that does nothing; the relay then polls. wake
	// must return at once.
	OnAppend(wake func()) (cancel func())
}

// ProcessedStore is the processed-event markers: the ids of the events a handler has handled,
// recorded in its transaction, so a redelivered event is skipped (at-least-once delivery,
// exactly-once effects). A marker also records when it was handled (the retention counts from
// it); a stage-1 marker of a delivered event carries its processor, source, segment and position,
// so a reset deletes exactly the markers of its replay range, while every other marker leaves
// them empty and is never deleted by a reset.
//
// ProcessedStore satisfies ProcessedEvents: the event-store kit records its marker through it.
type ProcessedStore interface {
	ProcessedEvents
	// Exists reports whether a marker with id is committed. A marker recorded in the
	// transaction carried by ctx is not visible yet.
	Exists(ctx context.Context, id string) (bool, error)
	// DeleteMarkers deletes the stage-1 markers of processor in ranges (a range's source and
	// segment, a position in [From, Until) in the event store's order) and nothing else; how
	// many. Markers without a position are never deleted. It runs outside any transaction.
	//
	// TODO(spec-gap): SG-30 (the opaque positions are ordered as document stores keep them:
	// canonical decimals by value, other text by bytes, and never across the two).
	DeleteMarkers(ctx context.Context, processor string, ranges []ReplayRange) (int64, error)
	// Purge removes the markers processed before before; how many.
	Purge(ctx context.Context, before time.Time) (int64, error)
}

// The most characters of a dead letter's error message.
const MaxDeadLetterErrorChars = 2000

// DeadLetterReason is why a record was parked.
type DeadLetterReason string

// The stored reasons.
const (
	// DeadLetterHandlerFailed: stage 1 failed on every attempt.
	DeadLetterHandlerFailed DeadLetterReason = "HANDLER_FAILED"
	// DeadLetterSequenced: an earlier letter of the record's key was parked; stage 1 did not
	// run.
	DeadLetterSequenced DeadLetterReason = "SEQUENCED"
	// DeadLetterInvalid: the record cannot be handled (no type header, a value that is not a
	// JSON object); it blocks nothing and can only be discarded.
	DeadLetterInvalid DeadLetterReason = "INVALID"
)

// String returns the stored name ("HANDLER_FAILED", ...).
func (r DeadLetterReason) String() string { return string(r) }

// DeadLetterError is a letter's error: Type is the error variant's name and Message is cut to
// MaxDeadLetterErrorChars characters.
type DeadLetterError struct {
	Type    string
	Message string
}

// NewDeadLetterError returns the error with message cut to MaxDeadLetterErrorChars characters.
func NewDeadLetterError(typeName, message string) DeadLetterError {
	return DeadLetterError{Type: typeName, Message: cutChars(message, MaxDeadLetterErrorChars)}
}

// SequencedDeadLetterError is a SEQUENCED letter's error, naming the key's head letter.
func SequencedDeadLetterError(headID string) DeadLetterError {
	return NewDeadLetterError("Sequenced", "an earlier letter for this key is parked: "+headID)
}

// InvalidDeadLetterError is an INVALID letter's error.
func InvalidDeadLetterError(message string) DeadLetterError {
	return NewDeadLetterError("InvalidMessage", message)
}

// cutChars cuts text to at most max characters, never inside one.
func cutChars(text string, max int) string {
	if max < 0 {
		return ""
	}
	count := 0
	for i := range text {
		if count == max {
			return text[:i]
		}
		count++
	}
	return text
}

// DeadLetter is one parked record. ID is "<processor>:<source>:<segment>:<position>"
// (LetterID), so parking a redelivered record finds the first letter. Segment and Position are
// the event store's opaque strings (decimal numbers for the stores shipped today).
//
// An empty EventID, EventType, BlockedBy or Key means none; Payload is the record's value byte
// for byte, JSON or anything an INVALID record carried.
type DeadLetter struct {
	ID        string
	Processor string
	// Source is the source the record was read from.
	Source string
	// Segment and Position are the record's place in it.
	Segment  string
	Position string
	// Key is the record's key as lossy UTF-8; nil when it had none.
	Key *string
	// EventID is the record's "id" header; empty when it had none or a blank one.
	EventID string
	// EventType is the record's "type" header; empty when it had none or a blank one.
	EventType string
	// Headers are every record header, lossy UTF-8.
	Headers map[string]string
	// Payload is the record's value, byte for byte.
	Payload []byte
	Reason  DeadLetterReason
	Error   DeadLetterError
	// BlockedBy is, for a SEQUENCED letter, the key's head letter when it was parked
	// (display only).
	BlockedBy string
	Attempts  int
	// ParkedAt and LastAttemptAt are the application's clock.
	ParkedAt      time.Time
	LastAttemptAt time.Time
	ExpireAt      time.Time
	Replay        bool
}

// DedupeID is the id the record's stage 1 is deduplicated by: its EventID, else (none or blank)
// "<source>-<segment>-<position>".
func (l DeadLetter) DedupeID() string {
	if strings.TrimSpace(l.EventID) != "" {
		return l.EventID
	}
	return l.Source + "-" + l.Segment + "-" + l.Position
}

// DeadLetterSummary is what DeadLetterStore.Summaries reads of a letter.
type DeadLetterSummary struct {
	ID       string
	Key      *string
	Position string
	Reason   DeadLetterReason
	ParkedAt time.Time
}

// ParkResult says whether DeadLetterStore.Park inserted the letter or found it parked already.
type ParkResult int

const (
	// ParkNew: the letter was inserted.
	ParkNew ParkResult = iota
	// ParkAlready: a letter of that id exists (a redelivery).
	ParkAlready
)

// DeadLetterStore is the dead letters (mode collection). Every operation is a write of its own,
// outside any transaction, but a retry's DeleteIn.
type DeadLetterStore interface {
	// Park inserts letter; ParkAlready when a letter of that id exists (a redelivery).
	Park(ctx context.Context, letter DeadLetter) (ParkResult, error)
	// Head is the id of the head letter of (processor, key): the earliest position among its
	// letters that are not INVALID (then the lowest id); ok is false when there is none.
	Head(ctx context.Context, processor, key string) (id string, ok bool, err error)
	// Earlier is the id of the earliest letter of (processor, key) at a position before
	// position that is not INVALID: what blocks a retry. ok is false when there is none.
	Earlier(ctx context.Context, processor, key, position string) (id string, ok bool, err error)
	// ParkedKeys is the keys with a letter (not INVALID) of processor on the given segments of
	// source; each key once, in no particular order, none for no segments.
	ParkedKeys(ctx context.Context, processor, source string, segments []string) ([]string, error)
	// Get returns the letter, or nil when there is none.
	Get(ctx context.Context, id string) (*DeadLetter, error)
	// List returns letters by ParkedAt, then id, every processor's when processor is empty, at
	// most limit when limit > 0.
	List(ctx context.Context, processor string, limit int) ([]DeadLetter, error)
	// Summaries returns every letter of processor, without its record, by position as Head
	// orders them, then id.
	Summaries(ctx context.Context, processor string) ([]DeadLetterSummary, error)
	// Count is how many letters there are; every processor's when processor is empty.
	Count(ctx context.Context, processor string) (int64, error)
	// Delete deletes the letter; false when there was none.
	Delete(ctx context.Context, id string) (bool, error)
	// DeleteIn deletes the letter in the transaction carried by ctx: a retry's, which commits
	// with the record's marker and its handlers' effects or not at all. False when there was
	// none.
	DeleteIn(ctx context.Context, id string) (bool, error)
	// RecordFailure records that a retry failed again: Attempts + 1, Error, LastAttemptAt =
	// now, reason HANDLER_FAILED, no BlockedBy any more. False when the letter is gone.
	RecordFailure(ctx context.Context, id string, failure DeadLetterError, now time.Time) (bool, error)
}

// LetterID is a letter's id: "<processor>:<source>:<segment>:<position>" (on Kafka
// "<processor>:<topic>:<partition>:<offset>", as always).
func LetterID(processor, source, segment, position string) string {
	return processor + ":" + source + ":" + segment + ":" + position
}

// ResetKind is ControlOperation.Kind of a reset, the only kind there is.
const ResetKind = "reset"

// ProcessorControlState is one processor's control state; a processor the store knows nothing of
// is not paused, not locked and not replaying. A zero time means none.
type ProcessorControlState struct {
	Processor string
	Paused    bool
	PausedAt  time.Time
	// PausedBy is the control id of the request that paused it, or "local".
	PausedBy string
	// Operation is the reset holding the processor's lock.
	Operation *ControlOperation
	// Replay is the open replay ranges.
	Replay    []ReplayRange
	UpdatedAt time.Time
}

// ControlOperation is the lock of a reset: held until LeaseUntil unless renewed. PreparedAt is
// the first prepare's time; a renewal keeps it.
type ControlOperation struct {
	ID         string
	Kind       string
	LeaseUntil time.Time
	PreparedAt time.Time
}

// HoldsAt reports whether the lease has not passed at now.
func (o ControlOperation) HoldsAt(now time.Time) bool { return o.LeaseUntil.After(now) }

// LockOutcome is what ProcessorControlStore.Lock reports.
type LockOutcome struct {
	// Taken: the operation holds (or renewed) the lock.
	Taken bool
	// Holder is the other operation holding the lock when Taken is false.
	Holder ControlOperation
}

// ProcessorControlStore is the processors' control state: per processor its pause, its reset
// lock and its replay ranges, the source of truth of all three. Every operation is a write of
// its own, outside any transaction; the times are the caller's clock.
type ProcessorControlStore interface {
	// Get is the state of processor; the zero state (not paused, nothing held) when the store
	// has none.
	Get(ctx context.Context, processor string) (ProcessorControlState, error)
	// SetPaused pauses (by the control id, or "local") or resumes processor, idempotently. A
	// resume clears PausedAt and PausedBy.
	SetPaused(ctx context.Context, processor string, paused bool, by string, now time.Time) error
	// Lock takes the lock of processor for operation until leaseUntil, atomically: when nothing
	// holds it or the holder's lease has passed (leaseUntil <= now); a renewal (the operation
	// holds it already) moves the lease only. The outcome names the holder when another holds
	// it.
	Lock(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (LockOutcome, error)
	// Extend moves the lease of operation; false when it does not hold the lock.
	Extend(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (bool, error)
	// Release clears the lock when operation holds it (never another operation's); false when
	// it did not.
	Release(ctx context.Context, processor, operation string, now time.Time) (bool, error)
	// Expire clears the lock when operation holds it and its lease has passed at now (a renewal
	// that came meanwhile keeps it); false otherwise.
	Expire(ctx context.Context, processor, operation string, now time.Time) (bool, error)
	// SetReplay replaces the replay ranges of processor. A store that keeps a range's segment
	// and positions as numbers refuses one that is not decimal with ErrInvalid.
	SetReplay(ctx context.Context, processor string, ranges []ReplayRange, now time.Time) error
	// RemoveRange removes that exact range from the replay ranges of processor; false when it
	// was not there.
	RemoveRange(ctx context.Context, processor string, replay ReplayRange, now time.Time) (bool, error)
}

// ExternalCallStatus is where an external call stands; the values are the stored names.
type ExternalCallStatus string

const (
	// ExternalCallInProgress: the call is running under its owner's lease.
	ExternalCallInProgress ExternalCallStatus = "IN_PROGRESS"
	// ExternalCallSucceeded: the call returned Reply and is never repeated.
	ExternalCallSucceeded ExternalCallStatus = "SUCCEEDED"
	// ExternalCallFailed: the call failed permanently, with Error.
	ExternalCallFailed ExternalCallStatus = "FAILED"
	// ExternalCallUnknown: the call may or may not have happened.
	ExternalCallUnknown ExternalCallStatus = "UNKNOWN"
)

// String returns the stored name ("IN_PROGRESS", ...).
func (s ExternalCallStatus) String() string { return string(s) }

// ExternalCall is the record of one idempotent external call. Reply is the JSON text of the
// handler's reply, Error the JSON text of a failed call's error ({"code","message"}); an empty
// string means none. An InProgress record holds Owner (the running call's random token) and a
// LeaseUntil; a settled one holds neither.
type ExternalCall struct {
	Key      string
	Status   ExternalCallStatus
	Reply    string
	Error    string
	Attempts int
	// UpdatedAt, LeaseUntil are the caller's clock; a zero LeaseUntil means none.
	UpdatedAt  time.Time
	LeaseUntil time.Time
	Owner      string
}

// ExternalCallStore is the records of idempotent external calls, keyed by their idempotency key.
// Every operation is a write of its own, outside any transaction (a rolled-back handler never
// forgets a call that happened); the times are the caller's clock.
//
// Every write after Insert matches the record a call owns (its key, InProgress and its Owner) or
// the record a call read (its key, InProgress, Owner and LeaseUntil as read), so a call whose
// lease ran out never overwrites the record of the call that took it over; false (or nil) when
// nothing matched.
type ExternalCallStore interface {
	// Insert inserts call; false when a record of its key exists.
	Insert(ctx context.Context, call ExternalCall) (bool, error)
	// Find returns the record of key, or nil when there is none.
	Find(ctx context.Context, key string) (*ExternalCall, error)
	// TakeOver takes the lapsed call seen over for owner: Owner, LeaseUntil, UpdatedAt = now
	// and Attempts + 1, when the record is still seen; the record as written, nil when it is
	// not.
	TakeOver(ctx context.Context, seen ExternalCall, owner string, leaseUntil, now time.Time) (*ExternalCall, error)
	// Expire gives the lapsed call seen up as UNKNOWN (no owner, no lease), when the record is
	// still seen.
	Expire(ctx context.Context, seen ExternalCall, now time.Time) (bool, error)
	// Succeed settles the record owner owns as SUCCEEDED with reply (and no error, owner or
	// lease).
	Succeed(ctx context.Context, key, owner, reply string, now time.Time) (bool, error)
	// Fail settles the record owner owns as FAILED with errText (no owner or lease).
	Fail(ctx context.Context, key, owner, errText string, now time.Time) (bool, error)
	// MarkUnknown settles the record owner owns as UNKNOWN (no owner or lease).
	MarkUnknown(ctx context.Context, key, owner string, now time.Time) (bool, error)
	// Reset deletes the record owner owns (a retryable failure: the next call begins afresh).
	Reset(ctx context.Context, key, owner string) (bool, error)
}

// Saga is one saga instance. State is the saga object as JSON text; Version is the optimistic
// version every write checks (0 at creation, +1 per write). While Ended is false the instance is
// live: its Associations route events to it and its StartKey ("<type>|<key>=<value>", nil for
// forceNew instances) is unique. An empty StartKey means none; a zero EndedAt means none.
type Saga struct {
	ID           string
	Type         string
	State        string
	Associations []SagaAssociation
	StartKey     *string
	Ended        bool
	EndedAt      time.Time
	Version      int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// SagaAssociation is an association "Key = Value" of a saga instance: events whose association
// value is Value for Key are routed to it.
type SagaAssociation struct {
	Key   string
	Value string
}

// DispatchKind says whether a saga dispatch entry is a command (through the hub or the service's
// own handler) or an event (through the outbox).
type DispatchKind string

const (
	// DispatchCommand is a command.
	DispatchCommand DispatchKind = "COMMAND"
	// DispatchEvent is an event.
	DispatchEvent DispatchKind = "EVENT"
)

// SagaDispatch is one command or event a saga recorded in its transaction, waiting in the
// saga-side outbox. Seq orders the entries of one saga (Version*1000 + index); the id
// "<sagaID>:<seq>" is the command's idempotency key. A command given up on carries its outcome
// (FailureReason, FailureMessage, FailedAt); NoticePending is true until the
// snodra.SagaCommandFailed notice was delivered locally. Zero times mean none.
type SagaDispatch struct {
	ID             string
	SagaID         string
	Seq            int64
	Kind           DispatchKind
	Type           string
	Payload        string
	Headers        map[string]string
	RoutingKey     *string
	CreatedAt      time.Time
	DispatchedAt   time.Time
	Attempts       int
	NextAttemptAt  time.Time
	FailedAt       time.Time
	FailureReason  FailureReason
	FailureMessage string
	NoticePending  bool
	NoticeAttempts int
	NoticeNextAt   time.Time
}

// DeadlineStatus is the lifecycle of a SagaDeadline; the values are the stored names.
type DeadlineStatus string

const (
	// DeadlineScheduled: waiting for its due time.
	DeadlineScheduled DeadlineStatus = "SCHEDULED"
	// DeadlineFired: its handler ran.
	DeadlineFired DeadlineStatus = "FIRED"
	// DeadlineCancelled: it never fires.
	DeadlineCancelled DeadlineStatus = "CANCELLED"
)

// SagaDeadline is a deadline of a saga instance. Payload is the JSON text of the payload (any
// JSON value) and PayloadType its type name, both nil when there is none. A scheduler claims a
// due deadline with ClaimedBy and ClaimedUntil (a lease: an expired one lets it fire again);
// FinishedAt is set when it is FIRED or CANCELLED; CorrelationID is the correlation id of the
// message that scheduled it (empty means none).
type SagaDeadline struct {
	ID            string
	SagaType      string
	SagaID        string
	Name          string
	Payload       *string
	PayloadType   *string
	DueAt         time.Time
	Status        DeadlineStatus
	ClaimedBy     string
	ClaimedUntil  time.Time
	FinishedAt    time.Time
	CorrelationID string
}

// FailureReason is why a saga command failed (the retry taxonomy), stored by name.
type FailureReason string

const (
	// FailureNoHandler: nobody handles the command type.
	FailureNoHandler FailureReason = "NO_HANDLER"
	// FailureDenied: the handler requires an authenticated caller and a saga command has none.
	FailureDenied FailureReason = "DENIED"
	// FailureInvalid: the handler could not read the payload.
	FailureInvalid FailureReason = "INVALID"
	// FailureFailed: the handler failed (any other error).
	FailureFailed FailureReason = "FAILED"
	// FailureUnknownOutcome: an idempotent adapter crashed mid-call and cannot tell whether the
	// call happened.
	FailureUnknownOutcome FailureReason = "UNKNOWN_OUTCOME"
	// FailureExhausted: a transient failure persisted through all the relay's attempts.
	FailureExhausted FailureReason = "EXHAUSTED"
)

// String returns the stored name ("NO_HANDLER", ...).
func (r FailureReason) String() string { return string(r) }

// SagaFailure is a saga event that failed all its attempts, parked in saga_failures (ID
// "<sagaType>:<eventId>"): the event as received, the last error and how many attempts were
// made.
type SagaFailure struct {
	ID            string
	SagaType      string
	EventID       string
	EventType     string
	Payload       string
	Headers       map[string]string
	Error         string
	Attempts      int
	FirstFailedAt time.Time
	LastFailedAt  time.Time
}

// SagaStores is the saga ports, all in the store's transactions (the context's).
type SagaStores struct {
	// Sagas is the sagas themselves.
	Sagas SagaRepository
	// Dispatch is the saga-side outbox and its leases.
	Dispatch SagaDispatchStore
	// Deadlines is the deadlines and their scheduler.
	Deadlines SagaDeadlineStore
	// Failures is the parked saga events.
	Failures SagaFailureStore
	// Markers is the exactly-once markers of the saga stage.
	Markers SagaMarkerStore
}

// SagaRepository is the sagas.
type SagaRepository interface {
	// FindByID returns the instance, or nil when there is none.
	FindByID(ctx context.Context, id string) (*Saga, error)
	// FindLive returns the live (not ended) instances of typeName associated with key = value.
	FindLive(ctx context.Context, typeName, key, value string) ([]Saga, error)
	// Insert inserts a new instance; ErrConflict when its StartKey is held by a live instance.
	Insert(ctx context.Context, saga Saga) error
	// Update replaces the stored instance by saga when the stored Version is
	// expectedVersion; false when it is not.
	Update(ctx context.Context, saga Saga, expectedVersion int64) (bool, error)
	// CountLiveByType is how many live instances there are per saga type, outside any
	// transaction; a type without one is absent.
	CountLiveByType(ctx context.Context) (map[string]int64, error)
}

// SagaDeadlineStore is the deadlines, as far as the runtime writes them.
type SagaDeadlineStore interface {
	// Insert inserts deadline.
	Insert(ctx context.Context, deadline SagaDeadline) error
	// FindScheduled returns the deadline when it is still SCHEDULED, else nil.
	FindScheduled(ctx context.Context, id string) (*SagaDeadline, error)
	// Cancel cancels the SCHEDULED deadline deadlineID of sagaID; no effect on any other.
	Cancel(ctx context.Context, sagaID, deadlineID string, now time.Time) error
	// CancelByName cancels the SCHEDULED deadlines of sagaID named name.
	CancelByName(ctx context.Context, sagaID, name string, now time.Time) error
	// CancelAll cancels every SCHEDULED deadline of sagaID.
	CancelAll(ctx context.Context, sagaID string, now time.Time) error
	// MarkFired sets the SCHEDULED deadline id to FIRED; false when it was not SCHEDULED.
	MarkFired(ctx context.Context, id string, now time.Time) (bool, error)
	// ClaimDue claims the SCHEDULED deadline due first (DueAt <= now) that nobody holds or whose
	// claim has passed (ClaimedUntil < now), for owner until claimedUntil, atomically: of two
	// schedulers asking at once, one gets it. It runs outside any transaction; nil when nothing
	// is due.
	ClaimDue(ctx context.Context, owner string, now, claimedUntil time.Time) (*SagaDeadline, error)
	// CountOverdueByName is how many SCHEDULED deadlines are due (DueAt <= now) per name,
	// claimed or not; a name without one is absent.
	CountOverdueByName(ctx context.Context, now time.Time) (map[string]int64, error)
}

// SagaFailureStore is the parked saga events.
type SagaFailureStore interface {
	// Find returns the failure, or nil when there is none.
	Find(ctx context.Context, id string) (*SagaFailure, error)
	// Save inserts or replaces the failure with that id.
	Save(ctx context.Context, failure SagaFailure) error
	// Delete deletes the failure; false when there was none.
	Delete(ctx context.Context, id string) (bool, error)
	// FailedAgain records that a retry of failure id failed again: Attempts grows by attempts,
	// Error and LastFailedAt are replaced. It never inserts: false when the failure no longer
	// exists, so the bookkeeping of a failed retry cannot bring a discarded failure back.
	FailedAgain(ctx context.Context, id, errText string, attempts int, now time.Time) (bool, error)
	// List returns the failures of sagaType (every type when sagaType is empty), oldest
	// LastFailedAt first.
	List(ctx context.Context, sagaType string) ([]SagaFailure, error)
}

// SagaMarkerStore is the exactly-once markers of the saga stage: processed-event ids
// "saga:<sagaType>:<eventId>".
type SagaMarkerStore interface {
	// Exists reports whether the marker is recorded.
	Exists(ctx context.Context, id string) (bool, error)
	// Insert inserts the marker; ErrConflict when it exists.
	Insert(ctx context.Context, id, typeName string, now time.Time) error
}

// DispatchFailure is how the dispatch relay gives an entry up: FailedAt, the reason and the
// message, with Attempts when non-nil (nil leaves the count unchanged), and a pending failure
// notice due at NoticeAt when non-nil (NoticePending true, NoticeAttempts 0): a command's
// failure has one, an event's none.
type DispatchFailure struct {
	Attempts *int
	Reason   FailureReason
	Message  string
	FailedAt time.Time
	NoticeAt *time.Time
}

// SagaDispatchStore is the saga-side outbox and its per-saga dispatch leases. The runtime
// records what a saga sends in the saga's transaction; the dispatch relay works through it. An
// entry is pending until it is dispatched or failed. Every relay operation is a write of its
// own, outside any transaction, but MarkDispatched, which joins the transaction the relay
// appends the event to the outbox in. A call naming an entry that no longer exists changes
// nothing.
type SagaDispatchStore interface {
	// Record records entries in the transaction carried by ctx. An EVENT entry whose payload
	// the outbox would refuse (not a JSON object, or an integer above the signed 64-bit range)
	// is refused with ErrInvalid: the saga's attempt fails and nothing of it commits.
	Record(ctx context.Context, entries []SagaDispatch) error
	// DueSagas returns the ids of the sagas whose first pending entry (lowest Seq) is due
	// (NextAttemptAt zero or <= now), oldest first pending entry first (its CreatedAt, then the
	// saga id), at most limit, leaving out the sagas in except (in flight on this instance).
	DueSagas(ctx context.Context, now time.Time, except []string, limit int) ([]string, error)
	// NextPending returns the first pending entry of sagaID (lowest Seq), due or not; nil when
	// there is none.
	NextPending(ctx context.Context, sagaID string) (*SagaDispatch, error)
	// MarkDispatched records the entry dispatched at now, in the transaction carried by ctx (an
	// event: with its outbox append).
	MarkDispatched(ctx context.Context, id string, now time.Time) error
	// ScheduleRetry records a transient failure: Attempts and the next attempt at nextAttempt.
	ScheduleRetry(ctx context.Context, id string, attempts int, nextAttempt time.Time) error
	// MarkFailed gives the entry up: see DispatchFailure.
	MarkFailed(ctx context.Context, id string, failure DispatchFailure) error
	// ClaimDueNotice claims the pending failure notice due first (NoticeNextAt zero or <= now),
	// moving its NoticeNextAt to reserveUntil (which reserves it for the caller), atomically;
	// the entry as claimed, nil when no notice is due.
	ClaimDueNotice(ctx context.Context, now, reserveUntil time.Time) (*SagaDispatch, error)
	// ClearNotice records that the failure notice was delivered (or given up): no notice is
	// pending any more.
	ClearNotice(ctx context.Context, id string) error
	// ScheduleNoticeRetry records that a delivery of the notice failed: NoticeAttempts and its
	// next try at next.
	ScheduleNoticeRetry(ctx context.Context, id string, noticeAttempts int, next time.Time) error
	// ClaimLease takes the dispatch lease of sagaID for owner until until, when nobody holds it
	// or its lease has passed (< now), atomically; false when another holder's lease runs.
	ClaimLease(ctx context.Context, sagaID, owner string, now, until time.Time) (bool, error)
	// RenewLease moves the lease owner holds to until; false when it holds it no longer.
	RenewLease(ctx context.Context, sagaID, owner string, until time.Time) (bool, error)
	// ReleaseLease lets go of the lease when owner holds it.
	ReleaseLease(ctx context.Context, sagaID, owner string) error
	// Pending is how many entries are pending.
	Pending(ctx context.Context) (int64, error)
}

// ErrConflict: another writer got to a record first (a duplicate key, a write conflict, a
// version check or a stale transaction). The whole attempt is retried, in a new transaction.
var ErrConflict = errors.New("snodra: another writer got there first")

// MaxPayloadDepth is the deepest an outbox payload may be, as MySQL's JSON_DEPTH counts: a
// scalar or an empty object or array is 1, a non-empty one 1 + its deepest member. MySQL keeps
// no document nesting more than 100 objects and arrays, so the shared bound is depth 100 (one
// under MySQL's own limit for a document ending in a scalar).
const MaxPayloadDepth = 100

// CheckOutboxPayload verifies the outbox contract on a payload: JSON (UTF-8, no leading byte
// order mark, no lone surrogate escape, no number a double cannot hold), a JSON object at most
// MaxPayloadDepth deep, and every integer in it (a number written without '.', 'e' or 'E')
// within the signed 64-bit range. The integers are read from the text itself, since a JSON
// parser reads one outside the range as a float that a store would keep changed. Any other
// payload is refused with an error wrapping ErrInvalid.
func CheckOutboxPayload(typeName string, payload []byte) error {
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("%w: the payload of event %s %s", ErrInvalid, strconv.Quote(typeName),
			fmt.Sprintf(format, args...))
	}
	if !utf8.Valid(payload) {
		return refuse("is not UTF-8, so it cannot be an outbox entry")
	}
	if bytes.HasPrefix(payload, []byte{0xEF, 0xBB, 0xBF}) {
		return refuse("starts with a byte order mark, so it cannot be an outbox entry")
	}
	if err := checkSurrogates(payload); err != nil {
		return refuse("holds the lone surrogate escape %s, so it cannot be an outbox entry", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	kind, depth, err := payloadValue(decoder, 0)
	if err != nil {
		var tooDeep *payloadTooDeep
		if errors.As(err, &tooDeep) {
			return refuse("is deeper than %d (JSON depth), deeper than every store keeps, so it "+
				"cannot be an outbox entry", MaxPayloadDepth)
		}
		var number *payloadNumberError
		if errors.As(err, &number) {
			// The payload is JSON; its number is what the contract refuses.
			return refuse("%s", number.Error())
		}
		return refuse("is not JSON, so it cannot be an outbox entry: %v", err)
	}
	if kind != 'o' {
		return refuse("is %s, not a JSON object, so it cannot be an outbox entry", jsonKindName(kind))
	}
	if depth > MaxPayloadDepth {
		return refuse("is %d deep (JSON depth), deeper than the %d every store keeps, so it "+
			"cannot be an outbox entry", depth, MaxPayloadDepth)
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return refuse("holds more than one JSON value (%v), so it cannot be an outbox "+
				"entry", token)
		}
		return refuse("is not JSON, so it cannot be an outbox entry: %v", err)
	}
	return nil
}

// payloadTooDeep is the refusal payloadValue reports before it would recurse further.
type payloadTooDeep struct{}

func (*payloadTooDeep) Error() string { return "the payload is too deep" }

// payloadValue parses one JSON value and returns its kind ('o' object, 'a' array, 's' string,
// 'n' number, 'b' boolean, 'z' null) and its JSON depth. containers is how many objects and
// arrays enclose it; a value inside MaxPayloadDepth containers is deeper than the bound even
// when it is a scalar, so it is refused before the recursion goes further.
func payloadValue(decoder *json.Decoder, containers int) (kind byte, depth int, err error) {
	if containers >= MaxPayloadDepth {
		// Even a scalar here would be deeper than the bound, so the recursion stops before
		// the parser can be driven further.
		return 0, 0, &payloadTooDeep{}
	}
	token, err := decoder.Token()
	if err != nil {
		return 0, 0, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		if number, ok := token.(json.Number); ok {
			if err := checkJSONNumber(number.String()); err != nil {
				return 0, 0, err
			}
			return 'n', 1, nil
		}
		switch token.(type) {
		case string:
			return 's', 1, nil
		case bool:
			return 'b', 1, nil
		case nil:
			return 'z', 1, nil
		}
		return 0, 0, fmt.Errorf("unexpected JSON value %v", token)
	}
	switch delim {
	case '{':
		deepest := 0
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return 0, 0, err
			}
			if _, ok := key.(string); !ok {
				return 0, 0, fmt.Errorf("an object key is not a string (%v)", key)
			}
			_, childDepth, err := payloadValue(decoder, containers+1)
			if err != nil {
				return 0, 0, err
			}
			if childDepth > deepest {
				deepest = childDepth
			}
		}
		if _, err := decoder.Token(); err != nil {
			return 0, 0, err
		}
		return 'o', 1 + deepest, nil
	case '[':
		deepest := 0
		for decoder.More() {
			_, childDepth, err := payloadValue(decoder, containers+1)
			if err != nil {
				return 0, 0, err
			}
			if childDepth > deepest {
				deepest = childDepth
			}
		}
		if _, err := decoder.Token(); err != nil {
			return 0, 0, err
		}
		return 'a', 1 + deepest, nil
	}
	return 0, 0, fmt.Errorf("unexpected JSON delimiter %v", delim)
}

// checkJSONNumber refuses a number no double holds (an overflow such as 1e400; an underflow
// such as 1e-400 merely rounds to zero and is fine) and an integer outside the signed 64-bit
// range. A number with a fraction or an exponent is a double, exactly as every store reads it.
// Every refusal is a *payloadNumberError: the payload is valid JSON, only its number is not.
func checkJSONNumber(text string) error {
	if !strings.ContainsAny(text, ".eE") {
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return &payloadNumberError{
				detail: "holds the integer " + shownInteger(text) + ", outside the signed 64-bit " +
					"range, so it cannot be an outbox entry",
			}
		}
		return nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return &payloadNumberError{
			detail: "holds the number " + shownInteger(text) + ", which a double cannot read, so it " +
				"cannot be an outbox entry (" + err.Error() + ")",
		}
	}
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return &payloadNumberError{
			detail: "holds the number " + shownInteger(text) + ", which no double holds, so it " +
				"cannot be an outbox entry",
		}
	}
	return nil
}

// payloadNumberError is a number the outbox contract refuses in an otherwise valid JSON payload.
type payloadNumberError struct {
	detail string
}

func (e *payloadNumberError) Error() string { return e.detail }

// shownInteger quotes a number token, cut so a bad payload cannot make the message long.
func shownInteger(text string) string {
	const max = 64
	if len(text) <= max {
		return text
	}
	return text[:max] + "..."
}

// checkSurrogates refuses a lone surrogate escape in a JSON string, in a value or a name (a
// pair is fine). A JSON parser would read the lone one as U+FFFD, which a store would then keep
// changed.
func checkSurrogates(payload []byte) error {
	inString := false
	for i := 0; i < len(payload); i++ {
		c := payload[i]
		if !inString {
			if c == '"' {
				inString = true
			}
			continue
		}
		switch c {
		case '"':
			inString = false
		case '\\':
			if i+1 >= len(payload) {
				return nil // a syntax error the parser reports
			}
			if payload[i+1] != 'u' {
				i++ // the escaped character, a quote included, does not end the string
				continue
			}
			if i+6 > len(payload) {
				return nil // a syntax error the parser reports
			}
			code, ok := hex4(payload[i+2 : i+6])
			if !ok {
				return nil // a syntax error the parser reports
			}
			i += 5
			switch {
			case code >= 0xD800 && code <= 0xDBFF:
				// A high surrogate must be followed at once by a low one.
				if i+6 < len(payload) && payload[i+1] == '\\' && payload[i+2] == 'u' {
					if low, ok := hex4(payload[i+3 : i+7]); ok && low >= 0xDC00 && low <= 0xDFFF {
						i += 6
						continue
					}
				}
				return fmt.Errorf("\\u%04x", code)
			case code >= 0xDC00 && code <= 0xDFFF:
				return fmt.Errorf("\\u%04x", code)
			}
		}
	}
	return nil
}

// hex4 reads four hexadecimal digits.
func hex4(text []byte) (int, bool) {
	value := 0
	for _, c := range text {
		value <<= 4
		switch {
		case c >= '0' && c <= '9':
			value += int(c - '0')
		case c >= 'a' && c <= 'f':
			value += int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			value += int(c-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

// jsonKindName names a JSON value's kind for a refusal.
func jsonKindName(kind byte) string {
	switch kind {
	case 'a':
		return "an array"
	case 's':
		return "a string"
	case 'n':
		return "a number"
	case 'b':
		return "a boolean"
	case 'z':
		return "null"
	default:
		return "not a JSON object"
	}
}
