package snodra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/traceparent"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// DefaultEventsSource is the source a service publishes its events to and its processors read
// (EventsSource, SNODRA_EVENTS_SOURCE) unless configured otherwise.
const DefaultEventsSource = "snodra.events"

// What publishing and event handlers fail with when an add-on is missing. The message names the
// option that sets the store.
const (
	missingMessageStore = "publishing an event needs a message store: set one with WithStore " +
		"(WithStore(snodra.NewMemoryMessageStore()) runs the in-memory one, not durable: tests and local runs)"
	missingEventStore = "publishing an event needs an event store: set one with WithEventStore " +
		"(WithEventStore(snodra.NewMemoryEventStore()) runs the in-memory one, not durable: tests and local runs)"
)

// eventSettings is what the event side of a service is configured with; the options and
// environment variables write it.
type eventSettings struct {
	source          string        // the event store source published to and read
	outboxEnabled   bool          // one relay runs in every instance
	consumerEnabled bool          // every processor reads its subscription
	poll            time.Duration // the relay's longest wait between two polls
	batch           int           // the most entries one claim returns
	outboxAttempts  int           // sends of one entry before it is given up
	lease           time.Duration // how long a claimed batch stays the relay's
	sendTimeout     time.Duration // the longest one publish may take
	consumerTries   int           // handler invocations per record, the first included
	consumerBackoff time.Duration // between two invocations of one record
	// The saga stage: conflict attempts and the handler retry, and the stage's backoff.
	sagaAttempts        int
	sagaHandlerAttempts int
	sagaHandlerPause    time.Duration
	sagaInitialBackoff  time.Duration
	sagaMaxBackoff      time.Duration
	// The dispatch relay and the deadline scheduler.
	sagaPoll          time.Duration
	sagaLease         time.Duration
	sagaSendTimeout   time.Duration
	sagaDispatchTries int
	sagaDispatchWait  time.Duration
	sagaDispatchMax   time.Duration
	sagaBatch         int
	sagaParallelism   int
	deadlinePoll      time.Duration
	deadlineLease     time.Duration
	resubscribeMin    time.Duration // after a subscription failed, before subscribing again
	resubscribeMax    time.Duration // the cap of that wait, and what counts as a stable subscription
	// deadLetterMode is where a record that exhausts its attempts goes: a letter in the message
	// store ("collection", the default). The event store's dead-letter source and logging arrive
	// with the remaining modes.
	deadLetterMode string
	// deadLetterRetention is how long a letter stays before its key unblocks ("collection").
	deadLetterRetention time.Duration
	storeInitSchema     bool // App.Start prepares the message store's schema
	// sleep waits d or until ctx ends; false when ctx ended. It is sleepCtx, and a test replaces
	// it to record the re-subscribe waits without waiting.
	sleep func(ctx context.Context, d time.Duration) bool
}

// defaultEventSettings are the shipped defaults, the reference libraries' values.
func defaultEventSettings() eventSettings {
	return eventSettings{
		source:              DefaultEventsSource,
		outboxEnabled:       true,
		consumerEnabled:     true,
		poll:                200 * time.Millisecond,
		batch:               100,
		outboxAttempts:      10,
		lease:               120 * time.Second,
		sendTimeout:         40 * time.Second,
		consumerTries:       5,
		consumerBackoff:     time.Second,
		sagaAttempts:        5,
		sagaHandlerAttempts: 2,
		sagaHandlerPause:    50 * time.Millisecond,
		sagaInitialBackoff:  200 * time.Millisecond,
		sagaMaxBackoff:      5 * time.Second,
		sagaPoll:            500 * time.Millisecond,
		sagaLease:           30 * time.Second,
		sagaSendTimeout:     time.Minute,
		sagaDispatchTries:   10,
		sagaDispatchWait:    time.Second,
		sagaDispatchMax:     5 * time.Minute,
		sagaBatch:           100,
		sagaParallelism:     4,
		deadlinePoll:        time.Second,
		deadlineLease:       30 * time.Second,
		resubscribeMin:      time.Second,
		resubscribeMax:      30 * time.Second,
		deadLetterMode:      DeadLetterModeCollection,
		// The references keep a letter for 30 days (snodra.messaging.events.dead-letter.retention).
		deadLetterRetention: 30 * 24 * time.Hour,
		storeInitSchema:     true,
		sleep:               sleepCtx,
	}
}

// An EventOption configures one event handler in [HandleEvent]: the processor it runs in
// ([InProcessor]).
type EventOption func(*eventConfig)

// eventConfig is what the [EventOption]s of one event handler set.
type eventConfig struct {
	processor      string
	disallowReplay bool
	err            error
}

// InProcessor runs the event handler in the named processor: its own subscription
// "<service>.<name>", its own stage-1 markers ("processor:<name>:<eventId>") and its own
// read of the events, so one event type handled in two processors reaches both and a failure in
// one does not affect the other. A name matches [a-z0-9][a-z0-9-]{0,47} (at most 48 characters)
// and is never "default", where handlers registered without this option run. Every instance of
// the service that runs the handler reads the processor's subscription; the instances share its
// segments.
func InProcessor(name string) EventOption {
	return func(c *eventConfig) {
		name = strings.TrimSpace(name)
		switch {
		case name == "":
			c.err = errors.New("InProcessor needs a non-empty name")
		case name == DefaultProcessor:
			c.err = errors.New("there is no named processor \"default\": handlers without InProcessor run there")
		case !processorName.MatchString(name):
			c.err = fmt.Errorf("InProcessor(%q): a processor name matches [a-z0-9][a-z0-9-]{0,47}", name)
		default:
			c.processor = name
		}
	}
}

// HandleEvent registers h as a handler of the event type E, by E's MessageType. The handler runs
// in the [DefaultProcessor] unless [InProcessor] names another processor; several handlers of one
// type in one processor all run, in registration order, and an error in one fails the record.
//
// A handler of a delivered event runs in the processor's transaction with the event's
// stage-1 marker: its writes and the marker commit together, and an event whose marker is
// recorded already (a duplicate) runs no handler. An event of a type no handler of the processor
// handles is acknowledged and runs nothing.
//
// The handler's context carries what [MessageFrom] reads (the event's id, type, correlation id
// and headers) and, through [Publish], the event's correlation id, userId and trace context. An
// event handler has no caller ([CallerFrom] reports false): an event carries a userId, never a
// token. Events are handled at least once: a handler that fails is tried again, and a record that
// fails every attempt is parked as a dead letter (mode collection, the default) and acknowledged.
//
// Handlers are registered before [App.Start], like commands, queries and subscription handlers.
// An event handler needs the message store ([WithStore]) and the event store
// ([WithEventStore]) of the service: [App.Start] fails without them, naming the missing add-on.
// Commands, queries and subscription queries need neither. See
// https://snodra.damoreira.com/docs/library-authors/messages/ (Events) and
// https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/ (The rule).
func HandleEvent[E Message](app *App, h func(context.Context, E) error, opts ...EventOption) error {
	if app == nil {
		return errors.New("snodra: registration on a nil App")
	}
	var cfg eventConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	name, err := messageTypeOf[E]()
	if err != nil {
		return app.addEventHandler(&eventHandler{}, err)
	}
	if h == nil {
		return app.addEventHandler(&eventHandler{name: name}, fmt.Errorf("snodra: nil event handler for %s", name))
	}
	if cfg.err != nil {
		return app.addEventHandler(&eventHandler{name: name}, fmt.Errorf("%w (%s)", cfg.err, name))
	}
	processor := cfg.processor
	if processor == "" {
		processor = DefaultProcessor
	}
	handler := &eventHandler{
		processor:      processor,
		name:           name,
		disallowReplay: cfg.disallowReplay,
		invoke: func(ctx context.Context, codec PayloadCodec, payload []byte) error {
			event, err := decodeMessage[E](codec, payload)
			if err != nil {
				return err
			}
			return h(ctx, event)
		},
	}
	return app.addEventHandler(handler, nil)
}

// addEventHandler records h, or err, for its processor and event type.
func (a *App) addEventHandler(h *eventHandler, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil {
		err = a.checkEventHandler(h)
	}
	if err != nil {
		a.regErrs = append(a.regErrs, err)
		return err
	}
	byType := a.eventHandlers[h.processor]
	if byType == nil {
		byType = map[string][]*eventHandler{}
		a.eventHandlers[h.processor] = byType
	}
	byType[h.name] = append(byType[h.name], h)
	return nil
}

// checkEventHandler refuses a handler registered after Start and a handler without a type name or
// a processor.
func (a *App) checkEventHandler(h *eventHandler) error {
	if a.started || a.stopping {
		return fmt.Errorf("%w: the event handler for %s registered after Start", ErrRegistrationClosed, h.name)
	}
	if h.name == "" || h.processor == "" {
		return errors.New("snodra: an event handler needs a type name and a processor")
	}
	return nil
}

// A PublishOption configures one [Publish] call.
type PublishOption func(*publishConfig)

// publishConfig is what the [PublishOption]s of one call set.
type publishConfig struct {
	key *string
}

// Key gives a published event its order key, typically an aggregate id: events with one key reach
// a processor in publish order. An empty key is a key; without Key the event has none and spreads
// over the event store's segments. See
// https://snodra.damoreira.com/docs/library-authors/messages/ (Event headers).
func Key(key string) PublishOption {
	return func(c *publishConfig) { c.key = &key }
}

// Publish builds the event e and appends it to the service's outbox: the event type is E's
// MessageType, the id a fresh UUID, the key what [Key] set, the timestamp the creation time and
// the headers of messages.md: six headers plus traceparent when the work that publishes carries
// one (type, id, correlationId, timestamp, source and userId, and traceparent inside a trace).
// The append joins the transaction the context carries (the store's Begin or the add-on's own
// transaction helper), so the event commits with the service's writes or not at all. It returns
// once the entry is appended; the relay in every instance publishes it to the event store
// afterwards, at least once.
//
// userId is the verified caller's subject for a command or a query; a token itself never crosses
// into an event. An event published while handling another message carries that message's
// correlation id and trace context, and the userId of the event being handled. Outside any
// handling the correlation id is a fresh UUID, the userId is empty and there is no traceparent.
//
// Publishing needs the message store ([WithStore]) and the event store ([WithEventStore]): the
// first publish fails naming the missing add-on, since a service that only handles commands cannot
// be told apart from one that publishes at startup. See
// https://snodra.damoreira.com/docs/library-authors/messages/ (Event headers) and
// https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/ (The rule).
func Publish[E Message](ctx context.Context, app *App, e E, opts ...PublishOption) error {
	if app == nil {
		return errors.New("snodra: Publish on a nil App")
	}
	if h := handlingFrom(ctx); h != nil && h.saga {
		return errors.New("snodra: a saga handler records events with SagaContext.Publish; app-level Publish is refused while it handles")
	}
	var cfg publishConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if app.cfg.store == nil {
		return errors.New("snodra: " + missingMessageStore)
	}
	if app.cfg.eventStore == nil {
		return errors.New("snodra: " + missingEventStore)
	}
	typ, err := messageTypeOf[E]()
	if err != nil {
		return err
	}
	payload, err := app.cfg.codec.Marshal(e)
	if err != nil {
		return fmt.Errorf("snodra: cannot encode the event %s: %w", typ, err)
	}
	if err := CheckOutboxPayload(typ, payload); err != nil {
		return err
	}
	now := app.now()
	id := uuid.New()
	entry := OutboxEntry{
		ID:        id,
		Type:      typ,
		Key:       cfg.key,
		Headers:   eventHeaders(ctx),
		Payload:   payload,
		CreatedAt: now,
		Seq:       app.nextOutboxSeq(),
	}
	return app.cfg.store.Outbox().Append(ctx, entry)
}

// eventHeaders are the headers [Publish] stores for an event: the correlation id, the userId and,
// when the work being handled carries one, the traceparent. The relay adds type, id, timestamp
// and source from the entry, so a store never sees a stale copy of them.
func eventHeaders(ctx context.Context) map[string]string {
	headers := map[string]string{
		HeaderCorrelationID: uuid.New(),
		HeaderUserID:        "",
	}
	h := handlingFrom(ctx)
	if h == nil {
		return headers
	}
	if h.info.CorrelationID != "" {
		headers[HeaderCorrelationID] = h.info.CorrelationID
	}
	headers[HeaderUserID] = actingUserID(h)
	if h.traceparent != "" {
		// TODO(spec-gap): SG-36 (outside any trace an event carries six headers, with no
		// traceparent: the reference writes one only when the work carries a valid one).
		headers[HeaderTraceparent] = h.traceparent
	}
	return headers
}

// actingUserID is the user an event published while handling h acts for: the verified caller's
// subject, or for an event handler the userId the event carried. A token never crosses.
func actingUserID(h *handling) string {
	if h.event {
		return h.info.Headers[HeaderUserID]
	}
	if h.caller != nil {
		return h.caller.Subject
	}
	return ""
}

// eventTimestamp writes when an event was created as its timestamp header: RFC 3339 in UTC, always
// with milliseconds ("2026-10-28T14:03:22.000Z").
func eventTimestamp(at time.Time) string {
	return at.UTC().Format("2006-01-02T15:04:05.000Z")
}

// nextOutboxSeq is the instance's counter that breaks CreatedAt ties, so per-key order holds for
// what one instance publishes.
func (a *App) nextOutboxSeq() int64 {
	return a.outboxSeq.Add(1)
}

// eventHandling is what an event handler's context says of the event being handled: its id, type,
// correlation id and headers, its userId, and its valid traceparent. The caller is anonymous: an
// event carries a user id, never a token.
func eventHandling(eventID, typeName string, headers map[string]string) *handling {
	copied := copyStringMap(headers)
	correlation := copied[HeaderCorrelationID]
	if correlation == "" {
		correlation = eventID
	}
	scope := &handling{
		event: true,
		info: MessageInfo{
			ID:            eventID,
			Type:          typeName,
			CorrelationID: correlation,
			Headers:       copied,
		},
	}
	if tp := copied[HeaderTraceparent]; tp != "" && traceparent.Valid(tp) {
		scope.traceparent = tp
	}
	return scope
}

// isJSONObject reports whether payload is a JSON object, the only event payload the suite holds.
func isJSONObject(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

// sagaBackoff is the wait after conflict attempt n (2 or more): the initial backoff doubling,
// capped, with the reference libraries' ±20% jitter.
func (s eventSettings) sagaBackoff(attempt int) time.Duration {
	wait := s.sagaInitialBackoff
	for i := 2; i < attempt; i++ {
		wait *= 2
		if wait >= s.sagaMaxBackoff {
			wait = s.sagaMaxBackoff
			break
		}
	}
	return jitteredWait(wait, s.sagaMaxBackoff)
}
