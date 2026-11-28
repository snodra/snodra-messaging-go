package snodra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// SagaRoute is how an event reaches a saga instance: the event's value for Key. A nil Value, or a
// blank value, means the event has no such association and matches no instance.
type SagaRoute[E Message] struct {
	Key   string
	Value func(E) string
}

// Associate builds a route for the saga event type E. The value is usually one field of the
// event, for example the order id; events whose value is blank match no instance.
func Associate[E Message](key string, value func(E) string) SagaRoute[E] {
	return SagaRoute[E]{Key: key, Value: value}
}

// SagaContext is what a saga handler records: associations, commands, events, deadlines and the
// end of its instance. Nothing is sent or published while the handler runs; the runtime applies
// what was recorded in the saga's transaction and the relay sends it afterwards. Use Send and
// Publish (and Schedule) here: the package-level Send, Query and Publish are refused inside a saga
// handler, because they would bypass the saga's transaction and its ordering. A nil *SagaContext
// is never passed to a handler.
type SagaContext struct {
	app           *App
	sagaType      string
	id            string
	version       int64
	correlationID string
	traceparent   string
	now           time.Time
	associations  []SagaAssociation
	end           bool
	entries       []SagaDispatch
	deadlineOps   []sagaDeadlineOp
}

// SagaID is the instance's id.
func (c *SagaContext) SagaID() string { return c.id }

// CorrelationID is the correlation id of the message whose handling recorded the effects.
func (c *SagaContext) CorrelationID() string { return c.correlationID }

// Associate adds key = value to the instance's associations. The same pair twice is recorded
// once.
func (c *SagaContext) Associate(key, value string) {
	for _, association := range c.associations {
		if association.Key == key && association.Value == value {
			return
		}
	}
	c.associations = append(c.associations, SagaAssociation{Key: key, Value: value})
}

// RemoveAssociation removes key = value from the instance's associations.
func (c *SagaContext) RemoveAssociation(key, value string) {
	for i, association := range c.associations {
		if association.Key == key && association.Value == value {
			c.associations = append(c.associations[:i], c.associations[i+1:]...)
			return
		}
	}
}

// End ends the instance after the handler returns. The runtime then cancels its scheduled
// deadlines; an ended instance receives no more events.
func (c *SagaContext) End() { c.end = true }

// Send records command, to be sent after the transaction commits with routingKey (empty for
// none). It returns why the command cannot be recorded (a nil message, an unencodable payload,
// or more than 999 messages in one handling); the handler decides whether to return the error,
// which fails the handling and retries or parks the event.
func (c *SagaContext) Send(command Message, routingKey string) error {
	return c.record(DispatchCommand, command, routingKey)
}

// Publish records event, to be appended to the outbox after the transaction commits, with key
// (empty for none). The same limits as [SagaContext.Send] apply.
func (c *SagaContext) Publish(event Message, key string) error {
	return c.record(DispatchEvent, event, key)
}

func (c *SagaContext) record(kind DispatchKind, message Message, routingKey string) error {
	if message == nil {
		return errors.New("snodra: a saga cannot record a nil message")
	}
	if len(c.entries) >= sagaSeqStride-1 {
		return fmt.Errorf("snodra: a saga handling records at most %d messages", sagaSeqStride-1)
	}
	typ, err := messageName(message)
	if err != nil {
		return err
	}
	payload, err := c.app.cfg.codec.Marshal(message)
	if err != nil {
		return fmt.Errorf("snodra: cannot encode %s: %w", typ, err)
	}
	if kind == DispatchEvent {
		// Refuse at record time what the outbox could never hold, as the references do; the
		// relay's INVALID path is only the backstop for an entry recorded anyway.
		if err := CheckOutboxPayload(typ, payload); err != nil {
			return err
		}
	}
	seq := c.version*sagaSeqStride + int64(len(c.entries))
	var key *string
	if routingKey != "" {
		key = &routingKey
	}
	c.entries = append(c.entries, SagaDispatch{
		ID: fmt.Sprintf("%s:%d", c.id, seq), SagaID: c.id, Seq: seq, Kind: kind,
		Type: typ, Payload: string(payload), Headers: map[string]string{
			sagaCorrelationHeader: c.correlationID,
		},
		RoutingKey: key, CreatedAt: c.now,
	})
	if c.traceparent != "" {
		c.entries[len(c.entries)-1].Headers[sagaTraceparentHeader] = c.traceparent
	}
	return nil
}

// Schedule records a deadline named name to fire after d, with payload (nil for none). It
// returns the deadline id, which [SagaContext.CancelDeadline] cancels.
func (c *SagaContext) Schedule(after time.Duration, name string, payload any) (string, error) {
	return c.ScheduleAt(c.now.Add(after), name, payload)
}

// ScheduleAt records a deadline named name to fire at at.
func (c *SagaContext) ScheduleAt(at time.Time, name string, payload any) (string, error) {
	id := uuid.New()
	var encoded *string
	var payloadType *string
	if payload != nil {
		raw, err := c.app.cfg.codec.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("snodra: cannot encode the deadline payload: %w", err)
		}
		text := string(raw)
		encoded = &text
		name := deadlinePayloadType(payload)
		payloadType = &name
	}
	c.deadlineOps = append(c.deadlineOps, sagaDeadlineOp{schedule: &SagaDeadline{
		ID: id, SagaType: c.sagaType, SagaID: c.id, Name: name, Payload: encoded,
		PayloadType: payloadType, DueAt: at, Status: DeadlineScheduled, CorrelationID: c.correlationID,
	}})
	return id, nil
}

// CancelDeadline cancels the deadline id. It has no effect when the deadline already fired or was
// cancelled.
func (c *SagaContext) CancelDeadline(id string) {
	c.deadlineOps = append(c.deadlineOps, sagaDeadlineOp{cancelID: id})
}

// CancelDeadlines cancels every scheduled deadline of the instance named name.
func (c *SagaContext) CancelDeadlines(name string) {
	c.deadlineOps = append(c.deadlineOps, sagaDeadlineOp{cancelName: name})
}

type sagaDeadlineOp struct {
	schedule   *SagaDeadline
	cancelID   string
	cancelName string
}

// SagaEvent is one event handler of a saga definition, built by StartedOn or HandledOn.
type SagaEvent[S any] struct {
	handler *sagaEventHandler
}

// Ending makes the event's handler end the instance when it returns. Later handlers of the same
// event do not run.
func (e SagaEvent[S]) Ending() SagaEvent[S] {
	e.handler.end = true
	return e
}

// ForceNew makes a start handler start a new instance even when instances already match. The
// new instance keeps no start key, so it never blocks another one.
func (e SagaEvent[S]) ForceNew() SagaEvent[S] {
	e.handler.forceNew = true
	return e
}

// StartedOn registers a start handler: an event matching no live instance starts one. A live
// instance with the same start association keeps the event instead of starting another; add
// [SagaEvent.ForceNew] to start one anyway.
func StartedOn[S any, E Message](association SagaRoute[E], handler func(context.Context, *SagaContext, *S, E) error) SagaEvent[S] {
	return sagaEvent[S, E](association, handler, true)
}

// HandledOn registers a handler of an event of a live instance.
func HandledOn[S any, E Message](association SagaRoute[E], handler func(context.Context, *SagaContext, *S, E) error) SagaEvent[S] {
	return sagaEvent[S, E](association, handler, false)
}

func sagaEvent[S any, E Message](association SagaRoute[E], handler func(context.Context, *SagaContext, *S, E) error, start bool) SagaEvent[S] {
	typ, err := messageTypeOf[E]()
	h := &sagaEventHandler{eventType: typ, keyName: association.Key, start: start, err: err}
	if handler == nil {
		h.err = fmt.Errorf("snodra: a nil saga handler for %s", typ)
	} else if association.Value == nil {
		h.err = fmt.Errorf("snodra: the saga handler for %s needs an association value", typ)
	} else if strings.TrimSpace(association.Key) == "" {
		h.err = fmt.Errorf("snodra: the saga handler for %s needs an association key", typ)
	} else {
		h.value = func(event any) (string, bool) {
			value := association.Value(event.(E))
			return value, value != ""
		}
		h.decode = func(codec PayloadCodec, payload []byte) (any, error) {
			return decodeMessage[E](codec, payload)
		}
		h.invoke = func(ctx context.Context, saga *SagaContext, state any, event any) error {
			return handler(ctx, saga, state.(*S), event.(E))
		}
	}
	return SagaEvent[S]{handler: h}
}

// SagaDeadlineHandler is one deadline handler of a saga definition.
type SagaDeadlineHandler[S any] struct {
	handler *sagaDeadlineHandler
}

// Ending makes the deadline handler end the instance.
func (d SagaDeadlineHandler[S]) Ending() SagaDeadlineHandler[S] {
	d.handler.end = true
	return d
}

// Deadline registers a handler of deadlines named name, receiving the payload as P. A deadline
// without a payload calls it with P's zero value.
func Deadline[S any, P any](name string, handler func(context.Context, *SagaContext, *S, P) error) SagaDeadlineHandler[S] {
	h := &sagaDeadlineHandler{name: strings.TrimSpace(name)}
	if h.name == "" {
		h.err = errors.New("snodra: a deadline handler needs a name")
	}
	if handler == nil {
		h.err = fmt.Errorf("snodra: a nil deadline handler for %q", name)
	} else {
		h.invoke = func(ctx context.Context, saga *SagaContext, state any, payload []byte) error {
			var value P
			if payload != nil {
				if err := saga.app.cfg.codec.Unmarshal(payload, &value); err != nil {
					return err
				}
			}
			return handler(ctx, saga, state.(*S), value)
		}
	}
	return SagaDeadlineHandler[S]{handler: h}
}

// SagaCommandFailedHandler is the handler of the local SagaCommandFailed event.
type SagaCommandFailedHandler[S any] struct {
	handler *sagaEventHandler
}

// CommandFailed registers a handler of the local SagaCommandFailed event. The runtime also
// routes the event to the instance that sent the command, by its id; no association is needed.
func CommandFailed[S any](handler func(context.Context, *SagaContext, *S, SagaCommandFailed) error) SagaCommandFailedHandler[S] {
	h := &sagaEventHandler{eventType: commandFailedTypeName, byID: true}
	if handler == nil {
		h.err = errors.New("snodra: a nil SagaCommandFailed handler")
	} else {
		h.decode = func(codec PayloadCodec, payload []byte) (any, error) {
			return decodeMessage[SagaCommandFailed](codec, payload)
		}
		h.invoke = func(ctx context.Context, saga *SagaContext, state any, event any) error {
			return handler(ctx, saga, state.(*S), event.(SagaCommandFailed))
		}
	}
	return SagaCommandFailedHandler[S]{handler: h}
}

// SagaDefinition collects a saga's handlers. Build it inside [HandleSaga], with [StartedOn],
// [HandledOn], [Deadline] and [CommandFailed], adding each with StartOn, On, OnDeadline or
// OnCommandFailed.
type SagaDefinition[S any] struct {
	name      string
	handlers  map[string][]*sagaEventHandler
	start     []*sagaEventHandler
	deadlines map[string]*sagaDeadlineHandler
	errs      []error
}

// StartOn adds a start handler.
func (d *SagaDefinition[S]) StartOn(event SagaEvent[S]) {
	if event.handler == nil {
		d.errs = append(d.errs, errors.New("snodra: nil saga event"))
		return
	}
	if !event.handler.start {
		event.handler.err = errors.New("snodra: StartOn needs a StartedOn handler")
	}
	d.add(event.handler)
	if event.handler.err == nil {
		d.start = append(d.start, event.handler)
	}
}

// On adds an event handler of a live instance.
func (d *SagaDefinition[S]) On(event SagaEvent[S]) {
	if event.handler == nil {
		d.errs = append(d.errs, errors.New("snodra: nil saga event"))
		return
	}
	if event.handler.start {
		event.handler.err = errors.New("snodra: On needs a HandledOn handler")
	}
	d.add(event.handler)
}

func (d *SagaDefinition[S]) add(handler *sagaEventHandler) {
	if handler.err != nil {
		d.errs = append(d.errs, handler.err)
		return
	}
	d.handlers[handler.eventType] = append(d.handlers[handler.eventType], handler)
}

// OnDeadline adds a deadline handler. A name may be registered once.
func (d *SagaDefinition[S]) OnDeadline(handler SagaDeadlineHandler[S]) {
	if handler.handler == nil {
		d.errs = append(d.errs, errors.New("snodra: nil deadline handler"))
		return
	}
	if handler.handler.err != nil {
		d.errs = append(d.errs, handler.handler.err)
		return
	}
	if _, exists := d.deadlines[handler.handler.name]; exists {
		d.errs = append(d.errs, fmt.Errorf("snodra: a deadline handler named %q is registered twice", handler.handler.name))
		return
	}
	d.deadlines[handler.handler.name] = handler.handler
}

// OnCommandFailed adds the handler of the local SagaCommandFailed event.
func (d *SagaDefinition[S]) OnCommandFailed(handler SagaCommandFailedHandler[S]) {
	if handler.handler == nil {
		d.errs = append(d.errs, errors.New("snodra: nil SagaCommandFailed handler"))
		return
	}
	if handler.handler.err != nil {
		d.errs = append(d.errs, handler.handler.err)
		return
	}
	d.handlers[commandFailedTypeName] = append(d.handlers[commandFailedTypeName], handler.handler)
}

// HandleSaga registers a saga named name, defined by define, to run in the default processor.
// The state S must encode to a JSON object and round-trip through the library's codec. A
// definition error names the saga and the handler; it is returned here and kept for [App.Start]
// like every other registration error. A name may be registered once.
//
// A handler records what it sends through its [SagaContext]; the package-level [Send], [Query]
// and [Publish] are refused with an error while a saga handler runs.
func HandleSaga[S any](app *App, name string, define func(*SagaDefinition[S])) error {
	if app == nil {
		return errors.New("snodra: registration on a nil App")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return app.addSaga(nil, errors.New("snodra: a saga needs a name"))
	}
	if strings.Contains(name, ":") {
		return app.addSaga(nil, fmt.Errorf("snodra: the saga name %q cannot contain ':' (the marker id and the dispatch entries use it)", name))
	}
	definition := &SagaDefinition[S]{name: name, handlers: map[string][]*sagaEventHandler{},
		deadlines: map[string]*sagaDeadlineHandler{}}
	if define == nil {
		return app.addSaga(nil, fmt.Errorf("snodra: nil definition for saga %s", name))
	}
	define(definition)
	if len(definition.errs) > 0 {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s: %w", name, errors.Join(definition.errs...)))
	}
	if len(definition.start) == 0 {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s has no start handler", name))
	}
	state := new(S)
	encoded, err := app.cfg.codec.Marshal(state)
	if err != nil {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s state does not encode: %w", name, err))
	}
	if !isJSONObject(encoded) {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s state is not a JSON object", name))
	}
	decoded := new(S)
	if err := app.cfg.codec.Unmarshal(encoded, decoded); err != nil {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s state does not round-trip: %w", name, err))
	}
	again, err := app.cfg.codec.Marshal(decoded)
	if err != nil {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s state does not round-trip: %w", name, err))
	}
	if string(again) != string(encoded) {
		return app.addSaga(nil, fmt.Errorf("snodra: saga %s state does not round-trip: encoding it again gave a different document: %s then %s",
			name, encoded, again))
	}
	runtime := &sagaTypeRuntime{name: name, handlers: definition.handlers, start: definition.start,
		deadlines: definition.deadlines, decodeState: func(text string) (any, error) {
			state := new(S)
			if err := app.cfg.codec.Unmarshal([]byte(text), state); err != nil {
				return nil, err
			}
			return state, nil
		}, encodeState: func(state any) (string, error) {
			raw, err := app.cfg.codec.Marshal(state)
			return string(raw), err
		}, newState: func() any { return new(S) }}
	return app.addSaga(runtime, nil)
}

const (
	sagaCorrelationHeader = "correlation-id"
	sagaTraceparentHeader = "traceparent"
	sagaSeqStride         = 1000
)

// deadlinePayloadType is the payload's type name for a deadline: its MessageType when it is a
// message, else its Go type name.
func deadlinePayloadType(payload any) string {
	if message, ok := payload.(Message); ok {
		if name, err := messageName(message); err == nil {
			return name
		}
	}
	return reflect.TypeOf(payload).String()
}

// sagaDeadlineHandler is one deadline handler.
type sagaDeadlineHandler struct {
	name   string
	end    bool
	invoke func(context.Context, *SagaContext, any, []byte) error
	err    error
}
