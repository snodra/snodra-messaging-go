package snodra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/traceparent"
	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// sagaEventHandler is one event handler of a saga type, with its payload erased to any.
type sagaEventHandler struct {
	eventType string
	keyName   string
	start     bool
	forceNew  bool
	end       bool
	byID      bool
	value     func(any) (string, bool)
	decode    func(PayloadCodec, []byte) (any, error)
	invoke    func(context.Context, *SagaContext, any, any) error
	err       error
}

// sagaTypeRuntime is one registered saga type, specialized to its state type.
type sagaTypeRuntime struct {
	name        string
	handlers    map[string][]*sagaEventHandler
	start       []*sagaEventHandler
	deadlines   map[string]*sagaDeadlineHandler
	decodeState func(string) (any, error)
	encodeState func(any) (string, error)
	newState    func() any
}

// sagaRuntime is stage 2: the saga types by event type, and the base context of their work.
type sagaRuntime struct {
	app     *App
	byEvent map[string][]*sagaTypeRuntime
	byName  map[string]*sagaTypeRuntime
}

// newSagaRuntime builds the stage from the registered types, in registration order.
func newSagaRuntime(app *App) *sagaRuntime {
	r := &sagaRuntime{app: app, byEvent: map[string][]*sagaTypeRuntime{}, byName: map[string]*sagaTypeRuntime{}}
	for _, typ := range app.sagaTypes {
		r.byName[typ.name] = typ
		for eventType := range typ.handlers {
			r.byEvent[eventType] = append(r.byEvent[eventType], typ)
		}
	}
	for eventType := range r.byEvent {
		sort.SliceStable(r.byEvent[eventType], func(i, j int) bool {
			return r.byEvent[eventType][i].name < r.byEvent[eventType][j].name
		})
	}
	return r
}

// addSaga records a saga type or the registration error. The App's lock must be held by the
// caller (registration takes it).
func (a *App) addSaga(typ *sagaTypeRuntime, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil && (a.started || a.stopping) {
		err = fmt.Errorf("%w: the saga %s registered after Start", ErrRegistrationClosed, typ.name)
	}
	if err == nil {
		if _, exists := a.sagaByName[typ.name]; exists {
			err = fmt.Errorf("snodra: saga %s is already registered", typ.name)
		}
	}
	if err != nil {
		a.regErrs = append(a.regErrs, err)
		return err
	}
	a.sagaByName[typ.name] = typ
	a.sagaTypes = append(a.sagaTypes, typ)
	return nil
}

// interested reports whether a saga type reacts to eventType.
func (r *sagaRuntime) interested(eventType string) bool {
	return r != nil && len(r.byEvent[eventType]) > 0
}

// sagaMessage is what stage 2 knows of the delivered message: its identity, correlation, trace
// and headers. The payload is passed separately, as stored.
type sagaMessage struct {
	id            string
	typeName      string
	correlationID string
	traceparent   string
	headers       map[string]string
}

// process applies one event to every interested saga type, each in its own transaction. reached
// reports that the event reached at least one saga type's stage (applied, skipped as a duplicate,
// or parked); the error is a store failure, which makes the consumer redeliver the record.
func (r *sagaRuntime) process(ctx context.Context, msg sagaMessage, payload []byte) (reached bool, err error) {
	if r == nil {
		return false, nil
	}
	for _, typ := range r.byEvent[msg.typeName] {
		kind, err := r.deliver(ctx, typ, msg, payload)
		if err != nil {
			return reached, err
		}
		if kind != sagaNoMatch {
			reached = true
		}
	}
	return reached, nil
}

type sagaKind int

const (
	sagaHandled sagaKind = iota
	sagaNoMatch
	sagaDuplicate
)

// deliver runs one saga type with the conflict and handler retries, parking the event when it
// cannot be applied. Conflicts are the transient kind a wait can resolve; a handler failure gets
// two quick attempts and is then parked.
func (r *sagaRuntime) deliver(ctx context.Context, typ *sagaTypeRuntime, msg sagaMessage, payload []byte) (sagaKind, error) {
	settings := r.app.cfg.events
	made, conflicts, failures := 0, 0, 0
	for {
		made++
		kind, err := r.applyOnce(ctx, typ, msg, payload)
		if err == nil {
			return kind, nil
		}
		if errors.Is(err, ErrConflict) {
			conflicts++
			if made >= settings.sagaAttempts {
				return r.park(ctx, typ, msg, payload, err, made)
			}
			r.app.log.Debug("a saga event conflicted; trying again",
				"saga", typ.name, "event", msg.id, "attempt", made, "error", err)
			if !settings.sleep(ctx, settings.sagaBackoff(conflicts+1)) {
				return sagaNoMatch, ctx.Err()
			}
			continue
		}
		failures++
		if failures >= min(settings.sagaHandlerAttempts, settings.sagaAttempts) {
			return r.park(ctx, typ, msg, payload, err, made)
		}
		r.app.log.Debug("a saga handler failed; trying again",
			"saga", typ.name, "event", msg.id, "attempt", made, "error", err)
		if !settings.sleep(ctx, settings.sagaHandlerPause) {
			return sagaNoMatch, ctx.Err()
		}
	}
}

// applyOnce is one transaction: the marker, every matching instance, their state, dispatch and
// deadline changes. A marker recorded even when nothing matched keeps a redelivery from applying
// the event later, as the references do.
func (r *sagaRuntime) applyOnce(ctx context.Context, typ *sagaTypeRuntime, msg sagaMessage, payload []byte) (kind sagaKind, err error) {
	store := r.app.cfg.store
	if store == nil {
		return sagaNoMatch, errors.New("snodra: sagas need a message store")
	}
	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		return sagaNoMatch, fmt.Errorf("begin the saga's transaction: %w", err)
	}
	defer func() {
		if rec := recover(); rec != nil {
			// A user association or a codec panicked: roll the attempt back like any failure,
			// so the marker's reservation never outlives it.
			_ = tx.Rollback(txctx)
			kind = sagaNoMatch
			err = fmt.Errorf("snodra: a saga handling of %s panicked: %v", typ.name, rec)
		}
	}()
	now := r.app.now()
	marker := "saga:" + typ.name + ":" + msg.id
	exists, err := store.Sagas().Markers.Exists(txctx, marker)
	if err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, err
	}
	if exists {
		_ = tx.Rollback(txctx)
		return sagaDuplicate, nil
	}
	if err := store.Sagas().Markers.Insert(txctx, marker, msg.typeName, now); err != nil {
		_ = tx.Rollback(txctx)
		// The marker may be in flight in another transaction: a conflict is retried, not a
		// duplicate acknowledged (the other attempt may still roll back).
		return sagaNoMatch, fmt.Errorf("record the saga marker: %w", err)
	}
	handlers := typ.handlers[msg.typeName]
	if len(handlers) == 0 {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, nil
	}
	event, err := handlers[0].decode(r.app.cfg.codec, payload)
	if err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, err
	}
	matched := map[string]*Saga{}
	handledBy := map[string]map[*sagaEventHandler]bool{}
	for _, handler := range handlers {
		if handler.byID || handler.value == nil {
			continue
		}
		value, ok := handler.value(event)
		if !ok {
			continue
		}
		live, err := store.Sagas().Sagas.FindLive(txctx, typ.name, handler.keyName, value)
		if err != nil {
			_ = tx.Rollback(txctx)
			return sagaNoMatch, err
		}
		for i := range live {
			doc := &live[i]
			matched[doc.ID] = doc
			if handledBy[doc.ID] == nil {
				handledBy[doc.ID] = map[*sagaEventHandler]bool{}
			}
			handledBy[doc.ID][handler] = true
		}
	}
	if msg.typeName == commandFailedTypeName {
		failed, ok := event.(SagaCommandFailed)
		if !ok {
			_ = tx.Rollback(txctx)
			return sagaNoMatch, errors.New("snodra: a SagaCommandFailed event did not decode as one")
		}
		doc, err := store.Sagas().Sagas.FindByID(txctx, failed.SagaID)
		if err != nil {
			_ = tx.Rollback(txctx)
			return sagaNoMatch, err
		}
		if doc != nil && doc.Type == typ.name && !doc.Ended {
			matched[doc.ID] = doc
			if handledBy[doc.ID] == nil {
				handledBy[doc.ID] = map[*sagaEventHandler]bool{}
			}
			for _, handler := range handlers {
				handledBy[doc.ID][handler] = true
			}
		}
	}
	planned := 0
	for _, doc := range matched {
		selected := selectedHandlers(handlers, handledBy[doc.ID])
		if err := r.runInstance(txctx, typ, doc, nil, selected, event, msg, now); err != nil {
			_ = tx.Rollback(txctx)
			return sagaNoMatch, err
		}
		planned++
	}
	// A forceNew start handler starts one instance per matching association, even when live
	// instances matched; it keeps no start key.
	for _, handler := range typ.start {
		if !handler.forceNew || handler.value == nil {
			continue
		}
		value, ok := handler.value(event)
		if !ok {
			continue
		}
		own := handlersFor(handlers, handler.keyName, value, event)
		if err := r.runInstance(txctx, typ, nil, &sagaStart{key: handler.keyName, value: value, keyless: true},
			own, event, msg, now); err != nil {
			_ = tx.Rollback(txctx)
			return sagaNoMatch, err
		}
		planned++
	}
	if len(matched) == 0 {
		for _, handler := range typ.start {
			if handler.forceNew || handler.value == nil {
				continue
			}
			value, ok := handler.value(event)
			if !ok {
				continue
			}
			own := handlersFor(handlers, handler.keyName, value, event)
			if err := r.runInstance(txctx, typ, nil, &sagaStart{key: handler.keyName, value: value},
				own, event, msg, now); err != nil {
				_ = tx.Rollback(txctx)
				return sagaNoMatch, err
			}
			planned++
			break
		}
	}
	if err := tx.Commit(txctx); err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, fmt.Errorf("commit the saga's transaction: %w", err)
	}
	if planned == 0 {
		return sagaNoMatch, nil
	}
	return sagaHandled, nil
}

// sagaStart is the association a start handler used to create an instance. A keyless start
// (ForceNew) keeps no start key.
type sagaStart struct {
	key, value string
	keyless    bool
}

// selectedHandlers picks the handlers of one instance in registration order.
func selectedHandlers(handlers []*sagaEventHandler, selected map[*sagaEventHandler]bool) []*sagaEventHandler {
	out := make([]*sagaEventHandler, 0, len(selected))
	for _, handler := range handlers {
		if selected[handler] {
			out = append(out, handler)
		}
	}
	return out
}

// handlersFor is every handler of the event associated with key = value, in registration order.
func handlersFor(handlers []*sagaEventHandler, key, value string, event any) []*sagaEventHandler {
	out := make([]*sagaEventHandler, 0, len(handlers))
	for _, handler := range handlers {
		if handler.byID || handler.value == nil || handler.keyName != key {
			continue
		}
		if own, ok := handler.value(event); ok && own == value {
			out = append(out, handler)
		}
	}
	return out
}

// runInstance runs selected handlers on one instance (a new one when doc is nil), then writes its
// state, its dispatch entries and its deadline changes in ctx's transaction.
func (r *sagaRuntime) runInstance(ctx context.Context, typ *sagaTypeRuntime, doc *Saga, start *sagaStart,
	handlers []*sagaEventHandler, event any, msg sagaMessage, now time.Time) error {
	var state any
	version := int64(0)
	associations := []SagaAssociation{}
	created := now
	startKey := ""
	if doc != nil {
		var err error
		state, err = typ.decodeState(doc.State)
		if err != nil {
			return fmt.Errorf("decode the state of saga %s: %w", doc.ID, err)
		}
		version = doc.Version + 1
		associations = append(associations, doc.Associations...)
		created = doc.CreatedAt
		if doc.StartKey != nil {
			startKey = *doc.StartKey
		}
	} else {
		state = typ.newState()
		if start != nil {
			associations = append(associations, SagaAssociation{Key: start.key, Value: start.value})
			if !start.keyless {
				startKey = sagaStartKey(typ.name, start.key, start.value)
			}
		}
	}
	id := ""
	if doc != nil {
		id = doc.ID
	} else {
		id = uuid.New()
	}
	saga := &SagaContext{app: r.app, sagaType: typ.name, id: id, version: version,
		correlationID: msg.correlationID, traceparent: msg.traceparent, now: now,
		associations: append([]SagaAssociation(nil), associations...)}
	handlerCtx := withHandling(ctx, sagaHandling(msg))
	for _, handler := range handlers {
		if err := callSagaHandler("event", msg.typeName, func() error {
			return handler.invoke(handlerCtx, saga, state, event)
		}); err != nil {
			return err
		}
		if handler.end {
			saga.end = true
		}
		if saga.end {
			break
		}
	}
	encoded, err := typ.encodeState(state)
	if err != nil {
		return fmt.Errorf("encode the state of saga %s: %w", id, err)
	}
	keptStart := ""
	if startKey != "" {
		for _, association := range saga.associations {
			if sagaStartKey(typ.name, association.Key, association.Value) == startKey {
				keptStart = startKey
				break
			}
		}
	}
	next := &Saga{ID: id, Type: typ.name, State: encoded, Associations: saga.associations,
		Version: version, CreatedAt: created, UpdatedAt: now, Ended: saga.end}
	if keptStart != "" {
		next.StartKey = &keptStart
	}
	if saga.end {
		next.EndedAt = now
	}
	store := r.app.cfg.store
	if doc == nil {
		if err := store.Sagas().Sagas.Insert(ctx, *next); err != nil {
			return err
		}
	} else {
		if ok, err := store.Sagas().Sagas.Update(ctx, *next, doc.Version); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: saga %s changed while handling", ErrConflict, id)
		}
	}
	for _, op := range saga.deadlineOps {
		switch {
		case op.schedule != nil:
			if err := store.Sagas().Deadlines.Insert(ctx, *op.schedule); err != nil {
				return err
			}
		case op.cancelID != "":
			if err := store.Sagas().Deadlines.Cancel(ctx, id, op.cancelID, now); err != nil {
				return err
			}
		case op.cancelName != "":
			if err := store.Sagas().Deadlines.CancelByName(ctx, id, op.cancelName, now); err != nil {
				return err
			}
		}
	}
	if len(saga.entries) > 0 {
		if err := store.Sagas().Dispatch.Record(ctx, saga.entries); err != nil {
			return err
		}
	}
	if saga.end {
		if err := store.Sagas().Deadlines.CancelAll(ctx, id, now); err != nil {
			return err
		}
	}
	return nil
}

// callSagaHandler runs one handler, turning a panic into an error so the event is retried and
// parked (or the deadline's claim lapses) instead of taking the process down.
func callSagaHandler(kind, name string, invoke func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("snodra: a saga %s handler for %s panicked: %v", kind, name, r)
		}
	}()
	return invoke()
}

// sagaHandling is the context a saga handler sees: the delivered message, its correlation and
// its valid trace (none for a deadline's handling), and no caller identity.
func sagaHandling(msg sagaMessage) *handling {
	scope := &handling{event: true, info: MessageInfo{
		ID: msg.id, Type: msg.typeName, CorrelationID: msg.correlationID, Headers: copyStringMap(msg.headers),
	}}
	if msg.traceparent != "" && traceparent.Valid(msg.traceparent) {
		scope.traceparent = msg.traceparent
	}
	return scope
}

// park records the event as a saga failure with its marker, unless the marker exists already.
func (r *sagaRuntime) park(ctx context.Context, typ *sagaTypeRuntime, msg sagaMessage, payload []byte, cause error, attempts int) (sagaKind, error) {
	store := r.app.cfg.store
	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		return sagaNoMatch, err
	}
	now := r.app.now()
	marker := "saga:" + typ.name + ":" + msg.id
	exists, err := store.Sagas().Markers.Exists(txctx, marker)
	if err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, err
	}
	if exists {
		_ = tx.Rollback(txctx)
		return sagaDuplicate, nil
	}
	if err := store.Sagas().Markers.Insert(txctx, marker, msg.typeName, now); err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, fmt.Errorf("record the saga's failure marker: %w", err)
	}
	headers := copyStringMap(msg.headers)
	if headers == nil {
		headers = map[string]string{}
	}
	if headers[HeaderCorrelationID] == "" {
		headers[HeaderCorrelationID] = msg.correlationID
	}
	failure := SagaFailure{ID: typ.name + ":" + msg.id, SagaType: typ.name, EventID: msg.id,
		EventType: msg.typeName, Payload: string(payload), Headers: headers,
		Error: cutSagaError(cause), Attempts: attempts, FirstFailedAt: now, LastFailedAt: now}
	if err := store.Sagas().Failures.Save(txctx, failure); err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, err
	}
	if err := tx.Commit(txctx); err != nil {
		_ = tx.Rollback(txctx)
		return sagaNoMatch, err
	}
	r.app.log.Error("a saga event could not be applied and was parked",
		"saga", typ.name, "event", msg.id, "type", msg.typeName, "attempts", attempts, "error", cause)
	return sagaHandled, nil
}

func cutSagaError(err error) string {
	return cutChars(err.Error(), 2000)
}

// sagaStartKey builds "<type>|<key>=<value>" with %, | and = escaped in every part.
func sagaStartKey(typeName, key, value string) string {
	escape := func(part string) string {
		part = strings.ReplaceAll(part, "%", "%25")
		part = strings.ReplaceAll(part, "|", "%7C")
		return strings.ReplaceAll(part, "=", "%3D")
	}
	return escape(typeName) + "|" + escape(key) + "=" + escape(value)
}

// validTraceparent is the delivered event's traceparent when it is valid, else "".
func validTraceparent(headers map[string]string) string {
	if tp := headers[HeaderTraceparent]; tp != "" && traceparent.Valid(tp) {
		return tp
	}
	return ""
}
