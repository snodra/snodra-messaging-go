package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/kafka"
	"github.com/snodra/snodra-messaging-go/mongo"
	"github.com/snodra/snodra-messaging-go/rabbitmq"
	"github.com/snodra/snodra-messaging-go/sql"
)

// probe is one family's app in this process: the library's App on the family's message store and
// event store, its command handlers and the ledger of what its consumer did.
type probe struct {
	app        *snodra.App
	store      *observedStore
	eventStore snodra.EventStore
	settings   settings
	family     string
	language   string
	run        string
	source     string
	database   string
	seq        atomic.Int64
	optedOut   atomic.Uint64
	logger     *slog.Logger

	// dlt is what the dead-letter subscription of an event-store-mode processor received; only
	// the rabbitmq family runs one (its processor publishes to <source>.dlt).
	dltMu  sync.Mutex
	dlt    []DltEvent
	dltSub snodra.EventSubscription
	dltEnd context.CancelFunc

	// allowed are the poison ids the driver lets the handlers take, in this process only: the
	// driver sends Allow to every instance before a retry, so the one the hub picks succeeds.
	allowMu sync.Mutex
	allowed map[string]bool

	// notices counts the ordinary SagaCommandFailed handlers this instance ran per saga: the
	// local notice dispatcher runs them, never the event store.
	noticeMu sync.Mutex
	notices  map[string]int

	// noticeWatch is what this instance's raw subscription to the family source saw of type
	// snodra.SagaCommandFailed: the notice is local, so it stays empty.
	noticeWatchMu  sync.Mutex
	noticeWatch    map[string]int
	noticeWatchSub snodra.EventSubscription
	noticeWatchEnd context.CancelFunc
}

// newProbe builds one family's app. It does not start it: the caller starts every app together.
func newProbe(s settings, family string, logger *slog.Logger) (*probe, error) {
	store, eventStore, database, err := buildStores(s, family)
	if err != nil {
		return nil, err
	}
	p := &probe{
		store:       newObservedStore(store, family, newLedger()),
		eventStore:  eventStore,
		settings:    s,
		family:      family,
		language:    s.language,
		run:         s.run,
		source:      sourceName(s.run, family),
		database:    database,
		logger:      logger,
		allowed:     map[string]bool{},
		notices:     map[string]int{},
		noticeWatch: map[string]int{},
	}
	app, err := snodra.New(
		snodra.Service(serviceName),
		snodra.Instance(instanceName(s.language, family)),
		snodra.Hubs(s.hub),
		snodra.WithStore(p.store),
		snodra.WithEventStore(eventStore),
		snodra.EventsSource(p.source),
		snodra.DeadLetterMode(s.deadLetterMode(family)),
		snodra.Logger(logger),
	)
	if err != nil {
		p.close(logger)
		return nil, fmt.Errorf("the %s app: %w", family, err)
	}
	p.app = app
	if err := p.register(); err != nil {
		p.close(logger)
		return nil, fmt.Errorf("the %s app: %w", family, err)
	}
	return p, nil
}

// buildStores creates the family's message store and event store, and names the database the
// driver checks for the shared families.
func buildStores(s settings, family string) (snodra.MessageStore, snodra.EventStore, string, error) {
	spec := families[family]
	var store snodra.MessageStore
	var database string
	switch family {
	case familyKafka, familyRabbitMQ:
		store = snodra.NewMemoryMessageStore()
	case familyPostgres, familyMySQL:
		database = s.sqlDatabaseName
		named, err := s.sqlDatabase(family)
		if err != nil {
			return nil, nil, "", err
		}
		if named != database {
			return nil, nil, "", fmt.Errorf("the %s database %q is not the run's database %q", family, named, database)
		}
		config := sql.Config{URL: s.postgresURL, Driver: sql.Postgres}
		if family == familyMySQL {
			config = sql.Config{URL: s.mysqlURL, Driver: sql.MySQL}
		}
		created, err := sql.New(config)
		if err != nil {
			return nil, nil, "", fmt.Errorf("the %s store: %w", family, err)
		}
		store = created
	case familyMongo:
		database = s.mongoDatabase
		created, err := mongo.New(mongo.Config{URI: s.mongoURI, Database: database})
		if err != nil {
			return nil, nil, "", fmt.Errorf("the mongo store: %w", err)
		}
		store = created
	default:
		return nil, nil, "", fmt.Errorf("the unknown family %q", family)
	}
	var eventStore snodra.EventStore
	switch spec.event {
	case "kafka":
		// The add-on's producer compresses nothing by default, the codec every consumer reads
		// without an optional library; the example keeps the default it ships.
		created, err := kafka.New(kafka.Config{BootstrapServers: s.kafkaBootstrap})
		if err != nil {
			closeStore(store)
			return nil, nil, "", fmt.Errorf("the kafka event store: %w", err)
		}
		eventStore = created
	case "rabbitmq-streams":
		created, err := rabbitmq.New(rabbitmq.Config{
			Hosts:    []string{s.rabbitMQHost},
			Port:     s.rabbitMQPort,
			Username: s.rabbitMQUser,
			Password: s.rabbitMQPass,
		})
		if err != nil {
			closeStore(store)
			return nil, nil, "", fmt.Errorf("the rabbitmq event store: %w", err)
		}
		eventStore = created
	default:
		closeStore(store)
		return nil, nil, "", fmt.Errorf("the unknown event store %q", spec.event)
	}
	return store, eventStore, database, nil
}

// register adds the probe's commands, its event handler and, for a family whose processor uses
// the event-store dead-letter mode, the handler a replay must not run again.
func (p *probe) register() error {
	for _, handler := range []func(*snodra.App) error{
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.who) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.publish) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.publishRaw) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.state) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.controlSet) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.poison) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.allow) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaPublish) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaState) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaDispatch) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaLease) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaRelease) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaNotice) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaSlow) },
		func(app *snodra.App) error { return snodra.HandleCommand(app, p.sagaFail) },
	} {
		if err := handler(p.app); err != nil {
			return err
		}
	}
	if err := snodra.HandleEvent(p.app, p.onPlaced, snodra.InProcessor(processorName(p.family))); err != nil {
		return err
	}
	// The opt-out handler proves a replay skips what it may not run again.
	if err := snodra.HandleEvent(p.app, p.onPlacedOptOut,
		snodra.InProcessor(processorName(p.family)), snodra.DisallowReplay()); err != nil {
		return err
	}
	// The ordinary notice handler runs from the local SagaCommandFailed dispatch; it also makes
	// the default processor report the notice type.
	if err := snodra.HandleEvent(p.app, p.onNotice); err != nil {
		return err
	}
	if err := snodra.HandleReset(p.app, processorName(p.family), p.onReset); err != nil {
		return err
	}
	return snodra.HandleSaga(p.app, SagaType, func(definition *snodra.SagaDefinition[InteropSagaState]) {
		definition.StartOn(snodra.StartedOn(
			snodra.Associate(SagaAssociationKey, func(event SagaStarted) string { return event.OrderID }),
			p.onSagaStarted,
		))
		definition.On(snodra.HandledOn(
			snodra.Associate(SagaAssociationKey, func(event SagaAdvanced) string { return event.OrderID }),
			p.onSagaAdvanced,
		))
		definition.OnDeadline(snodra.Deadline(SagaDeadlineName, p.onSagaDeadline))
		definition.OnCommandFailed(snodra.CommandFailed(p.onSagaCommandFailed))
	})
}

// start starts the app and, for the RabbitMQ event-store-mode processor, the subscription that
// receives the records its dead-letter source holds.
func (p *probe) start(ctx context.Context) error {
	if err := p.app.Start(ctx); err != nil {
		return err
	}
	if err := p.startDlt(ctx); err != nil {
		return err
	}
	return p.startNoticeWatch(ctx)
}

func (p *probe) who(context.Context, Who) (WhoReply, error) {
	return WhoReply{Instance: instanceName(p.language, p.family), Language: p.language, Family: p.family}, nil
}

// publish appends Count events to the outbox; the relay publishes them afterwards.
func (p *probe) publish(ctx context.Context, command Publish) (PublishReply, error) {
	if err := p.check(command.Family, command.Run); err != nil {
		return PublishReply{}, err
	}
	if command.Publisher == "" {
		return PublishReply{}, snodra.InvalidRequest("publisher is required")
	}
	if command.Count < 1 {
		return PublishReply{}, snodra.InvalidRequest("count must be positive")
	}
	if len(command.Keys) == 0 {
		return PublishReply{}, snodra.InvalidRequest("at least one key is required")
	}
	reply := PublishReply{Instance: instanceName(p.language, p.family), IDs: make([]string, 0, command.Count)}
	for i := 0; i < command.Count; i++ {
		key := command.Keys[i%len(command.Keys)]
		id := p.newID(command.Publisher)
		seq := p.seq.Add(1)
		payload, err := json.Marshal(OrderPlaced{
			ID:        id,
			Run:       p.run,
			Publisher: command.Publisher,
			Family:    p.family,
			Key:       key,
			Seq:       seq,
		})
		if err != nil {
			return PublishReply{}, err
		}
		entry := snodra.OutboxEntry{
			ID:        id,
			Type:      eventType,
			Key:       &key,
			Headers:   map[string]string{snodra.HeaderCorrelationID: id},
			Payload:   payload,
			CreatedAt: time.Now(),
			Seq:       seq,
		}
		if err := p.store.Outbox().Append(ctx, entry); err != nil {
			return PublishReply{}, fmt.Errorf("append outbox entry %s: %w", id, err)
		}
		reply.IDs = append(reply.IDs, id)
	}
	return reply, nil
}

// publishRaw puts events straight into the event store, from this instance: the driver uses it to
// choose the partition a delivery comes from.
func (p *probe) publishRaw(ctx context.Context, command PublishRaw) (PublishRawReply, error) {
	if err := p.check(command.Family, command.Run); err != nil {
		return PublishRawReply{}, err
	}
	if len(command.IDs) == 0 {
		return PublishRawReply{}, snodra.InvalidRequest("at least one id is required")
	}
	if len(command.Keys) == 0 {
		return PublishRawReply{}, snodra.InvalidRequest("at least one key is required")
	}
	reply := PublishRawReply{Instance: instanceName(p.language, p.family), Published: make([]string, 0, len(command.IDs))}
	for i, id := range command.IDs {
		if id == "" {
			return PublishRawReply{}, snodra.InvalidRequest("an event id must not be empty")
		}
		key := command.Keys[i%len(command.Keys)]
		payload, err := json.Marshal(OrderPlaced{
			ID:        id,
			Run:       p.run,
			Publisher: command.Publisher,
			Family:    p.family,
			Key:       key,
			Seq:       int64(i + 1),
		})
		if err != nil {
			return PublishRawReply{}, err
		}
		headers := map[string]string{
			snodra.HeaderType:          eventType,
			snodra.HeaderID:            id,
			snodra.HeaderCorrelationID: id,
			snodra.HeaderTimestamp:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
			snodra.HeaderSource:        serviceName,
			snodra.HeaderUserID:        "",
		}
		event := snodra.OutgoingEvent{
			Source:  p.source,
			ID:      id,
			Type:    eventType,
			Key:     &key,
			Headers: headers,
			Payload: payload,
		}
		if err := p.eventStore.Publish(ctx, event); err != nil {
			return PublishRawReply{}, fmt.Errorf("publish event %s: %w", id, err)
		}
		reply.Published = append(reply.Published, id)
	}
	return reply, nil
}

// state is the instance's view for the driver: what its consumer delivered, its processor's
// control state, the dead letters its store lists, the dead-letter events its probe subscription
// received, its stores' names and the processed markers the driver asks about.
func (p *probe) state(ctx context.Context, query State) (StateReply, error) {
	deliveries, handled, duplicates := p.store.ledger.snapshot()
	markers := make(map[string]bool, len(query.Markers))
	for _, id := range query.Markers {
		exists, err := p.store.Processed().Exists(ctx, markerID(p.family, id))
		if err != nil {
			return StateReply{}, fmt.Errorf("read the marker of %s: %w", id, err)
		}
		markers[id] = exists
	}
	pending, err := p.store.Outbox().Pending(ctx)
	if err != nil {
		return StateReply{}, fmt.Errorf("read the outbox: %w", err)
	}
	control, err := p.store.Control().Get(ctx, processorName(p.family))
	if err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return StateReply{}, fmt.Errorf("read the processor control state: %w", err)
	}
	letters, err := p.store.DeadLetters().List(ctx, processorName(p.family), 100)
	if err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return StateReply{}, fmt.Errorf("read the dead letters: %w", err)
	}
	total, err := p.store.DeadLetters().Count(ctx, processorName(p.family))
	if errors.Is(err, snodra.ErrUnsupported) {
		total = 0
	} else if err != nil {
		return StateReply{}, fmt.Errorf("count the dead letters: %w", err)
	}
	p.dltMu.Lock()
	dlt := append([]DltEvent(nil), p.dlt...)
	p.dltMu.Unlock()
	replay := make([]ReplayRange, 0, len(control.Replay))
	for _, open := range control.Replay {
		replay = append(replay, ReplayRange{Segment: open.Segment, From: open.From, Until: open.Until})
	}
	briefs := make([]LetterBrief, 0, len(letters))
	for _, letter := range letters {
		briefs = append(briefs, LetterBrief{
			ID:       letter.ID,
			Reason:   string(letter.Reason),
			EventID:  letter.EventID,
			Attempts: letter.Attempts,
		})
	}
	operation := ""
	if control.Operation != nil {
		operation = control.Operation.ID
	}
	return StateReply{
		Instance:        instanceName(p.language, p.family),
		Language:        p.language,
		Family:          p.family,
		Run:             p.run,
		Store:           p.store.Name(),
		EventStore:      p.eventStore.Name(),
		Source:          p.source,
		Database:        p.database,
		Subscription:    subscriptionName(p.family),
		OutboxPending:   pending,
		Handled:         handled,
		Duplicates:      duplicates,
		OptedOut:        p.optedOut.Load(),
		Paused:          control.Paused,
		PausedBy:        control.PausedBy,
		OperationID:     operation,
		Replay:          replay,
		DeadLetters:     briefs,
		DeadLetterTotal: total,
		Dlt:             dlt,
		DltTotal:        uint64(len(dlt)),
		Deliveries:      deliveries,
		Markers:         markers,
	}, nil
}

// controlSet pauses or resumes this instance's processor by writing the message store's control
// state directly: a pause one language persists this way is read by every instance, the hub's
// own control calls being the other path.
func (p *probe) controlSet(ctx context.Context, command ControlSet) (ControlReply, error) {
	if err := p.check(command.Family, command.Run); err != nil {
		return ControlReply{}, err
	}
	if command.By == "" {
		return ControlReply{}, snodra.InvalidRequest("by is required")
	}
	if err := p.store.Control().SetPaused(ctx, processorName(p.family), command.Paused, command.By, time.Now()); err != nil {
		return ControlReply{}, fmt.Errorf("set the pause: %w", err)
	}
	state, err := p.store.Control().Get(ctx, processorName(p.family))
	if err != nil {
		return ControlReply{}, fmt.Errorf("read the processor control state: %w", err)
	}
	return ControlReply{Instance: instanceName(p.language, p.family), Paused: state.Paused, PausedBy: state.PausedBy}, nil
}

// poison publishes one event that its handler rejects until the driver allows its id: it parks a
// dead letter in mode collection, or reaches <source>.dlt in mode event-store, and a later retry
// or the other language's service can then handle it.
func (p *probe) poison(ctx context.Context, command Poison) (PoisonReply, error) {
	if err := p.check(command.Family, command.Run); err != nil {
		return PoisonReply{}, err
	}
	if command.ID == "" {
		return PoisonReply{}, snodra.InvalidRequest("id is required")
	}
	if command.Key == "" {
		return PoisonReply{}, snodra.InvalidRequest("key is required")
	}
	key := command.Key
	payload, err := json.Marshal(OrderPlaced{
		ID:        command.ID,
		Run:       p.run,
		Publisher: command.Publisher,
		Family:    p.family,
		Key:       key,
		Seq:       1,
		Poison:    true,
	})
	if err != nil {
		return PoisonReply{}, err
	}
	headers := map[string]string{
		snodra.HeaderType:          eventType,
		snodra.HeaderID:            command.ID,
		snodra.HeaderCorrelationID: command.ID,
		snodra.HeaderTimestamp:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		snodra.HeaderSource:        serviceName,
		snodra.HeaderUserID:        "",
	}
	event := snodra.OutgoingEvent{
		Source:  p.source,
		ID:      command.ID,
		Type:    eventType,
		Key:     &key,
		Headers: headers,
		Payload: payload,
	}
	if err := p.eventStore.Publish(ctx, event); err != nil {
		return PoisonReply{}, fmt.Errorf("publish the poison event %s: %w", command.ID, err)
	}
	return PoisonReply{Instance: instanceName(p.language, p.family), Published: command.ID}, nil
}

// allow lets this process's handlers take the poison events of command.IDs.
func (p *probe) allow(ctx context.Context, command Allow) (AllowReply, error) {
	if err := p.check(command.Family, command.Run); err != nil {
		return AllowReply{}, err
	}
	p.allowMu.Lock()
	for _, id := range command.IDs {
		p.allowed[id] = true
	}
	allowed := len(p.allowed)
	p.allowMu.Unlock()
	return AllowReply{Instance: instanceName(p.language, p.family), Allowed: allowed}, nil
}

// isAllowed reports whether the driver let this process take a poison event.
func (p *probe) isAllowed(id string) bool {
	p.allowMu.Lock()
	defer p.allowMu.Unlock()
	return p.allowed[id]
}

// onPlaced is the event handler the library's consumer runs in its transaction with the stage-1
// marker; the ledger keeps the event in that transaction's record and shows it only once the
// transaction commits. A poison event fails until the driver allows its id.
func (p *probe) onPlaced(ctx context.Context, event OrderPlaced) error {
	if event.Poison && !p.isAllowed(event.ID) {
		return fmt.Errorf("the poison event %s is not allowed yet", event.ID)
	}
	p.store.ledger.observeHandled(event, txRecordFrom(ctx))
	return nil
}

// onPlacedOptOut is the same type's second handler, which a replay does not run again; its count
// is how the driver tells a replay from a live delivery.
func (p *probe) onPlacedOptOut(ctx context.Context, event OrderPlaced) error {
	if event.Poison && !p.isAllowed(event.ID) {
		return fmt.Errorf("the poison event %s is not allowed yet", event.ID)
	}
	p.optedOut.Add(1)
	return nil
}

// onReset runs before a reset deletes a marker or moves a position: the driver asserts the hub's
// reset report names one reset handler.
func (p *probe) onReset(ctx context.Context, reset snodra.ResetContext) error {
	p.logger.Info("the probe ran its reset handler",
		"family", p.family, "processor", reset.Processor, "operation", reset.OperationID,
		"ranges", len(reset.Ranges))
	return nil
}

// onSagaStarted starts the shared saga, keeps the association the other language continues by,
// and schedules the deadline the deadline scenario observes.
func (p *probe) onSagaStarted(ctx context.Context, saga *snodra.SagaContext, state *InteropSagaState, event SagaStarted) error {
	state.OrderID = event.OrderID
	state.Run = event.Run
	state.Family = event.Family
	state.Mode = event.Mode
	state.StartedBy = p.language
	state.DeadlineScheduledBy = p.language
	saga.Associate(SagaAssociationKey, event.OrderID)
	_, err := saga.Schedule(2*time.Second, SagaDeadlineName, SagaDeadlinePayload{
		OrderID: event.OrderID, Run: event.Run, Family: event.Family,
	})
	if err != nil {
		return fmt.Errorf("schedule the saga deadline: %w", err)
	}
	p.logger.Info("the probe started a saga", "family", p.family, "saga", saga.SagaID(),
		"order", event.OrderID, "mode", event.Mode, "language", p.language)
	return nil
}

// onSagaAdvanced continues the shared saga in this instance's language and records the command
// the lease or notice scenario needs.
func (p *probe) onSagaAdvanced(ctx context.Context, saga *snodra.SagaContext, state *InteropSagaState, event SagaAdvanced) error {
	state.AdvancedBy = p.language
	state.Advances++
	p.logger.Info("the probe continued a saga", "family", p.family, "saga", saga.SagaID(),
		"order", event.OrderID, "mode", event.Mode, "language", p.language)
	switch event.Mode {
	case "lease":
		state.SlowRecordedBy = p.language
		if err := saga.Send(SagaSlow{SagaID: saga.SagaID(), OrderID: event.OrderID}, ""); err != nil {
			return err
		}
	case "notice":
		state.NoticeCommandType = SagaFailType
		if err := saga.Send(SagaFail{SagaID: saga.SagaID(), OrderID: event.OrderID}, ""); err != nil {
			return err
		}
	}
	return nil
}

// onSagaDeadline runs when the shared scheduler claims the deadline; the language that claims it
// is what the driver asserts.
func (p *probe) onSagaDeadline(ctx context.Context, saga *snodra.SagaContext, state *InteropSagaState, payload SagaDeadlinePayload) error {
	state.DeadlineFired = true
	state.DeadlineFiredBy = p.language
	p.logger.Info("the probe fired a saga deadline", "family", p.family, "saga", saga.SagaID(),
		"order", payload.OrderID, "language", p.language)
	return nil
}

// onSagaCommandFailed runs the saga's local notice handler; the ordinary handler is onNotice.
func (p *probe) onSagaCommandFailed(ctx context.Context, saga *snodra.SagaContext, state *InteropSagaState, event snodra.SagaCommandFailed) error {
	state.NoticeBy = p.language
	state.NoticeReason = event.Reason.String()
	state.NoticeCommandType = event.CommandType
	state.NoticeKey = event.IdempotencyKey
	state.NoticeMessage = event.Message
	state.Notices++
	p.logger.Info("the probe's saga received a command failure", "family", p.family,
		"saga", saga.SagaID(), "reason", event.Reason, "language", p.language)
	return nil
}

// onNotice is the ordinary handler of the local failure notice: the notice dispatcher calls it in
// the relay's instance, never through the event store.
func (p *probe) onNotice(ctx context.Context, event snodra.SagaCommandFailed) error {
	p.noticeMu.Lock()
	p.notices[event.SagaID]++
	p.noticeMu.Unlock()
	p.logger.Info("the probe's ordinary notice handler ran", "family", p.family,
		"saga", event.SagaID, "reason", event.Reason, "language", p.language)
	return nil
}

// sagaFail is the command that fails permanently: the saga receives an INVALID notice.
func (p *probe) sagaFail(ctx context.Context, command SagaFail) (SagaFailReply, error) {
	return SagaFailReply{}, snodra.InvalidRequest("the saga fail command fails on purpose")
}

// sagaSlow holds the dispatch lease while it works: it tells the driver which language's
// instance runs it, then waits long enough for the other language's lease probe.
func (p *probe) sagaSlow(ctx context.Context, command SagaSlow) (SagaSlowReply, error) {
	if _, err := snodra.Send[snodra.Empty](ctx, p.app, SagaSlowStarted{
		SagaID: command.SagaID, OrderID: command.OrderID, Language: p.language,
	}); err != nil {
		return SagaSlowReply{}, fmt.Errorf("tell the driver the slow command started: %w", err)
	}
	time.Sleep(3 * time.Second)
	return SagaSlowReply{}, nil
}

// sagaPublish puts one saga event in the event store on the driver's key: that partition's owner
// consumes it and runs the saga stage.
func (p *probe) sagaPublish(ctx context.Context, command SagaPublish) (SagaPublishReply, error) {
	if err := p.check(command.Family, command.Run); err != nil {
		return SagaPublishReply{}, err
	}
	if command.ID == "" || command.Key == "" || command.OrderID == "" {
		return SagaPublishReply{}, snodra.InvalidRequest("id, key and orderId are required")
	}
	var (
		typeName string
		payload  []byte
		err      error
	)
	switch command.Kind {
	case "started":
		typeName = SagaStartedType
		payload, err = json.Marshal(SagaStarted{
			OrderID: command.OrderID, Run: p.run, Family: p.family, Mode: command.Mode,
		})
	case "advanced":
		typeName = SagaAdvancedType
		payload, err = json.Marshal(SagaAdvanced{
			OrderID: command.OrderID, Run: p.run, Family: p.family, Mode: command.Mode,
		})
	default:
		return SagaPublishReply{}, snodra.InvalidRequest("kind is started or advanced")
	}
	if err != nil {
		return SagaPublishReply{}, err
	}
	key := command.Key
	headers := map[string]string{
		snodra.HeaderType:          typeName,
		snodra.HeaderID:            command.ID,
		snodra.HeaderCorrelationID: command.ID,
		snodra.HeaderTimestamp:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		snodra.HeaderSource:        serviceName,
		snodra.HeaderUserID:        "",
	}
	if err := p.eventStore.Publish(ctx, snodra.OutgoingEvent{
		Source: p.source, ID: command.ID, Type: typeName, Key: &key, Headers: headers, Payload: payload,
	}); err != nil {
		return SagaPublishReply{}, fmt.Errorf("publish the saga event %s: %w", command.ID, err)
	}
	return SagaPublishReply{Instance: instanceName(p.language, p.family), Published: command.ID}, nil
}

// sagaState reads one shared saga by id, or by its live association.
func (p *probe) sagaState(ctx context.Context, request SagaStateRequest) (SagaStateReply, error) {
	if err := p.check(request.Family, request.Run); err != nil {
		return SagaStateReply{}, err
	}
	if request.Type == "" {
		return SagaStateReply{}, snodra.InvalidRequest("type is required")
	}
	var (
		saga *snodra.Saga
		err  error
	)
	if request.SagaID != "" {
		saga, err = p.store.Sagas().Sagas.FindByID(ctx, request.SagaID)
	} else {
		var found []snodra.Saga
		found, err = p.store.Sagas().Sagas.FindLive(ctx, request.Type, request.Key, request.Value)
		if len(found) > 0 {
			saga = &found[0]
		}
	}
	if err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return SagaStateReply{}, fmt.Errorf("read the saga: %w", err)
	}
	reply := SagaStateReply{Instance: instanceName(p.language, p.family)}
	if saga == nil {
		return reply, nil
	}
	reply.Found = true
	reply.SagaID = saga.ID
	reply.Type = saga.Type
	reply.State = json.RawMessage(saga.State)
	reply.Version = saga.Version
	reply.Ended = saga.Ended
	for _, association := range saga.Associations {
		reply.Associations = append(reply.Associations, SagaAssociationView{Key: association.Key, Value: association.Value})
	}
	return reply, nil
}

// sagaDispatch reads the first pending dispatch entry and the pending count of a shared saga.
func (p *probe) sagaDispatch(ctx context.Context, request SagaDispatchRequest) (SagaDispatchReply, error) {
	if err := p.check(request.Family, request.Run); err != nil {
		return SagaDispatchReply{}, err
	}
	if request.SagaID == "" {
		return SagaDispatchReply{}, snodra.InvalidRequest("sagaId is required")
	}
	reply := SagaDispatchReply{Instance: instanceName(p.language, p.family)}
	dispatch := p.store.Sagas().Dispatch
	entry, err := dispatch.NextPending(ctx, request.SagaID)
	if err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return SagaDispatchReply{}, fmt.Errorf("read the next pending entry: %w", err)
	}
	if entry != nil {
		reply.Pending = true
		reply.EntryID = entry.ID
		reply.Seq = entry.Seq
		reply.Kind = string(entry.Kind)
		reply.Type = entry.Type
		reply.Attempts = entry.Attempts
	}
	pending, err := dispatch.Pending(ctx)
	if err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return SagaDispatchReply{}, fmt.Errorf("count the pending entries: %w", err)
	}
	reply.PendingCount = pending
	return reply, nil
}

// sagaLease tries the dispatch lease of a saga as a probe owner: while a relay holds it, the
// other language's attempt must fail.
func (p *probe) sagaLease(ctx context.Context, request SagaLeaseRequest) (SagaLeaseReply, error) {
	if err := p.check(request.Family, request.Run); err != nil {
		return SagaLeaseReply{}, err
	}
	if request.SagaID == "" || request.Owner == "" || request.UntilMs <= 0 {
		return SagaLeaseReply{}, snodra.InvalidRequest("sagaId, owner and untilMs are required")
	}
	now := time.Now()
	taken, err := p.store.Sagas().Dispatch.ClaimLease(ctx, request.SagaID, request.Owner, now, time.UnixMilli(request.UntilMs))
	if err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return SagaLeaseReply{}, fmt.Errorf("claim the dispatch lease: %w", err)
	}
	return SagaLeaseReply{Instance: instanceName(p.language, p.family), Taken: taken}, nil
}

// sagaRelease releases a probe owner's dispatch lease.
func (p *probe) sagaRelease(ctx context.Context, request SagaReleaseRequest) (SagaReleaseReply, error) {
	if err := p.check(request.Family, request.Run); err != nil {
		return SagaReleaseReply{}, err
	}
	if request.SagaID == "" || request.Owner == "" {
		return SagaReleaseReply{}, snodra.InvalidRequest("sagaId and owner are required")
	}
	// ReleaseLease has no result: read the lease through a claim first? The port releases when
	// the owner holds it and has no answer, so report that the call was made.
	if err := p.store.Sagas().Dispatch.ReleaseLease(ctx, request.SagaID, request.Owner); err != nil && !errors.Is(err, snodra.ErrUnsupported) {
		return SagaReleaseReply{}, fmt.Errorf("release the dispatch lease: %w", err)
	}
	return SagaReleaseReply{Instance: instanceName(p.language, p.family), Released: true}, nil
}

// sagaNotice reads the local notice count and the event-store deliveries of one notice id.
func (p *probe) sagaNotice(ctx context.Context, request SagaNoticeRequest) (SagaNoticeReply, error) {
	if err := p.check(request.Family, request.Run); err != nil {
		return SagaNoticeReply{}, err
	}
	p.noticeMu.Lock()
	ordinary := p.notices[request.SagaID]
	p.noticeMu.Unlock()
	p.noticeWatchMu.Lock()
	published := p.noticeWatch[request.NoticeID]
	p.noticeWatchMu.Unlock()
	return SagaNoticeReply{
		Instance: instanceName(p.language, p.family), Language: p.language,
		Ordinary: ordinary, Published: published,
	}, nil
}

// startDlt subscribes to the family's dead-letter source when its processor uses the event-store
// mode: the subscription receives what the processor published there, with the six snodra-dlt-*
// headers, for the driver to assert.
func (p *probe) startDlt(ctx context.Context) error {
	if p.settings.deadLetterMode(p.family) != snodra.DeadLetterModeEventStore {
		return nil
	}
	spec := snodra.SubscriptionSpec{
		Name:   serviceName + ".interop-" + p.family + "-dlt-" + p.language,
		Source: snodra.DeadLetterSource(p.source),
	}
	sub, err := p.eventStore.Subscribe(ctx, spec)
	if err != nil {
		return fmt.Errorf("subscribe to the %s dead-letter source: %w", p.family, err)
	}
	watchCtx, stop := context.WithCancel(ctx)
	p.dltSub = sub
	p.dltEnd = stop
	go func() {
		for {
			next, err := sub.Next(watchCtx)
			if err != nil {
				return
			}
			delivery, ok := next.(snodra.Delivery)
			if !ok {
				continue
			}
			event := DltEvent{
				ID:       delivery.Headers[snodra.HeaderID],
				Type:     delivery.Headers[snodra.HeaderType],
				Segment:  delivery.Segment,
				Position: delivery.Position,
				Headers:  delivery.Headers,
				Payload:  append(json.RawMessage(nil), delivery.Payload...),
			}
			if delivery.Key != nil {
				event.Key = *delivery.Key
			}
			p.dltMu.Lock()
			p.dlt = append(p.dlt, event)
			p.dltMu.Unlock()
			if err := sub.Ack(delivery.Segment, delivery.Position); err != nil {
				p.logger.Warn("the dead-letter subscription could not acknowledge", "family", p.family, "error", err)
			}
		}
	}()
	return nil
}

// startNoticeWatch reads the family source as a raw subscription and records a published
// snodra.SagaCommandFailed, if any: the notice must stay local, so this stays empty.
func (p *probe) startNoticeWatch(ctx context.Context) error {
	spec := snodra.SubscriptionSpec{
		Name:   serviceName + ".interop-" + p.family + "-notice-" + p.language,
		Source: p.source,
	}
	sub, err := p.eventStore.Subscribe(ctx, spec)
	if err != nil {
		return fmt.Errorf("subscribe to the %s source for notices: %w", p.family, err)
	}
	watchCtx, stop := context.WithCancel(ctx)
	p.noticeWatchSub = sub
	p.noticeWatchEnd = stop
	go func() {
		for {
			next, err := sub.Next(watchCtx)
			if err != nil {
				return
			}
			delivery, ok := next.(snodra.Delivery)
			if !ok {
				continue
			}
			if delivery.Headers[snodra.HeaderType] == sagaCommandFailedType {
				id := delivery.Headers[snodra.HeaderID]
				p.noticeWatchMu.Lock()
				p.noticeWatch[id]++
				p.noticeWatchMu.Unlock()
			}
			_ = sub.Ack(delivery.Segment, delivery.Position)
		}
	}()
	return nil
}

// stopNoticeWatch ends the notice subscription before the event store closes.
func (p *probe) stopNoticeWatch(ctx context.Context) {
	if p.noticeWatchEnd == nil {
		return
	}
	p.noticeWatchEnd()
	if p.noticeWatchSub != nil {
		_ = p.noticeWatchSub.Close(ctx)
	}
	p.noticeWatchSub = nil
	p.noticeWatchEnd = nil
}

// stopDlt ends the dead-letter subscription before the event store closes.
func (p *probe) stopDlt(ctx context.Context) {
	if p.dltEnd == nil {
		return
	}
	p.dltEnd()
	if p.dltSub != nil {
		_ = p.dltSub.Close(ctx)
	}
	p.dltSub = nil
	p.dltEnd = nil
}

// check refuses a command aimed at another family or run: a misrouted key must fail loudly, never
// write into another family's store.
func (p *probe) check(family, run string) error {
	if family != "" && family != p.family {
		return snodra.InvalidRequest(fmt.Sprintf("this instance is the %s family, not %s", p.family, family))
	}
	if run != "" && run != p.run {
		return snodra.InvalidRequest(fmt.Sprintf("this instance belongs to run %s, not %s", p.run, run))
	}
	return nil
}

// newID is an event id unique to this run, publisher and family.
func (p *probe) newID(publisher string) string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%s-%s-%s-%d", p.run, publisher, p.family, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s-%s-%s", p.run, publisher, p.family, hex.EncodeToString(raw[:]))
}

// close releases the stores the probe owns, after the app stopped using them.
func (p *probe) close(logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	p.stopDlt(ctx)
	p.stopNoticeWatch(ctx)
	cancel()
	if p.eventStore != nil {
		if closer, ok := p.eventStore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				logger.Warn("the event store did not close cleanly", "family", p.family, "error", err)
			}
		}
	}
	if p.store != nil {
		closeStore(p.store.inner)
	}
}

// closeStore releases a message store, when it is one that owns a connection.
func closeStore(store snodra.MessageStore) {
	switch owned := store.(type) {
	case interface{ Close() error }:
		_ = owned.Close()
	case interface{ Close(context.Context) error }:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = owned.Close(ctx)
	}
}
