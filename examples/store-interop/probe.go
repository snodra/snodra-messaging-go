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
}

// newProbe builds one family's app. It does not start it: the caller starts every app together.
func newProbe(s settings, family string, logger *slog.Logger) (*probe, error) {
	store, eventStore, database, err := buildStores(s, family)
	if err != nil {
		return nil, err
	}
	p := &probe{
		store:      newObservedStore(store, family, newLedger()),
		eventStore: eventStore,
		settings:   s,
		family:     family,
		language:   s.language,
		run:        s.run,
		source:     sourceName(s.run, family),
		database:   database,
		logger:     logger,
		allowed:    map[string]bool{},
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
	return snodra.HandleReset(p.app, processorName(p.family), p.onReset)
}

// start starts the app and, for the RabbitMQ event-store-mode processor, the subscription that
// receives the records its dead-letter source holds.
func (p *probe) start(ctx context.Context) error {
	if err := p.app.Start(ctx); err != nil {
		return err
	}
	return p.startDlt(ctx)
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
		Instance:      instanceName(p.language, p.family),
		Language:      p.language,
		Family:        p.family,
		Run:           p.run,
		Store:         p.store.Name(),
		EventStore:    p.eventStore.Name(),
		Source:        p.source,
		Database:      p.database,
		Subscription:  subscriptionName(p.family),
		OutboxPending: pending,
		Handled:       handled,
		Duplicates:    duplicates,
		OptedOut:      p.optedOut.Load(),
		Paused:        control.Paused,
		PausedBy:      control.PausedBy,
		OperationID:   operation,
		Replay:        replay,
		DeadLetters:   briefs,
		DeadLetterN:   int64(len(letters)),
		Dlt:           dlt,
		DltTotal:      uint64(len(dlt)),
		Deliveries:    deliveries,
		Markers:       markers,
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
