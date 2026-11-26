package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
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
	logger     *slog.Logger
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
	}
	app, err := snodra.New(
		snodra.Service(serviceName),
		snodra.Instance(instanceName(s.language, family)),
		snodra.Hubs(s.hub),
		snodra.WithStore(p.store),
		snodra.WithEventStore(eventStore),
		snodra.EventsSource(p.source),
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
		// The add-on's producer compresses nothing by default, the codec every official
		// library's consumer reads; the example keeps the default it ships.
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

// register adds the probe's four commands and its event handler.
func (p *probe) register() error {
	if err := snodra.HandleCommand(p.app, p.who); err != nil {
		return err
	}
	if err := snodra.HandleCommand(p.app, p.publish); err != nil {
		return err
	}
	if err := snodra.HandleCommand(p.app, p.publishRaw); err != nil {
		return err
	}
	if err := snodra.HandleCommand(p.app, p.state); err != nil {
		return err
	}
	return snodra.HandleEvent(p.app, p.onPlaced, snodra.InProcessor(processorName(p.family)))
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

// state is the instance's view for the driver: what its consumer delivered, its stores' names and
// the processed markers the driver asks about.
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
		Deliveries:    deliveries,
		Markers:       markers,
	}, nil
}

// onPlaced is the event handler the library's consumer runs in its transaction with the stage-1
// marker; the ledger only keeps what the driver reads.
func (p *probe) onPlaced(_ context.Context, event OrderPlaced) error {
	p.store.ledger.observeHandled(event)
	return nil
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
