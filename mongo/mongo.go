// Package mongo is the MongoDB message-store add-on: the core's snodra.MessageStore over the
// service's own MongoDB database, in the collections and with the document fields every Snodra
// library writes, as specified by the published MongoDB layout. Existing data written by another
// Snodra library stays readable without a migration.
//
// # A replica set
//
// The store's transactions are MongoDB multi-document transactions on a client session, which
// MongoDB runs only on a replica set (a single-node one is enough). [Store.Begin] starts a
// transaction and returns a context that carries it: an [snodra.OutboxStore.Append] or a
// [snodra.ProcessedStore.Record] recorded with it commits or rolls back with the service's own
// writes. A service that already runs a MongoDB transaction binds its session with
// [Store.WithSession]; [Store.Session] returns the session a handler received, so the handler's own
// documents join the same transaction. A write outside any transaction is a single-document write
// that commits on its own.
//
// # The layout
//
// The store keeps the shared collections (outbox, processed_events, dead_letters,
// processor_control, external_calls, sagas, saga_dispatch, saga_dispatch_leases, deadlines,
// saga_failures). [Store.InitSchema] creates every index and TTL index idempotently, which also
// creates the collections a transaction writes to before the first transaction does; give every
// instance sharing a database the same retentions. [Store.CheckSchema] only checks the indexes.
// The page https://snodra.damoreira.com/docs/library-authors/mongodb-layout/ is the layout's
// normative reference.
//
// # The stores
//
// The outbox and the processed-event markers are implemented. The dead-letter,
// processor-control, external-call and saga ports of the interface frozen at level 2 return
// snodra.ErrUnsupported for now; the official add-ons implement every port by level 4.
//
// Times the store stamps itself (a claim's lease, an entry's sending and its failure) come from
// the application's clock ([Store.UseClock]), never the database's. MongoDB has no notification
// the add-on listens on: the outbox relay polls, and OnAppend registers nothing.
//
// The store is safe for concurrent use, but a transaction's context is not: MongoDB sessions are
// not safe for concurrent use, so one transaction's writes run one at a time. Store.Close
// disconnects a client the store created and leaves a service's own client open.
package mongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	snodra "github.com/snodra/snodra-messaging-go"
)

// Name is the store's name (snodra.MessageStore.Name), as every Snodra MongoDB add-on reports it.
const Name = "mongo"

// The store's defaults. A Config's zero value takes them; the retentions are the layout's
// defaults, the same in every MongoDB add-on.
const (
	// DefaultOutboxRetention keeps sent outbox entries this long (their sentAt TTL).
	DefaultOutboxRetention = 7 * 24 * time.Hour
	// DefaultProcessedRetention keeps processed-event markers this long (their processedAt TTL).
	DefaultProcessedRetention = 30 * 24 * time.Hour
	// DefaultExternalCallRetention keeps external-call records this long after their last update.
	DefaultExternalCallRetention = 30 * 24 * time.Hour
	// DefaultSagaEndedRetention keeps ended sagas this long.
	DefaultSagaEndedRetention = 30 * 24 * time.Hour
	// DefaultSagaDispatchRetention keeps dispatched saga entries this long.
	DefaultSagaDispatchRetention = 7 * 24 * time.Hour
	// DefaultDeadlineRetention keeps fired or cancelled deadlines this long.
	DefaultDeadlineRetention = 7 * 24 * time.Hour
	// DefaultFailedRetention keeps what failed for good this long: entries given up on and
	// parked saga failures.
	DefaultFailedRetention = 30 * 24 * time.Hour
)

// Config is how the store reaches its database and how long it keeps what the retentions let go.
// URI and Database are required; a client the service already owns is passed to
// [NewWithClient] instead.
type Config struct {
	// URI is the MongoDB connection string (mongodb://... or mongodb+srv://...). It is required
	// for [New]; [NewWithClient] ignores it.
	URI string
	// Database is the service's own database, which holds the shared collections. It is
	// required.
	Database string
	// OutboxRetention keeps sent outbox entries this long (default 7 days).
	OutboxRetention time.Duration
	// ProcessedRetention keeps processed-event markers this long (default 30 days).
	ProcessedRetention time.Duration
	// ExternalCallRetention keeps external-call records this long after their last update
	// (default 30 days).
	ExternalCallRetention time.Duration
	// SagaEndedRetention keeps ended sagas this long (default 30 days).
	SagaEndedRetention time.Duration
	// SagaDispatchRetention keeps dispatched saga entries this long (default 7 days).
	SagaDispatchRetention time.Duration
	// DeadlineRetention keeps fired or cancelled deadlines this long (default 7 days).
	DeadlineRetention time.Duration
	// FailedRetention keeps what failed for good this long (default 30 days): saga entries given
	// up on and parked saga failures.
	FailedRetention time.Duration
}

// FromEnv returns a Config from the environment: SNODRA_STORE_MONGO_URI and
// SNODRA_STORE_MONGO_DATABASE, both required. The retentions keep their defaults; set them on the
// returned value.
func FromEnv() Config {
	return Config{
		URI:      strings.TrimSpace(os.Getenv("SNODRA_STORE_MONGO_URI")),
		Database: strings.TrimSpace(os.Getenv("SNODRA_STORE_MONGO_DATABASE")),
	}
}

// Store is the MongoDB message store. Construct it with [New] or [NewWithClient]. Every reference
// to one Store is that store; do not copy the struct.
type Store struct {
	client     *mongo.Client
	db         *mongo.Database
	settings   Config
	ownsClient bool

	clockMu sync.RWMutex
	clock   func() time.Time

	closed atomic.Bool
}

// Compile-time checks: the store implements the message store and its lifecycle.
var (
	_ snodra.MessageStore   = (*Store)(nil)
	_ snodra.StoreLifecycle = (*Store)(nil)
)

// New creates the store over config.URI and config.Database, with a client it owns and
// [Store.Close] disconnects. It does not reach the server: [Store.InitSchema] or the first
// operation connects, and a bad URI or a missing database fails here with snodra.ErrInvalid.
func New(config Config) (*Store, error) {
	uri := strings.TrimSpace(config.URI)
	if uri == "" {
		return nil, invalidf("needs a connection string (Config.URI, SNODRA_STORE_MONGO_URI)")
	}
	database := strings.TrimSpace(config.Database)
	if database == "" {
		return nil, invalidf("needs a database (Config.Database, SNODRA_STORE_MONGO_DATABASE)")
	}
	settings, err := settingsOf(config, database)
	if err != nil {
		return nil, err
	}
	clientOptions := options.Client().ApplyURI(uri)
	if err := clientOptions.Validate(); err != nil {
		return nil, invalidf("cannot read the connection string: %v", err)
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, invalidf("cannot open a MongoDB client: %v", err)
	}
	return &Store{
		client:     client,
		db:         client.Database(database),
		settings:   settings,
		ownsClient: true,
		clock:      time.Now,
	}, nil
}

// NewWithClient creates the store over database on a client the service owns and configures (its
// TLS, credentials and pool): the store does not disconnect it, [Store.Close] does nothing to it,
// and config.URI is ignored. A nil client or an empty database is snodra.ErrInvalid.
func NewWithClient(client *mongo.Client, database string, config Config) (*Store, error) {
	if client == nil {
		return nil, invalidf("needs a MongoDB client")
	}
	database = strings.TrimSpace(database)
	if database == "" {
		return nil, invalidf("needs a database")
	}
	config.URI = ""
	settings, err := settingsOf(config, database)
	if err != nil {
		return nil, err
	}
	return &Store{
		client:   client,
		db:       client.Database(database),
		settings: settings,
		clock:    time.Now,
	}, nil
}

// settingsOf fills the defaults and validates the retentions.
func settingsOf(config Config, database string) (Config, error) {
	settings := config
	settings.Database = database
	if settings.OutboxRetention == 0 {
		settings.OutboxRetention = DefaultOutboxRetention
	}
	if settings.ProcessedRetention == 0 {
		settings.ProcessedRetention = DefaultProcessedRetention
	}
	if settings.ExternalCallRetention == 0 {
		settings.ExternalCallRetention = DefaultExternalCallRetention
	}
	if settings.SagaEndedRetention == 0 {
		settings.SagaEndedRetention = DefaultSagaEndedRetention
	}
	if settings.SagaDispatchRetention == 0 {
		settings.SagaDispatchRetention = DefaultSagaDispatchRetention
	}
	if settings.DeadlineRetention == 0 {
		settings.DeadlineRetention = DefaultDeadlineRetention
	}
	if settings.FailedRetention == 0 {
		settings.FailedRetention = DefaultFailedRetention
	}
	for _, retention := range []struct {
		name  string
		value time.Duration
	}{
		{"OutboxRetention", settings.OutboxRetention},
		{"ProcessedRetention", settings.ProcessedRetention},
		{"ExternalCallRetention", settings.ExternalCallRetention},
		{"SagaEndedRetention", settings.SagaEndedRetention},
		{"SagaDispatchRetention", settings.SagaDispatchRetention},
		{"DeadlineRetention", settings.DeadlineRetention},
		{"FailedRetention", settings.FailedRetention},
	} {
		if retention.value < 0 {
			return Config{}, invalidf("needs a %s of at least 0, has %v", retention.name, retention.value)
		}
		if seconds := retention.value / time.Second; seconds > 1<<31-1 {
			return Config{}, invalidf("needs a %s of at most %d seconds, has %v", retention.name, int64(1<<31-1), retention.value)
		}
	}
	return settings, nil
}

// Name returns "mongo".
func (s *Store) Name() string { return Name }

// Client is the MongoDB client the store runs on: the one New created, or the service's own.
func (s *Store) Client() *mongo.Client { return s.client }

// Database is the service's database the store keeps its collections in.
func (s *Store) Database() *mongo.Database { return s.db }

// Settings returns the store's normalized settings.
func (s *Store) Settings() Config { return s.settings }

// UseClock sets the application's clock: what the store stamps the times it sets itself with (an
// outbox entry's sending and failure, a claim's lease). A nil clock restores the wall clock. The
// clock must not call the store.
func (s *Store) UseClock(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	s.clockMu.Lock()
	s.clock = now
	s.clockMu.Unlock()
}

// now is the application's clock.
func (s *Store) now() time.Time {
	s.clockMu.RLock()
	clock := s.clock
	s.clockMu.RUnlock()
	if clock == nil {
		return time.Now()
	}
	return clock()
}

// Close disconnects a client the store created and does nothing to a service's own. The store is
// not usable afterwards; calling Close more than once is a no-op.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}
	if s.ownsClient && s.client != nil {
		return s.client.Disconnect(ctx)
	}
	return nil
}

// InitSchema creates every index and TTL index of the layout, idempotently: the outbox relay
// index, the dead letters' three, the saga indexes and the retention TTLs of the store's Config.
// Creating them creates the collections a transaction writes to before the first transaction
// does. Neither an index that exists nor its options change. config is the application's
// read-only settings and is not used.
func (s *Store) InitSchema(ctx context.Context, config *snodra.Config) error {
	return s.ensureIndexes(ctx)
}

// CheckSchema fails unless the database has every collection and index [Store.InitSchema]
// creates, with the same keys, TTLs and uniqueness; it creates nothing. A service that runs its
// own migrations calls it instead of InitSchema.
func (s *Store) CheckSchema(ctx context.Context) error {
	return s.checkIndexes(ctx)
}

// Background runs nothing: MongoDB's TTL monitor deletes expired documents on the server, so the
// store has no purge of its own. It returns at once.
func (s *Store) Background(ctx context.Context) error { return nil }

// Begin starts a transaction on a client session and returns a context that carries it:
// Outbox.Append and Processed.Record called with it join the transaction, and so do the service's
// own writes through [Store.WithSession] (or the session [Store.Session] returns).
func (s *Store) Begin(ctx context.Context) (context.Context, snodra.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	session, err := s.client.StartSession()
	if err != nil {
		return nil, nil, storeError(err)
	}
	if err := session.StartTransaction(); err != nil {
		session.EndSession(context.Background())
		return nil, nil, storeError(err)
	}
	started := &storeTx{store: s, session: session, base: s.committed(ctx)}
	return context.WithValue(mongo.NewSessionContext(ctx, session), txKey{}, started), started, nil
}

// Outbox returns the outbox.
func (s *Store) Outbox() snodra.OutboxStore { return mongoOutbox{store: s} }

// Processed returns the processed-event markers.
func (s *Store) Processed() snodra.ProcessedStore { return mongoProcessed{store: s} }

// DeadLetters returns the dead-letter port, which this level does not implement yet: every
// operation fails with snodra.ErrUnsupported.
func (s *Store) DeadLetters() snodra.DeadLetterStore { return unsupportedDeadLetters{} }

// Control returns the processor-control port, which this level does not implement yet: every
// operation fails with snodra.ErrUnsupported.
func (s *Store) Control() snodra.ProcessorControlStore { return unsupportedControl{} }

// ExternalCalls returns the external-call port, which this level does not implement yet: every
// operation fails with snodra.ErrUnsupported.
func (s *Store) ExternalCalls() snodra.ExternalCallStore { return unsupportedExternalCalls{} }

// Sagas returns the saga ports, which this level does not implement yet: every operation fails
// with snodra.ErrUnsupported.
func (s *Store) Sagas() snodra.SagaStores {
	return snodra.SagaStores{
		Sagas:     unsupportedSagas{},
		Dispatch:  unsupportedSagaDispatch{},
		Deadlines: unsupportedSagaDeadlines{},
		Failures:  unsupportedSagaFailures{},
		Markers:   unsupportedSagaMarkers{},
	}
}

// ---- sessions and transactions ---------------------------------------------------------------

// txKey is the context key of a transaction Begin started.
type txKey struct{}

// serviceSessionKey is the context key of a session the service bound with WithSession.
type serviceSessionKey struct{}

// storeTx is a transaction Begin started: its session, and the context before the session was
// attached, which a write after the transaction ended (and every read of committed state) runs
// with. Commit and Rollback end it.
type storeTx struct {
	store   *Store
	session *mongo.Session
	base    context.Context

	mu    sync.Mutex
	ended atomic.Bool
}

// Commit writes the transaction, all or nothing; then it ends the session.
func (t *storeTx) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended.Swap(true) {
		return nil
	}
	err := t.session.CommitTransaction(ctx)
	endSession(t.session, ctx)
	return storeError(err)
}

// Rollback abandons the transaction; it never fails, and a transaction that ended already is a
// no-op.
func (t *storeTx) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended.Swap(true) {
		return nil
	}
	_ = t.session.AbortTransaction(ctx)
	endSession(t.session, ctx)
	return nil
}

// endSession ends session, with a short context of its own when ctx is done, so a cancelled
// commit or rollback still releases it.
func endSession(session *mongo.Session, ctx context.Context) {
	if ctx == nil || ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	session.EndSession(ctx)
}

// serviceSession is a session the service bound with WithSession, and the context before it was
// attached.
type serviceSession struct {
	session *mongo.Session
	base    context.Context
}

// WithSession binds the service's own session to ctx, so an append or a marker recorded with the
// returned context runs in the transaction the service started on the session, and commits or
// rolls back with the service's writes. The session must come from the store's client
// ([Store.Client] or [Store.Database]'s); the store does not start, commit or abort anything on
// it. A nil session returns ctx, and a session bound over a store transaction replaces it.
func (s *Store) WithSession(ctx context.Context, session *mongo.Session) context.Context {
	if session == nil {
		return ctx
	}
	base := s.committed(ctx)
	bound := &serviceSession{session: session, base: base}
	return context.WithValue(mongo.NewSessionContext(base, session), serviceSessionKey{}, bound)
}

// Session returns the session carried by ctx: the one [Store.Begin] started while it runs, or the
// service's own bound with [Store.WithSession]. A handler runs its own writes in the transaction
// with it, since ctx already carries the session:
//
//	session, ok := store.Session(ctx)
//	if !ok {
//		return errors.New("no message-store session in this context")
//	}
//	_, err := store.Database().Collection("orders").InsertOne(ctx, order)
//
// ok is false for a plain context and for a transaction of another store.
func (s *Store) Session(ctx context.Context) (*mongo.Session, bool) {
	if started, ok := ctx.Value(txKey{}).(*storeTx); ok {
		if started.store != s || started.ended.Load() {
			return nil, false
		}
		return started.session, true
	}
	if bound, ok := ctx.Value(serviceSessionKey{}).(*serviceSession); ok && bound != nil {
		return bound.session, true
	}
	if session := mongo.SessionFromContext(ctx); session != nil {
		return session, true
	}
	return nil, false
}

// scope returns the context an append or a marker runs with: ctx itself when it carries this
// store's live transaction or a service session, the context before it when the transaction
// ended, and a context carrying another store's transaction is refused. The driver reads the
// session from the context, so returning ctx is what joins it.
//
// TODO(spec-gap): SG-41 (a context can carry only one store's private keys: a transaction of a
// different snodra.MessageStore implementation is invisible here, so the call runs on its own
// instead of being refused; another *Store's transaction is refused).
func (s *Store) scope(ctx context.Context) (context.Context, error) {
	if started, ok := ctx.Value(txKey{}).(*storeTx); ok {
		if started.store != s {
			return nil, invalidf("was given another MongoDB message store's transaction")
		}
		if started.ended.Load() {
			return started.base, nil
		}
		return ctx, nil
	}
	return ctx, nil
}

// committed is ctx without a session this store knows: reads of committed state and writes that
// commit on their own use it, so they never join an open transaction. A transaction's base is
// returned for any *Store's transaction, not only this store's, so a call cannot inherit another
// store's session either.
func (s *Store) committed(ctx context.Context) context.Context {
	if started, ok := ctx.Value(txKey{}).(*storeTx); ok {
		return started.base
	}
	if bound, ok := ctx.Value(serviceSessionKey{}).(*serviceSession); ok && bound != nil {
		return bound.base
	}
	return ctx
}

// ---- errors -----------------------------------------------------------------------------------

// The server error codes the store maps to snodra.ErrConflict.
const (
	duplicateKeyCode  = 11000
	writeConflictCode = 112
)

// transientTransaction is the label MongoDB puts on an error a retry of the whole transaction may
// resolve.
const transientTransaction = "TransientTransactionError"

// invalidf is a bad argument: errors.Is finds snodra.ErrInvalid.
func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: the MongoDB message store %s", snodra.ErrInvalid, fmt.Sprintf(format, args...))
}

// isDuplicateKey reports whether err is a duplicate key (code 11000).
func isDuplicateKey(err error) bool { return mongo.IsDuplicateKeyError(err) }

// storeError is what a driver error means to the store: a duplicate key, a write conflict or an
// error labeled a transient transaction error is snodra.ErrConflict (a retry of the whole
// attempt resolves it); every other error is returned as it is.
func storeError(err error) error {
	if err == nil {
		return nil
	}
	var command mongo.CommandError
	if errors.As(err, &command) {
		if command.Code == duplicateKeyCode || command.Code == writeConflictCode ||
			command.HasErrorLabel(transientTransaction) {
			return fmt.Errorf("%w: %s", snodra.ErrConflict, command.Message)
		}
		return err
	}
	var write mongo.WriteException
	if errors.As(err, &write) {
		if write.HasErrorCode(duplicateKeyCode) || write.HasErrorCode(writeConflictCode) ||
			write.HasErrorLabel(transientTransaction) {
			return fmt.Errorf("%w: %s", snodra.ErrConflict, write.Error())
		}
		return err
	}
	if mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("%w: %s", snodra.ErrConflict, err.Error())
	}
	return err
}
