// Package sql is the SQL message-store add-on: the core's snodra.MessageStore on PostgreSQL 13
// or later, or MySQL 8.0.17 or later on InnoDB with a strict sql_mode (MariaDB is not supported).
// It keeps the events a service publishes (the outbox), the processed-event markers, the dead
// letters, the processor control state, the idempotent external calls and the sagas in the
// service's own database, in the tables of the shared SQL layout version 1.
//
// # The transaction is the service's
//
// [Store.Begin] starts a transaction and returns a context that carries it: an append or a marker
// recorded with that context joins it, so the service's rows and the library's commit or roll
// back together. A service that already runs a transaction through database/sql binds it with
// [Store.WithTx] (a *sql.Tx) or [Store.WithConn] (a *sql.Conn the service keeps in a transaction
// of its own); [Store.Tx] returns the *sql.Tx a handler received, so the handler's own writes run
// in the same transaction. A store call with a context that carries none runs in a short
// transaction of its own. Every transaction the store runs on its own (a claim, a purge) is READ
// COMMITTED.
//
// # The layout
//
// The schema files are the published reference files, byte for byte
// (schema/postgresql/V1__snodra.sql and schema/mysql/V1__snodra.sql), embedded in the binary and
// checked against their published SHA-256 values. [Store.InitSchema] applies version 1 unless the
// database has it or later, under the database lock the shared layout fixes, so instances of any
// Snodra library starting together apply it once. [Store.CheckSchema] only checks it, for a
// service that runs its own migrations. Schema versions are additive: code that knows version 1
// reads a database at version 1 or later.
//
// # The stores
//
// The outbox and the processed-event markers are implemented. The dead-letter, processor-control,
// external-call and saga ports are part of the interface frozen at level 2 and return
// snodra.ErrUnsupported for now; the official add-ons implement every port by level 4.
//
// Times the store stamps itself (an entry's sending, a claim's lease) come from the application's
// clock ([Store.UseClock]), never the database's. On PostgreSQL an append's transaction sends
// pg_notify on "snodra_outbox" and [OutboxStore.OnAppend] listens on a connection of its own, so a
// relay claims as soon as an append commits; MySQL has no such notification and the relay polls.
//
// The store is safe for concurrent use. It owns the *sql.DB [Store.DB] returns; [Store.Close]
// closes the pool, ends the listener and nothing else.
package sql

import (
	"context"
	stdSQL "database/sql"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // the "pgx" database/sql driver

	snodra "github.com/snodra/snodra-messaging-go"
)

// The names the store reports (snodra.MessageStore.Name).
const (
	// NamePostgres is the PostgreSQL store's name.
	NamePostgres = "postgres"
	// NameMySQL is the MySQL store's name.
	NameMySQL = "mysql"
)

// Driver is the database the store runs on.
type Driver string

// The drivers the add-on implements.
const (
	Postgres Driver = "postgres"
	MySQL    Driver = "mysql"
)

// The store's defaults. A Config's zero value takes them; the retentions are the shared layout's
// defaults, the same in every SQL add-on.
const (
	// DefaultMaxOpenConns is the default connection pool size. The store needs at least two
	// connections: a write that commits on its own inside the service's transaction takes a
	// second one.
	DefaultMaxOpenConns = 10
	// DefaultMaxIdleConns is the default number of idle connections kept.
	DefaultMaxIdleConns = 5
	// DefaultConnMaxLifetime is how long a pooled connection is reused.
	DefaultConnMaxLifetime = 30 * time.Minute
	// DefaultPurgeInterval is how often the store deletes what the retentions let go.
	DefaultPurgeInterval = time.Minute
	// DefaultOutboxRetention keeps sent outbox entries this long.
	DefaultOutboxRetention = 7 * 24 * time.Hour
	// DefaultProcessedRetention keeps processed-event markers this long.
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
// URL is required and names the database: postgres:// or postgresql:// for PostgreSQL, mysql://
// (or a native go-sql-driver DSN) for MySQL.
type Config struct {
	// URL is the database to use: postgres://user:password@host:port/database?params,
	// postgresql://..., mysql://user:password@host:port/database?params, or a native
	// go-sql-driver/mysql DSN. It is required.
	URL string
	// Driver names the database ("postgres" or "mysql"); empty reads it from URL's scheme.
	Driver Driver
	// MaxOpenConns is the connection pool's size (default 10, at least 2): a write that commits
	// on its own inside the service's transaction takes a second connection.
	MaxOpenConns int
	// MaxIdleConns is how many idle connections the pool keeps (default 5).
	MaxIdleConns int
	// ConnMaxLifetime is how long a pooled connection is reused (default 30 minutes).
	ConnMaxLifetime time.Duration
	// DisableNotify turns off the PostgreSQL wake-up (pg_notify/LISTEN): appends then keep no
	// listener and the relay polls. It has no effect on MySQL, which always polls.
	DisableNotify bool
	// PurgeInterval is how often the background purge runs (default one minute); a negative
	// value turns it off and the service purges by other means.
	PurgeInterval time.Duration
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

// FromEnv returns a Config from the environment: SNODRA_STORE_SQL_URL (required),
// SNODRA_STORE_SQL_DRIVER (optional), SNODRA_STORE_SQL_NOTIFY (true or false, default true) and
// SNODRA_STORE_SQL_PURGE_INTERVAL_MS (milliseconds, default 60000; 0 turns the purge off). The
// other fields keep their defaults; set them on the returned value.
func FromEnv() Config {
	config := Config{URL: strings.TrimSpace(os.Getenv("SNODRA_STORE_SQL_URL"))}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SNODRA_STORE_SQL_DRIVER"))) {
	case "postgres", "postgresql":
		config.Driver = Postgres
	case "mysql":
		config.Driver = MySQL
	}
	if value, ok := os.LookupEnv("SNODRA_STORE_SQL_NOTIFY"); ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "false", "0", "no":
			config.DisableNotify = true
		}
	}
	if value, ok := os.LookupEnv("SNODRA_STORE_SQL_PURGE_INTERVAL_MS"); ok {
		if milliseconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			if milliseconds <= 0 {
				config.PurgeInterval = -1
			} else {
				config.PurgeInterval = time.Duration(milliseconds) * time.Millisecond
			}
		}
	}
	return config
}

// Store is the SQL message store. Construct it with New. Every reference to one Store is that
// store; do not copy the struct.
type Store struct {
	driver   Driver
	db       *stdSQL.DB
	dsn      string
	pgConfig *pgx.ConnConfig
	settings Config
	notifier *notifier

	clockMu sync.RWMutex
	clock   func() time.Time

	closed atomic.Bool
}

// Compile-time checks: the store implements the message store and its lifecycle.
var (
	_ snodra.MessageStore   = (*Store)(nil)
	_ snodra.StoreLifecycle = (*Store)(nil)
)

// New creates the store over config.URL. It does not connect: [Store.InitSchema] or the first
// operation opens the pool, and a bad URL, driver or pool size fails here with
// snodra.ErrInvalid.
func New(config Config) (*Store, error) {
	url := strings.TrimSpace(config.URL)
	if url == "" {
		return nil, invalidf("needs a database URL (Config.URL, SNODRA_STORE_SQL_URL)")
	}
	driver := config.Driver
	if driver == "" {
		switch {
		case strings.HasPrefix(strings.ToLower(url), "postgres://"),
			strings.HasPrefix(strings.ToLower(url), "postgresql://"):
			driver = Postgres
		case strings.HasPrefix(strings.ToLower(url), "mysql://"):
			driver = MySQL
		default:
			// A native go-sql-driver/mysql DSN names a database and carries @tcp(...)/@unix(...).
			if parsed, err := mysql.ParseDSN(url); err == nil && parsed.DBName != "" {
				driver = MySQL
			} else {
				return nil, invalidf("cannot tell the database from the URL's scheme; set Config.Driver to %q or %q (SNODRA_STORE_SQL_DRIVER)", Postgres, MySQL)
			}
		}
	}
	settings := config
	if settings.MaxOpenConns == 0 {
		settings.MaxOpenConns = DefaultMaxOpenConns
	}
	if settings.MaxOpenConns < 2 {
		return nil, invalidf("needs a connection pool of at least 2 (MaxOpenConns has %d): a write that commits on its own inside the service's transaction takes a second connection", settings.MaxOpenConns)
	}
	if settings.MaxIdleConns == 0 {
		settings.MaxIdleConns = DefaultMaxIdleConns
	}
	if settings.MaxIdleConns < 0 {
		return nil, invalidf("needs a MaxIdleConns of at least 0, has %d", settings.MaxIdleConns)
	}
	if settings.ConnMaxLifetime == 0 {
		settings.ConnMaxLifetime = DefaultConnMaxLifetime
	}
	if settings.ConnMaxLifetime < 0 {
		return nil, invalidf("needs a ConnMaxLifetime of at least 0, has %v", settings.ConnMaxLifetime)
	}
	if settings.PurgeInterval == 0 {
		settings.PurgeInterval = DefaultPurgeInterval
	}
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
			return nil, invalidf("needs a %s of at least 0, has %v", retention.name, retention.value)
		}
	}
	settings.Driver = driver

	store := &Store{driver: driver, settings: settings}
	switch driver {
	case Postgres:
		pgConfig, err := pgx.ParseConfig(url)
		if err != nil {
			return nil, invalidf("cannot read the PostgreSQL URL: %v", err)
		}
		db, err := stdSQL.Open("pgx", url)
		if err != nil {
			return nil, invalidf("cannot open the PostgreSQL pool: %v", err)
		}
		store.db = db
		store.dsn = url
		store.pgConfig = pgConfig
	case MySQL:
		dsn, err := mysqlDSN(url)
		if err != nil {
			return nil, err
		}
		db, err := stdSQL.Open("mysql", dsn)
		if err != nil {
			return nil, invalidf("cannot open the MySQL pool: %v", err)
		}
		store.db = db
		store.dsn = dsn
	default:
		return nil, invalidf("has an unknown driver %q; it supports %q and %q", driver, Postgres, MySQL)
	}
	store.db.SetMaxOpenConns(settings.MaxOpenConns)
	store.db.SetMaxIdleConns(settings.MaxIdleConns)
	store.db.SetConnMaxLifetime(settings.ConnMaxLifetime)
	store.clock = time.Now
	if driver == Postgres && !settings.DisableNotify {
		store.notifier = newNotifier(store)
	}
	return store, nil
}

// mysqlDSN turns a mysql:// URL or a native go-sql-driver DSN into the DSN the driver reads,
// with times parsed into time.Time in UTC (the layout stores UTC instants).
func mysqlDSN(url string) (string, error) {
	var config *mysql.Config
	var err error
	if strings.HasPrefix(strings.ToLower(url), "mysql://") {
		config, err = mysqlURLConfig(url)
	} else {
		config, err = mysql.ParseDSN(url)
	}
	if err != nil {
		return "", invalidf("cannot read the MySQL URL: %v", err)
	}
	if config.DBName == "" {
		return "", invalidf("the MySQL URL names no database")
	}
	delete(config.Params, "parseTime")
	delete(config.Params, "loc")
	delete(config.Params, "multiStatements")
	config.ParseTime = true
	config.Loc = time.UTC
	config.MultiStatements = false
	return config.FormatDSN(), nil
}

// Name returns the store's name: "postgres" or "mysql".
func (s *Store) Name() string { return string(s.driver) }

// DB is the connection pool the store uses. The service may begin its own transaction on it
// (BeginTx, at whatever isolation it needs) and bind it with [Store.WithTx]; the store closes the
// pool when [Store.Close] runs.
func (s *Store) DB() *stdSQL.DB { return s.db }

// Settings returns the store's normalized settings.
func (s *Store) Settings() Config { return s.settings }

// UseClock sets the application's clock: what the store stamps the times it sets itself with (an
// outbox entry's sending, a claim's lease, a purge bound). A nil clock restores the wall clock.
// The clock must not call the store.
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

// Close closes the connection pool and stops the PostgreSQL listener. The store is not usable
// afterwards. Calling Close more than once is a no-op.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	if s.notifier != nil {
		s.notifier.stop()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Begin starts a transaction and returns a context that carries it: Outbox.Append and
// Processed.Record called with it join it. The transaction runs at the database's default
// isolation, as the service's own would.
func (s *Store) Begin(ctx context.Context) (context.Context, snodra.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, storeError(err)
	}
	started := &storeTx{store: s, tx: tx}
	return context.WithValue(ctx, storeTxKey{}, started), started, nil
}

// Outbox returns the outbox.
func (s *Store) Outbox() snodra.OutboxStore { return sqlOutbox{store: s} }

// Processed returns the processed-event markers.
func (s *Store) Processed() snodra.ProcessedStore { return sqlProcessed{store: s} }

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

// ---- transactions -------------------------------------------------------------------------------

type storeTxKey struct{}
type serviceTxKey struct{}
type serviceConnKey struct{}

// storeTx is a transaction Begin started. The context Begin returns carries it; Commit and
// Rollback end it, and a write after it ended runs in a short transaction of its own.
type storeTx struct {
	store *Store
	tx    *stdSQL.Tx
	mu    sync.Mutex
	ended atomic.Bool
}

// Commit writes the transaction, all or nothing.
func (t *storeTx) Commit(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended.Swap(true) {
		return nil
	}
	return storeError(t.tx.Commit())
}

// Rollback abandons the transaction; it never fails, and a transaction that ended already is a
// no-op.
func (t *storeTx) Rollback(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended.Swap(true) {
		return nil
	}
	_ = t.tx.Rollback()
	return nil
}

// WithTx binds the service's own *sql.Tx to ctx: an append or a marker recorded with the returned
// context joins that transaction, so the service's rows and the library's commit or roll back
// together. The transaction must be one on this store's database; the store does not commit or
// roll it back.
func (s *Store) WithTx(ctx context.Context, tx *stdSQL.Tx) context.Context {
	if tx == nil {
		return ctx
	}
	return context.WithValue(ctx, serviceTxKey{}, tx)
}

// WithConn binds the service's own *sql.Conn to ctx: the store runs its statements on that
// connection, so a transaction the service keeps open on it (BEGIN, then its writes, then COMMIT)
// commits the library's writes with it. The store does not begin, commit or roll back anything on
// the connection.
func (s *Store) WithConn(ctx context.Context, conn *stdSQL.Conn) context.Context {
	if conn == nil {
		return ctx
	}
	return context.WithValue(ctx, serviceConnKey{}, conn)
}

// Tx returns the *sql.Tx carried by ctx: the one [Store.Begin] started, or the service's own
// bound with [Store.WithTx]. ok is false for a plain context and for a context carrying only a
// service's *sql.Conn (the service has that connection already). A handler runs its own writes
// with it, so they commit with the library's:
//
//	tx, ok := store.Tx(ctx)
//	if !ok {
//		return errors.New("no message-store transaction in this context")
//	}
//	_, err := tx.ExecContext(ctx, "INSERT INTO orders (id) VALUES ($1)", order.ID)
func (s *Store) Tx(ctx context.Context) (*stdSQL.Tx, bool) {
	if started, ok := ctx.Value(storeTxKey{}).(*storeTx); ok {
		if started.store != s || started.ended.Load() {
			return nil, false
		}
		return started.tx, true
	}
	if serviceTx, ok := ctx.Value(serviceTxKey{}).(*stdSQL.Tx); ok && serviceTx != nil {
		return serviceTx, true
	}
	return nil, false
}

// executor is what a store call runs on: a pooled connection, a transaction, or a service's
// connection.
type executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (stdSQL.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*stdSQL.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *stdSQL.Row
}

// scope returns where a store call runs: the context's transaction or connection when it carries
// one of this store's, else a short transaction of its own (whose commit or rollback finish
// runs).
//
// TODO(spec-gap): SG-39 (a Go context can carry only one store's private transaction key: a
// transaction of another message store is invisible here, so the call runs in its own
// transaction instead of being refused; the reference refuses one. The core has no shared
// transaction marker to detect a foreign one).
func (s *Store) scope(ctx context.Context) (executor, func(error) error, error) {
	if started, ok := ctx.Value(storeTxKey{}).(*storeTx); ok {
		if started.store != s {
			return nil, nil, invalidf("was given another SQL message store's transaction")
		}
		if !started.ended.Load() {
			return started.tx, noFinish, nil
		}
		// The transaction ended: what is written now commits on its own.
	}
	if serviceTx, ok := ctx.Value(serviceTxKey{}).(*stdSQL.Tx); ok && serviceTx != nil {
		return serviceTx, noFinish, nil
	}
	if serviceConn, ok := ctx.Value(serviceConnKey{}).(*stdSQL.Conn); ok && serviceConn != nil {
		return serviceConn, noFinish, nil
	}
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return tx, func(result error) error {
		return finishOwn(tx, result)
	}, nil
}

// noFinish is the finish of a joined transaction: the caller ends it.
func noFinish(error) error { return nil }

// finishOwn commits the store's own transaction on success, else rolls it back; a failed commit
// is the failure.
func finishOwn(tx *stdSQL.Tx, result error) error {
	if result == nil {
		return storeError(tx.Commit())
	}
	_ = tx.Rollback()
	return result
}

// beginOwn starts a transaction of the store's own, at READ COMMITTED: on MySQL a locking read at
// REPEATABLE READ keeps every row it scanned locked, so one relay's claim would hide the rows
// another holds from that relay's own SKIP LOCKED claim.
func (s *Store) beginOwn(ctx context.Context) (*stdSQL.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &stdSQL.TxOptions{Isolation: stdSQL.LevelReadCommitted})
	if err != nil {
		return nil, storeError(err)
	}
	return tx, nil
}

// driverName is the database/sql driver's name.
func (s *Store) driverName() string {
	if s.driver == Postgres {
		return "pgx"
	}
	return "mysql"
}

// quoteIdentifier quotes a database name for the test helpers and the schema messages.
func quoteIdentifier(driver Driver, name string) string {
	if driver == MySQL {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// mysqlURLConfig reads a mysql:// URL as the driver's config: user:password@host:port/database
// and the query parameters as DSN parameters.
func mysqlURLConfig(raw string) (*mysql.Config, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = parsed.Host
	if parsed.Port() == "" {
		config.Addr = net.JoinHostPort(parsed.Hostname(), "3306")
	}
	if parsed.User != nil {
		config.User = parsed.User.Username()
		if password, ok := parsed.User.Password(); ok {
			config.Passwd = password
		}
	}
	database, err := url.PathUnescape(strings.TrimPrefix(parsed.Path, "/"))
	if err != nil {
		return nil, err
	}
	if database == "" {
		return nil, errors.New("the MySQL URL names no database")
	}
	config.DBName = database
	config.Params = make(map[string]string, len(parsed.Query()))
	for name, values := range parsed.Query() {
		if len(values) > 0 {
			config.Params[name] = values[0]
		}
	}
	return config, nil
}

// bind turns the store's '?' placeholders into PostgreSQL's numbered ones; MySQL keeps '?
// placeholders. No statement has a literal question mark.
func (s *Store) bind(query string) string {
	if s.driver != Postgres {
		return query
	}
	var bound strings.Builder
	bound.Grow(len(query) + 8)
	number := 0
	for _, c := range query {
		if c == '?' {
			number++
			bound.WriteByte('$')
			bound.WriteString(strconv.Itoa(number))
			continue
		}
		bound.WriteRune(c)
	}
	return bound.String()
}

// jsonType is the column type a JSON value is cast to.
func (s *Store) jsonType() string {
	if s.driver == Postgres {
		return "jsonb"
	}
	return "JSON"
}

// textType is the type a JSON column is read as text.
func (s *Store) textType() string {
	if s.driver == Postgres {
		return "TEXT"
	}
	return "CHAR"
}

// ownUpdate runs one statement in a transaction of its own at READ COMMITTED, and commits it.
func (s *Store) ownUpdate(ctx context.Context, query string, args ...any) error {
	tx, err := s.beginOwn(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.bind(query), args...); err != nil {
		_ = tx.Rollback()
		return storeError(err)
	}
	return storeError(tx.Commit())
}
