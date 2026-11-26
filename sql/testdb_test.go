package sql

import (
	"context"
	"crypto/rand"
	stdSQL "database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The images are pinned by their multi-architecture index digests, so a run uses the same
// PostgreSQL and MySQL every time.
const (
	postgresImage = "postgres:17.7@sha256:2006493727bd5277eece187319af1ef82b4cf82cf4fc1ed00da0775b646ac2a4"
	mysqlImage    = "mysql:8.4.7@sha256:0426ec38c7a10aa45ba383887df7878f74ee70e2fd589c7b69207f3577901903"
)

// testServer is one database server container for the whole test run; every test gets a fresh,
// uniquely named database in it and drops it when the test ends.
type testServer struct {
	driver    Driver
	host      string
	port      string
	adminDSN  string
	container testcontainers.Container
}

var (
	postgresOnce   sync.Once
	postgresServer *testServer
	postgresErr    error
	mysqlOnce      sync.Once
	mysqlServer    *testServer
	mysqlErr       error
)

func TestMain(m *testing.M) {
	code := m.Run()
	terminate(postgresServer)
	terminate(mysqlServer)
	os.Exit(code)
}

func terminate(server *testServer) {
	if server == nil || server.container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = server.container.Terminate(ctx)
}

// postgresContainer returns the run's PostgreSQL container, starting it on first use. Without a
// working Docker the integration tests skip; the unit tests still run.
func postgresContainer(t *testing.T) *testServer {
	t.Helper()
	postgresOnce.Do(func() {
		postgresServer, postgresErr = startServer(Postgres, postgresImage, "5432/tcp", map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		}, "postgres://postgres:postgres@%s/postgres?sslmode=disable")
	})
	if postgresErr != nil {
		t.Skipf("the PostgreSQL tests need Docker: %v", postgresErr)
	}
	return postgresServer
}

// mysqlContainer returns the run's MySQL container, starting it on first use.
func mysqlContainer(t *testing.T) *testServer {
	t.Helper()
	mysqlOnce.Do(func() {
		mysqlServer, mysqlErr = startServer(MySQL, mysqlImage, "3306/tcp", map[string]string{
			"MYSQL_ROOT_PASSWORD": "snodra",
			"MYSQL_DATABASE":      "snodra_admin",
		}, "root:snodra@tcp(%s)/snodra_admin?parseTime=true&loc=UTC")
	})
	if mysqlErr != nil {
		t.Skipf("the MySQL tests need Docker: %v", mysqlErr)
	}
	return mysqlServer
}

// startServer starts one container, waits for its port and for the server to answer, and returns
// it with the admin DSN (a %s for host:port). A failed start is tried again, twice; a check is
// never retried.
func startServer(driver Driver, image, port string, env map[string]string, adminFormat string) (*testServer, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		instance, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        image,
				ExposedPorts: []string{port},
				Env:          env,
				WaitingFor:   wait.ForListeningPort(port).WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
		cancel()
		if err != nil {
			last = err
			continue
		}
		endpoint, err := instance.Endpoint(context.Background(), "")
		if err != nil {
			_ = instance.Terminate(context.Background())
			last = err
			continue
		}
		host, mapped, err := net.SplitHostPort(endpoint)
		if err != nil {
			_ = instance.Terminate(context.Background())
			last = err
			continue
		}
		server := &testServer{
			driver:    driver,
			host:      host,
			port:      mapped,
			adminDSN:  fmt.Sprintf(adminFormat, endpoint),
			container: instance,
		}
		if err := server.waitForServer(); err != nil {
			_ = instance.Terminate(context.Background())
			last = err
			continue
		}
		return server, nil
	}
	return nil, fmt.Errorf("no test database after three starts: %w", last)
}

// waitForServer retries until the server answers: the host's port forwarding can lag the
// server's own ready line.
func (s *testServer) waitForServer() error {
	admin, err := stdSQL.Open(s.driverName(), s.adminDSN)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		last = admin.PingContext(ctx)
		cancel()
		if last == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("the test database does not answer: %w", last)
}

// driverName is the database/sql driver that reads this server.
func (s *testServer) driverName() string {
	if s.driver == Postgres {
		return "pgx"
	}
	return "mysql"
}

// url is the store URL of the named database on this server.
func (s *testServer) url(database string) string {
	if s.driver == Postgres {
		return fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", net.JoinHostPort(s.host, s.port), database)
	}
	return fmt.Sprintf("mysql://root:snodra@%s/%s", net.JoinHostPort(s.host, s.port), database)
}

// database creates a fresh, uniquely named database and arranges its removal after the test. The
// store's Close is registered later, so it runs before the drop.
func (s *testServer) database(t *testing.T) string {
	t.Helper()
	name := "snodra_test_" + strings.ToLower(rand.Text())
	admin := s.openAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteIdentifier(s.driver, name)); err != nil {
		_ = admin.Close()
		t.Fatalf("creating the test database %s: %v", name, err)
	}
	_ = admin.Close()
	t.Cleanup(func() {
		drop := s.openAdmin(t)
		defer func() { _ = drop.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		statement := "DROP DATABASE IF EXISTS " + quoteIdentifier(s.driver, name)
		if s.driver == Postgres {
			statement += " WITH (FORCE)"
		}
		if _, err := drop.ExecContext(ctx, statement); err != nil {
			t.Logf("dropping the test database %s: %v", name, err)
		}
	})
	return name
}

// openAdmin opens a connection to the server's admin database.
func (s *testServer) openAdmin(t *testing.T) *stdSQL.DB {
	t.Helper()
	admin, err := stdSQL.Open(s.driverName(), s.adminDSN)
	if err != nil {
		t.Fatalf("opening the admin database: %v", err)
	}
	return admin
}

// newStore creates a store over a fresh database. With schema it applies the shared schema; the
// store and its database are removed when the test ends.
func (s *testServer) newStore(t *testing.T, schema bool) *Store {
	t.Helper()
	database := s.database(t)
	store, err := New(Config{URL: s.url(database)})
	if err != nil {
		t.Fatalf("the store over %s: %v", database, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if schema {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := store.InitSchema(ctx, nil); err != nil {
			t.Fatalf("applying the schema to %s: %v", database, err)
		}
	}
	return store
}

// kitStores prepares one store over a fresh database per check, with its schema ready: the kit
// calls the returned function once per check, so no check sees another's data.
func (s *testServer) kitStores(t *testing.T, count int) func() snodra.MessageStore {
	t.Helper()
	stores := make([]*Store, count)
	for i := range stores {
		stores[i] = s.newStore(t, true)
	}
	next := 0
	return func() snodra.MessageStore {
		if next >= len(stores) {
			t.Fatalf("the kit asked for more than the %d prepared stores", len(stores))
		}
		store := stores[next]
		next++
		return store
	}
}

// waitFor polls check until it is true or within has passed.
func waitFor(t *testing.T, what string, within time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testEntry is an outbox entry with a fresh id and correlation id. Its time is truncated to
// milliseconds, the precision the shared SQL layout keeps.
func testEntry(typeName string, createdAt time.Time, seq int64) snodra.OutboxEntry {
	key := fmt.Sprintf("key-%d", seq)
	return snodra.OutboxEntry{
		ID:        "entry-" + strings.ToLower(rand.Text()),
		Type:      typeName,
		Key:       &key,
		Headers:   map[string]string{snodra.HeaderCorrelationID: "correlation-" + rand.Text(), snodra.HeaderUserID: ""},
		Payload:   []byte(fmt.Sprintf(`{"seq":%d,"text":"é"}`, seq)),
		CreatedAt: createdAt.Truncate(time.Millisecond),
		Seq:       seq,
	}
}

// testMarker is a processed-event marker.
func testMarker() snodra.ProcessedMarker {
	return snodra.ProcessedMarker{
		ID:          "processor:default:" + strings.ToLower(rand.Text()),
		Type:        "conformance.Happened",
		ProcessedAt: time.Now(),
	}
}
