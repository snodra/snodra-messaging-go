package mongo

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The image is pinned by its multi-architecture index digest, so a run uses the same MongoDB
// every time.
const (
	mongoImage  = "mongo:8.0.15@sha256:f4d54619262ae3bc6a0a8efbebcef970b87b8ad70697479a75ce308a6f400158"
	replicaSet  = "rs0"
	mongoPort   = "27017/tcp"
	startBudget = 5 * time.Minute
)

// testServer is one replica set container for the whole test run; every test gets a fresh,
// uniquely named database in it and drops it when the test ends.
type testServer struct {
	endpoint  string
	container testcontainers.Container
}

var (
	mongoOnce   sync.Once
	mongoServer *testServer
	mongoErr    error
)

func TestMain(m *testing.M) {
	code := m.Run()
	terminate(mongoServer)
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

// mongoContainer returns the run's replica-set container, starting it on first use. A standalone
// MongoDB has no transactions, so the store's tests need the replica set. Without a working Docker
// the integration tests skip; the layout and error tests still run.
func mongoContainer(t *testing.T) *testServer {
	t.Helper()
	mongoOnce.Do(func() {
		var last error
		for attempt := 0; attempt < 3; attempt++ {
			server, err := startMongo()
			if err != nil {
				last = err
				continue
			}
			mongoServer = server
			return
		}
		mongoErr = fmt.Errorf("no MongoDB replica set after three starts: %w", last)
	})
	if mongoErr != nil {
		t.Skipf("the MongoDB tests need Docker: %v", mongoErr)
	}
	return mongoServer
}

// startMongo starts one single-node replica set and waits until it is the writable primary.
func startMongo() (*testServer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), startBudget)
	defer cancel()
	container, err := mongodb.Run(ctx, mongoImage, mongodb.WithReplicaSet(replicaSet))
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		return nil, err
	}
	endpoint, err := container.PortEndpoint(ctx, mongoPort, "")
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, err
	}
	server := &testServer{endpoint: endpoint, container: container}
	if err := server.waitForPrimary(); err != nil {
		_ = container.Terminate(context.Background())
		return nil, err
	}
	return server, nil
}

// uri is the connection string of the replica set. directConnection makes the driver talk to the
// mapped port rather than the member address the container advertises, which the host may not
// reach (Docker Desktop); a transaction still runs on the primary directly.
func (s *testServer) uri() string {
	parsed := url.URL{Scheme: "mongodb", Host: s.endpoint, Path: "/"}
	parsed.RawQuery = "directConnection=true"
	return parsed.String()
}

// waitForPrimary retries until the server reports itself the writable primary.
func (s *testServer) waitForPrimary() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(s.uri()))
	if err != nil {
		return err
	}
	defer func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer disconnectCancel()
		_ = client.Disconnect(disconnectCtx)
	}()
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Second)
		var hello bson.M
		last = client.Database("admin").RunCommand(queryCtx, bson.D{{Key: "hello", Value: int32(1)}}).Decode(&hello)
		queryCancel()
		if last == nil {
			if primary, ok := hello["isWritablePrimary"].(bool); ok && primary {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("the replica set never became the writable primary: %w", last)
}

// database creates a fresh, uniquely named database and arranges its removal after the test. The
// store's Close is registered later, so it runs before the drop.
func (s *testServer) database(t *testing.T) string {
	t.Helper()
	name := "snodra_test_" + strings.ToLower(rand.Text())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		client, err := mongo.Connect(options.Client().ApplyURI(s.uri()))
		if err != nil {
			t.Logf("dropping the test database %s: %v", name, err)
			return
		}
		defer func() {
			disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer disconnectCancel()
			_ = client.Disconnect(disconnectCtx)
		}()
		if err := client.Database(name).Drop(ctx); err != nil {
			t.Logf("dropping the test database %s: %v", name, err)
		}
	})
	return name
}

// newStore creates a store over a fresh database. With schema it creates the layout's indexes;
// the store and its database are removed when the test ends.
func (s *testServer) newStore(t *testing.T, schema bool) *Store {
	t.Helper()
	return s.newStoreWith(t, schema, Config{})
}

// newStoreWith is newStore with settings of its own.
func (s *testServer) newStoreWith(t *testing.T, schema bool, settings Config) *Store {
	t.Helper()
	database := s.database(t)
	settings.URI = s.uri()
	settings.Database = database
	store, err := New(settings)
	if err != nil {
		t.Fatalf("the store over %s: %v", database, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = store.Close(ctx)
	})
	if schema {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := store.InitSchema(ctx, nil); err != nil {
			t.Fatalf("creating the indexes in %s: %v", database, err)
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

// testEntry is an outbox entry with a fresh id and correlation id.
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

// testMarker is a processed-event marker that carries no position.
func testMarker() snodra.ProcessedMarker {
	return snodra.ProcessedMarker{
		ID:          "processor:default:" + strings.ToLower(rand.Text()),
		Type:        "conformance.Happened",
		ProcessedAt: time.Now(),
	}
}
