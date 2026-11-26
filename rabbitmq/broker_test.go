package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	snodra "github.com/snodra/snodra-messaging-go"
)

// rabbitImage pins RabbitMQ 4.4.0 with the management plugin by its multi-architecture index
// digest (the image the Rust add-on's tests run).
const rabbitImage = "rabbitmq@sha256:753099a19b577c322117f793f864b3129cbba96d40317221a69d73976e3c843c"

type testBroker struct {
	container     testcontainers.Container
	streamAddress string
	management    string
	env           *stream.Environment
}

var (
	brokerOnce  sync.Once
	brokerValue *testBroker
	brokerErr   error
	runSuffix   = randomSuffix()
	nameNumber  atomic.Uint64
)

func randomSuffix() string {
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(raw[:])
}

func TestMain(m *testing.M) {
	code := m.Run()
	if brokerValue != nil {
		if brokerValue.env != nil {
			_ = brokerValue.env.Close()
		}
		if brokerValue.container != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = brokerValue.container.Terminate(ctx)
			cancel()
		}
	}
	os.Exit(code)
}

// rabbitBroker is the run's broker, started on first use. Without a working Docker the tests
// skip: no other test in this package needs it.
func rabbitBroker(t *testing.T) *testBroker {
	t.Helper()
	brokerOnce.Do(func() {
		brokerValue, brokerErr = startBroker()
	})
	if brokerErr != nil {
		t.Skipf("the RabbitMQ tests need Docker: %v", brokerErr)
	}
	return brokerValue
}

func startBroker() (*testBroker, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		port, err := freePort()
		if err != nil {
			return nil, err
		}
		conf := fmt.Sprintf("stream.advertised_host = 127.0.0.1\nstream.advertised_port = %d\n", port)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		instance, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        rabbitImage,
				ExposedPorts: []string{"5552/tcp", "15672/tcp"},
				Files: []testcontainers.ContainerFile{
					{
						Reader:            strings.NewReader("[rabbitmq_management,rabbitmq_stream,rabbitmq_stream_management].\n"),
						ContainerFilePath: "/etc/rabbitmq/enabled_plugins",
						FileMode:          0o644,
					},
					{
						Reader:            strings.NewReader(conf),
						ContainerFilePath: "/etc/rabbitmq/conf.d/20-stream.conf",
						FileMode:          0o644,
					},
				},
				HostConfigModifier: func(hostConfig *container.HostConfig) {
					hostConfig.PortBindings = network.PortMap{
						network.MustParsePort("5552/tcp"): []network.PortBinding{{
							HostIP:   netip.MustParseAddr("127.0.0.1"),
							HostPort: strconv.Itoa(port),
						}},
					}
				},
				WaitingFor: wait.ForLog("Server startup complete").WithStartupTimeout(120 * time.Second),
			},
			Started: true,
		})
		cancel()
		if err != nil {
			last = err
			if strings.Contains(strings.ToLower(err.Error()), "bind") ||
				strings.Contains(strings.ToLower(err.Error()), "already allocated") ||
				strings.Contains(strings.ToLower(err.Error()), "address already in use") {
				continue
			}
			return nil, err
		}
		broker := &testBroker{
			container:     instance,
			streamAddress: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		}
		managementPort, err := instance.MappedPort(context.Background(), "15672/tcp")
		if err != nil {
			_ = instance.Terminate(context.Background())
			return nil, err
		}
		broker.management = "http://127.0.0.1:" + managementPort.Port()
		if err := broker.waitReady(); err != nil {
			_ = instance.Terminate(context.Background())
			return nil, err
		}
		environment, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().
				SetHost("127.0.0.1").
				SetPort(port).
				SetUser("guest").
				SetPassword("guest"))
		if err != nil {
			_ = instance.Terminate(context.Background())
			return nil, err
		}
		broker.env = environment
		return broker, nil
	}
	return nil, last
}

func (b *testBroker) waitReady() error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		connection, err := (&EventStore{config: Config{
			Hosts:          []string{"127.0.0.1"},
			Port:           portOf(b.streamAddress),
			VirtualHost:    "/",
			Username:       "guest",
			Password:       "guest",
			RequestTimeout: 5 * time.Second,
		}}).dialAny(context.Background(), storeConnection)
		if err == nil {
			_ = connection.Close()
			request, _ := http.NewRequest(http.MethodGet, b.management+"/api/overview", nil)
			request.SetBasicAuth("guest", "guest")
			response, apiErr := http.DefaultClient.Do(request)
			if apiErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("rabbitmq never answered (last: %w)", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func portOf(address string) int {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return 0
	}
	number, _ := strconv.Atoi(port)
	return number
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// uniqueName is a stream name no earlier test or run used.
func uniqueName(prefix string) string {
	return prefix + "." + runSuffix + "." + strconv.FormatUint(nameNumber.Add(1), 10)
}

// superStream creates a super stream with partitions and deletes it after the test.
func (b *testBroker) superStream(t *testing.T, partitions int) string {
	t.Helper()
	name := uniqueName("snodra.events")
	if err := b.env.DeclareSuperStream(name, stream.NewPartitionsOptions(partitions)); err != nil {
		t.Fatalf("creating the super stream %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := b.env.DeleteSuperStream(name); err != nil {
			t.Logf("deleting the super stream %s: %v", name, err)
		}
	})
	return name
}

// stream creates a plain stream and deletes it after the test.
func (b *testBroker) stream(t *testing.T) string {
	t.Helper()
	return b.streamNamed(t, uniqueName("snodra.events.dlt"))
}

// streamNamed creates a plain stream and deletes it after the test.
func (b *testBroker) streamNamed(t *testing.T, name string) string {
	t.Helper()
	if err := b.env.DeclareStream(name, stream.NewStreamOptions()); err != nil {
		t.Fatalf("creating the stream %s: %v", name, err)
	}
	t.Cleanup(func() {
		if err := b.env.DeleteStream(name); err != nil {
			t.Logf("deleting the stream %s: %v", name, err)
		}
	})
	return name
}

// storeSettings is this broker's store settings, with the management API.
func (b *testBroker) storeSettings() Config {
	return Config{
		Hosts:         []string{"127.0.0.1"},
		Port:          portOf(b.streamAddress),
		VirtualHost:   "/",
		Username:      "guest",
		Password:      "guest",
		ManagementURL: b.management,
	}
}

func (b *testBroker) store(t *testing.T) *EventStore {
	t.Helper()
	store, err := Open(context.Background(), b.storeSettings())
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func (b *testBroker) storeWithoutManagement(t *testing.T) *EventStore {
	t.Helper()
	settings := b.storeSettings()
	settings.ManagementURL = ""
	store, err := New(settings)
	if err != nil {
		t.Fatalf("creating the store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// consumers is the management API's stream consumer list.
func (b *testBroker) consumers(t *testing.T) []map[string]any {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, b.management+"/api/stream/consumers/%2F", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth("guest", "guest")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("the management API: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("the management API: HTTP %d", response.StatusCode)
	}
	var all []map[string]any
	if err := json.NewDecoder(response.Body).Decode(&all); err != nil {
		t.Fatalf("the management API's consumer list: %v", err)
	}
	return all
}

// membersOf polls the management API until it lists want members of the subscription, or fails.
func (b *testBroker) membersOf(t *testing.T, name string, want int) []string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last []string
	for {
		seen := make(map[string]bool)
		last = nil
		for _, consumer := range b.consumers(t) {
			properties, _ := consumer["properties"].(map[string]any)
			if properties["name"] != name {
				continue
			}
			member, _ := properties[memberProperty].(string)
			if member == "" || seen[member] {
				continue
			}
			seen[member] = true
			last = append(last, member)
		}
		if len(last) >= want {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("the management API lists %d members of %s within 30 s, want %d (%v)", len(last), name, want, last)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// event is one event with the seven headers and a JSON object payload.
func event(source, id string, key *string) snodra.OutgoingEvent {
	return snodra.OutgoingEvent{
		Source: source,
		ID:     id,
		Type:   "orders.Placed",
		Key:    key,
		Headers: map[string]string{
			snodra.HeaderType:          "orders.Placed",
			snodra.HeaderID:            id,
			snodra.HeaderCorrelationID: "correlation-" + id,
			snodra.HeaderTimestamp:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
			snodra.HeaderSource:        "orders-service",
			snodra.HeaderUserID:        "user-1",
			snodra.HeaderTraceparent:   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		},
		Payload: []byte(fmt.Sprintf(`{"id":%q}`, id)),
	}
}

// readDeliveries reads n deliveries from sub, acknowledging each.
func readDeliveries(t *testing.T, sub snodra.EventSubscription, n int, timeout time.Duration) []snodra.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var deliveries []snodra.Delivery
	for len(deliveries) < n {
		event, err := sub.Next(ctx)
		if err != nil {
			t.Fatalf("reading deliveries: %v (got %d of %d)", err, len(deliveries), n)
		}
		delivery, ok := event.(snodra.Delivery)
		if !ok {
			continue
		}
		if err := sub.Ack(delivery.Segment, delivery.Position); err != nil {
			t.Fatalf("acknowledging %s %s: %v", delivery.Segment, delivery.Position, err)
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries
}

// subscribe joins spec and waits for its first assignment.
func subscribe(t *testing.T, store *EventStore, spec snodra.SubscriptionSpec) snodra.EventSubscription {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sub, err := store.Subscribe(ctx, spec)
	if err != nil {
		t.Fatalf("subscribing %s: %v", spec.Name, err)
	}
	for {
		event, err := sub.Next(ctx)
		if err != nil {
			t.Fatalf("waiting for the first assignment of %s: %v", spec.Name, err)
		}
		if _, ok := event.(snodra.Assigned); ok {
			return sub
		}
	}
}

// storedOffset is the offset the broker stores under the subscription's name on stream.
func storedOffset(t *testing.T, store *EventStore, name, streamName string) (uint64, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	seed, err := store.dialAny(ctx, storeConnection)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer func() { _ = seed.Close() }()
	address, err := leaderOf(ctx, seed, streamName)
	if err != nil {
		t.Fatalf("the leader of %s: %v", streamName, err)
	}
	connection, err := store.dial(ctx, address, storeConnection)
	if err != nil {
		t.Fatalf("connecting to the leader: %v", err)
	}
	defer func() { _ = connection.Close() }()
	offset, found, err := connection.QueryOffset(ctx, name, streamName)
	if err != nil {
		t.Fatalf("querying the offset: %v", err)
	}
	return offset, found
}
