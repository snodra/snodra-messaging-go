package kafka

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	snodra "github.com/snodra/snodra-messaging-go"
)

// kafkaImage pins Apache Kafka 4.4.0 by its multi-architecture index digest. The layout is the
// one the site verified against 4.4.0.
const kafkaImage = "apache/kafka@sha256:76af3d0116abbb33b4cd964af410d4921c33d1f663e7a95d5453a9c35a84869b"

// testBroker is one Kafka container for the whole test run, and the admin client the tests share.
type testBroker struct {
	container testcontainers.Container
	bootstrap string
	admin     *kadm.Client
}

var (
	brokerOnce  sync.Once
	brokerValue *testBroker
	brokerErr   error
	// runSuffix keeps every topic of one run distinct from an earlier run against a broker that
	// is not the container's (there is none today, but a developer may point the tests at one).
	runSuffix = strings.ToLower(rand.Text())
)

func TestMain(m *testing.M) {
	code := m.Run()
	if brokerValue != nil {
		if brokerValue.admin != nil {
			brokerValue.admin.Close()
		}
		if brokerValue.container != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = brokerValue.container.Terminate(ctx)
			cancel()
		}
	}
	os.Exit(code)
}

// kafkaBroker returns the run's broker, starting the container on first use. Without a working
// Docker the test skips: no other test in this package needs it.
func kafkaBroker(t *testing.T) *testBroker {
	t.Helper()
	brokerOnce.Do(func() {
		brokerValue, brokerErr = startBroker()
	})
	if brokerErr != nil {
		t.Skipf("the Kafka tests need Docker: %v", brokerErr)
	}
	return brokerValue
}

// startBroker runs one KRaft broker on a host port it advertises. A host port is chosen before
// the container starts because the advertised listener must name it; three attempts cover the
// rare race with another process.
func startBroker() (*testBroker, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		port, err := freePort()
		if err != nil {
			return nil, err
		}
		bootstrap := "127.0.0.1:" + strconv.Itoa(port)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		instance, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        kafkaImage,
				ExposedPorts: []string{"9092/tcp"},
				Env: map[string]string{
					"CLUSTER_ID":                                     "5L6g3nShT-eMCtK--X86sw",
					"KAFKA_NODE_ID":                                  "1",
					"KAFKA_PROCESS_ROLES":                            "broker,controller",
					"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
					"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://" + bootstrap,
					"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
					"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
					"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
					"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
					"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
					"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
					"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
					"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
				},
				HostConfigModifier: func(hostConfig *container.HostConfig) {
					hostConfig.PortBindings = network.PortMap{
						network.MustParsePort("9092/tcp"): []network.PortBinding{{
							HostIP:   netip.MustParseAddr("127.0.0.1"),
							HostPort: strconv.Itoa(port),
						}},
					}
				},
				WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
			},
			Started: true,
		})
		cancel()
		if err != nil {
			last = err
			continue
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
		if err != nil {
			_ = instance.Terminate(context.Background())
			last = err
			continue
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = client.Ping(pingCtx)
		pingCancel()
		if err != nil {
			client.Close()
			_ = instance.Terminate(context.Background())
			last = err
			continue
		}
		return &testBroker{
			container: instance,
			bootstrap: bootstrap,
			admin:     kadm.NewClient(client),
		}, nil
	}
	return nil, fmt.Errorf("no Kafka broker after three attempts: %w", last)
}

// freePort is a port the kernel had free a moment ago.
func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// newTestStore is the add-on for a test; its producer and admin client are closed afterwards.
func newTestStore(t *testing.T, broker *testBroker) *EventStore {
	t.Helper()
	store, err := New(Config{
		BootstrapServers: broker.bootstrap,
		StatusRefresh:    100 * time.Millisecond,
		AdminTimeout:     15 * time.Second,
	})
	if err != nil {
		t.Fatalf("the Kafka store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// uniqueTopic is a topic name no other test or run uses.
func uniqueTopic(prefix string) string {
	return prefix + "." + runSuffix + "." + strings.ToLower(rand.Text())
}

// uniqueGroup is a subscription name (a consumer group) no other test or run uses, in the form
// `<service>.<processor>`.
func uniqueGroup(service, processor string) string {
	return service + "." + processor + "." + strings.ToLower(rand.Text())
}

// createTopic creates a topic and arranges its deletion.
func createTopic(t *testing.T, broker *testBroker, name string, partitions int32, configs map[string]*string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := broker.admin.CreateTopic(ctx, partitions, 1, configs, name)
	if err != nil {
		t.Fatalf("creating topic %s: %v", name, err)
	}
	if response.Err != nil {
		t.Fatalf("creating topic %s: %v", name, response.Err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = broker.admin.DeleteTopics(ctx, name)
	})
}

func stringPointer(value string) *string { return &value }

// subscribe joins spec and closes the subscription when the test ends.
func subscribe(t *testing.T, store *EventStore, spec snodra.SubscriptionSpec) *subscription {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	created, err := store.Subscribe(ctx, spec)
	if err != nil {
		t.Fatalf("subscribing %s: %v", spec.Name, err)
	}
	s := created.(*subscription)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

// next waits for one subscription event.
func next(t *testing.T, sub *subscription, within time.Duration) snodra.SubscriptionEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	event, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("%s: next: %v (take error: %q)", sub.spec.Name, err, sub.TakeError())
	}
	return event
}

// waitForAssignment waits for the first assignment, an empty one included.
func waitForAssignment(t *testing.T, sub *subscription, within time.Duration) snodra.Assigned {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		event := next(t, sub, time.Until(deadline))
		if assigned, ok := event.(snodra.Assigned); ok {
			return assigned
		}
	}
}

// readDeliveries reads count deliveries, acknowledging each, within the bound.
func readDeliveries(t *testing.T, sub *subscription, count int, within time.Duration) []snodra.Delivery {
	t.Helper()
	return readDeliveriesHolding(t, sub, count, within, nil)
}

// readDeliveriesHolding reads count deliveries within the bound; the deliveries whose header id
// is in hold are not acknowledged.
func readDeliveriesHolding(t *testing.T, sub *subscription, count int, within time.Duration,
	hold map[string]bool) []snodra.Delivery {
	t.Helper()
	deadline := time.Now().Add(within)
	deliveries := make([]snodra.Delivery, 0, count)
	for len(deliveries) < count {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d of %d deliveries within %v", sub.spec.Name, len(deliveries), count, within)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Until(deadline))
		event, err := sub.Next(ctx)
		cancel()
		if err != nil {
			t.Fatalf("%s: next after %d deliveries: %v", sub.spec.Name, len(deliveries), err)
		}
		delivery, ok := event.(snodra.Delivery)
		if !ok {
			continue
		}
		if !hold[delivery.Headers[snodra.HeaderID]] {
			if err := sub.Ack(delivery.Segment, delivery.Position); err != nil {
				t.Fatalf("%s: ack %s/%s: %v", sub.spec.Name, delivery.Segment, delivery.Position, err)
			}
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries
}

// event is one outgoing event with the seven headers and a JSON object payload.
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
		Payload: []byte(`{"id":"` + id + `"}`),
	}
}
