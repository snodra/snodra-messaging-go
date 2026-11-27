package main

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The service name every probe registers: one logical service, several instances per language,
// all handling the same commands. The processors are per store family, so two languages meet in
// one subscription per family.
const serviceName = "store-interop"

// The event type the outbox and the raw publish commands carry.
const eventType = "orders.OrderPlaced"

// The store families the profile runs.
const (
	familyKafka    = "kafka"
	familyRabbitMQ = "rabbitmq"
	familyPostgres = "postgres"
	familyMySQL    = "mysql"
	familyMongo    = "mongo"
)

// familyStore says which message store and which event store a family's app uses.
type familyStore struct {
	message string // the add-on's name: "memory", "postgres", "mysql", "mongo"
	event   string // the add-on's name: "kafka", "rabbitmq-streams"
	shared  bool   // the message store is shared by both languages
}

var families = map[string]familyStore{
	familyKafka:    {message: "memory", event: "kafka"},
	familyRabbitMQ: {message: "memory", event: "rabbitmq-streams"},
	familyPostgres: {message: "postgres", event: "kafka", shared: true},
	familyMySQL:    {message: "mysql", event: "kafka", shared: true},
	familyMongo:    {message: "mongo", event: "kafka", shared: true},
}

// familyOrder is the order the families run in, and the default when INTEROP_FAMILIES is unset.
var familyOrder = []string{familyKafka, familyRabbitMQ, familyPostgres, familyMySQL, familyMongo}

var (
	runPattern      = regexp.MustCompile(`^[a-z0-9]+$`)
	languagePattern = regexp.MustCompile(`^[a-z][a-z0-9]*$`)
)

// settings is what the probe reads from the environment.
type settings struct {
	language string
	run      string
	hub      string
	families []string

	kafkaBootstrap  string
	rabbitMQHost    string
	rabbitMQPort    int
	rabbitMQUser    string
	rabbitMQPass    string
	postgresURL     string
	mysqlURL        string
	mongoURI        string
	mongoDatabase   string
	sqlDatabaseName string
}

// settingsFromEnv reads the harness's variables. INTEROP_RUN, INTEROP_LANGUAGE and the endpoints
// of the families it runs are required; a blank value is not configured.
func settingsFromEnv(getenv func(string) string) (settings, error) {
	get := func(name, fallback string) string {
		value := strings.TrimSpace(getenv(name))
		if value == "" {
			return fallback
		}
		return value
	}
	required := func(name string) (string, error) {
		value := strings.TrimSpace(getenv(name))
		if value == "" {
			return "", fmt.Errorf("%s is required", name)
		}
		return value, nil
	}
	var s settings
	s.run = get("INTEROP_RUN", "")
	if s.run == "" {
		return settings{}, fmt.Errorf("INTEROP_RUN is required (the unique token of the run)")
	}
	if !runPattern.MatchString(s.run) {
		return settings{}, fmt.Errorf("INTEROP_RUN %q must be lowercase letters and digits only", s.run)
	}
	s.language = get("INTEROP_LANGUAGE", "")
	if s.language == "" {
		return settings{}, fmt.Errorf("INTEROP_LANGUAGE is required")
	}
	if !languagePattern.MatchString(s.language) {
		return settings{}, fmt.Errorf("INTEROP_LANGUAGE %q must be lowercase letters and digits, starting with a letter", s.language)
	}
	s.hub = get("INTEROP_HUB", "hub:7000")
	names := strings.Split(get("INTEROP_FAMILIES", strings.Join(familyOrder, ",")), ",")
	seen := map[string]bool{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := families[name]; !ok {
			return settings{}, fmt.Errorf("INTEROP_FAMILIES names the unknown family %q (the families are %s)",
				name, strings.Join(familyOrder, ", "))
		}
		if !seen[name] {
			seen[name] = true
			s.families = append(s.families, name)
		}
	}
	if len(s.families) == 0 {
		return settings{}, fmt.Errorf("INTEROP_FAMILIES names no family")
	}
	s.kafkaBootstrap = get("INTEROP_KAFKA_BOOTSTRAP", "kafka:9092")
	s.rabbitMQHost = get("INTEROP_RABBITMQ_HOST", "rabbitmq")
	port := get("INTEROP_RABBITMQ_PORT", "5552")
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return settings{}, fmt.Errorf("INTEROP_RABBITMQ_PORT %q is not a port", port)
	}
	s.rabbitMQPort = number
	s.rabbitMQUser = get("INTEROP_RABBITMQ_USERNAME", "guest")
	s.rabbitMQPass = get("INTEROP_RABBITMQ_PASSWORD", "guest")
	if seen[familyPostgres] {
		value, err := required("INTEROP_POSTGRES_URL")
		if err != nil {
			return settings{}, err
		}
		s.postgresURL = value
	}
	if seen[familyMySQL] {
		value, err := required("INTEROP_MYSQL_URL")
		if err != nil {
			return settings{}, err
		}
		s.mysqlURL = value
	}
	if seen[familyMongo] {
		value, err := required("INTEROP_MONGO_URI")
		if err != nil {
			return settings{}, err
		}
		s.mongoURI = value
	}
	s.mongoDatabase = get("INTEROP_MONGO_DATABASE", databaseName(s.run))
	s.sqlDatabaseName = databaseName(s.run)
	return s, nil
}

// databaseName is the one database both languages share for a run.
func databaseName(run string) string { return "snodra_interop_" + run }

// deadLetterMode is the family's dead-letter mode: the RabbitMQ processor publishes what no
// handler can take to its dead-letter source (the mode the harness proves end to end); every
// other family parks letters in its message store.
func (s settings) deadLetterMode(family string) string {
	if family == familyRabbitMQ {
		return snodra.DeadLetterModeEventStore
	}
	return snodra.DeadLetterModeCollection
}

// sourceName is the event store source (a Kafka topic, a RabbitMQ super stream) of one family.
func sourceName(run, family string) string { return "snodra.interop." + run + "." + family }

// instanceName is the hub instance id of one language in one family.
func instanceName(language, family string) string { return language + "-" + family }

// processorName is the processor (and so the subscription) of one family: both languages use it,
// so their consumers compete in the same group or under the same stream consumer name.
func processorName(family string) string { return "interop-" + family }

// subscriptionName is the event store subscription ("<service>.<processor>") of one family.
func subscriptionName(family string) string { return serviceName + "." + processorName(family) }

// markerID is the stage-1 processed-event marker the library records for an event.
func markerID(family, eventID string) string {
	return "processor:" + processorName(family) + ":" + eventID
}

// sqlDatabaseFromURL reads the database a postgres:// or mysql:// URL names, for the state the
// driver checks: both languages must share one database per run.
func sqlDatabaseFromURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("cannot read the database URL: %w", err)
	}
	name := strings.TrimPrefix(parsed.Path, "/")
	if name == "" {
		return "", fmt.Errorf("the database URL %q names no database", raw)
	}
	return name, nil
}

// sqlDatabase returns the database the SQL family's URL names.
func (s settings) sqlDatabase(family string) (string, error) {
	raw := s.postgresURL
	if family == familyMySQL {
		raw = s.mysqlURL
	}
	return sqlDatabaseFromURL(raw)
}
