package main

import (
	"strings"
	"testing"
)

func env(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func baseEnv() map[string]string {
	return map[string]string{
		"INTEROP_RUN":               "run42",
		"INTEROP_LANGUAGE":          "go",
		"INTEROP_KAFKA_BOOTSTRAP":   "kafka:9092",
		"INTEROP_RABBITMQ_HOST":     "rabbitmq",
		"INTEROP_RABBITMQ_PORT":     "5552",
		"INTEROP_POSTGRES_URL":      "postgresql://snodra:snodra@postgres:5432/" + databaseName("run42"),
		"INTEROP_MYSQL_URL":         "mysql://snodra:snodra@mysql:3306/" + databaseName("run42"),
		"INTEROP_MONGO_URI":         "mongodb://mongo:27017/?replicaSet=rs0",
		"INTEROP_MONGO_DATABASE":    databaseName("run42"),
		"INTEROP_RABBITMQ_USERNAME": "guest",
		"INTEROP_RABBITMQ_PASSWORD": "guest",
	}
}

func TestSettingsFromEnv(t *testing.T) {
	s, err := settingsFromEnv(env(baseEnv()))
	if err != nil {
		t.Fatalf("settingsFromEnv: %v", err)
	}
	if s.run != "run42" || s.language != "go" || s.hub != "hub:7000" {
		t.Fatalf("unexpected settings: %+v", s)
	}
	if got := strings.Join(s.families, ","); got != "kafka,rabbitmq,postgres,mysql,mongo" {
		t.Fatalf("families = %q", got)
	}
	if s.kafkaBootstrap != "kafka:9092" || s.rabbitMQPort != 5552 {
		t.Fatalf("endpoints = %+v", s)
	}
	if s.mongoDatabase != "snodra_interop_run42" || s.sqlDatabaseName != "snodra_interop_run42" {
		t.Fatalf("database names = %q, %q", s.mongoDatabase, s.sqlDatabaseName)
	}
}

func TestSettingsNamesArePerRun(t *testing.T) {
	s, err := settingsFromEnv(env(baseEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if got := sourceName(s.run, familyPostgres); got != "snodra.interop.run42.postgres" {
		t.Fatalf("source = %q", got)
	}
	if got := instanceName("python", familyMongo); got != "python-mongo" {
		t.Fatalf("instance = %q", got)
	}
	if got := subscriptionName(familyKafka); got != "store-interop.interop-kafka" {
		t.Fatalf("subscription = %q", got)
	}
	if got := markerID(familyMySQL, "e-1"); got != "processor:interop-mysql:e-1" {
		t.Fatalf("marker = %q", got)
	}
}

func TestSettingsRequiresTheSharedFamilyEndpoints(t *testing.T) {
	values := baseEnv()
	delete(values, "INTEROP_POSTGRES_URL")
	if _, err := settingsFromEnv(env(values)); err == nil || !strings.Contains(err.Error(), "INTEROP_POSTGRES_URL") {
		t.Fatalf("missing postgres URL: %v", err)
	}
}

func TestSettingsRejectsAnUnknownFamily(t *testing.T) {
	values := baseEnv()
	values["INTEROP_FAMILIES"] = "kafka,firestore"
	if _, err := settingsFromEnv(env(values)); err == nil || !strings.Contains(err.Error(), "firestore") {
		t.Fatalf("unknown family: %v", err)
	}
}

func TestSettingsRejectsAnUnsafeRun(t *testing.T) {
	for _, run := range []string{"", "Run-42", "run.42", "run 42"} {
		values := baseEnv()
		values["INTEROP_RUN"] = run
		if _, err := settingsFromEnv(env(values)); err == nil {
			t.Fatalf("run %q was accepted", run)
		}
	}
}

func TestSettingsFamiliesAreUniqueAndOrdered(t *testing.T) {
	values := baseEnv()
	values["INTEROP_FAMILIES"] = "mongo,kafka,mongo"
	s, err := settingsFromEnv(env(values))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.families, ","); got != "mongo,kafka" {
		t.Fatalf("families = %q", got)
	}
}

func TestSQLDatabaseFromURL(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want string
	}{
		{"postgresql://u:p@host:5432/snodra_interop_run42", "snodra_interop_run42"},
		{"postgres://u@host/snodra_interop_run42?sslmode=disable", "snodra_interop_run42"},
		{"mysql://u:p@host:3306/snodra_interop_run42", "snodra_interop_run42"},
	} {
		got, err := sqlDatabaseFromURL(tc.url)
		if err != nil || got != tc.want {
			t.Fatalf("sqlDatabaseFromURL(%q) = %q, %v", tc.url, got, err)
		}
	}
	if _, err := sqlDatabaseFromURL("postgresql://host:5432"); err == nil {
		t.Fatal("a URL without a database was accepted")
	}
}
