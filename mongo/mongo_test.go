package mongo

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/snodratest"
)

// The 17 message-store checks (nine level-2 and eight level-3), on a replica set.
func TestTheMongoMessageStorePassesTheKit(t *testing.T) {
	server := mongoContainer(t)
	snodratest.MessageStore(t, server.kitStores(t, len(snodratest.StoreCheckIDs())))
}

// A document written by the Rust add-on leaves out owner and leaseUntil, and writes key, sentAt
// and failedAt as null; one written by the Kotlin add-on leaves all five out. A reader accepts
// both: a missing or null optional field is none.
func TestAnEntryWithoutOptionalFieldsReadsAsNone(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	collection := store.Database().Collection(collectionOutbox)
	documents := []bson.D{
		{
			{Key: "_id", Value: "rust-no-key"},
			{Key: "type", Value: "conformance.NoKey"},
			{Key: "key", Value: nil},
			{Key: "headers", Value: bson.D{}},
			{Key: "payload", Value: bson.D{{Key: "n", Value: int32(1)}}},
			{Key: "createdAt", Value: bson.NewDateTimeFromTime(now)},
			{Key: "seq", Value: int64(1)},
			{Key: "sentAt", Value: nil},
			{Key: "attempts", Value: int32(2)},
			{Key: "failedAt", Value: nil},
		},
		{
			{Key: "_id", Value: "kotlin-all-absent"},
			// A document written by an older version of another library carries a _class field;
			// a reader ignores fields it does not know.
			{Key: "_class", Value: "com.example.OutboxDocument"},
			{Key: "type", Value: "conformance.Absent"},
			{Key: "headers", Value: bson.D{}},
			{Key: "payload", Value: bson.D{{Key: "n", Value: int64(2)}}},
			{Key: "createdAt", Value: bson.NewDateTimeFromTime(now.Add(time.Second))},
			{Key: "seq", Value: int64(2)},
			{Key: "attempts", Value: int32(0)},
		},
	}
	if _, err := collection.InsertMany(ctx, []any{documents[0], documents[1]}); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 2 {
		t.Fatalf("a missing optional field is pending: %d (%v)", pending, err)
	}
	claimed, err := store.Outbox().Claim(ctx, "relay-a", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d entries, want 2", len(claimed))
	}
	for i, entry := range claimed {
		if entry.Key != nil {
			t.Fatalf("entry %s: key %q, want none", entry.ID, *entry.Key)
		}
		if i == 0 && entry.Attempts != 2 {
			t.Fatalf("entry %s: attempts %d, want 2", entry.ID, entry.Attempts)
		}
		if i == 1 && entry.Attempts != 0 {
			t.Fatalf("entry %s: attempts %d, want 0", entry.ID, entry.Attempts)
		}
	}
}

// Append writes key, sentAt and failedAt as null, and owner and leaseUntil only once a relay
// claims the entry; a claim stamps the lease and a send stamps sentAt.
func TestAnAppendedEntryWritesTheLayoutsNullFields(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()
	entry := testEntry("conformance.Layout", time.Now(), 1)
	entry.Key = nil
	if err := store.Outbox().Append(ctx, entry); err != nil {
		t.Fatal(err)
	}
	read := func() bson.Raw {
		t.Helper()
		raw, err := store.Database().Collection(collectionOutbox).
			FindOne(ctx, bson.D{{Key: "_id", Value: entry.ID}}).Raw()
		if err != nil {
			t.Fatalf("reading %s: %v", entry.ID, err)
		}
		return raw
	}
	raw := read()
	for _, name := range []string{"key", "sentAt", "failedAt"} {
		value, err := raw.LookupErr(name)
		if err != nil {
			t.Fatalf("%s is absent: %v", name, err)
		}
		if value.Type != bson.TypeNull {
			t.Fatalf("%s is %v, want null", name, value.Type)
		}
	}
	for _, name := range []string{"owner", "leaseUntil"} {
		if _, err := raw.LookupErr(name); err == nil {
			t.Fatalf("%s is written before a claim", name)
		}
	}
	if _, err := raw.LookupErr("_class"); err == nil {
		t.Fatal("the store wrote a _class field")
	}
	if value, err := raw.LookupErr("seq"); err != nil || value.Type != bson.TypeInt64 {
		t.Fatalf("seq: %v (%v), want a long", value.Type, err)
	}
	if value, err := raw.LookupErr("attempts"); err != nil || value.Type != bson.TypeInt32 {
		t.Fatalf("attempts: %v (%v), want an int", value.Type, err)
	}
	if value, err := raw.LookupErr("createdAt"); err != nil || value.Type != bson.TypeDateTime {
		t.Fatalf("createdAt: %v (%v), want a date", value.Type, err)
	}

	claimed, err := store.Outbox().Claim(ctx, "relay-a", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %d entries (%v)", len(claimed), err)
	}
	raw = read()
	if value, err := raw.LookupErr("owner"); err != nil || value.StringValue() != "relay-a" {
		t.Fatalf("owner: %v (%v)", value, err)
	}
	if value, err := raw.LookupErr("leaseUntil"); err != nil || value.Type != bson.TypeDateTime {
		t.Fatalf("leaseUntil: %v (%v), want a date", value.Type, err)
	}
	for _, name := range []string{"key", "sentAt", "failedAt"} {
		if value, err := raw.LookupErr(name); err != nil || value.Type != bson.TypeNull {
			t.Fatalf("%s after a claim: %v (%v), want null", name, value.Type, err)
		}
	}

	if err := store.Outbox().MarkSent(ctx, []string{entry.ID}); err != nil {
		t.Fatal(err)
	}
	raw = read()
	if value, err := raw.LookupErr("sentAt"); err != nil || value.Type != bson.TypeDateTime {
		t.Fatalf("sentAt: %v (%v), want a date", value.Type, err)
	}
	if value, err := raw.LookupErr("failedAt"); err != nil || value.Type != bson.TypeNull {
		t.Fatalf("failedAt after a send: %v (%v), want null", value.Type, err)
	}
}

// An entry appended and a marker recorded in a rolled-back transaction leave nothing; a commit
// leaves both.
func TestARolledBackTransactionLeavesNothing(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()

	txctx, tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack := testEntry("conformance.RolledBack", time.Now(), 1)
	marker := testMarker()
	if err := store.Outbox().Append(txctx, rolledBack); err != nil {
		t.Fatalf("append in the transaction: %v", err)
	}
	if recorded, err := store.Processed().Record(txctx, marker); err != nil || !recorded {
		t.Fatalf("record in the transaction: %v (%v)", recorded, err)
	}
	if err := tx.Rollback(txctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 0 {
		t.Fatalf("a rollback left %d entries (%v)", pending, err)
	}
	if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || exists {
		t.Fatalf("a rollback left the marker (%v, %v)", exists, err)
	}

	txctx, tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	committed := testEntry("conformance.Committed", time.Now(), 2)
	marker = testMarker()
	if err := store.Outbox().Append(txctx, committed); err != nil {
		t.Fatalf("append in the committed transaction: %v", err)
	}
	if recorded, err := store.Processed().Record(txctx, marker); err != nil || !recorded {
		t.Fatalf("record in the committed transaction: %v (%v)", recorded, err)
	}
	if err := tx.Commit(txctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 1 {
		t.Fatalf("a commit left %d entries, want 1 (%v)", pending, err)
	}
	if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || !exists {
		t.Fatalf("a commit left no marker (%v, %v)", exists, err)
	}
}

// A service's own session transaction commits and rolls back with the library's writes and the
// service's documents; Store.Session returns it to a handler.
func TestAServiceSessionBindsTheStoresWrites(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()
	session, err := store.Client().StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(context.Background())
	orders := store.Database().Collection("orders")

	if err := session.StartTransaction(); err != nil {
		t.Fatal(err)
	}
	sctx := store.WithSession(ctx, session)
	if got, ok := store.Session(sctx); !ok || got != session {
		t.Fatal("the bound session is not returned")
	}
	entry := testEntry("conformance.ServiceSession", time.Now(), 1)
	marker := testMarker()
	if _, err := orders.InsertOne(sctx, bson.D{{Key: "_id", Value: "order-1"}}); err != nil {
		t.Fatalf("the service's own write: %v", err)
	}
	if err := store.Outbox().Append(sctx, entry); err != nil {
		t.Fatalf("append in the service's transaction: %v", err)
	}
	if recorded, err := store.Processed().Record(sctx, marker); err != nil || !recorded {
		t.Fatalf("record in the service's transaction: %v (%v)", recorded, err)
	}
	if err := session.AbortTransaction(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 0 {
		t.Fatalf("an abort left %d entries (%v)", pending, err)
	}
	if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || exists {
		t.Fatalf("an abort left the marker (%v, %v)", exists, err)
	}
	if count, err := orders.CountDocuments(ctx, bson.D{}); err != nil || count != 0 {
		t.Fatalf("an abort left the service's document (%d, %v)", count, err)
	}

	if err := session.StartTransaction(); err != nil {
		t.Fatal(err)
	}
	sctx = store.WithSession(ctx, session)
	entry = testEntry("conformance.ServiceSession", time.Now(), 2)
	marker = testMarker()
	if _, err := orders.InsertOne(sctx, bson.D{{Key: "_id", Value: "order-2"}}); err != nil {
		t.Fatalf("the service's own write: %v", err)
	}
	if err := store.Outbox().Append(sctx, entry); err != nil {
		t.Fatalf("append in the committed transaction: %v", err)
	}
	if recorded, err := store.Processed().Record(sctx, marker); err != nil || !recorded {
		t.Fatalf("record in the committed transaction: %v (%v)", recorded, err)
	}
	if err := session.CommitTransaction(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Outbox().Pending(ctx); err != nil || pending != 1 {
		t.Fatalf("a commit left %d entries, want 1 (%v)", pending, err)
	}
	if exists, err := store.Processed().Exists(ctx, marker.ID); err != nil || !exists {
		t.Fatalf("a commit left no marker (%v, %v)", exists, err)
	}
	if count, err := orders.CountDocuments(ctx, bson.D{}); err != nil || count != 1 {
		t.Fatalf("a commit left %d service documents, want 1 (%v)", count, err)
	}
}

// InitSchema creates every index of the layout and is idempotent; CheckSchema passes afterwards
// and fails on an empty database and on a retention another instance was created with.
func TestTheIndexesAreIdempotentAndCheckable(t *testing.T) {
	server := mongoContainer(t)
	ctx := context.Background()
	store := server.newStore(t, true)
	if err := store.InitSchema(ctx, nil); err != nil {
		t.Fatalf("creating the indexes twice: %v", err)
	}
	if err := store.CheckSchema(ctx); err != nil {
		t.Fatalf("checking the created indexes: %v", err)
	}
	for _, layout := range layoutIndexes(store.Settings()) {
		present, err := store.Database().ListCollectionNames(ctx, bson.D{{Key: "name", Value: layout.name}})
		if err != nil || len(present) != 1 {
			t.Fatalf("the %q collection: %v (%v)", layout.name, present, err)
		}
		actual, err := store.listIndexes(ctx, layout.name)
		if err != nil {
			t.Fatalf("the indexes of %q: %v", layout.name, err)
		}
		for _, definition := range layout.indexes {
			specification, ok := actual[definition.name]
			if !ok {
				t.Fatalf("%q has no index %q: %v", layout.name, definition.name, actual)
			}
			if got, want := specification.Keys.String(), definition.keys.String(); got != want {
				t.Fatalf("%q keys: got %s, want %s", definition.name, got, want)
			}
			if definition.ttl != nil {
				want := ttlSeconds(*definition.ttl)
				if specification.ExpireAfterSeconds == nil || *specification.ExpireAfterSeconds != want {
					t.Fatalf("%q TTL: got %v, want %d", definition.name, specification.ExpireAfterSeconds, want)
				}
			}
			unique := specification.Unique != nil && *specification.Unique
			if unique != definition.unique {
				t.Fatalf("%q unique: got %v, want %v", definition.name, unique, definition.unique)
			}
			var partial bson.D
			if len(specification.PartialFilterExpression) > 0 {
				if err := bson.Unmarshal(specification.PartialFilterExpression, &partial); err != nil {
					t.Fatalf("%q partial filter: %v", definition.name, err)
				}
			}
			if !samePartialFilter(partial, definition.partial) {
				t.Fatalf("%q partial filter: got %v, want %v", definition.name, partial, definition.partial)
			}
			delete(actual, definition.name)
		}
		if _, ok := actual["_id_"]; !ok || len(actual) != 1 {
			t.Fatalf("%q holds indexes the layout does not name: %v", layout.name, actual)
		}
	}

	bare := server.newStore(t, false)
	if err := bare.CheckSchema(ctx); err == nil || !strings.Contains(err.Error(), collectionOutbox) {
		t.Fatalf("an empty database: %v, want a missing-collection error", err)
	}

	database := server.database(t)
	first, err := New(Config{URI: server.uri(), Database: database})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.InitSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	_ = first.Close(ctx)
	second, err := New(Config{URI: server.uri(), Database: database, OutboxRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	if err := second.CheckSchema(ctx); err == nil || !strings.Contains(err.Error(), "retentions") {
		t.Fatalf("another retention: %v, want a TTL error", err)
	}
}

// Every store gets a database of its own: one store's entries are not another's.
func TestEveryStoreGetsADatabaseOfItsOwn(t *testing.T) {
	server := mongoContainer(t)
	first := server.newStore(t, true)
	second := server.newStore(t, true)
	if first.Database().Name() == second.Database().Name() {
		t.Fatalf("both stores use %q", first.Database().Name())
	}
	ctx := context.Background()
	if err := first.Outbox().Append(ctx, testEntry("conformance.Unique", time.Now(), 1)); err != nil {
		t.Fatal(err)
	}
	if pending, err := first.Outbox().Pending(ctx); err != nil || pending != 1 {
		t.Fatalf("the first store: %d pending (%v)", pending, err)
	}
	if pending, err := second.Outbox().Pending(ctx); err != nil || pending != 0 {
		t.Fatalf("the second store sees %d entries (%v)", pending, err)
	}
}

// The store stamps a claim's lease and an entry's sending and failure with the application's
// clock, never the database's.
func TestTheApplicationClockStampsTheStoresTimes(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	store.UseClock(func() time.Time { return at })
	ctx := context.Background()
	sent := testEntry("conformance.Clock", time.Now(), 1)
	failed := testEntry("conformance.Clock", time.Now(), 2)
	for _, entry := range []snodra.OutboxEntry{sent, failed} {
		if err := store.Outbox().Append(ctx, entry); err != nil {
			t.Fatalf("append %s: %v", entry.ID, err)
		}
	}
	claimed, err := store.Outbox().Claim(ctx, "relay-a", 30*time.Second, 10)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim: %d entries (%v)", len(claimed), err)
	}
	read := func(id string) bson.Raw {
		t.Helper()
		raw, err := store.Database().Collection(collectionOutbox).
			FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Raw()
		if err != nil {
			t.Fatalf("reading %s: %v", id, err)
		}
		return raw
	}
	date := func(raw bson.Raw, name string) int64 {
		t.Helper()
		value, err := raw.LookupErr(name)
		if err != nil || value.Type != bson.TypeDateTime {
			t.Fatalf("%s: %v (%v), want a date", name, value.Type, err)
		}
		return value.DateTime()
	}
	raw := read(sent.ID)
	if got, want := date(raw, "leaseUntil"), int64(bson.NewDateTimeFromTime(at.Add(30*time.Second))); got != want {
		t.Fatalf("the claim's lease: got %d, want %d", got, want)
	}
	if err := store.Outbox().MarkSent(ctx, []string{sent.ID}); err != nil {
		t.Fatal(err)
	}
	raw = read(sent.ID)
	if got, want := date(raw, "sentAt"), int64(bson.NewDateTimeFromTime(at)); got != want {
		t.Fatalf("sent at: got %d, want %d", got, want)
	}
	if err := store.Outbox().MarkFailed(ctx, failed.ID, "conformance: given up"); err != nil {
		t.Fatal(err)
	}
	raw = read(failed.ID)
	if got, want := date(raw, "failedAt"), int64(bson.NewDateTimeFromTime(at)); got != want {
		t.Fatalf("failed at: got %d, want %d", got, want)
	}
}

// DeleteMarkers removes exactly the stage-1 markers of one processor in a range: numeric positions
// by value, text positions by bytes, never across the two, and never a marker without a position.
// A marker written before the event store became an add-on (an int partition, a long offset) is
// found and deleted like a new one.
func TestDeleteMarkersRemovesOnlyTheRange(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()
	record := func(processor, source, segment, position string) string {
		t.Helper()
		id := "processor:" + processor + ":" + strings.ToLower(randText())
		recorded, err := store.Processed().Record(ctx, snodra.ProcessedMarker{
			ID:          id,
			Type:        "conformance.Happened",
			ProcessedAt: time.Now(),
			Processor:   processor,
			Source:      source,
			Segment:     segment,
			Position:    position,
		})
		if err != nil || !recorded {
			t.Fatalf("record %s: %v (%v)", id, recorded, err)
		}
		return id
	}
	nine := record("default", "s", "0", "9")
	ten := record("default", "s", "0", "10")
	text := record("default", "s", "0", "x7")
	none := record("default", "s", "0", "")
	otherProcessor := record("mail", "s", "0", "10")
	otherSource := record("default", "other", "0", "10")
	otherSegment := record("default", "s", "1", "10")
	old := "processor:default:old-4"
	if _, err := store.Database().Collection(collectionProcessed).InsertOne(ctx, bson.D{
		{Key: "_id", Value: old},
		{Key: "type", Value: "conformance.Happened"},
		{Key: "processedAt", Value: bson.NewDateTimeFromTime(time.Now())},
		{Key: "processor", Value: "default"},
		{Key: "topic", Value: "s"},
		{Key: "partition", Value: int32(1)},
		{Key: "offset", Value: int64(4)},
	}); err != nil {
		t.Fatal(err)
	}

	deleted, err := store.Processed().DeleteMarkers(ctx, "default", []snodra.ReplayRange{
		{Source: "s", Segment: "0", From: "10", Until: "20"},
		{Source: "s", Segment: "0", From: "x0", Until: "y"},
		{Source: "s", Segment: "1", From: "0", Until: "10"},
	})
	if err != nil {
		t.Fatalf("delete markers: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("deleted %d markers, want 3", deleted)
	}
	for id, want := range map[string]bool{
		nine: true, ten: false, text: false, none: true, old: false,
		otherProcessor: true, otherSource: true, otherSegment: true,
	} {
		exists, err := store.Processed().Exists(ctx, id)
		if err != nil {
			t.Fatalf("exists %s: %v", id, err)
		}
		if exists != want {
			t.Fatalf("marker %s exists=%v, want %v", id, exists, want)
		}
	}
}

// Close is idempotent, and a client the service owns is left connected.
func TestCloseIsIdempotentAndLeavesAServiceClientOpen(t *testing.T) {
	server := mongoContainer(t)
	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(server.uri()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(disconnectCtx)
	}()
	store, err := NewWithClient(client, server.database(t), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("the service's client was disconnected: %v", err)
	}
}

// Background has nothing to run: the TTL monitor is the server's.
func TestBackgroundHasNothingToRun(t *testing.T) {
	store := &Store{}
	if err := store.Background(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A transaction of another MongoDB store in the context is refused, never run on this store's
// client.
func TestAnotherStoresTransactionIsRefused(t *testing.T) {
	first, err := New(Config{URI: "mongodb://127.0.0.1:1/?directConnection=true", Database: "db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close(context.Background()) }()
	second := &Store{}
	ctx := context.WithValue(context.Background(), txKey{}, &storeTx{store: first})
	if err := second.Outbox().Append(ctx, testEntry("conformance.Foreign", time.Now(), 1)); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("append with another store's transaction: %v, want ErrInvalid", err)
	}
	if _, err := second.Processed().Record(ctx, testMarker()); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("record with another store's transaction: %v, want ErrInvalid", err)
	}
}

// OnAppend registers nothing: the MongoDB store has no notification and the relay polls.
func TestOnAppendRegistersNothing(t *testing.T) {
	store, err := New(Config{URI: "mongodb://127.0.0.1:1/?directConnection=true", Database: "db"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close(context.Background()) }()
	called := false
	cancel := store.Outbox().OnAppend(func() { called = true })
	cancel()
	cancel()
	if called {
		t.Fatal("the MongoDB store woke a relay without a change feed")
	}
}

// New validates its configuration, and FromEnv reads the two settings.
func TestConfiguration(t *testing.T) {
	for _, config := range []Config{
		{},
		{URI: "mongodb://127.0.0.1:1/"},
		{Database: "db"},
		{URI: "not a URI", Database: "db"},
		{URI: "mongodb://127.0.0.1:1/", Database: "db", OutboxRetention: -time.Second},
	} {
		store, err := New(config)
		if err == nil {
			_ = store.Close(context.Background())
			t.Fatalf("%+v was accepted", config)
		}
		if !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("%+v: %v, want ErrInvalid", config, err)
		}
	}
	if _, err := NewWithClient(nil, "db", Config{}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("a nil client: %v", err)
	}
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1/?directConnection=true"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(disconnectCtx)
	}()
	if _, err := NewWithClient(client, "", Config{}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("an empty database: %v", err)
	}
	t.Setenv("SNODRA_STORE_MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
	t.Setenv("SNODRA_STORE_MONGO_DATABASE", "orders")
	config := FromEnv()
	if config.URI != "mongodb://localhost:27017/?directConnection=true" || config.Database != "orders" {
		t.Fatalf("FromEnv: %+v", config)
	}
}

// randText is a random lowercase letters-and-digits string for a marker id.
func randText() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	raw := make([]byte, 20)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	for i := range raw {
		raw[i] = alphabet[int(raw[i])%len(alphabet)]
	}
	return string(raw)
}

// The processor_control and dead_letters documents have the published layout's fields and BSON
// types, so another Snodra add-on reads what this one writes.
func TestControlAndDeadLetterDocumentsWriteTheLayoutsShape(t *testing.T) {
	server := mongoContainer(t)
	store := server.newStore(t, true)
	ctx := context.Background()
	at := time.Date(2026, 11, 3, 10, 0, 0, 250_000_000, time.UTC)
	processor := "conformance-layout"
	if err := store.Control().SetPaused(ctx, processor, true, "control-1", at); err != nil {
		t.Fatal(err)
	}
	if outcome, err := store.Control().Lock(ctx, processor, "op-1", at.Add(30*time.Second), at.Add(time.Second)); err != nil || !outcome.Taken {
		t.Fatalf("lock: %+v, %v", outcome, err)
	}
	replay := snodra.ReplayRange{Source: "snodra.events", Segment: "0", From: "10", Until: "20"}
	if err := store.Control().SetReplay(ctx, processor, []snodra.ReplayRange{replay}, at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	raw, err := store.Database().Collection(collectionControl).FindOne(ctx, bson.D{{Key: "_id", Value: processor}}).Raw()
	if err != nil {
		t.Fatal(err)
	}
	typeOf := func(document bson.Raw, name string, want bson.Type) {
		t.Helper()
		value, err := document.LookupErr(name)
		if err != nil {
			t.Fatalf("%s is missing: %v", name, err)
		}
		if value.Type != want {
			t.Fatalf("%s has BSON type %v, want %v", name, value.Type, want)
		}
	}
	if _, err := raw.LookupErr("_class"); err == nil {
		t.Fatal("a processor_control document carries _class")
	}
	typeOf(raw, "paused", bson.TypeBoolean)
	typeOf(raw, "pausedAt", bson.TypeDateTime)
	typeOf(raw, "pausedBy", bson.TypeString)
	typeOf(raw, "operation", bson.TypeEmbeddedDocument)
	typeOf(raw, "replay", bson.TypeArray)
	typeOf(raw, "updatedAt", bson.TypeDateTime)
	operation := raw.Lookup("operation").Document()
	if operation.Lookup("id").StringValue() != "op-1" || operation.Lookup("kind").StringValue() != snodra.ResetKind {
		t.Fatalf("operation: %v", operation)
	}
	typeOf(operation, "leaseUntil", bson.TypeDateTime)
	typeOf(operation, "preparedAt", bson.TypeDateTime)
	storedRange := raw.Lookup("replay").Array().Index(0).Document()
	if storedRange.Lookup("topic").StringValue() != replay.Source {
		t.Fatalf("a replay range's topic: %v", storedRange)
	}
	typeOf(storedRange, "partition", bson.TypeInt32)
	typeOf(storedRange, "from", bson.TypeInt64)
	typeOf(storedRange, "until", bson.TypeInt64)

	letter := snodra.DeadLetter{
		ID:            "conformance-layout:snodra.events:1:43",
		Processor:     "conformance-layout",
		Source:        "snodra.events",
		Segment:       "1",
		Position:      "43",
		Key:           strPtrLayout("order-7"),
		EventID:       "evt-8",
		EventType:     "orders.OrderShipped",
		Headers:       map[string]string{"type": "orders.OrderShipped", "id": "evt-8"},
		Payload:       []byte(`{"orderId":"order-7"}`),
		Reason:        snodra.DeadLetterHandlerFailed,
		Error:         snodra.NewDeadLetterError("RemoteHandler", "boom"),
		Attempts:      3,
		ParkedAt:      at,
		LastAttemptAt: at,
		ExpireAt:      at.Add(24 * time.Hour),
		Replay:        true,
	}
	if result, err := store.DeadLetters().Park(ctx, letter); err != nil || result != snodra.ParkNew {
		t.Fatalf("park: %v, %v", result, err)
	}
	raw, err = store.Database().Collection(collectionDeadLetters).FindOne(ctx, bson.D{{Key: "_id", Value: letter.ID}}).Raw()
	if err != nil {
		t.Fatal(err)
	}
	typeOf(raw, "_id", bson.TypeString)
	typeOf(raw, "processor", bson.TypeString)
	typeOf(raw, "topic", bson.TypeString)
	typeOf(raw, "partition", bson.TypeInt32)
	typeOf(raw, "offset", bson.TypeInt64)
	typeOf(raw, "key", bson.TypeString)
	typeOf(raw, "eventId", bson.TypeString)
	typeOf(raw, "eventType", bson.TypeString)
	typeOf(raw, "headers", bson.TypeEmbeddedDocument)
	typeOf(raw, "payload", bson.TypeString)
	if value, err := raw.LookupErr("payloadBinary"); err == nil {
		t.Fatalf("a text payload also writes payloadBinary: %v", value)
	}
	if _, err := raw.LookupErr("_class"); err == nil {
		t.Fatal("a dead-letter document carries _class")
	}
	typeOf(raw, "reason", bson.TypeString)
	typeOf(raw, "error", bson.TypeEmbeddedDocument)
	typeOf(raw, "attempts", bson.TypeInt32)
	typeOf(raw, "parkedAt", bson.TypeDateTime)
	typeOf(raw, "lastAttemptAt", bson.TypeDateTime)
	typeOf(raw, "expireAt", bson.TypeDateTime)
	typeOf(raw, "replay", bson.TypeBoolean)

	binary := letter
	binary.ID = "conformance-layout:snodra.events:1:44"
	binary.Payload = []byte{0xff, 0xfe, 0x00, 0x01}
	binary.Key = nil
	binary.EventID = ""
	binary.EventType = ""
	binary.BlockedBy = ""
	binary.Replay = false
	if result, err := store.DeadLetters().Park(ctx, binary); err != nil || result != snodra.ParkNew {
		t.Fatalf("park binary: %v, %v", result, err)
	}
	raw, err = store.Database().Collection(collectionDeadLetters).FindOne(ctx, bson.D{{Key: "_id", Value: binary.ID}}).Raw()
	if err != nil {
		t.Fatal(err)
	}
	payloadBinary, err := raw.LookupErr("payloadBinary")
	if err != nil || payloadBinary.Type != bson.TypeBinary {
		t.Fatalf("payloadBinary: %v, %v", payloadBinary, err)
	}
	if _, err := raw.LookupErr("_class"); err == nil {
		t.Fatal("a binary dead-letter document carries _class")
	}
	for _, name := range []string{"key", "eventId", "eventType", "blockedBy", "payload", "replay"} {
		if value, err := raw.LookupErr(name); err == nil {
			t.Fatalf("%s is present: %v", name, value)
		}
	}
}

// strPtrLayout is a pointer to value.
func strPtrLayout(value string) *string { return &value }
