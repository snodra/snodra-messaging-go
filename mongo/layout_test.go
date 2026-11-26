package mongo

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	snodra "github.com/snodra/snodra-messaging-go"
)

// An appended entry has the layout's fields: key, sentAt and failedAt null when they have no
// value, no owner or leaseUntil until a claim. Numeric types are the layout's (seq long, attempts
// int), and the payload is a BSON document.
func TestAnOutboxDocumentWritesTheLayoutsShape(t *testing.T) {
	payload := []byte(`{"id":"e-1","n":1,"text":"é","nested":{"ok":true}}`)
	entry := snodra.OutboxEntry{
		ID:        "e-1",
		Type:      "t.Happened",
		Headers:   map[string]string{"userId": ""},
		Payload:   payload,
		CreatedAt: time.Date(2026, 11, 3, 10, 0, 0, 250_000_000, time.UTC),
		Seq:       42,
		Attempts:  3,
	}
	document, err := outboxDocumentOf(entry)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var stored bson.D
	if err := bson.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(stored))
	for _, element := range stored {
		keys = append(keys, element.Key)
	}
	sort.Strings(keys)
	if want := []string{"_id", "attempts", "createdAt", "failedAt", "headers", "key", "payload", "sentAt", "seq", "type"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("an appended entry's fields: got %v, want %v", keys, want)
	}
	values := make(map[string]any, len(stored))
	for _, element := range stored {
		values[element.Key] = element.Value
	}
	for _, name := range []string{"key", "sentAt", "failedAt"} {
		if values[name] != nil {
			t.Fatalf("%s: got %#v, want null", name, values[name])
		}
	}
	if values["seq"] != int64(42) || values["attempts"] != int32(3) {
		t.Fatalf("seq/attempts: %#v / %#v", values["seq"], values["attempts"])
	}
	createdAt, ok := values["createdAt"].(bson.DateTime)
	if !ok || createdAt != bson.NewDateTimeFromTime(entry.CreatedAt) {
		t.Fatalf("createdAt: %#v", values["createdAt"])
	}
	if payload, ok := values["payload"].(bson.D); !ok || !reflect.DeepEqual(payload, bson.D{
		{Key: "id", Value: "e-1"},
		{Key: "n", Value: int32(1)},
		{Key: "text", Value: "é"},
		{Key: "nested", Value: bson.D{{Key: "ok", Value: true}}},
	}) {
		t.Fatalf("payload: %#v", values["payload"])
	}
	back, err := entryOf(document)
	if err != nil {
		t.Fatal(err)
	}
	if back.ID != entry.ID || back.Type != entry.Type || back.Key != nil || back.Seq != entry.Seq || back.Attempts != entry.Attempts {
		t.Fatalf("the entry back: %+v", back)
	}
	if !back.CreatedAt.Equal(entry.CreatedAt) {
		t.Fatalf("createdAt back: %v, want %v", back.CreatedAt, entry.CreatedAt)
	}
	sameJSON(t, back.Payload, entry.Payload)
}

// A key is kept when it has one, and extended JSON keywords in the payload become their BSON
// values, as the layout says both add-ons read them.
func TestAnOutboxDocumentKeepsItsKeyAndReadsExtendedJSON(t *testing.T) {
	key := "order-7"
	entry := snodra.OutboxEntry{
		ID:        "e-2",
		Type:      "t.Happened",
		Key:       &key,
		Payload:   []byte(`{"n":{"$numberLong":"5"},"ok":true}`),
		CreatedAt: time.Now(),
		Seq:       1,
	}
	document, err := outboxDocumentOf(entry)
	if err != nil {
		t.Fatal(err)
	}
	if document.Key == nil || *document.Key != key {
		t.Fatalf("key: %v", document.Key)
	}
	if len(document.Payload) != 2 || document.Payload[0].Value != int64(5) || document.Payload[1].Value != true {
		t.Fatalf("the payload: %v", document.Payload)
	}
	back, err := entryOf(document)
	if err != nil {
		t.Fatal(err)
	}
	if back.Key == nil || *back.Key != key {
		t.Fatalf("key back: %v", back.Key)
	}
	sameJSON(t, back.Payload, []byte(`{"n":5,"ok":true}`))
}

// A payload the shared contract refuses is refused before a document exists.
func TestAnOutboxDocumentRefusesAPayloadTheContractRefuses(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`not json`),
		[]byte(`[1,2]`),
		[]byte(`{"n":18446744073709551615}`),
		[]byte(`{"s":"\ud800"}`),
	} {
		if _, err := outboxDocumentOf(snodra.OutboxEntry{
			ID: "bad", Type: "t.Bad", Payload: payload, CreatedAt: time.Now(), Seq: 1,
		}); !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("%s: %v, want ErrInvalid", payload, err)
		}
	}
}

// A marker without a position writes only the first three fields; a stage-1 marker writes its
// processor, source, segment and position, the segment as an int32 and the position as an int64
// when they are canonical decimals, else as text.
func TestAMarkerDocumentWritesTheLayoutsFields(t *testing.T) {
	marker := snodra.ProcessedMarker{
		ID:          "processor:default:e-1",
		Type:        "t.E",
		ProcessedAt: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC),
	}
	document := markerDocument(marker)
	if got, want := len(document), 3; got != want {
		t.Fatalf("a marker without a position has %d fields: %v", got, document)
	}
	if document[0].Key != "_id" || document[1].Key != "type" || document[2].Key != "processedAt" {
		t.Fatalf("a marker without a position: %v", document)
	}

	marker.Processor = "default"
	marker.Source = "snodra.events"
	marker.Segment = "2"
	marker.Position = "41"
	document = markerDocument(marker)
	want := bson.D{
		{Key: "_id", Value: marker.ID},
		{Key: "type", Value: marker.Type},
		{Key: "processedAt", Value: bson.NewDateTimeFromTime(marker.ProcessedAt)},
		{Key: "processor", Value: "default"},
		{Key: "topic", Value: "snodra.events"},
		{Key: "partition", Value: int32(2)},
		{Key: "offset", Value: int64(41)},
	}
	if !reflect.DeepEqual(document, want) {
		t.Fatalf("a stage-1 marker: got %v, want %v", document, want)
	}
	marker.Segment = "p-0"
	marker.Position = "x7"
	document = markerDocument(marker)
	if !reflect.DeepEqual(document[5], bson.E{Key: "partition", Value: "p-0"}) ||
		!reflect.DeepEqual(document[6], bson.E{Key: "offset", Value: "x7"}) {
		t.Fatalf("a marker of text positions: %v", document)
	}
}

// Segments and positions are numbers only when they are canonical decimals.
func TestSegmentsAndPositionsKeepCanonicalNumbers(t *testing.T) {
	for value, want := range map[string]any{
		"3":          int32(3),
		"0":          int32(0),
		"2147483647": int32(2147483647),
		"03":         "03",
		"+3":         "+3",
		"2147483648": "2147483648",
		"p-0":        "p-0",
		"":           "",
	} {
		if got := segmentValue(value); !reflect.DeepEqual(got, want) {
			t.Fatalf("segment %q: got %#v, want %#v", value, got, want)
		}
	}
	for value, want := range map[string]any{
		"41":                  int64(41),
		"0":                   int64(0),
		"9223372036854775807": int64(9223372036854775807),
		"9223372036854775808": "9223372036854775808",
		"007":                 "007",
		"+1":                  "+1",
		"x7":                  "x7",
		"":                    "",
	} {
		if got := positionValue(value); !reflect.DeepEqual(got, want) {
			t.Fatalf("position %q: got %#v, want %#v", value, got, want)
		}
	}
	for _, check := range []struct {
		value any
		want  string
		ok    bool
	}{
		{int32(3), "3", true},
		{int64(41), "41", true},
		{"x7", "x7", true},
		{true, "", false},
		{nil, "", false},
	} {
		got, ok := positionText(check.value)
		if got != check.want || ok != check.ok {
			t.Fatalf("positionText(%#v): got %q, %v, want %q, %v", check.value, got, ok, check.want, check.ok)
		}
	}
}

// The layout's index names, keys, TTLs and uniqueness are exactly the published ones.
func TestTheLayoutsIndexes(t *testing.T) {
	settings, err := settingsOf(Config{}, "db")
	if err != nil {
		t.Fatal(err)
	}
	byCollection := make(map[string]map[string]indexDefinition)
	for _, layout := range layoutIndexes(settings) {
		if _, exists := byCollection[layout.name]; exists {
			t.Fatalf("%q appears twice", layout.name)
		}
		indexes := make(map[string]indexDefinition, len(layout.indexes))
		for _, definition := range layout.indexes {
			if _, exists := indexes[definition.name]; exists {
				t.Fatalf("%q has two indexes named %q", layout.name, definition.name)
			}
			indexes[definition.name] = definition
		}
		byCollection[layout.name] = indexes
	}
	if _, ok := byCollection[collectionControl]; ok {
		t.Fatalf("processor_control has no index in the layout: %v", byCollection[collectionControl])
	}
	checks := []struct {
		collection string
		name       string
		keys       bson.D
		ttl        *time.Duration
		unique     bool
		partial    bson.D
	}{
		{collectionOutbox, "sentAt_1_createdAt_1_seq_1", ascending("sentAt", "createdAt", "seq"), nil, false, nil},
		{collectionOutbox, "sentAt_ttl", ascending("sentAt"), duration(DefaultOutboxRetention), false, nil},
		{collectionProcessed, "processedAt_ttl", ascending("processedAt"), duration(DefaultProcessedRetention), false, nil},
		{collectionExternalCalls, "updatedAt_ttl", ascending("updatedAt"), duration(DefaultExternalCallRetention), false, nil},
		{collectionDeadLetters, "dead_letters_processor_key_offset", ascending("processor", "key", "offset"), nil, false, nil},
		{collectionDeadLetters, "dead_letters_processor_parked", ascending("processor", "parkedAt"), nil, false, nil},
		{collectionDeadLetters, "dead_letters_expire", ascending("expireAt"), duration(0), false, nil},
		{collectionSagas, "type_associations_live", ascending("type", "associations.k", "associations.v"), nil, false, bson.D{{Key: "ended", Value: false}}},
		{collectionSagas, "startKey_live_unique", ascending("startKey"), nil, true, bson.D{
			{Key: "ended", Value: false},
			{Key: "startKey", Value: bson.D{{Key: "$exists", Value: true}}},
		}},
		{collectionSagas, "endedAt_ttl", ascending("endedAt"), duration(DefaultSagaEndedRetention), false, nil},
		{collectionSagaDispatch, "sagaId_1_seq_1", ascending("sagaId", "seq"), nil, false, nil},
		{collectionSagaDispatch, "dispatchedAt_1_failedAt_1_sagaId_1_seq_1", ascending("dispatchedAt", "failedAt", "sagaId", "seq"), nil, false, nil},
		{collectionSagaDispatch, "dispatchedAt_ttl", ascending("dispatchedAt"), duration(DefaultSagaDispatchRetention), false, nil},
		{collectionSagaDispatch, "failedAt_ttl", ascending("failedAt"), duration(DefaultFailedRetention), false, nil},
		{collectionSagaDispatch, "noticePending_noticeNextAt", ascending("noticeNextAt"), nil, false, bson.D{{Key: "noticePending", Value: true}}},
		{collectionSagaDispatchLease, "until_ttl", ascending("until"), duration(0), false, nil},
		{collectionDeadlines, "status_1_dueAt_1", ascending("status", "dueAt"), nil, false, nil},
		{collectionDeadlines, "sagaId_1_status_1", ascending("sagaId", "status"), nil, false, nil},
		{collectionDeadlines, "finishedAt_ttl", ascending("finishedAt"), duration(DefaultDeadlineRetention), false, nil},
		{collectionSagaFailures, "sagaType_1_lastFailedAt_1", ascending("sagaType", "lastFailedAt"), nil, false, nil},
		{collectionSagaFailures, "lastFailedAt_ttl", ascending("lastFailedAt"), duration(DefaultFailedRetention), false, nil},
	}
	for _, check := range checks {
		indexes, ok := byCollection[check.collection]
		if !ok {
			t.Fatalf("no collection %q", check.collection)
		}
		definition, ok := indexes[check.name]
		if !ok {
			t.Fatalf("no index %q on %q", check.name, check.collection)
		}
		if !reflect.DeepEqual(definition.keys, check.keys) {
			t.Fatalf("%q keys: got %v, want %v", check.name, definition.keys, check.keys)
		}
		switch {
		case definition.ttl == nil && check.ttl == nil:
		case definition.ttl == nil || check.ttl == nil:
			t.Fatalf("%q TTL: got %v, want %v", check.name, definition.ttl, check.ttl)
		case *definition.ttl != *check.ttl:
			t.Fatalf("%q TTL: got %v, want %v", check.name, *definition.ttl, *check.ttl)
		}
		if definition.unique != check.unique || !reflect.DeepEqual(definition.partial, check.partial) {
			t.Fatalf("%q unique/partial: got %v / %v, want %v / %v", check.name, definition.unique, definition.partial, check.unique, check.partial)
		}
	}
	if got, want := len(checks), 21; got != want {
		t.Fatalf("checked %d indexes, the layout has %d", got, want)
	}
}

// A TTL in seconds rounds a positive fraction up; zero stays zero (it is a real TTL).
func TestTTLSeconds(t *testing.T) {
	for retention, want := range map[time.Duration]int32{
		0:                       0,
		-time.Second:            0,
		500 * time.Millisecond:  1,
		1500 * time.Millisecond: 2,
		7 * 24 * time.Hour:      604800,
		30 * 24 * time.Hour:     2592000,
	} {
		if got := ttlSeconds(retention); got != want {
			t.Fatalf("ttlSeconds(%v): got %d, want %d", retention, got, want)
		}
	}
}

// A duplicate key, a write conflict and a transient transaction error are snodra.ErrConflict;
// every other driver error is returned as it is.
func TestStoreErrorMapsTheConflicts(t *testing.T) {
	conflicts := []error{
		mongo.CommandError{Code: duplicateKeyCode, Message: "duplicate"},
		mongo.CommandError{Code: writeConflictCode, Message: "write conflict"},
		mongo.CommandError{Code: 251, Message: "transient", Labels: []string{transientTransaction}},
		mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: duplicateKeyCode, Message: "duplicate"}}},
		mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: writeConflictCode, Message: "conflict"}}},
	}
	for _, err := range conflicts {
		if mapped := storeError(err); !errors.Is(mapped, snodra.ErrConflict) {
			t.Fatalf("%v: %v, want ErrConflict", err, mapped)
		}
	}
	for _, err := range []error{
		mongo.CommandError{Code: 2, Message: "bad value"},
		mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 121, Message: "document validation"}}},
		errors.New("network"),
	} {
		if mapped := storeError(err); errors.Is(mapped, snodra.ErrConflict) {
			t.Fatalf("%v: mapped to ErrConflict", err)
		}
	}
	if storeError(nil) != nil {
		t.Fatal("a nil error is not nil")
	}
}

// sameJSON fails unless two payloads are the same JSON value.
func sameJSON(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("the claimed payload is not JSON: %v", err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("the appended payload is not JSON: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("the payload changed: got %s, want %s", got, want)
	}
}
