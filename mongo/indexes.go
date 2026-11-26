package mongo

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// The layout's indexes. The names are the ones every Snodra MongoDB add-on has always given them,
// so a database indexed by one is accepted by the others. Every index has ascending keys and an
// explicit name: MongoDB refuses a second index with the same keys and options under another name.
type indexDefinition struct {
	name    string
	keys    bson.D
	ttl     *time.Duration
	unique  bool
	partial bson.D
}

// collectionLayout is the indexes of one collection.
type collectionLayout struct {
	name    string
	indexes []indexDefinition
}

// layoutIndexes returns every collection and index InitSchema creates, with the retentions of
// settings. processor_control has no index besides _id and is therefore not created here: its
// writes commit on their own, never inside a transaction, so the first one may create it.
func layoutIndexes(settings Config) []collectionLayout {
	return []collectionLayout{
		{
			name: collectionOutbox,
			indexes: []indexDefinition{
				{name: "sentAt_1_createdAt_1_seq_1", keys: ascending("sentAt", "createdAt", "seq")},
				{name: "sentAt_ttl", keys: ascending("sentAt"), ttl: duration(settings.OutboxRetention)},
			},
		},
		{
			name: collectionProcessed,
			indexes: []indexDefinition{
				{name: "processedAt_ttl", keys: ascending("processedAt"), ttl: duration(settings.ProcessedRetention)},
			},
		},
		{
			name: collectionExternalCalls,
			indexes: []indexDefinition{
				{name: "updatedAt_ttl", keys: ascending("updatedAt"), ttl: duration(settings.ExternalCallRetention)},
			},
		},
		{
			name: collectionDeadLetters,
			indexes: []indexDefinition{
				{name: "dead_letters_processor_key_offset", keys: ascending("processor", "key", "offset")},
				{name: "dead_letters_processor_parked", keys: ascending("processor", "parkedAt")},
				{name: "dead_letters_expire", keys: ascending("expireAt"), ttl: duration(0)},
			},
		},
		{
			name: collectionSagas,
			indexes: []indexDefinition{
				{
					name:    "type_associations_live",
					keys:    ascending("type", "associations.k", "associations.v"),
					partial: bson.D{{Key: "ended", Value: false}},
				},
				{
					name:   "startKey_live_unique",
					keys:   ascending("startKey"),
					unique: true,
					partial: bson.D{
						{Key: "ended", Value: false},
						{Key: "startKey", Value: bson.D{{Key: "$exists", Value: true}}},
					},
				},
				{name: "endedAt_ttl", keys: ascending("endedAt"), ttl: duration(settings.SagaEndedRetention)},
			},
		},
		{
			name: collectionSagaDispatch,
			indexes: []indexDefinition{
				{name: "sagaId_1_seq_1", keys: ascending("sagaId", "seq")},
				{
					name: "dispatchedAt_1_failedAt_1_sagaId_1_seq_1",
					keys: ascending("dispatchedAt", "failedAt", "sagaId", "seq"),
				},
				{name: "dispatchedAt_ttl", keys: ascending("dispatchedAt"), ttl: duration(settings.SagaDispatchRetention)},
				{name: "failedAt_ttl", keys: ascending("failedAt"), ttl: duration(settings.FailedRetention)},
				{
					name:    "noticePending_noticeNextAt",
					keys:    ascending("noticeNextAt"),
					partial: bson.D{{Key: "noticePending", Value: true}},
				},
			},
		},
		{
			name: collectionSagaDispatchLease,
			indexes: []indexDefinition{
				{name: "until_ttl", keys: ascending("until"), ttl: duration(0)},
			},
		},
		{
			name: collectionDeadlines,
			indexes: []indexDefinition{
				{name: "status_1_dueAt_1", keys: ascending("status", "dueAt")},
				{name: "sagaId_1_status_1", keys: ascending("sagaId", "status")},
				{name: "finishedAt_ttl", keys: ascending("finishedAt"), ttl: duration(settings.DeadlineRetention)},
			},
		},
		{
			name: collectionSagaFailures,
			indexes: []indexDefinition{
				{name: "sagaType_1_lastFailedAt_1", keys: ascending("sagaType", "lastFailedAt")},
				{name: "lastFailedAt_ttl", keys: ascending("lastFailedAt"), ttl: duration(settings.FailedRetention)},
			},
		},
	}
}

// ascending is an index key document of ascending fields.
func ascending(fields ...string) bson.D {
	keys := make(bson.D, 0, len(fields))
	for _, field := range fields {
		keys = append(keys, bson.E{Key: field, Value: int32(1)})
	}
	return keys
}

// duration returns a pointer to d: a TTL of zero is a real TTL (each document carries its own
// expiry in the field), so nil and zero must stay distinct.
func duration(d time.Duration) *time.Duration { return &d }

// ttlSeconds is d in whole seconds, floored as the reference stores a retention: a positive
// sub-second duration becomes zero, and the TTL monitor may then expire the document at once. A
// negative duration is zero.
func ttlSeconds(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	seconds := d / time.Second
	if seconds > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(seconds)
}

// indexModels is a collection's definitions as driver index models.
func indexModels(layout collectionLayout) []mongo.IndexModel {
	models := make([]mongo.IndexModel, 0, len(layout.indexes))
	for _, definition := range layout.indexes {
		builder := options.Index().SetName(definition.name)
		if definition.ttl != nil {
			builder.SetExpireAfterSeconds(ttlSeconds(*definition.ttl))
		}
		if definition.unique {
			builder.SetUnique(true)
		}
		if len(definition.partial) > 0 {
			builder.SetPartialFilterExpression(definition.partial)
		}
		models = append(models, mongo.IndexModel{Keys: definition.keys, Options: builder})
	}
	return models
}

// ensureIndexes creates every index of the layout, idempotently: an index that exists unchanged is
// left as it is, and creating the indexes creates the collections.
func (s *Store) ensureIndexes(ctx context.Context) error {
	for _, layout := range layoutIndexes(s.settings) {
		if _, err := s.db.Collection(layout.name).Indexes().CreateMany(ctx, indexModels(layout)); err != nil {
			return storeError(err)
		}
	}
	return nil
}

// listedIndex is one index as listIndexes returns it, with the options the layout fixes.
type listedIndex struct {
	Name                    string   `bson:"name"`
	Keys                    bson.D   `bson:"key"`
	ExpireAfterSeconds      *int32   `bson:"expireAfterSeconds"`
	Unique                  *bool    `bson:"unique"`
	PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
}

// listIndexes reads a collection's indexes by name.
func (s *Store) listIndexes(ctx context.Context, collection string) (map[string]listedIndex, error) {
	cursor, err := s.db.Collection(collection).Indexes().List(ctx)
	if err != nil {
		return nil, storeError(err)
	}
	indexes := make([]listedIndex, 0, 4)
	if err := cursor.All(ctx, &indexes); err != nil {
		_ = cursor.Close(ctx)
		return nil, storeError(err)
	}
	byName := make(map[string]listedIndex, len(indexes))
	for _, index := range indexes {
		byName[index.Name] = index
	}
	return byName, nil
}

// checkIndexes fails unless every collection and index of the layout exists with the same keys,
// TTL, uniqueness and partial filter. It creates nothing.
//
// TODO(spec-gap): SG-42 (the layout does not say whether a changed retention is a schema failure;
// this check treats a TTL that differs from the store's settings as one).
func (s *Store) checkIndexes(ctx context.Context) error {
	for _, layout := range layoutIndexes(s.settings) {
		present, err := s.db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: layout.name}})
		if err != nil {
			return storeError(err)
		}
		if len(present) == 0 {
			return fmt.Errorf("the MongoDB message store's schema has no %q collection: run InitSchema, or set SNODRA_STORE_INIT_SCHEMA=true", layout.name)
		}
		byName, err := s.listIndexes(ctx, layout.name)
		if err != nil {
			return err
		}
		for _, definition := range layout.indexes {
			actual, ok := byName[definition.name]
			if !ok {
				return fmt.Errorf("the MongoDB message store's schema has no index %q on %q: run InitSchema, or set SNODRA_STORE_INIT_SCHEMA=true", definition.name, layout.name)
			}
			if !reflect.DeepEqual(actual.Keys, definition.keys) {
				return fmt.Errorf("the MongoDB message store's index %q on %q has the keys %v, not %v: drop it and run InitSchema", definition.name, layout.name, actual.Keys, definition.keys)
			}
			wantTTL := int32(0)
			if definition.ttl != nil {
				wantTTL = ttlSeconds(*definition.ttl)
			}
			if definition.ttl == nil && actual.ExpireAfterSeconds != nil ||
				definition.ttl != nil && (actual.ExpireAfterSeconds == nil || *actual.ExpireAfterSeconds != wantTTL) {
				return fmt.Errorf("the MongoDB message store's index %q on %q has the TTL %v, not %v: every instance sharing a database needs the same retentions", definition.name, layout.name, actual.ExpireAfterSeconds, definition.ttl)
			}
			unique := actual.Unique != nil && *actual.Unique
			if unique != definition.unique {
				return fmt.Errorf("the MongoDB message store's index %q on %q is unique=%v, not %v: drop it and run InitSchema", definition.name, layout.name, unique, definition.unique)
			}
			var partial bson.D
			if len(actual.PartialFilterExpression) > 0 {
				if err := bson.Unmarshal(actual.PartialFilterExpression, &partial); err != nil {
					return fmt.Errorf("the MongoDB message store cannot read index %q on %q: %w", definition.name, layout.name, err)
				}
			}
			if !samePartialFilter(partial, definition.partial) {
				return fmt.Errorf("the MongoDB message store's index %q on %q filters %v, not %v: drop it and run InitSchema", definition.name, layout.name, partial, definition.partial)
			}
		}
	}
	return nil
}

// samePartialFilter compares two partial filter documents regardless of the order of their
// fields, which carries no meaning in MongoDB; an array keeps its order, which does.
func samePartialFilter(a, b bson.D) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == 0 && len(b) == 0
	}
	return reflect.DeepEqual(normalizedDocument(a), normalizedDocument(b))
}

// normalizedDocument sorts a document's fields by name and normalizes their values, recursively.
func normalizedDocument(document bson.D) bson.D {
	normalized := make(bson.D, len(document))
	for i, element := range document {
		normalized[i] = bson.E{Key: element.Key, Value: normalizedValue(element.Value)}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Key < normalized[j].Key })
	return normalized
}

// normalizedValue normalizes a document nested in a filter; an array keeps its order.
func normalizedValue(value any) any {
	switch value := value.(type) {
	case bson.D:
		return normalizedDocument(value)
	case bson.M:
		document := make(bson.D, 0, len(value))
		for key, element := range value {
			document = append(document, bson.E{Key: key, Value: element})
		}
		return normalizedDocument(document)
	case map[string]any:
		document := make(bson.D, 0, len(value))
		for key, element := range value {
			document = append(document, bson.E{Key: key, Value: element})
		}
		return normalizedDocument(document)
	case bson.A:
		normalized := make(bson.A, len(value))
		for i, element := range value {
			normalized[i] = normalizedValue(element)
		}
		return normalized
	case []any:
		normalized := make(bson.A, len(value))
		for i, element := range value {
			normalized[i] = normalizedValue(element)
		}
		return normalized
	default:
		return value
	}
}
