package mongo

import (
	"context"
	"errors"
	"sort"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	snodra "github.com/snodra/snodra-messaging-go"
)

// mongoDeadLetters is the snodra.DeadLetterStore on dead_letters. Every write but DeleteIn
// commits on its own, outside the session the context may carry (the layout's rule). MongoDB's
// TTL monitor removes an expired letter; until it does, the letter can still be read, as the
// published layout says.
type mongoDeadLetters struct {
	store *Store
}

var _ snodra.DeadLetterStore = mongoDeadLetters{}

// deadLetterDocument is one document of dead_letters, field for field as the published layout
// writes it: an optional field is absent, never null.
type deadLetterDocument struct {
	ID            string               `bson:"_id"`
	Processor     string               `bson:"processor"`
	Topic         string               `bson:"topic"`
	Partition     any                  `bson:"partition"`
	Offset        any                  `bson:"offset"`
	Key           *string              `bson:"key,omitempty"`
	EventID       *string              `bson:"eventId,omitempty"`
	EventType     *string              `bson:"eventType,omitempty"`
	Headers       map[string]string    `bson:"headers"`
	Payload       *string              `bson:"payload,omitempty"`
	PayloadBinary []byte               `bson:"payloadBinary,omitempty"`
	Reason        string               `bson:"reason"`
	Error         deadLetterErrorOfDoc `bson:"error"`
	BlockedBy     *string              `bson:"blockedBy,omitempty"`
	Attempts      int32                `bson:"attempts"`
	ParkedAt      bson.DateTime        `bson:"parkedAt"`
	LastAttemptAt bson.DateTime        `bson:"lastAttemptAt"`
	ExpireAt      bson.DateTime        `bson:"expireAt"`
	Replay        bool                 `bson:"replay,omitempty"`
}

// deadLetterErrorOfDoc is a letter's error: {"type", "message"}.
type deadLetterErrorOfDoc struct {
	Type    string `bson:"type"`
	Message string `bson:"message"`
}

// deadLetterDocumentOf converts a letter to its document: the payload is text when it is valid
// UTF-8 and not empty, else binary; neither when it is empty.
func deadLetterDocumentOf(letter snodra.DeadLetter) deadLetterDocument {
	document := deadLetterDocument{
		ID:            letter.ID,
		Processor:     letter.Processor,
		Topic:         letter.Source,
		Partition:     segmentValue(letter.Segment),
		Offset:        positionValue(letter.Position),
		Headers:       copyHeaders(letter.Headers),
		Reason:        string(letter.Reason),
		Error:         deadLetterErrorOfDoc{Type: letter.Error.Type, Message: letter.Error.Message},
		Attempts:      int32(letter.Attempts),
		ParkedAt:      controlDate(letter.ParkedAt),
		LastAttemptAt: controlDate(letter.LastAttemptAt),
		ExpireAt:      controlDate(letter.ExpireAt),
		Replay:        letter.Replay,
	}
	if document.Headers == nil {
		document.Headers = map[string]string{}
	}
	if letter.Key != nil {
		document.Key = copyString(letter.Key)
	}
	if letter.EventID != "" {
		document.EventID = copyString(&letter.EventID)
	}
	if letter.EventType != "" {
		document.EventType = copyString(&letter.EventType)
	}
	if letter.BlockedBy != "" {
		document.BlockedBy = copyString(&letter.BlockedBy)
	}
	if len(letter.Payload) > 0 {
		if utf8.Valid(letter.Payload) {
			payload := string(letter.Payload)
			document.Payload = &payload
		} else {
			document.PayloadBinary = append([]byte(nil), letter.Payload...)
		}
	}
	return document
}

// deadLetterOfDocument reads a letter document.
func deadLetterOfDocument(document deadLetterDocument) snodra.DeadLetter {
	letter := snodra.DeadLetter{
		ID:        document.ID,
		Processor: document.Processor,
		Source:    document.Topic,
		Segment:   positionTextValue(document.Partition),
		Position:  positionTextValue(document.Offset),
		Key:       copyString(document.Key),
		Headers:   copyHeaders(document.Headers),
		Reason:    snodra.DeadLetterReason(document.Reason),
		Error:     snodra.DeadLetterError{Type: document.Error.Type, Message: document.Error.Message},
		Attempts:  int(document.Attempts),
		ParkedAt:  document.ParkedAt.Time().UTC(),
		// A document that leaves lastAttemptAt out reads as parkedAt, the layout's rule.
		LastAttemptAt: document.LastAttemptAt.Time().UTC(),
		ExpireAt:      document.ExpireAt.Time().UTC(),
		Replay:        document.Replay,
	}
	if document.LastAttemptAt == 0 {
		letter.LastAttemptAt = letter.ParkedAt
	}
	if document.EventID != nil {
		letter.EventID = *document.EventID
	}
	if document.EventType != nil {
		letter.EventType = *document.EventType
	}
	if document.BlockedBy != nil {
		letter.BlockedBy = *document.BlockedBy
	}
	if document.Payload != nil {
		letter.Payload = []byte(*document.Payload)
	} else {
		letter.Payload = append([]byte(nil), document.PayloadBinary...)
	}
	if letter.Headers == nil {
		letter.Headers = map[string]string{}
	}
	return letter
}

// positionTextValue is a stored segment or position as text; "" when it is neither.
func positionTextValue(value any) string {
	text, _ := positionText(value)
	return text
}

// Park inserts letter; ParkAlready when a letter of that id exists.
func (d mongoDeadLetters) Park(ctx context.Context, letter snodra.DeadLetter) (snodra.ParkResult, error) {
	ctx = d.store.committed(ctx)
	_, err := d.store.db.Collection(collectionDeadLetters).InsertOne(ctx, deadLetterDocumentOf(letter))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return snodra.ParkAlready, nil
		}
		return 0, storeError(err)
	}
	return snodra.ParkNew, nil
}

// Head is the id of the head letter of (processor, key) that is not INVALID.
func (d mongoDeadLetters) Head(ctx context.Context, processor, key string) (string, bool, error) {
	ctx = d.store.committed(ctx)
	var document struct {
		ID string `bson:"_id"`
	}
	err := d.store.db.Collection(collectionDeadLetters).FindOne(ctx,
		bson.D{
			{Key: "processor", Value: processor},
			{Key: "key", Value: key},
			{Key: "reason", Value: bson.D{{Key: "$ne", Value: string(snodra.DeadLetterInvalid)}}},
		},
		options.FindOne().
			SetSort(bson.D{{Key: "offset", Value: int32(1)}, {Key: "_id", Value: int32(1)}}).
			SetProjection(bson.D{{Key: "_id", Value: int32(1)}}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, storeError(err)
	}
	return document.ID, true, nil
}

// Earlier is the id of the earliest blocking letter of (processor, key) before position.
func (d mongoDeadLetters) Earlier(ctx context.Context, processor, key, position string) (string, bool, error) {
	ctx = d.store.committed(ctx)
	var document struct {
		ID string `bson:"_id"`
	}
	err := d.store.db.Collection(collectionDeadLetters).FindOne(ctx,
		bson.D{
			{Key: "processor", Value: processor},
			{Key: "key", Value: key},
			{Key: "reason", Value: bson.D{{Key: "$ne", Value: string(snodra.DeadLetterInvalid)}}},
			{Key: "offset", Value: bson.D{{Key: "$lt", Value: positionValue(position)}}},
		},
		options.FindOne().
			SetSort(bson.D{{Key: "offset", Value: int32(1)}, {Key: "_id", Value: int32(1)}}).
			SetProjection(bson.D{{Key: "_id", Value: int32(1)}}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, storeError(err)
	}
	return document.ID, true, nil
}

// ParkedKeys is the keys with a letter (not INVALID) of processor on the given segments of
// source; each key once, in no particular order, none for no segments.
func (d mongoDeadLetters) ParkedKeys(ctx context.Context, processor, source string, segments []string) ([]string, error) {
	if len(segments) == 0 {
		return nil, nil
	}
	partitions := make(bson.A, len(segments))
	for i, segment := range segments {
		partitions[i] = segmentValue(segment)
	}
	ctx = d.store.committed(ctx)
	cursor, err := d.store.db.Collection(collectionDeadLetters).Find(ctx,
		bson.D{
			{Key: "processor", Value: processor},
			{Key: "topic", Value: source},
			{Key: "partition", Value: bson.D{{Key: "$in", Value: partitions}}},
			{Key: "reason", Value: bson.D{{Key: "$ne", Value: string(snodra.DeadLetterInvalid)}}},
			{Key: "key", Value: bson.D{{Key: "$exists", Value: true}}},
		},
		options.Find().SetProjection(bson.D{{Key: "key", Value: int32(1)}}))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	seen := map[string]bool{}
	var keys []string
	for cursor.Next(ctx) {
		var document struct {
			Key *string `bson:"key"`
		}
		if err := cursor.Decode(&document); err != nil {
			return nil, storeError(err)
		}
		if document.Key == nil || seen[*document.Key] {
			continue
		}
		seen[*document.Key] = true
		keys = append(keys, *document.Key)
	}
	if err := cursor.Err(); err != nil {
		return nil, storeError(err)
	}
	sort.Strings(keys)
	return keys, nil
}

// Get returns the letter, or nil when there is none.
func (d mongoDeadLetters) Get(ctx context.Context, id string) (*snodra.DeadLetter, error) {
	ctx = d.store.committed(ctx)
	var document deadLetterDocument
	err := d.store.db.Collection(collectionDeadLetters).FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	letter := deadLetterOfDocument(document)
	return &letter, nil
}

// List returns letters by ParkedAt, then id, every processor's when processor is empty, at most
// limit when limit > 0.
func (d mongoDeadLetters) List(ctx context.Context, processor string, limit int) ([]snodra.DeadLetter, error) {
	ctx = d.store.committed(ctx)
	filter := bson.D{}
	if processor != "" {
		filter = append(filter, bson.E{Key: "processor", Value: processor})
	}
	find := options.Find().
		SetSort(bson.D{{Key: "parkedAt", Value: int32(1)}, {Key: "_id", Value: int32(1)}})
	if limit > 0 {
		find = find.SetLimit(int64(limit))
	}
	cursor, err := d.store.db.Collection(collectionDeadLetters).Find(ctx, filter, find)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var documents []deadLetterDocument
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, storeError(err)
	}
	letters := make([]snodra.DeadLetter, len(documents))
	for i, document := range documents {
		letters[i] = deadLetterOfDocument(document)
	}
	return letters, nil
}

// Summaries returns every letter of processor, without its record, by position then id.
func (d mongoDeadLetters) Summaries(ctx context.Context, processor string) ([]snodra.DeadLetterSummary, error) {
	ctx = d.store.committed(ctx)
	cursor, err := d.store.db.Collection(collectionDeadLetters).Find(ctx,
		bson.D{{Key: "processor", Value: processor}},
		options.Find().
			SetProjection(bson.D{
				{Key: "key", Value: int32(1)},
				{Key: "offset", Value: int32(1)},
				{Key: "reason", Value: int32(1)},
				{Key: "parkedAt", Value: int32(1)},
			}).
			SetSort(bson.D{{Key: "offset", Value: int32(1)}, {Key: "_id", Value: int32(1)}}))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var summaries []snodra.DeadLetterSummary
	for cursor.Next(ctx) {
		var document struct {
			ID       string        `bson:"_id"`
			Key      *string       `bson:"key"`
			Offset   any           `bson:"offset"`
			Reason   string        `bson:"reason"`
			ParkedAt bson.DateTime `bson:"parkedAt"`
		}
		if err := cursor.Decode(&document); err != nil {
			return nil, storeError(err)
		}
		summary := snodra.DeadLetterSummary{
			ID:       document.ID,
			Key:      copyString(document.Key),
			Position: positionTextValue(document.Offset),
			Reason:   snodra.DeadLetterReason(document.Reason),
			ParkedAt: document.ParkedAt.Time().UTC(),
		}
		summaries = append(summaries, summary)
	}
	if err := cursor.Err(); err != nil {
		return nil, storeError(err)
	}
	return summaries, nil
}

// Count is how many letters there are; every processor's when processor is empty.
func (d mongoDeadLetters) Count(ctx context.Context, processor string) (int64, error) {
	ctx = d.store.committed(ctx)
	filter := bson.D{}
	if processor != "" {
		filter = append(filter, bson.E{Key: "processor", Value: processor})
	}
	count, err := d.store.db.Collection(collectionDeadLetters).CountDocuments(ctx, filter)
	if err != nil {
		return 0, storeError(err)
	}
	return count, nil
}

// Delete deletes the letter; false when there was none.
func (d mongoDeadLetters) Delete(ctx context.Context, id string) (bool, error) {
	ctx = d.store.committed(ctx)
	return d.delete(ctx, id)
}

// DeleteIn deletes the letter in the transaction carried by ctx.
func (d mongoDeadLetters) DeleteIn(ctx context.Context, id string) (bool, error) {
	scope, err := d.store.scope(ctx)
	if err != nil {
		return false, err
	}
	return d.delete(scope, id)
}

// delete is Delete and DeleteIn.
func (d mongoDeadLetters) delete(ctx context.Context, id string) (bool, error) {
	result, err := d.store.db.Collection(collectionDeadLetters).DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return false, storeError(err)
	}
	return result.DeletedCount > 0, nil
}

// RecordFailure records that a retry failed again: Attempts + 1, Error, LastAttemptAt = now,
// reason HANDLER_FAILED, no BlockedBy any more.
func (d mongoDeadLetters) RecordFailure(ctx context.Context, id string, failure snodra.DeadLetterError, now time.Time) (bool, error) {
	ctx = d.store.committed(ctx)
	result, err := d.store.db.Collection(collectionDeadLetters).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: "attempts", Value: int32(1)}}},
			{Key: "$set", Value: bson.D{
				{Key: "reason", Value: string(snodra.DeadLetterHandlerFailed)},
				{Key: "error", Value: deadLetterErrorOfDoc{Type: failure.Type, Message: failure.Message}},
				{Key: "lastAttemptAt", Value: controlDate(now)},
			}},
			{Key: "$unset", Value: bson.D{{Key: "blockedBy", Value: ""}}},
		})
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}
