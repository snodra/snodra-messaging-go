package mongo

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	snodra "github.com/snodra/snodra-messaging-go"
)

// mongoControl is the snodra.ProcessorControlStore on processor_control. Every write commits on
// its own, outside the session the context may carry: a pause, a lock or a replay range stays when
// the service's transaction rolls back (the layout's rule).
type mongoControl struct {
	store *Store
}

var _ snodra.ProcessorControlStore = mongoControl{}

// controlDate is t as the layout's date, at millisecond precision.
func controlDate(t time.Time) bson.DateTime {
	return bson.NewDateTimeFromTime(t.Truncate(time.Millisecond))
}

// Get is the state of processor; the zero state when the store has none.
func (c mongoControl) Get(ctx context.Context, processor string) (snodra.ProcessorControlState, error) {
	ctx = c.store.committed(ctx)
	var document bson.M
	err := c.store.db.Collection(collectionControl).FindOne(ctx, bson.D{{Key: "_id", Value: processor}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return snodra.ProcessorControlState{Processor: processor}, nil
	}
	if err != nil {
		return snodra.ProcessorControlState{}, storeError(err)
	}
	return controlStateOf(processor, document), nil
}

// controlStateOf reads a control document; every missing field is its empty value.
func controlStateOf(processor string, document bson.M) snodra.ProcessorControlState {
	state := snodra.ProcessorControlState{Processor: processor}
	if paused, ok := document["paused"].(bool); ok {
		state.Paused = paused
	}
	if at, ok := controlTimeOf(document["pausedAt"]); ok {
		state.PausedAt = at
	}
	if by, ok := document["pausedBy"].(string); ok {
		state.PausedBy = by
	}
	if operation, ok := controlDocumentOf(document["operation"]); ok {
		state.Operation = controlOperationOf(operation)
	}
	if stored, ok := document["replay"].(bson.A); ok {
		for _, item := range stored {
			rangeDocument, ok := controlDocumentOf(item)
			if !ok {
				continue
			}
			topic, _ := rangeDocument["topic"].(string)
			partition, okPartition := positionText(rangeDocument["partition"])
			from, okFrom := positionText(rangeDocument["from"])
			until, okUntil := positionText(rangeDocument["until"])
			if !okPartition || !okFrom || !okUntil {
				continue
			}
			state.Replay = append(state.Replay, snodra.ReplayRange{Source: topic, Segment: partition, From: from, Until: until})
		}
	}
	if at, ok := controlTimeOf(document["updatedAt"]); ok {
		state.UpdatedAt = at
	}
	return state
}

// controlDocumentOf reads an embedded document, which the driver may decode as a map or a
// sequence.
func controlDocumentOf(value any) (bson.M, bool) {
	switch value := value.(type) {
	case bson.M:
		return value, true
	case bson.D:
		document := make(bson.M, len(value))
		for _, field := range value {
			document[field.Key] = field.Value
		}
		return document, true
	}
	return nil, false
}

// controlOperationOf reads the operation document.
func controlOperationOf(document bson.M) *snodra.ControlOperation {
	operation := snodra.ControlOperation{Kind: snodra.ResetKind}
	if id, ok := document["id"].(string); ok {
		operation.ID = id
	}
	if kind, ok := document["kind"].(string); ok && kind != "" {
		operation.Kind = kind
	}
	if at, ok := controlTimeOf(document["leaseUntil"]); ok {
		operation.LeaseUntil = at
	}
	if at, ok := controlTimeOf(document["preparedAt"]); ok {
		operation.PreparedAt = at
	}
	return &operation
}

// controlTimeOf reads a stored date.
func controlTimeOf(value any) (time.Time, bool) {
	switch value := value.(type) {
	case bson.DateTime:
		return value.Time().UTC(), true
	case time.Time:
		return value.UTC(), true
	}
	return time.Time{}, false
}

// controlOperationDocument is an operation as the layout writes it: id, kind, leaseUntil,
// preparedAt, in that order.
func controlOperationDocument(operation string, leaseUntil, preparedAt time.Time) bson.D {
	return bson.D{
		{Key: "id", Value: operation},
		{Key: "kind", Value: snodra.ResetKind},
		{Key: "leaseUntil", Value: controlDate(leaseUntil)},
		{Key: "preparedAt", Value: controlDate(preparedAt)},
	}
}

// emptyControlFields are the fields an upsert inserts when it creates the document.
func emptyControlFields() bson.D {
	return bson.D{
		{Key: "paused", Value: false},
		{Key: "pausedAt", Value: nil},
		{Key: "pausedBy", Value: nil},
	}
}

// SetPaused pauses or resumes processor, idempotently.
func (c mongoControl) SetPaused(ctx context.Context, processor string, paused bool, by string, now time.Time) error {
	ctx = c.store.committed(ctx)
	set := bson.D{
		{Key: "paused", Value: paused},
		{Key: "pausedAt", Value: nil},
		{Key: "pausedBy", Value: nil},
		{Key: "updatedAt", Value: controlDate(now)},
	}
	if paused {
		set[1].Value = controlDate(now)
		set[2].Value = by
	}
	// $set already writes paused, pausedAt and pausedBy, on the insert too: only the other
	// empty fields go in $setOnInsert.
	insert := bson.D{{Key: "operation", Value: nil}, {Key: "replay", Value: bson.A{}}}
	update := bson.D{{Key: "$set", Value: set}, {Key: "$setOnInsert", Value: insert}}
	_, err := c.store.db.Collection(collectionControl).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: processor}}, update, options.UpdateOne().SetUpsert(true))
	return storeError(err)
}

// Lock takes the lock of processor for operation until leaseUntil, atomically.
func (c mongoControl) Lock(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (snodra.LockOutcome, error) {
	ctx = c.store.committed(ctx)
	collection := c.store.db.Collection(collectionControl)
	renewed, err := collection.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: processor}, {Key: "operation.id", Value: operation}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "operation.leaseUntil", Value: controlDate(leaseUntil)},
			{Key: "updatedAt", Value: controlDate(now)},
		}}})
	if err != nil {
		return snodra.LockOutcome{}, storeError(err)
	}
	if renewed.MatchedCount == 1 {
		return snodra.LockOutcome{Taken: true}, nil
	}
	free := bson.D{
		{Key: "_id", Value: processor},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "operation", Value: nil}},
			bson.D{{Key: "operation.leaseUntil", Value: bson.D{{Key: "$lte", Value: controlDate(now)}}}},
		}},
	}
	insert := append(emptyControlFields(), bson.E{Key: "replay", Value: bson.A{}})
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "operation", Value: controlOperationDocument(operation, leaseUntil, now)},
			{Key: "updatedAt", Value: controlDate(now)},
		}},
		{Key: "$setOnInsert", Value: insert},
	}
	taken, err := collection.UpdateOne(ctx, free, update, options.UpdateOne().SetUpsert(true))
	if err == nil && (taken.MatchedCount == 1 || taken.UpsertedCount == 1) {
		return snodra.LockOutcome{Taken: true}, nil
	}
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return snodra.LockOutcome{}, storeError(err)
	}
	state, err := c.Get(ctx, processor)
	if err != nil {
		return snodra.LockOutcome{}, err
	}
	outcome := snodra.LockOutcome{}
	if state.Operation != nil {
		outcome.Holder = *state.Operation
	}
	return outcome, nil
}

// Extend moves the lease of operation; false when it does not hold the lock.
func (c mongoControl) Extend(ctx context.Context, processor, operation string, leaseUntil, now time.Time) (bool, error) {
	ctx = c.store.committed(ctx)
	result, err := c.store.db.Collection(collectionControl).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: processor}, {Key: "operation.id", Value: operation}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "operation.leaseUntil", Value: controlDate(leaseUntil)},
			{Key: "updatedAt", Value: controlDate(now)},
		}}})
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// Release clears the lock when operation holds it.
func (c mongoControl) Release(ctx context.Context, processor, operation string, now time.Time) (bool, error) {
	return c.clearLock(ctx, processor, operation, now, false)
}

// Expire clears the lock when operation holds it and its lease has passed.
func (c mongoControl) Expire(ctx context.Context, processor, operation string, now time.Time) (bool, error) {
	return c.clearLock(ctx, processor, operation, now, true)
}

// clearLock is Release and Expire.
func (c mongoControl) clearLock(ctx context.Context, processor, operation string, now time.Time, expiredOnly bool) (bool, error) {
	ctx = c.store.committed(ctx)
	filter := bson.D{{Key: "_id", Value: processor}, {Key: "operation.id", Value: operation}}
	if expiredOnly {
		filter = append(filter, bson.E{Key: "operation.leaseUntil", Value: bson.D{{Key: "$lte", Value: controlDate(now)}}})
	}
	result, err := c.store.db.Collection(collectionControl).UpdateOne(ctx, filter,
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "operation", Value: nil},
			{Key: "updatedAt", Value: controlDate(now)},
		}}})
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// SetReplay replaces the replay ranges of processor.
func (c mongoControl) SetReplay(ctx context.Context, processor string, ranges []snodra.ReplayRange, now time.Time) error {
	ctx = c.store.committed(ctx)
	stored := make(bson.A, 0, len(ranges))
	for _, replay := range ranges {
		stored = append(stored, replayRangeDocument(replay))
	}
	insert := append(emptyControlFields(), bson.E{Key: "operation", Value: nil})
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "replay", Value: stored},
			{Key: "updatedAt", Value: controlDate(now)},
		}},
		{Key: "$setOnInsert", Value: insert},
	}
	_, err := c.store.db.Collection(collectionControl).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: processor}}, update, options.UpdateOne().SetUpsert(true))
	return storeError(err)
}

// replayRangeDocument is a range as the layout writes it: topic, partition, from, until, in that
// order; a segment and a position that are canonical decimals go as their numbers.
func replayRangeDocument(replay snodra.ReplayRange) bson.D {
	return bson.D{
		{Key: "topic", Value: replay.Source},
		{Key: "partition", Value: segmentValue(replay.Segment)},
		{Key: "from", Value: positionValue(replay.From)},
		{Key: "until", Value: positionValue(replay.Until)},
	}
}

// RemoveRange removes that exact range of processor; false when it was not there.
func (c mongoControl) RemoveRange(ctx context.Context, processor string, replay snodra.ReplayRange, now time.Time) (bool, error) {
	ctx = c.store.committed(ctx)
	document := replayRangeDocument(replay)
	filter := bson.D{
		{Key: "_id", Value: processor},
		{Key: "replay", Value: bson.D{{Key: "$elemMatch", Value: document}}},
	}
	update := bson.D{
		{Key: "$pull", Value: bson.D{{Key: "replay", Value: document}}},
		{Key: "$set", Value: bson.D{{Key: "updatedAt", Value: controlDate(now)}}},
	}
	result, err := c.store.db.Collection(collectionControl).UpdateOne(ctx, filter, update)
	if err != nil {
		return false, storeError(err)
	}
	return result.ModifiedCount == 1, nil
}
