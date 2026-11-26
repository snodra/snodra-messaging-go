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

// mongoProcessed is the snodra.ProcessedStore on the processed_events collection.
type mongoProcessed struct {
	store *Store
}

var (
	_ snodra.ProcessedStore  = mongoProcessed{}
	_ snodra.ProcessedEvents = mongoProcessed{}
)

// Record records marker in the transaction carried by ctx, or on its own; false when its id is
// recorded already. A duplicate key inside a transaction may have aborted it, which the caller's
// rollback finishes.
func (p mongoProcessed) Record(ctx context.Context, marker snodra.ProcessedMarker) (bool, error) {
	scope, err := p.store.scope(ctx)
	if err != nil {
		return false, err
	}
	_, err = p.store.db.Collection(collectionProcessed).InsertOne(scope, markerDocument(marker))
	if err != nil {
		if duplicateKey(err) {
			return false, nil
		}
		return false, storeError(err)
	}
	return true, nil
}

// Exists reports whether a marker with id is committed; a marker recorded in an open transaction
// is not visible yet.
func (p mongoProcessed) Exists(ctx context.Context, id string) (bool, error) {
	ctx = p.store.committed(ctx)
	err := p.store.db.Collection(collectionProcessed).FindOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: int32(1)}}),
	).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, storeError(err)
	}
	return true, nil
}

// DeleteMarkers deletes the stage-1 markers of processor in ranges, and nothing else; how many.
// Positions compare as MongoDB compares the stored values: numbers by value, strings by their
// bytes, never across the two. It never joins a transaction.
func (p mongoProcessed) DeleteMarkers(ctx context.Context, processor string, ranges []snodra.ReplayRange) (int64, error) {
	if len(ranges) == 0 {
		return 0, nil
	}
	ctx = p.store.committed(ctx)
	collection := p.store.db.Collection(collectionProcessed)
	var deleted int64
	for _, replay := range ranges {
		result, err := collection.DeleteMany(ctx, bson.D{
			{Key: "processor", Value: processor},
			{Key: "topic", Value: replay.Source},
			{Key: "partition", Value: segmentValue(replay.Segment)},
			{Key: "offset", Value: bson.D{
				{Key: "$gte", Value: positionValue(replay.From)},
				{Key: "$lt", Value: positionValue(replay.Until)},
			}},
		})
		if err != nil {
			return deleted, storeError(err)
		}
		deleted += result.DeletedCount
	}
	return deleted, nil
}

// Purge removes the markers processed before before; how many.
func (p mongoProcessed) Purge(ctx context.Context, before time.Time) (int64, error) {
	ctx = p.store.committed(ctx)
	result, err := p.store.db.Collection(collectionProcessed).DeleteMany(ctx,
		bson.D{{Key: "processedAt", Value: bson.D{{Key: "$lt", Value: bson.NewDateTimeFromTime(before)}}}})
	if err != nil {
		return 0, storeError(err)
	}
	return result.DeletedCount, nil
}
