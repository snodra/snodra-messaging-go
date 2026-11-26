package mongo

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	snodra "github.com/snodra/snodra-messaging-go"
)

// mongoOutbox is the snodra.OutboxStore on the outbox collection.
type mongoOutbox struct {
	store *Store
}

var _ snodra.OutboxStore = mongoOutbox{}

// Append appends entry in the transaction carried by ctx, or on its own. The payload contract is
// checked first, so a refused payload writes nothing and does not spoil a transaction. A duplicate
// id is snodra.ErrConflict.
func (o mongoOutbox) Append(ctx context.Context, entry snodra.OutboxEntry) error {
	document, err := outboxDocumentOf(entry)
	if err != nil {
		return err
	}
	scope, err := o.store.scope(ctx)
	if err != nil {
		return err
	}
	_, err = o.store.db.Collection(collectionOutbox).InsertOne(scope, document)
	return storeError(err)
}

// Claim claims up to limit pending entries for owner under a lease from the application's clock,
// in (createdAt, seq, _id) order: it finds the candidates, then takes each with its own conditional
// find-and-modify whose filter matches the entry only while it is still unfinished and claimable,
// so of two relays racing for an entry exactly one wins it. It repeats until limit entries are won
// or no candidate is left. It never joins a transaction: a claim commits on its own.
func (o mongoOutbox) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]snodra.OutboxEntry, error) {
	if owner == "" {
		return nil, invalidf("needs a non-empty claim owner")
	}
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx = o.store.committed(ctx)
	collection := o.store.db.Collection(collectionOutbox)
	won := make([]outboxDocument, 0, limit)
	wonIDs := make([]string, 0, limit)
	for len(won) < limit {
		now := o.store.now()
		filter := unfinished()
		filter = append(filter, claimableBy(owner, bson.NewDateTimeFromTime(now))...)
		if len(wonIDs) > 0 {
			filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$nin", Value: wonIDs}}})
		}
		findOptions := options.Find().
			SetSort(bson.D{
				{Key: "createdAt", Value: int32(1)},
				{Key: "seq", Value: int32(1)},
				{Key: "_id", Value: int32(1)},
			}).
			SetLimit(int64(limit - len(won)))
		cursor, err := collection.Find(ctx, filter, findOptions)
		if err != nil {
			return nil, storeError(err)
		}
		var candidates []outboxDocument
		if err := cursor.All(ctx, &candidates); err != nil {
			_ = cursor.Close(ctx)
			return nil, storeError(err)
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			now := o.store.now()
			var claimed outboxDocument
			err := collection.FindOneAndUpdate(ctx,
				claimFilter(candidate.ID, owner, bson.NewDateTimeFromTime(now)),
				bson.D{{Key: "$set", Value: bson.D{
					{Key: "owner", Value: owner},
					{Key: "leaseUntil", Value: bson.NewDateTimeFromTime(now.Add(lease))},
				}}},
				options.FindOneAndUpdate().SetReturnDocument(options.After),
			).Decode(&claimed)
			if errors.Is(err, mongo.ErrNoDocuments) {
				// Another relay won this entry between the find and the update.
				continue
			}
			if err != nil {
				return nil, storeError(err)
			}
			won = append(won, claimed)
			wonIDs = append(wonIDs, claimed.ID)
			if len(won) >= limit {
				break
			}
		}
	}
	sort.SliceStable(won, func(i, j int) bool {
		if won[i].CreatedAt != won[j].CreatedAt {
			return won[i].CreatedAt < won[j].CreatedAt
		}
		if won[i].Seq != won[j].Seq {
			return won[i].Seq < won[j].Seq
		}
		return won[i].ID < won[j].ID
	})
	entries := make([]snodra.OutboxEntry, 0, len(won))
	for _, document := range won {
		entry, err := entryOf(document)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// MarkSent marks the entries sent, from the application's clock; owner and leaseUntil stay, as the
// layout keeps them.
func (o mongoOutbox) MarkSent(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ctx = o.store.committed(ctx)
	_, err := o.store.db.Collection(collectionOutbox).UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "sentAt", Value: bson.NewDateTimeFromTime(o.store.now())},
		}}})
	return storeError(err)
}

// MarkAttemptFailed counts a failed send that will be retried; the entry stays its holder's until
// the lease passes.
func (o mongoOutbox) MarkAttemptFailed(ctx context.Context, id, cause string) error {
	ctx = o.store.committed(ctx)
	_, err := o.store.db.Collection(collectionOutbox).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "attempts", Value: int32(1)}}}})
	return storeError(err)
}

// MarkFailed gives the entry up: attempts grows, failedAt is stamped, and the entry is never
// claimed again nor purged by a TTL. The cause is not stored.
func (o mongoOutbox) MarkFailed(ctx context.Context, id, cause string) error {
	ctx = o.store.committed(ctx)
	_, err := o.store.db.Collection(collectionOutbox).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: "attempts", Value: int32(1)}}},
			{Key: "$set", Value: bson.D{
				{Key: "failedAt", Value: bson.NewDateTimeFromTime(o.store.now())},
			}},
		})
	return storeError(err)
}

// Purge removes the entries sent before sentBefore; how many.
func (o mongoOutbox) Purge(ctx context.Context, sentBefore time.Time) (int64, error) {
	ctx = o.store.committed(ctx)
	result, err := o.store.db.Collection(collectionOutbox).DeleteMany(ctx,
		bson.D{{Key: "sentAt", Value: bson.D{{Key: "$lt", Value: bson.NewDateTimeFromTime(sentBefore)}}}})
	if err != nil {
		return 0, storeError(err)
	}
	return result.DeletedCount, nil
}

// Pending counts the entries neither sent nor failed, held or not.
func (o mongoOutbox) Pending(ctx context.Context) (int64, error) {
	ctx = o.store.committed(ctx)
	pending, err := o.store.db.Collection(collectionOutbox).CountDocuments(ctx, unfinished())
	if err != nil {
		return 0, storeError(err)
	}
	return pending, nil
}

// OldestPending is the CreatedAt of the oldest pending entry (held or not).
func (o mongoOutbox) OldestPending(ctx context.Context) (time.Time, bool, error) {
	ctx = o.store.committed(ctx)
	var document struct {
		CreatedAt bson.DateTime `bson:"createdAt"`
	}
	err := o.store.db.Collection(collectionOutbox).FindOne(ctx, unfinished(),
		options.FindOne().
			SetSort(bson.D{
				{Key: "createdAt", Value: int32(1)},
				{Key: "seq", Value: int32(1)},
			}).
			SetProjection(bson.D{{Key: "createdAt", Value: int32(1)}}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, storeError(err)
	}
	return document.CreatedAt.Time(), true, nil
}

// OnAppend registers nothing: the MongoDB store has no change feed the relay could wait on, so the
// relay polls, as the contract allows. The returned cancel is safe to call.
func (o mongoOutbox) OnAppend(wake func()) func() { return func() {} }
