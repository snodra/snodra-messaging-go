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

// mongoExternalCalls is the snodra.ExternalCallStore on external_calls. Every write is a
// single-document write of its own, apart from the caller's transaction.
type mongoExternalCalls struct {
	store *Store
}

var _ snodra.ExternalCallStore = mongoExternalCalls{}

// externalCallDocument is one document of external_calls.
type externalCallDocument struct {
	ID         string         `bson:"_id"`
	Status     string         `bson:"status"`
	Reply      *string        `bson:"reply,omitempty"`
	Error      *string        `bson:"error,omitempty"`
	Attempts   int32          `bson:"attempts"`
	UpdatedAt  bson.DateTime  `bson:"updatedAt"`
	LeaseUntil *bson.DateTime `bson:"leaseUntil,omitempty"`
	Owner      *string        `bson:"owner,omitempty"`
}

func externalCallDocumentOf(call snodra.ExternalCall) externalCallDocument {
	document := externalCallDocument{ID: call.Key, Status: string(call.Status), Attempts: int32(call.Attempts),
		UpdatedAt: controlDate(call.UpdatedAt)}
	if call.Reply != "" {
		document.Reply = copyString(&call.Reply)
	}
	if call.Error != "" {
		document.Error = copyString(&call.Error)
	}
	if call.Owner != "" {
		document.Owner = copyString(&call.Owner)
	}
	if !call.LeaseUntil.IsZero() {
		until := controlDate(call.LeaseUntil)
		document.LeaseUntil = &until
	}
	return document
}

func externalCallOf(document externalCallDocument) snodra.ExternalCall {
	call := snodra.ExternalCall{Key: document.ID, Status: snodra.ExternalCallStatus(document.Status),
		Attempts: int(document.Attempts), UpdatedAt: document.UpdatedAt.Time().UTC()}
	if document.Reply != nil {
		call.Reply = *document.Reply
	}
	if document.Error != nil {
		call.Error = *document.Error
	}
	if document.Owner != nil {
		call.Owner = *document.Owner
	}
	if document.LeaseUntil != nil {
		call.LeaseUntil = document.LeaseUntil.Time().UTC()
	}
	return call
}

// Insert inserts call; false when a record of its key exists.
func (c mongoExternalCalls) Insert(ctx context.Context, call snodra.ExternalCall) (bool, error) {
	ctx = c.store.committed(ctx)
	_, err := c.store.db.Collection(collectionExternalCalls).InsertOne(ctx, externalCallDocumentOf(call))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, storeError(err)
	}
	return true, nil
}

// Find returns the record of key, or nil when there is none.
func (c mongoExternalCalls) Find(ctx context.Context, key string) (*snodra.ExternalCall, error) {
	ctx = c.store.committed(ctx)
	var document externalCallDocument
	err := c.store.db.Collection(collectionExternalCalls).FindOne(ctx, bson.D{{Key: "_id", Value: key}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	call := externalCallOf(document)
	return &call, nil
}

// TakeOver takes the lapsed call seen over for owner.
func (c mongoExternalCalls) TakeOver(ctx context.Context, seen snodra.ExternalCall, owner string, leaseUntil, now time.Time) (*snodra.ExternalCall, error) {
	ctx = c.store.committed(ctx)
	filter := bson.D{
		{Key: "_id", Value: seen.Key},
		{Key: "status", Value: string(snodra.ExternalCallInProgress)},
		{Key: "owner", Value: seen.Owner},
		{Key: "leaseUntil", Value: optionalDate(seen.LeaseUntil)},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "owner", Value: owner},
			{Key: "leaseUntil", Value: controlDate(leaseUntil)},
			{Key: "updatedAt", Value: controlDate(now)},
		}},
		{Key: "$inc", Value: bson.D{{Key: "attempts", Value: int32(1)}}},
	}
	var document externalCallDocument
	err := c.store.db.Collection(collectionExternalCalls).FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	call := externalCallOf(document)
	return &call, nil
}

// Expire gives the lapsed call seen up as UNKNOWN.
func (c mongoExternalCalls) Expire(ctx context.Context, seen snodra.ExternalCall, now time.Time) (bool, error) {
	ctx = c.store.committed(ctx)
	filter := bson.D{
		{Key: "_id", Value: seen.Key},
		{Key: "status", Value: string(snodra.ExternalCallInProgress)},
		{Key: "owner", Value: seen.Owner},
		{Key: "leaseUntil", Value: optionalDate(seen.LeaseUntil)},
	}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: string(snodra.ExternalCallUnknown)},
		{Key: "updatedAt", Value: controlDate(now)},
	}}, {Key: "$unset", Value: bson.D{{Key: "owner", Value: ""}, {Key: "leaseUntil", Value: ""}}}}
	return c.matched(ctx, filter, update)
}

// Succeed settles the record owner owns as SUCCEEDED with reply.
func (c mongoExternalCalls) Succeed(ctx context.Context, key, owner, reply string, now time.Time) (bool, error) {
	return c.settle(ctx, key, owner, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: string(snodra.ExternalCallSucceeded)},
			{Key: "reply", Value: reply},
			{Key: "updatedAt", Value: controlDate(now)},
		}},
		{Key: "$unset", Value: bson.D{{Key: "error", Value: ""}, {Key: "owner", Value: ""}, {Key: "leaseUntil", Value: ""}}},
	})
}

// Fail settles the record owner owns as FAILED with errText.
func (c mongoExternalCalls) Fail(ctx context.Context, key, owner, errText string, now time.Time) (bool, error) {
	return c.settle(ctx, key, owner, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: string(snodra.ExternalCallFailed)},
			{Key: "error", Value: errText},
			{Key: "updatedAt", Value: controlDate(now)},
		}},
		{Key: "$unset", Value: bson.D{{Key: "reply", Value: ""}, {Key: "owner", Value: ""}, {Key: "leaseUntil", Value: ""}}},
	})
}

// MarkUnknown settles the record owner owns as UNKNOWN.
func (c mongoExternalCalls) MarkUnknown(ctx context.Context, key, owner string, now time.Time) (bool, error) {
	return c.settle(ctx, key, owner, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: string(snodra.ExternalCallUnknown)},
			{Key: "updatedAt", Value: controlDate(now)},
		}},
		{Key: "$unset", Value: bson.D{{Key: "reply", Value: ""}, {Key: "error", Value: ""}, {Key: "owner", Value: ""}, {Key: "leaseUntil", Value: ""}}},
	})
}

// Reset deletes the record owner owns.
func (c mongoExternalCalls) Reset(ctx context.Context, key, owner string) (bool, error) {
	ctx = c.store.committed(ctx)
	result, err := c.store.db.Collection(collectionExternalCalls).DeleteOne(ctx, bson.D{
		{Key: "_id", Value: key},
		{Key: "status", Value: string(snodra.ExternalCallInProgress)},
		{Key: "owner", Value: owner},
	})
	if err != nil {
		return false, storeError(err)
	}
	return result.DeletedCount == 1, nil
}

// settle is Succeed, Fail and MarkUnknown: the owner's record only.
func (c mongoExternalCalls) settle(ctx context.Context, key, owner string, update bson.D) (bool, error) {
	ctx = c.store.committed(ctx)
	filter := bson.D{
		{Key: "_id", Value: key},
		{Key: "status", Value: string(snodra.ExternalCallInProgress)},
		{Key: "owner", Value: owner},
	}
	return c.matched(ctx, filter, update)
}

// matched applies update and reports whether it changed the record.
func (c mongoExternalCalls) matched(ctx context.Context, filter, update bson.D) (bool, error) {
	result, err := c.store.db.Collection(collectionExternalCalls).UpdateOne(ctx, filter, update)
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// optionalDate is a date or null, for a field read as absent.
func optionalDate(at time.Time) any {
	if at.IsZero() {
		return nil
	}
	return controlDate(at)
}
