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

// The saga ports on the shared collections. The runtime's writes join its transaction; the
// relays' and the scheduler's claims commit on their own.

type sagaAssociationDocument struct {
	Key   string `bson:"k"`
	Value string `bson:"v"`
}

type sagaDocument struct {
	ID           string                    `bson:"_id"`
	Type         string                    `bson:"type"`
	State        string                    `bson:"state"`
	Associations []sagaAssociationDocument `bson:"associations"`
	StartKey     *string                   `bson:"startKey,omitempty"`
	Ended        bool                      `bson:"ended"`
	EndedAt      *bson.DateTime            `bson:"endedAt,omitempty"`
	Version      int64                     `bson:"version"`
	CreatedAt    bson.DateTime             `bson:"createdAt"`
	UpdatedAt    bson.DateTime             `bson:"updatedAt"`
}

func sagaDocumentOf(saga snodra.Saga) sagaDocument {
	document := sagaDocument{ID: saga.ID, Type: saga.Type, State: saga.State, Ended: saga.Ended,
		Version: saga.Version, CreatedAt: controlDate(saga.CreatedAt), UpdatedAt: controlDate(saga.UpdatedAt)}
	if saga.StartKey != nil {
		document.StartKey = copyString(saga.StartKey)
	}
	if !saga.EndedAt.IsZero() {
		at := controlDate(saga.EndedAt)
		document.EndedAt = &at
	}
	document.Associations = make([]sagaAssociationDocument, 0, len(saga.Associations))
	for _, association := range saga.Associations {
		document.Associations = append(document.Associations, sagaAssociationDocument{Key: association.Key, Value: association.Value})
	}
	return document
}

func sagaOf(document sagaDocument) snodra.Saga {
	saga := snodra.Saga{ID: document.ID, Type: document.Type, State: document.State,
		StartKey: copyString(document.StartKey), Ended: document.Ended, Version: document.Version,
		CreatedAt: document.CreatedAt.Time().UTC(), UpdatedAt: document.UpdatedAt.Time().UTC()}
	if document.EndedAt != nil {
		saga.EndedAt = document.EndedAt.Time().UTC()
	}
	for _, association := range document.Associations {
		saga.Associations = append(saga.Associations, snodra.SagaAssociation{Key: association.Key, Value: association.Value})
	}
	return saga
}

// mongoSagaRepository is the snodra.SagaRepository on sagas.
type mongoSagaRepository struct {
	store *Store
}

var _ snodra.SagaRepository = mongoSagaRepository{}

// FindByID returns the instance, or nil when there is none.
func (r mongoSagaRepository) FindByID(ctx context.Context, id string) (*snodra.Saga, error) {
	scope, err := r.store.scope(ctx)
	if err != nil {
		return nil, err
	}
	var document sagaDocument
	err = r.store.db.Collection(collectionSagas).FindOne(scope, bson.D{{Key: "_id", Value: id}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	saga := sagaOf(document)
	return &saga, nil
}

// FindLive returns the live instances of typeName associated with key = value.
func (r mongoSagaRepository) FindLive(ctx context.Context, typeName, key, value string) ([]snodra.Saga, error) {
	scope, err := r.store.scope(ctx)
	if err != nil {
		return nil, err
	}
	filter := bson.D{
		{Key: "type", Value: typeName},
		{Key: "ended", Value: false},
		{Key: "associations", Value: bson.D{{Key: "$elemMatch", Value: bson.D{{Key: "k", Value: key}, {Key: "v", Value: value}}}}},
	}
	cursor, err := r.store.db.Collection(collectionSagas).Find(scope, filter,
		options.Find().SetSort(bson.D{{Key: "createdAt", Value: int32(1)}, {Key: "_id", Value: int32(1)}}))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(scope) }()
	var documents []sagaDocument
	if err := cursor.All(scope, &documents); err != nil {
		return nil, storeError(err)
	}
	sagas := make([]snodra.Saga, len(documents))
	for i, document := range documents {
		sagas[i] = sagaOf(document)
	}
	return sagas, nil
}

// Insert inserts a new instance; ErrConflict when its id exists or a live instance holds its
// StartKey.
func (r mongoSagaRepository) Insert(ctx context.Context, saga snodra.Saga) error {
	scope, err := r.store.scope(ctx)
	if err != nil {
		return err
	}
	_, err = r.store.db.Collection(collectionSagas).InsertOne(scope, sagaDocumentOf(saga))
	return storeError(err)
}

// Update replaces the stored instance when its Version is expectedVersion.
func (r mongoSagaRepository) Update(ctx context.Context, saga snodra.Saga, expectedVersion int64) (bool, error) {
	scope, err := r.store.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := r.store.db.Collection(collectionSagas).ReplaceOne(scope,
		bson.D{{Key: "_id", Value: saga.ID}, {Key: "version", Value: expectedVersion}}, sagaDocumentOf(saga))
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// CountLiveByType is how many live instances there are per saga type, outside any transaction.
func (r mongoSagaRepository) CountLiveByType(ctx context.Context) (map[string]int64, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "ended", Value: false}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$type"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
	}
	cursor, err := r.store.db.Collection(collectionSagas).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	counts := map[string]int64{}
	for cursor.Next(ctx) {
		var row struct {
			Type  string `bson:"_id"`
			Count int64  `bson:"n"`
		}
		if err := cursor.Decode(&row); err != nil {
			return nil, storeError(err)
		}
		counts[row.Type] = row.Count
	}
	if err := cursor.Err(); err != nil {
		return nil, storeError(err)
	}
	return counts, nil
}

type sagaDispatchDocument struct {
	ID             string            `bson:"_id"`
	SagaID         string            `bson:"sagaId"`
	Seq            int64             `bson:"seq"`
	Kind           string            `bson:"kind"`
	Type           string            `bson:"type"`
	Payload        string            `bson:"payload"`
	Headers        map[string]string `bson:"headers"`
	RoutingKey     *string           `bson:"routingKey,omitempty"`
	CreatedAt      bson.DateTime     `bson:"createdAt"`
	DispatchedAt   *bson.DateTime    `bson:"dispatchedAt,omitempty"`
	Attempts       int32             `bson:"attempts"`
	NextAttemptAt  *bson.DateTime    `bson:"nextAttemptAt,omitempty"`
	FailedAt       *bson.DateTime    `bson:"failedAt,omitempty"`
	FailureReason  *string           `bson:"failureReason,omitempty"`
	FailureMessage *string           `bson:"failureMessage,omitempty"`
	NoticePending  *bool             `bson:"noticePending,omitempty"`
	NoticeAttempts *int32            `bson:"noticeAttempts,omitempty"`
	NoticeNextAt   *bson.DateTime    `bson:"noticeNextAt,omitempty"`
}

func sagaDispatchDocumentOf(entry snodra.SagaDispatch) sagaDispatchDocument {
	document := sagaDispatchDocument{ID: entry.ID, SagaID: entry.SagaID, Seq: entry.Seq, Kind: string(entry.Kind),
		Type: entry.Type, Payload: entry.Payload, Headers: entry.Headers, CreatedAt: controlDate(entry.CreatedAt),
		Attempts: int32(entry.Attempts)}
	if document.Headers == nil {
		document.Headers = map[string]string{}
	}
	if entry.RoutingKey != nil {
		document.RoutingKey = copyString(entry.RoutingKey)
	}
	if !entry.DispatchedAt.IsZero() {
		at := controlDate(entry.DispatchedAt)
		document.DispatchedAt = &at
	}
	if !entry.NextAttemptAt.IsZero() {
		at := controlDate(entry.NextAttemptAt)
		document.NextAttemptAt = &at
	}
	if !entry.FailedAt.IsZero() {
		at := controlDate(entry.FailedAt)
		document.FailedAt = &at
	}
	if entry.FailureReason != "" {
		reason := string(entry.FailureReason)
		document.FailureReason = &reason
	}
	if entry.FailureMessage != "" {
		message := entry.FailureMessage
		document.FailureMessage = &message
	}
	if entry.NoticePending {
		pending := true
		attempts := int32(entry.NoticeAttempts)
		document.NoticePending = &pending
		document.NoticeAttempts = &attempts
	}
	if !entry.NoticeNextAt.IsZero() {
		at := controlDate(entry.NoticeNextAt)
		document.NoticeNextAt = &at
	}
	return document
}

func sagaDispatchOf(document sagaDispatchDocument) snodra.SagaDispatch {
	entry := snodra.SagaDispatch{ID: document.ID, SagaID: document.SagaID, Seq: document.Seq,
		Kind: snodra.DispatchKind(document.Kind), Type: document.Type, Payload: document.Payload,
		Headers: document.Headers, Attempts: int(document.Attempts), CreatedAt: document.CreatedAt.Time().UTC()}
	if entry.Headers == nil {
		entry.Headers = map[string]string{}
	}
	entry.RoutingKey = copyString(document.RoutingKey)
	if document.DispatchedAt != nil {
		entry.DispatchedAt = document.DispatchedAt.Time().UTC()
	}
	if document.NextAttemptAt != nil {
		entry.NextAttemptAt = document.NextAttemptAt.Time().UTC()
	}
	if document.FailedAt != nil {
		entry.FailedAt = document.FailedAt.Time().UTC()
	}
	if document.FailureReason != nil {
		entry.FailureReason = snodra.FailureReason(*document.FailureReason)
	}
	if document.FailureMessage != nil {
		entry.FailureMessage = *document.FailureMessage
	}
	if document.NoticePending != nil {
		entry.NoticePending = *document.NoticePending
	}
	if document.NoticeAttempts != nil {
		entry.NoticeAttempts = int(*document.NoticeAttempts)
	}
	if document.NoticeNextAt != nil {
		entry.NoticeNextAt = document.NoticeNextAt.Time().UTC()
	}
	return entry
}

// mongoSagaDispatch is the snodra.SagaDispatchStore on saga_dispatch and its leases.
type mongoSagaDispatch struct {
	store *Store
}

var _ snodra.SagaDispatchStore = mongoSagaDispatch{}

// Record records entries in the transaction carried by ctx.
func (q mongoSagaDispatch) Record(ctx context.Context, entries []snodra.SagaDispatch) error {
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if entry.Kind == snodra.DispatchEvent {
			if err := snodra.CheckOutboxPayload(entry.Type, []byte(entry.Payload)); err != nil {
				return err
			}
		}
	}
	scope, err := q.store.scope(ctx)
	if err != nil {
		return err
	}
	documents := make([]any, len(entries))
	for i, entry := range entries {
		documents[i] = sagaDispatchDocumentOf(entry)
	}
	_, err = q.store.db.Collection(collectionSagaDispatch).InsertMany(scope, documents)
	return storeError(err)
}

// DueSagas returns the ids of the sagas whose first pending entry is due, oldest first.
func (q mongoSagaDispatch) DueSagas(ctx context.Context, now time.Time, except []string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	ctx = q.store.committed(ctx)
	match := bson.D{{Key: "dispatchedAt", Value: nil}, {Key: "failedAt", Value: nil}}
	if len(except) > 0 {
		match = append(match, bson.E{Key: "sagaId", Value: bson.D{{Key: "$nin", Value: except}}})
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$sort", Value: bson.D{{Key: "sagaId", Value: int32(1)}, {Key: "seq", Value: int32(1)}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$sagaId"},
			{Key: "next", Value: bson.D{{Key: "$first", Value: "$nextAttemptAt"}}},
			{Key: "since", Value: bson.D{{Key: "$first", Value: "$createdAt"}}},
		}}},
		{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "next", Value: nil}},
			bson.D{{Key: "next", Value: bson.D{{Key: "$lte", Value: controlDate(now)}}}},
		}}}}},
		{{Key: "$sort", Value: bson.D{{Key: "since", Value: int32(1)}, {Key: "_id", Value: int32(1)}}}},
		{{Key: "$limit", Value: int64(limit)}},
	}
	cursor, err := q.store.db.Collection(collectionSagaDispatch).Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	var ids []string
	for cursor.Next(ctx) {
		var row struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&row); err != nil {
			return nil, storeError(err)
		}
		ids = append(ids, row.ID)
	}
	if err := cursor.Err(); err != nil {
		return nil, storeError(err)
	}
	return ids, nil
}

// NextPending returns the first pending entry of sagaID.
func (q mongoSagaDispatch) NextPending(ctx context.Context, sagaID string) (*snodra.SagaDispatch, error) {
	ctx = q.store.committed(ctx)
	var document sagaDispatchDocument
	err := q.store.db.Collection(collectionSagaDispatch).FindOne(ctx,
		bson.D{{Key: "sagaId", Value: sagaID}, {Key: "dispatchedAt", Value: nil}, {Key: "failedAt", Value: nil}},
		options.FindOne().SetSort(bson.D{{Key: "seq", Value: int32(1)}})).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	entry := sagaDispatchOf(document)
	return &entry, nil
}

// MarkDispatched records the entry dispatched at now, in the transaction carried by ctx.
func (q mongoSagaDispatch) MarkDispatched(ctx context.Context, id string, now time.Time) error {
	scope, err := q.store.scope(ctx)
	if err != nil {
		return err
	}
	result, err := q.store.db.Collection(collectionSagaDispatch).UpdateOne(scope,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "dispatchedAt", Value: controlDate(now)}}}})
	if err != nil {
		return storeError(err)
	}
	if result.MatchedCount == 0 {
		return snodra.ErrInvalid
	}
	return nil
}

// ScheduleRetry records a transient failure.
func (q mongoSagaDispatch) ScheduleRetry(ctx context.Context, id string, attempts int, nextAttempt time.Time) error {
	ctx = q.store.committed(ctx)
	_, err := q.store.db.Collection(collectionSagaDispatch).UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "attempts", Value: int32(attempts)},
			{Key: "nextAttemptAt", Value: controlDate(nextAttempt)},
		}}})
	return storeError(err)
}

// MarkFailed gives the entry up.
func (q mongoSagaDispatch) MarkFailed(ctx context.Context, id string, failure snodra.DispatchFailure) error {
	ctx = q.store.committed(ctx)
	set := bson.D{{Key: "failedAt", Value: controlDate(failure.FailedAt)},
		{Key: "failureReason", Value: string(failure.Reason)},
		{Key: "failureMessage", Value: failure.Message}}
	update := bson.D{{Key: "$set", Value: set}}
	if failure.Attempts != nil {
		set = append(set, bson.E{Key: "attempts", Value: int32(*failure.Attempts)})
		update[0].Value = set
	}
	if failure.NoticeAt != nil {
		set = append(set,
			bson.E{Key: "noticePending", Value: true},
			bson.E{Key: "noticeAttempts", Value: int32(0)},
			bson.E{Key: "noticeNextAt", Value: controlDate(*failure.NoticeAt)})
		update[0].Value = set
	} else {
		update = append(update, bson.E{Key: "$unset", Value: bson.D{
			{Key: "noticePending", Value: ""}, {Key: "noticeAttempts", Value: ""}, {Key: "noticeNextAt", Value: ""}}})
	}
	_, err := q.store.db.Collection(collectionSagaDispatch).UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, update)
	return storeError(err)
}

// ClaimDueNotice claims the pending failure notice due first.
func (q mongoSagaDispatch) ClaimDueNotice(ctx context.Context, now, reserveUntil time.Time) (*snodra.SagaDispatch, error) {
	ctx = q.store.committed(ctx)
	filter := bson.D{
		{Key: "noticePending", Value: true},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "noticeNextAt", Value: nil}},
			bson.D{{Key: "noticeNextAt", Value: bson.D{{Key: "$lte", Value: controlDate(now)}}}},
		}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "noticeNextAt", Value: controlDate(reserveUntil)}}}}
	var document sagaDispatchDocument
	err := q.store.db.Collection(collectionSagaDispatch).FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().
			SetSort(bson.D{{Key: "noticeNextAt", Value: int32(1)}}).
			SetReturnDocument(options.After)).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	entry := sagaDispatchOf(document)
	return &entry, nil
}

// ClearNotice records that the failure notice was delivered.
func (q mongoSagaDispatch) ClearNotice(ctx context.Context, id string) error {
	ctx = q.store.committed(ctx)
	_, err := q.store.db.Collection(collectionSagaDispatch).UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$unset", Value: bson.D{
			{Key: "noticePending", Value: ""}, {Key: "noticeAttempts", Value: ""}, {Key: "noticeNextAt", Value: ""}}}})
	return storeError(err)
}

// ScheduleNoticeRetry records that a delivery of the notice failed.
func (q mongoSagaDispatch) ScheduleNoticeRetry(ctx context.Context, id string, noticeAttempts int, next time.Time) error {
	ctx = q.store.committed(ctx)
	_, err := q.store.db.Collection(collectionSagaDispatch).UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "noticeAttempts", Value: int32(noticeAttempts)},
			{Key: "noticeNextAt", Value: controlDate(next)},
		}}})
	return storeError(err)
}

// ClaimLease takes the dispatch lease of sagaID for owner until until.
func (q mongoSagaDispatch) ClaimLease(ctx context.Context, sagaID, owner string, now, until time.Time) (bool, error) {
	ctx = q.store.committed(ctx)
	filter := bson.D{{Key: "_id", Value: sagaID}, {Key: "until", Value: bson.D{{Key: "$lt", Value: controlDate(now)}}}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: owner}, {Key: "until", Value: controlDate(until)}}}}
	result, err := q.store.db.Collection(collectionSagaDispatchLease).UpdateOne(ctx, filter, update,
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, storeError(err)
	}
	return result.MatchedCount == 1 || result.UpsertedCount == 1, nil
}

// RenewLease moves the lease owner holds to until.
func (q mongoSagaDispatch) RenewLease(ctx context.Context, sagaID, owner string, until time.Time) (bool, error) {
	ctx = q.store.committed(ctx)
	result, err := q.store.db.Collection(collectionSagaDispatchLease).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: sagaID}, {Key: "owner", Value: owner}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "until", Value: controlDate(until)}}}})
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// ReleaseLease lets go of the lease when owner holds it.
func (q mongoSagaDispatch) ReleaseLease(ctx context.Context, sagaID, owner string) error {
	ctx = q.store.committed(ctx)
	_, err := q.store.db.Collection(collectionSagaDispatchLease).DeleteOne(ctx,
		bson.D{{Key: "_id", Value: sagaID}, {Key: "owner", Value: owner}})
	return storeError(err)
}

// Pending is how many entries are pending.
func (q mongoSagaDispatch) Pending(ctx context.Context) (int64, error) {
	ctx = q.store.committed(ctx)
	count, err := q.store.db.Collection(collectionSagaDispatch).CountDocuments(ctx,
		bson.D{{Key: "dispatchedAt", Value: nil}, {Key: "failedAt", Value: nil}})
	if err != nil {
		return 0, storeError(err)
	}
	return count, nil
}

type sagaDeadlineDocument struct {
	ID            string         `bson:"_id"`
	SagaType      string         `bson:"sagaType"`
	SagaID        string         `bson:"sagaId"`
	Name          string         `bson:"name"`
	Payload       *string        `bson:"payload,omitempty"`
	PayloadType   *string        `bson:"payloadType,omitempty"`
	DueAt         bson.DateTime  `bson:"dueAt"`
	Status        string         `bson:"status"`
	ClaimedBy     *string        `bson:"claimedBy,omitempty"`
	ClaimedUntil  *bson.DateTime `bson:"claimedUntil,omitempty"`
	FinishedAt    *bson.DateTime `bson:"finishedAt,omitempty"`
	CorrelationID *string        `bson:"correlationId,omitempty"`
}

func sagaDeadlineDocumentOf(deadline snodra.SagaDeadline) sagaDeadlineDocument {
	document := sagaDeadlineDocument{ID: deadline.ID, SagaType: deadline.SagaType, SagaID: deadline.SagaID,
		Name: deadline.Name, DueAt: controlDate(deadline.DueAt), Status: string(deadline.Status)}
	document.Payload = copyString(deadline.Payload)
	document.PayloadType = copyString(deadline.PayloadType)
	if deadline.ClaimedBy != "" {
		document.ClaimedBy = copyString(&deadline.ClaimedBy)
	}
	if !deadline.ClaimedUntil.IsZero() {
		at := controlDate(deadline.ClaimedUntil)
		document.ClaimedUntil = &at
	}
	if !deadline.FinishedAt.IsZero() {
		at := controlDate(deadline.FinishedAt)
		document.FinishedAt = &at
	}
	if deadline.CorrelationID != "" {
		document.CorrelationID = copyString(&deadline.CorrelationID)
	}
	return document
}

func sagaDeadlineOf(document sagaDeadlineDocument) snodra.SagaDeadline {
	deadline := snodra.SagaDeadline{ID: document.ID, SagaType: document.SagaType, SagaID: document.SagaID,
		Name: document.Name, Payload: copyString(document.Payload), PayloadType: copyString(document.PayloadType),
		DueAt: document.DueAt.Time().UTC(), Status: snodra.DeadlineStatus(document.Status)}
	if document.ClaimedBy != nil {
		deadline.ClaimedBy = *document.ClaimedBy
	}
	if document.ClaimedUntil != nil {
		deadline.ClaimedUntil = document.ClaimedUntil.Time().UTC()
	}
	if document.FinishedAt != nil {
		deadline.FinishedAt = document.FinishedAt.Time().UTC()
	}
	if document.CorrelationID != nil {
		deadline.CorrelationID = *document.CorrelationID
	}
	return deadline
}

// mongoSagaDeadlines is the snodra.SagaDeadlineStore on deadlines.
type mongoSagaDeadlines struct {
	store *Store
}

var _ snodra.SagaDeadlineStore = mongoSagaDeadlines{}

// Insert inserts deadline.
func (d mongoSagaDeadlines) Insert(ctx context.Context, deadline snodra.SagaDeadline) error {
	scope, err := d.store.scope(ctx)
	if err != nil {
		return err
	}
	_, err = d.store.db.Collection(collectionDeadlines).InsertOne(scope, sagaDeadlineDocumentOf(deadline))
	return storeError(err)
}

// FindScheduled returns the deadline when it is still SCHEDULED.
func (d mongoSagaDeadlines) FindScheduled(ctx context.Context, id string) (*snodra.SagaDeadline, error) {
	scope, err := d.store.scope(ctx)
	if err != nil {
		return nil, err
	}
	var document sagaDeadlineDocument
	err = d.store.db.Collection(collectionDeadlines).FindOne(scope,
		bson.D{{Key: "_id", Value: id}, {Key: "status", Value: string(snodra.DeadlineScheduled)}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	deadline := sagaDeadlineOf(document)
	return &deadline, nil
}

// Cancel cancels the SCHEDULED deadline deadlineID of sagaID.
func (d mongoSagaDeadlines) Cancel(ctx context.Context, sagaID, deadlineID string, now time.Time) error {
	return d.cancelWhere(ctx, bson.D{{Key: "_id", Value: deadlineID}, {Key: "sagaId", Value: sagaID}}, now)
}

// CancelByName cancels the SCHEDULED deadlines of sagaID named name.
func (d mongoSagaDeadlines) CancelByName(ctx context.Context, sagaID, name string, now time.Time) error {
	return d.cancelWhere(ctx, bson.D{{Key: "sagaId", Value: sagaID}, {Key: "name", Value: name}}, now)
}

// CancelAll cancels every SCHEDULED deadline of sagaID.
func (d mongoSagaDeadlines) CancelAll(ctx context.Context, sagaID string, now time.Time) error {
	return d.cancelWhere(ctx, bson.D{{Key: "sagaId", Value: sagaID}}, now)
}

func (d mongoSagaDeadlines) cancelWhere(ctx context.Context, filter bson.D, now time.Time) error {
	scope, err := d.store.scope(ctx)
	if err != nil {
		return err
	}
	filter = append(filter, bson.E{Key: "status", Value: string(snodra.DeadlineScheduled)})
	_, err = d.store.db.Collection(collectionDeadlines).UpdateMany(scope, filter,
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "status", Value: string(snodra.DeadlineCancelled)},
			{Key: "finishedAt", Value: controlDate(now)},
		}}})
	return storeError(err)
}

// MarkFired sets the SCHEDULED deadline id to FIRED.
func (d mongoSagaDeadlines) MarkFired(ctx context.Context, id string, now time.Time) (bool, error) {
	scope, err := d.store.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := d.store.db.Collection(collectionDeadlines).UpdateOne(scope,
		bson.D{{Key: "_id", Value: id}, {Key: "status", Value: string(snodra.DeadlineScheduled)}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "status", Value: string(snodra.DeadlineFired)},
			{Key: "finishedAt", Value: controlDate(now)},
		}}})
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// ClaimDue claims the SCHEDULED deadline due first that nobody holds.
func (d mongoSagaDeadlines) ClaimDue(ctx context.Context, owner string, now, claimedUntil time.Time) (*snodra.SagaDeadline, error) {
	ctx = d.store.committed(ctx)
	filter := bson.D{
		{Key: "status", Value: string(snodra.DeadlineScheduled)},
		{Key: "dueAt", Value: bson.D{{Key: "$lte", Value: controlDate(now)}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "claimedUntil", Value: nil}},
			bson.D{{Key: "claimedUntil", Value: bson.D{{Key: "$lt", Value: controlDate(now)}}}},
		}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "claimedBy", Value: owner},
		{Key: "claimedUntil", Value: controlDate(claimedUntil)},
	}}}
	var document sagaDeadlineDocument
	err := d.store.db.Collection(collectionDeadlines).FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().
			SetSort(bson.D{{Key: "dueAt", Value: int32(1)}, {Key: "_id", Value: int32(1)}}).
			SetReturnDocument(options.After)).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	deadline := sagaDeadlineOf(document)
	return &deadline, nil
}

// CountOverdueByName is how many SCHEDULED deadlines are due per name.
func (d mongoSagaDeadlines) CountOverdueByName(ctx context.Context, now time.Time) (map[string]int64, error) {
	ctx = d.store.committed(ctx)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "status", Value: string(snodra.DeadlineScheduled)},
			{Key: "dueAt", Value: bson.D{{Key: "$lte", Value: controlDate(now)}}},
		}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$name"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: int32(1)}}}}}},
	}
	cursor, err := d.store.db.Collection(collectionDeadlines).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	counts := map[string]int64{}
	for cursor.Next(ctx) {
		var row struct {
			Name  string `bson:"_id"`
			Count int64  `bson:"n"`
		}
		if err := cursor.Decode(&row); err != nil {
			return nil, storeError(err)
		}
		counts[row.Name] = row.Count
	}
	if err := cursor.Err(); err != nil {
		return nil, storeError(err)
	}
	return counts, nil
}

type sagaFailureDocument struct {
	ID            string            `bson:"_id"`
	SagaType      string            `bson:"sagaType"`
	EventID       string            `bson:"eventId"`
	EventType     string            `bson:"eventType"`
	Payload       string            `bson:"payload"`
	Headers       map[string]string `bson:"headers"`
	Error         string            `bson:"error"`
	Attempts      int32             `bson:"attempts"`
	FirstFailedAt bson.DateTime     `bson:"firstFailedAt"`
	LastFailedAt  bson.DateTime     `bson:"lastFailedAt"`
}

func sagaFailureDocumentOf(failure snodra.SagaFailure) sagaFailureDocument {
	document := sagaFailureDocument{ID: failure.ID, SagaType: failure.SagaType, EventID: failure.EventID,
		EventType: failure.EventType, Payload: failure.Payload, Headers: failure.Headers, Error: failure.Error,
		Attempts: int32(failure.Attempts), FirstFailedAt: controlDate(failure.FirstFailedAt),
		LastFailedAt: controlDate(failure.LastFailedAt)}
	if document.Headers == nil {
		document.Headers = map[string]string{}
	}
	return document
}

func sagaFailureOf(document sagaFailureDocument) snodra.SagaFailure {
	failure := snodra.SagaFailure{ID: document.ID, SagaType: document.SagaType, EventID: document.EventID,
		EventType: document.EventType, Payload: document.Payload, Headers: document.Headers, Error: document.Error,
		Attempts: int(document.Attempts), FirstFailedAt: document.FirstFailedAt.Time().UTC(),
		LastFailedAt: document.LastFailedAt.Time().UTC()}
	if failure.Headers == nil {
		failure.Headers = map[string]string{}
	}
	return failure
}

// mongoSagaFailures is the snodra.SagaFailureStore on saga_failures.
type mongoSagaFailures struct {
	store *Store
}

var _ snodra.SagaFailureStore = mongoSagaFailures{}

// Find returns the failure, or nil when there is none.
func (f mongoSagaFailures) Find(ctx context.Context, id string) (*snodra.SagaFailure, error) {
	scope, err := f.store.scope(ctx)
	if err != nil {
		return nil, err
	}
	var document sagaFailureDocument
	err = f.store.db.Collection(collectionSagaFailures).FindOne(scope, bson.D{{Key: "_id", Value: id}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err)
	}
	failure := sagaFailureOf(document)
	return &failure, nil
}

// Save inserts or replaces the failure with that id.
func (f mongoSagaFailures) Save(ctx context.Context, failure snodra.SagaFailure) error {
	scope, err := f.store.scope(ctx)
	if err != nil {
		return err
	}
	_, err = f.store.db.Collection(collectionSagaFailures).ReplaceOne(scope, bson.D{{Key: "_id", Value: failure.ID}},
		sagaFailureDocumentOf(failure), options.Replace().SetUpsert(true))
	return storeError(err)
}

// Delete deletes the failure.
func (f mongoSagaFailures) Delete(ctx context.Context, id string) (bool, error) {
	scope, err := f.store.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := f.store.db.Collection(collectionSagaFailures).DeleteOne(scope, bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return false, storeError(err)
	}
	return result.DeletedCount == 1, nil
}

// FailedAgain records that a retry of failure id failed again.
func (f mongoSagaFailures) FailedAgain(ctx context.Context, id, errText string, attempts int, now time.Time) (bool, error) {
	scope, err := f.store.scope(ctx)
	if err != nil {
		return false, err
	}
	result, err := f.store.db.Collection(collectionSagaFailures).UpdateOne(scope, bson.D{{Key: "_id", Value: id}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: "attempts", Value: int32(attempts)}}},
			{Key: "$set", Value: bson.D{{Key: "error", Value: errText}, {Key: "lastFailedAt", Value: controlDate(now)}}},
		})
	if err != nil {
		return false, storeError(err)
	}
	return result.MatchedCount == 1, nil
}

// List returns the failures of sagaType (every type when empty), oldest first.
func (f mongoSagaFailures) List(ctx context.Context, sagaType string) ([]snodra.SagaFailure, error) {
	scope, err := f.store.scope(ctx)
	if err != nil {
		return nil, err
	}
	filter := bson.D{}
	if sagaType != "" {
		filter = append(filter, bson.E{Key: "sagaType", Value: sagaType})
	}
	cursor, err := f.store.db.Collection(collectionSagaFailures).Find(scope, filter,
		options.Find().SetSort(bson.D{{Key: "lastFailedAt", Value: int32(1)}, {Key: "_id", Value: int32(1)}}))
	if err != nil {
		return nil, storeError(err)
	}
	defer func() { _ = cursor.Close(scope) }()
	var documents []sagaFailureDocument
	if err := cursor.All(scope, &documents); err != nil {
		return nil, storeError(err)
	}
	failures := make([]snodra.SagaFailure, len(documents))
	for i, document := range documents {
		failures[i] = sagaFailureOf(document)
	}
	return failures, nil
}

// mongoSagaMarkers is the snodra.SagaMarkerStore in processed_events.
type mongoSagaMarkers struct {
	store *Store
}

var _ snodra.SagaMarkerStore = mongoSagaMarkers{}

// Exists reports whether the marker is recorded.
func (m mongoSagaMarkers) Exists(ctx context.Context, id string) (bool, error) {
	scope, err := m.store.scope(ctx)
	if err != nil {
		return false, err
	}
	err = m.store.db.Collection(collectionProcessed).FindOne(scope, bson.D{{Key: "_id", Value: id}},
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: int32(1)}})).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, storeError(err)
	}
	return true, nil
}

// Insert inserts the marker; ErrConflict when it exists.
func (m mongoSagaMarkers) Insert(ctx context.Context, id, typeName string, now time.Time) error {
	scope, err := m.store.scope(ctx)
	if err != nil {
		return err
	}
	_, err = m.store.db.Collection(collectionProcessed).InsertOne(scope,
		markerDocument(snodra.ProcessedMarker{ID: id, Type: typeName, ProcessedAt: now}))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return snodra.ErrConflict
		}
		return storeError(err)
	}
	return nil
}
