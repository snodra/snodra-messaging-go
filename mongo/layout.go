package mongo

import (
	"fmt"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"

	snodra "github.com/snodra/snodra-messaging-go"
)

// This file is the store's side of the shared layout: the documents it writes field for field and
// reads back, accepting the absence of every optional field (as the Rust add-on leaves one out) and
// a null for one (as it writes three outbox fields). A reader ignores a field it does not know,
// `_class` included; the order of a document's fields carries no meaning.

// The shared collections. The names are fixed: no prefix, no setting changes them.
const (
	collectionOutbox            = "outbox"
	collectionProcessed         = "processed_events"
	collectionDeadLetters       = "dead_letters"
	collectionControl           = "processor_control"
	collectionExternalCalls     = "external_calls"
	collectionSagas             = "sagas"
	collectionSagaDispatch      = "saga_dispatch"
	collectionSagaDispatchLease = "saga_dispatch_leases"
	collectionDeadlines         = "deadlines"
	collectionSagaFailures      = "saga_failures"
)

// outboxDocument is one document of the outbox collection.
//
// key, sentAt and failedAt are written as null when they have no value, the rule the Rust add-on
// follows (the Kotlin one leaves them out; every query uses {field: null}, which matches both).
// owner and leaseUntil are absent until a relay claims the entry. attempts is an int32, seq a
// long, and the payload is the event's JSON object as a BSON document.
type outboxDocument struct {
	ID         string            `bson:"_id"`
	Type       string            `bson:"type"`
	Key        *string           `bson:"key"`
	Headers    map[string]string `bson:"headers"`
	Payload    bson.D            `bson:"payload"`
	CreatedAt  bson.DateTime     `bson:"createdAt"`
	Seq        int64             `bson:"seq"`
	SentAt     *bson.DateTime    `bson:"sentAt"`
	Attempts   int32             `bson:"attempts"`
	FailedAt   *bson.DateTime    `bson:"failedAt"`
	Owner      string            `bson:"owner,omitempty"`
	LeaseUntil *bson.DateTime    `bson:"leaseUntil,omitempty"`
}

// outboxDocumentOf converts an entry to its document. The payload contract is checked before, so
// this only turns the JSON object into its BSON document; an integer outside the int32 range
// stays a long, as the layout stores it.
func outboxDocumentOf(entry snodra.OutboxEntry) (outboxDocument, error) {
	if err := snodra.CheckOutboxPayload(entry.Type, entry.Payload); err != nil {
		return outboxDocument{}, err
	}
	if entry.Attempts < math.MinInt32 || entry.Attempts > math.MaxInt32 {
		return outboxDocument{}, invalidf("stores an attempts count in an int32; this one has %d", entry.Attempts)
	}
	payload := bson.D{}
	if err := bson.UnmarshalExtJSON(entry.Payload, false, &payload); err != nil {
		return outboxDocument{}, invalidf("cannot read the payload of event %q as a BSON document: %v", entry.Type, err)
	}
	if payload == nil {
		payload = bson.D{}
	}
	headers := copyHeaders(entry.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	return outboxDocument{
		ID:        entry.ID,
		Type:      entry.Type,
		Key:       copyString(entry.Key),
		Headers:   headers,
		Payload:   payload,
		CreatedAt: bson.NewDateTimeFromTime(entry.CreatedAt),
		Seq:       entry.Seq,
		Attempts:  int32(entry.Attempts),
	}, nil
}

// entryOf reads the outbox entry a document holds: a missing or null key, and a missing or null
// sentAt or failedAt, are none; a missing attempts count is zero. The payload comes back as compact
// relaxed Extended JSON, a value equal to the one appended (the text need not be).
func entryOf(document outboxDocument) (snodra.OutboxEntry, error) {
	payload := document.Payload
	if payload == nil {
		payload = bson.D{}
	}
	encoded, err := bson.MarshalExtJSON(payload, false, false)
	if err != nil {
		return snodra.OutboxEntry{}, fmt.Errorf("the MongoDB message store cannot read the payload of outbox entry %q: %w", document.ID, err)
	}
	headers := copyHeaders(document.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	return snodra.OutboxEntry{
		ID:        document.ID,
		Type:      document.Type,
		Key:       copyString(document.Key),
		Headers:   headers,
		Payload:   encoded,
		CreatedAt: document.CreatedAt.Time().UTC(),
		Seq:       document.Seq,
		Attempts:  int(document.Attempts),
	}, nil
}

// copyString copies a string pointer, so a caller cannot change the store's value behind its back.
func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// copyHeaders copies a header map.
func copyHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return nil
	}
	copied := make(map[string]string, len(headers))
	for name, value := range headers {
		copied[name] = value
	}
	return copied
}

// markerDocument is one document of the processed_events collection. The processor, topic,
// partition and offset fields appear together on a stage-1 marker of a delivered record; every
// other marker has only the first three fields.
func markerDocument(marker snodra.ProcessedMarker) bson.D {
	document := bson.D{
		{Key: "_id", Value: marker.ID},
		{Key: "type", Value: marker.Type},
		{Key: "processedAt", Value: bson.NewDateTimeFromTime(marker.ProcessedAt)},
	}
	if marker.Processor != "" {
		document = append(document, bson.E{Key: "processor", Value: marker.Processor})
	}
	if marker.Source != "" {
		document = append(document, bson.E{Key: "topic", Value: marker.Source})
	}
	if marker.Segment != "" {
		document = append(document, bson.E{Key: "partition", Value: segmentValue(marker.Segment)})
	}
	if marker.Position != "" {
		document = append(document, bson.E{Key: "offset", Value: positionValue(marker.Position)})
	}
	return document
}

// segmentValue is a segment as the layout stores it: an int32 when it is a canonical decimal
// int32 ("3", not "03", "+3" or "2147483648"), else the string itself.
func segmentValue(segment string) any {
	number, err := strconv.ParseInt(segment, 10, 32)
	if err != nil || strconv.FormatInt(number, 10) != segment {
		return segment
	}
	return int32(number)
}

// positionValue is a position as the layout stores it: an int64 when it is a canonical decimal
// int64, else the string itself.
func positionValue(position string) any {
	number, err := strconv.ParseInt(position, 10, 64)
	if err == nil && strconv.FormatInt(number, 10) == position {
		return number
	}
	return position
}

// positionText is a stored partition or offset as its decimal text; a value that is not a number
// or a string is none.
func positionText(value any) (string, bool) {
	switch value := value.(type) {
	case int32:
		return strconv.FormatInt(int64(value), 10), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case string:
		return value, true
	default:
		return "", false
	}
}

// unfinished is the filter of an outbox entry neither sent nor failed: a missing field is null.
func unfinished() bson.D {
	return bson.D{
		{Key: "sentAt", Value: nil},
		{Key: "failedAt", Value: nil},
	}
}

// claimableBy is the filter of an entry nobody holds, owner holds, or whose holder's lease has
// passed at at.
func claimableBy(owner string, at bson.DateTime) bson.D {
	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "owner", Value: nil}},
		bson.D{{Key: "owner", Value: owner}},
		bson.D{{Key: "leaseUntil", Value: bson.D{{Key: "$lte", Value: at}}}},
	}}}
}

// claimFilter is the filter a claim takes one entry with: still unfinished and claimable by owner
// at at, and id.
func claimFilter(id, owner string, at bson.DateTime) bson.D {
	filter := bson.D{{Key: "_id", Value: id}}
	filter = append(filter, unfinished()...)
	filter = append(filter, claimableBy(owner, at)...)
	return filter
}

// duplicateKey reports whether an error is the duplicate key of Record. It is separate from
// storeError because a duplicated marker is a normal outcome (false), not a conflict.
func duplicateKey(err error) bool {
	return isDuplicateKey(err)
}
