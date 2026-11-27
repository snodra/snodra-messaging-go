package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// flakyDeadLetters is a dead-letter store whose Park fails while down is set.
type flakyDeadLetters struct {
	DeadLetterStore
	down atomic.Bool
}

func (f *flakyDeadLetters) Park(ctx context.Context, letter DeadLetter) (ParkResult, error) {
	if f.down.Load() {
		return 0, errors.New("the store is down")
	}
	return f.DeadLetterStore.Park(ctx, letter)
}

type flakyDeadLetterStore struct {
	MessageStore
	dead *flakyDeadLetters
}

func (s *flakyDeadLetterStore) DeadLetters() DeadLetterStore { return s.dead }

func newFlakyDeadLetterStore() (*flakyDeadLetterStore, *flakyDeadLetters) {
	inner := NewMemoryMessageStore()
	dead := &flakyDeadLetters{DeadLetterStore: inner.DeadLetters()}
	return &flakyDeadLetterStore{MessageStore: inner, dead: dead}, dead
}

// lettersOf is the processor's letters by position.
func lettersOf(t testing.TB, store MessageStore, processor string) []DeadLetter {
	t.Helper()
	letters, err := store.DeadLetters().List(context.Background(), processor, 0)
	if err != nil {
		t.Fatal(err)
	}
	return letters
}

// deadLetterListControl is a dead_letter_list request.
func deadLetterListControl(id, processor string, limit uint32) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_DeadLetterList{DeadLetterList: &hubpb.DeadLetterList{Limit: limit}},
	})
}

// deadLetterRetryControl is a dead_letter_retry request.
func deadLetterRetryControl(id, processor, letter string) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_DeadLetterRetry{DeadLetterRetry: &hubpb.DeadLetterRetry{LetterId: letter}},
	})
}

// deadLetterRetryAllControl is a dead_letter_retry_all request.
func deadLetterRetryAllControl(id, processor string) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_DeadLetterRetryAll{DeadLetterRetryAll: &hubpb.DeadLetterRetryAll{}},
	})
}

// deadLetterDiscardControl is a dead_letter_discard request.
func deadLetterDiscardControl(id, processor, letter string) *hubpb.ServerMessage {
	return controlRequest(&hubpb.ControlRequest{
		ControlId: id, Processor: processor,
		Action: &hubpb.ControlRequest_DeadLetterDiscard{DeadLetterDiscard: &hubpb.DeadLetterDiscard{LetterId: letter}},
	})
}

// L3-DEAD-LETTERS (collection): a record that exhausts its attempts is parked at
// "<processor>:<source>:<segment>:<position>" with its record, its error and its attempts, and
// the next event goes on.
func TestARecordThatExhaustsItsAttemptsIsParked(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	settings := consumerSettings()
	settings.consumerTries = 3
	var good atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(settings), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		if event.OrderID == "bad-1" {
			return errText("this event never works")
		}
		good.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	badKey, goodKey := "k1", "k2"
	publishTestEvent(t, events, "bad-1", "orders.OrderPlaced", &badKey, nil)
	publishTestEvent(t, events, "good-1", "orders.OrderPlaced", &goodKey, nil)
	waitUntil(t, "the good event", func() bool { return good.Load() == 1 })
	letters := lettersOf(t, store, DefaultProcessor)
	if len(letters) != 1 {
		t.Fatalf("letters: %v", letters)
	}
	letter := letters[0]
	is(t, letter.ID, LetterID(DefaultProcessor, DefaultEventsSource, "0", "0"))
	is(t, letter.Processor, DefaultProcessor)
	is(t, letter.Source, DefaultEventsSource)
	is(t, letter.Segment, "0")
	is(t, letter.Position, "0")
	if letter.Key == nil || *letter.Key != "k1" {
		t.Fatalf("key: %v", letter.Key)
	}
	is(t, letter.EventID, "bad-1")
	is(t, letter.EventType, "orders.OrderPlaced")
	is(t, letter.Headers[HeaderID], "bad-1")
	is(t, string(letter.Payload), `{"orderId":"bad-1"}`)
	is(t, letter.Reason, DeadLetterHandlerFailed)
	is(t, letter.Error.Type, "errText")
	is(t, letter.Error.Message, "this event never works")
	is(t, letter.Attempts, 3)
	if letter.ParkedAt.IsZero() || letter.LastAttemptAt.IsZero() || letter.ExpireAt.IsZero() {
		t.Fatalf("times: %v, %v, %v", letter.ParkedAt, letter.LastAttemptAt, letter.ExpireAt)
	}
	if letter.ExpireAt.Sub(letter.ParkedAt) != defaultEventSettings().deadLetterRetention {
		t.Fatalf("retention: %v", letter.ExpireAt.Sub(letter.ParkedAt))
	}
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "bad-1")); err != nil || exists {
		t.Fatalf("a parked record has a marker: %v, %v", exists, err)
	}
}

// L3-DEAD-LETTERS (collection): a failing key parks its later records SEQUENCED without running
// stage 1, while other keys flow; order per key holds.
func TestAFailingKeyParksItsLaterRecordsSequenced(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	settings := consumerSettings()
	settings.consumerTries = 1
	var calls atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(settings), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		calls.Add(1)
		if strings.HasPrefix(event.OrderID, "p-") {
			return errText("poison")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	key1, key2 := "k1", "k2"
	for _, event := range []struct {
		id  string
		key *string
	}{{"p-1", &key1}, {"p-2", &key1}, {"o-1", &key2}, {"p-3", &key1}} {
		publishTestEvent(t, events, event.id, "orders.OrderPlaced", event.key, nil)
	}
	waitUntil(t, "the three letters", func() bool { return len(lettersOf(t, store, DefaultProcessor)) == 3 })
	waitUntil(t, "the other key's record", func() bool { return calls.Load() >= 2 })
	letters := lettersOf(t, store, DefaultProcessor)
	if len(letters) != 3 {
		t.Fatalf("letters: %v", letters)
	}
	// The store lists by ParkedAt; order the three by position for the assertions.
	byPosition := map[string]DeadLetter{}
	for _, letter := range letters {
		byPosition[letter.Position] = letter
	}
	head := byPosition["0"]
	is(t, head.Reason, DeadLetterHandlerFailed)
	is(t, head.Attempts, 1)
	for _, position := range []string{"1", "3"} {
		letter := byPosition[position]
		is(t, letter.Reason, DeadLetterSequenced)
		is(t, letter.Attempts, 0)
		is(t, letter.BlockedBy, head.ID)
		is(t, letter.Error.Type, "Sequenced")
		is(t, letter.Error.Message, "an earlier letter for this key is parked: "+head.ID)
	}
	is(t, calls.Load(), int64(2)) // p-1 and o-1: stage 1 never ran for p-2 and p-3
	if exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "p-2")); err != nil || exists {
		t.Fatalf("a sequenced record has a marker: %v, %v", exists, err)
	}
}

// L3-DEAD-LETTERS (collection): INVALID records (no type, a value that is not a JSON object) are
// parked without blocking their key; a type nobody handles is acknowledged, not parked.
func TestInvalidRecordsAreParkedWithoutBlockingTheirKey(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	var handled atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	key := "k1"
	for _, record := range []OutgoingEvent{
		{Source: DefaultEventsSource, ID: "i-1", Key: &key, Headers: map[string]string{HeaderCorrelationID: "c-1"}, Payload: []byte(`{}`)},
		{Source: DefaultEventsSource, ID: "i-2", Key: &key, Headers: map[string]string{HeaderType: "orders.OrderPlaced", HeaderID: "i-2"}, Payload: []byte(`[1,2]`)},
		{Source: DefaultEventsSource, ID: "i-3", Key: &key, Headers: map[string]string{HeaderType: "orders.OrderPlaced", HeaderID: "i-3"}, Payload: []byte(`{"orderId":"i-3"}`)},
		{Source: DefaultEventsSource, ID: "i-4", Key: &key, Headers: map[string]string{HeaderType: "orders.Unknown", HeaderID: "i-4"}, Payload: []byte(`{"orderId":"i-4"}`)},
	} {
		if err := events.Publish(context.Background(), record); err != nil {
			t.Fatalf("publish %s: %v", record.ID, err)
		}
	}
	waitUntil(t, "the good record and the two invalid letters", func() bool {
		return handled.Load() == 1 && len(lettersOf(t, store, DefaultProcessor)) == 2
	})
	letters := lettersOf(t, store, DefaultProcessor)
	byPosition := map[string]DeadLetter{}
	for _, letter := range letters {
		byPosition[letter.Position] = letter
	}
	noType := byPosition["0"]
	is(t, noType.Reason, DeadLetterInvalid)
	is(t, noType.Error.Type, "InvalidMessage")
	is(t, noType.Error.Message, "record without type header")
	is(t, noType.Attempts, 0)
	noObject := byPosition["1"]
	is(t, noObject.Reason, DeadLetterInvalid)
	is(t, noObject.Error.Message, "payload is not a JSON object")
	is(t, noObject.Attempts, 0)
	is(t, handled.Load(), int64(1))
}

// L3-DEAD-LETTERS (collection): list, retry and discard through the control requests; a retry
// runs stage 1 and deletes the letter with it, an INVALID letter is never retried.
func TestDeadLettersListRetryAndDiscardThroughControl(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	settings := consumerSettings()
	settings.consumerTries = 1
	var fail atomic.Bool
	fail.Store(true)
	var handled atomic.Int64
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			handled.Add(1)
			if strings.HasPrefix(event.OrderID, "bad-2") {
				return errText("still broken")
			}
			if fail.Load() && strings.HasPrefix(event.OrderID, "bad") {
				return errText("still broken")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings))

	key := "k1"
	publishTestEvent(t, events, "bad-1", "orders.OrderPlaced", &key, nil)
	waitUntil(t, "the letter", func() bool { return len(lettersOf(t, store, DefaultProcessor)) == 1 })
	id := lettersOf(t, store, DefaultProcessor)[0].ID

	s.Send(t, deadLetterListControl("list-1", DefaultProcessor, 10))
	page := s.RecvControlReply(t).GetDeadLetters()
	if page == nil || len(page.GetLetters()) != 1 || page.GetTotal() != 1 {
		t.Fatalf("list: %v", page)
	}
	summary := page.GetLetters()[0]
	is(t, summary.GetId(), id)
	is(t, summary.GetProcessor(), DefaultProcessor)
	is(t, summary.GetSource(), DefaultEventsSource)
	is(t, summary.GetSegment(), "0")
	is(t, summary.GetPosition(), "0")
	is(t, summary.GetKey(), "k1")
	is(t, summary.GetEventId(), "bad-1")
	is(t, summary.GetEventType(), "orders.OrderPlaced")
	is(t, summary.GetReason(), "HANDLER_FAILED")
	is(t, summary.GetErrorType(), "errText")
	is(t, summary.GetErrorMessage(), "still broken")
	is(t, summary.GetBlockedBy(), "")
	is(t, summary.GetAttempts(), uint32(1))
	if summary.GetParkedAtMs() == 0 || summary.GetLastAttemptAtMs() == 0 {
		t.Fatalf("times: %d, %d", summary.GetParkedAtMs(), summary.GetLastAttemptAtMs())
	}

	// Retry: stage 1 runs once more and the letter goes with its transaction.
	fail.Store(false)
	s.Send(t, deadLetterRetryControl("retry-1", DefaultProcessor, id))
	outcome := s.RecvControlReply(t).GetRetry()
	if outcome == nil || outcome.GetResult() != retryRetried {
		t.Fatalf("retry: %v", outcome)
	}
	if got := len(lettersOf(t, store, DefaultProcessor)); got != 0 {
		t.Fatalf("letters after the retry: %d", got)
	}
	waitUntil(t, "the retried marker", func() bool {
		exists, err := store.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "bad-1"))
		return err == nil && exists
	})
	is(t, handled.Load(), int64(2))
	s.Send(t, deadLetterRetryControl("retry-2", DefaultProcessor, id))
	outcome = s.RecvControlReply(t).GetRetry()
	if outcome == nil || outcome.GetResult() != retryNotFound {
		t.Fatalf("retry of a gone letter: %v", outcome)
	}

	// Exhausted again: discard, then a missing letter is NOT_FOUND.
	publishTestEvent(t, events, "bad-2", "orders.OrderPlaced", &key, nil)
	waitUntil(t, "the second letter", func() bool { return len(lettersOf(t, store, DefaultProcessor)) == 1 })
	second := lettersOf(t, store, DefaultProcessor)[0].ID
	s.Send(t, deadLetterDiscardControl("discard-1", DefaultProcessor, second))
	if answer := s.RecvControlReply(t).GetOk(); answer == nil {
		t.Fatalf("discard: %v", answer)
	}
	if got := len(lettersOf(t, store, DefaultProcessor)); got != 0 {
		t.Fatalf("letters after the discard: %d", got)
	}
	s.Send(t, deadLetterDiscardControl("discard-2", DefaultProcessor, second))
	if answer := s.RecvControlReply(t).GetRefused(); answer == nil ||
		answer.GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_FOUND {
		t.Fatalf("discard of a gone letter: %v", answer)
	}

	// A list limit outside 1..100 is INVALID.
	s.Send(t, deadLetterListControl("list-0", DefaultProcessor, 0))
	if answer := s.RecvControlReply(t).GetRefused(); answer == nil ||
		answer.GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID {
		t.Fatalf("limit 0: %v", answer)
	}
	s.Send(t, deadLetterListControl("list-101", DefaultProcessor, 101))
	if answer := s.RecvControlReply(t).GetRefused(); answer == nil ||
		answer.GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID {
		t.Fatalf("limit 101: %v", answer)
	}
}

// L3-DEAD-LETTERS (collection): an INVALID letter is not retriable, and a letter of another
// processor is not found here.
func TestInvalidAndForeignLettersAreNotRetried(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	invalid := letterFor(DefaultProcessor, DefaultEventsSource, "0", "0", "", DeadLetterInvalid)
	if _, err := store.DeadLetters().Park(context.Background(), invalid); err != nil {
		t.Fatal(err)
	}
	foreign := letterFor("mail", DefaultEventsSource, "0", "1", "", DeadLetterHandlerFailed)
	if _, err := store.DeadLetters().Park(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	s.Send(t, deadLetterRetryControl("retry-1", DefaultProcessor, invalid.ID))
	if outcome := s.RecvControlReply(t).GetRetry(); outcome == nil || outcome.GetResult() != retryNotRetriable {
		t.Fatalf("retry of INVALID: %v", outcome)
	}
	s.Send(t, deadLetterRetryControl("retry-2", DefaultProcessor, foreign.ID))
	if outcome := s.RecvControlReply(t).GetRetry(); outcome == nil || outcome.GetResult() != retryNotFound {
		t.Fatalf("retry of another processor's letter: %v", outcome)
	}
	s.Send(t, deadLetterDiscardControl("discard-1", DefaultProcessor, foreign.ID))
	if answer := s.RecvControlReply(t).GetRefused(); answer == nil ||
		answer.GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_FOUND {
		t.Fatalf("discard of another processor's letter: %v", answer)
	}
}

// L3-DEAD-LETTERS (collection): a retry deletes its letter in the record's transaction, so a
// failed commit leaves the letter and no marker.
func TestARetryDeletesItsLetterInTheRecordsTransaction(t *testing.T) {
	hub := fakehub.Start(t)
	inner := NewMemoryMessageStore()
	store := &flakyCommitStore{MessageStore: inner}
	events := NewMemoryEventStore()
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	letter := letterFor(DefaultProcessor, DefaultEventsSource, "0", "0", "", DeadLetterHandlerFailed)
	letter.EventType = "orders.OrderPlaced"
	letter.Headers[HeaderType] = "orders.OrderPlaced"
	if _, err := inner.DeadLetters().Park(context.Background(), letter); err != nil {
		t.Fatal(err)
	}
	store.failures.Store(1)

	s.Send(t, deadLetterRetryControl("retry-1", DefaultProcessor, letter.ID))
	outcome := s.RecvControlReply(t).GetRetry()
	if outcome == nil || outcome.GetResult() != retryFailed {
		t.Fatalf("retry with a failed commit: %v", outcome)
	}
	kept, err := inner.DeadLetters().Get(context.Background(), letter.ID)
	if err != nil || kept == nil {
		t.Fatalf("the letter did not survive a failed commit: %v, %v", kept, err)
	}
	is(t, kept.Attempts, letter.Attempts+1)
	if exists, err := inner.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-0")); err != nil || exists {
		t.Fatalf("a marker from the failed transaction: %v, %v", exists, err)
	}

	s.Send(t, deadLetterRetryControl("retry-2", DefaultProcessor, letter.ID))
	if outcome := s.RecvControlReply(t).GetRetry(); outcome == nil || outcome.GetResult() != retryRetried {
		t.Fatalf("the second retry: %v", outcome)
	}
	if got, err := inner.DeadLetters().Get(context.Background(), letter.ID); err != nil || got != nil {
		t.Fatalf("the letter after the retry: %v, %v", got, err)
	}
	if exists, err := inner.Processed().Exists(context.Background(), ProcessorMarker(DefaultProcessor, "evt-0")); err != nil || !exists {
		t.Fatalf("the retried marker: %v, %v", exists, err)
	}
}

// L3-DEAD-LETTERS (collection): retry-all goes per key in position order, stopping a key at its
// first failure; INVALID letters are skipped.
func TestRetryAllStopsAKeyAtItsFirstFailure(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	settings := consumerSettings()
	settings.consumerTries = 1
	var mu sync.Mutex
	failing := map[string]bool{"a-1": true, "b-1": true}
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			mu.Lock()
			defer mu.Unlock()
			if failing[event.OrderID] {
				return errText("broken")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings))

	keyA, keyB := "a", "b"
	publishTestEvent(t, events, "a-1", "orders.OrderPlaced", &keyA, nil)
	publishTestEvent(t, events, "a-2", "orders.OrderPlaced", &keyA, nil)
	publishTestEvent(t, events, "b-1", "orders.OrderPlaced", &keyB, nil)
	if err := events.Publish(context.Background(), OutgoingEvent{
		Source: DefaultEventsSource, ID: "invalid-1", Key: &keyB,
		Headers: map[string]string{HeaderCorrelationID: "c"}, Payload: []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the four letters", func() bool { return len(lettersOf(t, store, DefaultProcessor)) == 4 })
	mu.Lock()
	failing["b-1"] = false
	mu.Unlock()

	s.Send(t, deadLetterRetryAllControl("retry-all-1", DefaultProcessor))
	answer := s.RecvControlReply(t).GetRetryAll()
	if answer == nil {
		t.Fatal("no retry-all outcome")
	}
	is(t, answer.GetRetried(), uint64(1))
	is(t, answer.GetFailed(), uint64(1))
	is(t, answer.GetBlocked(), uint64(1))
	is(t, answer.GetSkipped(), uint64(1))
	remaining := lettersOf(t, store, DefaultProcessor)
	if len(remaining) != 3 {
		t.Fatalf("remaining letters: %v", remaining)
	}
	reasons := map[DeadLetterReason]int{}
	for _, letter := range remaining {
		reasons[letter.Reason]++
	}
	is(t, reasons[DeadLetterHandlerFailed], 1)
	is(t, reasons[DeadLetterSequenced], 1)
	if reasons[DeadLetterInvalid] != 1 {
		t.Fatalf("the INVALID letter was retried or lost: %v", reasons)
	}
}

// L3-DEAD-LETTERS (collection): a record whose letter exists already (parked before a crash) is
// acknowledged on redelivery without running its handler again.
func TestAParkedRecordRedeliveredAfterACrashIsAcknowledged(t *testing.T) {
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	key1, key2 := "k1", "k2"
	pre := letterFor(DefaultProcessor, DefaultEventsSource, "0", "0", key1, DeadLetterHandlerFailed)
	pre.EventID = "r-1"
	pre.Headers = map[string]string{HeaderID: "r-1", HeaderType: "orders.OrderPlaced"}
	pre.Error = NewDeadLetterError("first", "parked before the crash")
	if _, err := store.DeadLetters().Park(context.Background(), pre); err != nil {
		t.Fatal(err)
	}
	var seen []string
	var mu sync.Mutex
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(consumerSettings()), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		mu.Lock()
		seen = append(seen, event.OrderID)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	publishTestEvent(t, events, "r-1", "orders.OrderPlaced", &key1, nil)
	publishTestEvent(t, events, "r-2", "orders.OrderPlaced", &key2, nil)
	waitUntil(t, "the other key's record", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range seen {
			if id == "r-2" {
				return true
			}
		}
		return false
	})
	mu.Lock()
	is(t, fmt.Sprint(seen), "[r-2]")
	mu.Unlock()
	letters := lettersOf(t, store, DefaultProcessor)
	if len(letters) != 1 {
		t.Fatalf("letters: %v", letters)
	}
	is(t, letters[0].Error.Message, "parked before the crash")
}

// L3-DEAD-LETTERS (collection): the record is not acknowledged while its letter cannot be
// written, its handlers do not run again on the redelivery, and the letter appears once the
// store is back.
func TestAParkingFailureLeavesTheRecordUnacknowledged(t *testing.T) {
	store, dead := newFlakyDeadLetterStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	settings := consumerSettings()
	settings.consumerTries = 2
	var calls atomic.Int64
	app := eventsApp(t, WithStore(store), WithEventStore(events),
		withEventSettings(settings), withTimings(controlTimings()), OutboxEnabled(false))
	if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
		calls.Add(1)
		return errText("never works")
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	dead.down.Store(true)
	publishTestEvent(t, events, "d-1", "orders.OrderPlaced", nil, nil)
	waitUntil(t, "the exhausted attempts", func() bool { return calls.Load() == 2 })
	neverWithin(t, 50*ms, "an acknowledgement without a letter", func() bool {
		return processorStatusOf(t, app, DefaultProcessor).GetSegments()[0].GetCommitted() != ""
	})
	is(t, calls.Load(), int64(2))
	dead.down.Store(false)
	waitUntil(t, "the parked letter and the acknowledgement", func() bool {
		letters := lettersOf(t, store, DefaultProcessor)
		return len(letters) == 1 &&
			processorStatusOf(t, app, DefaultProcessor).GetSegments()[0].GetCommitted() == "1"
	})
	is(t, calls.Load(), int64(2))
}

// L3-DEAD-LETTERS (collection): a sequenced letter is Blocked by its key's head, and retried once
// the head was discarded.
func TestASequencedLetterWaitsBehindItsHead(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events, err := NewMemoryEventStoreWithSegments(1)
	if err != nil {
		t.Fatal(err)
	}
	settings := consumerSettings()
	settings.consumerTries = 1
	var fail atomic.Bool
	fail.Store(true)
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
			if fail.Load() && strings.HasPrefix(event.OrderID, "p-") {
				return errText("poison")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events), withEventSettings(settings))
	key := "k1"
	publishTestEvent(t, events, "p-1", "orders.OrderPlaced", &key, nil)
	publishTestEvent(t, events, "p-2", "orders.OrderPlaced", &key, nil)
	waitUntil(t, "the head and its follower", func() bool { return len(lettersOf(t, store, DefaultProcessor)) == 2 })
	letters := lettersOf(t, store, DefaultProcessor)
	var head, follower string
	for _, letter := range letters {
		switch letter.Position {
		case "0":
			head = letter.ID
		case "1":
			follower = letter.ID
		}
	}
	s.Send(t, deadLetterRetryControl("retry-1", DefaultProcessor, follower))
	outcome := s.RecvControlReply(t).GetRetry()
	if outcome == nil || outcome.GetResult() != retryBlocked || outcome.GetDetail() != head {
		t.Fatalf("retry of a sequenced letter: %v", outcome)
	}
	s.Send(t, deadLetterDiscardControl("discard-1", DefaultProcessor, head))
	if answer := s.RecvControlReply(t).GetOk(); answer == nil {
		t.Fatal("discard of the head")
	}
	fail.Store(false)
	s.Send(t, deadLetterRetryControl("retry-2", DefaultProcessor, follower))
	if outcome := s.RecvControlReply(t).GetRetry(); outcome == nil || outcome.GetResult() != retryRetried {
		t.Fatalf("retry after the head left: %v", outcome)
	}
}

// L3-DEAD-LETTERS: a retried letter whose marker existed already is Duplicate and is deleted.
func TestARetriedLetterWithItsMarkerIsDuplicate(t *testing.T) {
	hub := fakehub.Start(t)
	store := NewMemoryMessageStore()
	events := NewMemoryEventStore()
	var calls atomic.Int64
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}, WithStore(store), WithEventStore(events))
	if _, err := store.Processed().Record(context.Background(), ProcessedMarker{
		ID: ProcessorMarker(DefaultProcessor, "evt-1"), Type: "orders.OrderPlaced", ProcessedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	letter := letterFor(DefaultProcessor, DefaultEventsSource, "0", "0", "", DeadLetterHandlerFailed)
	letter.EventID = "evt-1"
	letter.EventType = "orders.OrderPlaced"
	letter.Headers[HeaderID] = "evt-1"
	letter.Headers[HeaderType] = "orders.OrderPlaced"
	if _, err := store.DeadLetters().Park(context.Background(), letter); err != nil {
		t.Fatal(err)
	}
	s.Send(t, deadLetterRetryControl("retry-1", DefaultProcessor, letter.ID))
	outcome := s.RecvControlReply(t).GetRetry()
	if outcome == nil || outcome.GetResult() != retryDuplicate {
		t.Fatalf("retry of a handled letter: %v", outcome)
	}
	if got := len(lettersOf(t, store, DefaultProcessor)); got != 0 {
		t.Fatalf("letters: %d", got)
	}
	is(t, calls.Load(), int64(0))
}

// L3-DEAD-LETTERS: outside mode collection no dead-letter action is available.
func TestDeadLetterActionsOutsideCollectionAreInvalid(t *testing.T) {
	hub := fakehub.Start(t)
	settings := consumerSettings()
	settings.deadLetterMode = DeadLetterModeLog
	_, s := l3Serving(t, hub, func(app *App) {
		if err := HandleEvent(app, func(context.Context, OrderPlaced) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}, withEventSettings(settings))
	s.Send(t, deadLetterListControl("list-1", DefaultProcessor, 10))
	if answer := s.RecvControlReply(t).GetRefused(); answer == nil ||
		answer.GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID {
		t.Fatalf("list in log mode: %v", answer)
	}
	s.Send(t, deadLetterRetryAllControl("retry-all-1", DefaultProcessor))
	if answer := s.RecvControlReply(t).GetRefused(); answer == nil ||
		answer.GetCode() != hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID {
		t.Fatalf("retry-all in log mode: %v", answer)
	}
}
