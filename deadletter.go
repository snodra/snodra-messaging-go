package snodra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Where a record that exhausts its attempts goes (ProcessorStatus.dead_letter_mode).
const (
	// DeadLetterModeCollection parks it in the message store's dead letters, the default: an
	// operator lists, retries and discards it through the hub.
	DeadLetterModeCollection = "collection"
	// DeadLetterModeEventStore publishes it to the event store's dead-letter source
	// (<source>.dlt); the event store must declare CapabilityNativeDeadLetter (the in-memory one
	// does not, as the reference memory stores do not).
	DeadLetterModeEventStore = "event-store"
	// DeadLetterModeLog logs it and skips it; nothing is parked or published.
	DeadLetterModeLog = "log"
)

// newDeadLetter builds the letter of a delivered record, ready for DeadLetterStore.Park: the id
// "<processor>:<source>:<segment>:<position>", the record's key and headers, its id and type
// headers as EventID and EventType (a blank one is none), its value byte for byte, and ExpireAt
// at the configured retention from now.
func (a *App) newDeadLetter(p *processor, d Delivery, reason DeadLetterReason, failure DeadLetterError, attempts int, blockedBy string, replay bool) DeadLetter {
	now := a.now()
	headers := copyStringMap(d.Headers)
	eventID := ""
	if value := headers[HeaderID]; strings.TrimSpace(value) != "" {
		eventID = value
	}
	eventType := ""
	if value := headers[HeaderType]; strings.TrimSpace(value) != "" {
		eventType = value
	}
	return DeadLetter{
		ID:            LetterID(p.name, p.subscription.Source, d.Segment, d.Position),
		Processor:     p.name,
		Source:        p.subscription.Source,
		Segment:       d.Segment,
		Position:      d.Position,
		Key:           copyStringPointer(d.Key),
		EventID:       eventID,
		EventType:     eventType,
		Headers:       headers,
		Payload:       append([]byte(nil), d.Payload...),
		Reason:        reason,
		Error:         failure,
		BlockedBy:     blockedBy,
		Attempts:      attempts,
		ParkedAt:      now,
		LastAttemptAt: now,
		ExpireAt:      now.Add(a.cfg.events.deadLetterRetention),
		Replay:        replay,
	}
}

// deadLetterErrorOf is a handler failure's DeadLetterError: the Go concrete type's simple name
// (the analogue of the reference's exception class name) and the error's text. A payload its
// handler cannot decode is "InvalidMessage", as the references name it.
func deadLetterErrorOf(err error) DeadLetterError {
	typeName := "Error"
	var undecodable *decodeError
	if errors.As(err, &undecodable) {
		typeName = "InvalidMessage"
	} else if concrete := reflect.TypeOf(err); concrete != nil {
		for concrete.Kind() == reflect.Pointer {
			concrete = concrete.Elem()
		}
		if name := concrete.Name(); name != "" {
			typeName = name
		}
	}
	return NewDeadLetterError(typeName, err.Error())
}

// The six headers a record gains when it is published to the event store's dead-letter source.
const (
	headerDLTProcessor       = "snodra-dlt-processor"
	headerDLTReason          = "snodra-dlt-reason"
	headerDLTError           = "snodra-dlt-error"
	headerDLTOriginalSource  = "snodra-dlt-original-topic"
	headerDLTOriginalSegment = "snodra-dlt-original-partition"
	headerDLTOriginalOffset  = "snodra-dlt-original-offset"
)

// nativeDeadLetterHeaders are the six headers a record's own headers are stripped of before the
// library writes them.
var nativeDeadLetterHeaders = []string{
	headerDLTProcessor, headerDLTReason, headerDLTError,
	headerDLTOriginalSource, headerDLTOriginalSegment, headerDLTOriginalOffset,
}

// nativeDeadLetter is the event a record becomes in the event-store dead-letter mode: its key,
// payload and headers (minus the six the library writes), its id header or
// "<source>-<segment>-<position>", and the six snodra-dlt-* headers saying why and where from.
func (a *App) nativeDeadLetter(p *processor, d Delivery, reason DeadLetterReason, failure DeadLetterError) OutgoingEvent {
	headers := copyStringMap(d.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	for _, name := range nativeDeadLetterHeaders {
		delete(headers, name)
	}
	headers[headerDLTProcessor] = p.name
	headers[headerDLTReason] = string(reason)
	headers[headerDLTError] = cutChars(failure.Type+": "+failure.Message, MaxDeadLetterErrorChars)
	headers[headerDLTOriginalSource] = p.subscription.Source
	headers[headerDLTOriginalSegment] = d.Segment
	headers[headerDLTOriginalOffset] = d.Position
	id := strings.TrimSpace(d.Headers[HeaderID])
	if id == "" {
		id = p.subscription.Source + "-" + d.Segment + "-" + d.Position
	}
	return OutgoingEvent{
		Source:  DeadLetterSource(p.subscription.Source),
		ID:      id,
		Type:    d.Headers[HeaderType],
		Key:     copyStringPointer(d.Key),
		Headers: headers,
		Payload: append([]byte(nil), d.Payload...),
	}
}

// publishDeadLetterWork is the sink action of a record in the event-store mode: publish it to
// the dead-letter source, once. A redelivery after a failure runs it again; the record stays
// unacknowledged until the event store took it.
func (a *App) publishDeadLetterWork(p *processor, d Delivery, reason DeadLetterReason, failure DeadLetterError) func(context.Context) error {
	event := a.nativeDeadLetter(p, d, reason, failure)
	return func(ctx context.Context) error {
		if timeout := a.cfg.events.sendTimeout; timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		if err := a.cfg.eventStore.Publish(ctx, event); err != nil {
			return fmt.Errorf("publishing a record to the dead-letter source %s: %w", event.Source, err)
		}
		return nil
	}
}

// retryOutcome is what one dead-letter retry did, with the protocol's result names.
type retryOutcome struct {
	result string
	detail string
}

// The DeadLetterRetryOutcome.result names.
const (
	retryRetried      = "Retried"
	retryDuplicate    = "Duplicate"
	retryFailed       = "Failed"
	retryBlocked      = "Blocked"
	retryNotRetriable = "NotRetriable"
	retryNotFound     = "NotFound"
)

// retryDeadLetter retries one letter: stage 1 of the letter's processor runs under the record's
// marker, and the letter is deleted in that same transaction. A SEQUENCED letter whose earlier
// letter is still parked is Blocked; an INVALID letter is NotRetriable; one whose marker existed
// already is Duplicate (the letter is deleted).
func (a *App) retryDeadLetter(ctx context.Context, p *processor, letter *DeadLetter) (retryOutcome, error) {
	if letter.Reason == DeadLetterInvalid {
		return retryOutcome{result: retryNotRetriable}, nil
	}
	if letter.Key != nil {
		earlier, blocked, err := a.cfg.store.DeadLetters().Earlier(ctx, p.name, *letter.Key, letter.Position)
		if err != nil {
			return retryOutcome{}, err
		}
		if blocked {
			return retryOutcome{result: retryBlocked, detail: earlier}, nil
		}
	}
	typeName := letter.EventType
	if typeName == "" {
		return retryOutcome{result: retryNotRetriable}, nil
	}
	handlers := p.handlers[typeName]
	if len(handlers) == 0 {
		failure := NewDeadLetterError("NoHandler", "no handler for "+typeName+" in processor "+p.name)
		outcome, err := a.deadLetterRetryFailed(ctx, letter, failure)
		return outcome, err
	}
	id := letter.DedupeID()
	markerID := ProcessorMarker(p.name, id)
	scope := eventHandling(id, typeName, letter.Headers)
	done, duplicate, gone, failure := a.retryStage1(ctx, p, letter, markerID, typeName, handlers, scope)
	switch {
	case gone:
		return retryOutcome{result: retryNotFound}, nil
	case duplicate:
		if _, err := a.cfg.store.DeadLetters().Delete(ctx, letter.ID); err != nil {
			return retryOutcome{}, err
		}
		a.log.Info("a dead letter's event was handled before; deleted the letter",
			"processor", p.name, "letter", letter.ID)
		return retryOutcome{result: retryDuplicate}, nil
	case done:
		a.log.Info("retried a dead letter", "processor", p.name, "letter", letter.ID)
		return retryOutcome{result: retryRetried}, nil
	default:
		return a.deadLetterRetryFailed(ctx, letter, deadLetterErrorOf(failure))
	}
}

// retryStage1 runs the letter's processor's stage 1 in one transaction with the letter's
// deletion: done, a duplicate marker, the letter gone meanwhile, or the failure.
func (a *App) retryStage1(ctx context.Context, p *processor, letter *DeadLetter, markerID, typeName string, handlers []*eventHandler, scope *handling) (done, duplicate, gone bool, failure error) {
	store := a.cfg.store
	txctx, tx, err := store.Begin(a.handlerCtx)
	if err != nil {
		return false, false, false, fmt.Errorf("begin the retry's transaction: %w", err)
	}
	marker := ProcessedMarker{
		ID:          markerID,
		Type:        typeName,
		ProcessedAt: a.now(),
		Processor:   p.name,
		Source:      letter.Source,
		Segment:     letter.Segment,
		Position:    letter.Position,
	}
	recorded, err := store.Processed().Record(txctx, marker)
	if err != nil {
		_ = tx.Rollback(txctx)
		return false, false, false, fmt.Errorf("record the retry's marker: %w", err)
	}
	if !recorded {
		_ = tx.Rollback(txctx)
		return false, true, false, nil
	}
	deleted, err := store.DeadLetters().DeleteIn(txctx, letter.ID)
	if err != nil {
		_ = tx.Rollback(txctx)
		return false, false, false, fmt.Errorf("delete the retried dead letter: %w", err)
	}
	if !deleted {
		_ = tx.Rollback(txctx)
		return false, false, true, nil
	}
	handlerCtx := withHandling(txctx, scope)
	for _, handler := range handlers {
		if letter.Replay && handler.disallowReplay {
			continue
		}
		if err := handler.call(handlerCtx, a.cfg.codec, letter.Payload); err != nil {
			_ = tx.Rollback(txctx)
			return false, false, false, err
		}
	}
	if err := tx.Commit(txctx); err != nil {
		_ = tx.Rollback(txctx)
		return false, false, false, fmt.Errorf("commit the retry's transaction: %w", err)
	}
	return true, false, false, nil
}

// deadLetterRetryFailed records that a retry failed again: the letter stays with Attempts + 1,
// the new error and the new last attempt, and the outcome says why.
func (a *App) deadLetterRetryFailed(ctx context.Context, letter *DeadLetter, failure DeadLetterError) (retryOutcome, error) {
	changed, err := a.cfg.store.DeadLetters().RecordFailure(ctx, letter.ID, failure, a.now())
	if err != nil {
		return retryOutcome{}, err
	}
	if !changed {
		return retryOutcome{result: retryNotFound}, nil
	}
	a.log.Warn("a dead letter's retry failed again",
		"processor", letter.Processor, "letter", letter.ID,
		"errorType", failure.Type, "error", failure.Message)
	return retryOutcome{result: retryFailed, detail: failure.Message}, nil
}

// retryAllOutcome is what retrying every letter of a processor did.
type retryAllOutcome struct {
	retried, failed, blocked, skipped uint64
}

// retryAllDeadLetters retries every letter of p: per key in position order, a key stopping at
// its first failure (its later letters count as blocked); then the keyless letters by parkedAt.
// INVALID letters, and letters gone meanwhile, are skipped.
func (a *App) retryAllDeadLetters(ctx context.Context, p *processor) (retryAllOutcome, error) {
	summaries, err := a.cfg.store.DeadLetters().Summaries(ctx, p.name)
	if err != nil {
		return retryAllOutcome{}, err
	}
	var outcome retryAllOutcome
	retriable := make([]DeadLetterSummary, 0, len(summaries))
	for _, summary := range summaries {
		if summary.Reason == DeadLetterInvalid {
			outcome.skipped++
			continue
		}
		retriable = append(retriable, summary)
	}
	byKey := map[string][]DeadLetterSummary{}
	var keys []string
	var keyless []DeadLetterSummary
	for _, summary := range retriable {
		if summary.Key == nil {
			keyless = append(keyless, summary)
			continue
		}
		if _, seen := byKey[*summary.Key]; !seen {
			keys = append(keys, *summary.Key)
		}
		byKey[*summary.Key] = append(byKey[*summary.Key], summary)
	}
	// A key's letters keep the store's position order; the keys go by their head's parkedAt, id.
	sort.SliceStable(keys, func(i, j int) bool {
		a, b := byKey[keys[i]][0], byKey[keys[j]][0]
		if !a.ParkedAt.Equal(b.ParkedAt) {
			return a.ParkedAt.Before(b.ParkedAt)
		}
		return a.ID < b.ID
	})
	sort.SliceStable(keyless, func(i, j int) bool {
		if !keyless[i].ParkedAt.Equal(keyless[j].ParkedAt) {
			return keyless[i].ParkedAt.Before(keyless[j].ParkedAt)
		}
		return keyless[i].ID < keyless[j].ID
	})
	groups := make([][]DeadLetterSummary, 0, len(keys)+len(keyless))
	for _, key := range keys {
		groups = append(groups, byKey[key])
	}
	for _, summary := range keyless {
		groups = append(groups, []DeadLetterSummary{summary})
	}
	for _, group := range groups {
		stopped := false
		for _, summary := range group {
			if stopped {
				outcome.blocked++
				continue
			}
			letter, err := a.cfg.store.DeadLetters().Get(ctx, summary.ID)
			if err != nil {
				return outcome, err
			}
			if letter == nil {
				outcome.skipped++
				continue
			}
			if letter.Processor != p.name {
				outcome.skipped++
				continue
			}
			result, err := a.retryDeadLetter(ctx, p, letter)
			if err != nil {
				return outcome, err
			}
			switch result.result {
			case retryRetried, retryDuplicate:
				outcome.retried++
			case retryFailed:
				outcome.failed++
				stopped = true
			case retryBlocked:
				outcome.blocked++
				stopped = true
			default:
				outcome.skipped++
			}
		}
	}
	a.log.Info("retried a processor's dead letters",
		"processor", p.name, "retried", outcome.retried, "failed", outcome.failed,
		"blocked", outcome.blocked, "skipped", outcome.skipped)
	return outcome, nil
}
