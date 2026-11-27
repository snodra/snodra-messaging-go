package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// A record that fails every attempt is parked as a dead letter (mode collection, the default);
// the modes that send it to the event store instead or only log it arrive with their stores.
//
// The attempts and the wait between them are the references' (Kotlin EventConsumer: five
// invocations, one second apart); the pages name neither.
//
// TODO(spec-gap): SG-33 (the attempts and the wait between them).
//
// A failed subscription waits 1 s before the next one, doubling to 30 s on every failure that
// follows; the wait starts over after a subscription delivered a record or lived 30 s. The
// references use the same schedule.
//
// TODO(spec-gap): SG-35 (the re-subscribe backoff of a failed event subscription).

// positionKey names one delivered record within its subscription.
type positionKey struct {
	segment  string
	position string
}

// processorSession is one subscription's read: the processor, its subscription, the keys of its
// parked dead letters per segment and the sink actions a redelivery still has to run. It belongs
// to the consumer's goroutine alone.
type processorSession struct {
	p   *processor
	sub EventSubscription
	// parkedKeys are the keys with a parked letter on the assigned segments (loaded on
	// assignment, added on every park); a keyed record of one of them confirms against the store.
	parkedKeys map[string]struct{}
	// keysUnknown are the segments whose parked keys could not be loaded: every keyed record of
	// them confirms against the store.
	keysUnknown map[string]struct{}
	// pending are the sink actions (park) of records that were not acknowledged yet: the
	// redelivery runs the action only, not the handlers again.
	pending map[positionKey]func(context.Context) error
}

func newProcessorSession(p *processor, sub EventSubscription) *processorSession {
	return &processorSession{
		p:           p,
		sub:         sub,
		parkedKeys:  map[string]struct{}{},
		keysUnknown: map[string]struct{}{},
		pending:     map[positionKey]func(context.Context) error{},
	}
}

// consumeProcessor is one processor's loop: read its control state, subscribe, read until the
// subscription fails or the app stops, then subscribe again after a backoff. Every record is
// handled by one instance that holds its segment, in segment order; the processed-event marker
// skips a duplicate, and a record that exhausts its attempts is parked as a dead letter.
//
// Stage 1 is the processor's event handlers. Stage 2 (the sagas) runs only in the default
// processor and arrives with the saga support; a named processor never has one.
func (a *App) consumeProcessor(ctx context.Context, p *processor) {
	p.run.markStarted()
	defer p.run.markStopped()
	settings := a.cfg.events
	wait := settings.resubscribeMin
	for {
		if ctx.Err() != nil {
			return
		}
		// A pause persisted before this instance started must not be missed: no subscription
		// before the control state was read once. A reset lock keeps the processor out of its
		// subscription until it is released or its lease passes.
		if !p.run.waitControl(ctx) {
			return
		}
		if !p.run.waitRunnable(ctx) {
			return
		}
		started := a.now()
		stable := false
		var failure error
		subCtx, cancel := context.WithCancel(ctx)
		done := p.run.beginRead(cancel)
		sub, err := a.cfg.eventStore.Subscribe(subCtx, p.subscription)
		if err != nil {
			cancel()
			p.run.endRead(done)
			if ctx.Err() != nil {
				return
			}
			failure = err
		} else {
			session := newProcessorSession(p, sub)
			p.run.attach(sub)
			if !p.run.runnableNow() {
				// A reset took the lock while the subscription was being built: leave it at
				// once; the reset prepare waits for exactly this.
				p.run.detachAndClose(sub, func() { closeEventSubscription(sub) })
				cancel()
				p.run.endRead(done)
				continue
			}
			delivered, readErr := a.readProcessor(subCtx, session)
			p.run.detachAndClose(sub, func() { closeEventSubscription(sub) })
			cancel()
			p.run.endRead(done)
			if ctx.Err() != nil {
				return
			}
			if !p.run.runnableNow() {
				// A reset lock stopped the subscription, not a failure: wait for the lock, no
				// re-subscribe backoff.
				continue
			}
			failure = readErr
			stable = delivered || a.now().Sub(started) >= settings.resubscribeMax
		}
		if failure != nil {
			p.run.markFailed(fmt.Errorf("subscription: %w", failure), a.now())
			a.log.Warn("an event processor's subscription failed; subscribing again",
				"processor", p.name, "subscription", p.subscription.Name, "in", wait, "error", failure)
		}
		if stable {
			wait = settings.resubscribeMin
		}
		if !settings.sleep(ctx, wait) {
			return
		}
		wait = nextResubscribe(wait, settings)
	}
}

// nextResubscribe doubles wait up to the configured cap.
func nextResubscribe(wait time.Duration, s eventSettings) time.Duration {
	next := wait * 2
	if next > s.resubscribeMax {
		next = s.resubscribeMax
	}
	return next
}

// closeEventSubscription leaves sub, flushing what it acknowledged, within a bound of its own.
func closeEventSubscription(sub EventSubscription) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = sub.Close(ctx)
}

// readProcessor reads the session's subscription until it fails or ctx ends; delivered reports
// that it handed out at least one record (which resets the re-subscribe backoff).
func (a *App) readProcessor(ctx context.Context, s *processorSession) (delivered bool, err error) {
	settings := a.cfg.events
	for {
		event, err := s.sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return delivered, nil
			}
			if message := s.sub.TakeError(); message != "" {
				s.p.run.markFailed(errText(message), a.now())
			}
			return delivered, err
		}
		switch event := event.(type) {
		case Assigned:
			s.p.run.markAssigned()
			a.loadParkedKeys(ctx, s, event.Segments)
			a.log.Debug("an event processor was assigned segments",
				"processor", s.p.name, "segments", event.Segments)
		case Revoked:
			s.p.run.markRevoked()
			forgetParkedKeys(s, event.Segments)
			a.log.Debug("an event processor lost segments",
				"processor", s.p.name, "segments", event.Segments)
		case Delivery:
			if s.p.run.pausedNow() {
				// Paused after the subscription handed it out: not handled, handed out again on
				// the resume.
				if err := s.sub.Redeliver(event.Segment, event.Position); err != nil {
					a.log.Debug("could not redeliver a record handed out while pausing",
						"processor", s.p.name, "segment", event.Segment, "position", event.Position,
						"error", err)
				}
				continue
			}
			delivered = true
			if failure := a.handleDelivery(ctx, s, event); failure != nil {
				if ctx.Err() != nil {
					return delivered, nil
				}
				// The sink could not take it: not acknowledged, handed out again after the
				// back-off; its pending action runs then, not the handlers again.
				if err := s.sub.Redeliver(event.Segment, event.Position); err != nil {
					a.log.Debug("could not redeliver a record the dead letters did not take",
						"processor", s.p.name, "segment", event.Segment, "position", event.Position,
						"error", err)
				}
				if !sleepCtx(ctx, settings.consumerBackoff) {
					return delivered, nil
				}
			}
			if ctx.Err() != nil {
				return delivered, nil
			}
		}
		if message := s.sub.TakeError(); message != "" {
			s.p.run.markFailed(errText(message), a.now())
		}
	}
}

// handleDelivery handles one delivered event: the events its processor handles, the stage-1
// marker and the handlers in one transaction, and an acknowledgement only after the commit. An
// event type the processor does not handle is acknowledged and runs nothing; a malformed record
// and a record that exhausts its attempts become dead letters. A duplicate marker skips the
// handlers and is acknowledged. The error is a store failure (the sink, or the dead-letter check
// of a sequenced record): the record is not acknowledged and comes again.
func (a *App) handleDelivery(ctx context.Context, s *processorSession, d Delivery) error {
	key := positionKey{segment: d.Segment, position: d.Position}
	if work, pending := s.pending[key]; pending {
		return a.toSink(ctx, s, d, work)
	}
	typeName := strings.TrimSpace(d.Headers[HeaderType])
	if typeName == "" {
		a.log.Warn("an event record has no type header; parking it as a dead letter",
			"processor", s.p.name, "source", s.p.subscription.Source,
			"segment", d.Segment, "position", d.Position)
		return a.parkInvalid(ctx, s, d, "record without type header")
	}
	handlers := s.p.handlers[typeName]
	if len(handlers) == 0 {
		a.log.Debug("an event type has no handler in the processor; acknowledging it",
			"processor", s.p.name, "type", typeName)
		a.acknowledge(s.p, s.sub, d)
		return nil
	}
	if !isJSONObject(d.Payload) {
		a.log.Warn("an event record's payload is not a JSON object; parking it as a dead letter",
			"processor", s.p.name, "type", typeName, "source", s.p.subscription.Source,
			"segment", d.Segment, "position", d.Position)
		return a.parkInvalid(ctx, s, d, "payload is not a JSON object")
	}
	id := d.Headers[HeaderID]
	if strings.TrimSpace(id) == "" {
		id = s.p.subscription.Source + "-" + d.Segment + "-" + d.Position
	}
	scope := eventHandling(id, typeName, d.Headers)
	replay := a.inReplay(s.p, d)
	handled, err := a.sequence(ctx, s, d, typeName, replay)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}
	return a.attemptRecord(ctx, s, d, id, typeName, scope, replay)
}

// sequence parks a record SEQUENCED when an earlier letter of its key is parked, and
// acknowledges one whose own letter exists already (parked before a crash). handled reports that
// the record is done with; err is a store failure (the record is not acknowledged).
func (a *App) sequence(ctx context.Context, s *processorSession, d Delivery, typeName string, replay bool) (handled bool, err error) {
	if a.cfg.events.deadLetterMode != DeadLetterModeCollection || d.Key == nil {
		return false, nil
	}
	if len(s.p.handlers[typeName]) == 0 {
		return false, nil // only stage 1 is sequenced
	}
	key := *d.Key
	if _, unknown := s.keysUnknown[d.Segment]; !unknown {
		if _, parked := s.parkedKeys[key]; !parked {
			return false, nil
		}
	}
	head, ok, err := a.cfg.store.DeadLetters().Head(ctx, s.p.name, key)
	if err != nil {
		if errors.Is(err, ErrUnsupported) {
			return false, nil // a store without dead letters keeps consuming
		}
		s.p.run.markFailed(fmt.Errorf("could not check the dead letters of key %q: %w", key, err), a.now())
		return false, err
	}
	if !ok {
		delete(s.parkedKeys, key) // retried, discarded or expired, here or on another instance
		return false, nil
	}
	own := LetterID(s.p.name, s.p.subscription.Source, d.Segment, d.Position)
	if head == own {
		a.log.Info("a record's dead letter exists already; acknowledging the record",
			"processor", s.p.name, "letter", own)
		a.acknowledge(s.p, s.sub, d)
		return true, nil
	}
	letter, err := a.cfg.store.DeadLetters().Get(ctx, own)
	if err != nil && !errors.Is(err, ErrUnsupported) {
		s.p.run.markFailed(fmt.Errorf("could not read the record's dead letter: %w", err), a.now())
		return false, err
	}
	if letter != nil {
		a.log.Info("a record's dead letter exists already; acknowledging the record",
			"processor", s.p.name, "letter", own)
		a.acknowledge(s.p, s.sub, d)
		return true, nil
	}
	// The sagas of `default` see a SEQUENCED record even though stage 1 waits; they arrive with
	// the saga support.
	sequenced := a.newDeadLetter(s.p, d, DeadLetterSequenced, SequencedDeadLetterError(head), 0, head, replay)
	a.log.Info("a record is parked SEQUENCED behind an earlier letter",
		"processor", s.p.name, "key", key, "letter", sequenced.ID, "blockedBy", head)
	return true, a.toSink(ctx, s, d, a.parkWork(s, d, sequenced))
}

// attemptRecord runs the record's stage 1 until it succeeds, a duplicate marker skips it, or its
// attempts are exhausted (or the payload cannot be decoded) and it is parked.
func (a *App) attemptRecord(ctx context.Context, s *processorSession, d Delivery, id, typeName string, scope *handling, replay bool) error {
	markerID := ProcessorMarker(s.p.name, id)
	settings := a.cfg.events
	for attempt := 1; ; attempt++ {
		done, duplicate, failure := a.handleOnce(ctx, s.p, markerID, typeName, d, scope, replay)
		a.log.Debug("an event attempt ended", "processor", s.p.name, "type", typeName, "id", id,
			"attempt", attempt, "done", done, "duplicate", duplicate, "error", failure)
		if duplicate {
			a.log.Debug("a redelivered event was handled before; skipping it",
				"processor", s.p.name, "type", typeName, "id", id)
			s.p.processedTotal.Add(1)
			a.acknowledge(s.p, s.sub, d)
			return nil
		}
		if done {
			s.p.processedTotal.Add(1)
			a.acknowledge(s.p, s.sub, d)
			return nil
		}
		if ctx.Err() != nil {
			// The app is stopping: the record stays unacknowledged for the next subscription.
			return nil
		}
		if failure == nil {
			failure = errors.New("the event could not be handled")
		}
		var undecodable *decodeError
		if errors.As(failure, &undecodable) {
			// The payload cannot be read as the handler's message type: no retry can change it.
			return a.parkFailed(ctx, s, d, typeName, failure, attempt, true, replay)
		}
		if attempt >= settings.consumerTries {
			return a.parkFailed(ctx, s, d, typeName, failure, attempt, false, replay)
		}
		a.log.Warn("an event handler failed; trying again",
			"processor", s.p.name, "type", typeName, "id", id, "attempt", attempt,
			"in", settings.consumerBackoff, "error", failure)
		if !sleepCtx(ctx, settings.consumerBackoff) {
			return nil
		}
	}
}

// parkFailed parks a record that exhausted its attempts (HANDLER_FAILED) and acknowledges it.
func (a *App) parkFailed(ctx context.Context, s *processorSession, d Delivery, typeName string, failure error, attempts int, undecodable, replay bool) error {
	s.p.failedTotal.Add(1)
	s.p.run.markFailed(failure, a.now())
	if a.cfg.events.deadLetterMode != DeadLetterModeCollection {
		a.log.Error("an event handler failed on every attempt; skipping the event",
			"processor", s.p.name, "type", typeName, "attempts", attempts, "error", failure)
		a.acknowledge(s.p, s.sub, d)
		return nil
	}
	letter := a.newDeadLetter(s.p, d, DeadLetterHandlerFailed, deadLetterErrorOf(failure), attempts, "", replay)
	message := "an event handler failed on every attempt; parking it as a dead letter"
	if undecodable {
		message = "an event's payload cannot be read by its handler; parking it as a dead letter"
	}
	a.log.Error(message,
		"processor", s.p.name, "letter", letter.ID, "type", typeName, "attempts", attempts,
		"errorType", letter.Error.Type, "error", letter.Error.Message)
	return a.toSink(ctx, s, d, a.parkWork(s, d, letter))
}

// parkInvalid parks a record that can never be handled (INVALID: no type header, a value that is
// not a JSON object) and acknowledges it. It blocks no key and can only be discarded.
func (a *App) parkInvalid(ctx context.Context, s *processorSession, d Delivery, why string) error {
	if a.cfg.events.deadLetterMode != DeadLetterModeCollection {
		a.log.Warn("skipping a malformed record", "processor", s.p.name, "reason", why)
		a.acknowledge(s.p, s.sub, d)
		return nil
	}
	replay := a.inReplay(s.p, d)
	letter := a.newDeadLetter(s.p, d, DeadLetterInvalid, InvalidDeadLetterError(why), 0, "", replay)
	return a.toSink(ctx, s, d, a.parkWork(s, d, letter))
}

// parkWork is the sink action of a letter: the store's Park, once. A redelivery after a failure
// runs it again without the handlers.
func (a *App) parkWork(s *processorSession, d Delivery, letter DeadLetter) func(context.Context) error {
	return func(ctx context.Context) error {
		result, err := a.cfg.store.DeadLetters().Park(ctx, letter)
		if err != nil {
			if errors.Is(err, ErrUnsupported) {
				// TODO(spec-gap): SG-49 (a store without the dead-letter port skips the record).
				// A store without the dead-letter port: keep consuming, as level 2 did.
				a.log.Error("the message store keeps no dead letters; skipping the record",
					"processor", s.p.name, "letter", letter.ID)
				return nil
			}
			return err
		}
		if result == ParkNew {
			s.p.parkedSinceSample.Store(true)
			a.kickDeadLetterSample()
		}
		if d.Key != nil && letter.Reason != DeadLetterInvalid {
			// An INVALID letter blocks no key: adding it would only cost a useless Head check on
			// every later record of that key.
			s.parkedKeys[*d.Key] = struct{}{}
		}
		return nil
	}
}

// toSink runs a sink action and acknowledges the record once it took it. A failure keeps the
// action for the redelivery and records the failure.
func (a *App) toSink(ctx context.Context, s *processorSession, d Delivery, work func(context.Context) error) error {
	key := positionKey{segment: d.Segment, position: d.Position}
	s.pending[key] = work
	if err := work(ctx); err != nil {
		s.p.run.markFailed(fmt.Errorf("could not hand record %s-%s@%s to the dead letters: %w",
			s.p.subscription.Source, d.Segment, d.Position, err), a.now())
		return err
	}
	delete(s.pending, key)
	a.acknowledge(s.p, s.sub, d)
	return nil
}

// loadParkedKeys loads the keys of the processor's letters on the newly assigned segments, and
// marks them unknown when the store cannot answer (every keyed record then confirms).
func (a *App) loadParkedKeys(ctx context.Context, s *processorSession, segments []string) {
	if a.cfg.events.deadLetterMode != DeadLetterModeCollection || len(segments) == 0 {
		return
	}
	keys, err := a.cfg.store.DeadLetters().ParkedKeys(ctx, s.p.name, s.p.subscription.Source, segments)
	if err != nil {
		if errors.Is(err, ErrUnsupported) {
			for _, segment := range segments {
				delete(s.keysUnknown, segment)
			}
			return
		}
		for _, segment := range segments {
			s.keysUnknown[segment] = struct{}{}
		}
		a.log.Warn("could not load the keys of the processor's dead letters; every keyed record will confirm",
			"processor", s.p.name, "segments", segments, "error", err)
		return
	}
	for _, key := range keys {
		s.parkedKeys[key] = struct{}{}
	}
	for _, segment := range segments {
		delete(s.keysUnknown, segment)
	}
}

// forgetParkedKeys forgets what the revoked segments said about their keys. The parked keys are
// kept: a stale one only costs a confirmation, never a skipped record.
func forgetParkedKeys(s *processorSession, segments []string) {
	for _, segment := range segments {
		delete(s.keysUnknown, segment)
	}
}

// handleOnce runs one attempt: a transaction, the stage-1 marker, the handlers and the commit.
// done is true when the marker and the handlers committed; duplicate is true when the marker
// existed already (nothing ran and nothing must commit); failure says why the attempt failed.
func (a *App) handleOnce(ctx context.Context, p *processor, markerID, typeName string, d Delivery, scope *handling, replay bool) (done, duplicate bool, failure error) {
	store := a.cfg.store
	txctx, tx, err := store.Begin(a.handlerCtx)
	if err != nil {
		return false, false, fmt.Errorf("begin the event's transaction: %w", err)
	}
	marker := ProcessedMarker{
		ID:          markerID,
		Type:        typeName,
		ProcessedAt: a.now(),
		Processor:   p.name,
		Source:      p.subscription.Source,
		Segment:     d.Segment,
		Position:    d.Position,
	}
	recorded, err := store.Processed().Record(txctx, marker)
	if err != nil {
		_ = tx.Rollback(txctx)
		return false, false, fmt.Errorf("record the event's marker: %w", err)
	}
	if !recorded {
		_ = tx.Rollback(txctx)
		return false, true, nil
	}
	handlerCtx := withHandling(txctx, scope)
	for _, handler := range p.handlers[typeName] {
		if replay && handler.disallowReplay {
			continue
		}
		if err := handler.call(handlerCtx, a.cfg.codec, d.Payload); err != nil {
			_ = tx.Rollback(txctx)
			return false, false, err
		}
	}
	if err := tx.Commit(txctx); err != nil {
		_ = tx.Rollback(txctx)
		return false, false, fmt.Errorf("commit the event's transaction: %w", err)
	}
	return true, false, nil
}

// acknowledge acknowledges a record after it was handled (or skipped). A failure is logged and the
// record comes again later, where its marker skips it.
func (a *App) acknowledge(p *processor, sub EventSubscription, d Delivery) {
	if err := sub.Ack(d.Segment, d.Position); err != nil {
		a.log.Warn("an event could not be acknowledged",
			"processor", p.name, "segment", d.Segment, "position", d.Position, "error", err)
	}
}

// sleepCtx waits d, or less when ctx ends; false when ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
