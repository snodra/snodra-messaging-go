package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// A record that fails every attempt is logged and skipped at level 2; level 3 parks it as a dead
// letter. The attempts and the wait between them are the references' (Kotlin EventConsumer: five
// invocations, one second apart); the pages name neither at level 2.
//
// TODO(spec-gap): SG-33 (the attempts and the wait between them at level 2, and the log-and-skip
// that follows them).

// A record without a type header, or whose payload is not a JSON object, can never be handled.
// Level 2 logs it and acknowledges it; level 3 parks it as an INVALID dead letter. The reference
// marks it INVALID and sends it to its sink (Kotlin EventConsumer: a record without a type header
// or with a non-object payload is INVALID).
//
// TODO(spec-gap): SG-34 (what a level-2 library does with a malformed record).

// A failed subscription waits 1 s before the next one, doubling to 30 s on every failure that
// follows; the wait starts over after a subscription delivered a record or lived 30 s. The
// references use the same schedule.
//
// TODO(spec-gap): SG-35 (the re-subscribe backoff of a failed event subscription).

// consumeProcessor is one processor's loop: subscribe, read until the subscription fails or the
// app stops, then subscribe again after a backoff. Every record is handled by one instance that
// holds its segment, in segment order; the processed-event marker skips a duplicate.
func (a *App) consumeProcessor(ctx context.Context, p *processor) {
	settings := a.cfg.events
	wait := settings.resubscribeMin
	for {
		if ctx.Err() != nil {
			return
		}
		started := a.now()
		stable := false
		var failure error
		sub, err := a.cfg.eventStore.Subscribe(ctx, p.subscription)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failure = err
		} else {
			delivered, readErr := a.readProcessor(ctx, p, sub)
			closeEventSubscription(sub)
			if ctx.Err() != nil {
				return
			}
			failure = readErr
			stable = delivered || a.now().Sub(started) >= settings.resubscribeMax
		}
		if stable {
			wait = settings.resubscribeMin
		}
		if failure != nil {
			a.log.Warn("an event processor's subscription failed; subscribing again",
				"processor", p.name, "subscription", p.subscription.Name, "in", wait, "error", failure)
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

// readProcessor reads sub until it fails or ctx ends; delivered reports that it handed out at
// least one record (which resets the re-subscribe backoff).
func (a *App) readProcessor(ctx context.Context, p *processor, sub EventSubscription) (delivered bool, err error) {
	for {
		event, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return delivered, nil
			}
			return delivered, err
		}
		switch event := event.(type) {
		case Assigned:
			a.log.Debug("an event processor was assigned segments",
				"processor", p.name, "segments", event.Segments)
		case Revoked:
			a.log.Debug("an event processor lost segments",
				"processor", p.name, "segments", event.Segments)
		case Delivery:
			delivered = true
			a.handleDelivery(ctx, p, sub, event)
			if ctx.Err() != nil {
				return delivered, nil
			}
		}
	}
}

// handleDelivery handles one delivered event: the events its processor handles, the stage-1 marker
// and the handlers in one transaction, and an acknowledgement only after the commit. An event type
// the processor does not handle, and a malformed record, are acknowledged and run nothing; a
// duplicate marker skips the handlers and is acknowledged.
func (a *App) handleDelivery(ctx context.Context, p *processor, sub EventSubscription, d Delivery) {
	source := p.subscription.Source
	typeName := strings.TrimSpace(d.Headers[HeaderType])
	if typeName == "" {
		a.log.Warn("an event record has no type header; skipping it",
			"processor", p.name, "source", source, "segment", d.Segment, "position", d.Position)
		a.acknowledge(p, sub, d)
		return
	}
	handlers := p.handlers[typeName]
	if len(handlers) == 0 {
		a.log.Debug("an event type has no handler in the processor; acknowledging it",
			"processor", p.name, "type", typeName)
		a.acknowledge(p, sub, d)
		return
	}
	if !isJSONObject(d.Payload) {
		a.log.Warn("an event record's payload is not a JSON object; skipping it",
			"processor", p.name, "type", typeName, "source", source,
			"segment", d.Segment, "position", d.Position)
		a.acknowledge(p, sub, d)
		return
	}
	id := d.Headers[HeaderID]
	if strings.TrimSpace(id) == "" {
		id = source + "-" + d.Segment + "-" + d.Position
	}
	markerID := ProcessorMarker(p.name, id)
	scope := eventHandling(id, typeName, d.Headers)
	settings := a.cfg.events
	for attempt := 1; ; attempt++ {
		done, duplicate, failure := a.handleOnce(ctx, p, markerID, typeName, d, scope)
		a.log.Debug("an event attempt ended", "processor", p.name, "type", typeName, "id", id,
			"attempt", attempt, "done", done, "duplicate", duplicate, "error", failure)
		if duplicate {
			a.log.Debug("a redelivered event was handled before; skipping it",
				"processor", p.name, "type", typeName, "id", id)
			a.acknowledge(p, sub, d)
			return
		}
		if done {
			a.acknowledge(p, sub, d)
			return
		}
		if ctx.Err() != nil {
			// The app is stopping: the record stays unacknowledged for the next subscription.
			return
		}
		var undecodable *decodeError
		if errors.As(failure, &undecodable) {
			// The payload cannot be read as the handler's message type: no retry can change it.
			a.log.Error("an event's payload cannot be read by its handler; skipping the event",
				"processor", p.name, "type", typeName, "id", id, "error", failure)
			a.acknowledge(p, sub, d)
			return
		}
		if attempt >= settings.consumerTries {
			a.log.Error("an event handler failed on every attempt; skipping the event",
				"processor", p.name, "type", typeName, "id", id, "attempts", attempt, "error", failure)
			a.acknowledge(p, sub, d)
			return
		}
		a.log.Warn("an event handler failed; trying again",
			"processor", p.name, "type", typeName, "id", id, "attempt", attempt,
			"in", settings.consumerBackoff, "error", failure)
		if !sleepCtx(ctx, settings.consumerBackoff) {
			return
		}
	}
}

// handleOnce runs one attempt: a transaction, the stage-1 marker, the handlers and the commit.
// done is true when the marker and the handlers committed; duplicate is true when the marker
// existed already (nothing ran and nothing must commit); failure says why the attempt failed.
func (a *App) handleOnce(ctx context.Context, p *processor, markerID, typeName string, d Delivery, scope *handling) (done, duplicate bool, failure error) {
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
