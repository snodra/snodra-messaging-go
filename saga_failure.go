package snodra

import "context"

// SagaCommandFailedReason says why a command a saga sent was given up on.
type SagaCommandFailedReason string

const (
	// SagaCommandFailedNoHandler: nobody handles the command type (a service without a hub).
	SagaCommandFailedNoHandler SagaCommandFailedReason = "NO_HANDLER"
	// SagaCommandFailedDenied: the handler requires an authenticated caller, and a saga command
	// has none.
	SagaCommandFailedDenied SagaCommandFailedReason = "DENIED"
	// SagaCommandFailedInvalid: the handler could not read the payload.
	SagaCommandFailedInvalid SagaCommandFailedReason = "INVALID"
	// SagaCommandFailedFailed: the handler failed (any other error).
	SagaCommandFailedFailed SagaCommandFailedReason = "FAILED"
	// SagaCommandFailedUnknownOutcome: an idempotent adapter cannot tell whether its external
	// call happened.
	SagaCommandFailedUnknownOutcome SagaCommandFailedReason = "UNKNOWN_OUTCOME"
	// SagaCommandFailedExhausted: a transient failure lasted through all the relay's attempts.
	SagaCommandFailedExhausted SagaCommandFailedReason = "EXHAUSTED"
)

// String returns the stored reason.
func (r SagaCommandFailedReason) String() string { return string(r) }

const commandFailedTypeName = "snodra.SagaCommandFailed"

// SagaCommandFailed is the local event a saga receives when a command it recorded was given up
// on. It is never published to the event store. Its message id is "failed:<sagaId>:<seq>", from
// which its processed-event markers are built; it reaches the instance that sent the command by
// SagaID, without an association.
type SagaCommandFailed struct {
	// SagaID is the id of the saga that sent the command.
	SagaID string `json:"sagaId"`
	// CommandType is the command's wire type name.
	CommandType string `json:"commandType"`
	// IdempotencyKey is the failed command's entry id, "<sagaId>:<seq>".
	IdempotencyKey string `json:"idempotencyKey"`
	// Reason is why the command was given up.
	Reason SagaCommandFailedReason `json:"reason"`
	// Message is why, for people.
	Message string `json:"message"`
}

// MessageType is the event's wire type name.
func (SagaCommandFailed) MessageType() string { return commandFailedTypeName }

// deliverNotice delivers the pending failure notice of a claimed dispatch entry, or clears the
// claim when the entry has none. A notice that cannot be delivered is retried with the command's
// backoff and attempts; after the last attempt it is logged and dropped.
func (r *sagaRelay) deliverNotice(ctx context.Context, entry SagaDispatch) {
	if entry.Kind != DispatchCommand || entry.FailureReason == "" || !entry.NoticePending {
		_ = r.app.cfg.store.Sagas().Dispatch.ClearNotice(ctx, entry.ID)
		return
	}
	app := r.app
	failed := SagaCommandFailed{SagaID: entry.SagaID, CommandType: entry.Type,
		IdempotencyKey: entry.ID, Reason: SagaCommandFailedReason(entry.FailureReason),
		Message: entry.FailureMessage}
	payload, err := app.cfg.codec.Marshal(failed)
	if err != nil {
		_ = app.cfg.store.Sagas().Dispatch.ClearNotice(ctx, entry.ID)
		return
	}
	messageID := "failed:" + entry.ID
	correlation, present := entry.Headers[sagaCorrelationHeader]
	if !present {
		correlation = entry.ID
	}
	headers := map[string]string{HeaderID: messageID, HeaderType: commandFailedTypeName, HeaderCorrelationID: correlation}
	if err := app.dispatchLocalEvent(ctx, commandFailedTypeName, payload, messageID, correlation, "", headers); err != nil {
		attempts := entry.NoticeAttempts + 1
		if attempts >= app.cfg.events.sagaDispatchTries {
			app.log.Error("gave up delivering a saga command failure notice", "saga", entry.SagaID,
				"entry", entry.ID, "attempts", attempts, "error", err)
			_ = app.cfg.store.Sagas().Dispatch.ClearNotice(ctx, entry.ID)
			return
		}
		next := app.now().Add(sagaDispatchWait(app.cfg.events, attempts))
		_ = app.cfg.store.Sagas().Dispatch.ScheduleNoticeRetry(ctx, entry.ID, attempts, next)
		app.log.Warn("delivering a saga command failure notice failed; trying again",
			"saga", entry.SagaID, "entry", entry.ID, "attempt", attempts, "next", next, "error", err)
		return
	}
	_ = app.cfg.store.Sagas().Dispatch.ClearNotice(ctx, entry.ID)
}

// dispatchLocalEvent runs a local event through the ordinary event handlers and the saga stage,
// each with its processed-event marker, exactly once. It is how a saga command's failure notice
// reaches the service; the event is never published.
func (a *App) dispatchLocalEvent(ctx context.Context, typeName string, payload []byte, messageID, correlation, trace string, headers map[string]string) error {
	handled := false
	for _, p := range a.processors {
		if len(p.handlers[typeName]) == 0 {
			continue
		}
		txctx, tx, err := a.cfg.store.Begin(ctx)
		if err != nil {
			return err
		}
		marker := ProcessorMarker(p.name, messageID)
		recorded, err := a.cfg.store.Processed().Record(txctx, ProcessedMarker{ID: marker, Type: typeName,
			ProcessedAt: a.now(), Processor: p.name})
		if err != nil {
			_ = tx.Rollback(txctx)
			return err
		}
		if recorded {
			scope := eventHandling(messageID, typeName, headers)
			scope.traceparent = trace
			handlerCtx := withHandling(txctx, scope)
			for _, handler := range p.handlers[typeName] {
				if err := handler.call(handlerCtx, a.cfg.codec, payload); err != nil {
					_ = tx.Rollback(txctx)
					return err
				}
			}
			if err := tx.Commit(txctx); err != nil {
				return err
			}
		}
		handled = true
	}
	if a.sagaRuntime != nil && a.sagaRuntime.interested(typeName) {
		msg := sagaMessage{id: messageID, typeName: typeName, correlationID: correlation,
			traceparent: trace, headers: headers}
		reached, err := a.sagaRuntime.process(ctx, msg, payload)
		if err != nil {
			return err
		}
		handled = handled || reached
	}
	if !handled {
		a.log.Error("a saga command failure notice reached no handler", "type", typeName, "message", messageID)
	}
	return nil
}
