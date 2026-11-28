package snodra

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// deadlineScheduler fires the due deadlines of the saga types in this service. Every instance
// runs one; a deadline is claimed with a lease, so it fires on one instance and not on each.
type deadlineScheduler struct {
	app   *App
	owner string
}

func newDeadlineScheduler(app *App) *deadlineScheduler {
	return &deadlineScheduler{app: app, owner: uuid.New()}
}

func (s *deadlineScheduler) run(ctx context.Context) {
	settings := s.app.cfg.events
	ticker := time.NewTicker(settings.deadlinePoll)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// poll claims and fires due deadlines until none is due. A firing that fails leaves its claim to
// expire and fires again after the lease, so a deadline fires at least once.
func (s *deadlineScheduler) poll(ctx context.Context) {
	for n := 0; n < s.app.cfg.events.sagaBatch; n++ {
		if ctx.Err() != nil {
			return
		}
		now := s.app.now()
		deadline, err := s.app.cfg.store.Sagas().Deadlines.ClaimDue(ctx, s.owner, now, now.Add(s.app.cfg.events.deadlineLease))
		if err != nil {
			if ctx.Err() == nil {
				s.app.log.Warn("the deadline scheduler could not claim a deadline", "error", err)
			}
			return
		}
		if deadline == nil {
			return
		}
		if err := s.apply(ctx, *deadline); err != nil && ctx.Err() == nil {
			s.app.log.Warn("firing a deadline failed; its claim will lapse and it will fire again",
				"saga", deadline.SagaType, "deadline", deadline.ID, "name", deadline.Name, "error", err)
		}
	}
}

// apply retries one claimed deadline like an event: conflicts up to the stage's attempts with its
// backoff, other failures twice quickly. When every attempt failed, the claim is left to expire.
func (s *deadlineScheduler) apply(ctx context.Context, claimed SagaDeadline) error {
	typ := s.app.sagaRuntime.byName[claimed.SagaType]
	if typ == nil {
		return fmt.Errorf("snodra: deadline %s belongs to saga type %q, which this service does not have", claimed.ID, claimed.SagaType)
	}
	settings := s.app.cfg.events
	made, conflicts, failures := 0, 0, 0
	for {
		made++
		fired, err := s.fire(ctx, typ, claimed)
		if err == nil {
			if fired {
				s.app.log.Debug("a saga deadline fired", "saga", typ.name, "deadline", claimed.ID, "name", claimed.Name)
			}
			return nil
		}
		if errors.Is(err, ErrConflict) {
			conflicts++
			if made >= settings.sagaAttempts {
				return fmt.Errorf("gave up firing deadline %s after %d attempts: %w", claimed.ID, made, err)
			}
			if !settings.sleep(ctx, settings.sagaBackoff(conflicts+1)) {
				return ctx.Err()
			}
			continue
		}
		failures++
		if failures >= min(settings.sagaHandlerAttempts, settings.sagaAttempts) {
			return fmt.Errorf("gave up firing deadline %s after %d attempts: %w", claimed.ID, made, err)
		}
		if !settings.sleep(ctx, settings.sagaHandlerPause) {
			return ctx.Err()
		}
	}
}

// fire runs one claimed deadline in one transaction: its handler, the state write and FIRED. A
// deadline no longer SCHEDULED is left alone; one whose saga is gone or ended, or whose name has
// no handler, is cancelled.
func (s *deadlineScheduler) fire(ctx context.Context, typ *sagaTypeRuntime, claimed SagaDeadline) (fired bool, err error) {
	app := s.app
	txctx, tx, err := app.cfg.store.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(txctx) }()
	defer func() {
		if rec := recover(); rec != nil {
			fired = false
			err = fmt.Errorf("snodra: firing deadline %s panicked: %v", claimed.ID, rec)
		}
	}()
	now := app.now()
	scheduled, err := app.cfg.store.Sagas().Deadlines.FindScheduled(txctx, claimed.ID)
	if err != nil {
		return false, err
	}
	if scheduled == nil {
		return false, nil // cancelled or fired since the scheduler claimed it
	}
	doc, err := app.cfg.store.Sagas().Sagas.FindByID(txctx, claimed.SagaID)
	if err != nil {
		return false, err
	}
	if doc == nil || doc.Ended {
		if err := app.cfg.store.Sagas().Deadlines.Cancel(txctx, claimed.SagaID, claimed.ID, now); err != nil {
			return false, err
		}
		return false, tx.Commit(txctx)
	}
	handler := typ.deadlines[claimed.Name]
	if handler == nil {
		app.log.Error("a saga deadline has no handler; cancelling it",
			"saga", typ.name, "deadline", claimed.ID, "name", claimed.Name)
		if err := app.cfg.store.Sagas().Deadlines.Cancel(txctx, claimed.SagaID, claimed.ID, now); err != nil {
			return false, err
		}
		return false, tx.Commit(txctx)
	}
	state, err := typ.decodeState(doc.State)
	if err != nil {
		return false, err
	}
	var payload []byte
	if claimed.Payload != nil {
		payload = []byte(*claimed.Payload)
	}
	correlation := claimed.CorrelationID
	if correlation == "" {
		correlation = claimed.ID
	}
	saga := &SagaContext{app: app, sagaType: typ.name, id: doc.ID, version: doc.Version + 1,
		correlationID: correlation, now: now, associations: append([]SagaAssociation(nil), doc.Associations...)}
	// A deadline's work carries the scheduling message's correlation id and no trace context.
	scope := &handling{info: MessageInfo{ID: claimed.ID, Type: "deadline:" + claimed.Name,
		CorrelationID: correlation, Headers: map[string]string{HeaderCorrelationID: correlation}}}
	handlerCtx := withHandling(txctx, scope)
	if err := callSagaHandler("deadline", claimed.Name, func() error {
		return handler.invoke(handlerCtx, saga, state, payload)
	}); err != nil {
		return false, err
	}
	if handler.end {
		saga.end = true
	}
	encoded, err := typ.encodeState(state)
	if err != nil {
		return false, err
	}
	keptStart := ""
	if doc.StartKey != nil {
		for _, association := range saga.associations {
			if sagaStartKey(typ.name, association.Key, association.Value) == *doc.StartKey {
				keptStart = *doc.StartKey
				break
			}
		}
	}
	next := *doc
	next.State = encoded
	next.Associations = saga.associations
	next.Version = doc.Version + 1
	next.UpdatedAt = now
	next.Ended = saga.end
	if saga.end {
		next.EndedAt = now
	}
	if keptStart == "" {
		next.StartKey = nil
	}
	if ok, err := app.cfg.store.Sagas().Sagas.Update(txctx, next, doc.Version); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("%w: saga %s changed while its deadline fired", ErrConflict, doc.ID)
	}
	for _, op := range saga.deadlineOps {
		switch {
		case op.schedule != nil:
			if err := app.cfg.store.Sagas().Deadlines.Insert(txctx, *op.schedule); err != nil {
				return false, err
			}
		case op.cancelID != "":
			if err := app.cfg.store.Sagas().Deadlines.Cancel(txctx, doc.ID, op.cancelID, now); err != nil {
				return false, err
			}
		case op.cancelName != "":
			if err := app.cfg.store.Sagas().Deadlines.CancelByName(txctx, doc.ID, op.cancelName, now); err != nil {
				return false, err
			}
		}
	}
	if len(saga.entries) > 0 {
		if err := app.cfg.store.Sagas().Dispatch.Record(txctx, saga.entries); err != nil {
			return false, err
		}
	}
	if ok, err := app.cfg.store.Sagas().Deadlines.MarkFired(txctx, claimed.ID, now); err != nil {
		return false, err
	} else if !ok {
		return false, fmt.Errorf("%w: deadline %s changed while firing", ErrConflict, claimed.ID)
	}
	if saga.end {
		if err := app.cfg.store.Sagas().Deadlines.CancelAll(txctx, doc.ID, now); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(txctx); err != nil {
		return false, err
	}
	return true, nil
}
