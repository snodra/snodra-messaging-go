package snodra

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// sagaRelay sends what sagas recorded after their transactions committed, one saga's entries in
// seq order under a lease on that saga. Different sagas are sent in parallel by other instances
// (and by other relay runs), never two instances at once for one saga.
type sagaRelay struct {
	app   *App
	owner string
	// slots bounds how many sagas and notices are dispatched at once; inFlight keeps one relay
	// from submitting the same saga or notice twice, and is the except list of DueSagas.
	slots    chan struct{}
	mu       sync.Mutex
	inFlight map[string]bool
	tasks    sync.WaitGroup
}

func newSagaRelay(app *App) *sagaRelay {
	parallelism := app.cfg.events.sagaParallelism
	if parallelism < 1 {
		parallelism = 1
	}
	return &sagaRelay{app: app, owner: uuid.New(), slots: make(chan struct{}, parallelism),
		inFlight: map[string]bool{}}
}

// run polls until ctx ends, handing the due work to the pool without waiting for the sends: one
// saga whose command never answers holds back only itself (until its send bound) and the next
// poll keeps the others moving. The store's wake hook is not used: an entry is due now or at a
// known retry time, and one poll interval is the most a due entry waits.
func (r *sagaRelay) run(ctx context.Context) {
	settings := r.app.cfg.events
	ticker := time.NewTicker(settings.sagaPoll)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if _, err := r.submitDue(ctx); err != nil && ctx.Err() == nil {
			r.app.log.Warn("the saga dispatch poll failed", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	r.tasks.Wait()
}

// poll is one synchronous pass: it submits the due notices and sagas and waits for exactly those
// tasks, for tests and callers that want to know when the work is done.
func (r *sagaRelay) poll(ctx context.Context) error {
	futures, err := r.submitDue(ctx)
	for _, done := range futures {
		<-done
	}
	return err
}

// submitDue delivers the failure notices due first, then sends the due entries of every saga.
// Different sagas are dispatched at the same time (up to the parallelism), one saga's entries
// still in seq order under its lease.
func (r *sagaRelay) submitDue(ctx context.Context) ([]<-chan struct{}, error) {
	settings := r.app.cfg.events
	var futures []<-chan struct{}
	for n := 0; n < settings.sagaBatch; n++ {
		if ctx.Err() != nil {
			return futures, ctx.Err()
		}
		entry, err := r.app.cfg.store.Sagas().Dispatch.ClaimDueNotice(ctx, r.app.now(), r.app.now().Add(settings.sagaLease))
		if err != nil {
			return futures, err
		}
		if entry == nil {
			break
		}
		claimed := *entry
		if done := r.submit(ctx, "notice:"+claimed.ID, func() { r.deliverNotice(ctx, claimed) }); done != nil {
			futures = append(futures, done)
		}
	}
	due, err := r.app.cfg.store.Sagas().Dispatch.DueSagas(ctx, r.app.now(), r.inFlightSagas(), settings.sagaBatch)
	if err != nil {
		return futures, err
	}
	for _, sagaID := range due {
		if ctx.Err() != nil {
			return futures, ctx.Err()
		}
		id := sagaID
		if done := r.submit(ctx, "saga:"+id, func() { r.dispatchSaga(ctx, id) }); done != nil {
			futures = append(futures, done)
		}
	}
	return futures, nil
}

// submit runs work on the pool, unless the same key is in flight already (nil) or ctx ends
// before a slot frees (a done channel closed at once). The returned channel closes when work
// returned.
func (r *sagaRelay) submit(ctx context.Context, key string, work func()) <-chan struct{} {
	r.mu.Lock()
	if r.inFlight[key] {
		r.mu.Unlock()
		return nil
	}
	r.inFlight[key] = true
	r.mu.Unlock()
	done := make(chan struct{})
	r.tasks.Add(1)
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.inFlight, key)
		r.mu.Unlock()
		r.tasks.Done()
		close(done)
		return done
	}
	go func() {
		defer func() {
			<-r.slots
			r.mu.Lock()
			delete(r.inFlight, key)
			r.mu.Unlock()
			r.tasks.Done()
			close(done)
		}()
		work()
	}()
	return done
}

// inFlightSagas is the sagas this relay is dispatching now: DueSagas leaves them out.
func (r *sagaRelay) inFlightSagas() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.inFlight))
	for key := range r.inFlight {
		if id, ok := strings.CutPrefix(key, "saga:"); ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// dispatchSaga sends the pending entries of one saga under its lease, in seq order. It stops at
// the first entry that has to wait for a retry; the saga's later entries wait with it, other
// sagas do not.
func (r *sagaRelay) dispatchSaga(ctx context.Context, sagaID string) {
	settings := r.app.cfg.events
	store := r.app.cfg.store
	now := r.app.now()
	lease := settings.sagaLease
	if settings.sagaSendTimeout > 0 {
		// A send may take up to its bound, plus the guard a hub call waits out; the lease covers
		// it, so another instance cannot send the same saga while this one's call may still run.
		lease = settings.sagaSendTimeout + sagaHubGuard + settings.sagaLease
	}
	claimed, err := store.Sagas().Dispatch.ClaimLease(ctx, sagaID, r.owner, now, now.Add(lease))
	if err != nil || !claimed {
		return
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = store.Sagas().Dispatch.ReleaseLease(releaseCtx, sagaID, r.owner)
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		entry, err := store.Sagas().Dispatch.NextPending(ctx, sagaID)
		if err != nil {
			r.app.log.Warn("could not read a saga's next dispatch entry", "saga", sagaID, "error", err)
			return
		}
		if entry == nil {
			return
		}
		if !entry.NextAttemptAt.IsZero() && entry.NextAttemptAt.After(r.app.now()) {
			return
		}
		if renewed, err := store.Sagas().Dispatch.RenewLease(ctx, sagaID, r.owner, r.app.now().Add(lease)); err != nil || !renewed {
			if err != nil {
				r.app.log.Warn("could not renew a saga's dispatch lease", "saga", sagaID, "error", err)
			}
			return
		}
		if !r.dispatch(ctx, *entry) {
			return
		}
	}
}

// dispatch sends one entry; false when the saga's turn ends (it waits for a retry, or the store
// could not take the outcome).
func (r *sagaRelay) dispatch(ctx context.Context, entry SagaDispatch) bool {
	if entry.Kind == DispatchEvent {
		return r.dispatchEvent(ctx, entry)
	}
	return r.dispatchCommand(ctx, entry)
}

func (r *sagaRelay) dispatchEvent(ctx context.Context, entry SagaDispatch) bool {
	app := r.app
	now := app.now()
	txctx, tx, err := app.cfg.store.Begin(ctx)
	if err != nil {
		app.log.Warn("could not begin a saga event's outbox transaction", "saga", entry.SagaID, "entry", entry.ID, "error", err)
		return false
	}
	correlation, present := entry.Headers[sagaCorrelationHeader]
	if !present {
		correlation = entry.ID
	}
	headers := map[string]string{HeaderCorrelationID: correlation}
	if trace := validTraceparent(entry.Headers); trace != "" {
		headers[HeaderTraceparent] = trace
	}
	out := OutboxEntry{ID: uuid.New(), Type: entry.Type, Key: entry.RoutingKey,
		Headers: headers, Payload: []byte(entry.Payload),
		CreatedAt: now, Seq: app.nextOutboxSeq()}
	if err := app.cfg.store.Outbox().Append(txctx, out); err != nil {
		_ = tx.Rollback(txctx)
		if errors.Is(err, ErrInvalid) {
			return r.invalidEvent(ctx, entry, err)
		}
		app.log.Warn("could not append a saga event to the outbox", "saga", entry.SagaID, "entry", entry.ID, "error", err)
		return false
	}
	if err := app.cfg.store.Sagas().Dispatch.MarkDispatched(txctx, entry.ID, now); err != nil {
		_ = tx.Rollback(txctx)
		app.log.Warn("could not mark a saga event dispatched", "saga", entry.SagaID, "entry", entry.ID, "error", err)
		return false
	}
	if err := tx.Commit(txctx); err != nil {
		_ = tx.Rollback(txctx)
		app.log.Warn("could not commit a saga event to the outbox", "saga", entry.SagaID, "entry", entry.ID, "error", err)
		return false
	}
	return true
}

// invalidEvent gives up an event the outbox refuses. The saga's later entries still go on; an
// event's failure has no notice.
func (r *sagaRelay) invalidEvent(ctx context.Context, entry SagaDispatch, cause error) bool {
	now := r.app.now()
	if err := r.app.cfg.store.Sagas().Dispatch.MarkFailed(ctx, entry.ID, DispatchFailure{
		Reason: FailureInvalid, Message: cause.Error(), FailedAt: now}); err != nil {
		r.app.log.Warn("could not give up an invalid saga event", "entry", entry.ID, "error", err)
		return false
	}
	_ = r.app.cfg.store.Sagas().Dispatch.ClearNotice(ctx, entry.ID)
	r.app.log.Error("a saga event cannot be published and was given up as INVALID",
		"saga", entry.SagaID, "entry", entry.ID, "type", entry.Type, "error", cause)
	return true
}

func (r *sagaRelay) dispatchCommand(ctx context.Context, entry SagaDispatch) bool {
	app := r.app
	correlation, present := entry.Headers[sagaCorrelationHeader]
	if !present {
		correlation = entry.ID
	}
	sendCtx := withHandling(ctx, &handling{info: MessageInfo{
		ID: entry.ID, Type: entry.Type, CorrelationID: correlation,
	}, traceparent: validTraceparent(entry.Headers)})
	bound := app.cfg.events.sagaSendTimeout
	local := app.sagaCommandLocal(entry.Type)
	var cancel context.CancelFunc
	wait := bound
	if bound > 0 {
		// The request's own timeout is the bound. A hub call is waited out for its guard too, so
		// the command is never sent again while a hub may still be running the first attempt.
		if !local {
			wait += sagaHubGuard
		}
		sendCtx, cancel = context.WithTimeout(sendCtx, wait)
		defer cancel()
	}
	headers := map[string]string{
		idempotencyKeyHeader: entry.ID,
		"saga-id":            entry.SagaID,
	}
	err := sendSagaCommand(sendCtx, app, entry, headers, wait, bound)
	if err == nil {
		if err := app.cfg.store.Sagas().Dispatch.MarkDispatched(ctx, entry.ID, app.now()); err != nil {
			app.log.Warn("could not mark a saga command dispatched", "saga", entry.SagaID, "entry", entry.ID, "error", err)
			return false
		}
		return true
	}
	kind, reason, message := classifySagaSend(err)
	if kind == sagaSendTransient {
		attempts := entry.Attempts + 1
		if attempts >= app.cfg.events.sagaDispatchTries {
			return r.fail(ctx, entry, FailureExhausted,
				fmt.Sprintf("gave up after %d attempts, the last failure was: %s", attempts, message), attempts)
		}
		wait := sagaDispatchWait(app.cfg.events, attempts)
		if err := app.cfg.store.Sagas().Dispatch.ScheduleRetry(ctx, entry.ID, attempts, app.now().Add(wait)); err != nil {
			app.log.Warn("could not record a saga command's retry", "saga", entry.SagaID, "entry", entry.ID, "error", err)
			return false
		}
		app.log.Warn("a saga command failed; trying again",
			"saga", entry.SagaID, "entry", entry.ID, "type", entry.Type,
			"attempt", attempts, "next", wait, "error", message)
		return false
	}
	return r.fail(ctx, entry, reason, message, entry.Attempts)
}

// sagaHubGuard is how long past its own bound a call through the hub is waited out: the hub's
// last hop gets its deadline and a second, and the relay adds ten more before it abandons the
// call and retries later with the same idempotency key.
const sagaHubGuard = 11 * time.Second

// sagaCommandLocal reports whether a command would be answered by this service's own handler
// rather than through the hub.
func (a *App) sagaCommandLocal(typ string) bool {
	if a.cfg.alwaysRouteThroughHub {
		return false
	}
	a.mu.Lock()
	h := a.handlers[typ]
	a.mu.Unlock()
	return h != nil && h.kind == kindCommand
}

// sendSagaCommand sends one command entry, bounding the wait. A handler that ignores its context
// cannot hold the saga forever: the relay abandons the call at the bound and retries later with
// the same idempotency key. The goroutine may outlive the bound when the handler ignores its
// context; the library cannot stop a Go call, and the entry's key keeps a later retry harmless.
func sendSagaCommand(ctx context.Context, app *App, entry SagaDispatch, headers map[string]string, wait, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- sendRawCommand(ctx, app, entry.Type, []byte(entry.Payload), derefString(entry.RoutingKey), headers, timeout)
	}()
	if wait <= 0 {
		return <-done
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("%w: %s did not answer within %v", ErrOutcomeUnknown, entry.Type, wait)
	}
}

// fail gives a command up and delivers its pending notice. False when the store could not record
// the failure, so the relay retries it instead of losing the outcome.
func (r *sagaRelay) fail(ctx context.Context, entry SagaDispatch, reason FailureReason, message string, attempts int) bool {
	app := r.app
	now := app.now()
	// The notice is reserved for this relay for one lease (Kotlin's markFailed), so another
	// instance does not claim it while it is being delivered here; a failed delivery moves it to
	// its retry time.
	noticeAt := now.Add(app.cfg.events.sagaLease)
	if err := app.cfg.store.Sagas().Dispatch.MarkFailed(ctx, entry.ID, DispatchFailure{
		Attempts: &attempts, Reason: reason, Message: message, FailedAt: now, NoticeAt: &noticeAt}); err != nil {
		app.log.Warn("could not give up a saga command", "saga", entry.SagaID, "entry", entry.ID, "error", err)
		return false
	}
	app.log.Warn("a saga command was given up", "saga", entry.SagaID, "entry", entry.ID, "type", entry.Type,
		"reason", reason, "message", message)
	claimed := entry
	claimed.Attempts = attempts
	claimed.FailedAt = now
	claimed.FailureReason = reason
	claimed.FailureMessage = message
	claimed.NoticePending = true
	claimed.NoticeAttempts = 0
	claimed.NoticeNextAt = noticeAt
	r.deliverNotice(ctx, claimed)
	return true
}

type sagaSendKind int

const (
	sagaSendPermanent sagaSendKind = iota
	sagaSendTransient
)

// TODO(spec-gap): SG-52 (a permanent local NO_HANDLER needs a service without a hub; the Go
// library requires a hub, so a hub's NO_HANDLER is the transient row and the permanent reason is
// only kept for records from other libraries).
//
// classifySagaSend is the retry taxonomy of a saga command's failed attempt, as the public
// writing-a-library.md table names it.
func classifySagaSend(err error) (sagaSendKind, FailureReason, string) {
	if err == nil {
		return sagaSendPermanent, "", ""
	}
	if errors.Is(err, ErrNeverSent) || errors.Is(err, ErrOutcomeUnknown) {
		return sagaSendTransient, FailureExhausted, err.Error()
	}
	var refused *localRefusal
	if errors.As(err, &refused) {
		// A call refused before anything was built (an invalid option, a reserved header):
		// retrying the same stored command cannot succeed.
		return sagaSendPermanent, FailureInvalid, err.Error()
	}
	var protocol *Error
	if errors.As(err, &protocol) {
		switch protocol.Code {
		case CodeTimeout, CodeHandlerDisconnected, CodeResourceExhausted, CodeNoHandler:
			return sagaSendTransient, FailureExhausted, protocol.Message
		case CodeUnauthenticated, CodePermissionDenied:
			return sagaSendPermanent, FailureDenied, protocol.Message
		case CodeInvalidRequest:
			return sagaSendPermanent, FailureInvalid, protocol.Message
		case CodeHandlerError:
			switch strings.TrimSpace(string(protocol.Details)) {
			case `{"snodra":"retryable"}`:
				return sagaSendTransient, FailureExhausted, protocol.Message
			case `{"snodra":"unknown-outcome"}`:
				return sagaSendPermanent, FailureUnknownOutcome, protocol.Message
			default:
				return sagaSendPermanent, FailureFailed, protocol.Message
			}
		case CodeCancelled:
			return sagaSendPermanent, FailureFailed, protocol.Message
		}
	}
	return sagaSendTransient, FailureExhausted, err.Error()
}

// sagaDispatchWait is the wait before attempt n+1: 1 s doubling to 5 min, jittered by ±20 % and
// never above the maximum.
func sagaDispatchWait(settings eventSettings, attempts int) time.Duration {
	wait := settings.sagaDispatchWait
	for i := 1; i < attempts; i++ {
		wait *= 2
		if wait >= settings.sagaDispatchMax {
			return jitteredWait(settings.sagaDispatchMax, settings.sagaDispatchMax)
		}
	}
	return jitteredWait(wait, settings.sagaDispatchMax)
}

// jitteredWait applies the references' ±20 % jitter, never above max.
func jitteredWait(wait, max time.Duration) time.Duration {
	if wait <= 0 {
		return 0
	}
	wait = time.Duration(float64(wait) * (0.8 + 0.4*rand.Float64()))
	if max > 0 && wait > max {
		wait = max
	}
	return wait
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
