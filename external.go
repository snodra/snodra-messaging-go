package snodra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

const (
	// idempotencyKeyHeader is the header a saga's command carries and an idempotent adapter uses
	// by default: the caller's key for one external call.
	idempotencyKeyHeader = "idempotency-key"
	// externalCallLease bounds one adapter call: a record still IN_PROGRESS past it belongs to a
	// call that crashed, and its outcome is unknown.
	externalCallLease = 5 * time.Minute
	// externalRetryableMarker and externalUnknownOutcomeMarker are the two reserved
	// Error.details values; the bytes are part of the protocol.
	externalRetryableMarker      = `{"snodra":"retryable"}`
	externalUnknownOutcomeMarker = `{"snodra":"unknown-outcome"}`
)

var (
	externalRetryableJSON      = json.RawMessage(externalRetryableMarker)
	externalUnknownOutcomeJSON = json.RawMessage(externalUnknownOutcomeMarker)
)

// externalMarkerError is a handler failure that forces HANDLER_ERROR with one of the two
// reserved details values, whatever it wraps.
type externalMarkerError struct {
	msg     string
	details json.RawMessage
	cause   error
}

func (e *externalMarkerError) Error() string { return e.msg }
func (e *externalMarkerError) Unwrap() error { return e.cause }

// Retryable marks err as a failure a later attempt may cure: the answer is HANDLER_ERROR with
// details exactly `{"snodra":"retryable"}`. Use it in an idempotent adapter (or any handler whose
// failure the saga relay should retry) and do not retry inside the handler: the relay owns the
// backoff. A nil err still produces the marker with a generic message.
func Retryable(err error) error {
	msg := "the external call failed and may be retried"
	if err != nil {
		msg = err.Error()
	}
	return &externalMarkerError{msg: msg, details: externalRetryableJSON, cause: err}
}

// UnknownOutcome marks err as an external call whose outcome cannot be known: the answer is
// HANDLER_ERROR with details exactly `{"snodra":"unknown-outcome"}`, and a saga reports it as
// UNKNOWN_OUTCOME instead of retrying automatically. Use it when the request may or may not have
// reached the external system. A nil err still produces the marker with a generic message.
func UnknownOutcome(err error) error {
	msg := "the outcome of the external call is unknown"
	if err != nil {
		msg = err.Error()
	}
	return &externalMarkerError{msg: msg, details: externalUnknownOutcomeJSON, cause: err}
}

// IsRetryable reports whether err carries the retryable marker, locally (Retryable, a handler
// error whose details are the marker) or from a hub (an *Error's Details).
func IsRetryable(err error) bool { return hasExternalMarker(err, externalRetryableMarker) }

// IsUnknownOutcome reports whether err carries the unknown-outcome marker, locally or from a hub.
func IsUnknownOutcome(err error) bool { return hasExternalMarker(err, externalUnknownOutcomeMarker) }

// hasExternalMarker reads the marker from the nearest marker error, a handler error's details or a
// hub Error's details, trimming surrounding whitespace as the relay does.
func hasExternalMarker(err error, marker string) bool {
	if err == nil {
		return false
	}
	var marked *externalMarkerError
	if errors.As(err, &marked) {
		return strings.TrimSpace(string(marked.details)) == marker
	}
	var handler *handlerError
	if errors.As(err, &handler) && handler.details != nil {
		return strings.TrimSpace(string(handler.details)) == marker
	}
	var protocol *Error
	if errors.As(err, &protocol) && protocol.Details != nil {
		return strings.TrimSpace(string(protocol.Details)) == marker
	}
	return false
}

// ExternalIdempotent says the external partner de-duplicates by the idempotency key. A lapsed
// IN_PROGRESS call (its lease ran out) is then taken over and its body retried; without it, the
// outcome is unknown and the body is not run again. It applies to [HandleIdempotentCommand].
func ExternalIdempotent() HandlerOption {
	return func(c *handlerConfig) { c.externalIdempotent = true }
}

// HandleIdempotentCommand registers h as the idempotent handler of the command type C, by C's
// MessageType. Each call is recorded in the message store's external calls under the
// `idempotency-key` header (which a saga's relay sets to `<sagaId>:<seq>`): the first insert wins,
// a repeat after success returns the first reply without running h, a repeat after a permanent
// failure returns the stored error, and a repeat after an unknown outcome reports it again.
//
// A body that fails with [Retryable] resets the record so the next call runs it again (the saga
// relay owns the backoff); [UnknownOutcome] leaves the record UNKNOWN, so no later call runs the
// body automatically. Any other failure is stored FAILED with HANDLER_ERROR and its message, and
// replayed. A call still IN_PROGRESS within its lease asks the caller to come back ([Retryable]);
// past the lease it is given up as UNKNOWN unless [ExternalIdempotent] was given, in which case it
// is taken over and the body runs again. Every write matches the record the call owns or read, so
// a call whose lease ran out never overwrites the record of the call that took it over.
//
// The records are written in short writes of their own, outside the caller's transaction: a
// rolled-back caller never forgets a call that happened. The handler needs a message store (Start
// refuses one without) and is registered like [HandleCommand] (guards apply, Shared is refused).
func HandleIdempotentCommand[C Message, R any](app *App, h func(context.Context, C) (R, error), opts ...HandlerOption) error {
	if app == nil {
		return errors.New("snodra: registration on a nil App")
	}
	name, cfg, err := handlerSetup[C](kindCommand, h == nil, opts)
	handler := &handler{kind: kindCommand, name: name, idempotent: true, cfg: cfg}
	if err == nil {
		handler.invoke = idempotentInvoker(app, name, h, cfg.externalIdempotent)
	}
	return app.addHandler(handler, err)
}

// idempotentInvoker decodes the command, admits the caller, reads the key and runs the body under
// the external-call record.
func idempotentInvoker[C Message, R any](app *App, name string, h func(context.Context, C) (R, error), partnerIdempotent bool) invoker {
	return func(ctx context.Context, codec PayloadCodec, payload []byte, admit func() error) ([]byte, error) {
		msg, err := decodeMessage[C](codec, payload)
		if err != nil {
			return nil, err
		}
		if admit != nil {
			if err := admit(); err != nil {
				return nil, err
			}
		}
		info, ok := MessageFrom(ctx)
		if !ok {
			return nil, fmt.Errorf("snodra: the idempotent command %s runs outside a handler", name)
		}
		// TODO(spec-gap): SG-53 (the key is the idempotency-key header; Kotlin's
		// @IdempotentHandler key expression over the message and headers is not exposed yet).
		key := info.Headers[idempotencyKeyHeader]
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("snodra: the idempotent command %s needs an %s header", name, idempotencyKeyHeader)
		}
		return runIdempotentCall(app, ctx, name, key, partnerIdempotent, func() (R, error) {
			return h(ctx, msg)
		})
	}
}

// runIdempotentCall runs one call under its record: a replay from the record, or the body and its
// settlement. The writes are the store's own (the external-call port never joins a transaction).
func runIdempotentCall[R any](a *App, ctx context.Context, name, key string, partnerIdempotent bool, body func() (R, error)) ([]byte, error) {
	if a.cfg.store == nil {
		return nil, fmt.Errorf("snodra: the idempotent command %s needs the message store's external calls port", name)
	}
	calls := a.cfg.store.ExternalCalls()
	if calls == nil {
		return nil, fmt.Errorf("snodra: the idempotent command %s needs the message store's external calls port", name)
	}
	owner, replay, err := a.beginExternalCall(ctx, calls, name, key, partnerIdempotent)
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return replay, nil
	}
	result, err := body()
	if err != nil {
		a.settleExternalFailure(ctx, calls, name, key, owner, err)
		return nil, err
	}
	reply, err := encodeResult(a.cfg.codec, result)
	if err != nil {
		return nil, a.unrecordedExternalCall(ctx, calls, name, key, owner, partnerIdempotent, err)
	}
	ok, err := calls.Succeed(ctx, key, owner, string(reply), a.now())
	if err != nil || !ok {
		cause := err
		if cause == nil {
			cause = errors.New("the record is no longer owned by this call")
		}
		return nil, a.unrecordedExternalCall(ctx, calls, name, key, owner, partnerIdempotent, cause)
	}
	return reply, nil
}

// beginExternalCall inserts or follows a record until this call owns it or the record answers.
//
// It returns the owner token for the body's settlement, or a stored reply to return without
// running the body. A record that changes under the call is read again, at most three rounds.
func (a *App) beginExternalCall(ctx context.Context, calls ExternalCallStore, name, key string, partnerIdempotent bool) (string, []byte, error) {
	for round := 0; round < 3; round++ {
		now := a.now()
		owner := uuid.New()
		leaseUntil := now.Add(externalCallLease)
		inserted, err := calls.Insert(ctx, ExternalCall{
			Key: key, Status: ExternalCallInProgress, Attempts: 1,
			UpdatedAt: now, LeaseUntil: leaseUntil, Owner: owner,
		})
		if err != nil {
			return "", nil, err
		}
		if inserted {
			return owner, nil, nil
		}
		record, err := calls.Find(ctx, key)
		if err != nil {
			return "", nil, err
		}
		if record == nil {
			continue // reset or expired meanwhile: insert again
		}
		switch record.Status {
		case ExternalCallSucceeded:
			reply := record.Reply
			if reply == "" {
				reply = "{}"
			}
			return "", []byte(reply), nil
		case ExternalCallFailed:
			return "", nil, errors.New(externalStoredMessage(record))
		case ExternalCallUnknown:
			return "", nil, UnknownOutcome(fmt.Errorf(
				"the outcome of the call with key %q is unknown: an earlier call of %s did not finish and the external system may have acted", key, name))
		case ExternalCallInProgress:
			if record.LeaseUntil.After(now) {
				return "", nil, Retryable(fmt.Errorf("the call with key %q is already in progress (command %s)", key, name))
			}
			if partnerIdempotent {
				taken, err := calls.TakeOver(ctx, *record, owner, leaseUntil, now)
				if err != nil {
					return "", nil, err
				}
				if taken != nil {
					return owner, nil, nil
				}
				continue // another call changed the record: read it again
			}
			expired, err := calls.Expire(ctx, *record, now)
			if err != nil {
				return "", nil, err
			}
			if expired {
				return "", nil, UnknownOutcome(fmt.Errorf(
					"the outcome of the call with key %q is unknown: an earlier call of %s did not finish and the external system may have acted", key, name))
			}
			continue // another call changed the record: read it again
		default:
			return "", nil, fmt.Errorf("snodra: the external call record %q has status %q", key, record.Status)
		}
	}
	return "", nil, Retryable(fmt.Errorf("the call with key %q kept changing under this call (command %s)", key, name))
}

// settleExternalFailure records a body's failure: reset for a retryable one, UNKNOWN for an
// unknown outcome, FAILED for anything else. Its own errors are logged and never mask the body's.
func (a *App) settleExternalFailure(ctx context.Context, calls ExternalCallStore, name, key, owner string, bodyErr error) {
	now := a.now()
	switch {
	case IsRetryable(bodyErr):
		ok, err := calls.Reset(ctx, key, owner)
		if err != nil || !ok {
			a.log.Warn("could not reset the external call after a retryable failure",
				"command", name, "key", key, "reset", ok, "error", err)
		}
	case IsUnknownOutcome(bodyErr):
		ok, err := calls.MarkUnknown(ctx, key, owner, now)
		if err != nil || !ok {
			a.log.Warn("could not mark the external call unknown",
				"command", name, "key", key, "marked", ok, "error", err)
		}
	default:
		raw, _ := json.Marshal(struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{Code: "HANDLER_ERROR", Message: bodyErr.Error()})
		ok, err := calls.Fail(ctx, key, owner, string(raw), now)
		if err != nil || !ok {
			a.log.Warn("could not record the external call's failure",
				"command", name, "key", key, "failed", ok, "error", err)
		}
	}
}

// unrecordedExternalCall is a body that returned (or a reply that could not be encoded) whose
// record cannot say so. With a de-duplicating partner the next attempt is safe and transient;
// otherwise the outcome is unknown and the body is not run again automatically.
func (a *App) unrecordedExternalCall(ctx context.Context, calls ExternalCallStore, name, key, owner string, partnerIdempotent bool, cause error) error {
	a.log.Error("the external call ran but its outcome could not be recorded",
		"command", name, "key", key, "error", cause)
	if partnerIdempotent {
		return Retryable(fmt.Errorf(
			"the call with key %q (command %s) ran but its outcome could not be recorded; a repeat is safe because the partner de-duplicates by the key: %w",
			key, name, cause))
	}
	if ok, err := calls.MarkUnknown(ctx, key, owner, a.now()); err != nil || !ok {
		a.log.Warn("could not mark the unrecorded external call unknown",
			"command", name, "key", key, "marked", ok, "error", err)
	}
	return UnknownOutcome(fmt.Errorf(
		"the call with key %q (command %s) ran but its outcome could not be recorded", key, name))
}

// externalStoredMessage is the message of a stored FAILED record, else a fallback naming the key.
func externalStoredMessage(record *ExternalCall) string {
	var stored struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(record.Error), &stored); err == nil && stored.Message != "" {
		return stored.Message
	}
	return fmt.Sprintf("the call with key %q failed earlier", record.Key)
}
