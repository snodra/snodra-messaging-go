package snodra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/uuid"
)

// The relay's send timeout is a specification gap: the pages say a lease holds a claimed batch
// and that a send may fail, not how long one send may take nor how much of the lease it must
// leave free. Go follows the reference implementations (Kotlin OutboxPublisher.SEND_TIMEOUT 45 s,
// Rust OUTBOX_SEND_TIMEOUT 40 s) with 40 s and stops a batch one send timeout before its lease
// ends.
//
// TODO(spec-gap): SG-32 (the send timeout, the lease margin and the lease's lower bound).

// outboxRelay publishes one service's outbox to its event store. Every instance runs one unless
// configured off; several instances compete for the entries without a leader, each holding what
// it claimed under a lease.
//
//   - A batch is claimed in (CreatedAt, Seq) order and sent one entry after the other, so order
//     per key holds for what one instance publishes while one relay sends.
//   - A rejected event is given up at once; a failed one counts an attempt and ends the batch, so
//     the next claim retries it before the later entries. The wait before that retry is the poll
//     interval (a committed append may wake the relay sooner, which is harmless: the failed entry
//     still comes first).
//   - The relay publishes, then marks sent. A crash between the two leaves the entry pending:
//     after the lease it is claimed again and published again, and consumers skip the duplicate
//     by its processed-event marker. Nothing is lost.
//   - A batch stops before its lease could run out under a send, so a slow event store never
//     makes an entry claimable by another instance while this one still sends it.
type outboxRelay struct {
	log     *slog.Logger
	outbox  OutboxStore
	events  EventPublisher
	source  string
	service string
	owner   string
	// settings
	poll        time.Duration
	batch       int
	maxAttempts int
	lease       time.Duration
	sendTimeout time.Duration
	now         func() time.Time
}

// newOutboxRelay is the relay of app's outbox to its event store.
func newOutboxRelay(app *App) *outboxRelay {
	s := app.cfg.events
	return &outboxRelay{
		log:         app.log,
		outbox:      app.cfg.store.Outbox(),
		events:      app.cfg.eventStore,
		source:      s.source,
		service:     app.cfg.service,
		owner:       uuid.New(),
		poll:        s.poll,
		batch:       s.batch,
		maxAttempts: s.outboxAttempts,
		lease:       s.lease,
		sendTimeout: s.sendTimeout,
		now:         app.now,
	}
}

// run relays until ctx is done: a poll, a committed append's wake, or the next poll after a full
// batch.
func (r *outboxRelay) run(ctx context.Context) {
	wake := make(chan struct{}, 1)
	cancel := r.outbox.OnAppend(func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	if cancel != nil {
		defer cancel()
	}
	for {
		if ctx.Err() != nil {
			return
		}
		sent, _ := r.relayOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if sent >= r.batch {
			continue
		}
		timer := time.NewTimer(r.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// relayOnce claims one batch and sends it in order; sent is how many entries it marked sent, and
// retriable is true when a failure ended the batch and the entry waits to be tried again. Nothing
// is lost when the process ends here: an entry is pending until MarkSent returns.
func (r *outboxRelay) relayOnce(ctx context.Context) (sent int, retriable bool) {
	claimedAt := r.now()
	batch, err := r.outbox.Claim(ctx, r.owner, r.lease, r.batch)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("the outbox could not be claimed; trying again at the next poll", "error", err)
		}
		return 0, false
	}
	// A send started from here on could outlast the lease: the rest of the batch waits for the
	// next claim, which renews the lease of what this relay still holds.
	sendUntil := claimedAt.Add(r.lease - r.sendTimeout)
	for _, entry := range batch {
		if ctx.Err() != nil {
			return sent, false
		}
		if !r.now().Before(sendUntil) {
			r.log.Debug("the outbox batch stops before its lease could run out", "sent", sent,
				"left", len(batch)-sent)
			return sent, false
		}
		err := r.publish(ctx, entry)
		if err == nil {
			if err := r.outbox.MarkSent(ctx, []string{entry.ID}); err != nil {
				if ctx.Err() == nil {
					// The event is out; only its mark is missing. It is published again after
					// the lease, and consumers skip the duplicate.
					r.log.Error("an outbox entry was published but could not be marked sent; it will be sent again",
						"id", entry.ID, "type", entry.Type, "error", err)
				}
				return sent, false
			}
			sent++
			continue
		}
		attempt := entry.Attempts + 1
		if !retriableSend(err) || attempt >= r.maxAttempts {
			r.log.Error("an outbox entry was given up", "id", entry.ID, "type", entry.Type,
				"attempts", attempt, "error", err)
			if markErr := r.outbox.MarkFailed(ctx, entry.ID, err.Error()); markErr != nil && ctx.Err() == nil {
				r.log.Error("an outbox entry could not be marked failed", "id", entry.ID, "error", markErr)
				return sent, false
			}
			continue
		}
		r.log.Warn("an outbox send failed; it is tried again before the later entries",
			"id", entry.ID, "type", entry.Type, "attempt", attempt, "error", err)
		if markErr := r.outbox.MarkAttemptFailed(ctx, entry.ID, err.Error()); markErr != nil && ctx.Err() == nil {
			r.log.Error("an outbox entry could not count its failed attempt", "id", entry.ID, "error", markErr)
		}
		return sent, true
	}
	return sent, false
}

// publish sends one entry to the event store, bounded by the send timeout. A timeout counts as a
// failed send; the caller's own cancellation is returned as it is.
func (r *outboxRelay) publish(ctx context.Context, entry OutboxEntry) error {
	event := r.outgoing(entry)
	pctx, cancel := context.WithTimeout(ctx, r.sendTimeout)
	defer cancel()
	err := r.events.Publish(pctx, event)
	if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: no answer from the event store within %v", ErrFailed, r.sendTimeout)
	}
	return err
}

// outgoing is the event entry becomes: its stored headers, with type, id, timestamp (its
// creation) and source (the publishing service) set from the entry.
func (r *outboxRelay) outgoing(entry OutboxEntry) OutgoingEvent {
	headers := copyStringMap(entry.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	headers[HeaderType] = entry.Type
	headers[HeaderID] = entry.ID
	headers[HeaderTimestamp] = eventTimestamp(entry.CreatedAt)
	headers[HeaderSource] = r.service
	return OutgoingEvent{
		Source:  r.source,
		ID:      entry.ID,
		Type:    entry.Type,
		Key:     copyStringPointer(entry.Key),
		Headers: headers,
		Payload: append([]byte(nil), entry.Payload...),
	}
}

// retriableSend reports whether another publish of an event may succeed: not after the event store
// rejected it for good or found it invalid.
func retriableSend(err error) bool {
	return !errors.Is(err, ErrRejected) && !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrUnsupported)
}
