package snodra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// maxControlTextBytes is where a control reply's texts (messages and details) end; the protocol
// bounds them like a status.
const maxControlTextBytes = 512

// maxControlTypeBytes is where a control reply's type names end (the protocol bounds types at
// 256; ids and keys are sent as they are, so a cut id never breaks a retry).
const maxControlTypeBytes = 256

// maxDeadLetterList is the most letters a DeadLetterList may ask for.
const maxDeadLetterList = 100

// controlPlane answers the hub's processor controls: every request runs off the stream's read
// loop, one at a time per processor, and its one reply goes back on the stream the request
// arrived on.
type controlPlane struct {
	app *App
	ctx context.Context
	mu  sync.Mutex
	// queues holds one serial queue per processor this instance runs.
	queues map[string]*controlQueue
}

// controlQueue is one processor's control requests: a mutex-guarded list and a wake signal, so
// enqueueing never blocks the read loop, however many requests wait.
type controlQueue struct {
	mu      sync.Mutex
	pending []controlCall
	wake    chan struct{}
}

// controlCall is one request and the stream it arrived on.
type controlCall struct {
	request *hubpb.ControlRequest
	ref     *streamRef
}

// newControlPlane returns an empty control plane.
func newControlPlane(app *App) *controlPlane {
	return &controlPlane{app: app, queues: map[string]*controlQueue{}}
}

// start gives every processor a serial queue and its serving goroutine, until ctx ends.
func (c *controlPlane) start(ctx context.Context) {
	c.mu.Lock()
	c.ctx = ctx
	queues := make(map[string]*controlQueue, len(c.app.processors))
	for _, p := range c.app.processors {
		queue := &controlQueue{wake: make(chan struct{}, 1)}
		queues[p.name] = queue
	}
	c.queues = queues
	c.mu.Unlock()
	for name, queue := range queues {
		name, queue := name, queue
		// A control action in flight at shutdown is waited for within the background budget, so a
		// retry cannot touch the store after the app stopped.
		c.app.background("the control queue of processor "+name, func(ctx context.Context) error {
			c.serve(ctx, name, queue)
			return nil
		})
	}
}

// enqueue puts req on its processor's queue, off the caller's goroutine; a processor this
// instance does not run is answered UNKNOWN_PROCESSOR at once, also off the read loop.
func (c *controlPlane) enqueue(req *hubpb.ControlRequest, ref *streamRef) {
	name := req.GetProcessor()
	c.mu.Lock()
	queue := c.queues[name]
	c.mu.Unlock()
	if queue == nil {
		go c.answerOn(c.controlContext(), req, ref)
		return
	}
	queue.push(controlCall{request: req, ref: ref})
}

// controlContext is the plane's serving context, or a background one before it started.
func (c *controlPlane) controlContext() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

// push appends a call and wakes the queue.
func (q *controlQueue) push(call controlCall) {
	q.mu.Lock()
	q.pending = append(q.pending, call)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// next pops the oldest call, waiting until one arrives or ctx ends.
func (q *controlQueue) next(ctx context.Context) (controlCall, bool) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			call := q.pending[0]
			q.pending = q.pending[1:]
			q.mu.Unlock()
			return call, true
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return controlCall{}, false
		case <-q.wake:
		}
	}
}

// serve answers one processor's controls in arrival order until ctx ends.
func (c *controlPlane) serve(ctx context.Context, processor string, queue *controlQueue) {
	for {
		call, ok := queue.next(ctx)
		if !ok {
			return
		}
		c.answerOn(ctx, call.request, call.ref)
	}
}

// answerOn answers one control request: exactly one reply, logged at INFO, on its arrival stream.
func (c *controlPlane) answerOn(ctx context.Context, req *hubpb.ControlRequest, ref *streamRef) {
	reply := c.guarded(ctx, req)
	processor := req.GetProcessor()
	c.app.log.Info("processor control",
		"control", req.GetControlId(), "processor", processor,
		"action", controlActionName(req), "result", describeControlReply(reply))
	if ref == nil {
		return
	}
	if err := ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_ControlReply{ControlReply: reply}}); err != nil {
		c.app.log.Debug("a control reply was dropped: its hub stream is gone",
			"control", req.GetControlId(), "processor", processor)
	}
}

// guarded answers req, turning a panic or a missing processor into a refusal: the hub always gets
// its one reply.
func (c *controlPlane) guarded(ctx context.Context, req *hubpb.ControlRequest) (reply *hubpb.ControlReply) {
	defer func() {
		if r := recover(); r != nil {
			reply = controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
				fmt.Sprintf("the control failed: %v", r))
		}
	}()
	if req == nil {
		return controlRefused(&hubpb.ControlRequest{}, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
			"empty control request")
	}
	if c.app.processorByName(req.GetProcessor()) == nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_UNKNOWN_PROCESSOR,
			"processor "+req.GetProcessor()+" does not run on this instance")
	}
	switch action := req.GetAction().(type) {
	case *hubpb.ControlRequest_Pause:
		return c.setPaused(ctx, req, true)
	case *hubpb.ControlRequest_Resume:
		return c.setPaused(ctx, req, false)
	case *hubpb.ControlRequest_DeadLetterList:
		return c.deadLetterList(ctx, req, action.DeadLetterList)
	case *hubpb.ControlRequest_DeadLetterRetry:
		return c.deadLetterRetry(ctx, req, action.DeadLetterRetry)
	case *hubpb.ControlRequest_DeadLetterRetryAll:
		return c.deadLetterRetryAll(ctx, req)
	case *hubpb.ControlRequest_DeadLetterDiscard:
		return c.deadLetterDiscard(ctx, req, action.DeadLetterDiscard)
	case *hubpb.ControlRequest_ResetPrepare:
		return c.resetPrepare(ctx, req, action.ResetPrepare)
	case *hubpb.ControlRequest_ResetExecute:
		return c.resetExecute(ctx, req, action.ResetExecute)
	case *hubpb.ControlRequest_ResetFinish:
		return c.resetFinish(ctx, req, action.ResetFinish)
	default:
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
			"the control request has no action this instance knows")
	}
}

// setPaused persists a pause or resume and applies it here: consumption stops without leaving
// the subscription (or starts again), and the reset lock is not touched.
func (c *controlPlane) setPaused(ctx context.Context, req *hubpb.ControlRequest, paused bool) *hubpb.ControlReply {
	a := c.app
	if a.cfg.store == nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
			"the instance has no message store to keep the pause in")
	}
	if err := a.cfg.store.Control().SetPaused(ctx, req.GetProcessor(), paused, req.GetControlId(), a.now()); err != nil {
		if errors.Is(err, ErrUnsupported) {
			return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
				"the message store has no processor control port")
		}
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
	}
	if p := a.processorByName(req.GetProcessor()); p != nil {
		p.run.applyPaused(paused)
	}
	if paused {
		return controlOK(req, "paused")
	}
	return controlOK(req, "resumed, consuming again")
}

// deadLettersMode refuses every dead-letter action outside mode collection.
func (c *controlPlane) deadLettersMode(req *hubpb.ControlRequest) *hubpb.ControlReply {
	mode := c.app.cfg.events.deadLetterMode
	if mode == DeadLetterModeCollection {
		return nil
	}
	return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
		"dead letters are not kept in mode "+mode+": there is nothing to list, retry or discard")
}

// deadLetterList lists the processor's letters, oldest first, without payloads or headers.
func (c *controlPlane) deadLetterList(ctx context.Context, req *hubpb.ControlRequest, list *hubpb.DeadLetterList) *hubpb.ControlReply {
	if refusal := c.deadLettersMode(req); refusal != nil {
		return refusal
	}
	limit := list.GetLimit()
	if limit < 1 || limit > maxDeadLetterList {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID,
			fmt.Sprintf("limit must be 1 to %d, was %d", maxDeadLetterList, limit))
	}
	store := c.app.cfg.store.DeadLetters()
	letters, err := store.List(ctx, req.GetProcessor(), int(limit))
	if err != nil {
		return c.deadLetterFailure(req, err)
	}
	total, err := store.Count(ctx, req.GetProcessor())
	if err != nil {
		return c.deadLetterFailure(req, err)
	}
	page := &hubpb.DeadLetterPage{Total: uint64(max(total, 0))}
	for _, letter := range letters {
		page.Letters = append(page.Letters, deadLetterSummary(letter))
	}
	return &hubpb.ControlReply{
		ControlId: req.GetControlId(),
		Result:    &hubpb.ControlReply_DeadLetters{DeadLetters: page},
	}
}

// deadLetterRetry retries the letter letterID of the request's processor.
func (c *controlPlane) deadLetterRetry(ctx context.Context, req *hubpb.ControlRequest, retry *hubpb.DeadLetterRetry) *hubpb.ControlReply {
	if refusal := c.deadLettersMode(req); refusal != nil {
		return refusal
	}
	id := retry.GetLetterId()
	if strings.TrimSpace(id) == "" {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "no letter id")
	}
	letter, refusal := c.letterOf(ctx, req, id)
	if refusal != nil {
		return refusal
	}
	if letter == nil {
		return controlRetry(req, retryOutcome{result: retryNotFound})
	}
	p := c.app.processorByName(req.GetProcessor())
	outcome, err := c.app.retryDeadLetter(ctx, p, letter)
	if err != nil {
		return c.deadLetterFailure(req, err)
	}
	return controlRetry(req, outcome)
}

// deadLetterRetryAll retries every letter of the request's processor.
func (c *controlPlane) deadLetterRetryAll(ctx context.Context, req *hubpb.ControlRequest) *hubpb.ControlReply {
	if refusal := c.deadLettersMode(req); refusal != nil {
		return refusal
	}
	p := c.app.processorByName(req.GetProcessor())
	outcome, err := c.app.retryAllDeadLetters(ctx, p)
	if err != nil {
		return c.deadLetterFailure(req, err)
	}
	return &hubpb.ControlReply{
		ControlId: req.GetControlId(),
		Result: &hubpb.ControlReply_RetryAll{RetryAll: &hubpb.DeadLetterRetryAllOutcome{
			Retried: outcome.retried,
			Failed:  outcome.failed,
			Blocked: outcome.blocked,
			Skipped: outcome.skipped,
		}},
	}
}

// deadLetterDiscard deletes the letter letterID of the request's processor.
func (c *controlPlane) deadLetterDiscard(ctx context.Context, req *hubpb.ControlRequest, discard *hubpb.DeadLetterDiscard) *hubpb.ControlReply {
	if refusal := c.deadLettersMode(req); refusal != nil {
		return refusal
	}
	id := discard.GetLetterId()
	if strings.TrimSpace(id) == "" {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_INVALID, "no letter id")
	}
	letter, refusal := c.letterOf(ctx, req, id)
	if refusal != nil {
		return refusal
	}
	if letter == nil {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_FOUND,
			"no dead letter "+id+" in processor "+req.GetProcessor())
	}
	deleted, err := c.app.cfg.store.DeadLetters().Delete(ctx, id)
	if err != nil {
		return c.deadLetterFailure(req, err)
	}
	if !deleted {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_NOT_FOUND,
			"no dead letter "+id+" in processor "+req.GetProcessor())
	}
	c.app.log.Info("discarded a dead letter", "processor", req.GetProcessor(), "letter", id)
	return controlOK(req, "discarded")
}

// letterOf is the letter id of the request's processor, nil when it does not exist there.
func (c *controlPlane) letterOf(ctx context.Context, req *hubpb.ControlRequest, id string) (*DeadLetter, *hubpb.ControlReply) {
	letter, err := c.app.cfg.store.DeadLetters().Get(ctx, id)
	if err != nil {
		return nil, c.deadLetterFailure(req, err)
	}
	if letter == nil || letter.Processor != req.GetProcessor() {
		return nil, nil
	}
	return letter, nil
}

// deadLetterFailure is the refusal of a dead-letter store that failed or is not there.
func (c *controlPlane) deadLetterFailure(req *hubpb.ControlRequest, err error) *hubpb.ControlReply {
	if errors.Is(err, ErrUnsupported) {
		return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED,
			"the message store has no dead-letter port")
	}
	return controlRefused(req, hubpb.ControlRefusal_CONTROL_REFUSAL_FAILED, err.Error())
}

// deadLetterSummary is a letter without its payload or headers.
func deadLetterSummary(letter DeadLetter) *hubpb.DeadLetterSummary {
	key := ""
	if letter.Key != nil {
		key = *letter.Key
	}
	attempts := letter.Attempts
	if attempts < 0 {
		attempts = 0
	}
	parkedAt, lastAttemptAt := int64(0), int64(0)
	if !letter.ParkedAt.IsZero() {
		parkedAt = letter.ParkedAt.UnixMilli()
	}
	if !letter.LastAttemptAt.IsZero() {
		lastAttemptAt = letter.LastAttemptAt.UnixMilli()
	}
	return &hubpb.DeadLetterSummary{
		// The ids, keys and record coordinates are sent whole: the hub matches a retried letter by
		// its id, so a cut one would not be found.
		Id:              letter.ID,
		Processor:       letter.Processor,
		Source:          letter.Source,
		Segment:         letter.Segment,
		Position:        letter.Position,
		Key:             key,
		EventId:         letter.EventID,
		EventType:       cutUTF8Bytes(letter.EventType, maxControlTypeBytes),
		Reason:          letter.Reason.String(),
		ErrorType:       cutUTF8Bytes(letter.Error.Type, maxControlTypeBytes),
		ErrorMessage:    cutUTF8Bytes(letter.Error.Message, maxControlTextBytes),
		BlockedBy:       letter.BlockedBy,
		Attempts:        uint32(attempts),
		ParkedAtMs:      parkedAt,
		LastAttemptAtMs: lastAttemptAt,
	}
}

// controlOK is a ControlReply.ok with its detail.
func controlOK(req *hubpb.ControlRequest, detail string) *hubpb.ControlReply {
	return &hubpb.ControlReply{
		ControlId: req.GetControlId(),
		Result:    &hubpb.ControlReply_Ok{Ok: &hubpb.ControlOk{Detail: cutUTF8Bytes(detail, maxControlTextBytes)}},
	}
}

// controlRetry is a ControlReply.retry with its outcome.
func controlRetry(req *hubpb.ControlRequest, outcome retryOutcome) *hubpb.ControlReply {
	return &hubpb.ControlReply{
		ControlId: req.GetControlId(),
		Result: &hubpb.ControlReply_Retry{Retry: &hubpb.DeadLetterRetryOutcome{
			Result: outcome.result,
			Detail: cutUTF8Bytes(outcome.detail, maxControlTextBytes),
		}},
	}
}

// controlRefused is a refusal with its code and message.
func controlRefused(req *hubpb.ControlRequest, code hubpb.ControlRefusal, message string) *hubpb.ControlReply {
	return &hubpb.ControlReply{
		ControlId: req.GetControlId(),
		Result: &hubpb.ControlReply_Refused{Refused: &hubpb.ControlRefused{
			Code:    code,
			Message: cutUTF8Bytes(message, maxControlTextBytes),
		}},
	}
}

// controlActionName is the request's action as the log names it.
func controlActionName(req *hubpb.ControlRequest) string {
	switch req.GetAction().(type) {
	case *hubpb.ControlRequest_Pause:
		return "pause"
	case *hubpb.ControlRequest_Resume:
		return "resume"
	case *hubpb.ControlRequest_ResetPrepare:
		return "reset_prepare"
	case *hubpb.ControlRequest_ResetExecute:
		return "reset_execute"
	case *hubpb.ControlRequest_ResetFinish:
		return "reset_finish"
	case *hubpb.ControlRequest_DeadLetterList:
		return "dead_letter_list"
	case *hubpb.ControlRequest_DeadLetterRetry:
		return "dead_letter_retry"
	case *hubpb.ControlRequest_DeadLetterRetryAll:
		return "dead_letter_retry_all"
	case *hubpb.ControlRequest_DeadLetterDiscard:
		return "dead_letter_discard"
	default:
		return "none"
	}
}

// describeControlReply is the reply as the log names it.
func describeControlReply(reply *hubpb.ControlReply) string {
	if reply == nil {
		return "no reply"
	}
	switch result := reply.GetResult().(type) {
	case *hubpb.ControlReply_Ok:
		return "ok (" + result.Ok.GetDetail() + ")"
	case *hubpb.ControlReply_Refused:
		code := strings.TrimPrefix(result.Refused.GetCode().String(), "CONTROL_REFUSAL_")
		return "refused " + code + ": " + result.Refused.GetMessage()
	case *hubpb.ControlReply_DeadLetters:
		return fmt.Sprintf("%d of %d letters", len(result.DeadLetters.GetLetters()), result.DeadLetters.GetTotal())
	case *hubpb.ControlReply_Retry:
		return result.Retry.GetResult()
	case *hubpb.ControlReply_RetryAll:
		return fmt.Sprintf("%d retried, %d failed, %d blocked, %d skipped",
			result.RetryAll.GetRetried(), result.RetryAll.GetFailed(),
			result.RetryAll.GetBlocked(), result.RetryAll.GetSkipped())
	case *hubpb.ControlReply_Reset_:
		return fmt.Sprintf("reset (%d ranges, %d markers deleted, %d reset handlers)",
			len(result.Reset_.GetRanges()), result.Reset_.GetMarkersDeleted(), result.Reset_.GetResetHandlers())
	default:
		return "no result"
	}
}

// processorByName is the processor this instance runs under name, or nil.
func (a *App) processorByName(name string) *processor {
	for _, p := range a.processors {
		if p.name == name {
			return p
		}
	}
	return nil
}
