package snodra

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

func TestRetryBudget(t *testing.T) { // SG-2
	ack := &hubpb.RegisterAck{DefaultTimeoutMs: 5000, TypeTimeoutsMs: map[string]uint32{"orders.GetOrder": 1500}}
	is(t, retryBudget(ack, "x.Y", 3*time.Second), 3*time.Second)
	is(t, retryBudget(ack, "orders.GetOrder", 0), 1500*time.Millisecond)
	is(t, retryBudget(ack, "x.Y", 0), 5*time.Second)
	is(t, retryBudget(nil, "x.Y", 0), 5*time.Second)
	is(t, retryBudget(&hubpb.RegisterAck{}, "x.Y", 0), 5*time.Second)
}

func TestRetryable(t *testing.T) {
	code := func(c Code) error { return &Error{Code: c, Message: "m"} }
	cmd, qry := hubpb.RequestKind_COMMAND, hubpb.RequestKind_QUERY
	for _, c := range []struct {
		err      error
		cmd, qry bool
	}{
		{ErrNeverSent, true, true},
		{code(CodeNoHandler), true, true},
		{code(CodeResourceExhausted), true, true},
		{code(CodeTimeout), false, true},
		{code(CodeHandlerDisconnected), false, true},
		{ErrOutcomeUnknown, false, true},
		{code(CodeHandlerError), false, false},
		{code(CodeUnauthenticated), false, false},
		{code(CodePermissionDenied), false, false},
		{code(CodeInvalidRequest), false, false},
		{code(CodeCancelled), false, false},
		{errors.New("local"), false, false},
		{errors.Join(ErrOutcomeUnknown, context.Canceled), false, false}, // the caller gave up
	} {
		if retryable(cmd, c.err) != c.cmd || retryable(qry, c.err) != c.qry {
			t.Errorf("%v: command %v query %v, want %v %v", c.err, retryable(cmd, c.err), retryable(qry, c.err), c.cmd, c.qry)
		}
	}
}

func TestRetryPolicyWaits(t *testing.T) {
	p := RetryNeverRan
	is(t, p, RetryPolicy{MaxAttempts: 3, InitialBackoff: 100 * ms, Multiplier: 2, MaxBackoff: 2 * time.Second})
	is(t, p.backoff(1), 100*ms)
	is(t, p.backoff(2), 200*ms)
	is(t, p.backoff(6), 2*s) // capped
	for range 50 {
		within(t, p.wait(1), 80*ms, 120*ms)
		within(t, p.wait(9), 1600*ms, 2*s)
	}
}

func TestRetryAfterNoHandlerWithAFreshIDAndTheRemainingBudget(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: 20 * ms, Multiplier: 2, MaxBackoff: 2 * time.Second}
	sent := async(func() (Receipt, error) {
		return Send[Receipt](ctxWithin(t, 10*time.Second), app, PlaceOrder{}, Retry(policy), Timeout(5*time.Second))
	})
	r1 := s.RecvRequest(t)
	is(t, r1.GetTimeoutMs() > 0 && r1.GetTimeoutMs() <= 5000, true)
	s.Send(t, fakehub.HubError(r1.GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	r2 := s.RecvRequest(t)
	is(t, r2.GetId() != r1.GetId(), true)
	is(t, r2.GetTimeoutMs() > 0 && r2.GetTimeoutMs() < 5000, true)
	s.Send(t, replyTo(r2.GetId(), `{"total":3}`))
	o := await(t, sent)
	is(t, o.err, nil)
	is(t, o.value.Total, 3)
}

func TestRetryWithoutACallerTimeoutNeverSendsZero(t *testing.T) {
	app, _, s := sending(t, fakehub.Ack("hub-1"), DefaultRetry(RetryPolicy{MaxAttempts: 3, InitialBackoff: 20 * ms, Multiplier: 2, MaxBackoff: 2 * time.Second}))
	// No deadline on the context: the caller chose no timeout (await bounds the wait).
	sent := async(func() (Empty, error) { return Send[Empty](context.Background(), app, PlaceOrder{}) })
	r1 := s.RecvRequest(t)
	is(t, r1.GetTimeoutMs() > 0 && r1.GetTimeoutMs() <= 5000, true) // the ack's default 5000 is the budget
	s.Send(t, fakehub.HubError(r1.GetId(), hubpb.ErrorCode_RESOURCE_EXHAUSTED, "busy"))
	r2 := s.RecvRequest(t)
	is(t, r2.GetTimeoutMs() > 0 && r2.GetTimeoutMs() < 5000, true)
	s.Send(t, replyTo(r2.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

func TestACommandIsNeverRetriedAfterAnUnknownOutcome(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: 20 * ms, Multiplier: 2, MaxBackoff: 2 * time.Second}
	app, hub, s := sending(t, fakehub.Ack("hub-1"), DefaultRetry(policy))
	ctx := ctxWithin(t, 10*time.Second)
	for _, code := range []hubpb.ErrorCode{hubpb.ErrorCode_TIMEOUT, hubpb.ErrorCode_HANDLER_DISCONNECTED} {
		sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
		s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), code, "unknown"))
		var e *Error
		o := await(t, sent)
		is(t, errors.As(o.err, &e) && e.Code == Code(code), true)
		s.Nothing(t, 200*ms, "request")
	}
	// A query is retried after them.
	queried := async(func() (Empty, error) { return Query[Empty](ctx, app, GetOrder{}) })
	s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), hubpb.ErrorCode_TIMEOUT, "slow"))
	s.Send(t, replyTo(s.RecvRequest(t).GetId(), `{}`))
	is(t, await(t, queried).err, nil)
	// The stream broken after the write.
	sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
	s.RecvRequest(t)
	s.Close(codes.Unavailable, "restart")
	is(t, errors.Is(await(t, sent).err, ErrOutcomeUnknown), true)
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.Recv(t)
	s2.Ack(t, fakehub.Ack("hub-1"))
	s2.Nothing(t, 200*ms, "request")
	is(t, s2.Recv(t).GetHeartbeat() != nil, true) // alive
}

func TestARequestNeverSentIsRetried(t *testing.T) {
	held := make(chan *hubpb.Request, 2)
	release := make(chan struct{})
	policy := RetryPolicy{MaxAttempts: 10, InitialBackoff: 50 * ms, Multiplier: 2, MaxBackoff: 200 * ms}
	app, hub, s := sending(t, fakehub.Ack("hub-1"), DefaultRetry(policy), blockRequests(held, release), ReconnectBackoff(10*ms, 20*ms))
	sent := async(func() (Empty, error) { return Send[Empty](ctxWithin(t, 10*time.Second), app, PlaceOrder{}) })
	first := receive(t, held) // held in the first stream's buffer
	s.Close(codes.Unavailable, "restart")
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.Recv(t)
	s2.Ack(t, fakehub.Ack("hub-1"))
	second := receive(t, held) // the retry, on the new stream
	is(t, second.GetId() != first.GetId(), true)
	close(release)
	r := s2.RecvRequest(t)
	is(t, r.GetId(), second.GetId())
	s2.Send(t, replyTo(r.GetId(), `{}`))
	is(t, await(t, sent).err, nil)
}

func TestNoRetryWithLessThanTheMinimumLeft(t *testing.T) { // SG-2
	app, _, s := sending(t, fakehub.Ack("hub-1"))
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: 100 * ms, Multiplier: 2, MaxBackoff: 2 * time.Second}
	sent := async(func() (Empty, error) {
		return Send[Empty](ctxWithin(t, 5*time.Second), app, PlaceOrder{}, Retry(policy), Timeout(300*ms))
	})
	r := s.RecvRequest(t)
	time.Sleep(100 * ms) // the first answer at 100 ms: 300 - 100 - ~100 leaves less than 250 ms
	s.Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	var e *Error
	is(t, errors.As(await(t, sent).err, &e) && e.Code == CodeNoHandler, true)
	s.Nothing(t, 400*ms, "request")
	is(t, s.Recv(t).GetHeartbeat() != nil, true) // the stream is alive: no retry was written to it
}

func TestNoRetryAfterTheLastAttemptOrWithNoRetry(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * ms, Multiplier: 2, MaxBackoff: 2 * time.Second}
	app, _, s := sending(t, fakehub.Ack("hub-1"), DefaultRetry(policy))
	ctx := context.Background() // no deadline: no caller timeout (await bounds the wait)
	sent := async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}) })
	s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	s.Send(t, fakehub.HubError(s.RecvRequest(t).GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	is(t, await(t, sent).err != nil, true)
	s.Nothing(t, 200*ms, "request") // two attempts in all
	sent = async(func() (Empty, error) { return Send[Empty](ctx, app, PlaceOrder{}, NoRetry()) })
	r := s.RecvRequest(t)
	is(t, r.GetTimeoutMs(), uint32(0)) // one attempt: the hub decides, as without a policy
	s.Send(t, fakehub.HubError(r.GetId(), hubpb.ErrorCode_NO_HANDLER, "nobody"))
	is(t, await(t, sent).err != nil, true)
	s.Nothing(t, 200*ms, "request")
	is(t, s.Recv(t).GetHeartbeat() != nil, true) // the stream is alive: no retry was written to it
}
