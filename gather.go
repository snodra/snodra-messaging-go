package snodra

import (
	"context"
	"fmt"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// Part is one target's answer to a [Gather]: the service and instance that answered, and the
// answer decoded as R, or the target's failure. Err is an [*Error] with the target's code and
// message (its handler's error, or a failure the hub saw for that target, such as
// HANDLER_DISCONNECTED or RESOURCE_EXHAUSTED); a payload that does not decode as R is another
// error.
type Part[R any] struct {
	Service, Instance string
	Value             R
	Err               error
}

// Gathered is everything a [Gather] got by the hub's end.
type Gathered[R any] struct {
	// Parts holds one entry per answer, in arrival order, failed ones included.
	Parts []Part[R]
	// Targets is how many targets the gather had: the parts the hub sent plus TimedOut.
	Targets int
	// Failed counts the Parts whose Err is set.
	Failed int
	// TimedOut counts the targets that had not answered by the deadline (no part for them).
	TimedOut int
}

// Gather asks every live instance of every owner of the query type of q (a scatter-gather) and
// collects their answers until the hub's end. Partial answers are answers: a gather where some
// targets failed or timed out still succeeds, and Gathered says what was missing.
//
// The options are those of [Query]: [Timeout] (or ctx's deadline, or [DefaultTimeout]) is the
// deadline of the whole gather, after which the targets that have not answered count as
// TimedOut; [MaxResults] ends it after that many successful parts; [Retry] applies to the whole
// gather (a retry sends a new gather and collects from scratch). [RoutingKey] is ignored: the
// gather goes to one hub, unkeyed.
//
// A failed call is one of the errors of [Send]: an [*Error] for a failure of the whole gather
// (NO_HANDLER when the type has no live target, INVALID_REQUEST for a command type, the
// library's own TIMEOUT when the hub's end did not come within the hub's bound plus 1 s),
// [ErrNeverSent], [ErrOutcomeUnknown] (the hub stream broke before the end: the parts received
// are dropped), the context's error, or a local failure.
// See https://snodra.damoreira.com/library-authors/protocol/ (Scatter-gather).
func Gather[R any](ctx context.Context, app *App, q Message, opts ...SendOption) (*Gathered[R], error) {
	out, err := prepare(ctx, app, hubpb.RequestKind_QUERY, hubpb.RequestMode_SCATTER_GATHER, q, opts)
	if err != nil {
		return nil, err
	}
	return retrying(ctx, hubpb.RequestKind_QUERY, out.policy, out.budget, out.chosen, func(timeout time.Duration) (*Gathered[R], error) {
		ans, err := app.attempt(ctx, out.tmpl, timeout)
		if err != nil {
			return nil, err
		}
		if ans.failure != nil {
			return nil, errorFromWire(out.typ, ans.failure)
		}
		return assemble[R](app.cfg.codec, ans), nil
	})
}

// assemble is a gather's result from its parts (in arrival order) and its end; Failed is
// counted from the parts, as in the reference library, so a part that does not decode counts.
func assemble[R any](codec PayloadCodec, ans answer) *Gathered[R] {
	g := &Gathered[R]{Parts: make([]Part[R], 0, len(ans.parts))}
	for _, p := range ans.parts {
		part := Part[R]{Service: p.GetResponderService(), Instance: p.GetResponderInstance()}
		if e := p.GetError(); e != nil {
			part.Err = partError(e)
		} else if v, err := decodeAnswer[R](codec, p.GetPayload()); err != nil {
			part.Err = fmt.Errorf("snodra: cannot decode the answer of %s/%s: %w", part.Service, part.Instance, err)
		} else {
			part.Value = v
		}
		if part.Err != nil {
			g.Failed++
		}
		g.Parts = append(g.Parts, part)
	}
	g.TimedOut = int(ans.end.GetTimedOut())
	g.Targets = int(ans.end.GetParts()) + g.TimedOut
	return g
}

// partError is a target's failure as the caller sees it: its code (an unknown one reads as
// HANDLER_ERROR) and the message the target or the hub gave, unchanged, as in the reference
// library; the details only for HANDLER_ERROR, as for any error.
func partError(e *hubpb.Error) *Error {
	code := Code(e.GetCode())
	if code < CodeNoHandler || code > CodeCancelled {
		code = CodeHandlerError
	}
	out := &Error{Code: code, Message: e.GetMessage()}
	if code == CodeHandlerError && len(e.GetDetails()) > 0 {
		out.Details = append([]byte(nil), e.GetDetails()...)
	}
	return out
}
