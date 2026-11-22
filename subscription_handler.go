package snodra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// HandleSubscription registers the subscription query type Q, by Q's MessageType: initial
// answers a subscription's initial result, and the returned [Updates] pushes the updates to the
// subscriptions this instance serves, until they complete or fail. Q is registered in
// Register.query_types (with [Shared], in Register.shared_query_types).
//
// The hub subscribes every live instance of every owner of Q: each gets the subscription (a
// request <id>#n, which this instance registers at once, before its admission: the caller's
// token, the guards, the decoding of the query and the limit, [MaxSubscriptions]), and one of
// them also gets the initial request <id>#0, which initial answers like a [HandleQuery]
// handler. Any other unary request of Q is answered INVALID_REQUEST. A subscription takes no
// permit and no handler slot: it is passive, fed by [Updates.Emit] from anywhere in the
// application, and ends with [Updates.Complete] or [Updates.Fail], when the hub cancels it (the
// caller closed it, another owner failed, a limit), when its hub stream is lost, or at
// [App.Shutdown], which completes every one of them.
//
// Registration errors are those of [HandleCommand]; the Updates is nil then.
// See https://snodra.damoreira.com/library-authors/protocol/ (Subscription).
func HandleSubscription[Q Message, I any](app *App, initial func(context.Context, Q) (I, error), opts ...HandlerOption) (*Updates[Q], error) {
	if app == nil {
		return nil, errors.New("snodra: registration on a nil App")
	}
	name, cfg, err := handlerSetup[Q](kindSubscription, initial == nil, opts)
	h := &handler{kind: kindSubscription, name: name, invoke: invokerOf(initial), decode: queryDecoderOf[Q](), cfg: cfg}
	if err := app.addHandler(h, err); err != nil {
		return nil, err
	}
	return &Updates[Q]{app: app, typ: name}, nil
}

// queryDecoderOf decodes a fresh Q.
func queryDecoderOf[Q Message]() queryDecoder {
	return func(codec PayloadCodec, payload []byte) (any, error) { return decodeMessage[Q](codec, payload) }
}

// MaxSubscriptions is how many subscriptions the instance serves at once: one more is refused
// with ReplyEnd{RESOURCE_EXHAUSTED "subscription limit (<n>) reached"}. The default is 1024; n
// must be at least 1.
func MaxSubscriptions(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("snodra: MaxSubscriptions must be at least 1, got %d", n)
		}
		c.maxSubscriptions = n
		return nil
	}
}

// Updates pushes the updates of the subscription query type Q to the subscriptions this instance
// serves ([HandleSubscription]). Each call reaches every open subscription of Q whose query
// match accepts (a nil match accepts every one), in call order per subscription; a call never
// waits for a hub stream. It is safe for concurrent use, also from a match.
//
// A subscription is reached from the moment its request is read: what is emitted before its
// admission is decided is held, then sent first (or dropped when it is refused).
type Updates[Q Message] struct {
	app *App
	typ string
}

// Emit sends update, encoded with the app's codec, as the next part of every open subscription
// of Q that match accepts, and returns how many it reached. An update that does not encode
// reaches none (logged); a match that panics skips its subscription (logged).
func (u *Updates[Q]) Emit(match func(Q) bool, update any) int {
	if u == nil {
		return 0
	}
	payload, err := encodeResult(u.app.cfg.codec, update)
	if err != nil {
		u.app.log.Error("update not emitted: it does not encode", "type", u.typ, "error", err.Error())
		return 0
	}
	return u.app.subs.deliver(u.typ, u.matcher(match), func(id string, seq uint64) *hubpb.ClientMessage {
		return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Part{Part: &hubpb.ReplyPart{RequestId: id, Seq: seq, Payload: payload}}}
	}, false)
}

// Complete ends every open subscription of Q that match accepts with a clean ReplyEnd; each is
// removed. At the hub a clean end removes this instance from the subscription; the last owner's
// clean end completes it for the caller.
func (u *Updates[Q]) Complete(match func(Q) bool) {
	if u == nil {
		return
	}
	u.app.subs.deliver(u.typ, u.matcher(match), func(id string, seq uint64) *hubpb.ClientMessage {
		return subscriptionEnd(id, seq, nil)
	}, true)
}

// Fail ends every open subscription of Q that match accepts with ReplyEnd{HANDLER_ERROR}, err's
// message on one line and the details attached with [WithDetails] (always HANDLER_ERROR, also
// for an error of [Unauthenticated] and the like); each is removed. At the hub it ends the whole
// subscription with that error for the caller and cancels its other owners.
func (u *Updates[Q]) Fail(match func(Q) bool, err error) {
	if u == nil {
		return
	}
	failure := errorToWire("", err)
	if failure.GetCode() != hubpb.ErrorCode_HANDLER_ERROR {
		failure.Code, failure.Details = hubpb.ErrorCode_HANDLER_ERROR, nearestDetails(err)
	}
	u.app.subs.deliver(u.typ, u.matcher(match), func(id string, seq uint64) *hubpb.ClientMessage {
		return subscriptionEnd(id, seq, &hubpb.Error{RequestId: id, Code: failure.GetCode(),
			Message: failure.GetMessage(), Details: failure.GetDetails()})
	}, true)
}

// matcher is match on the decoded query of a subscription; nil accepts every one.
func (u *Updates[Q]) matcher(match func(Q) bool) func(any) bool {
	return func(query any) bool {
		q, ok := query.(Q)
		return ok && (match == nil || match(q))
	}
}

// nearestDetails are the details of the nearest error made by [WithDetails] in err's chain; nil
// when none.
func nearestDetails(err error) []byte {
	var he *handlerError
	for e := err; e != nil && errors.As(e, &he); e = he.cause {
		if he.details != nil {
			return he.details
		}
	}
	return nil
}

// subscriptionEnd is the ReplyEnd of subscription id after parts updates, with werr when it
// failed.
func subscriptionEnd(id string, parts uint64, werr *hubpb.Error) *hubpb.ClientMessage {
	return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_End{End: &hubpb.ReplyEnd{
		RequestId: id, Error: werr, Parts: uint32(min(parts, uint64(^uint32(0)))),
	}}}
}

// SubscriptionsServed is how many subscriptions this instance serves now, those whose admission
// is still being decided included.
func (a *App) SubscriptionsServed() int { return a.subs.count() }

// servedSubscription is one subscription target (<id>#n) served here: its decoded query, the
// hub stream it arrived on and what it sent.
type servedSubscription struct {
	id    string
	typ   string
	query any
	ref   *streamRef

	// Under servedSubscriptions.mu:
	seq      uint64                 // the updates sent (the seq of the last part)
	admitted bool                   // its admission passed: what it gets is written at once
	over     bool                   // cancelled, lost, refused or its stream gone: nothing more is written
	held     []*hubpb.ClientMessage // what it got before its admission, in order
}

// servedSubscriptions are the subscriptions this instance serves, by their request id (<id>#n),
// over every hub stream. Nothing user-supplied runs under mu: the matches run on a snapshot.
type servedSubscriptions struct {
	max int
	log *slog.Logger

	mu   sync.Mutex
	byID map[string]*servedSubscription
}

func newServedSubscriptions(max int, log *slog.Logger) *servedSubscriptions {
	return &servedSubscriptions{max: max, log: log, byID: map[string]*servedSubscription{}}
}

func (s *servedSubscriptions) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

// register records subscription id of typ with its decoded query, arrived on ref, pending its
// admission; nil when the instance already serves its limit (nothing recorded). A second
// subscription with the same id replaces the first, which sends nothing more.
func (s *servedSubscriptions) register(id, typ string, query any, ref *streamRef) *servedSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.byID[id]
	if old == nil && len(s.byID) >= s.max {
		return nil
	}
	if old != nil {
		old.over, old.held = true, nil
	}
	sub := &servedSubscription{id: id, typ: typ, query: query, ref: ref}
	s.byID[id] = sub
	return sub
}

// removeLocked forgets sub if it is still the one registered under its id.
func (s *servedSubscriptions) removeLocked(sub *servedSubscription) {
	if s.byID[sub.id] == sub {
		delete(s.byID, sub.id)
	}
}

// writeLocked sends m for sub: held until its admission, then written to its arrival stream. A
// stream that is gone takes the subscription with it.
func (s *servedSubscriptions) writeLocked(sub *servedSubscription, m *hubpb.ClientMessage) {
	switch {
	case sub.over:
	case !sub.admitted:
		sub.held = append(sub.held, m)
	case sub.ref.send(m) != nil:
		sub.over, sub.held = true, nil
		s.removeLocked(sub)
	}
}

// admit lets sub write: what it got meanwhile goes out first, in order. Nothing when it is over
// (the hub cancelled it, its stream was lost).
func (s *servedSubscriptions) admit(sub *servedSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub.over {
		return
	}
	sub.admitted = true
	held := sub.held
	sub.held = nil
	for _, m := range held {
		s.writeLocked(sub, m)
	}
}

// refuse ends sub with werr instead of what it got meanwhile, which is dropped; nothing at all
// when it is over.
func (s *servedSubscriptions) refuse(sub *servedSubscription, werr *hubpb.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub.over {
		return
	}
	sub.over, sub.held = true, nil
	s.removeLocked(sub)
	_ = sub.ref.send(subscriptionEnd(sub.id, 0, werr))
}

// cancel removes subscription id (the hub's Cancel): nothing more is sent for it. False when
// there is none.
func (s *servedSubscriptions) cancel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.byID[id]
	if sub == nil {
		return false
	}
	delete(s.byID, id)
	sub.over, sub.held = true, nil
	return true
}

// streamLost removes the subscriptions that arrived on ref, a stream that is gone (the hub forgot
// them with the session), and returns how many.
func (s *servedSubscriptions) streamLost(ref *streamRef) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, sub := range s.byID {
		if sub.ref == ref {
			delete(s.byID, id)
			sub.over, sub.held = true, nil
			n++
		}
	}
	return n
}

// endAll completes every subscription with a clean ReplyEnd (shutdown) and returns the streams
// the ends were queued on.
func (s *servedSubscriptions) endAll() []*streamRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	var refs []*streamRef
	seen := map[*streamRef]bool{}
	for id, sub := range s.byID {
		delete(s.byID, id)
		s.writeLocked(sub, subscriptionEnd(id, sub.seq, nil))
		if !seen[sub.ref] {
			seen[sub.ref] = true
			refs = append(refs, sub.ref)
		}
	}
	return refs
}

// deliver sends what frame makes (from the subscription's id and seq) to every subscription of
// typ whose query match accepts, and returns how many it reached. A part counts one more update
// (seq + 1); an end (remove) carries the count and removes the subscription first. The matches
// run outside the lock, on a snapshot: one that panics skips its subscription, and one may call
// deliver again.
func (s *servedSubscriptions) deliver(typ string, match func(any) bool, frame func(id string, seq uint64) *hubpb.ClientMessage, remove bool) int {
	s.mu.Lock()
	var candidates []*servedSubscription
	for _, sub := range s.byID {
		if sub.typ == typ {
			candidates = append(candidates, sub)
		}
	}
	s.mu.Unlock()
	var matched []*servedSubscription
	for _, sub := range candidates {
		if s.matches(match, sub) {
			matched = append(matched, sub)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range matched {
		if s.byID[sub.id] != sub {
			continue // cancelled, ended or replaced meanwhile
		}
		n++
		if remove {
			delete(s.byID, sub.id)
			s.writeLocked(sub, frame(sub.id, sub.seq))
			continue
		}
		sub.seq++
		s.writeLocked(sub, frame(sub.id, sub.seq))
	}
	return n
}

// matches runs match on sub's query; a panic is no match.
func (s *servedSubscriptions) matches(match func(any) bool, sub *servedSubscription) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			s.log.Warn("a match panicked: its subscription is skipped", "type", sub.typ, "request", sub.id,
				"panic", fmt.Sprint(r))
		}
	}()
	return match(sub.query)
}

// subscribe serves the subscription request req (<id>#n, mode SUBSCRIPTION) that arrived on
// ref. Called from the stream's read loop: it is registered at once, as far as it can be before
// its admission (its handler looked up, its query decoded, the limit), so that a hub Cancel or a
// lost stream that follows finds it and an update emitted after the initial request was read
// reaches it; the admission then runs on its own goroutine.
func (a *App) subscribe(req *hubpb.Request, ref *streamRef) {
	id := req.GetId()
	fail := func(code hubpb.ErrorCode, msg string) *hubpb.Error {
		return &hubpb.Error{RequestId: id, Code: code, Message: lineBreaks.Replace(msg)}
	}
	h, code, msg := a.lookupSubscription(req)
	var (
		unserved *hubpb.Error // no subscription handler of the type
		flaw     *hubpb.Error // the query does not decode, or the limit is reached
		sub      *servedSubscription
	)
	if h == nil {
		unserved = fail(code, msg)
	} else if query, err := h.decode(a.cfg.codec, req.GetPayload()); err != nil {
		flaw = handlerFailure(id, err)
	} else if sub = a.subs.register(id, req.GetType(), query, ref); sub == nil {
		flaw = fail(hubpb.ErrorCode_RESOURCE_EXHAUSTED, fmt.Sprintf("subscription limit (%d) reached", a.subs.max))
	}
	go a.admitSubscription(req, ref, h, sub, unserved, flaw)
}

// admitSubscription decides the admission of a subscription request, in the reference
// library's order: the caller's token, then a type not served here, then the guards (before
// the decoding's verdict: an anonymous caller cannot probe the query's shape), then the
// decoding and the limit. Admitted, what was emitted meanwhile goes out; refused, the refusal is
// its one ReplyEnd (none when the hub cancelled it meanwhile).
func (a *App) admitSubscription(req *hubpb.Request, ref *streamRef, h *handler, sub *servedSubscription, unserved, flaw *hubpb.Error) {
	id := req.GetId()
	werr := unserved
	// TODO(spec-gap): SG-18 (every forwarded token is checked, also for a handler without a guard).
	caller, err := a.identity.resolve(a.handlerCtx, req.GetHeaders()[authorizationHeader])
	switch {
	case err != nil:
		werr = &hubpb.Error{RequestId: id, Code: hubpb.ErrorCode_UNAUTHENTICATED, Message: lineBreaks.Replace(err.Error())}
	case werr != nil:
	default:
		if code, msg, refused := a.guardRefusal(h.cfg, req.GetType(), caller); refused {
			werr = &hubpb.Error{RequestId: id, Code: hubpb.ErrorCode(code), Message: lineBreaks.Replace(msg)}
		} else {
			werr = flaw
		}
	}
	if werr == nil {
		a.subs.admit(sub)
		a.log.Debug("subscription registered", "request", id, "type", req.GetType())
		return
	}
	a.log.Debug("subscription refused", "request", id, "type", req.GetType(), "code", werr.GetCode().String(), "reason", werr.GetMessage())
	if sub != nil {
		a.subs.refuse(sub, werr)
		return
	}
	if err := ref.send(subscriptionEnd(id, 0, werr)); err != nil {
		a.log.Debug("refusal dropped: its hub stream is gone", "request", id, "type", req.GetType())
	}
}

// lookupSubscription is the subscription handler of req, or the error code and message to
// answer: NO_HANDLER for a type not served here as a query (or a command); INVALID_REQUEST for
// an unknown kind and for a query handler of another mode, with the reference library's texts.
func (a *App) lookupSubscription(req *hubpb.Request) (*handler, hubpb.ErrorCode, string) {
	typ := req.GetType()
	h := a.handlers[typ] // read-only once started
	switch req.GetKind() {
	case hubpb.RequestKind_QUERY:
	case hubpb.RequestKind_COMMAND:
		return nil, hubpb.ErrorCode_NO_HANDLER, noHandlerText(typ)
	default:
		return nil, hubpb.ErrorCode_INVALID_REQUEST, fmt.Sprintf("unsupported request kind %d", req.GetKind())
	}
	switch {
	case h == nil || h.kind == kindCommand:
		return nil, hubpb.ErrorCode_NO_HANDLER, noHandlerText(typ)
	case h.kind == kindQuery:
		return nil, hubpb.ErrorCode_INVALID_REQUEST, typ + " is not a subscription query handler (it answers mode UNARY)"
	case h.kind == kindStream:
		return nil, hubpb.ErrorCode_INVALID_REQUEST, typ + " is not a subscription query handler (it answers mode STREAMING)"
	}
	return h, 0, ""
}
