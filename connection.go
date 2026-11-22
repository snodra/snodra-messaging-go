package snodramessaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/snodra/snodra-messaging-go/internal/backoff"
	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// connState is where a connection to one hub stands, as drawn in "A connection to one hub" of
// https://snodra.damoreira.com/library-authors/writing-a-library/.
type connState int

const (
	stateConnecting  connState = iota // opening a stream
	stateAwaitingAck                  // Register and Heartbeat written, no RegisterAck yet
	stateLive                         // acknowledged
	stateBackoff                      // waiting before the next attempt
	stateRejected                     // the hub refused the Register; waiting the backoff cap
	stateDraining                     // shutting down: the empty Register, then the half-close
	stateStopped                      // the connection's goroutine has ended
)

func (s connState) String() string {
	return [...]string{"connecting", "awaiting-ack", "live", "backoff", "rejected", "draining", "stopped"}[s]
}

// refusalsKept bounds the distinct refusals a connection remembers having logged at error.
const refusalsKept = 64

// errStreamGone is the error of a write to a stream that has ended.
var errStreamGone = errors.New("snodra: the hub stream is gone")

// hubConn is the connection to one hub: one goroutine (run) opens a stream, registers, keeps
// it alive with heartbeats and opens another after a backoff when it ends, until shutdown.
type hubConn struct {
	app     *App
	address string // host:port
	t       timings
	log     *slog.Logger

	stopping  chan struct{} // closed when shutdown begins: no reconnect, the empty Register
	closing   chan struct{} // closed when the stream must be half-closed
	deregDone chan struct{} // closed once the empty Register is acked, or there is none to wait for
	done      chan struct{} // closed when run has returned

	stopOnce, closeOnce, deregOnce sync.Once

	rollReq chan *streamRef // a client certificate rollover asked for this stream (buffered)

	// The credentials, touched by the connection's goroutine only.
	token           *secret         // the static token the streams register with (from a file: the last read)
	emptyWarned     bool            // the empty token file was warned about since the last good read
	jwt             *issuedToken    // the access token the Registers carry
	sentGen         uint64          // the newest token generation the current stream registered with or got
	presentedJWT    *secret         // the access token the current stream presented last
	identity        *clientIdentity // the last good client certificate pair
	identityProblem string          // the problem with the certificate files warned about last
	rolled          bool            // the stream ended for a rollover: reconnect at once

	mu            sync.Mutex
	state         connState
	current       *streamRef         // the open stream, nil between streams
	acked         *hubpb.RegisterAck // the current stream's RegisterAck, nil until it came
	presentedCert *clientIdentity    // the client certificate of the current stream
	rolling       bool               // being rolled over to a renewed client certificate
}

// String names the connection by its hub, never its credentials.
func (c *hubConn) String() string { return "hubConn{" + c.address + "}" }

// Format prints String whatever the verb.
func (c *hubConn) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, c.String()) }

// LogValue is String, for log/slog.
func (c *hubConn) LogValue() slog.Value { return slog.StringValue(c.String()) }

func newHubConn(a *App, address string) *hubConn {
	return &hubConn{
		app:       a,
		address:   address,
		t:         a.cfg.timings,
		log:       a.log.With("hub", address),
		stopping:  make(chan struct{}),
		closing:   make(chan struct{}),
		deregDone: make(chan struct{}),
		done:      make(chan struct{}),
		rollReq:   make(chan *streamRef, 1),
	}
}

// ack is the RegisterAck of the current stream; nil until it is acknowledged.
func (c *hubConn) ack() *hubpb.RegisterAck {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acked
}

// live is the current stream and its RegisterAck; both nil until the stream is acknowledged.
func (c *hubConn) live() (*streamRef, *hubpb.RegisterAck) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.acked == nil {
		return nil, nil
	}
	return c.current, c.acked
}

// stream is the current stream; nil between streams.
func (c *hubConn) stream() *streamRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// currentState is where the connection stands now.
func (c *hubConn) currentState() connState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *hubConn) setState(s connState) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
}

// stop begins the shutdown: the connection opens no stream any more and writes the empty
// Register to a live one.
func (c *hubConn) stop() { c.stopOnce.Do(func() { close(c.stopping) }) }

// close half-closes the stream and waits (bounded) for the hub to finish its side.
func (c *hubConn) close() { c.closeOnce.Do(func() { close(c.closing) }) }

func (c *hubConn) deregistered() { c.deregOnce.Do(func() { close(c.deregDone) }) }

// stopRequested is true once the shutdown began or the app was stopped hard.
func (c *hubConn) stopRequested() bool {
	select {
	case <-c.stopping:
		return true
	case <-c.app.killed:
		return true
	default:
		return false
	}
}

// isRefusal reports whether the hub closing a stream with code refuses the registration.
func isRefusal(code codes.Code) bool {
	switch code {
	case codes.AlreadyExists, codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}

// run is the connection's goroutine: Connecting, AwaitingAck, Live, then Backoff or Rejected,
// and again, until shutdown.
func (c *hubConn) run() {
	defer close(c.done)
	defer c.setState(stateStopped)
	defer c.deregistered()
	wait := c.t.backoffMin // the next ordinary wait, before its jitter
	// The refusals logged at error since the last ack, by code and message: each distinct one
	// is logged at error once, its repeats at debug. Bounded: past refusalsKept distinct
	// refusals, the set starts over.
	logged := map[[2]string]bool{}
	failures := 0     // ordinary closures since the last ack; the first is logged at warn
	tokenLogged := "" // the access token failure logged at error last; a repeat goes to debug
	for !c.stopRequested() {
		c.setState(stateConnecting)
		reached, err := c.runStream()
		if c.stopRequested() {
			return
		}
		if reached {
			// A hub that acknowledged the Register is healthy: come back quickly.
			wait, failures, tokenLogged = c.t.backoffMin, 0, ""
			clear(logged)
		}
		if c.rolled {
			// A client certificate rollover closed the stream on purpose: back at once.
			c.rolled = false
			continue
		}
		st := status.Convert(err)
		if err == nil {
			st = status.New(codes.Unavailable, "the hub ended the stream")
		}
		if st.Code() == codes.Unauthenticated {
			c.retirePresentedToken()
		}
		var noToken *noTokenError
		var pause time.Duration
		switch {
		case errors.As(err, &noToken):
			// No stream was opened: the token endpoint refused or could not be reached. Kept as
			// the latest rejection, retried at the ordinary backoff.
			// TODO(spec-gap): SG-7 (a token source failing before any stream).
			c.setState(stateBackoff)
			c.app.recordRejection(Rejection{Hub: c.address, Code: codeName(codes.Unauthenticated), Message: noToken.msg, At: time.Now()})
			pause = backoff.Jitter(wait, c.t.backoffMax, rand.Float64())
			wait = backoff.Next(wait, c.t.backoffMin, c.t.backoffMax)
			level := slog.LevelError
			if noToken.msg == tokenLogged {
				level = slog.LevelDebug
			}
			tokenLogged = noToken.msg
			c.log.Log(context.Background(), level, "could not get an access token for the hub; no stream opened",
				"reason", noToken.msg, "retry_in", pause)
		case reached && st.Code() == codes.Unauthenticated && c.app.cfg.clientCert != "":
			// The hub ends a certificate session at the certificate's notAfter ("certificate
			// expired"): an ordinary reconnect, which reads the files again, not a refusal.
			// TODO(spec-gap): SG-15 (the exception of the certificate session).
			c.setState(stateBackoff)
			pause = backoff.Jitter(wait, c.t.backoffMax, rand.Float64())
			wait = backoff.Next(wait, c.t.backoffMin, c.t.backoffMax)
			c.log.Info("the hub ended the certificate session; reconnecting with the certificate in the files",
				"reason", st.Message(), "retry_in", pause)
		// TODO(spec-gap): SG-15 (a refusal code counts as a refusal also on an acknowledged stream).
		case isRefusal(st.Code()):
			// TODO(spec-gap): SG-11 (the latest refusal is kept app-wide, never cleared).
			c.setState(stateRejected)
			rej := Rejection{Hub: c.address, Code: codeName(st.Code()), Message: st.Message(), At: time.Now()}
			c.app.recordRejection(rej)
			// TODO(spec-gap): SG-16 (80-100 % of the configured cap, without the 1 s floor).
			pause = backoff.Jitter(c.t.backoffMax, c.t.backoffMax, rand.Float64())
			level, key := slog.LevelError, [2]string{rej.Code, rej.Message}
			if logged[key] {
				level = slog.LevelDebug
			} else {
				if len(logged) >= refusalsKept {
					clear(logged)
				}
				logged[key] = true
			}
			c.log.Log(context.Background(), level, "the hub refused the registration",
				"code", rej.Code, "reason", rej.Message, "retry_in", pause)
		default:
			// TODO(spec-gap): SG-1 (UNAVAILABLE keeps doubling: the floor comes back only after an ack).
			c.setState(stateBackoff)
			pause = backoff.Jitter(wait, c.t.backoffMax, rand.Float64())
			wait = backoff.Next(wait, c.t.backoffMin, c.t.backoffMax)
			failures++
			level := slog.LevelDebug
			if failures == 1 {
				level = slog.LevelWarn
			}
			c.log.Log(context.Background(), level, "the hub stream ended; reconnecting",
				"code", codeName(st.Code()), "reason", st.Message(), "retry_in", pause)
		}
		timer := time.NewTimer(pause)
		select {
		case <-timer.C:
		case <-c.stopping:
			timer.Stop()
			return
		case <-c.app.killed:
			timer.Stop()
			return
		}
	}
}

// runStream runs one stream from its opening to its end. reached reports whether it was
// acknowledged; err is why it ended (nil: shutdown, or the hub finished after the half-close).
func (c *hubConn) runStream() (reached bool, err error) {
	if err := c.resolveToken(); err != nil {
		return false, err
	}
	var tokens <-chan struct{} // closed when the provider publishes a newer access token
	if p := c.app.tokens; p != nil {
		_, tokens = p.follow() // before the fetch: a token published meanwhile is a change to follow
		ctx, cancel := c.stopCtx()
		tok, err := p.token(ctx)
		cancel()
		if err != nil {
			if c.stopRequested() {
				return false, nil
			}
			return false, &noTokenError{msg: fmt.Sprintf("no access token for %s (%s): %v", c.app.cfg.service, c.app.cfg.instance, err)}
		}
		c.jwt, c.sentGen, c.presentedJWT = tok, tok.gen, tok.secret
	}
	var id *clientIdentity
	if c.app.cfg.clientCert != "" {
		if id, err = c.loadIdentity(); err != nil {
			return false, err
		}
	}
	creds, err := c.transport(id)
	if err != nil {
		return false, err
	}
	cc, err := grpc.NewClient("passthrough:///"+c.address,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: c.t.keepaliveTime, Timeout: c.t.keepaliveTimeout, PermitWithoutStream: true,
		}))
	if err != nil {
		return false, status.Errorf(codes.Unavailable, "cannot reach the hub: %v", err)
	}
	defer func() { _ = cc.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handshake := time.NewTimer(c.t.handshakeTimeout) // until the RegisterAck, from the attempt's start
	defer handshake.Stop()
	stream, err := c.open(ctx, cancel, cc, handshake.C)
	if stream == nil {
		return false, err
	}

	ref := newStreamRef(c.app.cfg.writeHook)
	register := c.app.registerMsg()
	register.Credentials = c.credentials()
	permits := newStreamPermits(ref, register.GetInitialPermits(), c.t.permitFlushAfter)
	_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: register}})
	_ = ref.send(heartbeat())
	c.mu.Lock()
	c.state, c.current = stateAwaitingAck, ref
	c.mu.Unlock()

	var wg sync.WaitGroup
	defer func() {
		c.mu.Lock()
		if c.current == ref {
			c.current, c.acked = nil, nil
		}
		c.mu.Unlock()
		ref.end()
		cancel()
		wg.Wait()
	}()
	written := make(chan error, 1)
	inbox := make(chan *hubpb.ServerMessage)
	ended := make(chan error, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		written <- ref.write(ctx, stream)
	}()
	go func() {
		defer wg.Done()
		for {
			m, err := stream.Recv()
			if err != nil {
				ended <- err
				return
			}
			select {
			case inbox <- m:
			case <-ctx.Done():
				return
			}
		}
	}()
	return c.serve(&streamRun{ref: ref, permits: permits, inbox: inbox, ended: ended, written: written,
		handshake: handshake.C, tokens: tokens, identity: id})
}

// streamRun is what serve needs of one stream.
type streamRun struct {
	ref       *streamRef
	permits   *streamPermits
	inbox     <-chan *hubpb.ServerMessage
	ended     <-chan error
	written   <-chan error
	handshake <-chan time.Time
	tokens    <-chan struct{} // closed at a newer access token; nil without JWT
	identity  *clientIdentity // the client certificate presented; nil without one
}

// open starts the Connect stream, giving up at the handshake deadline, at the shutdown and at
// a hard stop (a nil stream and a nil error for those two).
func (c *hubConn) open(ctx context.Context, cancel context.CancelFunc, cc *grpc.ClientConn,
	deadline <-chan time.Time) (hubpb.Hub_ConnectClient, error) {
	type opened struct {
		stream hubpb.Hub_ConnectClient
		err    error
	}
	ch := make(chan opened, 1)
	go func() {
		s, err := hubpb.NewHubClient(cc).Connect(ctx)
		ch <- opened{s, err}
	}()
	var err error
	select {
	case o := <-ch:
		return o.stream, o.err
	case <-deadline:
		err = status.Errorf(codes.DeadlineExceeded, "the hub did not open the stream within %v", c.t.handshakeTimeout)
	case <-c.stopping:
	case <-c.app.killed:
	}
	cancel()
	<-ch
	return nil, err
}

// serve is the stream's loop: the ack, heartbeats and the silent-hub check, the forwarded
// requests (also before the ack), the empty Register and the half-close at shutdown.
// It also follows the credentials: a newer access token, or a changed token file, goes to the
// registered stream as a credentials refresh; and it rolls the stream over to a renewed client
// certificate when asked.
func (c *hubConn) serve(sr *streamRun) (reached bool, err error) {
	ref, permits, inbox, ended, written, handshake := sr.ref, sr.permits, sr.inbox, sr.ended, sr.written, sr.handshake
	tokens := sr.tokens
	interval := c.t.heartbeatBeforeAck
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	tick := ticker.C
	// The silent-hub check, armed once the stream is acknowledged: on each heartbeat tick,
	// before its heartbeat (see hubSilent). The last sign of life is the ack, then each echo;
	// unanswered counts the heartbeats sent since it.
	// See https://snodra.damoreira.com/library-authors/protocol/ (Heartbeats).
	watchSilence := false
	var lastEcho time.Time
	unanswered := 0
	closeTimer := time.NewTimer(time.Hour)
	closeTimer.Stop()
	defer closeTimer.Stop()
	var closeWait <-chan time.Time // armed by the half-close
	drainTimer := time.NewTimer(time.Hour)
	drainTimer.Stop()
	defer drainTimer.Stop()
	// Armed while the half-close waits for the handlers in flight.
	var drained <-chan struct{}
	var drainExpired <-chan time.Time
	stopping, closing := c.stopping, c.closing
	halfClosed := false
	halfClose := func() {
		drained, drainExpired, halfClosed = nil, nil, true
		tick, handshake, watchSilence = nil, nil, false
		c.setState(stateDraining)
		ref.closeSend()
		closeTimer.Reset(c.t.closeWait)
		closeWait = closeTimer.C
	}
	registers, acks, deregisterAt := 1, 0, 0 // Registers written, acks received, the empty one's number
	// A client certificate rollover: the empty Register is written, then the stream's requests
	// are counted (after rollSettle, then every rollPoll) until none is left or rollIdleLimit.
	rollTimer := time.NewTimer(time.Hour)
	rollTimer.Stop()
	defer rollTimer.Stop()
	var rollCheck <-chan time.Time
	var rollUntil time.Time
	rolling := false

	for {
		select {
		case m := <-inbox:
			switch k := m.GetMessage().(type) {
			case *hubpb.ServerMessage_RegisterAck:
				acks++
				if !reached && c.stopRequested() {
					// The shutdown began before the ack: the stream only waits for its
					// half-close and never counts as registered (AwaitingAck ends on shutdown).
					c.log.Debug("ignored a RegisterAck that arrived during the shutdown")
					break
				}
				if !reached {
					reached, handshake = true, nil
					if n := k.RegisterAck.GetHeartbeatIntervalMs(); n > 0 {
						interval = time.Duration(n) * time.Millisecond
						if tick != nil {
							ticker.Reset(interval)
						}
					}
					if !halfClosed {
						watchSilence, lastEcho, unanswered = true, time.Now(), 0
					}
					c.wentLive(ref, k.RegisterAck, interval, sr.identity)
					if p := c.app.tokens; p != nil { // a token published since the Register
						tok, _ := p.follow()
						c.followToken(ref, true, tok)
					}
				}
				if deregisterAt > 0 && acks >= deregisterAt {
					c.deregistered()
				}
			case *hubpb.ServerMessage_Heartbeat:
				// Before the ack an echo confirms nothing; after it, it proves the hub alive.
				if watchSilence {
					lastEcho, unanswered = time.Now(), 0
				}
			case *hubpb.ServerMessage_Request:
				if halfClosed {
					// Nothing can be answered on a half-closed stream.
					c.log.Debug("request dropped: the stream is half-closed", "request", k.Request.GetId())
					break
				}
				c.app.dispatch(k.Request, ref, permits)
			case *hubpb.ServerMessage_Reply:
				a := answer{payload: k.Reply.GetPayload()}
				if e, ok := k.Reply.GetResult().(*hubpb.Reply_Error); ok {
					a = answer{failure: e.Error}
				}
				c.app.calls.reply(k.Reply.GetRequestId(), a)
			case *hubpb.ServerMessage_Error:
				c.app.calls.deliver(k.Error.GetRequestId(), answer{failure: k.Error})
			case *hubpb.ServerMessage_Part:
				c.app.calls.part(k.Part)
			case *hubpb.ServerMessage_End:
				c.app.calls.end(k.End)
			case *hubpb.ServerMessage_Cancel:
				c.app.cancelRequest(k.Cancel.GetRequestId(), k.Cancel.GetReason())
			}
		case err := <-ended:
			if errors.Is(err, io.EOF) {
				if halfClosed {
					return reached, nil
				}
				err = status.Error(codes.Unavailable, "the hub ended the stream")
			}
			return reached, err
		case err := <-written:
			written = nil
			if err != nil {
				return reached, err
			}
		case <-tick:
			if watchSilence {
				if hubSilent(time.Since(lastEcho), unanswered, interval, c.t.silentIntervals) {
					return reached, status.Errorf(codes.Unavailable, "no heartbeat echo for %v", time.Since(lastEcho).Round(time.Millisecond))
				}
				unanswered++
			}
			_ = ref.send(heartbeat())
			if reached && !rolling {
				c.followTokenFile(ref)
			}
		case <-tokens:
			var tok *issuedToken
			tok, tokens = c.app.tokens.follow()
			if !halfClosed {
				c.followToken(ref, reached, tok)
			}
		case r := <-c.rollReq:
			if r != ref || !reached || halfClosed || rolling || stopping == nil {
				break // not this stream's, or it is leaving anyway
			}
			rolling = true
			registers++
			_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: c.emptyRegister()}})
			rollUntil = time.Now().Add(c.t.rollIdleLimit)
			rollTimer.Reset(rollSettle)
			rollCheck = rollTimer.C
		case <-rollCheck:
			idle := c.streamIdle(ref, permits)
			if !idle && time.Now().Before(rollUntil) {
				rollTimer.Reset(rollPoll)
				break
			}
			if !idle {
				c.log.Warn("requests still running after the rollover's wait; closing the hub stream for the certificate rollover anyway",
					"waited", c.t.rollIdleLimit)
			}
			rollCheck, c.rolled = nil, true
			halfClose()
		case <-handshake:
			return false, status.Errorf(codes.DeadlineExceeded, "no RegisterAck within %v", c.t.handshakeTimeout)
		case <-stopping:
			stopping, rollCheck = nil, nil
			if !reached || halfClosed {
				c.deregistered() // nothing acknowledged (or left already), nothing to take out of the routing
				break
			}
			c.setState(stateDraining)
			registers++
			deregisterAt = registers
			_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: c.emptyRegister()}})
		case <-closing:
			// A request this stream forwarded after the shutdown's drain found no handler
			// running is still answered: the half-close waits for the handlers in flight, until
			// the drain's deadline. This loop is the only one dispatching this stream's
			// requests, so none can start between the count and the half-close.
			closing = nil
			c.setState(stateDraining)
			deadline := c.app.drainDeadline()
			if c.app.inflight.count() == 0 || !time.Now().Before(deadline) {
				halfClose()
				break
			}
			drained = c.app.inflight.drained()
			drainTimer.Reset(time.Until(deadline))
			drainExpired = drainTimer.C
		case <-drained:
			if c.app.inflight.count() > 0 { // a request came meanwhile
				drained = c.app.inflight.drained()
				break
			}
			halfClose()
		case <-drainExpired:
			halfClose()
		case <-closeWait:
			c.log.Warn("the hub did not finish the stream in time; closing it")
			return reached, nil
		case <-c.app.killed:
			return reached, nil
		}
	}
}

// wentLive records the stream's RegisterAck, and the client certificate it presents.
func (c *hubConn) wentLive(ref *streamRef, ack *hubpb.RegisterAck, interval time.Duration, id *clientIdentity) {
	c.mu.Lock()
	var before *clientIdentity
	if c.current == ref {
		c.acked = ack
		if c.state == stateAwaitingAck {
			c.state = stateLive
		}
		before, c.presentedCert = c.presentedCert, id
	}
	c.mu.Unlock()
	c.log.Info("registered with the hub", "hub_id", ack.GetHubId(), "heartbeat", interval)
	switch {
	case id == nil:
	case before == nil:
		c.log.Info("presenting the client certificate", "fingerprint", id.fingerprint, "not_after", id.notAfter)
	case before.fingerprint != id.fingerprint:
		c.log.Info("presenting a renewed client certificate", "fingerprint", id.fingerprint, "not_after", id.notAfter)
	}
	c.app.connLive()
}

// streamIdle is true when no handler of the stream's requests runs and none of the requests
// written to it awaits its answer.
func (c *hubConn) streamIdle(ref *streamRef, permits *streamPermits) bool {
	return !permits.busy() && c.app.calls.writtenTo(ref) == 0
}

func heartbeat() *hubpb.ClientMessage {
	return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Heartbeat{
		Heartbeat: &hubpb.Heartbeat{SentAtMs: time.Now().UnixMilli()},
	}}
}

// streamRef is one stream of a hubConn: the frames queued for it (its outbound buffer), written
// in order by its writer. Frames for a stream go to that stream only; once it is gone, send fails.
// A request stays in the buffer until the writer takes it: only then is it written (handed to the
// transport); the requests still in the buffer when the stream ends were never sent.
type streamRef struct {
	mu        sync.Mutex
	queue     []outFrame
	requests  int  // requests in queue
	closed    bool // no more frames: half-closed or gone
	halfClose bool // the half-close is due once the queue is written
	ended     bool // the stream is gone
	wake      chan struct{}
	room      chan struct{} // closed when a request may have room; nil while no send waits
	gone      chan struct{} // closed when the stream is gone
	hook      func(ctx context.Context, m *hubpb.ClientMessage)
}

// outFrame is a queued frame, with the call of a request (nil for the library's own frames).
type outFrame struct {
	m    *hubpb.ClientMessage
	call *call
}

// newStreamRef is a stream's buffer; hook (tests) runs before the writer takes each frame.
func newStreamRef(hook func(ctx context.Context, m *hubpb.ClientMessage)) *streamRef {
	return &streamRef{wake: make(chan struct{}, 1), gone: make(chan struct{}), hook: hook}
}

func (r *streamRef) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// freeRoom wakes the sends waiting for room. Called with r.mu held.
func (r *streamRef) freeRoom() {
	if r.room != nil {
		close(r.room)
		r.room = nil
	}
}

// send queues m, a frame of the library's own (heartbeat, reply, permits, ...), for this stream
// only, at once; an error once the stream is gone or half-closed.
func (r *streamRef) send(m *hubpb.ClientMessage) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errStreamGone
	}
	r.queue = append(r.queue, outFrame{m: m})
	r.mu.Unlock()
	r.signal()
	return nil
}

// sendRequest queues the request m of c for this stream, waiting while outboundLimit requests
// wait in the buffer, until ctx ends; errStreamGone once the stream is gone or half-closed.
func (r *streamRef) sendRequest(ctx context.Context, m *hubpb.ClientMessage, c *call) error {
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return errStreamGone
		}
		if r.requests < outboundLimit {
			r.queue = append(r.queue, outFrame{m: m, call: c})
			r.requests++
			r.mu.Unlock()
			r.signal()
			return nil
		}
		if r.room == nil {
			r.room = make(chan struct{})
		}
		room := r.room
		r.mu.Unlock()
		select {
		case <-room:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// withdraw takes c's request out of the buffer when the writer has not taken it; true when it
// was never written (withdrawn now, or dropped with the stream).
func (r *streamRef) withdraw(c *call) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.written {
		return false
	}
	for i, f := range r.queue {
		if f.call == c {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			r.requests--
			r.freeRoom()
			break
		}
	}
	return true
}

// closeSend half-closes the stream once what is queued is written; later sends fail.
func (r *streamRef) closeSend() {
	r.mu.Lock()
	if !r.closed {
		r.closed, r.halfClose = true, true
		r.freeRoom()
	}
	r.mu.Unlock()
	r.signal()
}

// end marks the stream gone: the frames still queued are dropped (never sent), later sends fail
// and gone is closed.
func (r *streamRef) end() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed, r.halfClose = true, false
	r.queue, r.requests = nil, 0
	r.freeRoom()
	if !r.ended {
		r.ended = true
		close(r.gone)
	}
}

// next waits for the next frame to write (half true when the half-close is due) and takes it
// from the buffer (a request is written from then on); ok is false when done closed first. The
// hook, when set, runs before each frame is taken, with ctx.
func (r *streamRef) next(ctx context.Context, done <-chan struct{}) (m *hubpb.ClientMessage, half, ok bool) {
	var hooked *hubpb.ClientMessage
	for {
		r.mu.Lock()
		if len(r.queue) > 0 {
			f := r.queue[0]
			if r.hook != nil && f.m != hooked {
				r.mu.Unlock()
				r.hook(ctx, f.m)
				hooked = f.m
				select {
				case <-done:
					return nil, false, false
				default:
				}
				continue
			}
			r.queue[0] = outFrame{}
			r.queue = r.queue[1:]
			if f.call != nil {
				f.call.written = true
				f.call.on.Store(r)
				r.requests--
				r.freeRoom()
			}
			r.mu.Unlock()
			return f.m, false, true
		}
		if r.halfClose {
			r.halfClose = false
			r.mu.Unlock()
			return nil, true, true
		}
		r.mu.Unlock()
		select {
		case <-r.wake:
		case <-done:
			return nil, false, false
		}
	}
}

// write is the stream's writer: it hands the queued frames to the transport in order, then
// half-closes when asked. It returns nil when the stream ended (the reader learns why) or
// after the half-close, and the error of a frame the transport refused.
func (r *streamRef) write(ctx context.Context, stream hubpb.Hub_ConnectClient) error {
	for {
		m, half, ok := r.next(ctx, ctx.Done())
		switch {
		case !ok:
			return nil
		case half:
			return stream.CloseSend()
		}
		if err := stream.Send(m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil // the stream ended: its status comes from Recv
			}
			return err
		}
	}
}

// codeNames are the gRPC status codes as the protocol pages write them.
var codeNames = map[codes.Code]string{
	codes.OK: "OK", codes.Canceled: "CANCELLED", codes.Unknown: "UNKNOWN",
	codes.InvalidArgument: "INVALID_ARGUMENT", codes.DeadlineExceeded: "DEADLINE_EXCEEDED",
	codes.NotFound: "NOT_FOUND", codes.AlreadyExists: "ALREADY_EXISTS",
	codes.PermissionDenied: "PERMISSION_DENIED", codes.ResourceExhausted: "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION", codes.Aborted: "ABORTED",
	codes.OutOfRange: "OUT_OF_RANGE", codes.Unimplemented: "UNIMPLEMENTED",
	codes.Internal: "INTERNAL", codes.Unavailable: "UNAVAILABLE", codes.DataLoss: "DATA_LOSS",
	codes.Unauthenticated: "UNAUTHENTICATED",
}

// codeName is code in upper snake case ("ALREADY_EXISTS").
func codeName(code codes.Code) string {
	if n, ok := codeNames[code]; ok {
		return n
	}
	return code.String()
}

// hubSilent decides, on a heartbeat tick, whether an acknowledged stream is silent: more than
// intervals heartbeat intervals since the last echo (the reference library's comparison), and
// intervals heartbeats sent since that echo all unanswered. The second condition keeps a tick
// that fires a little late, nominally exactly three intervals after the last echoed heartbeat,
// from dropping the stream one tick early: the drop comes on the tick after the third
// unanswered heartbeat, between three and four intervals after the last echo.
// TODO(spec-gap): SG-19 (the drop moment within "no echo for three heartbeat intervals").
func hubSilent(since time.Duration, unanswered int, interval time.Duration, intervals int) bool {
	return unanswered >= intervals && since > interval*time.Duration(intervals)
}
