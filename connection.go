package snodramessaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

	mu      sync.Mutex
	state   connState
	current *streamRef         // the open stream, nil between streams
	acked   *hubpb.RegisterAck // the current stream's RegisterAck, nil until it came
}

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
	}
}

// ack is the RegisterAck of the current stream; nil until it is acknowledged.
func (c *hubConn) ack() *hubpb.RegisterAck {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acked
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
	failures := 0 // ordinary closures since the last ack; the first is logged at warn
	for !c.stopRequested() {
		c.setState(stateConnecting)
		reached, err := c.runStream()
		if c.stopRequested() {
			return
		}
		if reached {
			// A hub that acknowledged the Register is healthy: come back quickly.
			wait, failures = c.t.backoffMin, 0
			clear(logged)
		}
		st := status.Convert(err)
		if err == nil {
			st = status.New(codes.Unavailable, "the hub ended the stream")
		}
		var pause time.Duration
		// TODO(spec-gap): SG-15 (a refusal code counts as a refusal also on an acknowledged stream).
		if isRefusal(st.Code()) {
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
		} else {
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
	cc, err := grpc.NewClient("passthrough:///"+c.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	ref := newStreamRef()
	_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: c.app.registerMsg()}})
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
	return c.serve(ref, inbox, ended, written, handshake.C)
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

// serve is the stream's loop: the ack, heartbeats and the silent-hub check, the empty Register
// and the half-close at shutdown.
func (c *hubConn) serve(ref *streamRef, inbox <-chan *hubpb.ServerMessage, ended, written <-chan error,
	handshake <-chan time.Time) (reached bool, err error) {
	interval := c.t.heartbeatBeforeAck
	silentAfter := func() time.Duration { return interval * time.Duration(c.t.silentIntervals) }
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	tick := ticker.C
	silent := time.NewTimer(time.Hour)
	silent.Stop()
	defer silent.Stop()
	var silentC <-chan time.Time // armed once the stream is acknowledged
	closeTimer := time.NewTimer(time.Hour)
	closeTimer.Stop()
	defer closeTimer.Stop()
	var closeWait <-chan time.Time // armed by the half-close
	stopping, closing := c.stopping, c.closing
	halfClosed := false
	registers, acks, deregisterAt := 1, 0, 0 // Registers written, acks received, the empty one's number

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
						silent.Reset(silentAfter())
						silentC = silent.C
					}
					c.wentLive(ref, k.RegisterAck, interval)
				}
				if deregisterAt > 0 && acks >= deregisterAt {
					c.deregistered()
				}
			case *hubpb.ServerMessage_Heartbeat:
				// Before the ack an echo confirms nothing; after it, it proves the hub alive.
				if silentC != nil {
					silent.Reset(silentAfter())
				}
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
			_ = ref.send(heartbeat())
		case <-handshake:
			return false, status.Errorf(codes.DeadlineExceeded, "no RegisterAck within %v", c.t.handshakeTimeout)
		case <-silentC:
			return reached, status.Errorf(codes.Unavailable, "no heartbeat echo for %v", silentAfter())
		case <-stopping:
			stopping = nil
			if !reached {
				c.deregistered() // nothing acknowledged, nothing to take out of the routing
				break
			}
			c.setState(stateDraining)
			registers++
			deregisterAt = registers
			_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: c.app.emptyRegisterMsg()}})
		case <-closing:
			closing, halfClosed = nil, true
			tick, silentC, handshake = nil, nil, nil
			c.setState(stateDraining)
			ref.closeSend()
			closeTimer.Reset(c.t.closeWait)
			closeWait = closeTimer.C
		case <-closeWait:
			c.log.Warn("the hub did not finish the stream in time; closing it")
			return reached, nil
		case <-c.app.killed:
			return reached, nil
		}
	}
}

// wentLive records the stream's RegisterAck.
func (c *hubConn) wentLive(ref *streamRef, ack *hubpb.RegisterAck, interval time.Duration) {
	c.mu.Lock()
	if c.current == ref {
		c.acked = ack
		if c.state == stateAwaitingAck {
			c.state = stateLive
		}
	}
	c.mu.Unlock()
	c.log.Info("registered with the hub", "hub_id", ack.GetHubId(), "heartbeat", interval)
	c.app.connLive()
}

func heartbeat() *hubpb.ClientMessage {
	return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Heartbeat{
		Heartbeat: &hubpb.Heartbeat{SentAtMs: time.Now().UnixMilli()},
	}}
}

// streamRef is one stream of a hubConn: the frames queued for it, written in order by its
// writer. Frames for a stream go to that stream only; once it is gone, send fails.
type streamRef struct {
	mu        sync.Mutex
	queue     []*hubpb.ClientMessage
	closed    bool // no more frames: half-closed or gone
	halfClose bool // the half-close is due once the queue is written
	wake      chan struct{}
}

func newStreamRef() *streamRef { return &streamRef{wake: make(chan struct{}, 1)} }

func (r *streamRef) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// send queues m for this stream only; an error once the stream is gone or half-closed.
func (r *streamRef) send(m *hubpb.ClientMessage) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errStreamGone
	}
	r.queue = append(r.queue, m)
	r.mu.Unlock()
	r.signal()
	return nil
}

// closeSend half-closes the stream once what is queued is written; later sends fail.
func (r *streamRef) closeSend() {
	r.mu.Lock()
	if !r.closed {
		r.closed, r.halfClose = true, true
	}
	r.mu.Unlock()
	r.signal()
}

// end marks the stream gone and returns the frames never handed to the transport.
func (r *streamRef) end() []*hubpb.ClientMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	unsent := r.queue
	r.closed, r.halfClose, r.queue = true, false, nil
	return unsent
}

// next waits for the next frame to write (half true when the half-close is due); ok is false
// when done closed first.
func (r *streamRef) next(done <-chan struct{}) (m *hubpb.ClientMessage, half, ok bool) {
	for {
		r.mu.Lock()
		if len(r.queue) > 0 {
			m = r.queue[0]
			r.queue[0] = nil
			r.queue = r.queue[1:]
			r.mu.Unlock()
			return m, false, true
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
		m, half, ok := r.next(ctx.Done())
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
