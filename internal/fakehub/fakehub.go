// Package fakehub is an in-process gRPC server implementing Hub.Connect, scripted frame by
// frame by unit tests, so the library's suite runs without a real hub.
//
// A test starts a [Hub], lets the library under test connect, takes each connection with
// [Hub.Next] and then reads what the library wrote ([Stream.Recv] and the Recv helpers) and
// writes what the hub would ([Stream.Send], [Stream.Ack], [Stream.Close]). Nothing is timed by
// the hub itself: no deadline, no eviction, no queue. Every wait in this package is a bounded
// wait for something the script expects, never a pause for synchronisation.
//
// Where it matters the hub behaves as the reference hub does (see the "How the runner's hub
// differs" section of https://snodra.damoreira.com/library-authors/conformance/): a first frame
// that is not a Register closes the stream with FAILED_PRECONDITION; a Register with an empty
// identity, without permits, or with a reserved name closes it with INVALID_ARGUMENT; the
// RegisterAck is written only when the script sends it and heartbeat echoes only follow it.
// A refused stream is still handed to [Hub.Next], with the offending frame readable, before it
// is closed.
package fakehub

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// bound is how long the script waits for a stream or a frame it expects.
const bound = 5 * time.Second

// Option configures [Start].
type Option func(*Hub)

// WithTLS serves TLS with cert. With clientCAs non-nil a client certificate verified against
// it is required (mutual TLS) and exposed as [Stream].PeerCertificates; nil asks for none.
func WithTLS(cert tls.Certificate, clientCAs *x509.CertPool) Option {
	return func(h *Hub) {
		cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		if clientCAs != nil {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
			cfg.ClientCAs = clientCAs
		}
		h.creds = credentials.NewTLS(cfg)
	}
}

// Hub is a scripted fake hub listening on 127.0.0.1.
type Hub struct {
	hubpb.UnimplementedHubServer

	addr    string
	creds   credentials.TransportCredentials
	streams chan *Stream

	mu      sync.Mutex
	srv     *grpc.Server
	served  chan struct{}
	count   int
	refuse  bool
	refCode codes.Code
	refMsg  string

	connecting atomic.Int32 // Connect handlers running (tests of the package)
	buffer     int          // streams waiting for Next before Connect blocks
}

// withStreamBuffer sets how many streams wait for Next before Connect blocks (tests).
func withStreamBuffer(n int) Option { return func(h *Hub) { h.buffer = n } }

// Start listens on 127.0.0.1:0 and serves until the test ends.
func Start(t testing.TB, opts ...Option) *Hub {
	t.Helper()
	h := &Hub{buffer: 1024}
	for _, o := range opts {
		o(h)
	}
	h.streams = make(chan *Stream, h.buffer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakehub: listen: %v", err)
	}
	h.addr = ln.Addr().String()
	h.serve(ln)
	t.Cleanup(h.stop)
	return h
}

func (h *Hub) serve(ln net.Listener) {
	var opts []grpc.ServerOption
	if h.creds != nil {
		opts = append(opts, grpc.Creds(h.creds))
	}
	srv := grpc.NewServer(opts...)
	hubpb.RegisterHubServer(srv, h)
	served := make(chan struct{})
	h.srv, h.served = srv, served
	go func() {
		defer close(served)
		_ = srv.Serve(ln)
	}()
}

func (h *Hub) stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopLocked()
}

func (h *Hub) stopLocked() {
	if h.srv == nil {
		return
	}
	h.srv.Stop()
	<-h.served
	h.srv, h.served = nil, nil
}

// Addr is the host:port the hub listens on; it stays the same across Down and Up.
func (h *Hub) Addr() string { return h.addr }

// Next returns the next Connect stream, in arrival order. It fails the test after 5 s.
func (h *Hub) Next(t testing.TB) *Stream {
	t.Helper()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case s := <-h.streams:
		return s
	case <-timer.C:
		t.Fatalf("fakehub: no stream within %v", bound)
		return nil
	}
}

// NoStreamWithin fails the test if a stream arrives within d.
func (h *Hub) NoStreamWithin(t testing.TB, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case s := <-h.streams:
		t.Fatalf("fakehub: unexpected stream %d within %v", s.Index, d)
	case <-timer.C:
	}
}

// Refuse closes every new stream at once with code and msg, without handing it to Next,
// until [Hub.Accept].
func (h *Hub) Refuse(code codes.Code, msg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.refuse, h.refCode, h.refMsg = true, code, msg
}

// Accept ends [Hub.Refuse].
func (h *Hub) Accept() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.refuse = false
}

// Down closes the listener, so new connections are refused at the TCP level, and severs the
// live connections (their streams end Unavailable), as a hub that went away does. Streams
// already handed to Next stay readable for what they received.
func (h *Hub) Down() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopLocked()
}

// Up reopens the listener on the same address. It is a no-op while the hub is up.
func (h *Hub) Up() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.srv != nil {
		return
	}
	ln, err := net.Listen("tcp", h.addr)
	if err != nil {
		panic(fmt.Sprintf("fakehub: cannot listen again on %s: %v", h.addr, err))
	}
	h.serve(ln)
}

// Topology answers an empty snapshot.
func (h *Hub) Topology(_ context.Context, _ *hubpb.TopologyRequest) (*hubpb.TopologyResponse, error) {
	return &hubpb.TopologyResponse{}, nil
}

// Connect hands the stream to the script and holds it open until the script closes it or the
// client leaves.
func (h *Hub) Connect(srv grpc.BidiStreamingServer[hubpb.ClientMessage, hubpb.ServerMessage]) error {
	h.connecting.Add(1)
	defer h.connecting.Add(-1)
	h.mu.Lock()
	if h.refuse {
		code, msg := h.refCode, h.refMsg
		h.mu.Unlock()
		return status.Error(code, msg)
	}
	idx := h.count
	h.count++
	h.mu.Unlock()

	s := &Stream{
		Index:  idx,
		srv:    srv,
		in:     newInbox(),
		done:   make(chan struct{}),
		halfCl: make(chan struct{}),
	}
	if p, ok := peer.FromContext(srv.Context()); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			s.PeerCertificates = ti.State.PeerCertificates
		}
	}
	select {
	case h.streams <- s:
	case <-srv.Context().Done(): // the client left before the script took the stream
		return status.FromContextError(srv.Context().Err()).Err()
	}
	go s.pump()
	select {
	case <-s.done:
		return s.err
	case <-srv.Context().Done():
		return status.FromContextError(srv.Context().Err()).Err()
	}
}

// inbox is an unbounded queue of the frames the library wrote, in order.
type inbox struct {
	mu     sync.Mutex
	q      []*hubpb.ClientMessage
	ended  bool
	err    error
	sig    chan struct{}
	endedC chan struct{} // closed by end
}

func newInbox() *inbox { return &inbox{sig: make(chan struct{}, 1), endedC: make(chan struct{})} }

func (b *inbox) wake() {
	select {
	case b.sig <- struct{}{}:
	default:
	}
}

func (b *inbox) push(m *hubpb.ClientMessage) {
	b.mu.Lock()
	b.q = append(b.q, m)
	b.mu.Unlock()
	b.wake()
}

// end marks the stream ended with err (the first call wins).
func (b *inbox) end(err error) {
	b.mu.Lock()
	if !b.ended {
		b.ended, b.err = true, err
		close(b.endedC)
	}
	b.mu.Unlock()
	b.wake()
}

// endErr is why the stream ended; call it once endedC is closed.
func (b *inbox) endErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// ended describes the end of a stream for a failed script step.
func ended(err error) string {
	if err == nil || errors.Is(err, io.EOF) {
		return "fakehub: stream ended: the client half-closed (EOF)"
	}
	return "fakehub: stream ended: " + err.Error()
}

// pop waits for the next frame; ok is false, with err, when the stream ended, and timedOut
// is true when d elapsed first.
func (b *inbox) pop(d time.Duration) (m *hubpb.ClientMessage, err error, timedOut bool) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		b.mu.Lock()
		if len(b.q) > 0 {
			m = b.q[0]
			b.q = b.q[1:]
			b.mu.Unlock()
			return m, nil, false
		}
		ended, e := b.ended, b.err
		b.mu.Unlock()
		if ended {
			return nil, e, false
		}
		select {
		case <-b.sig:
		case <-timer.C:
			return nil, nil, true
		}
	}
}

// Stream is one Connect stream. The Recv family and Nothing belong to the script's goroutine;
// Send, Ack, Close and EchoHeartbeats may be called from any goroutine.
type Stream struct {
	// Index counts the streams handed out, from 0.
	Index int
	// PeerCertificates is the client certificate chain, when the hub runs mutual TLS.
	PeerCertificates []*x509.Certificate

	srv    grpc.BidiStreamingServer[hubpb.ClientMessage, hubpb.ServerMessage]
	in     *inbox
	pushed []*hubpb.ClientMessage // frames Nothing took and Recv has yet to return
	done   chan struct{}
	halfCl chan struct{}

	closeOnce sync.Once
	err       error

	sendMu  sync.Mutex
	acked   bool
	echo    echoMode
	hbs     []time.Time
	statuss []*hubpb.InstanceStatus
}

type echoMode int

const (
	echoAfterAck echoMode = iota
	echoAlways
	echoNever
)

func (s *Stream) pump() {
	first := true
	for {
		m, err := s.srv.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				close(s.halfCl)
			}
			s.in.end(err)
			return
		}
		s.observe(m)
		s.in.push(m)
		if first {
			first = false
			if code, msg := checkFirst(m); code != codes.OK {
				s.Close(code, msg)
				s.in.end(status.Error(code, msg)) // the script learns of it at once
				return
			}
		}
	}
}

// observe records heartbeats and statuses and echoes a heartbeat when the mode says so,
// before the frame is queued for the script: a script that has consumed a heartbeat knows
// whether it was echoed.
func (s *Stream) observe(m *hubpb.ClientMessage) {
	switch k := m.GetMessage().(type) {
	case *hubpb.ClientMessage_Heartbeat:
		s.sendMu.Lock()
		defer s.sendMu.Unlock()
		s.hbs = append(s.hbs, time.Now())
		if s.echo == echoAlways || (s.echo == echoAfterAck && s.acked) {
			_ = s.srv.Send(&hubpb.ServerMessage{Message: &hubpb.ServerMessage_Heartbeat{
				Heartbeat: &hubpb.Heartbeat{SentAtMs: k.Heartbeat.GetSentAtMs()},
			}})
		}
	case *hubpb.ClientMessage_Status:
		s.sendMu.Lock()
		s.statuss = append(s.statuss, k.Status)
		s.sendMu.Unlock()
	}
}

func checkFirst(m *hubpb.ClientMessage) (codes.Code, string) {
	r := m.GetRegister()
	switch {
	case r == nil:
		return codes.FailedPrecondition, "the first message must be a Register"
	case r.GetServiceName() == "":
		return codes.InvalidArgument, "service_name must not be empty"
	case r.GetInstanceId() == "":
		return codes.InvalidArgument, "instance_id must not be empty"
	case r.GetInitialPermits() == 0:
		return codes.InvalidArgument, "initial_permits must be at least 1"
	case r.GetServiceName() == "snodra-server":
		return codes.InvalidArgument, "service_name is reserved"
	case strings.HasPrefix(r.GetInstanceId(), "snodra-server/"):
		return codes.InvalidArgument, "instance_id is reserved"
	}
	return codes.OK, ""
}

// Recv returns the next frame the library wrote, of any kind, heartbeats and statuses
// included. It fails the test after 5 s or when the stream has ended.
func (s *Stream) Recv(t testing.TB) *hubpb.ClientMessage {
	t.Helper()
	return s.recvBy(t, time.Now().Add(bound))
}

// recvBy is Recv, failing the test at deadline.
func (s *Stream) recvBy(t testing.TB, deadline time.Time) *hubpb.ClientMessage {
	t.Helper()
	if len(s.pushed) > 0 {
		m := s.pushed[0]
		s.pushed = s.pushed[1:]
		return m
	}
	m, err, timedOut := s.in.pop(time.Until(deadline))
	switch {
	case timedOut:
		t.Fatalf("fakehub: no frame within %v", bound)
	case m == nil:
		t.Fatalf("%s (before the expected frame)", ended(err))
	}
	return m
}

func kindOf(m *hubpb.ClientMessage) string {
	switch m.GetMessage().(type) {
	case *hubpb.ClientMessage_Register:
		return "register"
	case *hubpb.ClientMessage_Request:
		return "request"
	case *hubpb.ClientMessage_Reply:
		return "reply"
	case *hubpb.ClientMessage_Heartbeat:
		return "heartbeat"
	case *hubpb.ClientMessage_Permits:
		return "permits"
	case *hubpb.ClientMessage_Part:
		return "part"
	case *hubpb.ClientMessage_End:
		return "end"
	case *hubpb.ClientMessage_Flow:
		return "flow"
	case *hubpb.ClientMessage_Cancel:
		return "cancel"
	case *hubpb.ClientMessage_Status:
		return "status"
	case *hubpb.ClientMessage_Credentials:
		return "credentials"
	case *hubpb.ClientMessage_ControlReply:
		return "control_reply"
	}
	return "empty"
}

var knownKinds = map[string]bool{
	"register": true, "request": true, "reply": true, "heartbeat": true, "permits": true,
	"part": true, "end": true, "flow": true, "cancel": true, "status": true,
	"credentials": true, "control_reply": true,
}

// recvKind returns the next frame that is not a Heartbeat or an InstanceStatus and fails the
// test unless it is of the wanted kind; the 5 s bound covers the whole wait, heartbeats and
// statuses included.
func recvKind[T any](t testing.TB, s *Stream, want string, get func(*hubpb.ClientMessage) T) T {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		m := s.recvBy(t, deadline)
		switch m.GetMessage().(type) {
		case *hubpb.ClientMessage_Heartbeat, *hubpb.ClientMessage_Status:
			continue
		}
		if kindOf(m) != want {
			var zero T
			t.Fatalf("fakehub: expected a %s frame, got %s: %v", want, kindOf(m), m)
			return zero
		}
		return get(m)
	}
}

// RecvRegister returns the next Register, skipping Heartbeat and InstanceStatus frames; the
// other Recv helpers do the same. Any other kind of frame fails the test.
func (s *Stream) RecvRegister(t testing.TB) *hubpb.Register {
	t.Helper()
	return recvKind(t, s, "register", (*hubpb.ClientMessage).GetRegister)
}

// RecvRequest returns the next Request.
func (s *Stream) RecvRequest(t testing.TB) *hubpb.Request {
	t.Helper()
	return recvKind(t, s, "request", (*hubpb.ClientMessage).GetRequest)
}

// RecvReply returns the next Reply.
func (s *Stream) RecvReply(t testing.TB) *hubpb.Reply {
	t.Helper()
	return recvKind(t, s, "reply", (*hubpb.ClientMessage).GetReply)
}

// RecvPermits returns the next Permits.
func (s *Stream) RecvPermits(t testing.TB) *hubpb.Permits {
	t.Helper()
	return recvKind(t, s, "permits", (*hubpb.ClientMessage).GetPermits)
}

// RecvPart returns the next ReplyPart.
func (s *Stream) RecvPart(t testing.TB) *hubpb.ReplyPart {
	t.Helper()
	return recvKind(t, s, "part", (*hubpb.ClientMessage).GetPart)
}

// RecvEnd returns the next ReplyEnd.
func (s *Stream) RecvEnd(t testing.TB) *hubpb.ReplyEnd {
	t.Helper()
	return recvKind(t, s, "end", (*hubpb.ClientMessage).GetEnd)
}

// RecvFlow returns the next Flow.
func (s *Stream) RecvFlow(t testing.TB) *hubpb.Flow {
	t.Helper()
	return recvKind(t, s, "flow", (*hubpb.ClientMessage).GetFlow)
}

// RecvCancel returns the next Cancel.
func (s *Stream) RecvCancel(t testing.TB) *hubpb.Cancel {
	t.Helper()
	return recvKind(t, s, "cancel", (*hubpb.ClientMessage).GetCancel)
}

// RecvCredentials returns the next Credentials refresh.
func (s *Stream) RecvCredentials(t testing.TB) *hubpb.Credentials {
	t.Helper()
	return recvKind(t, s, "credentials", (*hubpb.ClientMessage).GetCredentials)
}

// Heartbeats returns the arrival time of every Heartbeat received so far.
func (s *Stream) Heartbeats() []time.Time {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return append([]time.Time(nil), s.hbs...)
}

// Statuses returns every InstanceStatus received so far.
func (s *Stream) Statuses() []*hubpb.InstanceStatus {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return append([]*hubpb.InstanceStatus(nil), s.statuss...)
}

// Nothing fails the test if a frame of one of kinds arrives within d. Kinds are the oneof
// names in lower case: register, request, reply, heartbeat, permits, part, end, flow, cancel,
// status, credentials, control_reply. With no kinds it watches every kind except heartbeat and
// status. Frames of other kinds that arrive meanwhile stay available to Recv. On a stream that
// ended it returns at once (nothing more can arrive). This is the one place the script waits
// for time to pass, because it asserts an absence.
func (s *Stream) Nothing(t testing.TB, d time.Duration, kinds ...string) {
	t.Helper()
	watch := map[string]bool{}
	for _, k := range kinds {
		if !knownKinds[k] {
			t.Fatalf("fakehub: unknown frame kind %q", k)
		}
		watch[k] = true
	}
	deadline := time.Now().Add(d)
	for {
		m, _, timedOut := s.in.pop(time.Until(deadline))
		if timedOut || m == nil {
			return
		}
		k := kindOf(m)
		if watch[k] || (len(watch) == 0 && k != "heartbeat" && k != "status") {
			t.Fatalf("fakehub: unexpected %s frame within %v: %v", k, d, m)
			return
		}
		s.pushed = append(s.pushed, m)
	}
}

// Send writes a frame to the library.
func (s *Stream) Send(t testing.TB, m *hubpb.ServerMessage) {
	t.Helper()
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.srv.Send(m); err != nil {
		t.Fatalf("fakehub: send: %v", err)
	}
}

// Ack writes a RegisterAck and from then on echoes heartbeats (unless [Stream.EchoHeartbeats]
// says otherwise). The ack and the echo of a later heartbeat are written in that order.
func (s *Stream) Ack(t testing.TB, a *hubpb.RegisterAck) {
	t.Helper()
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	err := s.srv.Send(&hubpb.ServerMessage{Message: &hubpb.ServerMessage_RegisterAck{RegisterAck: a}})
	if err != nil {
		t.Fatalf("fakehub: ack: %v", err)
	}
	s.acked = true
}

// EchoHeartbeats overrides the default (echo only after the ack): true echoes every
// heartbeat received from now on, false none. A heartbeat is judged when it arrives, so to
// script one before the ack deterministically, consume it with [Stream.Recv] first.
func (s *Stream) EchoHeartbeats(on bool) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if on {
		s.echo = echoAlways
	} else {
		s.echo = echoNever
	}
}

// Close ends the stream with the status code and message (codes.OK ends it cleanly).
func (s *Stream) Close(code codes.Code, msg string) {
	s.closeOnce.Do(func() {
		if code != codes.OK {
			s.err = status.Error(code, msg)
		}
		close(s.done)
	})
}

// HalfClosed fails the test unless the library half-closed its side within d; it fails at
// once when the stream ended otherwise.
func (s *Stream) HalfClosed(t testing.TB, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-s.halfCl:
	case <-s.in.endedC:
		select {
		case <-s.halfCl: // closed before the end is recorded
		default:
			t.Fatalf("%s (no half-close)", ended(s.in.endErr()))
		}
	case <-timer.C:
		t.Fatalf("fakehub: the client did not half-close within %v", d)
	}
}

// Ack is a RegisterAck with the reference hub's defaults: heartbeat every 3000 ms, eviction
// after 10000 ms, request timeout 5000 ms clamped at 60000 ms, context "default".
func Ack(hubID string) *hubpb.RegisterAck {
	return &hubpb.RegisterAck{
		HubId:               hubID,
		HeartbeatIntervalMs: 3000,
		HeartbeatTimeoutMs:  10000,
		DefaultTimeoutMs:    5000,
		MaxTimeoutMs:        60000,
		Context:             "default",
	}
}

// Forward is a frame delivering r to the library's handler.
func Forward(r *hubpb.Request) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Request{Request: r}}
}

// Flow grants n credits on request id.
func Flow(id string, n uint32) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Flow{Flow: &hubpb.Flow{RequestId: id, Credits: n}}}
}

// Cancel cancels request id.
func Cancel(id, reason string) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Cancel{Cancel: &hubpb.Cancel{RequestId: id, Reason: reason}}}
}

// HubError is a failure the hub detected for request id.
func HubError(id string, c hubpb.ErrorCode, msg string) *hubpb.ServerMessage {
	return &hubpb.ServerMessage{Message: &hubpb.ServerMessage_Error{Error: &hubpb.Error{RequestId: id, Code: c, Message: msg}}}
}
