package fakehub

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func dial(t testing.TB, addr string, opts ...grpc.DialOption) hubpb.Hub_ConnectClient {
	t.Helper()
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	c, err := hubpb.NewHubClient(conn).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func register(service, instance string) *hubpb.ClientMessage {
	return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: &hubpb.Register{
		ServiceName: service, InstanceId: instance, InitialPermits: 10,
	}}}
}

func heartbeat(ms int64) *hubpb.ClientMessage {
	return &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Heartbeat{Heartbeat: &hubpb.Heartbeat{SentAtMs: ms}}}
}

func send(t testing.TB, c hubpb.Hub_ConnectClient, m *hubpb.ClientMessage) {
	t.Helper()
	if err := c.Send(m); err != nil {
		t.Fatal(err)
	}
}

func recv(t testing.TB, c hubpb.Hub_ConnectClient) *hubpb.ServerMessage {
	t.Helper()
	m, err := c.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	return m
}

func recvErr(t testing.TB, c hubpb.Hub_ConnectClient) error {
	t.Helper()
	m, err := c.Recv()
	if err == nil {
		t.Fatalf("recv: got a frame %v, want an error", m)
	}
	return err
}

func TestAStreamIsScriptedFrameByFrame(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	send(t, c, heartbeat(1))
	s := hub.Next(t)
	is(t, s.RecvRegister(t).GetServiceName(), "orders-service")
	is(t, s.Index, 0)
	// Consuming the early heartbeat proves it reached the hub before the ack.
	is(t, s.Recv(t).GetHeartbeat().GetSentAtMs(), int64(1))
	s.Ack(t, Ack("hub-1"))
	is(t, recv(t, c).GetRegisterAck().GetHubId(), "hub-1")
	send(t, c, heartbeat(7))
	is(t, recv(t, c).GetHeartbeat().GetSentAtMs(), int64(7)) // echo after the ack
	s.Close(codes.AlreadyExists, "taken")
	is(t, status.Code(recvErr(t, c)), codes.AlreadyExists)
}

func TestAckDefaults(t *testing.T) {
	a := Ack("hub-9")
	is(t, a.GetHubId(), "hub-9")
	is(t, a.GetHeartbeatIntervalMs(), uint32(3000))
	is(t, a.GetHeartbeatTimeoutMs(), uint32(10000))
	is(t, a.GetDefaultTimeoutMs(), uint32(5000))
	is(t, a.GetMaxTimeoutMs(), uint32(60000))
	is(t, a.GetContext(), "default")
}

func TestFrameBuilders(t *testing.T) {
	r := &hubpb.Request{Id: "r-1", Type: "orders.PlaceOrder"}
	is(t, Forward(r).GetRequest(), r)
	is(t, Flow("r-1", 5).GetFlow().GetCredits(), uint32(5))
	is(t, Flow("r-1", 5).GetFlow().GetRequestId(), "r-1")
	is(t, Cancel("r-1", "gone").GetCancel().GetReason(), "gone")
	e := HubError("r-1", hubpb.ErrorCode_NO_HANDLER, "nobody").GetError()
	is(t, e.GetCode(), hubpb.ErrorCode_NO_HANDLER)
	is(t, e.GetMessage(), "nobody")
	is(t, e.GetRequestId(), "r-1")
}

func TestRecvHelpersSkipHeartbeatsAndStatuses(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	send(t, c, heartbeat(1))
	send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Status{Status: &hubpb.InstanceStatus{HandlersInFlight: 2}}})
	send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Permits{Permits: &hubpb.Permits{Count: 4}}})
	send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Flow{Flow: &hubpb.Flow{RequestId: "x", Credits: 1}}})
	s := hub.Next(t)
	s.RecvRegister(t)
	is(t, s.RecvPermits(t).GetCount(), uint32(4))
	is(t, s.RecvFlow(t).GetCredits(), uint32(1))
	is(t, len(s.Heartbeats()), 1)
	is(t, len(s.Statuses()), 1)
	is(t, s.Statuses()[0].GetHandlersInFlight(), uint32(2))
	if s.Heartbeats()[0].IsZero() {
		t.Fatal("heartbeat arrival time not recorded")
	}
}

func TestRecvHelperFailsOnAnotherKind(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	ft := &fakeT{TB: t}
	func() {
		defer func() { _ = recover() }()
		s.RecvRequest(ft)
	}()
	if !ft.failed {
		t.Fatal("RecvRequest accepted a Register")
	}
}

// fakeT records Fatalf instead of ending the test; it unwinds with a panic.
type fakeT struct {
	testing.TB
	failed bool
	msg    string
}

func (f *fakeT) Helper() {}
func (f *fakeT) Fatalf(format string, args ...any) {
	f.failed, f.msg = true, fmt.Sprintf(format, args...)
	panic("fatal")
}
func (f *fakeT) Fatal(args ...any) {
	f.failed, f.msg = true, fmt.Sprint(args...)
	panic("fatal")
}

// fails runs step with a fakeT and returns the failure message, or fails the test when step
// passed.
func fails(t *testing.T, step func(testing.TB)) string {
	t.Helper()
	ft := &fakeT{TB: t}
	func() {
		defer func() { _ = recover() }()
		step(ft)
	}()
	if !ft.failed {
		t.Fatal("the step passed")
	}
	return ft.msg
}

func TestNothingFailsOnAMatchingKindAndKeepsTheRest(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Permits{Permits: &hubpb.Permits{Count: 3}}})
	// A Permits frame is not a request: Nothing passes and keeps it for later.
	s.Nothing(t, 100*time.Millisecond, "request")
	is(t, s.RecvPermits(t).GetCount(), uint32(3))

	send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Request{Request: &hubpb.Request{Id: "r"}}})
	ft := &fakeT{TB: t}
	func() {
		defer func() { _ = recover() }()
		s.Nothing(ft, 2*time.Second, "request")
	}()
	if !ft.failed {
		t.Fatal("Nothing missed a request")
	}
}

func TestNoEchoBeforeTheAck(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	send(t, c, heartbeat(1))
	is(t, s.Recv(t).GetHeartbeat().GetSentAtMs(), int64(1))
	// Nothing is echoed before the ack: the next frame the client sees is the ack itself.
	s.Ack(t, Ack("hub-1"))
	is(t, recv(t, c).GetRegisterAck().GetHubId(), "hub-1")

	// A second stream, forced to echo before its ack.
	c2 := dial(t, hub.Addr())
	send(t, c2, register("orders-service", "i-2"))
	s2 := hub.Next(t)
	s2.RecvRegister(t)
	s2.EchoHeartbeats(true)
	send(t, c2, heartbeat(2))
	is(t, recv(t, c2).GetHeartbeat().GetSentAtMs(), int64(2))
}

func TestEchoCanBeSwitchedOffAfterTheAck(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, Ack("hub-1"))
	recv(t, c)
	s.EchoHeartbeats(false)
	send(t, c, heartbeat(1))
	is(t, s.Recv(t).GetHeartbeat().GetSentAtMs(), int64(1))
	s.EchoHeartbeats(true)
	send(t, c, heartbeat(2))
	is(t, recv(t, c).GetHeartbeat().GetSentAtMs(), int64(2)) // 1 was never echoed
}

func TestEchoedRecordsTheHeartbeatsThatWereEchoed(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	send(t, c, heartbeat(1)) // before the ack: not echoed
	s.Recv(t)
	s.Ack(t, Ack("hub-1"))
	recv(t, c)
	send(t, c, heartbeat(2)) // echoed
	is(t, recv(t, c).GetHeartbeat().GetSentAtMs(), int64(2))
	s.EchoHeartbeats(false)
	send(t, c, heartbeat(3)) // not echoed
	s.Recv(t)
	s.Recv(t)
	beats, echoed := s.Heartbeats(), s.Echoed()
	is(t, len(beats), 3)
	is(t, len(echoed), 1)
	is(t, echoed[0], beats[1])
}

func TestEndedSaysWhenTheStreamEnded(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	before := time.Now()
	if err := c.CloseSend(); err != nil {
		t.Fatal(err)
	}
	at := s.Ended(t, time.Second)
	is(t, !at.Before(before) && time.Since(at) < time.Second, true)
	is(t, s.Ended(t, time.Second), at) // the same moment, at once, when asked again
}

func TestFirstFrameThatIsNotARegisterIsRefused(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, heartbeat(1))
	is(t, status.Code(recvErr(t, c)), codes.FailedPrecondition)
	s := hub.Next(t)
	is(t, s.Recv(t).GetHeartbeat().GetSentAtMs(), int64(1))
}

func TestInvalidRegistersAreRefused(t *testing.T) {
	for name, r := range map[string]*hubpb.Register{
		"empty service":     {InstanceId: "i-1", InitialPermits: 1},
		"empty instance":    {ServiceName: "orders-service", InitialPermits: 1},
		"no permits":        {ServiceName: "orders-service", InstanceId: "i-1"},
		"reserved service":  {ServiceName: "snodra-server", InstanceId: "i-1", InitialPermits: 1},
		"reserved instance": {ServiceName: "orders-service", InstanceId: "snodra-server/x", InitialPermits: 1},
	} {
		t.Run(name, func(t *testing.T) {
			hub := Start(t)
			c := dial(t, hub.Addr())
			send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Register{Register: r}})
			is(t, status.Code(recvErr(t, c)), codes.InvalidArgument)
			is(t, hub.Next(t).RecvRegister(t).GetInstanceId(), r.GetInstanceId())
		})
	}
}

func TestSendsHubFramesInOrder(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	s.Ack(t, Ack("hub-1"))
	s.Send(t, Forward(&hubpb.Request{Id: "r-1", Type: "orders.PlaceOrder"}))
	s.Send(t, Flow("r-1", 2))
	s.Send(t, Cancel("r-1", "x"))
	is(t, recv(t, c).GetRegisterAck().GetHubId(), "hub-1")
	is(t, recv(t, c).GetRequest().GetId(), "r-1")
	is(t, recv(t, c).GetFlow().GetCredits(), uint32(2))
	is(t, recv(t, c).GetCancel().GetReason(), "x")
}

func TestHalfClosed(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	ft := &fakeT{TB: t}
	func() {
		defer func() { _ = recover() }()
		s.HalfClosed(ft, 50*time.Millisecond)
	}()
	if !ft.failed {
		t.Fatal("HalfClosed passed on an open stream")
	}
	if err := c.CloseSend(); err != nil {
		t.Fatal(err)
	}
	s.HalfClosed(t, 5*time.Second)
}

func TestRecvAfterTheClientLeftFails(t *testing.T) {
	hub := Start(t)
	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	c, err := hubpb.NewHubClient(conn).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	cancel()
	_ = conn.Close()
	ft := &fakeT{TB: t}
	func() {
		defer func() { _ = recover() }()
		s.Recv(ft)
	}()
	if !ft.failed {
		t.Fatal("Recv returned a frame from a dead stream")
	}
}

func TestStreamsAreNumberedAndNoStreamWithin(t *testing.T) {
	hub := Start(t)
	hub.NoStreamWithin(t, 50*time.Millisecond)
	c1 := dial(t, hub.Addr())
	send(t, c1, register("orders-service", "i-1"))
	c2 := dial(t, hub.Addr())
	send(t, c2, register("orders-service", "i-2"))
	a, b := hub.Next(t), hub.Next(t)
	is(t, a.Index, 0)
	is(t, b.Index, 1)
	ft := &fakeT{TB: t}
	c3 := dial(t, hub.Addr())
	send(t, c3, register("orders-service", "i-3"))
	func() {
		defer func() { _ = recover() }()
		hub.NoStreamWithin(ft, 5*time.Second)
	}()
	if !ft.failed {
		t.Fatal("NoStreamWithin missed a stream")
	}
}

func TestRefuseAndDown(t *testing.T) {
	hub := Start(t)
	hub.Refuse(codes.Unavailable, "busy")
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	is(t, status.Code(recvErr(t, c)), codes.Unavailable)
	hub.NoStreamWithin(t, 50*time.Millisecond) // a refused stream is never handed to the script

	hub.Accept()
	c = dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	hub.Next(t).RecvRegister(t)

	addr := hub.Addr()
	hub.Down()
	_, err := net.DialTimeout("tcp", addr, time.Second)
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial while down: %v, want ECONNREFUSED", err)
	}
	hub.Up()
	is(t, hub.Addr(), addr)
	c = dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-2"))
	is(t, hub.Next(t).RecvRegister(t).GetInstanceId(), "i-2")
}

func TestDownSeversLiveStreams(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	hub.Next(t).RecvRegister(t)
	hub.Down()
	is(t, status.Code(recvErr(t, c)), codes.Unavailable)
}

func TestTopologyIsEmpty(t *testing.T) {
	hub := Start(t)
	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	r, err := hubpb.NewHubClient(conn).Topology(t.Context(), &hubpb.TopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(r.GetServices()), 0)
}

func selfSigned(t testing.TB) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fakehub-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

func TestTLSAndMutualTLS(t *testing.T) {
	cert, pool := selfSigned(t)

	t.Run("mutual", func(t *testing.T) {
		hub := Start(t, WithTLS(cert, pool))
		creds := credentials.NewTLS(&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}})
		c := dial(t, hub.Addr(), grpc.WithTransportCredentials(creds))
		send(t, c, register("orders-service", "i-1"))
		s := hub.Next(t)
		s.RecvRegister(t)
		is(t, len(s.PeerCertificates), 1)
		is(t, s.PeerCertificates[0].Equal(cert.Leaf), true)
	})

	t.Run("server only", func(t *testing.T) {
		hub := Start(t, WithTLS(cert, nil))
		creds := credentials.NewTLS(&tls.Config{RootCAs: pool})
		c := dial(t, hub.Addr(), grpc.WithTransportCredentials(creds))
		send(t, c, register("orders-service", "i-1"))
		s := hub.Next(t)
		s.RecvRegister(t)
		is(t, len(s.PeerCertificates), 0)
	})
}

func TestCredentialsAreRecorded(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	send(t, c, &hubpb.ClientMessage{Message: &hubpb.ClientMessage_Credentials{Credentials: &hubpb.Credentials{}}})
	s := hub.Next(t)
	s.RecvRegister(t)
	if s.RecvCredentials(t) == nil {
		t.Fatal("no credentials")
	}
}

func TestARefusedStreamEndsAtOnceForTheScript(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, heartbeat(1))
	is(t, status.Code(recvErr(t, c)), codes.FailedPrecondition)
	s := hub.Next(t)
	s.Recv(t) // the offending frame
	start := time.Now()
	for name, step := range map[string]func(testing.TB){
		"Recv":       func(tb testing.TB) { s.Recv(tb) },
		"HalfClosed": func(tb testing.TB) { s.HalfClosed(tb, 3*time.Second) },
	} {
		msg := fails(t, step)
		if !strings.Contains(msg, "stream ended") || !strings.Contains(msg, "FailedPrecondition") {
			t.Fatalf("%s: %q", name, msg)
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the script learnt of the end after %v", d)
	}
	s.Nothing(t, 2*time.Second) // nothing can arrive on an ended stream: passes at once
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Nothing waited %v on an ended stream", d)
	}
}

func TestConnectDoesNotBlockOnAStreamNobodyTakes(t *testing.T) {
	hub := Start(t, withStreamBuffer(0))
	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	c, err := hubpb.NewHubClient(conn).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	send(t, c, register("orders-service", "i-1"))
	waitUntil(t, func() bool { return hub.connecting.Load() == 1 }) // waiting for Next
	cancel()
	waitUntil(t, func() bool { return hub.connecting.Load() == 0 }) // and returned with the client
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 1 s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestARecvHelperGivesUpAfterFiveSecondsDespiteHeartbeats(t *testing.T) {
	hub := Start(t)
	c := dial(t, hub.Addr())
	send(t, c, register("orders-service", "i-1"))
	s := hub.Next(t)
	s.RecvRegister(t)
	stop := make(chan struct{})
	defer close(stop)
	go func() { // a heartbeat every second: each frame comes well within the bound
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
				_ = c.Send(heartbeat(1))
			}
		}
	}()
	start := time.Now()
	msg := fails(t, func(tb testing.TB) { s.RecvReply(tb) })
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("RecvReply gave up after %v (%s)", d, msg)
	}
}
