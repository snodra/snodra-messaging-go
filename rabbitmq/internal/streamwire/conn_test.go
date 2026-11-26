package streamwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

// rawFrame is one frame as it travels: the size, key, version and content.
func rawFrame(key, version uint16, content []byte) []byte {
	body := (&encoder{}).u16(key).u16(version).raw(content).buf
	out := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	return append(out, body...)
}

// responseFrame is one response: its correlation id and code follow the version.
func responseFrame(key uint16, correlationID uint32, code uint16, content []byte) []byte {
	return rawFrame(key|responseBit, 1, (&encoder{}).u32(correlationID).u16(code).raw(content).buf)
}

func readRawFrame(t *testing.T, frame []byte) Frame {
	t.Helper()
	got, err := readFrame(bufio.NewReader(bytes.NewReader(frame)))
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	return got
}

func chunkFrameContent(subscriptionID uint8, version uint16, first uint64, records uint32, timestampMs int64, chunkType uint8) []byte {
	e := (&encoder{}).u8(subscriptionID)
	if version >= 2 {
		e.u64(99) // the committed chunk id a v2 deliver adds
	}
	e.u8(0).u8(chunkType).u16(1).u32(records).u64(uint64(timestampMs)).u64(1).u64(first).
		u32(0).u32(0).u32(0).u32(0)
	return e.buf
}

func TestReadFrame(t *testing.T) {
	deliverV1 := chunkFrameContent(3, 1, 40, 2, 1_700_000_000_000, 0)
	deliverV2 := chunkFrameContent(4, 2, 41, 1, 1_700_000_000_001, 0)
	cases := []struct {
		name          string
		raw           []byte
		key           uint16
		version       uint16
		correlationID uint32
		code          uint16
		response      bool
		content       []byte
	}{
		{"a literal frame keeps its big-endian size, key and version", []byte{0, 0, 0, 9, 0x12, 0x34, 0x56, 0x78, 1, 2, 3, 4, 5}, 0x1234, 0x5678, 0, 0, false, []byte{1, 2, 3, 4, 5}},
		{"a request has no correlation in its header", rawFrame(cmdSubscribe, 1, []byte{1, 2, 3}), cmdSubscribe, 1, 0, 0, false, []byte{1, 2, 3}},
		{"a generic response has its correlation and code", responseFrame(cmdSubscribe, 42, CodeOK, []byte{9}), cmdSubscribe | responseBit, 1, 42, CodeOK, true, []byte{9}},
		{"a metadata response has no code", rawFrame(cmdMetadata|responseBit, 1, (&encoder{}).u32(7).u32(2).u32(0).buf), cmdMetadata | responseBit, 1, 7, 0, true, (&encoder{}).u32(2).u32(0).buf},
		{"a tune response has no correlation", rawFrame(cmdTune|responseBit, 1, (&encoder{}).u32(1<<20).u32(30).buf), cmdTune | responseBit, 1, 0, 0, true, (&encoder{}).u32(1 << 20).u32(30).buf},
		{"a v1 deliver has its chunk", rawFrame(cmdDeliver, 1, deliverV1), cmdDeliver, 1, 0, 0, false, deliverV1},
		{"a v2 deliver has its committed chunk id", rawFrame(cmdDeliver, 2, deliverV2), cmdDeliver, 2, 0, 0, false, deliverV2},
		{"a consumer update is a server request", rawFrame(cmdConsumerUpdate, 1, (&encoder{}).u32(77).u8(3).u8(1).buf), cmdConsumerUpdate, 1, 0, 0, false, (&encoder{}).u32(77).u8(3).u8(1).buf},
		{"a confirm has its ids", rawFrame(cmdPublishConfirm, 1, (&encoder{}).u8(1).u32(2).u64(10).u64(11).buf), cmdPublishConfirm, 1, 0, 0, false, (&encoder{}).u8(1).u32(2).u64(10).u64(11).buf},
		{"a publish error has its code", rawFrame(cmdPublishError, 1, (&encoder{}).u8(1).u32(1).u64(12).u16(CodeAccessRefused).buf), cmdPublishError, 1, 0, 0, false, (&encoder{}).u8(1).u32(1).u64(12).u16(CodeAccessRefused).buf},
		{"a close names its reason", rawFrame(cmdClose, 1, (&encoder{}).u32(9).u16(CodeOK).str("bye").buf), cmdClose, 1, 0, 0, false, (&encoder{}).u32(9).u16(CodeOK).str("bye").buf},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := readRawFrame(t, c.raw)
			if got.Key != c.key || got.Version != c.version || got.Response != c.response ||
				got.CorrelationID != c.correlationID || got.Code != c.code || !bytes.Equal(got.Content, c.content) {
				t.Fatalf("got %+v, want key 0x%04x version %d response %t correlation %d code 0x%02x content % x",
					got, c.key, c.version, c.response, c.correlationID, c.code, c.content)
			}
		})
	}
}

func TestChunkHeaders(t *testing.T) {
	for _, c := range []struct {
		version   uint16
		content   []byte
		offset    uint64
		records   uint32
		timestamp int64
	}{
		{1, chunkFrameContent(3, 1, 40, 2, 1_700_000_000_000, 0), 40, 2, 1_700_000_000_000},
		{2, chunkFrameContent(4, 2, 41, 1, 1_700_000_000_001, 1), 41, 1, 1_700_000_000_001},
	} {
		frame := readRawFrame(t, rawFrame(cmdDeliver, c.version, c.content))
		header, err := ParseChunkHeader(frame)
		if err != nil {
			t.Fatalf("ParseChunkHeader v%d: %v", c.version, err)
		}
		if header.FirstOffset != c.offset || header.Records != c.records || header.TimestampMs != c.timestamp {
			t.Fatalf("v%d header: %+v", c.version, header)
		}
	}
	updateFrame := readRawFrame(t, rawFrame(cmdConsumerUpdate, 1, (&encoder{}).u32(77).u8(3).u8(1).buf))
	correlationID, subscriptionID, active, err := ParseConsumerUpdate(updateFrame)
	if err != nil || correlationID != 77 || subscriptionID != 3 || !active {
		t.Fatalf("consumer update: %d %d %t %v", correlationID, subscriptionID, active, err)
	}
}

func TestReadFrameRejectsBadSizes(t *testing.T) {
	for _, raw := range [][]byte{
		{0, 0, 0, 3},
		{0, 0, 0, 4},
		{0, 0, 0, 5, 0, 0},
	} {
		if _, err := readFrame(bufio.NewReader(bytes.NewReader(raw))); err == nil {
			t.Fatalf("readFrame(% x) accepted a bad size", raw)
		}
	}
	raw := rawFrame(cmdSubscribe|responseBit, 1, (&encoder{}).u32(1).buf)
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(raw))); !errors.Is(err, errShort) {
		t.Fatalf("a short response: %v", err)
	}
}

// pipeConn is one side of a net.Pipe with the read loop running.
func pipeConn(t *testing.T, timeout time.Duration) (*Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	c := &Conn{
		cfg:     Config{RequestTimeout: timeout},
		netConn: client,
		reader:  bufio.NewReader(client),
		writer:  bufio.NewWriter(client),
		pending: make(map[uint32]chan result),
		done:    make(chan struct{}),
		tune:    make(chan tuneFrame, 1),
	}
	go c.readLoop()
	t.Cleanup(func() {
		c.shutdown(nil)
		_ = server.Close()
	})
	return c, server
}

func TestReadLoopDispatchesServerCommands(t *testing.T) {
	c, server := pipeConn(t, time.Second)
	delivers := make(chan Frame, 1)
	updates := make(chan [3]uint32, 1)
	confirms := make(chan Confirm, 2)
	metadata := make(chan string, 1)
	c.SetDeliverHandler(func(frame Frame) { delivers <- frame })
	c.SetUpdateHandler(func(subscriptionID uint8, active bool, correlationID uint32) {
		updates <- [3]uint32{uint32(subscriptionID), boolToUint32(active), correlationID}
	})
	c.SetConfirmHandler(func(confirm Confirm) { confirms <- confirm })
	c.SetMetadataUpdateHandler(func(stream string, code uint16) { metadata <- stream })

	write := func(raw []byte) {
		if _, err := server.Write(raw); err != nil {
			t.Fatalf("writing a server frame: %v", err)
		}
	}
	write(rawFrame(cmdDeliver, 1, chunkFrameContent(3, 1, 5, 1, 7, 0)))
	write(rawFrame(cmdConsumerUpdate, 1, (&encoder{}).u32(77).u8(3).u8(1).buf))
	write(rawFrame(cmdPublishConfirm, 1, (&encoder{}).u8(2).u32(2).u64(10).u64(11).buf))
	write(rawFrame(cmdPublishError, 1, (&encoder{}).u8(2).u32(1).u64(12).u16(CodeAccessRefused).buf))
	write(rawFrame(cmdMetadataUpdate, 1, (&encoder{}).u16(6).str("snodra.events-1").buf))

	if frame := <-delivers; frame.Key != cmdDeliver {
		t.Fatalf("deliver: %+v", frame)
	}
	if got := <-updates; got != [3]uint32{3, 1, 77} {
		t.Fatalf("consumer update: %v", got)
	}
	first := <-confirms
	if first.PublisherID != 2 || len(first.Accepted) != 2 || first.Accepted[0] != 10 || first.Accepted[1] != 11 {
		t.Fatalf("confirm: %+v", first)
	}
	second := <-confirms
	if second.Errors[12] != CodeAccessRefused {
		t.Fatalf("publish error: %+v", second)
	}
	if got := <-metadata; got != "snodra.events-1" {
		t.Fatalf("metadata update: %q", got)
	}
}

func boolToUint32(value bool) uint32 {
	if value {
		return 1
	}
	return 0
}

func TestReadLoopAnswersClose(t *testing.T) {
	c, server := pipeConn(t, time.Second)
	answers := make(chan Frame, 1)
	go func() {
		frame, err := readFrame(bufio.NewReader(server))
		if err == nil {
			answers <- frame
		}
	}()
	if _, err := server.Write(rawFrame(cmdClose, 1, (&encoder{}).u32(9).u16(CodeOK).str("bye").buf)); err != nil {
		t.Fatalf("writing close: %v", err)
	}
	select {
	case answer := <-answers:
		if !answer.Response || answer.Key != cmdClose|responseBit || answer.CorrelationID != 9 || answer.Code != CodeOK {
			t.Fatalf("the close answer: %+v", answer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the close was not answered")
	}
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the connection did not end")
	}
}

func TestRequestRoundTripDeletesItsPendingEntry(t *testing.T) {
	c, server := pipeConn(t, time.Second)
	go func() {
		frame, err := readFrame(bufio.NewReader(server))
		if err != nil {
			return
		}
		correlationID := binary.BigEndian.Uint32(frame.Content)
		response := responseFrame(cmdSubscribe, correlationID, CodeOK, []byte{7})
		_, _ = server.Write(response)
	}()
	frame, err := c.Request(context.Background(), cmdSubscribe, []byte{1, 2})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if frame.Code != CodeOK || !bytes.Equal(frame.Content, []byte{7}) {
		t.Fatalf("the response: %+v", frame)
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.pendingMu.Lock()
		left := len(c.pending)
		c.pendingMu.Unlock()
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d pending request entries remain", left)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRequestAppliesTheDefaultTimeout(t *testing.T) {
	c, server := pipeConn(t, 50*time.Millisecond)
	go func() {
		_, _ = readFrame(bufio.NewReader(server))
	}()
	start := time.Now()
	_, err := c.Request(context.Background(), cmdSubscribe, nil)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Request without a deadline: %v, want DeadlineExceeded", err)
	}
	if elapsed < 40*time.Millisecond || elapsed > time.Second {
		t.Fatalf("the request wait: %v", elapsed)
	}
	c.pendingMu.Lock()
	left := len(c.pending)
	c.pendingMu.Unlock()
	if left != 0 {
		t.Fatalf("%d pending request entries remain after a timeout", left)
	}
}

func TestClientTLSGainsTheDialHost(t *testing.T) {
	got := clientTLS(&tls.Config{}, "rabbitmq.test:5552")
	if got.ServerName != "rabbitmq.test" {
		t.Fatalf("a bare TLS config: ServerName %q", got.ServerName)
	}
	explicit := clientTLS(&tls.Config{ServerName: "orders.test"}, "rabbitmq.test:5552")
	if explicit.ServerName != "orders.test" {
		t.Fatalf("an explicit ServerName was replaced: %q", explicit.ServerName)
	}
	original := &tls.Config{}
	_ = clientTLS(original, "rabbitmq.test:5552")
	if original.ServerName != "" {
		t.Fatal("the caller's config was mutated")
	}
}

func TestDialSetsTheServersName(t *testing.T) {
	certificate := selfSignedCertificate(t, "localhost")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	names := make(chan string, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = raw.Close() }()
		conn := tls.Server(raw, &tls.Config{
			Certificates: []tls.Certificate{certificate},
			GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				select {
				case names <- hello.ServerName:
				default:
				}
				return nil, nil
			},
		})
		if err := conn.Handshake(); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = Dial(ctx, Config{
		Address:        net.JoinHostPort("localhost", port),
		TLS:            &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
		RequestTimeout: 500 * time.Millisecond,
	})
	select {
	case name := <-names:
		if name != "localhost" {
			t.Fatalf("the server saw ServerName %q, want localhost", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the TLS handshake did not reach the server")
	}
}

func selfSignedCertificate(t *testing.T, host string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		DNSNames:              []string{host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
