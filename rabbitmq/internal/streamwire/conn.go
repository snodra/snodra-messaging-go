package streamwire

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The frame bounds of the protocol. The publish frame the event store sends stays under
// MaxFrame, the client and broker default; a larger event is refused for good.
const (
	// MaxFrame is the largest frame the store sends, and the most it negotiates.
	MaxFrame = 1 << 20
	// readLimit guards a frame the broker sends: a chunk over it is a protocol failure, never a
	// wait for memory.
	readLimit = 64 << 20
)

// Config is how one connection reaches a node.
type Config struct {
	// Address is `host:port`.
	Address string
	// VirtualHost is the vhost opened (default "/").
	VirtualHost string
	// Username and Password authenticate with SASL PLAIN; both default to "guest".
	Username string
	Password string
	// TLS is the TLS configuration; nil connects in the clear.
	TLS *tls.Config
	// ConnectionName is the `connection_name` peer property (shown by the management API).
	ConnectionName string
	// RequestTimeout bounds one request and one write (default 10 s).
	RequestTimeout time.Duration
	// Heartbeat is the heartbeat the store asks for (default 30 s; 0 with a broker that asks
	// for none).
	Heartbeat time.Duration
}

func (c Config) validate() error {
	if strings.TrimSpace(c.Address) == "" {
		return errors.New("rabbitmq stream: the address is empty")
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return fmt.Errorf("rabbitmq stream: the address %q: %w", c.Address, err)
	}
	return nil
}

// Frame is one frame of the protocol. For a response, Code is the response code and Content
// follows it; for a server command, Code is zero and Content follows the version.
type Frame struct {
	Key           uint16
	Version       uint16
	CorrelationID uint32
	Code          uint16
	Content       []byte
	Response      bool
}

type result struct {
	frame Frame
	err   error
}

type tuneFrame struct {
	maxFrame  uint32
	heartbeat uint32
}

// Conn is one connection to one node. It serves one publisher or one subscription in this
// add-on, and temporary reads; every server command goes to the handler the owner set.
type Conn struct {
	cfg       Config
	netConn   net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[uint32]chan result
	nextCorr  atomic.Uint32
	done      chan struct{}
	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
	closed    atomic.Bool
	// The negotiated heartbeat in nanoseconds, and the most a publish frame may be.
	heartbeatNs atomic.Int64
	maxFrame    atomic.Uint32

	handlerMu       sync.Mutex
	deliverHandler  func(Frame)
	updateHandler   func(subscriptionID uint8, active bool, correlationID uint32)
	confirmHandler  func(Confirm)
	metadataHandler func(stream string, code uint16)
	closeHandler    func(error)
	tune            chan tuneFrame
}

// Dial connects and runs the protocol handshake. A context that ends stops it.
func Dial(ctx context.Context, cfg Config) (*Conn, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.VirtualHost == "" {
		cfg.VirtualHost = "/"
	}
	if cfg.Username == "" {
		cfg.Username = "guest"
	}
	if cfg.Password == "" {
		cfg.Password = "guest"
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 30 * time.Second
	}
	dialer := net.Dialer{Timeout: cfg.RequestTimeout, KeepAlive: 30 * time.Second}
	raw, err := dialer.DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq stream: connecting to %s: %w", cfg.Address, err)
	}
	if cfg.TLS != nil {
		tlsConn := tls.Client(raw, cfg.TLS)
		if deadline, ok := ctx.Deadline(); ok {
			_ = tlsConn.SetDeadline(deadline)
		}
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("rabbitmq stream: the TLS handshake with %s: %w", cfg.Address, err)
		}
		_ = tlsConn.SetDeadline(time.Time{})
		raw = tlsConn
	}
	c := &Conn{
		cfg:     cfg,
		netConn: raw,
		reader:  bufio.NewReaderSize(raw, 64<<10),
		writer:  bufio.NewWriterSize(raw, 64<<10),
		pending: make(map[uint32]chan result),
		done:    make(chan struct{}),
		tune:    make(chan tuneFrame, 1),
	}
	go c.readLoop()
	if err := c.handshake(ctx); err != nil {
		c.fail(err)
		return nil, err
	}
	return c, nil
}

// SetDeliverHandler sets what receives a deliver frame.
func (c *Conn) SetDeliverHandler(h func(Frame)) {
	c.handlerMu.Lock()
	c.deliverHandler = h
	c.handlerMu.Unlock()
}

// SetUpdateHandler sets what receives a consumer update.
func (c *Conn) SetUpdateHandler(h func(subscriptionID uint8, active bool, correlationID uint32)) {
	c.handlerMu.Lock()
	c.updateHandler = h
	c.handlerMu.Unlock()
}

// SetConfirmHandler sets what receives publish confirmations and publish errors.
func (c *Conn) SetConfirmHandler(h func(Confirm)) {
	c.handlerMu.Lock()
	c.confirmHandler = h
	c.handlerMu.Unlock()
}

// SetMetadataUpdateHandler sets what receives a stream metadata update (deleted or moved).
func (c *Conn) SetMetadataUpdateHandler(h func(stream string, code uint16)) {
	c.handlerMu.Lock()
	c.metadataHandler = h
	c.handlerMu.Unlock()
}

// SetCloseHandler sets what is told why the connection ended, once. It is not called by Close.
func (c *Conn) SetCloseHandler(h func(error)) {
	c.handlerMu.Lock()
	c.closeHandler = h
	existed := c.closed.Load()
	c.handlerMu.Unlock()
	if existed {
		h(c.Err())
	}
}

// Err is why the connection ended, nil while it is open and after Close.
func (c *Conn) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

// IsClosed reports whether the connection ended (explicitly or by a failure).
func (c *Conn) IsClosed() bool { return c.closed.Load() }

// MaxFrame is the frame size negotiated with the broker.
func (c *Conn) MaxFrame() int { return int(c.maxFrame.Load()) }

func (c *Conn) handshake(ctx context.Context) error {
	properties := map[string]string{
		"product":   "snodra-messaging",
		"platform":  "Go",
		"version":   "1.0",
		"copyright": "Snodra",
	}
	if c.cfg.ConnectionName != "" {
		properties["connection_name"] = c.cfg.ConnectionName
	}
	content := (&encoder{}).u32(uint32(len(properties))).buf
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		content = (&encoder{buf: content}).str(name).str(properties[name]).buf
	}
	frame, err := c.Request(ctx, cmdPeerProperties, content)
	if err != nil {
		return fmt.Errorf("rabbitmq stream: peer properties: %w", err)
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "peer properties", Code: frame.Code}
	}

	frame, err = c.Request(ctx, cmdSaslHandshake, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq stream: SASL handshake: %w", err)
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "SASL handshake", Code: frame.Code}
	}
	mechanisms := (&decoder{buf: frame.Content}).strings()
	mechanism := ""
	for _, offered := range mechanisms {
		if offered == "PLAIN" {
			mechanism = "PLAIN"
			break
		}
	}
	if mechanism == "" {
		return fmt.Errorf("rabbitmq stream: no PLAIN SASL mechanism among %v", mechanisms)
	}

	opaque := []byte("\x00" + c.cfg.Username + "\x00" + c.cfg.Password)
	content = (&encoder{}).str(mechanism).bytes(opaque).buf
	frame, err = c.Request(ctx, cmdSaslAuthenticate, content)
	if err != nil {
		return fmt.Errorf("rabbitmq stream: SASL authenticate: %w", err)
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "SASL authenticate", Code: frame.Code}
	}

	var tune tuneFrame
	select {
	case tune = <-c.tune:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return fmt.Errorf("rabbitmq stream: the connection closed during the handshake: %w", c.Err())
	}
	maxFrame := uint32(MaxFrame)
	if tune.maxFrame != 0 && tune.maxFrame < maxFrame {
		maxFrame = tune.maxFrame
	}
	heartbeat := uint32(c.cfg.Heartbeat / time.Second)
	if tune.heartbeat == 0 {
		heartbeat = 0
	} else if heartbeat == 0 || tune.heartbeat < heartbeat {
		heartbeat = tune.heartbeat
	}
	c.maxFrame.Store(maxFrame)
	c.heartbeatNs.Store(int64(time.Duration(heartbeat) * time.Second))
	if err := c.SendFrame(cmdTune|responseBit, 1, (&encoder{}).u32(maxFrame).u32(heartbeat).buf); err != nil {
		return fmt.Errorf("rabbitmq stream: the tune response: %w", err)
	}

	frame, err = c.Request(ctx, cmdOpen, (&encoder{}).str(c.cfg.VirtualHost).buf)
	if err != nil {
		return fmt.Errorf("rabbitmq stream: open: %w", err)
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "open", Code: frame.Code}
	}
	if c.heartbeat() > 0 {
		go c.heartbeatLoop()
	}
	return nil
}

func (c *Conn) heartbeat() time.Duration {
	return time.Duration(c.heartbeatNs.Load())
}

func (c *Conn) heartbeatLoop() {
	period := c.heartbeat() / 2
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.SendFrame(cmdHeartbeat, 1, nil); err != nil {
				c.fail(fmt.Errorf("rabbitmq stream: the heartbeat: %w", err))
				return
			}
		}
	}
}

func (c *Conn) readLoop() {
	for {
		if heartbeat := c.heartbeat(); heartbeat > 0 {
			_ = c.netConn.SetReadDeadline(time.Now().Add(3 * heartbeat))
		}
		frame, err := readFrame(c.reader)
		if err != nil {
			if !c.closed.Load() {
				c.fail(err)
			}
			return
		}
		if frame.Response {
			if frame.Key == cmdTune|responseBit {
				continue
			}
			c.pendingMu.Lock()
			waiting := c.pending[frame.CorrelationID]
			c.pendingMu.Unlock()
			if waiting != nil {
				select {
				case waiting <- result{frame: frame}:
				default:
				}
			}
			continue
		}
		switch frame.Key {
		case cmdTune:
			d := &decoder{buf: frame.Content}
			tune := tuneFrame{maxFrame: d.u32(), heartbeat: d.u32()}
			select {
			case c.tune <- tune:
			default:
			}
		case cmdHeartbeat:
			// The read deadline was refreshed by the read itself.
		case cmdDeliver:
			c.handlerMu.Lock()
			h := c.deliverHandler
			c.handlerMu.Unlock()
			if h != nil {
				h(frame)
			}
		case cmdConsumerUpdate:
			d := &decoder{buf: frame.Content}
			correlationID := d.u32()
			subscriptionID := d.u8()
			active := d.u8() == 1
			c.handlerMu.Lock()
			h := c.updateHandler
			c.handlerMu.Unlock()
			if h != nil {
				h(subscriptionID, active, correlationID)
			}
		case cmdPublishConfirm:
			d := &decoder{buf: frame.Content}
			confirm := Confirm{PublisherID: d.u8()}
			count := d.u32()
			for i := uint32(0); i < count; i++ {
				confirm.Accepted = append(confirm.Accepted, d.u64())
			}
			c.handlerMu.Lock()
			h := c.confirmHandler
			c.handlerMu.Unlock()
			if h != nil {
				h(confirm)
			}
		case cmdPublishError:
			d := &decoder{buf: frame.Content}
			confirm := Confirm{PublisherID: d.u8(), Errors: make(map[uint64]uint16)}
			count := d.u32()
			for i := uint32(0); i < count; i++ {
				id := d.u64()
				confirm.Errors[id] = d.u16()
			}
			c.handlerMu.Lock()
			h := c.confirmHandler
			c.handlerMu.Unlock()
			if h != nil {
				h(confirm)
			}
		case cmdMetadataUpdate:
			d := &decoder{buf: frame.Content}
			code := d.u16()
			stream := d.str()
			c.handlerMu.Lock()
			h := c.metadataHandler
			c.handlerMu.Unlock()
			if h != nil {
				h(stream, code)
			}
		case cmdClose:
			// Answer the broker's close before ending: correlation id and a response code.
			if len(frame.Content) >= 4 {
				correlationID := binary.BigEndian.Uint32(frame.Content)
				_ = c.SendFrame(cmdClose|responseBit, 1, (&encoder{}).u32(correlationID).u16(CodeOK).buf)
			}
			c.fail(errors.New("rabbitmq stream: the broker closed the connection"))
			return
		}
	}
}

func readFrame(r *bufio.Reader) (Frame, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return Frame{}, err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	if size < 4 || size > readLimit {
		return Frame{}, fmt.Errorf("rabbitmq stream: a frame of %d bytes", size)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return Frame{}, err
	}
	frame := Frame{
		Key:      binary.BigEndian.Uint16(body[0:]),
		Version:  binary.BigEndian.Uint16(body[2:]),
		Response: binary.BigEndian.Uint16(body[0:])&responseBit != 0,
	}
	if frame.Response {
		switch frame.Key {
		case cmdTune | responseBit:
			frame.Content = body[4:]
			return frame, nil
		case cmdMetadata | responseBit:
			// A metadata answer has no response code: the brokers follow the correlation id.
			if len(body) < 8 {
				return Frame{}, errShort
			}
			frame.CorrelationID = binary.BigEndian.Uint32(body[4:])
			frame.Content = body[8:]
			return frame, nil
		}
		if len(body) < 10 {
			return Frame{}, errShort
		}
		frame.CorrelationID = binary.BigEndian.Uint32(body[4:])
		frame.Code = binary.BigEndian.Uint16(body[8:])
		frame.Content = body[10:]
	} else {
		frame.Content = body[4:]
	}
	return frame, nil
}

// Request sends a request and waits for its response (or the context, or the connection).
func (c *Conn) Request(ctx context.Context, key uint16, content []byte) (Frame, error) {
	if c.IsClosed() {
		return Frame{}, fmt.Errorf("rabbitmq stream: the connection is closed: %w", c.Err())
	}
	correlationID := c.nextCorr.Add(1)
	waiting := make(chan result, 1)
	c.pendingMu.Lock()
	c.pending[correlationID] = waiting
	c.pendingMu.Unlock()
	frame := (&encoder{}).u32(correlationID).raw(content).buf
	if err := c.SendFrame(key, 1, frame); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
		return Frame{}, err
	}
	select {
	case got := <-waiting:
		return got.frame, got.err
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
		return Frame{}, ctx.Err()
	case <-c.done:
		c.pendingMu.Lock()
		delete(c.pending, correlationID)
		c.pendingMu.Unlock()
		return Frame{}, fmt.Errorf("rabbitmq stream: the connection closed: %w", c.Err())
	}
}

// SendFrame writes one command or response frame.
func (c *Conn) SendFrame(key, version uint16, content []byte) error {
	if c.IsClosed() {
		return fmt.Errorf("rabbitmq stream: the connection is closed: %w", c.Err())
	}
	size := 4 + len(content)
	if max := c.MaxFrame(); max > 0 && size > max {
		return fmt.Errorf("rabbitmq stream: a frame of %d bytes is over the negotiated %d", size, max)
	}
	var head [8]byte
	binary.BigEndian.PutUint32(head[0:], uint32(size))
	binary.BigEndian.PutUint16(head[4:], key)
	binary.BigEndian.PutUint16(head[6:], version)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.netConn.SetWriteDeadline(time.Now().Add(c.cfg.RequestTimeout))
	if _, err := c.writer.Write(head[:]); err != nil {
		return err
	}
	if _, err := c.writer.Write(content); err != nil {
		return err
	}
	return c.writer.Flush()
}

// Close ends the connection: it tells the broker and closes the socket. It is idempotent, and
// does not call the close handler.
func (c *Conn) Close() error { return c.closeWith("snodra: the connection is closed") }

func (c *Conn) closeWith(reason string) error {
	if c.closed.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, _ = c.Request(ctx, cmdClose, (&encoder{}).u16(CodeOK).str(reason).buf)
	cancel()
	c.shutdown(nil)
	return nil
}

func (c *Conn) fail(err error) {
	c.shutdown(err)
}

func (c *Conn) shutdown(err error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		close(c.done)
		_ = c.netConn.Close()
		c.pendingMu.Lock()
		for id, waiting := range c.pending {
			delete(c.pending, id)
			select {
			case waiting <- result{err: err}:
			default:
			}
		}
		c.pendingMu.Unlock()
		c.handlerMu.Lock()
		h := c.closeHandler
		c.handlerMu.Unlock()
		if h != nil {
			h(err)
		}
	})
}
