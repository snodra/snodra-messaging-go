package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/rabbitmq/internal/streamwire"
)

// layoutFor is how long a source's partitions and a stream's leader are trusted before they are
// read again, so a partition added to a super stream is published to.
const layoutFor = 30 * time.Second

type cachedLayout struct {
	layout sourceLayout
	at     time.Time
}

type cachedLeader struct {
	address string
	at      time.Time
}

// publisher publishes one event at a time (the outbox relay is sequential), each waiting for
// the broker's confirm. It holds one connection per node the leaders name.
type publisher struct {
	store   *EventStore
	mu      sync.Mutex
	seed    *streamwire.Conn
	layouts map[string]cachedLayout
	leaders map[string]cachedLeader
	conns   map[string]*pubConn
	nextID  atomic.Uint64
}

type pubConn struct {
	conn       *streamwire.Conn
	publishers map[string]uint8
	nextID     uint8
	mu         sync.Mutex
	waiting    map[uint64]chan confirmResult
}

type confirmResult struct {
	err error
}

func newPublisher(store *EventStore) *publisher {
	return &publisher{
		store:   store,
		layouts: make(map[string]cachedLayout),
		leaders: make(map[string]cachedLeader),
		conns:   make(map[string]*pubConn),
	}
}

func (p *publisher) publish(ctx context.Context, event snodra.OutgoingEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	seed, err := p.seedConn(ctx)
	if err != nil {
		return publishError(ctx, err)
	}
	layout, err := p.layout(ctx, seed, event.Source)
	if err != nil {
		return publishError(ctx, err)
	}
	if len(layout.parts) == 0 {
		return fmt.Errorf("%w: the source %s has no partitions", snodra.ErrFailed, event.Source)
	}
	part := layout.parts[partitionIndex(routingKey(event.Key, event.ID), len(layout.parts))]
	address, err := p.leader(ctx, seed, part.stream)
	if err != nil {
		return publishError(ctx, err)
	}
	connection, err := p.connection(ctx, address)
	if err != nil {
		return publishError(ctx, err)
	}
	publisherID, err := connection.publisher(ctx, p.store, part.stream)
	if err != nil {
		p.forget(address, connection)
		return publishError(ctx, err)
	}
	encoded, err := marshalMessage(event)
	if err != nil {
		return fmt.Errorf("%w: %w", snodra.ErrInvalid, err)
	}
	frameSize := len(encoded) + 25
	if frameSize > streamwire.MaxFrame {
		return fmt.Errorf("%w: the event %s is %d bytes on the wire, over the %d-byte frame",
			snodra.ErrRejected, event.ID, frameSize, streamwire.MaxFrame)
	}
	publishingID := p.nextID.Add(1)
	confirm := make(chan confirmResult, 1)
	connection.expect(publishingID, confirm)
	if err := connection.conn.Publish(publisherID, publishingID, encoded); err != nil {
		connection.forget(publishingID)
		p.forget(address, connection)
		return fmt.Errorf("%w: publishing to %s: %w", snodra.ErrFailed, part.stream, err)
	}
	timer := time.NewTimer(p.store.config.ConfirmTimeout)
	defer timer.Stop()
	select {
	case result := <-confirm:
		if result.err != nil {
			return result.err
		}
		return nil
	case <-ctx.Done():
		connection.forget(publishingID)
		return ctx.Err()
	case <-timer.C:
		// A connection that confirms nothing in time is replaced: its read task may have
		// stopped, and every later publish on it would wait as long.
		connection.forget(publishingID)
		p.forget(address, connection)
		return fmt.Errorf("%w: the broker did not confirm an event to %s within %v: the connection is replaced",
			snodra.ErrFailed, event.Source, p.store.config.ConfirmTimeout)
	}
}

// publishError keeps a done context as-is.
func publishError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (p *publisher) seedConn(ctx context.Context) (*streamwire.Conn, error) {
	if p.seed != nil && !p.seed.IsClosed() {
		return p.seed, nil
	}
	seed, err := p.store.dialAny(ctx, storeConnection)
	if err != nil {
		return nil, err
	}
	p.seed = seed
	return seed, nil
}

func (p *publisher) layout(ctx context.Context, seed *streamwire.Conn, source string) (sourceLayout, error) {
	if cached, ok := p.layouts[source]; ok && time.Since(cached.at) < layoutFor {
		return cached.layout, nil
	}
	layout, err := layoutOf(ctx, seed, source)
	if err != nil {
		return sourceLayout{}, err
	}
	p.layouts[source] = cachedLayout{layout: layout, at: time.Now()}
	return layout, nil
}

func (p *publisher) leader(ctx context.Context, seed *streamwire.Conn, stream string) (string, error) {
	if cached, ok := p.leaders[stream]; ok && time.Since(cached.at) < layoutFor {
		return cached.address, nil
	}
	address, err := leaderOf(ctx, seed, stream)
	if err != nil {
		return "", err
	}
	p.leaders[stream] = cachedLeader{address: address, at: time.Now()}
	return address, nil
}

func (p *publisher) connection(ctx context.Context, address string) (*pubConn, error) {
	if existing, ok := p.conns[address]; ok && !existing.conn.IsClosed() {
		return existing, nil
	}
	conn, err := p.store.dial(ctx, address, storeConnection)
	if err != nil {
		return nil, err
	}
	created := &pubConn{
		conn:       conn,
		publishers: make(map[string]uint8),
		nextID:     1,
		waiting:    make(map[uint64]chan confirmResult),
	}
	conn.SetConfirmHandler(created.confirmed)
	conn.SetCloseHandler(func(err error) {
		// A publish waiting for a confirm fails at once; the next call dials a new connection.
		if err == nil {
			created.failAll(fmt.Errorf("%w: the connection to %s closed", snodra.ErrFailed, address))
		} else {
			created.failAll(fmt.Errorf("%w: the connection to %s closed: %w", snodra.ErrFailed, address, err))
		}
	})
	p.conns[address] = created
	return created, nil
}

// forget drops a broken connection and everything it held.
func (p *publisher) forget(address string, connection *pubConn) {
	if existing, ok := p.conns[address]; ok && existing == connection {
		delete(p.conns, address)
	}
	p.layouts = make(map[string]cachedLayout)
	p.leaders = make(map[string]cachedLeader)
	_ = connection.conn.Close()
	connection.failAll(fmt.Errorf("%w: the connection to %s was replaced", snodra.ErrFailed, address))
}

func (p *publisher) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seed != nil {
		_ = p.seed.Close()
		p.seed = nil
	}
	for address, connection := range p.conns {
		_ = connection.conn.Close()
		connection.failAll(fmt.Errorf("%w: the store is closed", snodra.ErrFailed))
		delete(p.conns, address)
	}
}

func (c *pubConn) confirmed(confirm streamwire.Confirm) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range confirm.Accepted {
		if waiting, ok := c.waiting[id]; ok {
			delete(c.waiting, id)
			waiting <- confirmResult{}
		}
	}
	for id, code := range confirm.Errors {
		if waiting, ok := c.waiting[id]; ok {
			delete(c.waiting, id)
			waiting <- confirmResult{err: refusal("publish", code)}
		}
	}
}

func (c *pubConn) expect(id uint64, waiting chan confirmResult) {
	c.mu.Lock()
	c.waiting[id] = waiting
	c.mu.Unlock()
}

func (c *pubConn) forget(id uint64) {
	c.mu.Lock()
	delete(c.waiting, id)
	c.mu.Unlock()
}

func (c *pubConn) failAll(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, waiting := range c.waiting {
		delete(c.waiting, id)
		waiting <- confirmResult{err: err}
	}
}

func (c *pubConn) publisher(ctx context.Context, store *EventStore, stream string) (uint8, error) {
	if id, ok := c.publishers[stream]; ok {
		return id, nil
	}
	if c.nextID == 0 {
		return 0, fmt.Errorf("%w: 255 publishers were declared on the connection to %s: it is replaced", snodra.ErrFailed, store.address(store.config.Hosts[0]))
	}
	id := c.nextID
	c.nextID++
	if err := c.conn.DeclarePublisher(ctx, id, stream); err != nil {
		return 0, classify("declare publisher", err)
	}
	c.publishers[stream] = id
	return id, nil
}

// refusal is the publisher's answer as an error: for good (ErrRejected) when publishing again
// cannot succeed (not authorized, a frame too large), else failed.
func refusal(command string, code uint16) error {
	if code == streamwire.CodeAccessRefused || code == streamwire.CodeFrameTooLarge {
		return fmt.Errorf("%w: rabbitmq: %s: response code 0x%02x", snodra.ErrRejected, command, code)
	}
	return fmt.Errorf("%w: rabbitmq: %s: response code 0x%02x", snodra.ErrFailed, command, code)
}

// classify maps a stream command's error onto the core's errors.
func classify(command string, err error) error {
	var refused *streamwire.ResponseError
	if errors.As(err, &refused) {
		return refusal(command, refused.Code)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: rabbitmq: %s: %w", snodra.ErrFailed, command, err)
}
