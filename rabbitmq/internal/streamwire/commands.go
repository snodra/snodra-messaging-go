package streamwire

import (
	"context"
	"fmt"
)

// The command keys of the stream protocol (the most significant bit marks a response).
const (
	cmdDeclarePublisher          = 0x0001
	cmdPublish                   = 0x0002
	cmdPublishConfirm            = 0x0003
	cmdPublishError              = 0x0004
	cmdQueryPublisherSeq         = 0x0005
	cmdDeletePublisher           = 0x0006
	cmdSubscribe                 = 0x0007
	cmdDeliver                   = 0x0008
	cmdCredit                    = 0x0009
	cmdStoreOffset               = 0x000a
	cmdQueryOffset               = 0x000b
	cmdUnsubscribe               = 0x000c
	cmdCreate                    = 0x000d
	cmdDelete                    = 0x000e
	cmdMetadata                  = 0x000f
	cmdMetadataUpdate            = 0x0010
	cmdPeerProperties            = 0x0011
	cmdSaslHandshake             = 0x0012
	cmdSaslAuthenticate          = 0x0013
	cmdTune                      = 0x0014
	cmdOpen                      = 0x0015
	cmdClose                     = 0x0016
	cmdHeartbeat                 = 0x0017
	cmdRoute                     = 0x0018
	cmdPartitions                = 0x0019
	cmdConsumerUpdate            = 0x001a
	cmdExchangeCommandVer        = 0x001b
	cmdStreamStats               = 0x001c
	cmdCreateSuperStream         = 0x001d
	cmdDeleteSuperStream         = 0x001e
	cmdResolveOffsetSpec         = 0x001f
	responseBit           uint16 = 0x8000
)

// The protocol's response codes this add-on acts on.
const (
	CodeOK                 uint16 = 0x01
	CodeStreamDoesNotExist uint16 = 0x02
	CodeFrameTooLarge      uint16 = 0x0e
	CodeAccessRefused      uint16 = 0x10
	CodeNoOffset           uint16 = 0x13
)

// ResponseError is a refused command: its response code and what was refused.
type ResponseError struct {
	Command string
	Code    uint16
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("rabbitmq stream: %s: response code 0x%02x", e.Command, e.Code)
}

// Rejected reports whether the broker's refusal is final (another try cannot succeed).
func (e *ResponseError) Rejected() bool {
	return e.Code == CodeAccessRefused || e.Code == CodeFrameTooLarge
}

// OffsetType is how a subscription resumes.
type OffsetType uint16

// The offset specifications of the protocol.
const (
	OffsetFirst     OffsetType = 1
	OffsetLast      OffsetType = 2
	OffsetNext      OffsetType = 3
	OffsetOffset    OffsetType = 4
	OffsetTimestamp OffsetType = 5
)

// OffsetSpec is one offset specification: its type and, for OffsetOffset and OffsetTimestamp,
// its value.
type OffsetSpec struct {
	Type  OffsetType
	Value uint64
}

// First starts at the first offset the stream still holds.
func First() OffsetSpec { return OffsetSpec{Type: OffsetFirst} }

// Last starts at the last chunk.
func Last() OffsetSpec { return OffsetSpec{Type: OffsetLast} }

// Next starts at the next offset written.
func Next() OffsetSpec { return OffsetSpec{Type: OffsetNext} }

// At starts at an offset.
func At(offset uint64) OffsetSpec { return OffsetSpec{Type: OffsetOffset, Value: offset} }

// Timestamp starts at the first chunk written at or after epoch milliseconds.
func Timestamp(ms int64) OffsetSpec {
	return OffsetSpec{Type: OffsetTimestamp, Value: uint64(ms)}
}

func (o OffsetSpec) write(e *encoder) *encoder {
	e.u16(uint16(o.Type))
	if o.Type == OffsetOffset || o.Type == OffsetTimestamp {
		e.u64(o.Value)
	}
	return e
}

// Broker is a node as metadata advertises it.
type Broker struct {
	Reference uint16
	Host      string
	Port      uint32
}

// StreamMetadata is a stream's answer in a metadata query.
type StreamMetadata struct {
	Name         string
	ResponseCode uint16
	Leader       Broker
	Replicas     []Broker
}

// Metadata is one metadata answer: the nodes it names and the streams it answered for.
type Metadata struct {
	Brokers []Broker
	Streams map[string]StreamMetadata
}

// Metadata asks every stream's leader.
func (c *Conn) Metadata(ctx context.Context, streams ...string) (Metadata, error) {
	content := (&encoder{}).strings(streams).buf
	frame, err := c.Request(ctx, cmdMetadata, content)
	if err != nil {
		return Metadata{}, err
	}
	// A metadata answer has no response code; a stream not found is in its stream entry.
	d := &decoder{buf: frame.Content}
	answer := Metadata{Streams: make(map[string]StreamMetadata)}
	brokerCount := d.u32()
	if brokerCount > 1<<16 {
		return Metadata{}, fmt.Errorf("rabbitmq stream: a metadata answer names %d nodes", brokerCount)
	}
	for i := uint32(0); i < brokerCount; i++ {
		answer.Brokers = append(answer.Brokers, Broker{
			Reference: d.u16(),
			Host:      d.str(),
			Port:      d.u32(),
		})
	}
	streamCount := d.u32()
	for i := uint32(0); i < streamCount; i++ {
		name := d.str()
		code := d.u16()
		leader := d.u16()
		replicaCount := d.u32()
		replicas := make([]uint16, 0, replicaCount)
		for j := uint32(0); j < replicaCount; j++ {
			replicas = append(replicas, d.u16())
		}
		found := StreamMetadata{Name: name, ResponseCode: code}
		for _, broker := range answer.Brokers {
			if broker.Reference == leader {
				found.Leader = broker
			}
			for _, replica := range replicas {
				if broker.Reference == replica {
					found.Replicas = append(found.Replicas, broker)
				}
			}
		}
		answer.Streams[name] = found
	}
	if err := d.finish(); err != nil {
		return Metadata{}, fmt.Errorf("rabbitmq stream: a metadata answer: %w", err)
	}
	return answer, nil
}

// Partitions is the partition streams of a super stream, in the broker's order.
func (c *Conn) Partitions(ctx context.Context, superStream string) ([]string, error) {
	content := (&encoder{}).str(superStream).buf
	frame, err := c.Request(ctx, cmdPartitions, content)
	if err != nil {
		return nil, err
	}
	if frame.Code != CodeOK {
		return nil, &ResponseError{Command: "partitions", Code: frame.Code}
	}
	d := &decoder{buf: frame.Content}
	partitions := d.strings()
	if err := d.finish(); err != nil {
		return nil, fmt.Errorf("rabbitmq stream: a partitions answer: %w", err)
	}
	return partitions, nil
}

// DeclarePublisher declares publisherID on stream, without a publisher reference (no broker
// deduplication).
func (c *Conn) DeclarePublisher(ctx context.Context, publisherID uint8, stream string) error {
	content := (&encoder{}).u8(publisherID).str("").str(stream).buf
	frame, err := c.Request(ctx, cmdDeclarePublisher, content)
	if err != nil {
		return err
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "declare publisher", Code: frame.Code}
	}
	return nil
}

// DeletePublisher deletes publisherID.
func (c *Conn) DeletePublisher(ctx context.Context, publisherID uint8) error {
	content := (&encoder{}).u8(publisherID).buf
	frame, err := c.Request(ctx, cmdDeletePublisher, content)
	if err != nil {
		return err
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "delete publisher", Code: frame.Code}
	}
	return nil
}

// Publish sends one AMQP message under publisherID, with the given publishing id. It returns
// when the frame is written; the broker's confirm comes through OnConfirm.
func (c *Conn) Publish(publisherID uint8, publishingID uint64, message []byte) error {
	content := (&encoder{}).
		u8(publisherID).
		u32(1).
		u64(publishingID).
		bytes(message).
		buf
	return c.SendFrame(cmdPublish, 1, content)
}

// OnConfirm is called for every publish confirmation and publish error the broker sends.
type OnConfirm func(confirm Confirm)

// Confirm is a broker's answer for published messages: the ones it accepted, and the errors.
type Confirm struct {
	PublisherID uint8
	Accepted    []uint64
	Errors      map[uint64]uint16
}

// Subscribe subscribes subscriptionID to stream with the given offset and properties, and one
// credit.
func (c *Conn) Subscribe(ctx context.Context, subscriptionID uint8, stream string, offset OffsetSpec, credit uint16, properties map[string]string) error {
	e := (&encoder{}).u8(subscriptionID).str(stream)
	offset.write(e)
	e.u16(credit)
	e.properties(properties)
	frame, err := c.Request(ctx, cmdSubscribe, e.buf)
	if err != nil {
		return err
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "subscribe", Code: frame.Code}
	}
	return nil
}

// Credit grants the broker credits more chunks for subscriptionID.
func (c *Conn) Credit(subscriptionID uint8, credits int16) error {
	content := (&encoder{}).u8(subscriptionID).i16(credits).buf
	return c.SendFrame(cmdCredit, 1, content)
}

// StoreOffset stores offset as the next offset to read under reference on stream. The broker
// applies it asynchronously; QueryOffset reads it back.
func (c *Conn) StoreOffset(reference, stream string, offset uint64) error {
	content := (&encoder{}).str(reference).str(stream).u64(offset).buf
	return c.SendFrame(cmdStoreOffset, 1, content)
}

// QueryOffset reads the offset stored under reference on stream; found is false when none is.
func (c *Conn) QueryOffset(ctx context.Context, reference, stream string) (offset uint64, found bool, err error) {
	content := (&encoder{}).str(reference).str(stream).buf
	frame, err := c.Request(ctx, cmdQueryOffset, content)
	if err != nil {
		return 0, false, err
	}
	switch frame.Code {
	case CodeOK:
		d := &decoder{buf: frame.Content}
		offset = d.u64()
		if err := d.finish(); err != nil {
			return 0, false, fmt.Errorf("rabbitmq stream: a query offset answer: %w", err)
		}
		return offset, true, nil
	case CodeNoOffset:
		return 0, false, nil
	default:
		return 0, false, &ResponseError{Command: "query offset", Code: frame.Code}
	}
}

// Unsubscribe ends subscriptionID.
func (c *Conn) Unsubscribe(ctx context.Context, subscriptionID uint8) error {
	content := (&encoder{}).u8(subscriptionID).buf
	frame, err := c.Request(ctx, cmdUnsubscribe, content)
	if err != nil {
		return err
	}
	if frame.Code != CodeOK {
		return &ResponseError{Command: "unsubscribe", Code: frame.Code}
	}
	return nil
}

// ConsumerUpdateResponse answers a consumer update: from is where an active consumer starts, or
// Next for a consumer that stops being active.
func (c *Conn) ConsumerUpdateResponse(correlationID uint32, from OffsetSpec) error {
	e := (&encoder{}).u32(correlationID).u16(CodeOK)
	from.write(e)
	return c.SendFrame(cmdConsumerUpdate|responseBit, 1, e.buf)
}

// Close tells the broker the connection ends and closes it.
