package streamwire

import (
	"fmt"
)

// Chunk is one delivered chunk of a stream: where its first message is, when its leader wrote
// it, and the messages themselves.
type Chunk struct {
	SubscriptionID uint8
	FirstOffset    uint64
	TimestampMs    int64
	// Messages are the AMQP messages of the chunk, in offset order.
	Messages []ChunkMessage
	// Records is the chunk's record count (including sub-entries the store cannot decode).
	Records uint32
}

// ChunkMessage is one AMQP message and its offset.
type ChunkMessage struct {
	Offset uint64
	Data   []byte
}

// ChunkHeader is what a delivered chunk says without decoding its messages.
type ChunkHeader struct {
	SubscriptionID uint8
	FirstOffset    uint64
	Records        uint32
	TimestampMs    int64
	ChunkType      uint8
}

// ParseChunkHeader reads a deliver frame's chunk header, without its messages.
func ParseChunkHeader(frame Frame) (ChunkHeader, error) {
	if frame.Key != cmdDeliver {
		return ChunkHeader{}, fmt.Errorf("rabbitmq stream: frame 0x%04x is not a deliver", frame.Key)
	}
	d := &decoder{buf: frame.Content}
	header := ChunkHeader{SubscriptionID: d.u8()}
	if frame.Version >= 2 {
		_ = d.u64()
	}
	_ = d.u8() // magic version
	header.ChunkType = d.u8()
	_ = d.u16() // entries
	header.Records = d.u32()
	header.TimestampMs = d.i64()
	_ = d.u64() // epoch
	header.FirstOffset = d.u64()
	if err := d.finish(); err != nil {
		return ChunkHeader{}, fmt.Errorf("rabbitmq stream: a deliver chunk header: %w", err)
	}
	return header, nil
}

// ParseDeliver reads a deliver frame's chunk.
func ParseDeliver(frame Frame) (Chunk, error) {
	if frame.Key != cmdDeliver {
		return Chunk{}, fmt.Errorf("rabbitmq stream: frame 0x%04x is not a deliver", frame.Key)
	}
	d := &decoder{buf: frame.Content}
	chunk := Chunk{SubscriptionID: d.u8()}
	if frame.Version >= 2 {
		// The committed chunk id of a super stream consumer; not part of the layout here.
		_ = d.u64()
	}
	_ = d.u8() // magic version
	chunkType := d.u8()
	_ = d.u16() // entries
	chunk.Records = d.u32()
	chunk.TimestampMs = d.i64()
	_ = d.u64() // epoch
	chunk.FirstOffset = d.u64()
	_ = d.u32() // crc
	dataLength := d.u32()
	trailerLength := d.u32()
	_ = d.u32() // bloom size and reserved
	if err := d.finish(); err != nil {
		return Chunk{}, fmt.Errorf("rabbitmq stream: a deliver chunk header: %w", err)
	}
	data := d.raw(int(dataLength))
	if err := d.finish(); err != nil {
		return Chunk{}, fmt.Errorf("rabbitmq stream: a deliver chunk: %w", err)
	}
	_ = d.raw(int(trailerLength))
	// A tracking chunk (the stream's offset-tracking entries) carries no messages to deliver;
	// it still takes offsets, which the chunk's header accounted for.
	if chunkType != 0 {
		return chunk, nil
	}
	inner := &decoder{buf: data}
	offset := chunk.FirstOffset
	for len(inner.buf)-inner.off > 0 && uint32(len(chunk.Messages)) < chunk.Records {
		// An entry is a 32-bit size; the most significant bit marks a sub-entry (a batch or a
		// compressed one), which the wire rules forbid on Snodra streams.
		sizeWord := inner.u32()
		if inner.err != nil {
			break
		}
		if sizeWord&0x80000000 != 0 {
			return Chunk{}, fmt.Errorf("rabbitmq stream: a delivered chunk holds a sub-entry (batch or compressed): the publisher must send one message per entry")
		}
		message := inner.raw(int(sizeWord))
		if inner.err != nil {
			return Chunk{}, fmt.Errorf("rabbitmq stream: a delivered message ended early: %w", inner.err)
		}
		chunk.Messages = append(chunk.Messages, ChunkMessage{Offset: offset, Data: message})
		offset++
	}
	if err := inner.finish(); err != nil {
		return Chunk{}, fmt.Errorf("rabbitmq stream: a delivered chunk: %w", err)
	}
	return chunk, nil
}

// ParseConsumerUpdate reads a consumer update's correlation id, subscription id and whether the
// consumer is active.
func ParseConsumerUpdate(frame Frame) (correlationID uint32, subscriptionID uint8, active bool, err error) {
	d := &decoder{buf: frame.Content}
	correlationID = d.u32()
	subscriptionID = d.u8()
	active = d.u8() == 1
	return correlationID, subscriptionID, active, d.finish()
}
