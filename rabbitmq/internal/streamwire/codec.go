// Package streamwire is the low-level RabbitMQ Streams protocol this add-on speaks: the
// handshake, the frames the event store needs (metadata, partitions, publish, subscribe,
// consumer updates and offset tracking), and the chunk layout of a delivery.
//
// It is not a general stream client. The official Go client's consumers hard-code their
// subscribe properties and cannot carry the `snodra-member` property the Snodra wire layout
// requires, so the add-on speaks the few commands it needs itself, on connections of its own.
// The AMQP 1.0 messages it carries are still encoded and decoded by the official client's
// `pkg/amqp` package.
package streamwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// errShort reports a frame or a message shorter than the field it was read for.
var errShort = errors.New("rabbitmq stream: a frame ended before its field")

// encoder builds a command's content.
type encoder struct {
	buf []byte
}

func (e *encoder) u8(v uint8) *encoder {
	e.buf = append(e.buf, v)
	return e
}

func (e *encoder) u16(v uint16) *encoder {
	e.buf = binary.BigEndian.AppendUint16(e.buf, v)
	return e
}

func (e *encoder) i16(v int16) *encoder {
	return e.u16(uint16(v))
}

func (e *encoder) u32(v uint32) *encoder {
	e.buf = binary.BigEndian.AppendUint32(e.buf, v)
	return e
}

func (e *encoder) u64(v uint64) *encoder {
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
	return e
}

func (e *encoder) raw(b []byte) *encoder {
	e.buf = append(e.buf, b...)
	return e
}

// str writes a string (an int16 byte length and the bytes). A null string is a length of -1.
func (e *encoder) str(s string) *encoder {
	if len(s) > 0x7fff {
		s = s[:0x7fff]
	}
	e.i16(int16(len(s)))
	e.buf = append(e.buf, s...)
	return e
}

// bytes writes a byte sequence (an int32 length and the bytes); null is a length of -1.
func (e *encoder) bytes(b []byte) *encoder {
	if b == nil {
		return e.i32(-1)
	}
	e.u32(uint32(len(b)))
	e.buf = append(e.buf, b...)
	return e
}

func (e *encoder) i32(v int32) *encoder { return e.u32(uint32(v)) }

// strings writes a string array (an int32 count and the strings).
func (e *encoder) strings(all []string) *encoder {
	e.u32(uint32(len(all)))
	for _, s := range all {
		e.str(s)
	}
	return e
}

// properties writes a property map (an int32 count and the pairs, names sorted so a test can
// pin the bytes).
func (e *encoder) properties(all map[string]string) *encoder {
	e.u32(uint32(len(all)))
	for _, name := range sortedKeys(all) {
		e.str(name).str(all[name])
	}
	return e
}

// decoder reads a command's content.
type decoder struct {
	buf []byte
	off int
	err error
}

func (d *decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

func (d *decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if len(d.buf)-d.off < n {
		d.fail(errShort)
		return false
	}
	return true
}

func (d *decoder) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.buf[d.off]
	d.off++
	return v
}

func (d *decoder) u16() uint16 {
	if !d.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(d.buf[d.off:])
	d.off += 2
	return v
}

func (d *decoder) i16() int16 { return int16(d.u16()) }

func (d *decoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf[d.off:])
	d.off += 4
	return v
}

func (d *decoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf[d.off:])
	d.off += 8
	return v
}

func (d *decoder) i64() int64 { return int64(d.u64()) }

func (d *decoder) raw(n int) []byte {
	if !d.need(n) {
		return nil
	}
	v := d.buf[d.off : d.off+n]
	d.off += n
	return v
}

// str reads a string; a null string reads as empty.
func (d *decoder) str() string {
	n := d.i16()
	if d.err != nil || n < 0 {
		return ""
	}
	if !d.need(int(n)) {
		return ""
	}
	v := string(d.buf[d.off : d.off+int(n)])
	d.off += int(n)
	return v
}

// strings reads a string array.
func (d *decoder) strings() []string {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if n > 1<<20 {
		d.fail(fmt.Errorf("rabbitmq stream: a string array of %d entries", n))
		return nil
	}
	all := make([]string, 0, n)
	for i := uint32(0); i < n; i++ {
		all = append(all, d.str())
	}
	return all
}

// finish reports the first read error.
func (d *decoder) finish() error { return d.err }

// String is for test failures.
func (d *decoder) String() string { return fmt.Sprintf("decoder{%d/%d}", d.off, len(d.buf)) }

func sortedKeys(all map[string]string) []string {
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
