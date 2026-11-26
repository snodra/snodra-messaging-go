package rabbitmq

import (
	"bytes"
	"fmt"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The sections of one encoded AMQP 1.0 message, with the type codes the Snodra layout fixes.
type amqpValue struct {
	code byte
	str  []byte
	sym  []byte
	bin  []byte
	list []amqpValue
	m    []amqpValue
}

func parseAMQPValue(t *testing.T, b *[]byte) amqpValue {
	t.Helper()
	take := func(n int) []byte {
		if len(*b) < n {
			t.Fatalf("an AMQP value ended before its %d bytes", n)
		}
		out := (*b)[:n]
		*b = (*b)[n:]
		return out
	}
	u32 := func() int {
		b4 := take(4)
		return int(b4[0])<<24 | int(b4[1])<<16 | int(b4[2])<<8 | int(b4[3])
	}
	code := take(1)[0]
	switch code {
	case 0x40:
		return amqpValue{code: code}
	case 0x43, 0x44:
		return amqpValue{code: code}
	case 0x52, 0x53:
		take(1)
		return amqpValue{code: code}
	case 0xa1:
		return amqpValue{code: code, str: take(int(take(1)[0]))}
	case 0xb1:
		return amqpValue{code: code, str: take(u32())}
	case 0xa3:
		return amqpValue{code: code, sym: take(int(take(1)[0]))}
	case 0xb3:
		return amqpValue{code: code, sym: take(u32())}
	case 0xa0:
		return amqpValue{code: code, bin: take(int(take(1)[0]))}
	case 0xb0:
		return amqpValue{code: code, bin: take(u32())}
	case 0x45:
		return amqpValue{code: code, list: []amqpValue{}}
	case 0xc0, 0xc1:
		size := int(take(1)[0]) - 1
		count := int(take(1)[0])
		inner := take(size)
		items := make([]amqpValue, 0, count)
		for i := 0; i < count; i++ {
			items = append(items, parseAMQPValue(t, &inner))
		}
		if code == 0xc1 {
			return amqpValue{code: code, m: items}
		}
		return amqpValue{code: code, list: items}
	case 0xd0, 0xd1:
		size := u32() - 4
		count := u32()
		inner := take(size)
		items := make([]amqpValue, 0, count)
		for i := 0; i < count; i++ {
			items = append(items, parseAMQPValue(t, &inner))
		}
		if code == 0xd1 {
			return amqpValue{code: code, m: items}
		}
		return amqpValue{code: code, list: items}
	default:
		t.Fatalf("unexpected AMQP type 0x%02x", code)
		return amqpValue{}
	}
}

func sectionsOf(t *testing.T, encoded []byte) map[byte]amqpValue {
	t.Helper()
	sections := make(map[byte]amqpValue)
	b := encoded
	for len(b) > 0 {
		if len(b) < 3 || b[0] != 0x00 || b[1] != 0x53 {
			t.Fatalf("a section descriptor: % x", b[:min(3, len(b))])
		}
		b = b[2:]
		code := b[0]
		b = b[1:]
		sections[code] = parseAMQPValue(t, &b)
	}
	return sections
}

func TestTheMessageHasTheTypesOfTheWireLayout(t *testing.T) {
	key := "order-42"
	event := snodra.OutgoingEvent{
		Source:  "snodra.events",
		ID:      "e-42",
		Type:    "orders.Placed",
		Key:     &key,
		Headers: map[string]string{"type": "orders.Placed", "id": "e-42", "x-extra": "kept"},
		Payload: []byte(`{"total":42}`),
	}
	encoded, err := marshalMessage(event)
	if err != nil {
		t.Fatal(err)
	}
	sections := sectionsOf(t, encoded)
	properties, ok := sections[0x73]
	if !ok || len(properties.list) <= 6 {
		t.Fatalf("properties (0x73): %+v", properties)
	}
	if properties.list[0].code != 0xa1 && properties.list[0].code != 0xb1 {
		t.Fatalf("message-id is an AMQP string, got type 0x%02x", properties.list[0].code)
	}
	if string(properties.list[0].str) != "e-42" {
		t.Fatalf("message-id: %q", properties.list[0].str)
	}
	if properties.list[6].code != 0xa3 && properties.list[6].code != 0xb3 {
		t.Fatalf("content-type is an AMQP symbol, got type 0x%02x", properties.list[6].code)
	}
	if string(properties.list[6].sym) != "application/json" {
		t.Fatalf("content-type: %q", properties.list[6].sym)
	}
	annotations, ok := sections[0x72]
	if !ok || len(annotations.m) != 2 {
		t.Fatalf("message annotations (0x72): %+v", annotations)
	}
	if annotations.m[0].code != 0xa3 && annotations.m[0].code != 0xb3 {
		t.Fatalf("the annotation key is a symbol, got type 0x%02x", annotations.m[0].code)
	}
	if string(annotations.m[0].sym) != "x-routing-key" || string(annotations.m[1].str) != "order-42" {
		t.Fatalf("the annotation: %q = %q", annotations.m[0].sym, annotations.m[1].str)
	}
	applications, ok := sections[0x74]
	if !ok || len(applications.m) != 6 {
		t.Fatalf("application properties (0x74): %+v", applications)
	}
	for i := 0; i < len(applications.m); i += 2 {
		if applications.m[i].code != 0xa1 && applications.m[i].code != 0xb1 {
			t.Fatalf("a header name is an AMQP string, got type 0x%02x", applications.m[i].code)
		}
		if applications.m[i+1].code != 0xa1 && applications.m[i+1].code != 0xb1 {
			t.Fatalf("a header value is an AMQP string, got type 0x%02x", applications.m[i+1].code)
		}
	}
	data, ok := sections[0x75]
	if !ok || !bytes.Equal(data.bin, event.Payload) {
		t.Fatalf("one data section with the payload: %+v", data)
	}
	if _, ok := sections[0x77]; ok {
		t.Fatal("no amqp-value section")
	}

	decodedKey, headers, payload, err := decodeMessage(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decodedKey == nil || *decodedKey != key {
		t.Fatalf("the key round-trips: %v", decodedKey)
	}
	for name, value := range event.Headers {
		if headers[name] != value {
			t.Fatalf("header %s: got %q, want %q", name, headers[name], value)
		}
	}
	if !bytes.Equal(payload, event.Payload) {
		t.Fatalf("the payload round-trips: %q", payload)
	}
}

func TestAnUnkeyedEventHasNoAnnotation(t *testing.T) {
	encoded, err := marshalMessage(snodra.OutgoingEvent{ID: "e-1", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sectionsOf(t, encoded)[0x72]; ok {
		t.Fatal("an unkeyed event has no x-routing-key annotation")
	}
}

func TestAnEmptyKeyIsAKey(t *testing.T) {
	key := ""
	encoded, err := marshalMessage(snodra.OutgoingEvent{ID: "e-1", Key: &key, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	annotations, ok := sectionsOf(t, encoded)[0x72]
	if !ok || len(annotations.m) != 2 || string(annotations.m[1].str) != "" {
		t.Fatalf("an empty key is announced as an empty string: %+v", annotations)
	}
}

func TestDecodeRefusesGarbage(t *testing.T) {
	if _, _, _, err := decodeMessage([]byte{0xff}); err == nil {
		t.Fatal("garbage is not an AMQP message")
	}
}

func (v amqpValue) String() string {
	return fmt.Sprintf("%+v", struct {
		Code byte
		Str  string
		Sym  string
		Bin  string
	}{v.code, string(v.str), string(v.sym), string(v.bin)})
}
