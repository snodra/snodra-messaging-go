package snodra

import (
	"encoding/json"
	"reflect"
)

// Message is a typed message of the Snodra messaging suite. Its wire name
// (for example "orders.PlaceOrder") is the type every handler registers for
// and every sender addresses. The method may have a value or a pointer
// receiver. See https://snodra.damoreira.com/docs/library-authors/messages/.
type Message interface{ MessageType() string }

// PayloadCodec turns message values into payload bytes and back.
type PayloadCodec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// JSON is the default payload codec: encoding/json, with unknown fields
// ignored on read. See https://snodra.damoreira.com/docs/library-authors/messages/.
var JSON PayloadCodec = jsonCodec{}

// Empty is the result type of a command, which answers without a result. It
// encodes as {}.
type Empty struct{}

// typeName returns the wire name of M, for a value or a pointer message type.
func typeName[M Message]() string {
	return newValue[M]().MessageType()
}

// newValue returns a fresh M; for a pointer type, a pointer to a new value.
func newValue[M Message]() M {
	var zero M
	t := reflect.TypeOf(zero)
	if t != nil && t.Kind() == reflect.Pointer {
		return reflect.New(t.Elem()).Interface().(M)
	}
	return zero
}
