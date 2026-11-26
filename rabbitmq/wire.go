package rabbitmq

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The layout of one event as an AMQP 1.0 message, as every Snodra RabbitMQ Streams add-on
// writes it (https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules):
// exactly one data section holding the JSON payload, the event id as the string message-id,
// application/json as the content type, the key as the symbol-keyed message annotation
// x-routing-key (absent for an event without one), and every header as an application property
// with a string value.
const (
	contentType   = "application/json"
	keyAnnotation = "x-routing-key"
)

// message is the AMQP message of event.
func message(event snodra.OutgoingEvent) *amqp.Message {
	properties := &amqp.MessageProperties{
		MessageID:   event.ID,
		ContentType: contentType,
	}
	headers := make(map[string]any, len(event.Headers))
	names := make([]string, 0, len(event.Headers))
	for name := range event.Headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		headers[name] = event.Headers[name]
	}
	out := &amqp.Message{
		Properties:            properties,
		ApplicationProperties: headers,
		Data:                  [][]byte{event.Payload},
	}
	if event.Key != nil {
		out.Annotations = amqp.Annotations{keyAnnotation: *event.Key}
	}
	return out
}

// marshalMessage is the AMQP 1.0 encoding of event.
func marshalMessage(event snodra.OutgoingEvent) ([]byte, error) {
	encoded, err := message(event).MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: the AMQP message of %s: %w", event.ID, err)
	}
	return encoded, nil
}

// decodeMessage reads one delivered AMQP message into a delivery's key, headers and payload.
func decodeMessage(data []byte) (key *string, headers map[string]string, payload []byte, err error) {
	decoded := &amqp.Message{}
	if err := decoded.UnmarshalBinary(data); err != nil {
		return nil, nil, nil, fmt.Errorf("rabbitmq: a delivered AMQP message: %w", err)
	}
	if value, ok := decoded.Annotations[keyAnnotation]; ok {
		if text, ok := asText(value); ok {
			key = &text
		}
	}
	headers = make(map[string]string, len(decoded.ApplicationProperties))
	for name, value := range decoded.ApplicationProperties {
		if value == nil {
			headers[name] = ""
			continue
		}
		if text, ok := asText(value); ok {
			headers[name] = text
			continue
		}
		headers[name] = fmt.Sprint(value)
	}
	for _, section := range decoded.Data {
		payload = append(payload, section...)
	}
	return key, headers, payload, nil
}

// asText is an AMQP simple value as a header shows it: a string's UTF-8, a binary's bytes,
// other values as their text.
func asText(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	case bool:
		return strconv.FormatBool(value), true
	case int8:
		return strconv.FormatInt(int64(value), 10), true
	case int16:
		return strconv.FormatInt(int64(value), 10), true
	case int32:
		return strconv.FormatInt(int64(value), 10), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case uint8:
		return strconv.FormatUint(uint64(value), 10), true
	case uint16:
		return strconv.FormatUint(uint64(value), 10), true
	case uint32:
		return strconv.FormatUint(uint64(value), 10), true
	case uint64:
		return strconv.FormatUint(value, 10), true
	case float32:
		return strconv.FormatFloat(float64(value), 'g', -1, 32), true
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64), true
	default:
		return "", false
	}
}
