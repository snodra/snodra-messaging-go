package snodramessaging

import "context"

// MessageInfo describes the request a handler is handling.
type MessageInfo struct {
	// ID is the forwarded Request.id: the sender's id for a command or a point query, a
	// derived one ("<id>#<n>") for the multi-part modes. Never use it to detect a retried
	// command: a retry has a fresh id.
	ID string
	// Type is the message type, for example "orders.PlaceOrder".
	Type string
	// CorrelationID is the request's correlationId header, else its ID.
	CorrelationID string
	// Headers are the request's headers, as the hub forwarded them.
	Headers map[string]string
}

// correlationIDHeader is the header that carries the correlation id
// (https://snodra.damoreira.com/library-authors/messages/).
const correlationIDHeader = "correlationId"

type messageInfoKey struct{}

// MessageFrom returns what the handler's context says of the request being handled, and false
// outside a handler.
func MessageFrom(ctx context.Context) (MessageInfo, bool) {
	info, ok := ctx.Value(messageInfoKey{}).(MessageInfo)
	return info, ok
}

func withMessageInfo(ctx context.Context, info MessageInfo) context.Context {
	return context.WithValue(ctx, messageInfoKey{}, info)
}
