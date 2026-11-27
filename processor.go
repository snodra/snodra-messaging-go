package snodra

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync/atomic"
)

// DefaultProcessor is the processor of the event handlers registered without [InProcessor]. Its
// subscription is "<service>.default" and its stage-1 markers are "processor:default:<eventId>".
const DefaultProcessor = "default"

// processorName is the shape a processor's name has: a lower-case letter or digit, then up to 47
// lower-case letters, digits or dashes.
var processorName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

// ProcessorSubscription is the name a processor's subscription has on the event store:
// "<service>.<processor>". Every instance of the service joins it, so they share the processor's
// segments.
func ProcessorSubscription(service, processor string) string {
	return service + "." + processor
}

// ProcessorMarker is the id of the stage-1 marker a processor records for the event with id
// eventID: "processor:<processor>:<eventId>". A marker recorded in the handler's transaction is
// what makes a redelivered event a duplicate.
func ProcessorMarker(processor, eventID string) string {
	return "processor:" + processor + ":" + eventID
}

// eventHandler is one handler of one event type in one processor.
type eventHandler struct {
	// processor is the name of the processor it runs in.
	processor string
	// name is the event's wire type name.
	name string
	// invoke decodes one event and runs the handler.
	invoke func(ctx context.Context, codec PayloadCodec, payload []byte) error
}

// call runs the handler on payload: it decodes a fresh message of the handler's type, runs the
// handler, and turns a panic into an error so the processor survives it.
func (h *eventHandler) call(ctx context.Context, codec PayloadCodec, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("snodra: the event handler for %s panicked: %v", h.name, r)
		}
	}()
	return h.invoke(ctx, codec, payload)
}

// processor is one named event processor: its subscription, its handlers by event type and the
// types it handles, sorted. The consumer reads the subscription and runs the handlers; the
// runtime state beside it is what its status reports and what pause acts on.
type processor struct {
	name         string
	subscription SubscriptionSpec
	// handlers are the handlers of each event type, in registration order.
	handlers map[string][]*eventHandler
	// eventTypes are the types with a handler here, sorted.
	eventTypes []string
	// run is the processor's in-memory state: pause, assignment, last error, replay ranges.
	run *processorRuntime
	// processedTotal counts the records whose stage 1 completed, handled or duplicate.
	processedTotal atomic.Uint64
	// failedTotal counts the records that exhausted their attempts.
	failedTotal atomic.Uint64
	// deadLetterCount is the processor's letters, sampled every deadLetterSample.
	deadLetterCount atomic.Uint64
	// parkedSinceSample is set when a letter was parked: the sampler looks before its next tick.
	parkedSinceSample atomic.Bool
}

// newProcessor builds a processor of name reading source for service, with handlers by type. It
// keeps the handler order and sorts the types.
func newProcessor(service, name, source string, handlers map[string][]*eventHandler) *processor {
	p := &processor{
		name:         name,
		subscription: SubscriptionSpec{Name: ProcessorSubscription(service, name), Source: source},
		handlers:     handlers,
		eventTypes:   make([]string, 0, len(handlers)),
		run:          newProcessorRuntime(),
	}
	for typ, list := range handlers {
		if len(list) > 0 {
			p.eventTypes = append(p.eventTypes, typ)
		}
	}
	sort.Strings(p.eventTypes)
	return p
}
