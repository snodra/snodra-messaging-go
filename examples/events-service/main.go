// The events service example: a command appends an event to the service's outbox in the same
// transaction as its own handling, the relay publishes it, and the service's own event processor
// handles it.
//
// The two in-memory stores make it runnable with nothing else; a real service replaces them with
// its own database (the message store) and its broker (the event store), for example
// snodra-messaging-go/mongo and snodra-messaging-go/kafka.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	snodra "github.com/snodra/snodra-messaging-go"
)

// PlaceOrder is a command: a customer orders something.
type PlaceOrder struct {
	OrderID string `json:"orderId"`
	SKU     string `json:"sku"`
	Cents   int64  `json:"cents"`
}

func (PlaceOrder) MessageType() string { return "orders.PlaceOrder" }

// OrderPlaced is the event the command records: it states what happened, and other services may
// handle it too.
type OrderPlaced struct {
	OrderID string `json:"orderId"`
	SKU     string `json:"sku"`
	Cents   int64  `json:"cents"`
}

func (OrderPlaced) MessageType() string { return "orders.OrderPlaced" }

// GetPlacedOrder is a query over what the event handler projected.
type GetPlacedOrder struct {
	OrderID string `json:"orderId"`
}

func (GetPlacedOrder) MessageType() string { return "orders.GetPlacedOrder" }

// Receipt is the answer of GetPlacedOrder.
type Receipt struct {
	OrderID string `json:"orderId"`
	SKU     string `json:"sku"`
	Cents   int64  `json:"cents"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// SNODRA_SERVICE_NAME, SNODRA_SERVER_ADDRESS and the other SNODRA_* variables override the
	// settings given here.
	if err := run(ctx, snodra.FromEnv()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, opts ...snodra.Option) error {
	// The message store is the service's own database; the event store is where events are
	// published. Both in-memory ones are for tests and local runs: they are not durable.
	app, err := snodra.New(append([]snodra.Option{
		snodra.Service("orders-service"),
		snodra.WithStore(snodra.NewMemoryMessageStore()),
		snodra.WithEventStore(snodra.NewMemoryEventStore()),
	}, opts...)...)
	if err != nil {
		return err
	}

	// The event handler's projection. A real service writes it to its own database with the
	// store's transaction helper, so the projection and the processed-event marker commit
	// together.
	var mu sync.Mutex
	placed := map[string]Receipt{}

	err = snodra.HandleCommand(app, func(ctx context.Context, cmd PlaceOrder) (snodra.Empty, error) {
		if cmd.OrderID == "" {
			return snodra.Empty{}, snodra.InvalidRequest("orderId is required")
		}
		// The event is appended to the outbox in the command's transaction: it is published only
		// if this handler succeeds. A command that writes to a database first writes both in one
		// store transaction, so neither the data nor the event is lost.
		return snodra.Empty{}, snodra.Publish(ctx, app, OrderPlaced(cmd), snodra.Key(cmd.OrderID))
	})
	if err != nil {
		return err
	}

	// The processor reads <service>.default: every event instance of the service competes for
	// its segments.
	err = snodra.HandleEvent(app, func(ctx context.Context, event OrderPlaced) error {
		mu.Lock()
		defer mu.Unlock()
		placed[event.OrderID] = Receipt(event)
		return nil
	})
	if err != nil {
		return err
	}

	err = snodra.HandleQuery(app, func(ctx context.Context, q GetPlacedOrder) (Receipt, error) {
		mu.Lock()
		defer mu.Unlock()
		receipt, ok := placed[q.OrderID]
		if !ok {
			return Receipt{}, snodra.InvalidRequest("the order is not placed yet")
		}
		return receipt, nil
	})
	if err != nil {
		return err
	}

	// Run starts the hub connections, the relay and the processors, serves until ctx ends, then
	// shuts down in order.
	return app.Run(ctx)
}
