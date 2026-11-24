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

// PlaceOrder is a command: the orders-service owns it.
type PlaceOrder struct {
	OrderID string `json:"orderId"`
	Cents   int64  `json:"cents"`
}

func (PlaceOrder) MessageType() string { return "orders.PlaceOrder" }

// Receipt answers PlaceOrder.
type Receipt struct {
	OrderID string `json:"orderId"`
	Total   int64  `json:"total"`
}

// GetOrder is a query.
type GetOrder struct {
	OrderID string `json:"orderId"`
}

func (GetOrder) MessageType() string { return "orders.GetOrder" }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// SNODRA_SERVICE_NAME, SNODRA_SERVER_ADDRESS and the other SNODRA_* variables override
	// the name given here.
	if err := run(ctx, snodra.FromEnv()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, opts ...snodra.Option) error {
	app, err := snodra.New(append([]snodra.Option{snodra.Service("orders-service")}, opts...)...)
	if err != nil {
		return err
	}

	var mu sync.Mutex
	orders := map[string]Receipt{}

	err = snodra.HandleCommand(app, func(ctx context.Context, cmd PlaceOrder) (Receipt, error) {
		if cmd.OrderID == "" {
			return Receipt{}, snodra.InvalidRequest("orderId is required")
		}
		mu.Lock()
		defer mu.Unlock()
		orders[cmd.OrderID] = Receipt{OrderID: cmd.OrderID, Total: cmd.Cents}
		return orders[cmd.OrderID], nil
	})
	if err != nil {
		return err
	}
	err = snodra.HandleQuery(app, func(ctx context.Context, q GetOrder) (Receipt, error) {
		mu.Lock()
		defer mu.Unlock()
		order, ok := orders[q.OrderID]
		if !ok {
			return Receipt{}, snodra.InvalidRequest("no such order")
		}
		return order, nil
	})
	if err != nil {
		return err
	}

	// Run connects to every hub, serves until ctx ends, then shuts down in order.
	return app.Run(ctx)
}
