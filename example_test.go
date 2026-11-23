package snodra_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The examples below are the README's snippets: they are compiled here (and only compiled: they
// have no Output) so they cannot rot, and readme_test.go checks that the README shows exactly
// the lines between the readme markers.

type PlaceOrder struct {
	OrderID string `json:"orderId"`
	Cents   int64  `json:"cents"`
}

func (PlaceOrder) MessageType() string { return "orders.PlaceOrder" }

type Receipt struct {
	OrderID string `json:"orderId"`
	Total   int64  `json:"total"`
}

type GetOrder struct {
	OrderID string `json:"orderId"`
}

func (GetOrder) MessageType() string { return "orders.GetOrder" }

type ListOrders struct{}

func (ListOrders) MessageType() string { return "orders.ListOrders" }

type WatchOrder struct {
	OrderID string `json:"orderId"`
}

func (WatchOrder) MessageType() string { return "orders.WatchOrder" }

type OrderStatus struct {
	Status string `json:"status"`
}

type ArchiveOrder struct {
	OrderID string `json:"orderId"`
}

func (ArchiveOrder) MessageType() string { return "orders.ArchiveOrder" }

func Example_send() {
	var (
		ctx context.Context
		app *snodra.App
	)
	// readme:begin send
	receipt, err := snodra.Send[Receipt](ctx, app, PlaceOrder{OrderID: "o-1", Cents: 1250},
		snodra.RoutingKey("o-1"), snodra.Timeout(3*time.Second))
	if err != nil {
		var e *snodra.Error
		switch {
		case errors.As(err, &e):
			fmt.Println("the hub or the handler answered", e.Code, e.Message)
		case errors.Is(err, snodra.ErrNeverSent):
			fmt.Println("no handler saw it: sending it again is safe")
		case errors.Is(err, snodra.ErrOutcomeUnknown):
			fmt.Println("it may or may not have run")
		}
		return
	}
	order, err := snodra.Query[Receipt](ctx, app, GetOrder{OrderID: receipt.OrderID})
	// readme:end
	_, _ = order, err
}

func Example_streams() {
	var (
		ctx context.Context
		app *snodra.App
	)
	// readme:begin streams
	// Serving a stream: the items go out under the caller's credit.
	_ = snodra.HandleStream(app, func(ctx context.Context, _ ListOrders, w *snodra.StreamWriter[Receipt]) error {
		for _, r := range []Receipt{{OrderID: "o-1", Total: 1250}, {OrderID: "o-2", Total: 80}} {
			if err := w.Send(ctx, r); err != nil {
				return err // the caller stopped reading, or the hub stream is gone
			}
		}
		return nil
	})

	// Reading one: breaking out of the loop cancels the stream at the hub.
	for r, err := range snodra.Stream[Receipt](ctx, app, ListOrders{}) {
		if err != nil {
			return // the last thing the sequence yields
		}
		fmt.Println(r.OrderID, r.Total)
	}

	// Asking every instance of every owner of a shared query type.
	gathered, err := snodra.Gather[Receipt](ctx, app, GetOrder{OrderID: "o-1"}, snodra.Timeout(2*time.Second))
	if err == nil {
		fmt.Println(len(gathered.Parts), "answers from", gathered.Targets, "targets")
	}
	// readme:end
}

func Example_subscriptions() {
	var (
		ctx context.Context
		app *snodra.App
	)
	// readme:begin subscriptions
	// Serving a subscription: the initial result now, updates whenever you emit them. The type
	// arguments are the query, its initial result and its updates, as Subscribe[I, U] reads them.
	updates, _ := snodra.HandleSubscription[WatchOrder, OrderStatus, OrderStatus](app, func(_ context.Context, q WatchOrder) (OrderStatus, error) {
		return OrderStatus{Status: "placed"}, nil
	})
	updates.Emit(func(q WatchOrder) bool { return q.OrderID == "o-1" }, OrderStatus{Status: "shipped"})

	// Following one.
	sub, err := snodra.Subscribe[OrderStatus, OrderStatus](ctx, app, WatchOrder{OrderID: "o-1"})
	if err != nil {
		return
	}
	defer sub.Close()
	fmt.Println(sub.Initial.Status)
	for u, err := range sub.Updates() {
		if err != nil {
			return
		}
		fmt.Println(u.Status)
	}
	// readme:end
}

func Example_guards() {
	var app *snodra.App
	// readme:begin guards
	// An anonymous caller is refused before the payload is decoded; a guard needs snodra.JWKS.
	_ = snodra.HandleCommand(app, func(ctx context.Context, cmd ArchiveOrder) (snodra.Empty, error) {
		caller, _ := snodra.CallerFrom(ctx)
		fmt.Println("archived by", caller.Subject)
		return snodra.Empty{}, nil
	}, snodra.RequireRole("orders-admin"))
	// readme:end
}

func Example_credentials() {
	// readme:begin credentials
	app, err := snodra.New(
		snodra.Service("orders-service"),
		snodra.Hubs("https://hub.example.test:7000"), // https:// implies TLS
		snodra.JWT(snodra.NewClientCredentials(snodra.ClientCredentials{
			TokenURL:         "https://idp.example.test/oauth2/token",
			ClientSecretFile: "/run/secrets/orders-service-secret",
		})),
	)
	// readme:end
	_, _ = app, err
}
