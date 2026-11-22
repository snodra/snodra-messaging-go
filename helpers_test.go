package snodramessaging

import "testing"

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// PlaceOrder is a message with a value receiver.
type PlaceOrder struct {
	SKU string `json:"sku"`
}

func (PlaceOrder) MessageType() string { return "orders.PlaceOrder" }

// CancelOrder is a message with a pointer receiver.
type CancelOrder struct {
	ID string `json:"id"`
}

func (*CancelOrder) MessageType() string { return "orders.CancelOrder" }
