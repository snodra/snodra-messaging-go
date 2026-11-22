package snodramessaging

import "testing"

func TestTypeNamesOfValueAndPointerMessages(t *testing.T) {
	is(t, typeName[PlaceOrder](), "orders.PlaceOrder")
	is(t, typeName[*CancelOrder](), "orders.CancelOrder")
	is(t, newValue[*CancelOrder]() != nil, true)
}

func TestEmptyEncodesAsAnEmptyObject(t *testing.T) {
	b, _ := JSON.Marshal(Empty{})
	is(t, string(b), "{}")
}

func TestJSONIgnoresUnknownFieldsOnRead(t *testing.T) {
	var p PlaceOrder
	if err := JSON.Unmarshal([]byte(`{"sku":"a","extra":1}`), &p); err != nil {
		t.Fatal(err)
	}
	is(t, p.SKU, "a")
}
