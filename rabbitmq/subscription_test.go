package rabbitmq

import "testing"

func TestShouldStoreOffset(t *testing.T) {
	for _, c := range []struct {
		has    bool
		last   uint64
		offset uint64
		want   bool
	}{
		{false, 0, 0, true},
		{false, 0, 7, true},
		{true, 7, 7, false},
		{true, 7, 6, false},
		{true, 7, 8, true},
	} {
		if got := shouldStoreOffset(c.has, c.last, c.offset); got != c.want {
			t.Fatalf("shouldStoreOffset(has %t, last %d, offset %d): got %t, want %t", c.has, c.last, c.offset, got, c.want)
		}
	}
}
