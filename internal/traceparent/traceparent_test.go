package traceparent_test

import (
	"testing"

	"github.com/snodra/snodra-messaging-go/internal/traceparent"
)

func is[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestOnlyAW3CTraceparentOfVersion00IsValid(t *testing.T) {
	is(t, traceparent.Valid(tp), true)
	is(t, traceparent.Valid("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"), true) // not sampled
	for _, bad := range []string{
		"",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",  // another version
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",  // upper case
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",  // trace id all zeros
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",  // parent id all zeros
		"00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",   // trace id too short
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",   // flags too short
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-", // a fifth part
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902g7-01",  // not hex
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",     // no flags
		" 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // blank
	} {
		if traceparent.Valid(bad) {
			t.Fatalf("%q read as valid", bad)
		}
	}
}

func TestTraceIDIsTheSecondPartOfAValidOne(t *testing.T) {
	is(t, traceparent.TraceID(tp), "4bf92f3577b34da6a3ce929d0e0e4736")
	is(t, traceparent.TraceID("00-00000000000000000000000000000000-00f067aa0ba902b7-01"), "")
	is(t, traceparent.TraceID("garbage"), "")
}
