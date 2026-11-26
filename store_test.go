package snodra

import (
	"errors"
	"strings"
	"testing"
)

// jsonNested is a JSON object depth deep as MySQL's JSON_DEPTH counts: {"a":{"a":...{"a":1}...}},
// {"a":1} being depth 2.
func jsonNested(depth int) string {
	objects := depth - 1
	return strings.Repeat(`{"a":`, objects) + "1" + strings.Repeat("}", objects)
}

func TestCheckOutboxPayloadAcceptsTheContract(t *testing.T) {
	for _, payload := range []string{
		`{}`,
		`{"n":1}`,
		`{"s":"123456789012345678901234567890","d":1.5,"e":1e30,"max":9223372036854775807,"min":-9223372036854775808}`,
		`{"n":1e-400}`,
		`{"n":-0.5}`,
		`{"s":"é"}`,
		`{"s":"\ud83d\ude00"}`,
		`{"s":"\\ud800"}`,
		`{"s":"\ufeff"}`,
		`{"a":[{"b":true},{"c":null}]}`,
		jsonNested(MaxPayloadDepth),
	} {
		if err := CheckOutboxPayload("conformance.Event", []byte(payload)); err != nil {
			t.Errorf("%s: %v", payload, err)
		}
	}
}

func TestCheckOutboxPayloadRefusesTheContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
	}{
		{"not JSON", `not json`},
		{"an overflow", `{"n":1e400}`},
		{"a negative overflow", `{"n":-1e400}`},
		{"a lone high surrogate", `{"s":"\ud800"}`},
		{"a lone low surrogate", `{"\udc00":1}`},
		{"a byte order mark", "\ufeff{}"},
		{"invalid UTF-8", `{"s":"` + "\xff" + `"}`},
		{"one too deep", jsonNested(MaxPayloadDepth + 1)},
		{"an array", `[1,2]`},
		{"a string", `"text"`},
		{"null", `null`},
		{"a number", `1`},
		{"a boolean", `true`},
		{"above the unsigned range", `{"n":18446744073709551615}`},
		{"below the signed range", `{"n":-9223372036854775809}`},
		{"above 2^64", `{"n":18446744073709551616}`},
		{"a very long integer", `{"n":123456789012345678901234567890}`},
		{"a deep integer", `{"a":[1,{"b":[2,-99999999999999999999]}]}`},
		{"trailing text", `{"a":1}garbage`},
		{"two values", `{"a":1}{"b":2}`},
		{"an empty payload", ``},
	} {
		payload := test.payload
		t.Run(test.name, func(t *testing.T) {
			err := CheckOutboxPayload("conformance.Event", []byte(payload))
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
}

// A number the outbox contract refuses is reported as what it is: the payload is JSON, only its
// number is out of the contract, not "not JSON".
func TestCheckOutboxPayloadClassifiesANumberRefusal(t *testing.T) {
	for _, test := range []struct {
		payload string
		wants   []string
	}{
		{`{"n":9223372036854775808}`, []string{"holds the integer", "outside the signed 64-bit range"}},
		{`{"n":-9223372036854775809}`, []string{"holds the integer", "outside the signed 64-bit range"}},
		{`{"n":1e400}`, []string{"holds the number", "no double holds"}},
		{`{"n":-1e400}`, []string{"holds the number", "no double holds"}},
	} {
		err := CheckOutboxPayload("conformance.Event", []byte(test.payload))
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: got %v, want ErrInvalid", test.payload, err)
		}
		message := err.Error()
		if strings.Contains(message, "is not JSON") {
			t.Errorf("%s: a number refusal read as bad JSON: %s", test.payload, message)
		}
		for _, want := range test.wants {
			if !strings.Contains(message, want) {
				t.Errorf("%s: %q does not say %q", test.payload, message, want)
			}
		}
	}
}
