// Package traceparent reads W3C trace context headers. Only a valid traceparent is forwarded: see
// https://snodra.damoreira.com/docs/library-authors/messages/ (Trace context).
package traceparent

import "strings"

// Valid reports whether s is a W3C traceparent of version 00: "00-<32 hex>-<16 hex>-<2 hex>",
// lower case, neither the trace id nor the parent id all zeros.
func Valid(s string) bool {
	parts := strings.Split(s, "-")
	return len(parts) == 4 &&
		parts[0] == "00" &&
		lowerHex(parts[1], 32) && !zeros(parts[1]) &&
		lowerHex(parts[2], 16) && !zeros(parts[2]) &&
		lowerHex(parts[3], 2)
}

// TraceID is the trace id of the valid traceparent s; "" when s is not valid.
func TraceID(s string) string {
	if !Valid(s) {
		return ""
	}
	return s[3:35]
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func zeros(s string) bool { return strings.Trim(s, "0") == "" }
