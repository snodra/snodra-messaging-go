package otel_test

import (
	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/otel"
)

// The README's snippet: compiled here so it cannot rot (see readme_test.go in the root module).
func Example() {
	// readme:begin otel
	app, err := snodra.New(
		snodra.Service("orders-service"),
		snodra.Hubs("snodra-server:7000"),
		snodra.Telemetry(otel.New()), // uses the global OpenTelemetry providers
	)
	// readme:end
	_, _ = app, err
}
