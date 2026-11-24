package observability_test

import (
	snodra "github.com/snodra/snodra-messaging-go"
	"github.com/snodra/snodra-messaging-go/observability"
)

// The documentation's snippet: compiled here so it cannot rot (see example_test.go in the root module).
func Example() {
	// snippet:begin observability
	app, err := snodra.New(
		snodra.Service("orders-service"),
		snodra.Hubs("snodra-server:7000"),
		snodra.Telemetry(observability.New()), // uses the global OpenTelemetry providers
	)
	// snippet:end
	_, _ = app, err
}
