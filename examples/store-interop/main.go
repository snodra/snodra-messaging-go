// Command store-interop is the probe service of the stores interoperability profile
// (snodra-conformance/interop): it runs one instance of the neutral orders-service shape per
// store family, uses the library's real outbox, relay, event consumer, event store and message
// store add-ons, and exposes the commands that profile's driver calls.
//
// The process starts one App per family: kafka and rabbitmq on the in-memory message store, and
// postgres, mysql and mongo on the matching database add-on (each with Kafka as its event store).
// Each store-interop process registers the same service name, the same processor per family and
// the same subscription, so the consumers of every language compete in one Kafka consumer group or
// under one RabbitMQ Streams single-active-consumer name.
//
// The commands are internal to the harness (the neutral orders-service naming is deliberate):
//
//	orders.interop.Who         which instance, language and family answered
//	orders.interop.Publish     append events to the outbox (the relay publishes them)
//	orders.interop.PublishRaw  publish events through the event store directly
//	orders.interop.State       the instance's handled and duplicate deliveries and store views
//
// The probe observes the library's processed-event markers by wrapping the message store's
// processed port: the consumer and its transaction still belong to the library, the wrapper only
// notes which marker the stage-1 record inserted and which it found. No library-internal hook is
// used.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// shutdownBudget bounds the shutdown of every app after SIGTERM or SIGINT.
const shutdownBudget = 10 * time.Second

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		fmt.Fprintln(os.Stderr, "store-interop:", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	settings, err := settingsFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	probes := make([]*probe, 0, len(settings.families))
	for _, family := range settings.families {
		p, err := newProbe(settings, family, logger)
		if err != nil {
			closeProbes(probes, logger)
			return err
		}
		probes = append(probes, p)
	}
	started := 0
	for _, p := range probes {
		if err := p.start(ctx); err != nil {
			shutDown(probes[:started], logger)
			return fmt.Errorf("the %s instance could not start: %w", p.family, err)
		}
		started++
	}
	logger.Info("the store interoperability probes are registered",
		"language", settings.language, "families", settings.families, "run", settings.run)
	<-ctx.Done()
	shutDown(probes, logger)
	return nil
}

// shutDown stops every app in reverse order, each before the next, and then the stores the probes
// own. The five apps fit the compose stop_grace_period (60 s) within their per-app budget.
func shutDown(probes []*probe, logger *slog.Logger) {
	for i := len(probes) - 1; i >= 0; i-- {
		p := probes[i]
		ctx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
		err := p.app.Shutdown(ctx)
		cancel()
		if err != nil {
			logger.Warn("an app did not shut down cleanly", "family", p.family, "error", err)
		}
		p.close(logger)
	}
}

// closeProbes closes what was built before an app started.
func closeProbes(probes []*probe, logger *slog.Logger) {
	for _, p := range probes {
		p.close(logger)
	}
}
