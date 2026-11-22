// Command conformance-service is the service the Snodra conformance runner drives to check
// this library (https://github.com/snodra/snodra-conformance/blob/main/docs/contract.md),
// written with the library's public API as an application would write it. It reads its
// settings from CONFORMANCE_* variables, registers on every hub and stops on SIGTERM or
// SIGINT, within the runner's 5 s.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// shutdownBudget keeps the shutdown inside the runner's 5 s after SIGTERM.
const shutdownBudget = 4 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "conformance-service:", err)
		os.Exit(1)
	}
}

func run() error {
	e, err := readEnv(os.Getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	app, err := snodra.New(append(e.options(), snodra.Logger(logger))...)
	if err != nil {
		return err
	}
	if err := registerHandlers(app, e); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := app.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	defer cancel()
	return app.Shutdown(sctx)
}
