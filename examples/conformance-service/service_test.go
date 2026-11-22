//go:build unix

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/fakehub"
)

func TestStopsWithinFiveSecondsOfSIGTERM(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "conformance-service")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	hub := fakehub.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = append(os.Environ(),
		"CONFORMANCE_SERVICE_NAME=conformance-go",
		"CONFORMANCE_INSTANCE_ID=i-1",
		"CONFORMANCE_HUBS="+hub.Addr(),
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := hub.Next(t)
	r := s.RecvRegister(t)
	if r.GetServiceName() != "conformance-go" || r.GetInstanceId() != "i-1" || r.GetInitialPermits() != 64 {
		t.Fatalf("Register %v", r)
	}
	s.Ack(t, fakehub.Ack("hub-1"))
	time.Sleep(200 * time.Millisecond) // registered
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	signalled := time.Now()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("still running 5 s after SIGTERM")
	}
	t.Logf("stopped %v after SIGTERM", time.Since(signalled))
	if empty := s.RecvRegister(t); len(empty.GetCommandTypes())+len(empty.GetQueryTypes()) != 0 {
		t.Fatalf("the Register at shutdown names types: %v", empty)
	}
}
