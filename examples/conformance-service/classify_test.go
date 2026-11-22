package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
)

func TestClassifyNamesTheContractsKinds(t *testing.T) {
	kind := func(err error) string { return classify(err)["kind"].(string) }
	wrap := func(err error) error { return fmt.Errorf("place order: %w", err) }
	for _, c := range []struct {
		err  error
		want string
	}{
		{wrap(snodra.ErrNeverSent), "never-sent"},
		{wrap(snodra.ErrOutcomeUnknown), "outcome-unknown"},
		{fmt.Errorf("%w: %w", snodra.ErrOutcomeUnknown, context.Canceled), "outcome-unknown"},
		{wrap(snodra.ErrLostParts), "lost-parts"},
		{wrap(snodra.ErrReservedHeader), "refused"},
		{wrap(&snodra.Error{Code: snodra.CodeTimeout, Message: "slow"}), "code"},
		{errors.New("cannot decode"), "local"},
	} {
		if got := kind(c.err); got != c.want {
			t.Errorf("%v: %s, want %s", c.err, got, c.want)
		}
	}
	e := classify(&snodra.Error{Code: snodra.CodeHandlerError, Message: "insufficient stock", Details: json.RawMessage(`{"sku":"a"}`)})
	if e["code"] != "HANDLER_ERROR" || e["message"] != "insufficient stock" || e["details"] != `{"sku":"a"}` {
		t.Fatalf("got %v", e)
	}
	e = classify(&snodra.Error{Code: snodra.CodeNoHandler, Message: "nobody"})
	if e["code"] != "NO_HANDLER" || e["details"] != nil {
		t.Fatalf("got %v", e)
	}
	if e := classify(snodra.ErrNeverSent); e["code"] != nil || e["message"] != snodra.ErrNeverSent.Error() {
		t.Fatalf("got %v", e)
	}
}

func TestEnvMapsTheRetryPolicy(t *testing.T) {
	base := map[string]string{
		"CONFORMANCE_SERVICE_NAME": "conformance-go",
		"CONFORMANCE_INSTANCE_ID":  "i-7",
		"CONFORMANCE_HUBS":         "127.0.0.1:7000",
	}
	e, err := readEnv(lookup(base))
	if err != nil || e.retry != nil {
		t.Fatalf("no policy without CONFORMANCE_RETRY_MAX_ATTEMPTS: %+v %v", e, err)
	}
	base["CONFORMANCE_RETRY_MAX_ATTEMPTS"] = "3"
	e, err = readEnv(lookup(base))
	if err != nil || e.retry == nil || *e.retry != (snodra.RetryPolicy{MaxAttempts: 3, InitialBackoff: 100e6, Multiplier: 2, MaxBackoff: 2e9}) {
		t.Fatalf("got %+v %v", e.retry, err)
	}
	base["CONFORMANCE_RETRY_INITIAL_BACKOFF_MS"] = "250"
	e, err = readEnv(lookup(base))
	if err != nil || e.retry.InitialBackoff != 250e6 {
		t.Fatalf("got %+v %v", e.retry, err)
	}
	for name, bad := range map[string]string{"CONFORMANCE_RETRY_MAX_ATTEMPTS": "0", "CONFORMANCE_RETRY_INITIAL_BACKOFF_MS": "soon"} {
		vars := map[string]string{}
		for k, v := range base {
			vars[k] = v
		}
		vars[name] = bad
		if _, err := readEnv(lookup(vars)); err == nil {
			t.Errorf("%s=%s accepted", name, bad)
		}
	}
}
