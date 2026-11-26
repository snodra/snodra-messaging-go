package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
)

func TestParseConsumers(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		want  []consumer
		fails bool
	}{
		{
			name: "a named member",
			body: `[{"queue":{"name":"snodra.events-0"},"properties":{"name":"orders-service.projector","snodra-member":"m-1"}}]`,
			want: []consumer{{name: "orders-service.projector", stream: "snodra.events-0", member: "m-1"}},
		},
		{
			name: "a consumer without snodra-member falls back to its connection and subscription",
			body: `[{"queue":{"name":"snodra.events-1"},"properties":{"name":"orders-service.projector"},"connection_details":{"name":"10.0.0.1:5000 -> 10.0.0.2:5552"},"subscription_id":7}]`,
			want: []consumer{{name: "orders-service.projector", stream: "snodra.events-1", member: "10.0.0.1:5000 -> 10.0.0.2:5552:7"}},
		},
		{
			name: "a paged answer is read from items",
			body: `{"items":[{"queue":{"name":"snodra.events-2"},"properties":{"name":"orders-service.projector","snodra-member":"m-2"}}]}`,
			want: []consumer{{name: "orders-service.projector", stream: "snodra.events-2", member: "m-2"}},
		},
		{
			name: "an unnamed reader is not a subscription's member",
			body: `[{"queue":{"name":"snodra.events-3"},"properties":{},"connection_details":{"name":"browser"},"subscription_id":9}]`,
			want: []consumer{{stream: "snodra.events-3", member: "browser:9"}},
		},
		{
			name:  "a consumer without a queue",
			body:  `[{"properties":{"name":"x"}}]`,
			fails: true,
		},
		{
			name:  "a consumer without queue.name",
			body:  `[{"queue":{},"properties":{"name":"x"}}]`,
			fails: true,
		},
		{
			name:  "a consumer without properties",
			body:  `[{"queue":{"name":"s"}}]`,
			fails: true,
		},
		{
			name:  "a non-string consumer name fails closed",
			body:  `[{"queue":{"name":"s"},"properties":{"name":7}}]`,
			fails: true,
		},
		{
			name:  "a non-string member fails closed",
			body:  `[{"queue":{"name":"s"},"properties":{"name":"x","snodra-member":7}}]`,
			fails: true,
		},
		{
			name:  "an answer that is neither an array nor items",
			body:  `{"consumers":[]}`,
			fails: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var body any
			if err := json.Unmarshal([]byte(c.body), &body); err != nil {
				t.Fatal(err)
			}
			got, err := parseConsumers(body)
			if c.fails {
				if err == nil {
					t.Fatalf("parseConsumers(%s): %v, want an error", c.body, got)
				}
				if !errors.Is(err, snodra.ErrFailed) {
					t.Fatalf("parseConsumers(%s): %v, want ErrFailed", c.body, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseConsumers(%s): %v", c.body, err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("parseConsumers(%s): %+v, want %+v", c.body, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("parseConsumers(%s): %+v, want %+v", c.body, got, c.want)
				}
			}
		})
	}
}

func overviewServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/overview" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestStatsEnabled(t *testing.T) {
	for _, c := range []struct {
		name  string
		body  string
		want  bool
		fails bool
	}{
		{"metrics are on", `{"disable_stats":false}`, true, false},
		{"metrics are off", `{"disable_stats":true}`, false, false},
		{"an older answer without the field", `{}`, true, false},
		{"a non-boolean field fails", `{"disable_stats":"no"}`, false, true},
		{"a bad answer fails", `{"disable_stats":`, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := overviewServer(t, c.body)
			managementAPI, err := newManagement(Config{ManagementURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			got, err := managementAPI.statsEnabled(context.Background())
			if c.fails {
				if err == nil || !errors.Is(err, snodra.ErrFailed) {
					t.Fatalf("statsEnabled: got %t %v, want ErrFailed", got, err)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("statsEnabled: got %t %v, want %t", got, err, c.want)
			}
		})
	}
}

func TestOpenDropsReplayWhenMetricsAreOff(t *testing.T) {
	off := overviewServer(t, `{"disable_stats":true}`)
	store, err := Open(context.Background(), Config{ManagementURL: off.URL})
	if err != nil {
		t.Fatalf("Open with metrics off: %v", err)
	}
	if store.management != nil || snodra.Supports(store, snodra.CapabilityReplay) {
		t.Fatal("a store with metrics off declares replay")
	}

	on := overviewServer(t, `{"disable_stats":false}`)
	store, err = Open(context.Background(), Config{ManagementURL: on.URL})
	if err != nil {
		t.Fatalf("Open with metrics on: %v", err)
	}
	if store.management == nil || !snodra.Supports(store, snodra.CapabilityReplay) {
		t.Fatal("a store with metrics on does not declare replay")
	}
}

func TestManagementMembersFilterByNameAndStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/overview" {
			_, _ = w.Write([]byte(`{"disable_stats":false}`))
			return
		}
		_, _ = w.Write([]byte(`[
			{"queue":{"name":"snodra.events-0"},"properties":{"name":"orders-service.p","snodra-member":"m-2"}},
			{"queue":{"name":"snodra.events-0"},"properties":{"name":"orders-service.p","snodra-member":"m-1"}},
			{"queue":{"name":"snodra.events-0"},"properties":{"name":"orders-service.p","snodra-member":"m-1"}},
			{"queue":{"name":"snodra.events-1"},"properties":{"name":"orders-service.p","snodra-member":"m-3"}},
			{"queue":{"name":"snodra.events-9"},"properties":{"name":"orders-service.p","snodra-member":"m-4"}},
			{"queue":{"name":"snodra.events-0"},"properties":{"name":"other.p","snodra-member":"m-5"}}
		]`))
	}))
	defer server.Close()
	managementAPI, err := newManagement(Config{ManagementURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	members, err := managementAPI.members(context.Background(), "orders-service.p", []string{"snodra.events-0", "snodra.events-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 || members[0] != "m-1" || members[1] != "m-2" || members[2] != "m-3" {
		t.Fatalf("members: %v, want [m-1 m-2 m-3]", members)
	}
}
