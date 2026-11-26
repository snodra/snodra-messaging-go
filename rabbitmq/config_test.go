package rabbitmq

import (
	"errors"
	"testing"

	snodra "github.com/snodra/snodra-messaging-go"
)

func TestConfigDefaultsAndValidation(t *testing.T) {
	store, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if store.config.Hosts[0] != "rabbitmq" || store.config.Port != DefaultPort {
		t.Fatalf("the defaults: %+v", store.config)
	}
	if store.config.Username != "guest" || store.config.Password != "guest" || store.config.VirtualHost != "/" {
		t.Fatalf("the defaults: %+v", store.config)
	}
	if snodra.Supports(store, snodra.CapabilityReplay) {
		t.Fatal("no management API, no replay")
	}

	withManagement, err := New(Config{ManagementURL: "http://rabbitmq:15672"})
	if err != nil {
		t.Fatal(err)
	}
	if !snodra.Supports(withManagement, snodra.CapabilityReplay) {
		t.Fatal("a management API declares replay")
	}
	for _, capability := range []snodra.Capability{snodra.CapabilityLag, snodra.CapabilityNativeDeadLetter} {
		if !snodra.Supports(withManagement, capability) {
			t.Fatalf("%s is declared", capability)
		}
	}
	if snodra.Supports(withManagement, snodra.CapabilityBrowse) {
		t.Fatal("no event browser, no browse")
	}

	for _, invalid := range []Config{
		{Hosts: []string{" "}},
		{Hosts: []string{"a:b:c"}},
		{Port: 70000},
		{ManagementURL: "ftp://rabbitmq"},
		{ManagementURL: "http://user:pass@rabbitmq"},
		{ConfirmTimeout: -1},
		{RequestTimeout: -1},
		{OffsetStoreInterval: -1},
	} {
		if _, err := New(invalid); !errors.Is(err, snodra.ErrInvalid) {
			t.Fatalf("New(%+v): %v, want ErrInvalid", invalid, err)
		}
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("SNODRA_RABBITMQ_HOSTS", "a, b:5553")
	t.Setenv("SNODRA_RABBITMQ_PORT", "5554")
	t.Setenv("SNODRA_RABBITMQ_VIRTUAL_HOST", "/orders")
	t.Setenv("SNODRA_RABBITMQ_USERNAME", "svc")
	t.Setenv("SNODRA_RABBITMQ_PASSWORD", "secret")
	t.Setenv("SNODRA_RABBITMQ_MANAGEMENT_URL", "http://rabbitmq:15672")
	config := FromEnv()
	if got := config.Hosts; len(got) != 2 || got[0] != "a" || got[1] != "b:5553" {
		t.Fatalf("the hosts: %v", got)
	}
	if config.Port != 5554 || config.VirtualHost != "/orders" || config.Username != "svc" || config.Password != "secret" {
		t.Fatalf("the environment: %+v", config)
	}
	if config.ManagementURL != "http://rabbitmq:15672" {
		t.Fatalf("the management URL: %q", config.ManagementURL)
	}
}
