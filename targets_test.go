package snodra

import (
	"net"
	"strings"
	"testing"
	"time"
)

// ips builds the answer of a DNS lookup.
func ips(list ...string) []net.IPAddr {
	out := make([]net.IPAddr, len(list))
	for i, s := range list {
		out[i] = net.IPAddr{IP: net.ParseIP(s)}
	}
	return out
}

func TestShareOf(t *testing.T) {
	is(t, shareOf(5, 2), uint32(3))
	is(t, shareOf(5, 1), uint32(5))
	is(t, shareOf(4, 3), uint32(2))
	is(t, shareOf(1, 2), uint32(1)) // never 0: every hub may forward something
	is(t, shareOf(256, 3), uint32(86))
	is(t, shareOf(10, 0), uint32(10)) // no hub counts as one
}

func TestHubIDOf(t *testing.T) {
	is(t, hubIDOf("hub-1", "h:7000"), "hub-1")
	is(t, hubIDOf("", "static://h:7000"), "h:7000")
	is(t, hubIDOf("", "h:7000"), "h:7000")
	is(t, hubIDOf("", "http://h:7000"), "h:7000")
	is(t, hubIDOf("", "https://h:7000"), "h:7000") // SG-14
	is(t, hubIDOf("", "[::1]:7000"), "[::1]:7000")
	got := oneFamily(ips("10.0.0.1", "::1", "10.0.0.1"), "7000")
	is(t, len(got), 1)
	is(t, got[0], "10.0.0.1:7000")
	is(t, oneFamily(ips("::1"), "7000")[0], "[::1]:7000")
	v6 := oneFamily(ips("::1", "2001:db8::5", "::1"), "7000")
	is(t, strings.Join(v6, ","), "[::1]:7000,[2001:db8::5]:7000")
	v4 := oneFamily(ips("::1", "127.0.0.1", "fe80::2", "10.0.0.2", "127.0.0.1"), "7000")
	is(t, strings.Join(v4, ","), "127.0.0.1:7000,10.0.0.2:7000")
	is(t, len(oneFamily(nil, "7000")), 0)
	// A zone is not part of the target.
	zoned := oneFamily([]net.IPAddr{{IP: net.ParseIP("fe80::1"), Zone: "en0"}}, "7000")
	is(t, zoned[0], "[fe80::1]:7000")
}

func TestParseTargetsTakesAListInOrderWithoutRepeats(t *testing.T) {
	got, err := parseTargets([]string{"static://hub-1:7000", " hub-2:7000 ", "hub-1:7000", "http://hub-1:7000", "https://hub-3:7100"})
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(got), 3)
	is(t, got[0], target{address: "hub-1:7000"})
	is(t, got[1], target{address: "hub-2:7000"})
	is(t, got[2], target{address: "hub-3:7100", https: true})
}

func TestParseTargetsTakesOneDNSName(t *testing.T) {
	got, err := parseTargets([]string{" dns:///hubs.example:7100 "})
	if err != nil {
		t.Fatal(err)
	}
	is(t, len(got), 1)
	is(t, got[0], target{address: "hubs.example:7100", dns: true})
	is(t, got[0].String(), "dns:///hubs.example:7100")
	host, port := got[0].hostPort()
	is(t, host, "hubs.example")
	is(t, port, "7100")
}

func TestParseTargetsRefusesMalformedAddresses(t *testing.T) {
	for _, bad := range [][]string{
		nil,
		{"", " "},
		{"hub-1"},                            // no port
		{":7000"},                            // no host
		{"dns:///snodra-server"},             // no port
		{"dns:///snodra-server:0"},           // port 0
		{"dns:///snodra-server:x"},           // not a port
		{"dns:///snodra-server:70000"},       // out of range
		{"dns:///:7000"},                     // no host
		{"dns:///snodra/hub:7000"},           // a path, not a host
		{"dns://8.8.8.8/snodra-server:7000"}, // an authority (DNS server) is not supported
		{"dns:///a:7000", "dns:///b:7000"},   // dns only alone
		{"hub-1:7000", "dns:///b:7000"},
	} {
		if got, err := parseTargets(bad); err == nil {
			t.Errorf("%q accepted: %v", bad, got)
		}
	}
}

func TestHubsAndTheEnvironmentTakeADNSName(t *testing.T) {
	app, err := New(Service("orders-service"), Hubs("dns:///hubs.example:7000"))
	if err != nil {
		t.Fatal(err)
	}
	is(t, strings.Join(app.cfg.hubs, ","), "dns:///hubs.example:7000")
	var c config
	c = defaultConfig()
	env := map[string]string{"SNODRA_SERVER_ADDRESS": "dns:///hubs.example:7100", "SNODRA_SERVER_STATUS_INTERVAL_MS": "2500"}
	if err := c.applyEnv(func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
	is(t, strings.Join(c.hubs, ","), "dns:///hubs.example:7100")
	is(t, c.timings.statusInterval, 2500*ms)
	for _, bad := range []string{"0", "-1", "x"} {
		c = defaultConfig()
		env["SNODRA_SERVER_STATUS_INTERVAL_MS"] = bad
		if err := c.applyEnv(func(k string) string { return env[k] }); err == nil {
			t.Errorf("SNODRA_SERVER_STATUS_INTERVAL_MS=%s accepted", bad)
		}
	}
	if _, err := New(Service("orders-service"), Hubs("h:7000"), StatusInterval(0)); err == nil {
		t.Error("StatusInterval(0) accepted")
	}
	app, err = New(Service("orders-service"), Hubs("h:7000"), StatusInterval(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	is(t, app.cfg.timings.statusInterval, time.Second)
}

func TestDefaultTimingsOfSeveralHubs(t *testing.T) {
	d := defaultTimings()
	is(t, d.statusInterval, 5*time.Second)
	is(t, d.dnsRefresh, 30*time.Second)
	is(t, maxUnwrittenMoves, 8)
	is(t, dnsDrainLimit, 65*time.Second)
	is(t, dnsDrainPoll, 100*time.Millisecond)
}
