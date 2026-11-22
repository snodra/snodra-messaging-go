package snodra

import (
	"context"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
)

// target is one entry of the hub addresses: a hub at host:port, or a DNS name whose every
// address (on its port) is a hub.
type target struct {
	address string // host:port; for a DNS target the name and the port
	dns     bool   // a dns:///name:port target, resolved again every dnsRefresh
}

// String is the target as configured: host:port, or dns:///name:port.
func (t target) String() string {
	if t.dns {
		return dnsScheme + t.address
	}
	return t.address
}

// hostPort splits the target's address.
func (t target) hostPort() (host, port string) {
	i := strings.LastIndexByte(t.address, ':')
	return t.address[:i], t.address[i+1:]
}

const dnsScheme = "dns:///"

// staticPrefixes may precede a static host:port; they are not part of the address.
var staticPrefixes = []string{"static://", "http://", "https://"}

// parseTargets reads the hub addresses ([Hubs], SNODRA_SERVER_ADDRESS): a list of host:port
// (a static://, http:// or https:// prefix optional), in order, repeats of one host:port
// removed; or exactly one dns:///name:port. It fails when they name no hub, when a DNS target is
// not alone, or when an entry lacks a host or a port.
func parseTargets(addresses []string) ([]target, error) {
	var entries []string
	for _, a := range addresses {
		if e := strings.TrimSpace(a); e != "" {
			entries = append(entries, e)
		}
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("snodra: the hub addresses (Hubs, SNODRA_SERVER_ADDRESS) name no hub")
	}
	if slices.ContainsFunc(entries, func(e string) bool { return strings.HasPrefix(e, "dns:") }) {
		if len(entries) > 1 {
			return nil, fmt.Errorf("snodra: hub addresses %q (Hubs, SNODRA_SERVER_ADDRESS): a dns:/// target must be the only one", entries)
		}
		t, err := parseDNSTarget(entries[0])
		if err != nil {
			return nil, err
		}
		return []target{t}, nil
	}
	var out []target
	for _, e := range entries {
		hostport := e
		for _, p := range staticPrefixes {
			if rest, ok := strings.CutPrefix(e, p); ok {
				hostport = rest
				break
			}
		}
		host, port, err := net.SplitHostPort(hostport)
		if err != nil || host == "" || port == "" {
			return nil, fmt.Errorf("snodra: hub address %q (Hubs, SNODRA_SERVER_ADDRESS) is not host:port", e)
		}
		t := target{address: hostport}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

// parseDNSTarget reads dns:///name:port, with the reference library's rules: no DNS server
// authority, a name without a path, a port from 1 to 65535.
func parseDNSTarget(entry string) (target, error) {
	bad := func(why string) error {
		return fmt.Errorf("snodra: hub address %q (Hubs, SNODRA_SERVER_ADDRESS): %s", entry, why)
	}
	name, ok := strings.CutPrefix(entry, dnsScheme)
	if !ok {
		return target{}, bad("only dns:///name:port is supported (no DNS server authority)")
	}
	i := strings.LastIndexByte(name, ':')
	if i < 0 {
		return target{}, bad("dns:///name:port needs a port")
	}
	host, port := name[:i], name[i+1:]
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return target{}, bad("dns:///name:port needs a port from 1 to 65535")
	}
	switch {
	case host == "":
		return target{}, bad("dns:///name:port needs a name")
	case strings.Contains(host, "/"):
		return target{}, bad("dns:///name:port takes a host name, not a path")
	}
	return target{address: name, dns: true}, nil
}

// hubIDOf is the id of a hub: its RegisterAck.hub_id, or, when that is empty (a real hub always
// names itself), its target as host:port without a static://, http:// or https:// prefix.
// TODO(spec-gap): SG-14 (http:// and https:// are stripped too, as in the reference library).
func hubIDOf(ackID, target string) string {
	if ackID != "" {
		return ackID
	}
	for _, p := range staticPrefixes {
		if rest, ok := strings.CutPrefix(target, p); ok {
			return rest
		}
	}
	return target
}

// oneFamily is the targets of one DNS answer on port: its IPv4 addresses when it has any, else
// its IPv6 ones (as [ip]:port, without a zone), in order, without repeats. A dual-stack name
// lists every hub under both families; two streams of one instance to one hub would replace
// each other.
func oneFamily(ips []net.IPAddr, port string) []string {
	v4 := slices.ContainsFunc(ips, func(a net.IPAddr) bool { return a.IP.To4() != nil })
	var out []string
	for _, a := range ips {
		if (a.IP.To4() != nil) != v4 {
			continue
		}
		t := net.JoinHostPort(a.IP.String(), port)
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out
}

// shareOf is the permits one stream offers when n hubs share max: ceil(max / n), n 0 counting
// as 1, never 0.
func shareOf(total, n int) uint32 {
	n = max(n, 1)
	return uint32(min((total+n-1)/n, math.MaxUint32))
}

// resolver turns the name and port of a dns:/// target into hub targets (host:port).
type resolver func(ctx context.Context, host, port string) ([]string, error)

// lookupTargets is the resolver of the system: the name's addresses, of one family.
func lookupTargets(ctx context.Context, host, port string) ([]string, error) {
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	return oneFamily(ips, port), nil
}

// withResolver replaces the resolver of a dns:/// target (tests).
func withResolver(r resolver) Option {
	return func(c *config) error {
		c.resolve = r
		return nil
	}
}
