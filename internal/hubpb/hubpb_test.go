package hubpb_test

import (
	"testing"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

func TestBindingsAreHubProto(t *testing.T) {
	fd := hubpb.File_hub_proto
	is(t, string(fd.Package()), "snodra.hub.v1")
	m := fd.Services().ByName("Hub").Methods()
	is(t, m.ByName("Connect").IsStreamingClient() && m.ByName("Connect").IsStreamingServer(), true)
	is(t, m.ByName("Topology") != nil, true)
}
