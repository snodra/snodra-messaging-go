package snodramessaging

import "testing"

func TestClientVersion(t *testing.T) { is(t, clientVersion(), "snodra-messaging-go/0.1.0") }
