package snodramessaging

import "testing"

func TestDefaultTimings(t *testing.T) {
	d := defaultTimings()
	is(t, d.heartbeatBeforeAck, 3*s)
	is(t, d.handshakeTimeout, 10*s)
	is(t, d.silentIntervals, 3)
	is(t, d.keepaliveTime, 10*s)
	is(t, d.keepaliveTimeout, 5*s)
	is(t, d.backoffMin, 500*ms)
	is(t, d.backoffMax, 5*s)
	is(t, d.deregisterWait, 1*s)
	is(t, d.handlerDrain, 10*s)
	is(t, d.subscriptionEnd, 2*s)
	is(t, d.closeWait, 2*s)
	is(t, d.permitFlushAfter, 50*ms)
	is(t, permitBatch, uint32(16))
	is(t, guardMargin, 1*s)
	is(t, fallbackMaxTimeout, 60*s)
	is(t, defaultBudget, 5*s)
	is(t, retryMinRemaining, 250*ms)
	is(t, outboundLimit, 256)
}
