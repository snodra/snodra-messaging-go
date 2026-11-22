package snodramessaging

import (
	"fmt"
	"sync/atomic"
)

// pool is the connections of an App: one per configured hub, each sending the identical
// Register. Routing, the permit split, dedupe by hub id and failover come with several hubs
// (https://snodra.damoreira.com/library-authors/protocol/, Several hubs).
type pool struct {
	conns []*hubConn
	next  atomic.Uint64 // the round-robin position
}

func newPool(a *App) *pool {
	p := &pool{}
	for _, address := range a.cfg.hubs {
		p.conns = append(p.conns, newHubConn(a, address))
	}
	return p
}

// start runs every connection's goroutine.
func (p *pool) start() {
	for _, c := range p.conns {
		go c.run()
	}
}

// pick returns an acknowledged connection, round-robin; with none, an error wrapping
// ErrNeverSent. The routing key is not used yet.
func (p *pool) pick(key string) (*hubConn, error) {
	_ = key
	var live []*hubConn
	for _, c := range p.conns {
		if c.ack() != nil {
			live = append(live, c)
		}
	}
	if len(live) == 0 {
		return nil, fmt.Errorf("%w: no hub has acknowledged the registration", ErrNeverSent)
	}
	return live[(p.next.Add(1)-1)%uint64(len(live))], nil
}

// deregistered are the channels closed once each connection's empty Register is acked or
// needs no wait.
func (p *pool) deregistered() []<-chan struct{} {
	out := make([]<-chan struct{}, len(p.conns))
	for i, c := range p.conns {
		out[i] = c.deregDone
	}
	return out
}

// dones are the channels closed once each connection's goroutine has returned.
func (p *pool) dones() []<-chan struct{} {
	out := make([]<-chan struct{}, len(p.conns))
	for i, c := range p.conns {
		out[i] = c.done
	}
	return out
}
