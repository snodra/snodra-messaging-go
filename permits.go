package snodra

import (
	"sync"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// permitLedger is the permit accounting of one stream
// (https://snodra.damoreira.com/library-authors/protocol/, Flow control). The hub holds
// offered - received + returned of the stream's permits; a finished request's grant goes back
// at once when that is at or below min(permitBatch, initial/2) (with one permit: every
// completion) or when permitBatch grants are pending, else when the timer fires. The mark is the
// Register's initial_permits alone: a top-up does not move it (SG-4).
type permitLedger struct {
	initial  uint32 // Register.initial_permits
	offered  uint32 // Register.initial_permits plus the top-ups (several hubs)
	received uint32 // requests that arrived on the stream
	returned uint32 // grants written in Permits messages
	pending  uint32 // finished requests whose grant is not written yet
	armed    bool   // the flush timer is armed
}

// arrived counts a request the hub forwarded on the stream (it spent one of the permits).
func (l *permitLedger) arrived() { l.received++ }

// raise tops the stream up to share permits in all when it offered fewer, and returns the
// permits to add (the Permits count); it never lowers them.
func (l *permitLedger) raise(share uint32) uint32 {
	if share <= l.offered {
		return 0
	}
	n := share - l.offered
	l.offered = share
	return n
}

// finished counts a finished request: flushNow grants to write at once (0: none), armTimer
// when the flush timer must be started.
func (l *permitLedger) finished() (flushNow uint32, armTimer bool) {
	l.pending++
	// The counters may wrap; their difference, the permits the hub spent and did not get
	// back, is small, so it is read as a signed 32-bit number.
	held := int64(l.offered) - int64(int32(l.received-l.returned))
	// TODO(spec-gap): SG-4 (the mark of a stream that was topped up is its Register's).
	lowWater := int64(min(permitBatch, l.initial/2))
	switch {
	case held <= lowWater || l.pending >= permitBatch:
		return l.take(), false
	case !l.armed:
		l.armed = true
		return 0, true
	}
	return 0, false
}

// timerFired takes every pending grant once the flush timer fired.
func (l *permitLedger) timerFired() uint32 {
	l.armed = false
	return l.take()
}

func (l *permitLedger) take() uint32 {
	n := l.pending
	l.pending = 0
	l.returned += n
	return n
}

// streamPermits are the permits of one stream: its ledger, and the Permits messages written to
// that stream only (never to a later one: the hub resets a lost session's permits).
type streamPermits struct {
	ref        *streamRef
	flushAfter time.Duration

	mu     sync.Mutex
	ledger permitLedger
}

func newStreamPermits(ref *streamRef, offered uint32, flushAfter time.Duration) *streamPermits {
	return &streamPermits{ref: ref, flushAfter: flushAfter, ledger: permitLedger{initial: offered, offered: offered}}
}

// arrived counts a forwarded request; called from the stream's read loop, before the request
// is served.
func (p *streamPermits) arrived() {
	p.mu.Lock()
	p.ledger.arrived()
	p.mu.Unlock()
}

// busy is true while a request that arrived on the stream has not finished.
func (p *streamPermits) busy() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ledger.received-p.ledger.returned-p.ledger.pending != 0
}

// finished gives the permit of a finished request back, at once or batched.
func (p *streamPermits) finished() {
	p.mu.Lock()
	n, arm := p.ledger.finished()
	p.mu.Unlock()
	if arm {
		time.AfterFunc(p.flushAfter, p.flush)
	}
	p.write(n)
}

func (p *streamPermits) flush() {
	p.mu.Lock()
	n := p.ledger.timerFired()
	p.mu.Unlock()
	p.write(n)
}

// offeredNow is what the stream offered the hub so far: its Register's permits and the top-ups.
func (p *streamPermits) offeredNow() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ledger.offered
}

// topUp raises the stream's permits to share in all, when it offered fewer, with one
// Permits{count = share - offered, grant = share}: the grant raises the hub's cap on the session
// before the count is added, so the stream keeps them. Best effort, as every permit write.
func (p *streamPermits) topUp(share uint32) (count uint32) {
	p.mu.Lock()
	count = p.ledger.raise(share)
	p.mu.Unlock()
	if count > 0 {
		_ = p.ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Permits{Permits: &hubpb.Permits{Count: count, Grant: share}}})
	}
	return count
}

// write sends Permits{count n}; best effort: a stream that is gone takes its permits with it.
func (p *streamPermits) write(n uint32) {
	if n == 0 {
		return
	}
	_ = p.ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Permits{Permits: &hubpb.Permits{Count: n}}})
}
