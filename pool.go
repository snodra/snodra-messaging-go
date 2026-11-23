package snodra

import (
	"context"
	"fmt"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
	"github.com/snodra/snodra-messaging-go/internal/rendezvous"
)

// memberRole is what the pool makes of one connection.
type memberRole int

const (
	roleActive    memberRole = iota // routed to, counted in the permit split
	roleDuplicate                   // its hub is reached by another connection: closed and parked
	roleDraining                    // its address left the DNS answer: out of the routing, closed once idle
	roleGone                        // drained and being closed; removed once its goroutine ended
)

// pool is the connections of an App, one per hub
// (https://snodra.damoreira.com/library-authors/protocol/, Several hubs), all sending the
// identical Register:
//
//   - Dedupe. A hub is its RegisterAck.hub_id (for an empty one, its target as host:port). The
//     connection that acknowledged an id first owns it while it is kept, live or reconnecting; a
//     later one answering the same id is closed and parked (on one hub its Register has replaced
//     the owner's session already, so the owner reconnects, once). A parked address is connected
//     again once the owner is gone: it answers another hub id, or it left the DNS answer (SG-22).
//   - Routing. An unkeyed request goes round-robin over the hubs whose stream is acknowledged, a
//     keyed one to the live hub with the highest rendezvous score for (hub id, key). A hub that is
//     leaving (shutting down, rolled over to a renewed client certificate) is used only when no
//     other is live.
//   - Permits. A stream offers ceil(MaxConcurrent / N), N the hubs live or on their first
//     connect attempt, itself included. When N shrinks every live stream is topped up to
//     ceil(MaxConcurrent / N) with one Permits{count, grant}; nothing is ever revoked, and the
//     local bound stays MaxConcurrent.
//   - Status. Every statusInterval, and whenever the live hubs change, each live hub gets an
//     InstanceStatus.
//   - DNS. A dns:///name:port target is resolved at start and every dnsRefresh: a new address
//     gets a connection; a vanished one leaves the routing at once, gets a Register with no
//     types and is closed once idle (at most dnsDrainLimit later). A failed or empty answer
//     keeps the hubs.
type pool struct {
	app *App
	dns *target // the dns:/// target; nil for a list of hubs

	mu       sync.Mutex
	conns    []*hubConn          // in target (or resolution) order, parked duplicates and drained ones included
	owners   map[string]*hubConn // hub id → the connection that acknowledged it first
	resolved []string            // the latest DNS answer applied (a dns:/// target only)
	closed   bool                // the shutdown began: no connection is added

	turn atomic.Uint64 // the round-robin position
}

func newPool(a *App) *pool {
	p := &pool{app: a, owners: map[string]*hubConn{}}
	targets, err := parseTargets(a.cfg.hubs)
	if err != nil { // checked by the options already
		panic(err)
	}
	for _, t := range targets {
		if t.dns {
			p.dns = &t
			continue
		}
		p.conns = append(p.conns, p.newConn(t.address, "", t.https))
	}
	return p
}

// newConn is a connection of the pool to address; serverName is the name a TLS hub certificate
// is checked against when the configuration names none (a resolved address's DNS name); https
// reaches it over TLS even without a TLS configuration (an https:// target).
func (p *pool) newConn(address, serverName string, https bool) *hubConn {
	c := newHubConn(p.app, address)
	c.pool, c.serverName, c.https = p, serverName, https
	return c
}

// start runs every connection's goroutine, the status reports, the resolution of a dns:///
// target and the client certificate watch.
func (p *pool) start(a *App) {
	for _, c := range p.all() {
		go c.run()
	}
	go p.reportStatusEvery(a.cfg.timings.statusInterval)
	if p.dns != nil {
		go p.resolveEvery(a.cfg.timings.dnsRefresh)
	}
	if a.cfg.clientCert != "" {
		go p.watchCertificates(a)
	}
}

// all is every connection of the pool now.
func (p *pool) all() []*hubConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.conns)
}

// closeMembership stops the pool from adding connections (the shutdown) and returns them all.
func (p *pool) closeMembership() []*hubConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return slices.Clone(p.conns)
}

// liveHub is a hub with an acknowledged stream, at one moment.
type liveHub struct {
	conn *hubConn
	id   string
	ack  *hubpb.RegisterAck
}

// live is the kept connections whose stream is acknowledged, in target order.
func (p *pool) live() []liveHub {
	var out []liveHub
	for _, c := range p.all() {
		if c.memberRole() != roleActive {
			continue
		}
		if ack := c.ack(); ack != nil {
			out = append(out, liveHub{conn: c, id: hubIDOf(ack.GetHubId(), c.address), ack: ack})
		}
	}
	return out
}

// memberRole is the connection's role in its pool (roleActive without a pool).
func (c *hubConn) memberRole() memberRole {
	if c.pool == nil {
		return roleActive
	}
	c.pool.mu.Lock()
	defer c.pool.mu.Unlock()
	return c.role
}

// leaving is true while the connection is on its way out of the routing: its shutdown began,
// or it is being rolled over to a renewed client certificate.
func (c *hubConn) leaving() bool {
	return c.stopRequested() || c.isRolling() || c.currentState() == stateDraining
}

// routable is the live hubs a request may go to: those not leaving, or, when every live hub
// is leaving, those.
func (p *pool) routable() []liveHub {
	var staying, leaving []liveHub
	for _, h := range p.live() {
		if h.conn.leaving() {
			leaving = append(leaving, h)
		} else {
			staying = append(staying, h)
		}
	}
	if len(staying) == 0 {
		return leaving
	}
	return staying
}

// ranked is the hubs a request with key tries, in order: round-robin from the next turn for an
// unkeyed request, by descending rendezvous score for a keyed one.
func (p *pool) ranked(key string) []*hubConn {
	return p.order(key, true)
}

// order is ranked; an unkeyed request takes a round-robin turn only when turn is set.
func (p *pool) order(key string, turn bool) []*hubConn {
	hubs := p.routable()
	if len(hubs) == 0 {
		return nil
	}
	out := make([]*hubConn, 0, len(hubs))
	if key == "" {
		first := 0
		if turn {
			first = int((p.turn.Add(1) - 1) % uint64(len(hubs)))
		}
		for i := range hubs {
			out = append(out, hubs[(first+i)%len(hubs)].conn)
		}
		return out
	}
	scores := make([]uint64, len(hubs))
	for i, h := range hubs {
		scores[i] = rendezvous.Score(h.id, key)
	}
	idx := make([]int, len(hubs))
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(x, y int) int {
		switch {
		case rendezvous.Less(hubs[x].id, scores[x], hubs[y].id, scores[y]):
			return -1
		case rendezvous.Less(hubs[y].id, scores[y], hubs[x].id, scores[x]):
			return 1
		}
		return 0
	})
	for _, i := range idx {
		out = append(out, hubs[i].conn)
	}
	return out
}

// rolling is true while a client certificate rollover has a kept connection out of the routing.
func (p *pool) rolling() bool {
	for _, c := range p.all() {
		if c.memberRole() == roleActive && c.isRolling() {
			return true
		}
	}
	return false
}

// pick is the hub a request with key goes to first (taking a round-robin turn); with none, an
// error wrapping ErrNeverSent.
func (p *pool) pick(key string) (*hubConn, error) {
	ranked := p.ranked(key)
	if len(ranked) == 0 {
		return nil, fmt.Errorf("%w: no hub has acknowledged the registration", ErrNeverSent)
	}
	return ranked[0], nil
}

// ackFor is the RegisterAck of the hub a request with key would go to first now, without
// taking a round-robin turn; nil when no hub is acknowledged.
func (p *pool) ackFor(key string) *hubpb.RegisterAck {
	for _, c := range p.order(key, false) {
		if ack := c.ack(); ack != nil {
			return ack
		}
	}
	return nil
}

// hubCount is N of the permit split: the kept connections whose stream is acknowledged or that
// are still on their first connect attempt, and including (when not nil).
// TODO(spec-gap): SG-23 (a hub being rolled over to a renewed client certificate stays counted).
func (p *pool) hubCount(including *hubConn) int {
	p.mu.Lock()
	conns := slices.Clone(p.conns)
	roles := make([]memberRole, len(conns))
	for i, c := range conns {
		roles[i] = c.role
	}
	p.mu.Unlock()
	n := 0
	for i, c := range conns {
		if roles[i] == roleActive && (c == including || c.ack() != nil || !c.failedOnce.Load()) {
			n++
		}
	}
	return n
}

// initialPermits is what a new stream of c offers: ceil(MaxConcurrent / N), c included.
func (p *pool) initialPermits(c *hubConn) uint32 {
	return shareOf(p.app.cfg.maxConcurrent, p.hubCount(c))
}

// changed is called when the set of live hubs (or of hubs on their first attempt) may have
// changed: every live stream is topped up to ceil(MaxConcurrent / N) (more hubs change nothing:
// permits are never revoked), and every live hub gets an InstanceStatus at once, as in the
// reference library.
func (p *pool) changed() {
	defer p.reportStatus()
	share := shareOf(p.app.cfg.maxConcurrent, p.hubCount(nil))
	for _, h := range p.live() {
		if perms := h.conn.livePermits(); perms != nil {
			if n := perms.topUp(share); n > 0 {
				h.conn.log.Debug("permits topped up", "count", n, "share", share)
			}
		}
	}
}

// accept decides whether c, whose stream was just acknowledged with ack, keeps its hub: false
// when another kept connection acknowledged the same hub id first; c is then a duplicate.
func (p *pool) accept(c *hubConn, ack *hubpb.RegisterAck) bool {
	id := hubIDOf(ack.GetHubId(), c.address)
	p.mu.Lock()
	// c may reach another hub than before (a restarted hub has a new id): it gives up the id it
	// owned, and an address parked behind that id is connected again.
	for k, owner := range p.owners {
		if owner == c {
			delete(p.owners, k)
		}
	}
	if owner, ok := p.owners[id]; ok && owner.role == roleActive {
		c.role, c.parkedID = roleDuplicate, id
		// The id c gave up may have an address parked behind it (c itself stays parked: its id
		// is owned).
		revived := p.reviveLocked()
		p.mu.Unlock()
		c.log.Warn("two hub addresses reach the same hub; closing and parking the second", "hub_id", id, "kept", owner.address)
		p.startRevived(revived)
		return false
	}
	p.owners[id] = c
	revived := p.reviveLocked()
	p.mu.Unlock()
	p.startRevived(revived)
	return true
}

// reviveLocked replaces every parked duplicate whose owner is gone (no kept connection owns its
// hub id any more) with a new connection to its address, unless its address left the DNS answer,
// and returns the new connections, to start. Caller holds p.mu.
// TODO(spec-gap): SG-22 (a parked address is retried once its owner is gone).
func (p *pool) reviveLocked() []*hubConn {
	if p.closed {
		return nil
	}
	for k, owner := range p.owners {
		if owner.role != roleActive {
			delete(p.owners, k) // a draining or gone owner keeps nothing
		}
	}
	var out []*hubConn
	for i, c := range p.conns {
		if c.role != roleDuplicate {
			continue
		}
		if _, owned := p.owners[c.parkedID]; owned {
			continue
		}
		if p.dns != nil && !slices.Contains(p.resolved, c.address) {
			continue
		}
		n := p.newConn(c.address, c.serverName, c.https)
		p.conns[i] = n
		out = append(out, n)
	}
	return out
}

// startRevived runs the goroutines of revived parked addresses.
func (p *pool) startRevived(revived []*hubConn) {
	for _, c := range revived {
		c.log.Info("the hub this address was parked behind has no other address any more; connecting again")
		go c.run()
	}
}

// remove takes a closed connection out of the pool.
func (p *pool) remove(c *hubConn) {
	p.mu.Lock()
	p.conns = slices.DeleteFunc(p.conns, func(x *hubConn) bool { return x == c })
	for k, owner := range p.owners {
		if owner == c {
			delete(p.owners, k)
		}
	}
	revived := p.reviveLocked()
	p.mu.Unlock()
	p.startRevived(revived)
}

// reportStatusEvery sends an InstanceStatus to every live hub every interval (changed sends one
// too, whenever the live hubs change), until the hard
// stop (the shutdown's streams are still reported to until they close).
func (p *pool) reportStatusEvery(interval time.Duration) {
	ticker := time.NewTicker(max(interval, time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.reportStatus()
		case <-p.app.killed:
			return
		}
	}
}

// reportStatus sends one InstanceStatus to every live hub: the ids of the live hubs, the
// handlers in flight over all hubs, and no processor. A hub being rolled over to a renewed
// client certificate is out of both while another live hub is not, as in the reference library
// (which takes a hub out of the routing for a rollover only then).
// See https://snodra.damoreira.com/library-authors/protocol/ (Instance status).
func (p *pool) reportStatus() {
	var hubs []liveHub
	var ids []string
	live := p.live()
	steady := slices.ContainsFunc(live, func(h liveHub) bool { return !h.conn.isRolling() })
	for _, h := range live {
		if steady && h.conn.isRolling() {
			continue // another hub serves meanwhile; a lone hub keeps its reports
		}
		hubs = append(hubs, h)
		if !slices.Contains(ids, h.id) {
			ids = append(ids, h.id)
		}
	}
	if len(hubs) == 0 {
		return
	}
	status := &hubpb.InstanceStatus{
		ConnectedHubIds:  ids,
		HandlersInFlight: uint32(min(p.app.inflight.count(), math.MaxUint32)),
	}
	for _, h := range hubs {
		if ref, _ := h.conn.live(); ref != nil {
			st := proto.CloneOf(status)
			_ = ref.send(&hubpb.ClientMessage{Message: &hubpb.ClientMessage_Status{Status: st}})
		}
	}
}

// resolveEvery resolves the dns:/// target now and every interval until the shutdown.
func (p *pool) resolveEvery(interval time.Duration) {
	host, port := p.dns.hostPort()
	log := p.app.log.With("hubs", p.dns.String())
	for {
		ctx, cancel := p.stopCtx()
		addrs, err := p.app.cfg.resolve(ctx, host, port)
		cancel()
		switch {
		case p.stopRequested():
			return
		case err != nil:
			log.Warn("the hub name was not resolved; keeping the current hubs", "reason", err.Error())
		case len(addrs) == 0:
			log.Warn("the hub name resolved to no address; keeping the current hubs")
		default:
			p.apply(addrs)
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-p.app.stopped:
			timer.Stop()
			return
		case <-p.app.killed:
			timer.Stop()
			return
		}
	}
}

// stopRequested is true once the shutdown began or the app was stopped hard.
func (p *pool) stopRequested() bool {
	select {
	case <-p.app.stopped:
		return true
	case <-p.app.killed:
		return true
	default:
		return false
	}
}

// stopCtx is a context that ends at the shutdown; cancel releases it.
func (p *pool) stopCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-p.app.stopped:
		case <-p.app.killed:
		case <-ctx.Done():
		}
		cancel()
	}()
	return ctx, cancel
}

// drained is a connection whose address left the DNS answer, and its role before.
type drained struct {
	conn *hubConn
	was  memberRole
}

// apply connects to the resolved addresses that have no connection, revives a draining one
// that is back, and drains the ones that vanished.
func (p *pool) apply(resolved []string) {
	host, _ := p.dns.hostPort()
	if net.ParseIP(host) != nil {
		host = "" // an address is checked against itself
	}
	var added, revived []*hubConn
	var gone []drained
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.resolved = slices.Clone(resolved)
	current := map[string]*hubConn{}
	for _, c := range p.conns {
		if c.role != roleGone {
			current[c.address] = c
		}
	}
	for _, address := range resolved {
		switch c, ok := current[address]; {
		case !ok:
			c = p.newConn(address, host, false)
			p.conns = append(p.conns, c)
			added = append(added, c)
		case c.role == roleDraining:
			c.role = roleActive
			c.withdrawn.Store(false)
			revived = append(revived, c)
		}
	}
	for address, c := range current {
		if !slices.Contains(resolved, address) && c.role != roleDraining {
			gone = append(gone, drained{conn: c, was: c.role})
			if c.role != roleDuplicate {
				// Decided here, with the role: a revival in a later answer undoes both.
				c.withdrawn.Store(true)
			}
			c.role = roleDraining
		}
	}
	parked := p.reviveLocked()
	p.mu.Unlock()
	p.startRevived(parked)
	for _, c := range added {
		c.log.Info("new hub address in DNS; connecting")
		go c.run()
	}
	for _, c := range revived {
		c.log.Info("the hub address is back in DNS; kept")
		c.reregister()
	}
	for _, d := range gone {
		d.conn.log.Info("the hub address left DNS; closing it once idle")
		if d.was != roleDuplicate {
			d.conn.reregister() // the Register with no types
		}
		go p.drain(d.conn, d.was)
	}
	p.changed()
}

// drain closes a connection whose address left the DNS answer: a Register with no types first
// (the hub stops routing to the instance there; a reconnect registers none either), then the
// stream is closed once no request of the instance written to it is unanswered and no handler
// of its requests runs, at most dnsDrainLimit later. A duplicate has nothing to wait for.
func (p *pool) drain(c *hubConn, was memberRole) {
	if was != roleDuplicate {
		deadline := time.Now().Add(dnsDrainLimit)
		ticker := time.NewTicker(dnsDrainPoll)
		defer ticker.Stop()
		for !c.idle() && time.Now().Before(deadline) {
			select {
			case <-ticker.C:
			case <-p.app.stopped:
				return // the shutdown closes it
			case <-p.app.killed:
				return
			}
			if c.memberRole() != roleDraining {
				return // back in DNS
			}
		}
	}
	p.mu.Lock()
	if c.role != roleDraining {
		p.mu.Unlock()
		return
	}
	c.role = roleGone
	p.mu.Unlock()
	c.stop()
	c.close()
	select {
	case <-c.done:
	case <-p.app.killed:
	}
	p.remove(c)
	c.log.Info("the hub connection is closed (its address left DNS)")
}

// deregistered are the channels closed once each connection's empty Register is acked or
// needs no wait.
func deregistered(conns []*hubConn) []<-chan struct{} {
	out := make([]<-chan struct{}, len(conns))
	for i, c := range conns {
		out[i] = c.deregDone
	}
	return out
}

// dones are the channels closed once each connection's goroutine has returned.
func dones(conns []*hubConn) []<-chan struct{} {
	out := make([]<-chan struct{}, len(conns))
	for i, c := range conns {
		out[i] = c.done
	}
	return out
}
