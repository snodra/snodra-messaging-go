package snodra

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snodra/snodra-messaging-go/internal/hubpb"
)

// runShutdownBudget bounds the Shutdown that Run does when its context ends.
const runShutdownBudget = 15 * time.Second

// killGrace bounds the wait for the connections' goroutines after they were stopped hard.
const killGrace = 50 * time.Millisecond

var (
	errShutDown       = errors.New("snodra: the app is shut down")
	errAlreadyStarted = errors.New("snodra: the app is already started")
)

// App is one instance of a service on the hubs: it registers with every configured hub,
// keeps the streams alive and reconnects them until [App.Shutdown].
type App struct {
	cfg config
	log *slog.Logger

	ready     chan struct{} // closed at the first RegisterAck
	readyOnce sync.Once
	stopped   chan struct{} // closed when Shutdown begins
	closed    chan struct{} // closed when Shutdown has finished
	killed    chan struct{} // closed to stop every connection at once
	killOnce  sync.Once

	// handlerCtx is the base context of every handler; cancelled at the hard stop.
	handlerCtx     context.Context
	cancelHandlers context.CancelFunc
	slots          chan struct{} // MaxConcurrent handlers at once
	inflight       *inflight     // requests being served
	// handlers by type, written before Start only; commandTypes, queryTypes and
	// sharedQueryTypes are their names, sorted, fixed by Start.
	handlers                                   map[string]*handler
	commandTypes, queryTypes, sharedQueryTypes []string
	calls                                      *pendingCalls    // the sent requests awaiting their answer
	running                                    *runningRequests // the forwarded requests being served, for the hub's Cancel
	subs                                       *servedSubscriptions
	identity                                   *identityResolver
	tokens                                     *tokenProvider // the access tokens towards the hubs; nil without JWT
	tel                                        *telemetry     // what the library reports; never nil
	gaugesEnded                                atomic.Bool    // Shutdown finished: the gauges read zeros
	certMu                                     sync.Mutex
	certSeen                                   map[string]struct{} // the client certificates any hub presented, for the rotation count

	drainMu    sync.Mutex
	drainUntil time.Time // when the shutdown's handler drain ends; zero before it

	mu        sync.Mutex
	started   bool
	stopping  bool
	stopErr   error // the first Shutdown's result, returned by every later one
	pool      *pool
	rejection *Rejection
	regErrs   []error // every registration error, returned by Start
}

// Rejection is a hub's refusal of the instance's Register: the hub closed the stream with
// ALREADY_EXISTS (a type another service owns), INVALID_ARGUMENT, UNAUTHENTICATED or
// PERMISSION_DENIED instead of acknowledging it. The instance keeps running and retries at
// its backoff cap. With [JWT], a failure to get an access token, when no stream could be
// opened, is kept too (Code UNAUTHENTICATED, a Message that names the access token); it is
// retried at the ordinary backoff. Like any kept refusal it is not cleared when a hub accepts
// the Register later: a token endpoint that failed once at startup leaves it here while the
// instance runs registered, and it replaces an earlier refusal by a hub.
type Rejection struct {
	// Hub is the hub's address (host:port); a refused Register carries no hub id.
	Hub string
	// Code is the gRPC status in upper snake case: "ALREADY_EXISTS", "INVALID_ARGUMENT",
	// "UNAUTHENTICATED" or "PERMISSION_DENIED".
	Code string
	// Message is the hub's reason, for example "type orders.PlaceOrder is owned by billing-service".
	Message string
	// At is when the refusal arrived.
	At time.Time
}

// New builds an App from opts; it connects to nothing before [App.Start]. It fails on missing
// or invalid settings (no service name, no hub, ...).
func New(opts ...Option) (*App, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o(&cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	hctx, cancel := context.WithCancel(context.Background())
	log := cfg.logger.With("service", cfg.service, "instance", cfg.instance)
	var tokens *tokenProvider
	if cfg.tokenSource != nil {
		tokens = newTokenProvider(cfg.tokenSource, log, cfg.timings.tokenFetchWait)
	}
	tel := newTelemetry(cfg.telemetry, log)
	if cc, ok := cfg.tokenSource.(*clientCredentials); ok {
		cc.observe = tel.tokenFetched // the copy the options made for this App
	}
	return &App{
		tel:            tel,
		cfg:            cfg,
		log:            log,
		ready:          make(chan struct{}),
		stopped:        make(chan struct{}),
		closed:         make(chan struct{}),
		killed:         make(chan struct{}),
		handlerCtx:     hctx,
		cancelHandlers: cancel,
		slots:          make(chan struct{}, cfg.maxConcurrent),
		inflight:       newInflight(),
		handlers:       map[string]*handler{},
		calls:          newPendingCalls(),
		running:        newRunningRequests(),
		subs:           newServedSubscriptions(cfg.maxSubscriptions, log),
		identity:       newIdentityResolver(cfg, log),
		tokens:         tokens,
	}, nil
}

// Start opens a connection to every hub and returns at once; each connection registers and
// reconnects on its own until [App.Shutdown]. Handlers can no longer be registered after it.
// [App.Ready] waits for the first acknowledged hub. When a registration failed, Start returns
// every registration error, joined, and starts nothing.
//
// ctx is only checked on entry (Start returns its error when it is already done): it does not
// bound the app, and cancelling it later stops nothing. [App.Shutdown] stops the app; [App.Run]
// serves until its own context ends.
func (a *App) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.start(); err != nil {
		return err
	}
	a.tel.observeGauges(a.gauges) // no lock held: the implementation may read at once
	return nil
}

// start is Start's work under the app's lock.
func (a *App) start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case a.stopping:
		return errShutDown
	case a.started:
		return errAlreadyStarted
	}
	if err := a.registrationError(); err != nil {
		return err
	}
	a.freezeTypes()
	a.started = true
	a.warnPlainTextCredential()
	a.pool = newPool(a)
	a.pool.start(a)
	return nil
}

// certPresented notes that a stream went live presenting the client certificate with fingerprint
// fp: one rotation is counted when no hub presented this certificate before and another one was
// presented already. One renewal is one rotation, however many hubs roll over to it, and a hub
// reconnecting with an older pair (or acks arriving in reverse order) counts nothing.
func (a *App) certPresented(fp string) {
	a.certMu.Lock()
	_, seen := a.certSeen[fp]
	first := len(a.certSeen) == 0
	if !seen {
		if a.certSeen == nil {
			a.certSeen = map[string]struct{}{}
		}
		a.certSeen[fp] = struct{}{}
	}
	a.certMu.Unlock()
	if !seen && !first {
		a.tel.certificateRotated()
	}
}

// gauges are the library's gauges now, read from memory.
func (a *App) gauges() Gauges {
	if a.gaugesEnded.Load() {
		return Gauges{}
	}
	g := Gauges{HandlersInFlight: int64(a.inflight.count())}
	a.mu.Lock()
	p := a.pool
	a.mu.Unlock()
	if p != nil {
		g.ConnectedHubs = int64(len(p.live()))
	}
	return g
}

// Ready waits until a hub has acknowledged the registration (its RegisterAck arrived). It
// returns ctx's error when ctx ends first, and an error when the app shuts down first.
func (a *App) Ready(ctx context.Context) error {
	select {
	case <-a.ready:
		return nil
	default:
	}
	select {
	case <-a.ready:
		return nil
	case <-a.stopped:
		return errShutDown
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the app, waits for ctx to end, then shuts down within 15 s. It returns nil on a
// clean stop.
func (a *App) Run(ctx context.Context) error {
	if err := a.Start(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-a.stopped:
	}
	sctx, cancel := context.WithTimeout(context.Background(), runShutdownBudget)
	defer cancel()
	return a.Shutdown(sctx)
}

// Shutdown leaves the hubs in order: a Register with no types to every acknowledged hub (so
// it routes elsewhere at once), waiting at most 1 s for its acks; then every subscription served
// here completes with a ReplyEnd; then the handlers in flight finish (at most 5 s), the ends
// written meanwhile (waited for at most 2 s more); then each stream is half-closed and the hub
// gets at most 2 s to finish its side.
// Every wait is also bounded by ctx: when ctx ends first, the streams are dropped and ctx's
// error is returned, at most 50 ms after ctx's deadline (the time given to the dropped streams'
// goroutines to end). A second call waits for the first and returns its result.
// See https://snodra.damoreira.com/library-authors/protocol/ (Shutting down).
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	if a.stopping {
		a.mu.Unlock()
		select {
		case <-a.closed:
			a.mu.Lock()
			defer a.mu.Unlock()
			return a.stopErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.stopping = true
	close(a.stopped)
	p := a.pool
	a.mu.Unlock()
	defer close(a.closed)
	if p == nil {
		return nil
	}
	defer func() { // before closed is closed
		a.gaugesEnded.Store(true)
		a.tel.stopGauges()
	}()
	conns := p.closeMembership()
	err := a.leave(ctx, conns)
	a.kill()
	// Every wait of a connection also watches killed, so they end at once; the grace only
	// keeps a defect there from holding Shutdown past its deadline.
	_ = waitAll(context.Background(), killGrace, dones(conns))
	a.mu.Lock()
	a.stopErr = err
	a.mu.Unlock()
	return err
}

// kill stops every connection and ends the handlers' context.
func (a *App) kill() {
	a.killOnce.Do(func() {
		close(a.killed)
		a.cancelHandlers()
		if a.tokens != nil {
			a.tokens.close()
		}
	})
}

// leave runs the shutdown's steps; the error is ctx's when it ended first.
// TODO(spec-gap): SG-9 (only the 1 s for the empty Register's acks is specified).
func (a *App) leave(ctx context.Context, conns []*hubConn) error {
	for _, c := range conns {
		c.stop()
	}
	if err := waitAll(ctx, a.cfg.timings.deregisterWait, deregistered(conns)); err != nil {
		return err
	}
	// Every served subscription completes at once, as in the reference library: its subscribers
	// are told now, not after a slow handler, and its end is written before the half-close.
	refs := a.subs.endAll()
	// The handlers in flight finish; their replies still go out. Requests the hubs forward
	// meanwhile are served too, also those arriving until each stream's half-close (which waits
	// for them until the same deadline).
	a.setDrainDeadline(time.Now().Add(a.cfg.timings.handlerDrain))
	if err := waitAll(ctx, a.cfg.timings.handlerDrain, []<-chan struct{}{a.inflight.drained()}); err != nil {
		return err
	}
	if n := a.inflight.count(); n > 0 {
		a.log.Warn("handlers still running after the drain; closing the hub streams anyway",
			"handlers", n, "waited", a.cfg.timings.handlerDrain)
	}
	if len(refs) > 0 {
		flushed := make([]<-chan struct{}, len(refs))
		for i, ref := range refs {
			flushed[i] = ref.flushed()
		}
		if err := waitAll(ctx, a.cfg.timings.subscriptionEnd, flushed); err != nil {
			return err
		}
	}
	for _, c := range conns {
		c.close()
	}
	return waitAll(ctx, 0, dones(conns)) // each connection bounds its own close wait
}

// setDrainDeadline sets when the handler drain of the shutdown ends.
func (a *App) setDrainDeadline(t time.Time) {
	a.drainMu.Lock()
	a.drainUntil = t
	a.drainMu.Unlock()
}

// drainDeadline is when the handler drain of the shutdown ends; zero before the drain.
func (a *App) drainDeadline() time.Time {
	a.drainMu.Lock()
	defer a.drainMu.Unlock()
	return a.drainUntil
}

// waitAll waits until every channel is closed, at most bound (0: no bound) and at most until
// ctx ends; only the end of ctx is an error.
func waitAll(ctx context.Context, bound time.Duration, chans []<-chan struct{}) error {
	var expired <-chan time.Time
	if bound > 0 {
		timer := time.NewTimer(bound)
		defer timer.Stop()
		expired = timer.C
	}
	for _, ch := range chans {
		select {
		case <-ch:
		case <-expired:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// HubIDs are the RegisterAck.hub_id of every hub whose stream is acknowledged now, sorted (a
// second address of one hub, closed as a duplicate, is not among them; an empty hub_id reads
// as the hub's host:port).
func (a *App) HubIDs() []string {
	a.mu.Lock()
	p := a.pool
	a.mu.Unlock()
	if p == nil {
		return nil
	}
	var ids []string
	for _, h := range p.live() {
		ids = append(ids, h.id)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// Rejection is the latest refusal of the Register by any hub, and false while none ever
// refused it. A later acceptance does not clear it.
func (a *App) Rejection() (Rejection, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rejection == nil {
		return Rejection{}, false
	}
	return *a.rejection, true
}

func (a *App) recordRejection(r Rejection) {
	a.mu.Lock()
	a.rejection = &r
	a.mu.Unlock()
}

// connLive is called by a connection whose stream was acknowledged.
func (a *App) connLive() { a.readyOnce.Do(func() { close(a.ready) }) }

// registerMsg is the Register every stream opens with, identical for every hub.
func (a *App) registerMsg() *hubpb.Register {
	return &hubpb.Register{
		ServiceName:      a.cfg.service,
		InstanceId:       a.cfg.instance,
		CommandTypes:     slices.Clone(a.commandTypes),
		QueryTypes:       slices.Clone(a.queryTypes),
		SharedQueryTypes: slices.Clone(a.sharedQueryTypes),
		InitialPermits:   uint32(a.cfg.maxConcurrent),
		LoadFactor:       a.cfg.loadFactor,
		Tags:             maps.Clone(a.cfg.tags),
		ClientVersion:    clientVersion(),
	}
}

// emptyRegisterMsg is the Register with no types of a shutdown: the hub takes the instance out
// of its routing.
func (a *App) emptyRegisterMsg() *hubpb.Register {
	r := a.registerMsg()
	r.CommandTypes, r.QueryTypes, r.SharedQueryTypes = nil, nil, nil
	return r
}
