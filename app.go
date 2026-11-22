package snodramessaging

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
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
	// handlers by type, written before Start only; commandTypes and queryTypes are their
	// names, sorted, fixed by Start.
	handlers                 map[string]*handler
	commandTypes, queryTypes []string

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
// its backoff cap.
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
	return &App{
		cfg:            cfg,
		log:            cfg.logger.With("service", cfg.service, "instance", cfg.instance),
		ready:          make(chan struct{}),
		stopped:        make(chan struct{}),
		closed:         make(chan struct{}),
		killed:         make(chan struct{}),
		handlerCtx:     hctx,
		cancelHandlers: cancel,
		slots:          make(chan struct{}, cfg.maxConcurrent),
		inflight:       newInflight(),
		handlers:       map[string]*handler{},
	}, nil
}

// Start opens a connection to every hub and returns at once; each connection registers and
// reconnects on its own until [App.Shutdown]. Handlers can no longer be registered after it.
// [App.Ready] waits for the first acknowledged hub. When a registration failed, Start returns
// every registration error, joined, and starts nothing.
func (a *App) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	a.pool = newPool(a)
	a.pool.start()
	return nil
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
// it routes elsewhere at once), waiting at most 1 s for its acks; then the handlers in flight
// finish; then each stream is half-closed and the hub gets at most 2 s to finish its side.
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
	err := a.leave(ctx, p)
	a.kill()
	// Every wait of a connection also watches killed, so they end at once; the grace only
	// keeps a defect there from holding Shutdown past its deadline.
	_ = waitAll(context.Background(), killGrace, p.dones())
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
	})
}

// leave runs the shutdown's steps; the error is ctx's when it ended first.
// TODO(spec-gap): SG-9 (only the 1 s for the empty Register's acks is specified).
func (a *App) leave(ctx context.Context, p *pool) error {
	for _, c := range p.conns {
		c.stop()
	}
	if err := waitAll(ctx, a.cfg.timings.deregisterWait, p.deregistered()); err != nil {
		return err
	}
	// The handlers in flight finish; their replies still go out. Requests the hubs forward
	// meanwhile are served too.
	if err := waitAll(ctx, a.cfg.timings.handlerDrain, []<-chan struct{}{a.inflight.drained()}); err != nil {
		return err
	}
	if n := a.inflight.count(); n > 0 {
		a.log.Warn("handlers still running after the drain; closing the hub streams anyway",
			"handlers", n, "waited", a.cfg.timings.handlerDrain)
	}
	for _, c := range p.conns {
		c.close()
	}
	return waitAll(ctx, 0, p.dones()) // each connection bounds its own close wait
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

// HubIDs are the RegisterAck.hub_id of every hub whose stream is acknowledged now, sorted.
func (a *App) HubIDs() []string {
	a.mu.Lock()
	p := a.pool
	a.mu.Unlock()
	if p == nil {
		return nil
	}
	var ids []string
	for _, c := range p.conns {
		if ack := c.ack(); ack != nil {
			ids = append(ids, hubID(ack, c.address))
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// hubID is the hub's own id, or its address for an ack without one (a real hub always names
// itself).
func hubID(ack *hubpb.RegisterAck, address string) string {
	if id := ack.GetHubId(); id != "" {
		return id
	}
	return address
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
		ServiceName:    a.cfg.service,
		InstanceId:     a.cfg.instance,
		CommandTypes:   slices.Clone(a.commandTypes),
		QueryTypes:     slices.Clone(a.queryTypes),
		InitialPermits: uint32(a.cfg.maxConcurrent),
		LoadFactor:     a.cfg.loadFactor,
		Tags:           maps.Clone(a.cfg.tags),
		ClientVersion:  clientVersion(),
	}
}

// emptyRegisterMsg is the Register with no types of a shutdown: the hub takes the instance out
// of its routing.
func (a *App) emptyRegisterMsg() *hubpb.Register {
	r := a.registerMsg()
	r.CommandTypes, r.QueryTypes, r.SharedQueryTypes = nil, nil, nil
	return r
}
