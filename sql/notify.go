package sql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// The channel appends notify and relays listen on; the payload is empty. It is part of the
// shared layout, so every Snodra SQL add-on uses it.
const notifyChannel = "snodra_outbox"

const (
	notifyHealthCheck    = 30 * time.Second
	notifyNetworkTimeout = 10 * time.Second
	notifyMinBackoff     = 200 * time.Millisecond
	notifyMaxBackoff     = 30 * time.Second
)

// notifier is the PostgreSQL relay wake-up: a dedicated connection listens on snodra_outbox, and
// every notification an append's commit delivers wakes the registered wakes. It runs while at
// least one relay is registered; a failure is retried with a backoff, and listening again wakes
// the relays (an append may have committed meanwhile).
type notifier struct {
	store  *Store
	config *pgx.ConnConfig

	mu      sync.Mutex
	wakers  map[uint64]func()
	next    uint64
	cancel  context.CancelFunc
	stopped bool
	listens atomic.Uint64
}

func newNotifier(store *Store) *notifier {
	return &notifier{store: store, config: store.pgConfig, wakers: make(map[uint64]func())}
}

// register adds wake and starts the listener when it was not running; the returned function
// removes it, and stops the listener when it was the last one. It is safe to call more than once,
// and a store that has stopped its listener (Close) registers nothing: a listener never starts
// after Close.
func (n *notifier) register(wake func()) func() {
	if wake == nil {
		return func() {}
	}
	n.mu.Lock()
	if n.stopped || n.store.closed.Load() {
		n.mu.Unlock()
		return func() {}
	}
	id := n.next
	n.next++
	n.wakers[id] = wake
	if n.cancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		n.cancel = cancel
		go n.run(ctx)
	}
	n.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			n.mu.Lock()
			delete(n.wakers, id)
			if len(n.wakers) == 0 && n.cancel != nil {
				n.cancel()
				n.cancel = nil
			}
			n.mu.Unlock()
		})
	}
}

// stop ends the listener, forgets every registered wake and refuses a later register; Store.Close
// calls it.
func (n *notifier) stop() {
	n.mu.Lock()
	n.stopped = true
	clear(n.wakers)
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	n.mu.Unlock()
}

// wakeAll calls every registered wake, outside the lock; a wake must return at once. A stopped
// notifier wakes nobody.
func (n *notifier) wakeAll() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	wakers := make([]func(), 0, len(n.wakers))
	for _, wake := range n.wakers {
		wakers = append(wakers, wake)
	}
	n.mu.Unlock()
	for _, wake := range wakers {
		wake()
	}
}

// run listens until ctx is done, reconnecting with a backoff from notifyMinBackoff to
// notifyMaxBackoff.
func (n *notifier) run(ctx context.Context) {
	backoff := notifyMinBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := n.connect(ctx)
		if err == nil {
			backoff = notifyMinBackoff
			n.listens.Add(1)
			// An append may have committed while nobody listened.
			n.wakeAll()
			err = n.receive(ctx, conn)
			_ = conn.Close(context.Background())
		}
		if ctx.Err() != nil {
			return
		}
		_ = err
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > notifyMaxBackoff {
			backoff = notifyMaxBackoff
		}
	}
}

// connect opens a dedicated connection (never one of the store's pool) and listens on the
// channel.
func (n *notifier) connect(ctx context.Context) (*pgx.Conn, error) {
	connectCtx, cancel := context.WithTimeout(ctx, notifyNetworkTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(connectCtx, n.config.Copy())
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(connectCtx, "LISTEN "+notifyChannel); err != nil {
		_ = conn.Close(context.Background())
		return nil, err
	}
	return conn, nil
}

// receive wakes the relays on every notification, and checks a quiet connection is still there:
// a half-open connection must not wait forever for a notification that will not come.
func (n *notifier) receive(ctx context.Context, conn *pgx.Conn) error {
	for {
		waitCtx, cancel := context.WithTimeout(ctx, notifyHealthCheck)
		_, err := conn.WaitForNotification(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			healthCtx, healthCancel := context.WithTimeout(ctx, notifyNetworkTimeout)
			_, healthErr := conn.Exec(healthCtx, "SELECT 1")
			healthCancel()
			if healthErr != nil {
				return healthErr
			}
			continue
		}
		n.wakeAll()
	}
}

// listens is how many times the listener established LISTEN; the tests wait for the first one.
func (n *notifier) listenCount() uint64 { return n.listens.Load() }

// listens is how many times this store's listener established LISTEN; 0 without a listener.
func (s *Store) listens() uint64 {
	if s.notifier == nil {
		return 0
	}
	return s.notifier.listenCount()
}
