package rabbitmq

import "sync"

// liveRegistry is the subscriptions this process reads now, per (source, name): a reset must not
// move a subscription this very process reads, whatever the management API says.
type liveRegistry struct {
	mu    sync.Mutex
	byKey map[string]map[string]bool
}

func newLiveRegistry() *liveRegistry {
	return &liveRegistry{byKey: make(map[string]map[string]bool)}
}

func liveKey(source, name string) string { return source + "\x00" + name }

// enter counts a member of a subscription until the returned guard is dropped.
func (l *liveRegistry) enter(source, name, member string) *liveGuard {
	l.mu.Lock()
	key := liveKey(source, name)
	if l.byKey[key] == nil {
		l.byKey[key] = make(map[string]bool)
	}
	l.byKey[key][member] = true
	l.mu.Unlock()
	return &liveGuard{live: l, key: key, member: member}
}

// members is the members of one subscription this process reads now.
func (l *liveRegistry) members(source, name string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	all := l.byKey[liveKey(source, name)]
	if len(all) == 0 {
		return nil
	}
	members := make([]string, 0, len(all))
	for member := range all {
		members = append(members, member)
	}
	return members
}

type liveGuard struct {
	live   *liveRegistry
	key    string
	member string
	once   sync.Once
}

func (g *liveGuard) leave() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		g.live.mu.Lock()
		if members := g.live.byKey[g.key]; members != nil {
			delete(members, g.member)
			if len(members) == 0 {
				delete(g.live.byKey, g.key)
			}
		}
		g.live.mu.Unlock()
	})
}
