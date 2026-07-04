package foreach

import (
	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
)

// gate pauses Item's dispatch of new work while pairs are retrying under WithGate. It is counting —
// dispatch proceeds only while no pauses are outstanding, so several pairs retrying against the same
// sick dependency keep dispatch paused until the last one resolves. Each Item range gets its own
// gate. All methods are safe for concurrent use.
type gate struct {
	mu sync.Mutex
	// count is the number of outstanding pauses.
	count int
	// opened is created on the transition to paused and closed on the transition back; waiters block on
	// the channel they observed, then re-check.
	opened chan struct{}
}

// pause moves the gate to (or keeps it in) the paused state.
func (g *gate) pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.count == 0 {
		g.opened = make(chan struct{})
	}
	g.count++
}

// resume undoes one pause; the gate opens when no pauses remain outstanding.
func (g *gate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.count--
	if g.count == 0 {
		close(g.opened)
	}
}

// open reports whether no pauses are outstanding.
func (g *gate) open() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.count == 0
}

// wait blocks while the gate is paused. It returns nil when the gate is open and ctx.Err() if ctx is
// cancelled first.
func (g *gate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		if g.count == 0 {
			g.mu.Unlock()
			return nil
		}
		opened := g.opened
		g.mu.Unlock()
		select {
		case <-opened:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
