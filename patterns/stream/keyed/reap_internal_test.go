package keyed

import (
	"testing"
	"time"

	"github.com/gostdlib/base/context"
)

// newReg builds a keyReg with one idle lane registered under "k" for the reap tests.
func newReg(idle time.Duration) (*keyReg[int, int, int], *perLane[int, int]) {
	reg := &keyReg[int, int, int]{
		fn:    func(_ context.Context, _ int, v int) (int, error) { return v, nil },
		out:   make(chan emit[int, int], laneBuffer),
		idle:  idle,
		lanes: map[string]*perLane[int, int]{},
	}
	lane := &perLane[int, int]{inbox: make(chan laneItem[int, int], laneBuffer)}
	reg.lanes["k"] = lane
	return reg, lane
}

// TestRunLaneReaps verifies a lane with nothing in flight retires itself (returns and deletes itself
// from the registry) after the idle timeout.
func TestRunLaneReaps(t *testing.T) {
	t.Parallel()

	reg, lane := newReg(20 * time.Millisecond)
	done := make(chan struct{})
	context.Pool(t.Context()).Submit(t.Context(), func() {
		defer close(done)
		reg.runLane(t.Context(), "k", lane)
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("TestRunLaneReaps: runLane did not retire when idle")
	}

	reg.mu.Lock()
	_, present := reg.lanes["k"]
	reg.mu.Unlock()
	if present {
		t.Error("TestRunLaneReaps: lane still in the registry after retiring")
	}
}

// TestRunLaneKeepsReserved verifies a lane the dispatcher has reserved (inflight > 0) is not retired
// when the idle timer fires; it retires only once cancelled.
func TestRunLaneKeepsReserved(t *testing.T) {
	t.Parallel()

	reg, lane := newReg(20 * time.Millisecond)
	reg.mu.Lock()
	lane.inflight = 1 // reserved by a dispatcher that has not sent yet
	reg.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(done)
		reg.runLane(ctx, "k", lane)
	})

	// Past several idle timeouts, the reserved lane must still be present (not retired).
	time.Sleep(100 * time.Millisecond)
	reg.mu.Lock()
	_, present := reg.lanes["k"]
	reg.mu.Unlock()
	if !present {
		t.Error("TestRunLaneKeepsReserved: reserved lane was retired while inflight > 0")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("TestRunLaneKeepsReserved: runLane did not exit on cancel")
	}
}
