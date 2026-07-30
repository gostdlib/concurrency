package sequencer

import (
	"testing"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/errors"
)

// TestDone pins the turn-advancement logic: a Done at the current turn advances and drains any
// higher sequences already resolved, while a Done above the turn is buffered until the turn reaches
// it. All cases here resolve sequences without any waiter, so they exercise advancement in
// isolation.
func TestDone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// dones is the order Done is called in.
		dones []uint64
		// wantTurn is the turn after every Done has been applied.
		wantTurn uint64
	}{
		{
			name:     "Success: a single Done at zero advances the turn to one",
			dones:    []uint64{0},
			wantTurn: 1,
		},
		{
			name:     "Success: in-order Dones advance the turn contiguously",
			dones:    []uint64{0, 1, 2},
			wantTurn: 3,
		},
		{
			name:     "Success: out-of-order Dones buffer then drain when the turn arrives",
			dones:    []uint64{2, 1, 0},
			wantTurn: 3,
		},
		{
			name:     "Success: a resolved sequence above a gap leaves the turn parked at the gap",
			dones:    []uint64{0, 2},
			wantTurn: 1,
		},
		{
			name:     "Success: filling a gap drains every buffered sequence above it",
			dones:    []uint64{1, 2, 0},
			wantTurn: 3,
		},
	}

	for _, test := range tests {
		s := New()
		for _, seq := range test.dones {
			s.Done(seq)
		}
		if got := s.Turn(); got != test.wantTurn {
			t.Errorf("TestDone(%s): got turn == %d, want turn == %d", test.name, got, test.wantTurn)
		}
	}
}

// TestWaitOrdering is the core guarantee under the race detector: many goroutines, each waiting on
// its own sequence, may only proceed in sequence order. Each records its sequence when Wait returns
// and then resolves it, so the recorded order must be 0..n-1 regardless of launch order.
func TestWaitOrdering(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	const n = 200

	s := New()

	var mu sync.Mutex
	got := make([]uint64, 0, n)

	g := sync.Group{}
	for i := 0; i < n; i++ {
		seq := uint64(i)
		g.Go(ctx, func(ctx context.Context) error {
			if err := s.Wait(ctx, seq); err != nil {
				return err
			}
			mu.Lock()
			got = append(got, seq)
			mu.Unlock()
			s.Done(seq)
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestWaitOrdering: got err == %s, want err == nil", err)
	}

	if len(got) != n {
		t.Fatalf("TestWaitOrdering: got %d entries, want %d", len(got), n)
	}
	for i, seq := range got {
		if seq != uint64(i) {
			t.Fatalf("TestWaitOrdering: entry %d == %d, want %d (order not preserved)", i, seq, i)
		}
	}
}

// TestWaitCancel verifies that Wait honors ctx: an already-cancelled ctx returns an error at once
// (the top-of-loop check), and an uncancelled ctx at the current turn returns nil. Only the cancel
// flag differs between the two cases so each pins one input; the blocked-then-cancelled path is
// covered separately by TestWaitCancelWhileBlocked.
func TestWaitCancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// cancel cancels ctx before Wait is called; both cases wait on seq 0, the current turn.
		cancel  bool
		wantErr bool
	}{
		{
			name: "Success: waiting on the current turn returns without error",
		},
		{
			name:    "Error: an already-cancelled ctx returns before entering",
			cancel:  true,
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(t.Context())
		if test.cancel {
			cancel()
		}

		s := New()
		err := s.Wait(ctx, 0)
		cancel()

		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestWaitCancel(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestWaitCancel(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

// TestWaitCancelWhileBlocked covers the ctx.Done() arm of Wait's select: a waiter parked below the
// turn (seq 1, turn 0 never resolved) must wake with ctx.Err() when ctx is cancelled after it has
// blocked. TestWaitCancel's pre-cancelled case cannot reach this arm, as Wait returns at the
// top-of-loop check before ever selecting.
func TestWaitCancelWhileBlocked(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	s := New()

	started := make(chan struct{})
	errc := make(chan error, 1)

	g := sync.Group{}
	g.Go(ctx, func(ctx context.Context) error {
		close(started)
		errc <- s.Wait(ctx, 1)
		return nil
	})

	<-started
	cancel()

	err := <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TestWaitCancelWhileBlocked: got err == %v, want errors.Is(err, context.Canceled)", err)
	}
	_ = g.Wait(t.Context())
}

// TestAbort verifies teardown: every Wait blocked below its turn wakes with ErrAborted, and a Wait
// started after Abort returns ErrAborted at once. The turn is never resolved, so without Abort these
// waiters would block forever.
func TestAbort(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	const n = 50

	s := New()

	// Each waiter records its own Wait error; g.Wait aggregates errors into a composite that
	// errors.Is cannot penetrate, so the per-waiter result is what is checked.
	errs := make([]error, n)
	g := sync.Group{}
	// Sequences 1..n all sit above turn 0, which is never resolved, so every one blocks.
	for i := 1; i <= n; i++ {
		idx := i - 1
		seq := uint64(i)
		g.Go(ctx, func(ctx context.Context) error {
			errs[idx] = s.Wait(ctx, seq)
			return nil
		})
	}

	// Tear down. Abort must wake every parked waiter.
	s.Abort()

	_ = g.Wait(ctx)
	for i, err := range errs {
		if !errors.Is(err, ErrAborted) {
			t.Fatalf("TestAbort: waiter %d got err == %v, want errors.Is(err, ErrAborted)", i+1, err)
		}
	}

	// A Wait after Abort returns ErrAborted immediately rather than blocking.
	if err := s.Wait(ctx, 0); !errors.Is(err, ErrAborted) {
		t.Errorf("TestAbort: post-abort Wait got err == %v, want ErrAborted", err)
	}

	// A Done after Abort is a no-op: an exit-clear that races teardown must neither advance the turn
	// (still 0, never resolved) nor panic.
	if didPanic(func() { s.Done(0) }) {
		t.Errorf("TestAbort: Done after Abort panicked, want no-op")
	}
	if got := s.Turn(); got != 0 {
		t.Errorf("TestAbort: Done after Abort advanced turn to %d, want 0", got)
	}
}

// TestPanicOnMisuse pins the contract violations that must panic: resolving a sequence twice (at the
// turn or buffered ahead) and waiting on a sequence the turn has already passed. A clean sequence of
// distinct operations must not panic, so the caller can tell a real bug from normal use.
func TestPanicOnMisuse(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		name string
		// op exercises one usage pattern; wantPanic says whether it must panic.
		op        func()
		wantPanic bool
	}{
		{
			name: "Success: distinct in-order operations do not panic",
			op: func() {
				s := New()
				s.Done(0)
				s.Done(1)
			},
		},
		{
			name: "Error: resolving the current turn twice panics",
			op: func() {
				s := New()
				s.Done(0)
				s.Done(0)
			},
			wantPanic: true,
		},
		{
			name: "Error: resolving a buffered sequence twice panics",
			op: func() {
				s := New()
				s.Done(2)
				s.Done(2)
			},
			wantPanic: true,
		},
		{
			name: "Error: waiting on a sequence the turn has passed panics",
			op: func() {
				s := New()
				s.Done(0)
				_ = s.Wait(ctx, 0)
			},
			wantPanic: true,
		},
	}

	for _, test := range tests {
		got := didPanic(test.op)
		switch {
		case !got && test.wantPanic:
			t.Errorf("TestPanicOnMisuse(%s): got no panic, want panic", test.name)
		case got && !test.wantPanic:
			t.Errorf("TestPanicOnMisuse(%s): got panic, want no panic", test.name)
		}
	}
}

// didPanic reports whether calling f panicked.
func didPanic(f func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	f()
	return false
}
