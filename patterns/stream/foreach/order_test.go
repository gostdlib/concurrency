package foreach

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/context"
	"github.com/kylelemons/godebug/pretty"
)

// pair is an index/value pair yielded by order.all, used to assert both the indexes and the order of
// output.
type pair struct {
	K int
	V int
}

func TestAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// adds are applied in slice order before all is consumed.
		adds []pair
		// finish finishes the order after the adds; when false the Context is cancelled instead so all
		// ends.
		finish bool
		cancel bool
		want   []pair
	}{
		{
			name:   "Success: values added in index order yield in index order",
			adds:   []pair{{0, 10}, {1, 11}, {2, 12}},
			finish: true,
			want:   []pair{{0, 10}, {1, 11}, {2, 12}},
		},
		{
			name:   "Success: values added out of index order yield in index order",
			adds:   []pair{{2, 12}, {0, 10}, {1, 11}},
			finish: true,
			want:   []pair{{0, 10}, {1, 11}, {2, 12}},
		},
		{
			name:   "Success: an index that was never added is skipped when draining after finish",
			adds:   []pair{{3, 13}, {0, 10}, {2, 12}},
			finish: true,
			want:   []pair{{0, 10}, {2, 12}, {3, 13}},
		},
		{
			name:   "Success: a cancelled Context drains what finish left instead of truncating",
			adds:   []pair{{0, 10}},
			finish: true,
			cancel: true,
			want:   []pair{{0, 10}},
		},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(t.Context())

		ord := newOrder[int]()

		for _, p := range test.adds {
			ord.add(p.K, p.V)
		}
		if test.finish {
			ord.finish()
		}
		if test.cancel {
			cancel()
		}

		var got []pair
		for k, v := range ord.all(ctx) {
			got = append(got, pair{k, v})
		}
		cancel()

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestAll(%s): yielded pairs: -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestAllCancelDrains proves cancellation does not truncate: all blocks until finish arrives (as the
// dispatcher guarantees after its in-flight work adds) and then drains every held value in order.
func TestAllCancelDrains(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ord := newOrder[int]()
	ord.add(1, 11)
	ord.add(0, 10)

	cancel()
	// finish arrives late, the way the dispatcher's finish follows g.Wait; all must wait for it.
	context.Pool(t.Context()).Submit(t.Context(), func() {
		time.Sleep(50 * time.Millisecond)
		ord.finish()
	})

	var got []pair
	for k, v := range ord.all(ctx) {
		got = append(got, pair{k, v})
	}

	if diff := pretty.Compare([]pair{{0, 10}, {1, 11}}, got); diff != "" {
		t.Errorf("TestAllCancelDrains: yielded pairs: -want/+got:\n%s", diff)
	}
}

// TestAllStreaming proves all streams each value as soon as its index is next instead of waiting for
// finish: every value is added only after all has yielded the previous one.
func TestAllStreaming(t *testing.T) {
	t.Parallel()

	const total = 100

	ord := newOrder[int]()

	want := make([]int, 0, total)
	for i := 0; i < total; i++ {
		want = append(want, i)
	}

	ord.add(0, 0)
	got := make([]int, 0, total)
	for k, v := range ord.all(t.Context()) {
		got = append(got, v)
		next := k + 1
		if next < total {
			ord.add(next, next)
			continue
		}
		ord.finish()
	}

	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestAllStreaming: yielded values: -want/+got:\n%s", diff)
	}
}

// TestAddConcurrent has multiple goroutines calling add while a single consumer ranges all, verifying
// every pair arrives and output stays in index order.
func TestAddConcurrent(t *testing.T) {
	t.Parallel()

	const workers = 8
	const perWorker = 1000
	const total = workers * perWorker

	ctx := t.Context()

	ord := newOrder[int]()

	p := context.Pool(ctx)
	g := p.Group()
	for w := 0; w < workers; w++ {
		g.Go(ctx, func(ctx context.Context) error {
			for i := 0; i < perWorker; i++ {
				k := w*perWorker + i
				ord.add(k, k*3)
			}
			return nil
		})
	}
	waitErr := make(chan error, 1)
	p.Submit(ctx, func() {
		defer ord.finish()
		waitErr <- g.Wait(ctx)
	})

	want := make([]pair, total)
	for i := 0; i < total; i++ {
		want[i] = pair{i, i * 3}
	}

	got := make([]pair, 0, total)
	for k, v := range ord.all(ctx) {
		got = append(got, pair{k, v})
	}

	if err := <-waitErr; err != nil {
		t.Fatalf("TestAddConcurrent: adders had unexpected error: %v", err)
	}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestAddConcurrent: yielded pairs: -want/+got:\n%s", diff)
	}
}

// TestAllPanicsOnSecondConsumer verifies ranging all while another consumer is mid-iteration panics,
// and that ranging again after an iteration has ended does not.
func TestAllPanicsOnSecondConsumer(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	ord := newOrder[int]()
	ord.add(0, 0)

	ready := make(chan struct{})
	release := make(chan struct{})
	g := context.Pool(ctx).Group()
	g.Go(ctx, func(ctx context.Context) error {
		for range ord.all(ctx) {
			ready <- struct{}{}
			<-release
		}
		return nil
	})

	<-ready // The first consumer is now mid-iteration.

	rangeAll := func() (panicked bool) {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		for range ord.all(ctx) {
		}
		return false
	}

	if !rangeAll() {
		t.Errorf("TestAllPanicsOnSecondConsumer: got no panic from a second concurrent all(), want panic")
	}

	close(release)
	ord.finish()
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestAllPanicsOnSecondConsumer: first consumer had unexpected error: %v", err)
	}

	// Success case: once the first iteration has ended, all can be ranged again.
	if rangeAll() {
		t.Errorf("TestAllPanicsOnSecondConsumer: got panic ranging all() after the first iteration ended, want no panic")
	}
}

func TestAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		finishFirst bool
		wantPanic   bool
	}{
		{
			name: "Success: add on an open order",
		},
		{
			name:        "Error: add after finish panics",
			finishFirst: true,
			wantPanic:   true,
		},
	}

	for _, test := range tests {
		ord := newOrder[int]()
		if test.finishFirst {
			ord.finish()
		}

		panicked := func() (p bool) {
			defer func() {
				if recover() != nil {
					p = true
				}
			}()
			ord.add(0, 1)
			return false
		}()

		switch {
		case panicked && !test.wantPanic:
			t.Errorf("TestAdd(%s): got panic, want no panic", test.name)
		case !panicked && test.wantPanic:
			t.Errorf("TestAdd(%s): got no panic, want panic", test.name)
		}
	}
}

// TestFinishAddRace is a -race tripwire for the add/finish linearization: several goroutines hammer
// add on a fresh order while another finishes it concurrently. A panic is an acceptable outcome (add
// after finish is a documented bug), but the invariant it pins is that no non-panicking add is
// silently lost — whatever an add wrote without panicking must be present when all drains after
// finish. Must stay clean and stable under -race at -count=20.
func TestFinishAddRace(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	p := context.Pool(ctx)

	const rounds = 1000
	const adders = 4

	for r := 0; r < rounds; r++ {
		ord := newOrder[int]()

		// landed[a] is set only when adder a's add returned without panicking.
		landed := make([]atomic.Bool, adders)
		g := p.Group()
		for a := 0; a < adders; a++ {
			g.Go(ctx, func(ctx context.Context) error {
				defer func() { recover() }() // add after finish panics; that is an acceptable outcome.
				ord.add(a, a*10)
				landed[a].Store(true) // Only reached when add did not panic.
				return nil
			})
		}
		finishDone := make(chan struct{})
		p.Submit(ctx, func() {
			defer close(finishDone)
			ord.finish() // Races the adders.
		})

		if err := g.Wait(ctx); err != nil {
			t.Fatalf("TestFinishAddRace: round %d adders returned err %v, want nil", r, err)
		}
		<-finishDone

		// all runs strictly after every add and finish completed, so any landed add's buf write
		// happens-before this drain and must appear.
		got := map[int]int{}
		for k, v := range ord.all(ctx) {
			got[k] = v
		}
		for a := 0; a < adders; a++ {
			if !landed[a].Load() {
				continue // add panicked racing finish; losing its value is acceptable.
			}
			switch v, ok := got[a]; {
			case !ok:
				t.Fatalf("TestFinishAddRace: round %d adder %d landed without panic but its value is absent after finish", r, a)
			case v != a*10:
				t.Fatalf("TestFinishAddRace: round %d adder %d: got value %d, want %d", r, a, v, a*10)
			}
		}
	}
}

// TestWaitBelow pins the dispatcher-side backpressure deterministically: waitBelow blocks at the held
// bound and each trigger unblocks it with the expected result.
func TestWaitBelow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// trigger unblocks a waitBelow parked at the bound.
		trigger func(ctx context.Context, cancel context.CancelFunc, ord *order[int])
		wantErr bool
	}{
		{
			name: "Success: all removing a value below the bound unblocks with nil",
			trigger: func(ctx context.Context, _ context.CancelFunc, ord *order[int]) {
				for range ord.all(ctx) {
					break // Remove a single value.
				}
			},
		},
		{
			name: "Error: finish unblocks with an error so dispatch stops instead of hanging",
			trigger: func(_ context.Context, _ context.CancelFunc, ord *order[int]) {
				ord.finish()
			},
			wantErr: true,
		},
		{
			name: "Error: ctx cancellation unblocks with the ctx error",
			trigger: func(_ context.Context, cancel context.CancelFunc, _ *order[int]) {
				cancel()
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(t.Context())

		ord := newOrder[int]()
		ord.add(0, 100)
		ord.add(1, 101)

		waitErr := make(chan error, 1)
		context.Pool(ctx).Submit(t.Context(), func() {
			waitErr <- ord.waitBelow(ctx, 2)
		})

		select {
		case err := <-waitErr:
			t.Fatalf("TestWaitBelow(%s): waitBelow returned %v while at the bound, want it to block", test.name, err)
		case <-time.After(50 * time.Millisecond):
		}

		test.trigger(ctx, cancel, ord)

		err := <-waitErr
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestWaitBelow(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestWaitBelow(%s): got err == %s, want err == nil", test.name, err)
		}
		cancel()
	}
}
