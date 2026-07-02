package foreach

import (
	"fmt"
	"iter"
	"sort"
	"testing"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/kylelemons/godebug/pretty"
)

// seqOf builds an iter.Seq2 keyed by index from vals, mirroring what stream.Slice produces without
// coupling this test to the stream package.
func seqOf(vals ...int) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for i, v := range vals {
			if !yield(i, v) {
				return
			}
		}
	}
}

func TestItem(t *testing.T) {
	t.Parallel()

	const never = -1

	tests := []struct {
		name string
		// seq is the input; nil exercises the no-op path.
		seq iter.Seq2[int, int]
		// errOn is the value fn returns an error for; never means fn always succeeds.
		errOn int
		opts  []Option
		// checkSeen guards wantSeen: with WithStopOnErr the set of processed values is
		// non-deterministic, so it is not asserted.
		checkSeen bool
		wantSeen  []int
		wantErr   bool
	}{
		{
			name:      "Success: applies fn to every value",
			seq:       seqOf(1, 2, 3, 4),
			errOn:     never,
			checkSeen: true,
			wantSeen:  []int{1, 2, 3, 4},
		},
		{
			name:      "Success: nil sequence is a no-op",
			seq:       nil,
			errOn:     never,
			checkSeen: true,
			wantSeen:  nil,
		},
		{
			name:      "Error: fn error is returned and the other values still process",
			seq:       seqOf(1, 2, 3, 4),
			errOn:     2,
			checkSeen: true,
			wantSeen:  []int{1, 3, 4},
			wantErr:   true,
		},
		{
			name:    "Error: fn error with WithStopOnErr is returned",
			seq:     seqOf(1, 2, 3, 4),
			errOn:   2,
			opts:    []Option{WithStopOnErr()},
			wantErr: true,
		},
	}

	for _, test := range tests {
		var mu sync.Mutex
		var seen []int

		fn := func(ctx context.Context, _ int, v int) error {
			if v == test.errOn {
				return fmt.Errorf("boom on %d", v)
			}
			mu.Lock()
			seen = append(seen, v)
			mu.Unlock()
			return nil
		}

		err := Item(t.Context(), test.seq, fn, test.opts...)

		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestItem(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestItem(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		if !test.checkSeen {
			continue
		}

		sort.Ints(seen)
		if diff := pretty.Compare(test.wantSeen, seen); diff != "" {
			t.Errorf("TestItem(%s): processed values: -want/+got:\n%s", test.name, diff)
		}
	}
}

// pair is a key/value pair yielded by Order.All, used to assert both the keys and the order of output.
type pair struct {
	K int
	V int
}

func TestAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// adds are applied in slice order before All is consumed.
		adds []pair
		// close closes the Order after the adds; when false the Context is cancelled instead so All ends.
		close  bool
		cancel bool
		want   []pair
	}{
		{
			name:  "Success: values added in key order yield in key order",
			adds:  []pair{{0, 10}, {1, 11}, {2, 12}},
			close: true,
			want:  []pair{{0, 10}, {1, 11}, {2, 12}},
		},
		{
			name:  "Success: values added out of key order yield in key order",
			adds:  []pair{{2, 12}, {0, 10}, {1, 11}},
			close: true,
			want:  []pair{{0, 10}, {1, 11}, {2, 12}},
		},
		{
			name:  "Success: a key that was never added is skipped when draining after Close",
			adds:  []pair{{3, 13}, {0, 10}, {2, 12}},
			close: true,
			want:  []pair{{0, 10}, {2, 12}, {3, 13}},
		},
		{
			name:   "Success: a cancelled Context stops the iteration without Close",
			adds:   []pair{{0, 10}},
			cancel: true,
			want:   nil,
		},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(t.Context())

		ord, err := New[int](ctx)
		if err != nil {
			t.Fatalf("TestAll(%s): New() had unexpected error: %v", test.name, err)
		}

		for _, p := range test.adds {
			ord.Add(ctx, p.K, p.V)
		}
		if test.close {
			ord.Close()
		}
		if test.cancel {
			cancel()
		}

		var got []pair
		for k, v := range ord.All(ctx) {
			got = append(got, pair{k, v})
		}
		cancel()

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestAll(%s): yielded pairs: -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestAllStreaming proves All streams each value as soon as its key is next instead of waiting for
// Close: every value is Added only after All has yielded the previous one.
func TestAllStreaming(t *testing.T) {
	t.Parallel()

	const total = 100

	ord, err := New[int](t.Context())
	if err != nil {
		t.Fatalf("TestAllStreaming: New() had unexpected error: %v", err)
	}

	want := make([]int, 0, total)
	for i := 0; i < total; i++ {
		want = append(want, i)
	}

	ord.Add(t.Context(), 0, 0)
	got := make([]int, 0, total)
	for k, v := range ord.All(t.Context()) {
		got = append(got, v)
		next := k + 1
		if next < total {
			ord.Add(t.Context(), next, next)
			continue
		}
		ord.Close()
	}

	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestAllStreaming: yielded values: -want/+got:\n%s", diff)
	}
}

// TestAllConcurrent runs Order under Item the way fan's ordered mode worked: parallel Funcs Add results
// keyed by input index while the consumer ranges All concurrently.
func TestAllConcurrent(t *testing.T) {
	t.Parallel()

	const total = 10000

	ctx := t.Context()

	ord, err := New[int](ctx)
	if err != nil {
		t.Fatalf("TestAllConcurrent: New() had unexpected error: %v", err)
	}

	vals := make([]int, total)
	want := make([]pair, total)
	for i := 0; i < total; i++ {
		vals[i] = i
		want[i] = pair{i, i * 2}
	}

	fn := func(ctx context.Context, k int, v int) error {
		ord.Add(ctx, k, v*2)
		return nil
	}

	itemErr := make(chan error, 1)
	context.Pool(ctx).Submit(ctx, func() {
		defer ord.Close()
		itemErr <- Item(ctx, seqOf(vals...), fn)
	})

	got := make([]pair, 0, total)
	for k, v := range ord.All(ctx) {
		got = append(got, pair{k, v})
	}

	if err := <-itemErr; err != nil {
		t.Fatalf("TestAllConcurrent: Item() had unexpected error: %v", err)
	}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestAllConcurrent: yielded pairs: -want/+got:\n%s", diff)
	}
}

// TestAddConcurrent has multiple goroutines calling Add directly (not through Item) while a single
// consumer ranges All, verifying every pair arrives and output stays in key order.
func TestAddConcurrent(t *testing.T) {
	t.Parallel()

	const workers = 8
	const perWorker = 1000
	const total = workers * perWorker

	ctx := t.Context()

	ord, err := New[int](ctx)
	if err != nil {
		t.Fatalf("TestAddConcurrent: New() had unexpected error: %v", err)
	}

	p := context.Pool(ctx)
	g := p.Group()
	for w := 0; w < workers; w++ {
		g.Go(ctx, func(ctx context.Context) error {
			for i := 0; i < perWorker; i++ {
				k := w*perWorker + i
				ord.Add(ctx, k, k*3)
			}
			return nil
		})
	}
	waitErr := make(chan error, 1)
	p.Submit(ctx, func() {
		defer ord.Close()
		waitErr <- g.Wait(ctx)
	})

	want := make([]pair, total)
	for i := 0; i < total; i++ {
		want[i] = pair{i, i * 3}
	}

	got := make([]pair, 0, total)
	for k, v := range ord.All(ctx) {
		got = append(got, pair{k, v})
	}

	if err := <-waitErr; err != nil {
		t.Fatalf("TestAddConcurrent: adders had unexpected error: %v", err)
	}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestAddConcurrent: yielded pairs: -want/+got:\n%s", diff)
	}
}

// TestAllPanicsOnSecondConsumer verifies ranging All while another consumer is mid-iteration panics,
// and that ranging again after an iteration has ended does not.
func TestAllPanicsOnSecondConsumer(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	ord, err := New[int](ctx)
	if err != nil {
		t.Fatalf("TestAllPanicsOnSecondConsumer: New() had unexpected error: %v", err)
	}
	ord.Add(ctx, 0, 0)

	ready := make(chan struct{})
	release := make(chan struct{})
	g := context.Pool(ctx).Group()
	g.Go(ctx, func(ctx context.Context) error {
		for range ord.All(ctx) {
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
		for range ord.All(ctx) {
		}
		return false
	}

	if !rangeAll() {
		t.Errorf("TestAllPanicsOnSecondConsumer: got no panic from a second concurrent All(), want panic")
	}

	close(release)
	ord.Close()
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestAllPanicsOnSecondConsumer: first consumer had unexpected error: %v", err)
	}

	// Success case: once the first iteration has ended, All can be ranged again.
	if rangeAll() {
		t.Errorf("TestAllPanicsOnSecondConsumer: got panic ranging All() after the first iteration ended, want no panic")
	}
}

func TestAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		k          int
		closeFirst bool
		wantPanic  bool
	}{
		{
			name: "Success: valid key on an open Order",
			k:    0,
		},
		{
			name:      "Error: negative key panics",
			k:         -1,
			wantPanic: true,
		},
		{
			name:       "Error: Add after Close panics",
			k:          0,
			closeFirst: true,
			wantPanic:  true,
		},
	}

	for _, test := range tests {
		ord, err := New[int](t.Context())
		if err != nil {
			t.Fatalf("TestAdd(%s): New() had unexpected error: %v", test.name, err)
		}
		if test.closeFirst {
			ord.Close()
		}

		panicked := func() (p bool) {
			defer func() {
				if recover() != nil {
					p = true
				}
			}()
			ord.Add(t.Context(), test.k, 1)
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
