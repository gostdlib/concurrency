package foreach

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/kylelemons/godebug/pretty"
)

// seqOf adapts vals into an index-keyed iter.Seq2, mirroring stream.Slice without coupling this test
// to the stream package.
func seqOf(vals ...int) iter.Seq2[int, int] {
	return slices.All(vals)
}

// ints returns the slice [0, 1, ..., n-1].
func ints(n int) []int {
	vals := make([]int, n)
	for i := 0; i < n; i++ {
		vals[i] = i
	}
	return vals
}

// testBoff returns a sleepless backoff so retries in tests run instantly.
func testBoff() *exponential.Backoff {
	return exponential.Must(exponential.New(exponential.WithTesting()))
}

func TestItem(t *testing.T) {
	t.Parallel()

	const never = -1

	boff := testBoff()

	tests := []struct {
		name string
		// seq is the input; nil exercises the no-op path.
		seq iter.Seq2[int, int]
		// errOn is the value fn returns an error for; never means fn always succeeds.
		errOn int
		opts  []Option
		// wantVals are the successful response values, compared as a sorted set (completion order is
		// nondeterministic). wantErrs is the number of responses that must carry an Err.
		wantVals []int
		wantErrs int
		// wantPermanent asserts every error response wraps ErrPermanent (the invalid-option shape).
		wantPermanent bool
		// exact guards wantVals/wantErrs: with WithStopOnErr the yielded set is nondeterministic, so
		// only "at least one error response arrived" is asserted.
		exact bool
	}{
		{
			name:     "Success: applies fn to every value and yields each result",
			seq:      seqOf(1, 2, 3, 4),
			errOn:    never,
			wantVals: []int{2, 4, 6, 8},
			exact:    true,
		},
		{
			name:  "Success: an empty sequence yields nothing",
			seq:   seqOf(),
			errOn: never,
			exact: true,
		},
		{
			name:     "Error: fn error for one value arrives as Response.Err and other values still process",
			seq:      seqOf(1, 2, 3, 4),
			errOn:    2,
			wantVals: []int{2, 6, 8},
			wantErrs: 1,
			exact:    true,
		},
		{
			name:  "Error: fn error with WithStopOnErr ends the iteration promptly",
			seq:   seqOf(1, 2, 3, 4),
			errOn: 2,
			opts:  []Option{WithStopOnErr()},
		},
		{
			name:          "Error: an invalid option yields a single Response wrapping ErrPermanent",
			seq:           seqOf(1, 2, 3),
			errOn:         never,
			opts:          []Option{WithMaxHeld(0)},
			wantErrs:      1,
			wantPermanent: true,
			exact:         true,
		},
		{
			name:     "Success: a WithMaxHeld of 1 with ordered delivery still processes every value",
			seq:      seqOf(1, 2, 3),
			errOn:    never,
			opts:     []Option{WithOrdered(), WithMaxHeld(1)},
			wantVals: []int{2, 4, 6},
			exact:    true,
		},
		{
			name:     "Success: a WithGate backoff that never engages does not block dispatch",
			seq:      seqOf(1, 2, 3),
			errOn:    never,
			opts:     []Option{WithGate(boff)},
			wantVals: []int{2, 4, 6},
			exact:    true,
		},
		{
			name:          "Error: a nil backoff yields a single Response wrapping ErrPermanent",
			seq:           seqOf(1, 2, 3),
			errOn:         never,
			opts:          []Option{WithGate(nil)},
			wantErrs:      1,
			wantPermanent: true,
			exact:         true,
		},
	}

	for _, test := range tests {
		fn := func(ctx context.Context, _ int, v int) (int, error) {
			if v == test.errOn {
				return 0, fmt.Errorf("boom on %d", v)
			}
			return v * 2, nil
		}

		var vals []int
		errCount := 0
		for _, resp := range Item(t.Context(), test.seq, fn, test.opts...) {
			if resp.Err != nil {
				errCount++
				if test.wantPermanent && !errors.Is(resp.Err, ErrPermanent) {
					t.Errorf("TestItem(%s): got Response.Err that does not wrap ErrPermanent: %v", test.name, resp.Err)
				}
				continue
			}
			vals = append(vals, resp.V)
		}

		if !test.exact {
			if errCount == 0 {
				t.Errorf("TestItem(%s): got no error responses, want at least one", test.name)
			}
			continue
		}

		sort.Ints(vals)
		if diff := pretty.Compare(test.wantVals, vals); diff != "" {
			t.Errorf("TestItem(%s): successful values: -want/+got:\n%s", test.name, diff)
		}
		if errCount != test.wantErrs {
			t.Errorf("TestItem(%s): got %d error responses, want %d", test.name, errCount, test.wantErrs)
		}
	}
}

// TestItemPanicsOnNilArg pins that Item panics at call time on a nil in or fn (missing required
// arguments), and does not panic when both are supplied.
func TestItemPanicsOnNilArg(t *testing.T) {
	t.Parallel()

	fn := func(context.Context, int, int) (int, error) { return 0, nil }

	tests := []struct {
		name      string
		nilSeq    bool
		nilFn     bool
		wantPanic bool
	}{
		{
			name: "Success: valid in and fn do not panic",
		},
		{
			name:      "Error: nil in panics",
			nilSeq:    true,
			wantPanic: true,
		},
		{
			name:      "Error: nil fn panics",
			nilFn:     true,
			wantPanic: true,
		},
	}

	for _, test := range tests {
		seq := seqOf(1, 2, 3)
		if test.nilSeq {
			seq = nil
		}
		itemFn := fn
		if test.nilFn {
			itemFn = nil
		}

		panicked := func() (p bool) {
			defer func() {
				if recover() != nil {
					p = true
				}
			}()
			// Item panics at call time (before the lazy iterator is returned), so no range is needed.
			Item(t.Context(), seq, itemFn)
			return false
		}()

		switch {
		case panicked && !test.wantPanic:
			t.Errorf("TestItemPanicsOnNilArg(%s): got panic, want no panic", test.name)
		case !panicked && test.wantPanic:
			t.Errorf("TestItemPanicsOnNilArg(%s): got no panic, want panic", test.name)
		}
	}
}

func TestWithOrdered(t *testing.T) {
	t.Parallel()

	const never = -1

	big := ints(10000)

	tests := []struct {
		name string
		vals []int
		// errOn is the key whose fn errors; never means fn always succeeds.
		errOn int
		opts  []Option
	}{
		{
			name:  "Success: responses yield in input order",
			vals:  []int{10, 11, 12, 13, 14},
			errOn: never,
			opts:  []Option{WithOrdered()},
		},
		{
			name:  "Success: 10000 values under a small held bound yield in input order",
			vals:  big,
			errOn: never,
			opts:  []Option{WithOrdered(), WithMaxHeld(4)},
		},
		{
			name:  "Error: an fn error yields in position as Response.Err while every other key succeeds",
			vals:  big[:50],
			errOn: 3,
			opts:  []Option{WithOrdered(), WithMaxHeld(2)},
		},
	}

	for _, test := range tests {
		fn := func(ctx context.Context, k int, v int) (int, error) {
			if k == test.errOn {
				return 0, fmt.Errorf("boom on key %d", k)
			}
			return v * 2, nil
		}

		var gotKeys []int
		var errKeys []int
		gotVals := map[int]int{}
		for k, resp := range Item(t.Context(), seqOf(test.vals...), fn, test.opts...) {
			gotKeys = append(gotKeys, k)
			if resp.Err != nil {
				errKeys = append(errKeys, k)
				continue
			}
			gotVals[k] = resp.V
		}

		wantKeys := make([]int, len(test.vals))
		for i := 0; i < len(test.vals); i++ {
			wantKeys[i] = i
		}
		if diff := pretty.Compare(wantKeys, gotKeys); diff != "" {
			t.Errorf("TestWithOrdered(%s): yielded keys: -want/+got:\n%s", test.name, diff)
		}

		var wantErrKeys []int
		if test.errOn != never {
			wantErrKeys = []int{test.errOn}
		}
		if diff := pretty.Compare(wantErrKeys, errKeys); diff != "" {
			t.Errorf("TestWithOrdered(%s): error keys: -want/+got:\n%s", test.name, diff)
		}

		for k, v := range gotVals {
			if want := test.vals[k] * 2; v != want {
				t.Errorf("TestWithOrdered(%s): value for key %d: got %d, want %d", test.name, k, v, want)
			}
		}
	}
}

// TestWithOrderedKeys proves ordered mode works with non-int keys: the engine orders by dispatch
// index, not by the key type.
func TestWithOrderedKeys(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c", "d"}
	var seq iter.Seq2[string, int] = func(yield func(string, int) bool) {
		for i, k := range keys {
			if !yield(k, i) {
				return
			}
		}
	}

	fn := func(_ context.Context, k string, v int) (string, error) {
		return fmt.Sprintf("%s%d", k, v), nil
	}

	var gotKeys []string
	var gotVals []string
	for k, resp := range Item(t.Context(), seq, fn, WithOrdered()) {
		if resp.Err != nil {
			t.Fatalf("TestWithOrderedKeys: got unexpected Response.Err: %v", resp.Err)
		}
		gotKeys = append(gotKeys, k)
		gotVals = append(gotVals, resp.V)
	}

	if diff := pretty.Compare(keys, gotKeys); diff != "" {
		t.Errorf("TestWithOrderedKeys: yielded keys: -want/+got:\n%s", diff)
	}
	if diff := pretty.Compare([]string{"a0", "b1", "c2", "d3"}, gotVals); diff != "" {
		t.Errorf("TestWithOrderedKeys: yielded values: -want/+got:\n%s", diff)
	}
}

// TestItemLimitedPool proves Item completes on a pool with a single worker slot: the coordinators run
// as background tasks, so they cannot starve the ItemFuncs of pool slots (this deadlocked when the
// dispatcher occupied a slot on the same pool).
func TestItemLimitedPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []Option
	}{
		{
			name: "Success: completion order completes with a single-slot pool",
		},
		{
			name: "Success: input order completes with a single-slot pool",
			opts: []Option{WithOrdered()},
		},
	}

	for _, test := range tests {
		ctx := context.SetPool(t.Context(), context.Pool(t.Context()).Limited(t.Context(), "TestItemLimitedPool", 1))

		fn := func(_ context.Context, _ int, v int) (int, error) {
			return v * 2, nil
		}

		var vals []int
		for _, resp := range Item(ctx, seqOf(1, 2, 3, 4), fn, test.opts...) {
			if resp.Err != nil {
				t.Fatalf("TestItemLimitedPool(%s): got unexpected Response.Err: %v", test.name, resp.Err)
			}
			vals = append(vals, resp.V)
		}

		sort.Ints(vals)
		if diff := pretty.Compare([]int{2, 4, 6, 8}, vals); diff != "" {
			t.Errorf("TestItemLimitedPool(%s): yielded values: -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestItemSaturatedPool proves cancellation unblocks an Item whose dispatch is parked waiting for a
// slot on a saturated shared Limited pool: the single slot is held by an external Submit for the whole
// test, so the dispatcher can only park in the slot acquire. Cancelling ctx must return the range
// promptly (this hung before the acquire was made cancellable), and the dispatched-but-never-ran pair
// must still yield a cancellation-error Response (the ledger sweep).
func TestItemSaturatedPool(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Hold the single slot of a Limited(1) pool for the whole test with an external Submit, so any
	// dispatch by Item must park in the slot acquire.
	limited := context.Pool(t.Context()).Limited(t.Context(), "TestItemSaturatedPool", 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	held := make(chan struct{})
	limited.Submit(t.Context(), func() {
		close(held)
		<-release
	})
	<-held // The external blocker now owns the only slot.

	ctx = context.SetPool(ctx, limited)

	fn := func(_ context.Context, _ int, v int) (int, error) {
		return v * 2, nil
	}

	// Cancel after the dispatcher has had time to park in the acquire.
	context.Pool(t.Context()).Submit(t.Context(), func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	})

	type result struct {
		resps  int
		errs   int
		cancel int
	}
	got := make(chan result, 1)
	context.Pool(t.Context()).Submit(t.Context(), func() {
		var r result
		for _, resp := range Item(ctx, seqOf(1, 2), fn) {
			r.resps++
			if resp.Err != nil {
				r.errs++
				if errors.Is(resp.Err, context.Canceled) {
					r.cancel++
				}
			}
		}
		got <- r
	})

	select {
	case r := <-got:
		// The dispatched pair could not run, so it must arrive as a cancellation-error Response.
		if r.errs == 0 {
			t.Errorf("TestItemSaturatedPool: got %d error responses, want at least one (the dispatched pair cut short)", r.errs)
		}
		if r.errs != r.cancel {
			t.Errorf("TestItemSaturatedPool: got %d error responses, %d wrapping context.Canceled, want them all cancellation errors", r.errs, r.cancel)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("TestItemSaturatedPool: got a hung range, want a prompt return after cancel (the slot acquire must be cancellable)")
	}
}

// TestWithStopOnErr pins the ordered StopOnErr contract: cancellation must not truncate what was
// already delivered (Group.Wait fires CancelOnErr even on success), and the error response itself must
// always yield, in position.
func TestWithStopOnErr(t *testing.T) {
	t.Parallel()

	const never = -1

	big := ints(2000)

	tests := []struct {
		name string
		vals []int
		// errOn is the key whose fn errors; never means fn always succeeds.
		errOn int
	}{
		{
			name:  "Success: no errors yields every response despite the unconditional end-of-run cancel",
			vals:  big,
			errOn: never,
		},
		{
			name:  "Error: the error response yields in position and earlier keys are complete",
			vals:  big[:500],
			errOn: 50,
		},
	}

	for _, test := range tests {
		fn := func(ctx context.Context, k int, v int) (int, error) {
			if k == test.errOn {
				return 0, fmt.Errorf("boom on key %d", k)
			}
			return v * 2, nil
		}

		var gotKeys []int
		var errKeys []int
		for k, resp := range Item(t.Context(), seqOf(test.vals...), fn, WithOrdered(), WithStopOnErr(), WithMaxHeld(8)) {
			gotKeys = append(gotKeys, k)
			if resp.Err != nil {
				errKeys = append(errKeys, k)
			}
		}

		// Ordered yielding means the keys must be the contiguous prefix of dispatched work.
		for i, k := range gotKeys {
			if k != i {
				t.Errorf("TestWithStopOnErr(%s): got key %d at position %d, want contiguous keys", test.name, k, i)
				break
			}
		}

		if test.errOn == never {
			if len(gotKeys) != len(test.vals) {
				t.Errorf("TestWithStopOnErr(%s): got %d responses, want %d after a successful run", test.name, len(gotKeys), len(test.vals))
			}
			if len(errKeys) != 0 {
				t.Errorf("TestWithStopOnErr(%s): got error responses on keys %v, want none", test.name, errKeys)
			}
			continue
		}

		// Pairs dispatched but cancelled by the stop also yield error responses, so the fn error's key
		// must be present but need not be alone.
		if !slices.Contains(errKeys, test.errOn) {
			t.Errorf("TestWithStopOnErr(%s): error keys %v do not include key %d", test.name, errKeys, test.errOn)
		}
		if len(gotKeys) <= test.errOn {
			t.Errorf("TestWithStopOnErr(%s): got %d responses, want the error response at key %d included", test.name, len(gotKeys), test.errOn)
		}
	}
}

// TestStopOnErrPromptCancel pins the promptness of WithStopOnErr in unordered mode: the first error
// must cancel dispatch the moment it is known, before that pair's Response is delivered. With a tiny
// delivery buffer (WithMaxHeld(1)) behind a consumer blocked in the range body, the errored worker's
// own delivery blocks; deferring the cancel until the worker returns (the Group fires CancelOnErr only
// after the fn returns) would let the dispatcher keep filling the pool while that worker is stuck, so
// the invocation count would climb to pool saturation instead of freezing near the error. A Limited
// pool makes saturation deterministic: before the fix the count freezes at the pool limit, after it at
// a handful. Releasing the consumer must then drain every dispatched pair and end the iteration,
// proving the early cancel does not deadlock the erroring worker's delivery or the join.
func TestStopOnErrPromptCancel(t *testing.T) {
	// Not t.Parallel: this is a timing/promptness measurement (a settle sleep then a frozen-count read),
	// so running many copies concurrently under -race would contend for the scheduler and blur the
	// measurement. Sequential runs keep the count deterministic.

	const poolLimit = 50
	limited := context.Pool(t.Context()).Limited(t.Context(), "TestStopOnErrPromptCancel", poolLimit)
	ctx := context.SetPool(t.Context(), limited)

	const total = 10000
	const errKey = 5

	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)

	var calls atomic.Int64
	fn := func(_ context.Context, k int, v int) (int, error) {
		if k == errKey {
			return 0, fmt.Errorf("boom on key %d", k)
		}
		calls.Add(1)
		return v, nil
	}

	// The consumer runs on the default pool (not the Limited one, which is reserved for the ItemFuncs)
	// and blocks on the first response so the delivery buffer stays full behind it.
	itemDone := make(chan struct{})
	context.Pool(t.Context()).Submit(t.Context(), func() {
		defer close(itemDone)
		blocked := false
		for range Item(ctx, seqOf(ints(total)...), fn, WithStopOnErr(), WithMaxHeld(1)) {
			if !blocked {
				blocked = true
				<-release
			}
		}
	})

	// Let dispatch either saturate the pool (unfixed) or stop at the error (fixed), then confirm the
	// count is frozen and far below pool saturation while the consumer is still blocked.
	time.Sleep(200 * time.Millisecond)
	frozen := calls.Load()
	time.Sleep(200 * time.Millisecond)
	switch n := calls.Load(); {
	case n != frozen:
		t.Errorf("TestStopOnErrPromptCancel: got %d fn calls after settling, want the count frozen at %d", n, frozen)
	case n >= poolLimit/2:
		t.Errorf("TestStopOnErrPromptCancel: got %d fn calls frozen while the consumer blocked, want far fewer than pool saturation (%d) — the first error must cancel dispatch before delivering", n, poolLimit)
	}

	// Releasing the consumer must drain every dispatched pair and end the iteration: the erroring
	// worker's blocked delivery unblocks, the join completes, and the range returns.
	releaseOnce()
	select {
	case <-itemDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("TestStopOnErrPromptCancel: got a hung range after releasing the consumer, want the drain and join to terminate")
	}
}

// TestItemBreakBlockedSeq proves breaking out of the range returns promptly even when the input
// sequence is parked on an idle channel: the join waits only for dispatched work, not for the puller,
// which unwinds when the sequence's own Context ends.
func TestItemBreakBlockedSeq(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// One value is buffered; after it, the sequence parks on the idle channel like stream.Chan would.
	ch := make(chan int, 1)
	ch <- 42
	var seq iter.Seq2[int, int] = func(yield func(int, int) bool) {
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				if !yield(i, v) {
					return
				}
				i++
			}
		}
	}

	fn := func(_ context.Context, _ int, v int) (int, error) {
		return v, nil
	}

	start := time.Now()
	for _, resp := range Item(ctx, seq, fn) {
		if resp.Err != nil {
			t.Fatalf("TestItemBreakBlockedSeq: got unexpected Response.Err: %v", resp.Err)
		}
		break
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("TestItemBreakBlockedSeq: got a %v break, want a prompt return (the join must not wait on the parked sequence)", elapsed)
	}
}

// TestItemContextCancel proves cancelling the Context ends an ordered iteration whose dispatcher is
// paused at the held bound instead of hanging.
func TestItemContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const total = 100
	vals := ints(total)

	fn := func(_ context.Context, _ int, v int) (int, error) {
		return v, nil
	}

	got := 0
	for range Item(ctx, seqOf(vals...), fn, WithOrdered(), WithMaxHeld(1)) {
		got++
		cancel()
	}

	// Reaching here is the main assertion: cancellation ended the iteration.
	if got >= total {
		t.Errorf("TestItemContextCancel: got %d responses after cancelling on the first, want fewer than %d", got, total)
	}
}

// TestItemEarlyBreak proves breaking out of the range unwinds dispatch: with the consumer gone, fn
// stops being invoked instead of processing the whole sequence.
func TestItemEarlyBreak(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	const total = 10000
	vals := ints(total)

	var calls atomic.Int64
	fn := func(_ context.Context, _ int, v int) (int, error) {
		calls.Add(1)
		return v, nil
	}

	got := 0
	for range Item(ctx, seqOf(vals...), fn, WithMaxHeld(2)) {
		got++
		if got == 3 {
			break
		}
	}

	// The join means every dispatched fn finished before the break returned: the count must be frozen
	// immediately, and far below total (consumed + buffer + in-flight workers).
	frozen := calls.Load()
	time.Sleep(50 * time.Millisecond)
	switch n := calls.Load(); {
	case n != frozen:
		t.Errorf("TestItemEarlyBreak: got %d fn calls after the break returned, want the count frozen at %d", n, frozen)
	case n >= total:
		t.Errorf("TestItemEarlyBreak: got %d fn calls after breaking early, want fewer than %d", n, total)
	}
}
