package keyed_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/concurrency/patterns/stream/keyed"
)

// item is a keyed test payload: Key is the partition key, N the 0-based index within that key.
type item struct {
	Key string
	N   int
}

func itemKey(_ int, it item) string { return it.Key }

// perKeyOpts is the WithLanePerKey option the strategy-table tests use for their per-key row.
func perKeyOpts() []keyed.Option { return []keyed.Option{keyed.WithLanePerKey(50 * time.Millisecond)} }

// orderedPerKeyOpts is the per-key row's option set for the ordered-delivery test: WithOrdered on top
// of the per-key lane strategy.
func orderedPerKeyOpts() []keyed.Option {
	return append([]keyed.Option{keyed.WithOrdered()}, perKeyOpts()...)
}

// TestItemOrdering verifies that pairs sharing a partition key are processed in input order even
// when adversarial per-pair delays would let later pairs overtake earlier ones without the per-lane
// serialization. Different keys run concurrently. It runs for both lane strategies; the ItemFunc
// records the order it observed per key.
func TestItemOrdering(t *testing.T) {
	t.Parallel()

	const perKey = 12
	keys := []string{"a", "b", "c", "d"}

	tests := []struct {
		name string
		opts []keyed.Option
	}{
		{name: "Success: fixed-lane default preserves per-key order", opts: nil},
		{name: "Success: per-key lane preserves per-key order", opts: perKeyOpts()},
	}

	for _, test := range tests {
		ctx := t.Context()
		var in []item
		for n := 0; n < perKey; n++ {
			for _, k := range keys {
				in = append(in, item{Key: k, N: n})
			}
		}

		var mu sync.Mutex
		order := map[string][]int{}
		fn := func(ctx context.Context, _ int, it item) (int, error) {
			// Earlier N sleeps longer, so completion order would reverse input order without the lane
			// serializing same-key pairs.
			time.Sleep(time.Duration(perKey-it.N) * 200 * time.Microsecond)
			mu.Lock()
			order[it.Key] = append(order[it.Key], it.N)
			mu.Unlock()
			return it.N, nil
		}

		count := 0
		for _, resp := range keyed.Item(ctx, stream.Slice(in), itemKey, fn, test.opts...) {
			if resp.Err != nil {
				t.Fatalf("TestItemOrdering(%s): got err == %s, want nil", test.name, resp.Err)
			}
			count++
		}

		if count != len(in) {
			t.Fatalf("TestItemOrdering(%s): got %d results, want %d", test.name, count, len(in))
		}
		for _, k := range keys {
			got := order[k]
			if len(got) != perKey {
				t.Fatalf("TestItemOrdering(%s, key %s): got %d observations, want %d", test.name, k, len(got), perKey)
			}
			for i, n := range got {
				if n != i {
					t.Fatalf("TestItemOrdering(%s, %s): observation %d == %d, want %d: %v", test.name, k, i, n, i, got)
				}
			}
		}
	}
}

// TestItemOrdered verifies WithOrdered delivers results in input order across all keys even though the
// ItemFunc makes earlier pairs finish last. Without the reorder buffer, completion order would reverse
// input order; with it, the Nth result must carry the Nth input's key (the slice index). Runs for both
// lane strategies.
func TestItemOrdered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []keyed.Option
	}{
		{name: "Success: fixed lanes deliver in input order", opts: []keyed.Option{keyed.WithOrdered()}},
		{name: "Success: per-key lanes deliver in input order", opts: orderedPerKeyOpts()},
	}

	for _, test := range tests {
		ctx := t.Context()
		const n = 200
		keys := []string{"a", "b", "c", "d", "e"}
		var in []item
		for i := 0; i < n; i++ {
			in = append(in, item{Key: keys[i%len(keys)], N: i})
		}
		fn := func(ctx context.Context, k int, _ item) (int, error) {
			// Earlier input positions sleep longer, so completion order reverses input order without the
			// reorder buffer. k is the slice index from stream.Slice, i.e. the input position.
			time.Sleep(time.Duration(n-k) * 40 * time.Microsecond)
			return k, nil
		}

		var got []int
		for k, resp := range keyed.Item(ctx, stream.Slice(in), itemKey, fn, test.opts...) {
			if resp.Err != nil {
				t.Fatalf("TestItemOrdered(%s): got err == %s, want nil", test.name, resp.Err)
			}
			got = append(got, k)
		}

		if len(got) != n {
			t.Fatalf("TestItemOrdered(%s): got %d results, want %d", test.name, len(got), n)
		}
		for i, k := range got {
			if k != i {
				t.Fatalf("TestItemOrdered(%s): result %d key %d, want %d (input order): %v", test.name, i, k, i, got)
			}
		}
	}
}

// TestItemEarlyBreak verifies that breaking out of the range stops the run without hanging: dispatch
// of not-yet-started pairs stops and the in-flight work drains. If cleanup leaked or deadlocked, the
// range would never return and the test would time out.
func TestItemEarlyBreak(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	var in []item
	for n := 0; n < 500; n++ {
		in = append(in, item{Key: "k", N: n})
	}

	fn := func(ctx context.Context, _ int, it item) (int, error) {
		time.Sleep(time.Millisecond)
		return it.N, nil
	}

	got := 0
	for _, resp := range keyed.Item(ctx, stream.Slice(in), itemKey, fn) {
		if resp.Err != nil {
			t.Fatalf("TestItemEarlyBreak: got err == %s, want nil", resp.Err)
		}
		got++
		if got == 3 {
			break
		}
	}
	// Reaching here means the range returned after the break rather than hanging.
	if got != 3 {
		t.Fatalf("TestItemEarlyBreak: consumed %d results before break, want 3", got)
	}
}

// TestItemErrors verifies that an ItemFunc error is delivered in-band as Result.Err under that pair's
// key while the other pairs still yield their values.
func TestItemErrors(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	var in []item
	for _, k := range []string{"a", "b"} {
		for n := 0; n < 10; n++ {
			in = append(in, item{Key: k, N: n})
		}
	}

	fn := func(ctx context.Context, _ int, it item) (int, error) {
		if it.N%2 == 1 {
			return 0, fmt.Errorf("boom %s/%d", it.Key, it.N)
		}
		return it.N, nil
	}

	gotErr := make([]error, len(in))
	gotV := make([]int, len(in))
	seen := 0
	for k, resp := range keyed.Item(ctx, stream.Slice(in), itemKey, fn) {
		gotErr[k] = resp.Err
		gotV[k] = resp.V
		seen++
	}

	if seen != len(in) {
		t.Fatalf("TestItemErrors: got %d results, want %d", seen, len(in))
	}
	for i, it := range in {
		if wantErr := it.N%2 == 1; (gotErr[i] != nil) != wantErr {
			t.Errorf("TestItemErrors(%s/%d): got err != nil == %v, want %v", it.Key, it.N, gotErr[i] != nil, wantErr)
			continue
		}
		// The even-N pairs succeed, so their value must have come through.
		if it.N%2 == 0 && gotV[i] != it.N {
			t.Errorf("TestItemErrors(%s/%d): got value %d, want %d", it.Key, it.N, gotV[i], it.N)
		}
	}
}

// TestLanePerKeyReuse verifies correctness across an idle gap: a key's lane retires while the source
// pauses longer than the idle timeout, and a fresh lane processes the key's later pairs in order. A
// channel source drives the gap. Nothing is lost and order 0..N-1 is preserved across the retire.
func TestLanePerKeyReuse(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	const idle = 30 * time.Millisecond

	ch := make(chan item, 5)
	context.Pool(ctx).Submit(ctx, func() {
		defer close(ch)
		for n := 0; n < 5; n++ {
			ch <- item{Key: "k", N: n}
		}
		time.Sleep(idle + 120*time.Millisecond) // let the lane go idle and retire
		for n := 5; n < 10; n++ {
			ch <- item{Key: "k", N: n}
		}
	})

	var mu sync.Mutex
	var order []int
	fn := func(ctx context.Context, _ int, it item) (int, error) {
		mu.Lock()
		order = append(order, it.N)
		mu.Unlock()
		return it.N, nil
	}

	count := 0
	for _, resp := range keyed.Item(ctx, stream.Chan(ctx, ch), itemKey, fn, keyed.WithLanePerKey(idle)) {
		if resp.Err != nil {
			t.Fatalf("TestLanePerKeyReuse: got err == %s, want nil", resp.Err)
		}
		count++
	}

	if count != 10 {
		t.Fatalf("TestLanePerKeyReuse: got %d results, want 10 (retire-recreate lost pairs)", count)
	}
	for i, n := range order {
		if n != i {
			t.Fatalf("TestLanePerKeyReuse: observation %d == %d, want %d (out of order across retire): %v", i, n, i, order)
		}
	}
}

// TestItemPanic verifies that an ItemFunc panic does not crash the process: it is recovered, the run
// tears down, and the original panic re-raises from the range as a PanicError carrying the panic
// value and a captured stack. It runs for both lane strategies. A hang (leak/deadlock) would time out.
func TestItemPanic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []keyed.Option
	}{
		{name: "Success: fixed lanes re-raise the panic with value and stack", opts: nil},
		{name: "Success: per-key lanes re-raise the panic with value and stack", opts: perKeyOpts()},
	}

	for _, test := range tests {
		ctx := t.Context()
		var in []item
		for n := 0; n < 30; n++ {
			in = append(in, item{Key: "x", N: n})
		}
		fn := func(ctx context.Context, _ int, it item) (int, error) {
			if it.N == 5 {
				panic(fmt.Sprintf("boom %d", it.N))
			}
			return it.N, nil
		}

		var got any
		func() {
			defer func() { got = recover() }()
			for range keyed.Item(ctx, stream.Slice(in), itemKey, fn, test.opts...) { //nolint:revive // drain
			}
		}()

		pe, ok := got.(keyed.PanicError)
		if !ok {
			t.Fatalf("TestItemPanic(%s): recovered %T, want PanicError (a stall would hang instead)", test.name, got)
		}
		if s, _ := pe.Value.(string); s != "boom 5" {
			t.Errorf("TestItemPanic(%s): PanicError.Value == %v, want %q", test.name, pe.Value, "boom 5")
		}
		if len(pe.Stack) == 0 {
			t.Errorf("TestItemPanic(%s): PanicError.Stack is empty, want the captured origin stack", test.name)
		}
	}
}

// TestItemPanicOnBreak verifies the break-path re-raise: a consumer that breaks the range must still
// surface a recorded ItemFunc panic rather than swallow it. The first pair panics, and the consumer
// breaks on the first result it sees; the range must still re-raise a PanicError. The WithOrdered rows
// exercise the same break-then-raise path through the reorder buffer's stop() branch.
func TestItemPanicOnBreak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []keyed.Option
	}{
		{name: "Success: fixed lanes surface the panic on break", opts: nil},
		{name: "Success: per-key lanes surface the panic on break", opts: perKeyOpts()},
		{name: "Success: fixed lanes ordered surface the panic on break", opts: []keyed.Option{keyed.WithOrdered()}},
		{name: "Success: per-key lanes ordered surface the panic on break", opts: orderedPerKeyOpts()},
	}

	for _, test := range tests {
		ctx := t.Context()
		var in []item
		for n := 0; n < 30; n++ {
			in = append(in, item{Key: "x", N: n})
		}
		fn := func(ctx context.Context, _ int, it item) (int, error) {
			if it.N == 0 {
				panic("boom 0")
			}
			return it.N, nil
		}

		var got any
		func() {
			defer func() { got = recover() }()
			for range keyed.Item(ctx, stream.Slice(in), itemKey, fn, test.opts...) {
				break // stop on the first result; the recorded panic must still surface
			}
		}()

		if _, ok := got.(keyed.PanicError); !ok {
			t.Fatalf("TestItemPanicOnBreak(%s): recovered %T, want PanicError (panic swallowed on break)", test.name, got)
		}
	}
}

// TestItemLimitedPool verifies a run makes progress when ctx carries a worker pool limited to a single
// slot. A run must need at most one pool slot (the lanes run on the sync.Group's own goroutines, not
// the pool), so it cannot wedge on a second Submit that blocks before delivery starts draining. The
// input is larger than the lane and output buffers, so a run that needed two concurrent slots would
// hang. Both lane strategies are covered.
func TestItemLimitedPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []keyed.Option
	}{
		{name: "Success: fixed lanes finish under a single-slot pool", opts: []keyed.Option{keyed.WithFixedLanes(2)}},
		{name: "Success: per-key lanes finish under a single-slot pool", opts: perKeyOpts()},
	}

	for _, test := range tests {
		base := t.Context()
		lctx := context.SetPool(base, context.Pool(base).Limited(base, "keyedtest", 1))

		keys := []string{"a", "b", "c", "d"}
		var in []item
		for i := 0; i < 500; i++ {
			in = append(in, item{Key: keys[i%len(keys)], N: i})
		}
		fn := func(ctx context.Context, _ int, it item) (int, error) { return it.N, nil }

		done := make(chan int, 1)
		// Run the consumer on the unconstrained base pool so this test goroutine stays free to time out;
		// the Item run itself uses lctx's single-slot pool.
		context.Pool(base).Submit(base, func() {
			count := 0
			for _, resp := range keyed.Item(lctx, stream.Slice(in), itemKey, fn, test.opts...) {
				if resp.Err != nil {
					t.Errorf("TestItemLimitedPool(%s): got err == %s, want nil", test.name, resp.Err)
				}
				count++
			}
			done <- count
		})

		select {
		case count := <-done:
			if count != len(in) {
				t.Errorf("TestItemLimitedPool(%s): got %d results, want %d", test.name, count, len(in))
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("TestItemLimitedPool(%s): timed out — deadlock under a single-slot pool", test.name)
		}
	}
}
