package foreach_test

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/values/generics/promises"
	"github.com/gostdlib/base/values/generics/result"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/concurrency/patterns/stream/foreach"
)

// ExampleItem_singleFlight fans a stream of keys out over the pool while collapsing duplicate keys with a
// sync.Flight (single flight). The same host appears several times, but the expensive per-host resolve
// runs at most once while a resolve for that host is in flight; every duplicate shares the one result,
// and foreach still streams a response back for every pair.
func ExampleItem_singleFlight() {
	ctx := context.Background()

	// alpha/beta appear multiple times; the flight suppresses the concurrent duplicates.
	hosts := []string{"alpha", "beta", "alpha", "alpha", "beta"}

	// The zero value of Flight is ready to use; it dedups by key.
	var flight sync.Flight[string, int]
	var resolves atomic.Int64 // counts how many times the resolve actually ran

	fn := func(ctx context.Context, _ int, host string) (int, error) {
		// Do runs resolve once per host while a resolve is in flight; duplicates wait and share it.
		v, err, _ := flight.Do(ctx, host, func() (int, error) {
			resolves.Add(1)       // stand-in for an expensive resolve
			return len(host), nil // the "resolved" value for this host
		})
		return v, err
	}

	// The range loop body runs in a single goroutine as responses arrive, so a plain map needs no lock
	// here (unlike a fanout Worker, which writes concurrently and wants a sync.ShardedMap).
	resolved := map[string]int{}
	for k, resp := range foreach.Item(ctx, stream.Slice(hosts), fn) {
		if resp.Err != nil {
			fmt.Println(resp.Err)
			return
		}
		resolved[hosts[k]] = resp.V
	}
	fmt.Println(resolved["alpha"], resolved["beta"])
	// resolves.Load() is <= 2 (one per distinct host), but the exact count depends on how many duplicates
	// overlapped in flight, so it is not printed.

	// Output:
	// 5 4
}

// ExampleItem_response shows that foreach streams its results back as promises.Response values:
// stream.Result[T], the type Item yields, is an alias of promises.Response[T]. Each response carries a
// value OR an error in-band, so a failing pair does not stop the others — you inspect Response.Err per
// pair. Here the negative input is the one that carries an error while every other pair carries a value.
func ExampleItem_response() {
	ctx := context.Background()

	inputs := []int{4, -1, 9}

	fn := func(ctx context.Context, _ int, v int) (float64, error) {
		if v < 0 {
			return 0, fmt.Errorf("negative input %d", v)
		}
		return math.Sqrt(float64(v)), nil
	}

	// resp is a promises.Response[float64]; collect by index so the completion-order stream prints
	// deterministically.
	got := make([]promises.Response[float64], len(inputs))
	for k, resp := range foreach.Item(ctx, stream.Slice(inputs), fn) {
		got[k] = resp
	}

	for _, resp := range got {
		if resp.Err != nil {
			fmt.Println("error:", resp.Err)
			continue
		}
		fmt.Println(resp.V)
	}

	// Output:
	// 2
	// error: negative input -1
	// 3
}

// ExampleItem_result funnels foreach's results into a result.Value per input so other goroutines can
// await a specific input's outcome without caring about completion order. Several goroutines may Wait on
// the same result.Value; each unblocks when the funnel Sets it. Here foreach runs on a background worker
// while the main goroutine waits on the results it needs.
func ExampleItem_result() {
	ctx := context.Background()

	nums := []int{1, 2, 3, 4}

	// One result.Value per input, created up front so waiters can hold theirs before processing starts.
	results := make([]*result.Value[int], len(nums))
	for i := range results {
		results[i] = result.New[int]()
	}

	fn := func(ctx context.Context, _ int, v int) (int, error) {
		return v * v, nil
	}

	// Run foreach on a background worker, funneling each result into its result.Value as it completes.
	context.Pool(ctx).Submit(ctx, func() {
		for k, resp := range foreach.Item(ctx, stream.Slice(nums), fn) {
			results[k].Set(resp.V, resp.Err) // Set is called at most once per Value.
		}
	})

	// Meanwhile, the caller blocks on the specific results it needs, in any order.
	got := make([]int, len(nums))
	for i, r := range results {
		v, _ := r.Wait(ctx) // Wait can only error on a cancelled ctx.
		got[i] = v
	}
	fmt.Println(got)

	// Output:
	// [1 4 9 16]
}

// ExampleItem_promise uses foreach as the parallel engine behind a Promise pipeline. Callers submit
// Promises carrying their input onto a shared channel; a single foreach drains the channel in parallel
// and resolves each Promise with Set, and each caller reads its own result back with Get — decoupled
// from every other caller and from the completion order. The Results themselves are unused here since
// the Promise is the delivery mechanism, so the ItemFunc returns struct{}. A Maker builds the promises
// because we are making many of them: it reuses the underlying channels.
func ExampleItem_promise() {
	ctx := context.Background()

	maker := promises.Maker[int, int]{}
	proms := make([]promises.Promise[int, int], 3)
	for i := range proms {
		proms[i] = maker.New(ctx, i+1)
	}

	// Submit each Promise into the pipeline on a background worker.
	pipeline := make(chan promises.Promise[int, int])
	context.Pool(ctx).Submit(ctx, func() {
		defer close(pipeline)
		for _, p := range proms {
			pipeline <- p
		}
	})

	fn := func(ctx context.Context, _ int, p promises.Promise[int, int]) (struct{}, error) {
		p.Set(ctx, p.In*p.In, nil) // resolve the Promise with the square of its input.
		return struct{}{}, nil
	}

	// Drain the pipeline in parallel; the Promise carries the result, so the Results are ignored.
	for range foreach.Item(ctx, stream.Chan(ctx, pipeline), fn) {
	}

	for i := range proms {
		resp, _ := proms[i].Get(ctx) // Get can only error on a cancelled ctx.
		fmt.Println(proms[i].In, resp.V)
	}

	// Output:
	// 1 1
	// 2 4
	// 3 9
}
