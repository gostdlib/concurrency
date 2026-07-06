package fanout_test

import (
	"fmt"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/values/generics/promises"
	"github.com/gostdlib/base/values/generics/result"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/concurrency/patterns/stream/fanout"
)

// ExampleLimited_singleFlight fans a stream of keys out over a pool while collapsing duplicate keys
// with a sync.Flight (single flight). The same key appears several times in the stream, but the
// expensive per-key load runs at most once while a load for that key is in flight, so every duplicate
// shares the one result. fanout is side-effect only, so the Worker stores the deduplicated value into a
// map that the caller reads once the run drains.
func ExampleLimited_singleFlight() {
	ctx := context.Background()

	// alice/bob appear multiple times; the flight suppresses the concurrent duplicates.
	ids := []string{"alice", "bob", "alice", "carol", "bob", "alice"}

	// The zero value of Flight is ready to use; it dedups by key.
	var flight sync.Flight[string, int]
	var loads atomic.Int64 // counts how many times the load actually ran

	// ShardedMap is concurrency-safe on its own, so Workers write to it without a Mutex.
	var loaded sync.ShardedMap[string, int]
	fn := func(ctx context.Context, _ int, id string) error {
		// Do runs load once per key while a load is in flight; duplicates wait and share the result.
		v, err, _ := flight.Do(ctx, id, func() (int, error) {
			loads.Add(1)        // stand-in for an expensive fetch
			return len(id), nil // the "loaded" value for this key
		})
		if err != nil {
			return err
		}
		loaded.Set(id, v)
		return nil
	}

	<-fanout.Limited(ctx, "loader", 4, stream.Slice(ids), fn)

	alice, _ := loaded.Get("alice")
	bob, _ := loaded.Get("bob")
	carol, _ := loaded.Get("carol")
	fmt.Println(alice, bob, carol)
	// loads.Load() is <= 3 (one per distinct key), but the exact count depends on how many duplicates
	// overlapped in flight, so it is not printed.

	// Output:
	// 5 3 5
}

// ExampleLimited_result pairs fanout with a result.Value per input. fanout streams no results back, so
// each Worker resolves the result.Value it is responsible for with Set, and the caller (or any other
// goroutine) collects the outcome with Wait. result.Value is the right fit when a single eventual value
// may be awaited by several goroutines.
func ExampleLimited_result() {
	ctx := context.Background()

	nums := []int{1, 2, 3, 4}

	// One result.Value per input; the side-effecting Worker sets it, waiters block on Wait until it is.
	results := make([]*result.Value[int], len(nums))
	for i := range results {
		results[i] = result.New[int]()
	}

	fn := func(ctx context.Context, i int, v int) error {
		results[i].Set(v*v, nil) // Set is called at most once per Value.
		return nil
	}

	<-fanout.Limited(ctx, "squarer", 4, stream.Slice(nums), fn)

	got := make([]int, len(nums))
	for i, r := range results {
		v, _ := r.Wait(ctx) // Wait can only error on a cancelled ctx.
		got[i] = v
	}
	fmt.Println(got)

	// Output:
	// [1 4 9 16]
}

// ExampleLimited_promise fans a slice of Promises out over the pool. Each Promise carries its input and
// a slot for the eventual result; the Worker is handed a Promise (by copy, but its result channel is
// shared) and resolves it with Set, and the caller reads each result back with Get. This is the pattern
// for sending values into a pipeline and picking their results up later. A Maker is used to build the
// promises since we are making many of them: it reuses the underlying channels across promises.
func ExampleLimited_promise() {
	ctx := context.Background()

	words := []string{"go", "fan", "out"}

	maker := promises.Maker[string, int]{}
	proms := make([]promises.Promise[string, int], len(words))
	for i, w := range words {
		proms[i] = maker.New(ctx, w)
	}

	fn := func(ctx context.Context, _ int, p promises.Promise[string, int]) error {
		p.Set(ctx, len(p.In), nil) // resolve the Promise with the length of its input.
		return nil
	}

	fanout.Limited(ctx, "measure", 4, stream.Slice(proms), fn)

	for i := range proms {
		resp, _ := proms[i].Get(ctx) // Get can only error on a cancelled ctx.
		fmt.Println(proms[i].In, resp.V)
	}

	// Output:
	// go 2
	// fan 3
	// out 3
}
