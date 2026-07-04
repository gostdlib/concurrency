package foreach_test

import (
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/concurrency/patterns/stream/foreach"
)

// ExampleItem processes a slice in parallel with the default completion-order stream: responses arrive
// as they finish, so the example collects and sorts them for deterministic output.
func ExampleItem() {
	ctx := context.Background()

	words := []string{"10", "11", "12", "13", "14"}

	fn := func(ctx context.Context, _ int, v string) (int, error) {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return n * 10, nil
	}

	nums := make([]int, 0, len(words))
	for _, resp := range foreach.Item(ctx, stream.Slice(words), fn) {
		if resp.Err != nil {
			fmt.Println(resp.Err)
			return
		}
		nums = append(nums, resp.V)
	}
	sort.Ints(nums)
	fmt.Println(nums)

	// Output:
	// [100 110 120 130 140]
}

// ExampleItem_ordered shows the order-retaining fan-out/fan-in: values process in parallel and the
// responses stream out in input order while processing is still running.
func ExampleItem_ordered() {
	ctx := context.Background()

	words := []string{"10", "11", "12", "13", "14"}

	fn := func(ctx context.Context, _ int, v string) (int, error) {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return n * 10, nil
	}

	for k, resp := range foreach.Item(ctx, stream.Slice(words), fn, foreach.WithOrdered()) {
		if resp.Err != nil {
			fmt.Println(k, resp.Err)
			continue
		}
		fmt.Println(k, resp.V)
	}

	// Output:
	// 0 100
	// 1 110
	// 2 120
	// 3 130
	// 4 140
}

// ExampleWithGate shows built-in retries with backpressure: a failed ItemFunc retries with the given
// backoff, and while it retries, Item stops feeding new work to the dependency that is already
// struggling. WithTesting makes the retries sleepless for the example; drop it in real code.
func ExampleWithGate() {
	ctx := context.Background()

	boff := exponential.Must(exponential.New(exponential.WithTesting()))

	// flaky stands in for a dependency having a bad moment: the first two calls for "13" fail, then it
	// recovers.
	var failures atomic.Int64
	flaky := func(ctx context.Context, v string) (int, error) {
		if v == "13" && failures.Add(1) <= 2 {
			return 0, fmt.Errorf("dependency unavailable")
		}
		return strconv.Atoi(v)
	}

	fn := func(ctx context.Context, _ int, v string) (int, error) {
		n, err := flaky(ctx, v)
		return n * 10, err
	}

	words := []string{"10", "11", "12", "13", "14"}
	for k, resp := range foreach.Item(ctx, stream.Slice(words), fn, foreach.WithGate(boff), foreach.WithOrdered()) {
		if resp.Err != nil {
			fmt.Println(k, resp.Err)
			continue
		}
		fmt.Println(k, resp.V)
	}

	// Output:
	// 0 100
	// 1 110
	// 2 120
	// 3 130
	// 4 140
}
