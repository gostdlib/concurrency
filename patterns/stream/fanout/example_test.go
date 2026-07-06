package fanout_test

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/concurrency/patterns/stream/fanout"
)

// ExampleLimited fans a slice out over a pool of 4 Workers for side effects only and waits on the
// returned channel for the whole run to finish. No results are streamed back, so the Workers record
// what they did and the example sorts it for deterministic output.
func ExampleLimited() {
	ctx := context.Background()

	nums := []int{1, 2, 3, 4, 5}

	mu := sync.Mutex{}
	doubled := make([]int, 0, len(nums))
	fn := func(ctx context.Context, _ int, v int) error {
		mu.Lock()
		doubled = append(doubled, v*2)
		mu.Unlock()
		return nil
	}

	<-fanout.Limited(ctx, "doubler", 4, stream.Slice(nums), fn)

	sort.Ints(doubled)
	fmt.Println(doubled)

	// Output:
	// [2 4 6 8 10]
}

// ExampleLimited_channel fans values received on a channel out over a pool of 2 Workers. stream.Chan
// adapts the channel into the input sequence; the run finishes once the channel is closed and every
// received value has been processed.
func ExampleLimited_channel() {
	ctx := context.Background()

	ch := make(chan string)
	context.Pool(ctx).Submit(ctx, func() {
		defer close(ch)
		for _, s := range []string{"a", "b", "c"} {
			ch <- s
		}
	})

	mu := sync.Mutex{}
	upper := make([]string, 0, 3)
	fn := func(ctx context.Context, _ int, v string) error {
		mu.Lock()
		upper = append(upper, strings.ToUpper(v))
		mu.Unlock()
		return nil
	}

	<-fanout.Limited(ctx, "upper", 2, stream.Chan(ctx, ch), fn)

	sort.Strings(upper)
	fmt.Println(upper)

	// Output:
	// [A B C]
}

// ExampleWithGate retries a flaky Worker with backpressure: the first attempt on 20 fails, and WithGate
// retries it with the backoff while pausing dispatch of new pairs, so every value is still processed.
// WithTesting makes the retries sleepless for the example; drop it in real code.
func ExampleWithGate() {
	ctx := context.Background()

	boff := exponential.Must(exponential.New(exponential.WithTesting()))

	var failures atomic.Int64
	mu := sync.Mutex{}
	saved := make([]int, 0, 3)
	fn := func(ctx context.Context, _ int, v int) error {
		if v == 20 && failures.Add(1) == 1 {
			return fmt.Errorf("dependency unavailable")
		}
		mu.Lock()
		saved = append(saved, v)
		mu.Unlock()
		return nil
	}

	<-fanout.Limited(ctx, "saver", 4, stream.Slice([]int{10, 20, 30}), fn, fanout.WithGate(boff))

	sort.Ints(saved)
	fmt.Println(saved)

	// Output:
	// [10 20 30]
}
