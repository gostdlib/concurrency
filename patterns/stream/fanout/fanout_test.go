package fanout

import (
	"errors"
	"iter"
	"testing"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/kylelemons/godebug/pretty"
)

var errBoom = errors.New("boom")

func testBoff() *exponential.Backoff {
	return exponential.Must(exponential.New(exponential.WithTesting()))
}

func TestLimited(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name string
		size int
		opts []Option
		// failFirst makes every pair's Worker fail its first attempt and succeed after. Paired with
		// WithGate it proves the run still processes every pair; it is the only field that exercises the
		// Worker's error return.
		failFirst bool
		// build returns the input sequence and the key/value pairs the run must process.
		build func(ctx context.Context) (iter.Seq2[int, int], map[int]int)
	}{
		{
			name: "Success: a slice fans out every element keyed by index",
			size: 4,
			build: func(ctx context.Context) (iter.Seq2[int, int], map[int]int) {
				return stream.Slice([]int{10, 20, 30, 40, 50}), map[int]int{0: 10, 1: 20, 2: 30, 3: 40, 4: 50}
			},
		},
		{
			name: "Success: a map fans out every key/value pair",
			size: 4,
			build: func(ctx context.Context) (iter.Seq2[int, int], map[int]int) {
				m := map[int]int{1: 100, 2: 200, 3: 300}
				return stream.Map(m), map[int]int{1: 100, 2: 200, 3: 300}
			},
		},
		{
			name: "Success: a channel fans out every received value keyed by receive index",
			size: 4,
			build: func(ctx context.Context) (iter.Seq2[int, int], map[int]int) {
				ch := make(chan int)
				context.Pool(ctx).Submit(ctx, func() {
					defer close(ch)
					for i := 0; i < 5; i++ {
						ch <- i * i
					}
				})
				return stream.Chan(ctx, ch), map[int]int{0: 0, 1: 1, 2: 4, 3: 9, 4: 16}
			},
		},
		{
			name: "Success: a size of one processes every element instead of deadlocking on its driver",
			size: 1,
			build: func(ctx context.Context) (iter.Seq2[int, int], map[int]int) {
				return stream.Slice([]int{1, 2, 3, 4, 5, 6, 7, 8}), map[int]int{0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8}
			},
		},
		{
			name: "Success: an empty input runs no Workers and still closes done",
			size: 4,
			build: func(ctx context.Context) (iter.Seq2[int, int], map[int]int) {
				return stream.Slice([]int{}), map[int]int{}
			},
		},
		{
			name:      "Success: a WithGate backoff retries a failing Worker until every pair is processed",
			size:      4,
			opts:      []Option{WithGate(testBoff())},
			failFirst: true,
			build: func(ctx context.Context) (iter.Seq2[int, int], map[int]int) {
				return stream.Slice([]int{7, 8, 9}), map[int]int{0: 7, 1: 8, 2: 9}
			},
		},
	}

	for _, test := range tests {
		seq, want := test.build(ctx)

		mu := sync.Mutex{}
		got := map[int]int{}
		attempts := map[int]int{}
		fn := func(ctx context.Context, k int, v int) error {
			mu.Lock()
			attempts[k]++
			first := attempts[k] == 1
			mu.Unlock()
			if test.failFirst && first {
				return errBoom
			}
			mu.Lock()
			got[k] = v
			mu.Unlock()
			return nil
		}

		<-Limited(ctx, test.name, test.size, seq, fn, test.opts...)

		if diff := pretty.Compare(want, got); diff != "" {
			t.Errorf("TestLimited(%s): processed pairs: -want/+got:\n%s", test.name, diff)
		}
	}
}
