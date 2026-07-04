package stream

import (
	"iter"
	"slices"
	"testing"

	"github.com/gostdlib/base/context"
	"github.com/kylelemons/godebug/pretty"
)

// pair is a collected key/value emitted by an iter.Seq2.
type pair[K, V any] struct {
	K K
	V V
}

// collect drains seq into a slice of pairs in iteration order.
func collect[K, V any](seq iter.Seq2[K, V]) []pair[K, V] {
	var out []pair[K, V]
	for k, v := range seq {
		out = append(out, pair[K, V]{k, v})
	}
	return out
}

func TestChan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []int
		want []pair[int, int]
	}{
		{
			name: "Success: empty channel yields nothing",
			in:   nil,
			want: nil,
		},
		{
			name: "Success: values are yielded keyed by receive index",
			in:   []int{10, 20, 30},
			want: []pair[int, int]{{0, 10}, {1, 20}, {2, 30}},
		},
	}

	for _, test := range tests {
		ch := make(chan int, len(test.in))
		for _, v := range test.in {
			ch <- v
		}
		close(ch)

		got := collect(Chan(t.Context(), ch))

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestChan(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestChanCancel verifies that when ctx is cancelled before ranging begins, Chan yields nothing and does
// not consume from c. This covers an idle channel (never sends, never closes; without Context awareness
// this would block forever) as well as a channel pre-filled with buffered values, which must be left
// untouched because cancellation is checked before any receive.
func TestChanCancel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		buffered []int // Values pre-filled into the channel before iteration begins.
	}{
		{
			name:     "Success: idle channel yields nothing after ctx cancelled",
			buffered: nil,
		},
		{
			name:     "Success: buffered values are not consumed after ctx cancelled",
			buffered: []int{1, 2, 3},
		},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		ch := make(chan int, len(test.buffered))
		for _, v := range test.buffered {
			ch <- v
		}

		got := collect(Chan(ctx, ch))
		if got != nil {
			t.Errorf("TestChanCancel(%s): got %v, want no values after ctx cancelled", test.name, got)
		}
		if len(ch) != len(test.buffered) {
			t.Errorf("TestChanCancel(%s): got %d buffered values remaining, want %d (Chan must not consume from c after cancellation)", test.name, len(ch), len(test.buffered))
		}
	}
}

// TestChanCancelDuringIteration verifies that cancelling ctx mid-iteration over a continuously-fed
// channel terminates the iteration promptly rather than yielding unbounded values. The consumer cancels
// from inside the range body after a fixed number of values; because Chan checks ctx.Err() at the top of
// every iteration, the loop must stop immediately, yielding exactly that many values. This is
// deterministic (no timing) and stable under -race -count=50.
func TestChanCancelDuringIteration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch := make(chan int) // Unbuffered so the producer only advances as the consumer receives.

	// Producer feeds continuously until ctx is cancelled, so it never leaks past the test.
	go func() {
		for i := 0; ; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	const cancelAfter = 5

	var got []int
	for _, v := range Chan(ctx, ch) {
		got = append(got, v)
		if len(got) == cancelAfter {
			cancel()
		}
	}

	if len(got) != cancelAfter {
		t.Errorf("TestChanCancelDuringIteration: got %d values, want exactly %d (iteration must stop promptly after cancel)", len(got), cancelAfter)
	}
}

func TestSeq(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []int
		want []pair[int, int]
	}{
		{
			name: "Success: empty sequence yields nothing",
			in:   nil,
			want: nil,
		},
		{
			name: "Success: values are yielded keyed by index",
			in:   []int{5, 6, 7},
			want: []pair[int, int]{{0, 5}, {1, 6}, {2, 7}},
		},
	}

	for _, test := range tests {
		got := collect(Seq(slices.Values(test.in)))

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestSeq(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}
