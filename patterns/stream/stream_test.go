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

// TestChanCancel verifies Chan stops iterating when the Context is cancelled even though the channel is
// idle (never sends, never closes). Without Context awareness this would block forever.
func TestChanCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	ch := make(chan int) // Never written to or closed.

	got := collect(Chan(ctx, ch))
	if got != nil {
		t.Errorf("TestChanCancel: got %v, want no values after ctx cancelled", got)
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
