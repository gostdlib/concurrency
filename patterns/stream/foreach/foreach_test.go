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
