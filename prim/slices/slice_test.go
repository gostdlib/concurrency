package slices

import (
	"context"
	"fmt"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestSliceMut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		desc string
		s    []int
		a    Accessor[int]
		want []int
		err  bool
	}{
		{
			desc: "normal case",
			s:    []int{1, 2, 3, 4, 5},
			a: func(ctx context.Context, i int, v int, m Modifier[int]) error {
				m(v + 1)
				return nil
			},
			want: []int{2, 3, 4, 5, 6},
		},
		{
			desc: "error case",
			s:    []int{1, 2, 3, 4, 5},
			a: func(ctx context.Context, i int, v int, m Modifier[int]) error {
				if i == 3 {
					return fmt.Errorf("mock error")
				}
				m(v + 1)
				return nil
			},
			want: []int{2, 3, 3, 5, 6},
			err:  true,
		},
	}

	for _, test := range tests {
		err := Access(ctx, test.s, test.a)
		switch {
		case err == nil && test.err:
			t.Errorf("TestSlice(%s): want err != nil, got err == nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestSlice(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, test.s); diff != "" {
			t.Errorf("TestSlice(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}

func TestResultSlice(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	s := []int{1, 2, 3, 4, 5}
	want := []float64{2.2, 3.2, 4.2, 5.2, 6.2}
	got := make([]float64, len(s))

	a := func(ctx context.Context, i int, v int, m Modifier[int]) error {
		got[i] = float64(v) + 1.2
		return nil
	}

	err := Access(ctx, s, a)

	if err != nil {
		t.Errorf("TestResultSlice: got err == %s, want err == nil", err)
		return
	}

	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestResultSlice: -want/+got:\n%s", diff)
	}
}
