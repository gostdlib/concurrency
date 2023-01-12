package prim

import (
	"context"
	"fmt"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestSlice(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		desc string
		s    []int
		m    Mutator[int, int]
		want []int
		err  bool
	}{
		{
			desc: "normal case",
			s:    []int{1, 2, 3, 4, 5},
			m: func(ctx context.Context, i int) (int, error) {
				return i + 1, nil
			},
			want: []int{2, 3, 4, 5, 6},
		},
		{
			desc: "error case",
			s:    []int{1, 2, 3, 4, 5},
			m: func(ctx context.Context, i int) (int, error) {
				if i == 3 {
					return 0, fmt.Errorf("mock error")
				}
				return i + 1, nil
			},
			want: []int{2, 3, 3, 5, 6},
			err:  true,
		},
	}

	for _, test := range tests {
		err := Slice(ctx, test.s, test.m, nil)
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
	ctx := context.Background()
	tests := []struct {
		desc string
		s    []int
		m    Mutator[int, float64]
		want []float64
		err  bool
	}{
		{
			desc: "normal case",
			s:    []int{1, 2, 3, 4, 5},
			m: func(ctx context.Context, i int) (float64, error) {
				return float64(i) + 1.2, nil
			},
			want: []float64{2.2, 3.2, 4.2, 5.2, 6.2},
		},
		{
			desc: "error case",
			s:    []int{1, 2, 3, 4, 5},
			m: func(ctx context.Context, i int) (float64, error) {
				if i == 3 {
					return 0, fmt.Errorf("mock error")
				}
				return float64(i) + 1.2, nil
			},
			want: []float64{2.2, 3.2, 0, 5.2, 6.2},
			err:  true,
		},
	}

	for _, test := range tests {
		got, err := ResultSlice(ctx, test.s, test.m, nil)
		switch {
		case err == nil && test.err:
			t.Errorf("TestResultSlice(%s): want err != nil, got err == nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestResultSlice(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestResultSlice(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
