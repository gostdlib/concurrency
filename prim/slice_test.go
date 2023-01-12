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
			t.Errorf("Test(%s): want err != nil, got err == nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("Test(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, test.s); diff != "" {
			t.Errorf("Test(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
