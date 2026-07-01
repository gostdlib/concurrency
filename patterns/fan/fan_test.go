package fan

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

var runSize = 10000

var want []int

func init() {
	for i := 0; i < runSize; i++ {
		want = append(want, i)
	}
}

func TestOutIn_Unordered(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	got, err := runTest(ctx, false)
	if err != nil {
		t.Fatalf("TestOutIn: a value had an unexpected error: %v", err)
	}

	sort.Ints(got)

	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestOutIn: -want/+got:\n%s", diff)
	}
}

func TestOutIn_Ordered(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	got, err := runTest(ctx, true)
	if err != nil {
		t.Fatalf("TestOutIn: a value had an unexpected error: %v", err)
	}

	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestOutIn: -want/+got:\n%s", diff)
	}
}

// TestVerifyOutInResuse makes sure that OutIn can be reused.
func TestVerifyOutInResuse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	oi := OutIn[string, int]{
		Input: func(f *Exec[string, int]) {
			for i := 0; i < runSize; i++ {
				s := strconv.Itoa(i)
				f.Send(ctx, s)
			}
		},
		Processor: func(ctx context.Context, in string) (int, error) {
			return strconv.Atoi(in)
		},
		RetainOrder: true,
	}

	for i := 0; i < 10; i++ {
		out, err := oi.Run(ctx)
		if err != nil {
			t.Fatalf("TestVerifyOutInResuse: a Run() had an unexpected error: %v", err)
		}

		got := make([]int, 0, runSize)
		// Print the results. The output channel is automatically closed when the processing is done.
		// You must consume all output.
		for r := range out {
			if r.Err != nil {
				t.Fatalf("TestVerifyOutInResuse: a value had an unexpected return error: %v", r.Err)
			}
			got = append(got, r.V)
		}

		if diff := pretty.Compare(want, got); diff != "" {
			t.Errorf("TestOutIn: -want/+got:\n%s", diff)
		}
	}
}

func runTest(ctx context.Context, ordered bool) ([]int, error) {
	oi := OutIn[string, int]{
		Input: func(f *Exec[string, int]) {
			for i := 0; i < runSize; i++ {
				s := strconv.Itoa(i)
				f.Send(ctx, s)
			}
		},
		Processor: func(ctx context.Context, in string) (int, error) {
			return strconv.Atoi(in)
		},
		RetainOrder: ordered,
	}

	out, err := oi.Run(ctx)
	if err != nil {
		return nil, err
	}
	got := make([]int, 0, runSize)
	// Print the results. The output channel is automatically closed when the processing is done.
	// You must consume all output.
	for r := range out {
		if r.Err != nil {
			return nil, r.Err
		}
		got = append(got, r.V)
	}
	return got, nil
}
