package limited

import (
	"context"
	"runtime"
	"testing"
)

func TestPool(t *testing.T) {
	p, err := New(runtime.NumCPU())
	if err != nil {
		panic(err)
	}

	answer := make([]bool, 1000)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		i := i
		p.Submit(
			ctx,
			func(ctx context.Context) {
				answer[i] = true
			},
		)
	}
	p.Wait()

	for i, e := range answer {
		if !e {
			t.Fatalf("TestPool: entry(%d) was not set to true as expected", i)
		}
	}
}
