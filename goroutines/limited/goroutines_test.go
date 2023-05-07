package limited

import (
	"context"
	"runtime"
	"testing"

	"github.com/gostdlib/concurrency/goroutines"
)

//lint:ignore U1000 This is for internal use only.
var g goroutines.Pool = &Pool{}

func TestPool(t *testing.T) {
	p, err := New("name", runtime.NumCPU())
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
