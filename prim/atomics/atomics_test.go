package atomics

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRWValue(t *testing.T) {
	t.Parallel()

	v := RWValue[int]{}
	v.Store(1)
	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				got := v.Load()
				fmt.Println(got)
			}
		}()
	}

	storeWG := sync.WaitGroup{}
	storeWG.Add(1)
	go func() {
		defer storeWG.Done()
		for i := 0; i < 100; i++ {
			v.Store(i)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// This is here to make sure we don't have a writer race condition.
	storeWG.Add(1)
	go func() {
		defer storeWG.Done()
		lr := func(i int) (int, error) {
			return i, nil
		}

		for i := 0; i < 100; i++ {
			v.LoadReplace(lr)
		}
	}()

	storeWG.Wait() // Wait for our storing routines
	cancel()

	// Now wait for everything using the values.
	wg.Wait()
	if v.Load() != 99 {
		t.Fatalf("expected 99, got %d", v.Load())
	}
}
