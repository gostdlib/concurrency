package prim

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/concurrency/goroutines/pooled"
)

func TestWaitGroupBasic(t *testing.T) {
	limit, err := limited.New("", 5)
	if err != nil {
		t.Fatalf("TestWaitGroupBasic: %s", err)
	}
	defer limit.Close()
	pooler, err := pooled.New("", 5)
	if err != nil {
		t.Fatalf("TestWaitGroupBasic: %s", err)
	}
	defer pooler.Close()

	tests := []struct {
		desc string
		pool goroutines.Pool
	}{
		{desc: "No pool", pool: nil},
		{desc: "With limited pool", pool: limit},
		{desc: "With pooled pool", pool: pooler},
	}

	for _, test := range tests {
		// setup
		var wg = WaitGroup{Pool: test.pool}
		var count atomic.Int32
		var exit = make(chan struct{})

		// test go routine
		f := func(ctx context.Context) error {
			count.Add(1)
			defer count.Add(-1)
			<-exit
			return nil
		}

		// spin off 5 go routines
		for i := 0; i < 5; i++ {
			wg.Go(context.Background(), f)
		}

		for count.Load() != 5 {
			time.Sleep(10 * time.Millisecond)
		}

		// check that running count is correct
		if wg.Running() != 5 {
			t.Errorf("TestWaitGroupBasic: Expected Running() to return 5, got %d", wg.Running())
		}
		close(exit)

		// wait for all go routines to finish
		wg.Wait(context.Background())

		// check that running count is 0 after wait
		if wg.Running() != 0 {
			t.Errorf("TestWaitGroupBasic: Expected Running() to return 0, got %d", wg.Running())
		}
	}
}

func TestWaitGroupCancelOnErr(t *testing.T) {
	// setup
	ctx, cancel := context.WithCancel(context.Background())
	wg := WaitGroup{CancelOnErr: cancel}

	// spin off 5 go routines
	for i := 0; i < 5; i++ {
		i := i
		wg.Go(
			ctx,
			func(ctx context.Context) error {
				if i == 3 {
					return errors.New("error")
				}
				<-ctx.Done()

				return nil
			},
		)
	}
	if err := wg.Wait(ctx); err == nil {
		t.Errorf("TestWaitGroupCancelOnErr: want error != nil, got nil")
	}
}
