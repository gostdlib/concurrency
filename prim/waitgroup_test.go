package prim

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitGroupBasic(t *testing.T) {
	// setup
	var wg WaitGroup
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
	wg.Wait()

	// check that running count is 0 after wait
	if wg.Running() != 0 {
		t.Errorf("TestWaitGroupBasic: Expected Running() to return 0, got %d", wg.Running())
	}
}

func TestWaitGroupCancelOnErr(t *testing.T) {
	// setup
	var wg WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	wg.CancelOnErr(cancel)

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
	wg.Wait()

	if wg.Err() == nil {
		t.Errorf("TestWaitGroupCancelOnErr: want error != nil, got nil")
	}
}
