/*
Package limited provides a groutine execution Pool that spins a goroutine per Submit()
but is hard limited to the number of goroutines that can run at any time.

As Go has matured, goroutines have become more efficient. This type of pool starts
very fast and is only slightly slower than our pooled version.  For pools that you
want to start up and tear down quickly, this might be the best choice.

See the examples in the parent package "goroutines" for an overview of using pools.
*/
package limited

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/internal/pool"
)

var _ goroutines.Pool = &Pool{}

// Pool is a pool of goroutines.
type Pool struct {
	wg      sync.WaitGroup
	running atomic.Int64
	queue   chan struct{}
}

// New creates a new Pool. Size is the number of goroutines that can execute concurrently.
func New(size int) (*Pool, error) {
	if size < 1 {
		return nil, fmt.Errorf("cannot have a Pool with size < 1")
	}

	return &Pool{queue: make(chan struct{}, size)}, nil
}

// Close waits for all submitted jobs to stop, then stops all goroutines.
func (p *Pool) Close() {
	p.wg.Wait()
	close(p.queue)
}

// Wait will wait for all goroutines in the pool to finish. If you need to only
// wait on a subset of jobs, use a WaitGroup in your job.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// Len returns the current size of the pool.
func (p *Pool) Len() int {
	return len(p.queue)
}

// Running returns the number of running jobs in the pool.
func (p *Pool) Running() int {
	return int(p.running.Load())
}

// NonBlocking indicates that if we are at our limit, we still run the goroutine
// and it is not counted against the total. This is useful when you want to track
// the statistics still but need this goroutine to run and don't want to do it naked.
func NonBlocking() goroutines.SubmitOption {
	return func(opt *pool.SubmitOptions) error {
		if opt.Type != pool.PTLimited {
			return fmt.Errorf("cannot use limited.NotBlocking() with a non limited.Pool")
		}
		opt.NonBlocking = true
		return nil
	}
}

// Submit submits the runner to be executed.
func (p *Pool) Submit(ctx context.Context, runner goroutines.Job, options ...goroutines.SubmitOption) error {
	if runner == nil {
		return fmt.Errorf("cannot submit a runner that is nil")
	}

	opts := pool.SubmitOptions{Type: pool.PTLimited}

	for _, o := range options {
		if err := o(&opts); err != nil {
			return err
		}
	}

	if !opts.NonBlocking {
		p.queue <- struct{}{}
		defer func() { <-p.queue }()
	}

	p.wg.Add(1)
	p.running.Add(1)

	go func() {
		defer p.wg.Done()
		defer p.running.Add(-1)
		runner(ctx)
	}()
	return nil
}
