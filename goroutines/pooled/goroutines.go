/*
Package goroutiones provides a Pool of goroutines where you can submit Jobs
to be run when by an exisiting goroutine instead of spinning off a new goroutine.

See the examples in the parent package "goroutines" for an overview of using pools.
*/
package pooled

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/johnsiilver/pools/goroutines"
	"github.com/johnsiilver/pools/goroutines/internal/pool"
)

var _ goroutines.Pool = &Pool{}

// Pool is a pool of goroutines.
type Pool struct {
	queue chan submit
	wg    sync.WaitGroup

	running atomic.Int64
}

// New creates a new Pool. Size is the number of goroutines in the pool.
func New(size int) (*Pool, error) {
	if size < 1 {
		return nil, fmt.Errorf("cannot have a Pool with size < 1")
	}

	ch := make(chan submit, 1)
	p := &Pool{queue: ch}
	for i := 0; i < size; i++ {
		go p.runner()
	}

	return p, nil
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

type submit struct {
	ctx context.Context
	job goroutines.Job
}

// NonBlocking indicates that if a pooled goroutine is not available, spin off
// a goroutine and do not block.
func NonBlocking() goroutines.SubmitOption {
	return func(opt *pool.SubmitOptions) error {
		if opt.Type != pool.PTPooled {
			return fmt.Errorf("cannot use pooled.NotBlocking() with a non pooled.Pool")
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

	opts := pool.SubmitOptions{Type: pool.PTPooled}

	for _, o := range options {
		if err := o(&opts); err != nil {
			return err
		}
	}

	s := submit{ctx: ctx, job: runner}

	p.wg.Add(1)
	p.running.Add(1)
	if opts.NonBlocking {
		select {
		case p.queue <- s:
		default:
			go func() {
				defer p.wg.Done()
				defer p.running.Add(-1)
				s.job(ctx)
			}()
		}
		return nil
	}

	p.queue <- s
	return nil
}

// runner is used to run any function that comes in on the in channel.
func (p *Pool) runner() {
	for s := range p.queue {
		s.job(s.ctx)
		p.running.Add(-1)
		p.wg.Done()
	}
}
