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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/internal/pool"
	"github.com/gostdlib/concurrency/goroutines/internal/register"
	"github.com/gostdlib/internals/otel/span"
)

var _ goroutines.Pool = &Pool{}

// Pool is a pool of goroutines.
type Pool struct {
	wg        sync.WaitGroup
	running   atomic.Int64
	pool.Pool // Implements the pool.Preventer interface
	queue     chan struct{}
	name      string
}

// New creates a new Pool. "name" is the name of the pool which is used to get OTEL
// metrics and traces. These names must be globally unique, so it is best to set it
// to the package path + pool name that is using it. However, if not unique,
// a unique name will be created. If name is the empty string, the pool will not
// be registered, which is useful if creating and tearing down the pool instead of
// using it for the lifetime of the program. Names cannot contain spaces, hyphens, or numbers.
// "size" is the number of goroutines that can execute concurrently.
func New(name string, size int) (*Pool, error) {
	if size < 1 {
		return nil, fmt.Errorf("cannot have a Pool with size < 1")
	}
	if err := register.ValidateBaseName(name); err != nil {
		return nil, err
	}

	p := &Pool{name: name, queue: make(chan struct{}, size)}

	for {
		if err := register.Register(p); err != nil {
			p.name = register.NewName(name)
			continue
		}
		break
	}
	return p, nil
}

// Close waits for all submitted jobs to stop, then stops all goroutines.
func (p *Pool) Close() {
	p.wg.Wait()
	close(p.queue)
	register.Unregister(p)
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

// GetName gets the name of the goroutines pool.
func (p *Pool) GetName() string {
	return p.name
}

// setName sets the name of the goroutines pool.
func (p *Pool) setName(name string) {
	p.name = name
}

// NonBlocking indicates that if we are at our limit, we still run the goroutine
// and it is not counted against the total. This is useful when you want to track
// the statistics still but need this goroutine to run and don't want to do it naked.
func NonBlocking() goroutines.SubmitOption {
	return func(opt pool.SubmitOptions) (pool.SubmitOptions, error) {
		if opt.Type != pool.PTLimited {
			return opt, fmt.Errorf("cannot use limited.NotBlocking() with a non limited.Pool")
		}
		opt.NonBlocking = true
		return opt, nil
	}
}

// Caller sets the name of the calling function so that metrics can differentiate
// who is using the goroutines in the pool. With the introduction of generics, there is no
// way to get the name of function call reliably, as generic functions are written dynamically and
// runtime.FuncForPC does not work for generics. If this is not set, we will use runtime.FuncForPC().
func Caller(name string) goroutines.SubmitOption {
	return func(opt pool.SubmitOptions) (pool.SubmitOptions, error) {
		if opt.Type != pool.PTLimited {
			return opt, fmt.Errorf("cannot use limited.NotBlocking() with a non limited.Pool")
		}
		opt.Caller = name
		return opt, nil
	}
}

// Submit submits the runner to be executed.
func (p *Pool) Submit(ctx context.Context, runner goroutines.Job, options ...goroutines.SubmitOption) error {
	spanner := span.Get(ctx)
	if runner == nil {
		err := fmt.Errorf("cannot submit a runner that is nil")
		spanner.Error(err)
		return err
	}

	opts := pool.SubmitOptions{Type: pool.PTLimited}
	var err error
	for _, o := range options {
		opts, err = o(opts)
		if err != nil {
			spanner.Error(err)
			return err
		}
	}

	now := time.Now()
	fcn := p.callerName(opts)

	if !opts.NonBlocking {
		select {
		case p.queue <- struct{}{}:
		default:
			p.blockEvent(spanner, fcn, now)
			p.queue <- struct{}{}
		}
		defer func() { <-p.queue }()
	}
	p.submitEvent(spanner, fcn, opts.NonBlocking, now)

	p.wg.Add(1)
	p.running.Add(1)

	go func() {
		defer p.wg.Done()
		defer p.running.Add(-1)
		runner(ctx)
	}()

	return nil
}

func (p *Pool) submitEvent(spanner span.Span, fcn string, nonBlock bool, t time.Time) {
	spanner.Event(
		"Pool.Submit() called",
		"pkg", "github.com/gostdlib/concurrency/goroutines/limited",
		"caller", fcn,
		"name", p.name,
		"non_blocking", nonBlock,
		"submit_latency_ns", time.Since(t),
	)
}

func (p *Pool) blockEvent(spanner span.Span, fcn string, t time.Time) {
	spanner.Event(
		"Pool.Submit() blocking....",
		"pkg", "github.com/gostdlib/concurrency/goroutines/limited",
		"caller", fcn,
		"name", p.name,
		"event", "blocking",
		"submit_latency_ns", time.Since(t),
	)
}

func (p *Pool) unblockEvent(spanner span.Span, fcn string, t time.Time) {
	spanner.Event(
		"Pool.Submit() unblocking....",
		"pkg", "github.com/gostdlib/concurrency/goroutines/limited",
		"caller", fcn,
		"name", p.name,
		"event", "unblocking",
		"submit_latency_ns", time.Since(t),
	)
}

func (p *Pool) callerName(opts pool.SubmitOptions) string {
	if opts.Caller != "" {
		return opts.Caller
	}

	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		return details.Name()
	}
	return ""
}
