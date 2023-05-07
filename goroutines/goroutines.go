/*
Package goroutines provides the interfaces and defintions that goroutine pools must
implement/use. Implementations are in sub-directories and can be used directly
without using this package.

Pools are generally a low-level construct that are passed to higher level constructs that provide
safety and more targeted functionality. The prim package is a good example of this. There you will
find an enhanced WaitGroup, functions for processing slices in parallel, map processing and more.

If you are looking to create pipelines, the pipeline package provides concurrent and parallel processing
that will be more beneficial that trying to use pools directly.

As this is the parent package, we will show some basic examples that are valid across
all implementations using the "pooled" sub-package that implements the Pool interface found here.

Example of using a pool where errors don't matter:

	ctx := context.Background()
	p, err := pooled.New("name", runtime.NumCPU())
	if err != nil {
		panic(err)
	}
	defer p.Close()

	for i := 0; i < 100; i++ {
		i := i

		p.Submit(
			ctx,
			func(ctx context.Context) {
				fmt.Println("Hello number ", i)
			},
		)
	}

	p.Wait()

Example of using a pool where errors occur, but shouldn't stop execution:

	ctx := context.Background()
	client := http.Client{}

	p, err := pooled.New("name", runtime.NumCPU())
	if err != nil {
		panic(err)
	}
	defer p.Close()

	e := goroutines.Errors{}
	ch := make(chan *client.Response, runtime.NumCPU())

	// urls would just be some []string containing URLs.
	for _, url := range urls {
		url := url

		p.Submit(
			ctx,
			func(ctx context.Context) {
				resp, err := client.Get(url)
				if err != nil {
					e.Record(err)
					return
				}
				ch <- resp
			},
		)
	}

	go func() {
		for _, resp := range ch {
			if resp.Status != 200 {
				fmt.Printf("URL(%s) has response %d", resp.Request.URL, resp.Status)
				continue
			}
			b, err := ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("URL(%s) had error when reading the body: %s", resp.Request.URL, err)
			}
			fmt.Println(string(b))
		}
	}()

	p.Wait()
	close(ch)

	for _, err := range e.Errors() {
		fmt.Println("had http.Client error: ", err")
	}

Example of using a pool where errors occur and should stop exeuction:

	ctx, cancel := context.WithCancel(context.Background())
	client := http.Client{}

	p, err := pooled.New("name", runtime.NumCPU())
	if err != nil {
		panic(err)
	}
	defer p.Close()

	e := goroutines.Errors{}
	ch := make(chan *client.Response, runtime.NumCPU())

	// urls would just be some []string containing URLs.
	for _, url := range urls {
		url := url

		if ctx.Err() != nil {
			break
		}

		p.Submit(
			ctx,
			func(ctx context.Context) {
				if ctx.Err() != nil {
					return
				}
				resp, err := client.Get(url)
				if err != nil {
					e.Record(err)
					cancel()
					return
				}
				ch <- resp
			},
		)
	}

	... Rest of the code from the last example
*/
package goroutines

import (
	"context"
	"sync"

	"github.com/gostdlib/concurrency/goroutines/internal/pool"
)

// Job is a job for a Pool to execute.
type Job func(ctx context.Context)

// SubmitOption is an option for Pool.Submit().
type SubmitOption func(opt *pool.SubmitOptions) error

// Pool is the minimum interface that any goroutine pool must implement. This can
// only be created by using a sub-package that implements this interface.
type Pool interface {
	// Submit submits a Job to be run.
	Submit(ctx context.Context, runner Job, options ...SubmitOption) error
	// Close closes the goroutine pool. This will call Wait() before it closes.
	Close()
	// Wait will wait for all goroutines to finish. This should only be called if
	// you have stopped calling Submit().
	Wait()
	// Len indicates how big the pool is.
	Len() int
	// Running returns how many goroutines are currently in flight.
	Running() int

	// GetName gets the name of the goroutines pool. This may differ from the name
	// that was passed in when creating the pool. This can be caused by a naming conflict,
	// which causes the name to be appended by a dash(-) and a number.
	GetName() string

	pool.Preventer // Prevents outside packages from implementing Pool.
}

// Errors is a concurrency safe way of capturing a set of errors in multiple goroutines.
type Errors struct {
	mu     sync.Mutex
	errors []error
}

// Record writes an error to Errors.
func (e *Errors) Record(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.errors = append(e.errors, err)
}

// Error returns the first error recieved.
func (e *Errors) Error() error {
	if len(e.errors) == 0 {
		return nil
	}
	return e.errors[0]
}

// Errors returns all errors.
func (e *Errors) Errors() []error {
	return e.errors
}
