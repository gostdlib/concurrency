/*
Package goroutines provides the interfaces and defintions that goroutine pools must
implement/use. Implementations are in sub-directories and can be used directly
without using this package.

As this is the parent package, we will show some basic examples that are valid across
all implementations using the "pooled" package.

Example of using a pool where errors don't matter:

	ctx := context.Background()
	p, err := pooled.New(runtime.NumCPU())
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

	p, err := pooled.New(runtime.NumCPU())
	if err != nil {
		panic(err)
	}
	defer p.Close()

	e := goroutines.Errors{}
	ch := make(chan *client.Response, runtime.NumCPU())

	// urls would just be some []string containing URLs.
	for _, url := range urls {
		i := i

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

	p, err := pooled.New(runtime.NumCPU())
	if err != nil {
		panic(err)
	}
	defer p.Close()

	e := goroutines.Errors{}
	ch := make(chan *client.Response, runtime.NumCPU())

	// urls would just be some []string containing URLs.
	for _, url := range urls {
		i := i

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

// Job is a job for a Pool.
type Job func(ctx context.Context)

// SubmitOption is an option for Pool.Submit().
type SubmitOption func(opt *pool.SubmitOptions) error

// Pool is the minimum interface that any goroutine pool must implement.
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
}

// Errors is a concurrency safe way of capturing a set of errors in multiple goroutines.
type Errors struct {
	errors []error
	mu     sync.Mutex
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
