/*
Package chans provides functions for operating on recv channels in parallel. This is useful when processing a large amount of data
with complex transformations. We will provide simple examples, though in reality those would probably be better served
with a standard for loop in a single goroutine.

The first function is Access() and uses an Accessor to process data from a channel in parallel.
You can utilize a goroutine pool from the goroutines package or just use the default parallelism.

Let's say you want to simply read numbers and print the number multiplied by 5 as they come in from a channel:

	input := make(chan int, 1)
	go func() {
		defer close(input)
		for i := 0; i < 1000; i++ {
			input <- i
		}
	}()

	accessor := func(ctx context.Context, i int) error {
		fmt.Println(i * 5)
		return nil
	}
		
	err := chans.Access(ctx, input, accessor)
	if err != nil {
		// Do something
	}

Because I didn't provide any options, this will process until either input is closed or
Context is cancelled. If you want this to cancel on an error, you can pass WithStopOnErr().

And we are using defaults here, so it will create a new goroutine pool with runtime.NumCPU()
number of goroutines.

If there are multiple errors, they are each wrapped in the final error.

The second function Modify() works in a similar fashion, as it is a fancy wrapper around Access().

Modify() takes in a stream from input and outputs to a stream. The input and output do not have
to be the same. And you get the additional benefit of knowing which entries had an error inside StreamResult.

Let's say we simply want to take in a channel of ints and turn them into floats. Here's how we can
do that:

	input := make(chan int, 1)
	
	go func() {
		defer close(input)
		for i := 0; i < 1000; i++ {
			input <- i
		}
	}()

	modifier := func(ctx context.Context, i int) (float64, error){
		return float64(i), nil
	}

	output := make(chan StreamResult[float64]), 1)

	var err error

	go func() {
		err = chans.Modify(ctx, input, output chan, modifier)
	}()

	for sr := range output {
		fmt.Println(sr.Value)
	} 

	if err != nil {
		// handle error
	}
*/
package chans

import (
	"context"
	"fmt"
	"runtime"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/concurrency/prim/wait"
	"github.com/gostdlib/internals/otel/span"
	"github.com/johnsiilver/calloptions"
)

// Accessor is called for all values in a channel.
type Accessor[V any] func(context.Context, V) error

type chanOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
	stopOnErr   bool
}

// ChanOption is an option for Chan().
type Option interface {
	chanFunc()
}

// Access applies the Accessor" to each element in "input" using the goroutines Pool. If WithPool() isn't provided,
// we use a limited.Pool using up to runtime.NumCPU().
// If the Accessor has an error, the error will be returned but this will not stop
// processing unless WithStopOnErr() is provided. You can cancel the context to stop processing early.
func Access[T any](ctx context.Context, input <-chan T, access Accessor[T], options ...Option) error {
	spanner := span.Get(ctx)

	opts := chanOptions{}
	if err := calloptions.ApplyOptions(&opts, options); err != nil {
		return err
	}

	if input == nil {
		return nil
	}

	if opts.pool == nil {
		var err error
		opts.pool, err = limited.New("", runtime.NumCPU())
		if err != nil {
			spanner.Error(err)
			return err
		}
		defer opts.pool.Close()
	}

	var cancel = func() {}
	if opts.stopOnErr {
		ctx, cancel = context.WithCancel(ctx)
	}
	wg := wait.Group{Pool: opts.pool, PoolOptions: opts.poolOptions, CancelOnErr: cancel}

	for v := range input {
		v := v

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wg.Go(
			ctx,
			func(ctx context.Context) error {
				return access(
					ctx,
					v,
				)
			},
		)
	}
	if err := wg.Wait(ctx); err != nil {
		spanner.Error(err)
		return err
	}
	return nil
}

// StreamResult is a result from a Stream operation.
type StreamResult[T any] struct {
	// Value is the value returned in the stream.
	Value T
	// Err is the error returned in the stream.
	Err error
}

type Modifier[T, R any] func(context.Context, T) (R, error)

// Modify applies the Modifier to each element in "input" using the goroutines Pool in parallel.
// If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// If the Modifier has an error, the error will be returned but this will not stop
// processing unless WithStopOnErr() is provided. The output channel will be closed when
// processing is complete. You can cancel the context to stop processing early.
func Modify[T, R any](ctx context.Context, input <-chan T, output chan <-StreamResult[R], mod Modifier[T, R], options ...Option) error {
	spanner := span.Get(ctx)

	if input == nil {
		err := fmt.Errorf("input channel is nil")
		spanner.Error(err)
		return err
	}
	if output == nil {
		err := fmt.Errorf("output channel is nil")
		spanner.Error(err)
		return err
	}

	defer close(output)

	a := func(ctx context.Context, v T) error {
		r, err := mod(ctx, v)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- StreamResult[R]{Err: err}:
			}
			return err
		}
		output <- StreamResult[R]{Value: r}
		return nil
	}

	return Access(ctx, input, a, options...)
}
