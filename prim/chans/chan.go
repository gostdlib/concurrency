// Package chans provides functions for operating on a channel in parallel.
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
func Access[T any](ctx context.Context, input chan T, access Accessor[T], options ...Option) error {
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
func Modify[T, R any](ctx context.Context, input chan T, output chan StreamResult[R], mod Modifier[T, R], options ...Option) error {
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
