// Package slices provides functions for operating on slices in parallel.
package slices

import (
	"context"
	"runtime"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/concurrency/prim/wait"
	"github.com/gostdlib/internals/otel/span"
	"github.com/johnsiilver/calloptions"
)

// Modifier sets a value in a slice to v.
type Modifier[V any] func(v V)

// Accessor is a function that is called on each element in a slice.
// This passes the index of the element, the value at that element, and
// a Modifier function that can be used to modify the slice element.
type Accessor[T any] func(context.Context, int, T, Modifier[T]) error

func modifier[V any](s []V, i int, v V) {
	s[i] = v
}

type sliceOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
	stopOnErr   bool
}

// SliceOption is an option for functions operating on slices.
type SliceOption interface {
	slice()
}

// Access calls the Accessor for each element in "s". This can be used to mutate every element
// in a slice, send values to a channel, create a new slice, etc.
// If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// The first error encountered will be returned, but will not stop this
// from completing (this behavior can be overridden with the WithStopOnErr() option).
// This is a lock free implementation that does everything in parallel.
func Access[T any](ctx context.Context, s []T, access Accessor[T], options ...SliceOption) error {
	spanner := span.Get(ctx)

	opts := &sliceOptions{}
	if err := calloptions.ApplyOptions(&opts, options); err != nil {
		return err
	}

	if len(s) == 0 {
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

	for i := 0; i < len(s); i++ {
		i := i

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wg.Go(
			ctx,
			func(ctx context.Context) error {
				return access(ctx, i, s[i], func(v T) { modifier(s, i, v) })
			},
		)
	}
	if err := wg.Wait(ctx); err != nil {
		spanner.Error(err)
		return err
	}
	return nil
}
