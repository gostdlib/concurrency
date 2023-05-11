package prim

import (
	"context"
	"runtime"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/internals/otel/span"
	"github.com/johnsiilver/calloptions"
)

// Accessor is a function that is called on each element in a slice.
// This passes the slice being iterated over and the index of the element.
type Accessor[T any] func(context.Context, []T, int) error

// Mutator is a function that takes in a value T and returns an element R.
// T and R can be the same type.
type Mutator[T, R any] func(context.Context, T) (R, error)

type sliceOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
	stopOnErr   bool
}

// SliceOption is an option for functions operating on slices.
type SliceOption interface {
	slice()
}

// Slice calls Accessor "access" for each element in "s". This can be used to mutate every element
// in a slice, send values to a channel, create a new slice, etc.
// If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// The first error encountered will be returned, but will not stop this
// from completing (this behavior can be overridden with the WithStopOnErr() option).
// Slice is used to implement all other slice functions.
func Slice[T any](ctx context.Context, s []T, access Accessor[T], options ...SliceOption) error {
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
	wg := WaitGroup{Pool: opts.pool, PoolOptions: opts.poolOptions, CancelOnErr: cancel}

	for i := 0; i < len(s); i++ {
		i := i

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wg.Go(
			ctx,
			func(ctx context.Context) error {
				return access(ctx, s, i)
			},
		)
	}
	if err := wg.Wait(ctx); err != nil {
		spanner.Error(err)
		return err
	}
	return nil
}

// SliceMut applies Mutator "m" to each element in "s".
// If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// The first error encountered will be returned, but will not stop this
// from completing (this behavior can be overridden with the WithStopOnErr() option).
// Values at the position that return an error will remain unchanged.
func SliceMut[T any](ctx context.Context, s []T, mut Mutator[T, T], options ...SliceOption) error {
	access := func(ctx context.Context, s []T, i int) error {
		var err error
		s[i], err = mut(ctx, s[i])
		return err
	}

	return Slice(ctx, s, access, options...)
}

// ResultSlice takes values in slice "s" and applies Mutator "m" to get a new result slice []R.
// Slice "s" is not mutated. This allows you to have a returns slice of a different type or
// simply to leave the passed slice untouched.
// The first error encountered will be returned, but will not stop this
// from completing (this behavior can be overridden with the WithStopOnErr() option).
// Values at the position that return an error will be the zero value for the R type.
func ResultSlice[T, R any](ctx context.Context, s []T, mut Mutator[T, R], options ...SliceOption) ([]R, error) {
	if len(s) == 0 {
		if s == nil {
			return nil, nil
		}
		return []R{}, nil
	}

	results := make([]R, len(s))

	access := func(ctx context.Context, s []T, i int) error {
		var err error
		results[i], err = mut(ctx, s[i])
		return err
	}

	return results, Slice(ctx, s, access, options...)
}
