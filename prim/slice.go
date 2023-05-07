package prim

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/internals/otel/span"
	"github.com/johnsiilver/calloptions"
)

// Mutator is a function that takes in a value T and returns an element R.
// T and R can be the same type.
type Mutator[T, R any] func(context.Context, T) (R, error)

type sliceOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
}

// SliceOption is an option for Slice().
type SliceOption interface {
	slice()
}

// Slice applies Mutator "m" to each element in "s".
// If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// Errors will be returned, but will not stop this from completing.
// Values at the position that return an error will remain unchanged.
func Slice[T any](ctx context.Context, s []T, mut Mutator[T, T], options ...SliceOption) error {
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

	ptr := atomic.Pointer[error]{}
	wg := sync.WaitGroup{}

	for i := 0; i < len(s); i++ {
		i := i

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wg.Add(1)
		err := opts.pool.Submit(
			ctx,
			func(ctx context.Context) {
				defer wg.Done()
				var err error
				s[i], err = mut(ctx, s[i])
				if err != nil {
					applyErr(&ptr, err)
				}
			},
			opts.poolOptions...,
		)
		if err != nil {
			return err
		}
	}
	wg.Wait()

	errPtr := ptr.Load()
	if errPtr != nil {
		spanner.Error(*errPtr)
		return *errPtr
	}
	return nil
}

type resultSliceOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
}

// ResultSliceOption is an option for ResultSlice().
type ResultSliceOption interface {
	resultSlice()
}

// ResultSlice takes values in slice "s" and applies Mutator "m" to get a new result slice []R.
// Slice "s" is not mutated. This allows you to have a returns slice of a different type or
// simply to leave the passed slice untouched.
// Errors will be returned, but will not stop this from completing. Values at the
// position that return an error will be the zero value for the R type.
func ResultSlice[T, R any](ctx context.Context, s []T, mut Mutator[T, R], options ...ResultSliceOption) ([]R, error) {
	spanner := span.Get(ctx)

	opts := &resultSliceOptions{}
	if err := calloptions.ApplyOptions(&opts, options); err != nil {
		return nil, err
	}

	if len(s) == 0 {
		if s == nil {
			return nil, nil
		}
		return []R{}, nil
	}

	if opts.pool == nil {
		var err error
		opts.pool, err = limited.New("", runtime.NumCPU())
		if err != nil {
			spanner.Error(err)
			return nil, err
		}
		defer opts.pool.Close()
	}

	ptr := atomic.Pointer[error]{}
	results := make([]R, len(s))
	wg := sync.WaitGroup{}

	for i := 0; i < len(s); i++ {
		i := i

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		wg.Add(1)
		opts.pool.Submit(
			ctx,
			func(ctx context.Context) {
				defer wg.Done()
				var err error
				results[i], err = mut(ctx, s[i])
				if err != nil {
					applyErr(&ptr, err)
				}
			},
			opts.poolOptions...,
		)
	}
	wg.Wait()

	errPtr := ptr.Load()
	if errPtr != nil {
		spanner.Error(*errPtr)
		return results, *errPtr
	}
	return results, nil
}

func applyErr(ptr *atomic.Pointer[error], err error) {
	for {
		existing := ptr.Load()
		if existing == nil {
			if ptr.CompareAndSwap(nil, &err) {
				return
			}
		} else {
			if err == context.Canceled {
				return
			}
			err = fmt.Errorf("%w", err)
			if ptr.CompareAndSwap(existing, &err) {
				return
			}
		}
	}
}
