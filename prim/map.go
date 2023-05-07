package prim

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/internals/otel/span"
	"github.com/johnsiilver/calloptions"
)

// MapMutator is a function that takes in a key K and value V and returns a key and value
// that should be set in a map.
type MapMutator[K comparable, V any] func(ctx context.Context, key K, val V) (K, V, error)

type mapOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
}

// MapOption is an option for Map().
type MapOption interface {
	mapFunc()
}

// Map applies MapMutator "mut" to each element in map "m".
// If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// Mutations may not change the key passed and instead may change a different key or
// add a new key.
// Errors will be returned, but will not stop this from completing.
// Values at the position that return an error will remain unchanged.
func Map[K comparable, V any](ctx context.Context, m map[K]V, mut MapMutator[K, V], options ...MapOption) error {
	spanner := span.Get(ctx)

	if len(m) == 0 {
		return nil
	}

	opts := &mapOptions{}
	if err := calloptions.ApplyOptions(&opts, options); err != nil {
		return err
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

	for k, v := range m {
		k := k
		v := v

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wg.Add(1)
		err := opts.pool.Submit(
			ctx,
			func(ctx context.Context) {
				defer wg.Done()
				key, val, err := mut(ctx, k, v)
				if err != nil {
					applyErr(&ptr, err)
					return
				}
				m[key] = val
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

type resultMapOptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
}

// ResultMapOption is an option for ResultMap().
type ResultMapOption interface {
	resultMapFunc()
}

// ResultMap takes values in map "m" and applies Mutator "mut" to get a new result map[K]V.
// Map "m" is not mutated. If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// Errors will be returned, but will not stop this from completing.
func ResultMap[K comparable, V any](ctx context.Context, m map[K]V, mut MapMutator[K, V], options ...ResultMapOption) (map[K]V, error) {
	spanner := span.Get(ctx)

	if len(m) == 0 {
		if m == nil {
			return nil, nil
		}
		return map[K]V{}, nil
	}

	opts := &resultMapOptions{}
	if err := calloptions.ApplyOptions(&opts, options); err != nil {
		return nil, err
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
	results := make(map[K]V, len(m))
	wg := sync.WaitGroup{}

	for k, v := range m {
		k := k
		v := v

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		wg.Add(1)
		opts.pool.Submit(
			ctx,
			func(ctx context.Context) {
				defer wg.Done()
				var err error
				key, val, err := mut(ctx, k, v)
				if err != nil {
					applyErr(&ptr, err)
					return
				}
				results[key] = val
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
