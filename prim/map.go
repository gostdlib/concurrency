package prim

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/internals/otel/span"
)

// MapMutator is a function that takes in a key K and value V and returns a key and value
// that should be set in a map.
type MapMutator[K comparable, V any] func(ctx context.Context, key K, val V) (K, V, error)

// Map applies MapMutator "mut" to each element in map "m" using the goroutines Pool
// "p". If p == nil, p becomes a limited.Pool using up to runtime.NumCPU().
// Mutations may not change the key passed and instead may change a different key or
// add a new key.
// Errors will be returned, but will not stop this from completing.
// Values at the position that return an error will remain unchanged.
func Map[K comparable, V any](ctx context.Context, m map[K]V, mut MapMutator[K, V], p goroutines.Pool, subOpts ...goroutines.SubmitOption) error {
	spanner := span.Get(ctx)

	if len(m) == 0 {
		return nil
	}

	if p == nil {
		var err error
		p, err = limited.New("", runtime.NumCPU())
		if err != nil {
			spanner.Error(err)
			return err
		}
		defer p.Close()
	}

	ptr := atomic.Pointer[error]{}

	for k, v := range m {
		k := k
		v := v

		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := p.Submit(
			ctx,
			func(ctx context.Context) {
				key, val, err := mut(ctx, k, v)
				if err != nil {
					applyErr(&ptr, err)
					return
				}
				m[key] = val
			},
			subOpts...,
		)
		if err != nil {
			return err
		}
	}

	p.Wait()

	errPtr := ptr.Load()
	if errPtr != nil {
		spanner.Error(*errPtr)
		return *errPtr
	}
	return nil
}

// ResultMap takes values in map "m" and applies Mutator "mut" to get a new result map[K]V.
// Map "m" is not mutated.
// Errors will be returned, but will not stop this from completing.
func ResultMap[K comparable, V any](ctx context.Context, m map[K]V, mut MapMutator[K, V], p goroutines.Pool, subOpts ...goroutines.SubmitOption) (map[K]V, error) {
	spanner := span.Get(ctx)

	if len(m) == 0 {
		if m == nil {
			return nil, nil
		}
		return map[K]V{}, nil
	}

	if p == nil {
		var err error
		p, err = limited.New("", runtime.NumCPU())
		if err != nil {
			spanner.Error(err)
			return nil, err
		}
		defer p.Close()
	}

	ptr := atomic.Pointer[error]{}
	results := make(map[K]V, len(m))
	for k, v := range m {
		k := k
		v := v

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		p.Submit(
			ctx,
			func(ctx context.Context) {
				var err error
				key, val, err := mut(ctx, k, v)
				if err != nil {
					applyErr(&ptr, err)
					return
				}
				results[key] = val
			},
			subOpts...,
		)
	}
	p.Wait()

	errPtr := ptr.Load()
	if errPtr != nil {
		spanner.Error(*errPtr)
		return results, *errPtr
	}
	return results, nil
}
