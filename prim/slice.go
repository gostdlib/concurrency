package prim

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
)

// Mutator is a function that takes in a value T and returns an element T that
// may/may not be changed.
type Mutator[T, R any] func(context.Context, T) (R, error)

// Slice applies Mutator "m" to each element in "s" using the goroutines Pool
// "p". If p == nil, p becomes a limited.Pool using up to runtime.NumCPU().
// Errors will be returned, but will not stop this from completing.
func Slice[T any](ctx context.Context, s []T, m Mutator[T, T], p goroutines.Pool) error {
	if len(s) == 0 {
		return nil
	}

	if p == nil {
		var err error
		p, err = limited.New(runtime.NumCPU())
		if err != nil {
			return err
		}
	}

	ptr := atomic.Pointer[error]{}

	for i := 0; i < len(s); i++ {
		i := i
		p.Submit(
			ctx,
			func(ctx context.Context) {
				log.Println("doing: ", i)
				defer log.Println("exiting: ", i)
				var err error
				s[i], err = m(ctx, s[i])
				if err != nil {
					applyErr(&ptr, err)
				}
			},
		)
	}
	p.Wait()

	errPtr := ptr.Load()
	if errPtr != nil {
		return *errPtr
	}
	return nil
}

// ResultSlice takes values in slice "s" and applies Mutator "m" to get a new result slice []R.
func ResultSlice[T, R any](ctx context.Context, s []T, m Mutator[T, R], p goroutines.Pool) ([]R, error) {
	if len(s) == 0 {
		if s == nil {
			return nil, nil
		}
		return []R{}, nil
	}

	if p == nil {
		var err error
		p, err = limited.New(runtime.NumCPU())
		if err != nil {
			return nil, err
		}
	}

	ptr := atomic.Pointer[error]{}
	results := make([]R, len(s))
	for i := 0; i < len(s); i++ {
		i := i
		p.Submit(
			ctx,
			func(ctx context.Context) {
				var err error
				results[i], err = m(ctx, s[i])
				if err != nil {
					applyErr(&ptr, err)
				}
			},
		)
	}
	return results, *ptr.Load()
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
