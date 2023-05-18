// Package maps provides functions for operating on maps in parallel.
package maps

import (
	"context"
	"runtime"
	"sync"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
	"github.com/gostdlib/concurrency/prim/wait"
	"github.com/gostdlib/internals/otel/span"
	"github.com/johnsiilver/calloptions"
)

// Modifer sets the current key to value v in a map.
type Modifer[V any] func(v V)

// Accessor is a function that is called on each element in a map.
// This passes the key and value of a map entry and a Modifier function that can be used
// to modify the map for that entry. The Modifier function is safe to call concurrently.
type Accessor[K comparable, V any] func(context.Context, K, V, Modifer[V]) error

func modifier[K comparable, V any](m map[K]V, k K, v V, lock *sync.Mutex) {
	lock.Lock()
	m[k] = v
	lock.Unlock()
}

type moptions struct {
	pool        goroutines.Pool
	poolOptions []goroutines.SubmitOption
	stopOnErr   bool
}

// MapOption is an option for Map().
type Option interface {
	mapFunc()
}

// Access calls Accessor "access" for each element in "m" in parrallel. This can be used to mutate every element or
// read every element. If WithPool() isn't provided, we use a limited.Pool using up to runtime.NumCPU().
// The first error encountered will be returned, but will not stop this from completing (this behavior can be
// overridden with the WithStopOnErr() option). A sync.Mutex is automatically used in the Modifier passed to the Accessor function
// to allow concurrent access to the map.
func Access[K comparable, V any](ctx context.Context, m map[K]V, access Accessor[K, V], options ...Option) error {
	spanner := span.Get(ctx)

	opts := moptions{}
	if err := calloptions.ApplyOptions(&opts, options); err != nil {
		return err
	}

	if len(m) == 0 {
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

	lock := &sync.Mutex{}

	for k, v := range m {
		k := k
		v := v

		if ctx.Err() != nil {
			return ctx.Err()
		}

		wg.Go(
			ctx,
			func(ctx context.Context) error {
				return access(
					ctx,
					k,
					v,
					func(v V) {
						modifier(m, k, v, lock)
					},
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
