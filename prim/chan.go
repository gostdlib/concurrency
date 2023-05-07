package prim

import (
	"context"
	"runtime"
	"sync"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/concurrency/goroutines/limited"
)

// StreamResult is a result from a Stream operation.
type StreamResult[T any] struct {
	// Value is the value returned in the stream.
	Value T
	// Err is the error returned in the stream.
	Err error
}

// Chan applies Mutator "mut" to each element in "input" using the goroutines Pool. If p == nil,
// p becomes a limited.Pool using up to runtime.NumCPU().
// If the Mutator has an error, the error will be returned in the StreamResult but this will not stop
// processing. If a subOpts is passed, it will be applied to each goroutine. If a subOpts causes an
// error (because it is invalid), this will panic. You can cancel the context to stop processing.
func Chan[T, R any](ctx context.Context, input chan T, mut Mutator[T, R], p goroutines.Pool, subOpts ...goroutines.SubmitOption) chan StreamResult[R] {
	if p == nil {
		var err error
		p, err = limited.New("", runtime.NumCPU())
		if err != nil {
			panic(err) // This should never happen.
		}
		defer p.Close()
	}

	out := make(chan StreamResult[R], 1)

	go func() {
		defer close(out)

		wg := sync.WaitGroup{}

		for {
			var in T
			var ok bool
			select {
			case <-ctx.Done():
				wg.Wait()
				out <- StreamResult[R]{Err: ctx.Err()}
				return
			case in, ok = <-input:
				if !ok {
					wg.Wait()
					return
				}
			}

			wg.Add(1)
			err := p.Submit(
				ctx,
				func(ctx context.Context) {
					defer wg.Done()

					r, err := mut(ctx, in)
					if err != nil {
						out <- StreamResult[R]{Err: err}
						return
					}
					out <- StreamResult[R]{Value: r}
				},
				subOpts...,
			)
			if err != nil {
				panic(err)
			}
		}
	}()
	return out
}
