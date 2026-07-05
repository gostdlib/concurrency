/*
Package fanout runs a function over a stream of key/value pairs in parallel on a size-limited worker
pool for its side effects, without streaming results back. It is a thin, output-free wrapper over
patterns/stream/foreach: reach for foreach.Item when you need each result, and fanout when the work is
fire-and-forget — writing rows, sending events, warming a cache — and you only need to know when all of
it has finished.

Limited fans w out over any iter.Seq2. Adapt a channel, slice, or map into one with stream.Chan,
stream.Slice, or stream.Map. It returns a channel that is closed once every pair has been processed:

	ch := make(chan int)
	context.Pool(ctx).Submit(ctx, func() {
		defer close(ch)
		for i := 0; i < 100; i++ {
			ch <- i
		}
	})

	fn := func(ctx context.Context, _ int, v int) error {
		return writeRow(ctx, v) // side effect only; no result is streamed back
	}

	done := fanout.Limited(ctx, "row-writer", 8, stream.Chan(ctx, ch), fn)
	<-done // wait for every row to be written (or ignore done for pure fire-and-forget)

The Worker error is not streamed back to the caller; it is only consumed by foreach's retry/stop-on-err
options passed through as Option (WithGate to retry a flaky Worker, WithStopOnErr to cancel the run on
the first error). Options that shape result delivery (WithOrdered) have no effect here because fanout
delivers no results.
*/
package fanout

import (
	"fmt"
	"iter"

	"github.com/gostdlib/base/concurrency/worker"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream/foreach"
)

// Worker does work for a single key/value pair for its side effects. The value is passed by copy and
// Workers run concurrently, so this is safe only when each pair is independent. A returned error is not
// streamed back to the caller: it only feeds the foreach options passed to Limited (WithGate retries it,
// WithStopOnErr cancels the remaining work on it); without those options the error is dropped.
type Worker[K, V any] func(ctx context.Context, k K, v V) error

// Option configures a Limited run. It is an alias for foreach's Option, since fanout is implemented on
// foreach, but only the options that affect side-effect work are re-exported here: WithStopOnErr and
// WithGate. foreach's result-delivery options (WithOrdered, WithMaxHeld) still type-check but do
// nothing, because fanout delivers no results.
type Option = foreach.Option

// WithStopOnErr causes the first Worker error to cancel processing of the remaining pairs. Pairs
// already dispatched still run to completion; done still closes once the run drains.
func WithStopOnErr() Option {
	return foreach.WithStopOnErr()
}

// WithGate retries a failing Worker with boff behind an internal gate that pauses dispatch of new
// pairs while any pair is retrying, so a struggling dependency gets time to recover instead of more
// traffic. An error wrapping ErrPermanent is never retried, and a nil boff is an error surfaced when
// the run starts. Build boff with exponential.New.
func WithGate(boff *exponential.Backoff) Option {
	return foreach.WithGate(boff)
}

// ErrPermanent marks a Worker error that WithGate must never retry. Wrap it into an error that cannot
// succeed on a retry: fmt.Errorf("%w: %w", origErr, fanout.ErrPermanent).
var ErrPermanent = foreach.ErrPermanent

// Limited runs w over every key/value pair in seq in parallel for side effects only, on a worker pool
// Limited to size (and named name) derived from ctx's pool, and returns a channel that is closed once
// every pair has been processed. No results are streamed back; adapt a channel, slice, or map into seq
// with stream.Chan, stream.Slice, or stream.Map.
//
// At most size Workers run at once. The foreach coordinator and the goroutine that drives the run stay
// on the unbounded default pool, so neither occupies a Limited slot — a size of 1 still makes progress
// instead of deadlocking. Cancelling ctx stops dispatch of pairs not yet started and lets the pairs
// already dispatched drain; done still closes. If seq never ends — for example a stream.Chan whose
// channel is never closed — the run never finishes and done never closes; the caller owns ending seq.
func Limited[K, V any](ctx context.Context, name string, size int, seq iter.Seq2[K, V], w Worker[K, V], options ...Option) (done chan struct{}) {
	ctxLimit := context.Pool(ctx).Limit()
	if size < 1 {
		panic("fanout.Limited: cannot have a size < 1")
	}
	if ctxLimit != 0 && ctxLimit < size {
		panic(fmt.Sprintf("fanout.Limited: size %d exceeds the Context pool's limit of %d", size, ctxLimit))
	}

	done = make(chan struct{})
	poolCtx := context.SetPool(ctx, context.Pool(ctx).Limited(ctx, name, size))
	fn := func(ctx context.Context, k K, v V) (struct{}, error) {
		return struct{}{}, w(ctx, k, v)
	}
	// Drive the lazy foreach range on the unbounded default pool, discarding every Response since fanout
	// has no output. The driver blocks on delivery, so keeping it off the Limited pool ensures it never
	// consumes one of size's slots (a size-1 pool would otherwise deadlock against its own driver).
	ok := worker.Default().Submit(ctx, func() {
		for range foreach.Item(poolCtx, seq, fn, options...) {
		}
		close(done)
	})
	if !ok {
		close(done)
	}
	return done
}
