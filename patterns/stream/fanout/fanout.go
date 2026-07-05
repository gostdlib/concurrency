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
the first error). Options that shape result delivery (WithOrdered) are not exposed, since fanout
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

// Option configures a Limited run. Only the options that affect side-effect work are exposed:
// WithStopOnErr and WithGate. Option is a distinct type rather than an alias of foreach.Option on
// purpose — foreach's result-delivery options (WithOrdered, WithMaxHeld) cannot be passed here, so an
// unsupported option whose validation error fanout would silently drop can never reach the run.
type Option func() (foreach.Option, error)

// WithStopOnErr causes the first Worker error to cancel processing of the remaining pairs. Pairs
// already dispatched still run to completion; done still closes once the run drains.
func WithStopOnErr() Option {
	return func() (foreach.Option, error) {
		return foreach.WithStopOnErr(), nil
	}
}

// WithGate retries a failing Worker with boff behind an internal gate that pauses dispatch of new
// pairs while any pair is retrying, so a struggling dependency gets time to recover instead of more
// traffic. An error wrapping ErrPermanent is never retried; a nil boff panics. Build boff with
// exponential.New.
func WithGate(boff *exponential.Backoff) Option {
	return func() (foreach.Option, error) {
		if boff == nil {
			return nil, fmt.Errorf("fanout.WithGate: boff cannot be nil: %w", ErrPermanent)
		}
		return foreach.WithGate(boff), nil
	}
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
func Limited[K, V any](ctx context.Context, name string, size int, seq iter.Seq2[K, V], w Worker[K, V], options ...Option) (done <-chan struct{}) {
	ctxLimit := context.Pool(ctx).Limit()
	if size < 1 {
		panic("fanout.Limited: cannot have a size < 1")
	}
	if ctxLimit != 0 && ctxLimit < size {
		panic(fmt.Sprintf("fanout.Limited: size %d exceeds the Context pool's limit of %d", size, ctxLimit))
	}
	if w == nil {
		panic("fanout.Limited: cannot have a nil Worker")
	}
	if seq == nil {
		panic("fanout.Limited: cannot have a nil seq")
	}

	opts := make([]foreach.Option, 0, len(options))
	for _, o := range options {
		if o == nil {
			panic("fanout.Limited: cannot have a nil Option")
		}
		opt, err := o()
		if err != nil {
			panic(err)
		}
		opts = append(opts, opt)
	}

	d := make(chan struct{})
	poolCtx := context.SetPool(ctx, context.Pool(ctx).Limited(ctx, name, size))
	fn := func(ctx context.Context, k K, v V) (struct{}, error) {
		return struct{}{}, w(ctx, k, v)
	}

	ctx = context.WithoutCancel(ctx)
	// Drive the lazy foreach range on the unbounded default pool, discarding every Response since fanout
	// has no output. The driver blocks on delivery, so keeping it off the Limited pool ensures it never
	// consumes one of size's slots (a size-1 pool would otherwise deadlock against its own driver).
	// The driver runs on the WithoutCancel ctx so Submit never declines; that is what lets us drop the
	// close-on-decline fallback and still guarantee done always closes.
	_ = worker.Default().Submit(ctx, func() {
		for range foreach.Item(poolCtx, seq, fn, opts...) {
		}
		close(d)
	})
	return d
}
