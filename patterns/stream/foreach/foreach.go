/*
Package foreach runs a function over every key/value pair in an iter.Seq2 in parallel and streams the
results back. It is the parallel analog of a "for range" loop whose body transforms each element: Item
takes a sequence and an ItemFunc and returns a lazy iter.Seq2 yielding one stream.Result per input
pair under that pair's key — the value the ItemFunc produced or the error it returned.

foreach is useful when each pair needs expensive, independent work: calling an API, reading a database,
transforming a record. For cheap operations a plain "for" loop in a single goroutine will be faster, as
the cost of parallelism will outweigh the gain.

Say you want to multiply a slice of numbers by 5 in parallel:

	nums := []int{1, 2, 3, 4}

	fn := func(ctx context.Context, _ int, i int) (int, error) {
		return i * 5, nil
	}

	for k, resp := range foreach.Item(ctx, stream.Slice(nums), fn) {
		if resp.Err != nil {
			// Handle the error.
			continue
		}
		fmt.Println(k, resp.V)
	}

Item is lazy: no work starts until the returned sequence is ranged, and stopping the range early
cancels the work that has not started while waiting for the work already dispatched — side effects of
started ItemFuncs always happen before the range returns. Each range over the returned sequence
processes the input again, so range it once. Any iter.Seq2 works as input — over a slice
(stream.Slice, keyed by index), a map (stream.Map, keyed by key), or a channel (stream.Chan, keyed by
receive index).

By default responses arrive in completion order: whatever finishes first yields first, under its input
key. Pass WithOrdered() to receive responses in input order instead; that holds completed later responses
in memory until every earlier one has been yielded, and WithMaxHeld bounds how many are held by pausing
dispatch until the consumer catches up. Without WithOrdered, WithMaxHeld sizes the delivery buffer.

Errors do not stop processing: they arrive in-band as Response.Err and every other pair still yields a
response. Pass WithStopOnErr to cancel the remaining work on the first error; every pair already
dispatched still yields a Response (the error's own, completed values, or the cancellation error for
pairs cut short), then the iteration ends. For side effects only, use an ItemFunc that returns a zero
value (say struct{}) and check only Response.Err.

Item runs the ItemFuncs on the worker pool attached to the Context. If the pool is unlimited, up to
runtime.NumCPU() * 10 workers are used.

Failures can retry with backpressure. WithGate takes a base/retry/exponential Backoff: a failed
ItemFunc retries with it, and while any pair is retrying, dispatch of new pairs pauses — a dependency
having a bad moment gets time to recover instead of more traffic. The first attempt of every pair runs
ungated, an error wrapping ErrPermanent is never retried, and work already dispatched keeps running.
The gate belongs to a single Item range; bound recovery with the Backoff's Policy or a ctx deadline.
See the example on WithGate.
*/
package foreach

import (
	"errors"
	"fmt"
	"iter"
	"runtime"

	"github.com/gostdlib/base/concurrency/worker"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream"
)

// ErrPermanent prevents a retry from happening when a call is wrapped in a retry from
// base/retry/exponential. Errors that cannot succeed on a retry (option validation) wrap it. Usually
// added to an existing error with fmt.Errorf("%w: %w", origErr, ErrPermanent).
var ErrPermanent = exponential.ErrPermanent

// ItemFunc transforms one key/value pair from the input sequence into a result. The value is passed by
// copy, so mutating it is not observable to the caller unless the sequence yields a pointer or
// reference type; because ItemFuncs run concurrently, that is only safe when each pair is independent.
type ItemFunc[K, V, R any] func(ctx context.Context, k K, v V) (R, error)

// options holds Item's settings, built by applying each Option over the zero defaults.
type options struct {
	// stopOnErr cancels the remaining work on the first ItemFunc error. Default false: errors arrive
	// in-band and processing continues.
	stopOnErr bool
	// boff, when non-nil, retries a failed ItemFunc behind the gate. Default nil (no retries).
	boff *exponential.Backoff
	// gate is internal wiring, not an option: Item installs it when boff is set, and dispatch pauses
	// while any pair is retrying under it.
	gate *gate
	// ordered yields responses in input order through the order engine. Default false (completion
	// order through the delivery buffer).
	ordered bool
	// maxHeld bounds undelivered responses: the delivery buffer's capacity when unordered, the order
	// engine's held bound when ordered. Default 0, resolved by held() to the worker count capped at
	// maxHeldDefault.
	maxHeld int
	// orderWait is internal wiring, not an option: the ordered path installs it to block while the
	// order engine holds too many undelivered responses.
	orderWait func(ctx context.Context) error
}

// resolveOptions applies opts in order over the zero defaults.
func resolveOptions(opts []Option) (options, error) {
	o := options{}
	for _, opt := range opts {
		var err error
		o, err = opt(o)
		if err != nil {
			return o, err
		}
	}
	return o, nil
}

// wait is Item's pre-dispatch checkpoint: it blocks until every configured pause condition (gate open,
// order below its held bound) holds on a single pass. The gate is re-checked after an order wait, so a
// retry that engages while dispatch is parked at a full order is honored before the next dispatch
// instead of leaking one ItemFunc through the paused gate.
func (o options) wait(ctx context.Context) error {
	for {
		if o.gate != nil {
			if err := o.gate.wait(ctx); err != nil {
				return err
			}
		}
		if o.orderWait != nil {
			if err := o.orderWait(ctx); err != nil {
				return err
			}
		}
		if o.gate == nil || o.orderWait == nil || o.gate.open() {
			return nil
		}
	}
}

// maxHeldDefault caps the default held bound: without it, a service-wide pool Limited to a huge count
// would make every Item call eagerly allocate a delivery buffer that size and couple its memory
// behavior to a compute knob owned elsewhere.
const maxHeldDefault = 1024

// held resolves WithMaxHeld: the option's value when set, otherwise the pool's worker count capped at
// maxHeldDefault.
func (o options) held(p *worker.Pool) int {
	if o.maxHeld > 0 {
		return o.maxHeld
	}
	return min(p.Limit(), maxHeldDefault)
}

// Option is an option for Item. Options are applied in order, so a later option overrides an earlier
// one; all options are optional, with zero values asking for the documented defaults.
type Option func(o options) (options, error)

// WithStopOnErr causes the first ItemFunc error to cancel processing of the remaining pairs. Every
// pair already dispatched still yields a Response — the error's own, values that completed, and a
// Response carrying the cancellation error for pairs that were cancelled before or while running;
// pairs never dispatched yield nothing. A response is dropped only when the consumer abandons the
// range by breaking out of it.
func WithStopOnErr() Option {
	return func(o options) (options, error) {
		o.stopOnErr = true
		return o, nil
	}
}

// WithGate makes Item retry a failed ItemFunc with boff behind an internal gate. The first attempt of
// every pair runs ungated, but once an attempt fails with a retryable error, that pair retries under
// boff while the gate pauses dispatch of new pairs — a dependency having a bad moment gets time to
// recover instead of more traffic. Several retrying pairs keep the gate closed until the last one
// resolves; work already dispatched is unaffected. An ItemFunc error wrapping ErrPermanent is never
// retried, and a retrying ItemFunc should honor its Context so cancellation can end the retry.
//
// boff must be constructed with exponential.New (a zero-value Backoff retries in a zero-delay loop
// with no bound), and retries run until the error resolves, is classified permanent, or ctx ends —
// against a dependency that never recovers, bound the recovery with the Policy's MaxAttempts or a ctx
// deadline or the gate stays closed and the range does not finish. The gate belongs to one Item range
// and engages only on ItemFunc errors: concurrent Item calls against the same dependency gate
// independently, so shared protection needs a shared Limited pool instead. A nil boff is an error.
func WithGate(boff *exponential.Backoff) Option {
	return func(o options) (options, error) {
		if boff == nil {
			return o, fmt.Errorf("foreach.WithGate: boff cannot be nil: %w", ErrPermanent)
		}
		o.boff = boff
		return o, nil
	}
}

// WithOrdered makes Item yield responses in input order instead of completion order. A response is
// held until every earlier pair's response has been yielded, so one slow pair causes completed later
// responses to accumulate in memory; WithMaxHeld bounds that accumulation by pausing dispatch.
func WithOrdered() Option {
	return func(o options) (options, error) {
		o.ordered = true
		return o, nil
	}
}

// WithMaxHeld bounds how many undelivered responses Item holds: with WithOrdered it pauses dispatch
// of new work at the bound, resuming as the consumer drains; without it, it sizes the delivery
// buffer, so workers block on delivery when the consumer lags. The ordered bound is approximate:
// ItemFuncs already in flight can still deliver, so up to max plus the pool size may be held.
// Defaults to the worker count, capped at 1024; max must be > 0.
func WithMaxHeld(max int) Option {
	return func(o options) (options, error) {
		if max < 1 {
			return o, fmt.Errorf("foreach.WithMaxHeld: max must be > 0, got %d: %w", max, ErrPermanent)
		}
		o.maxHeld = max
		return o, nil
	}
}

// keyed carries a pair's input key alongside its response from the workers to the consumer.
type keyed[K, R any] struct {
	k    K
	resp stream.Result[R]
}

// Item runs fn on every key/value pair yielded by seq, in parallel on the worker pool attached to ctx,
// and returns a lazy iterator that yields one stream.Result per pair under the pair's input key —
// fn's value or its error. No work starts until the returned sequence is ranged, and ranging again
// processes the input again. Adapt a channel, slice, or map into seq with stream.Chan, stream.Slice,
// or stream.Map. A nil in or fn panics (those adapters yield a non-nil empty iterator, so an empty
// input yields nothing without a nil seq). If an option is invalid, the sequence yields a single
// Response whose Err reports it (wrapping ErrPermanent) under K's zero value.
//
// Stopping the range early (or cancelling ctx) cancels the work that has not started, and returning
// from the range — normally or by breaking — waits for the ItemFuncs already dispatched to finish, so
// their side effects happen before the range returns. If ctx is cancelled, every pair already
// dispatched still yields a Response (values that completed, or the cancellation error for pairs cut
// short), but pairs never dispatched yield nothing — check ctx.Err() after the range to distinguish a
// truncated run from a complete one. A sequence blocked on an
// external source (stream.Chan on an idle channel) is released only by its source or the Context the
// sequence captured; Item pulls it on a separate goroutine so an early break still returns promptly,
// but that puller parks until the source unblocks and at most one pulled pair is discarded. Calling
// Item from inside an ItemFunc on a Limited pool can deadlock when the outer calls hold every slot —
// nested parallelism needs a pool sized for both levels.
func Item[K, V, R any](ctx context.Context, in iter.Seq2[K, V], fn ItemFunc[K, V, R], options ...Option) iter.Seq2[K, stream.Result[R]] {
	if in == nil {
		panic("foreach.Item: in cannot be nil")
	}
	if fn == nil {
		panic("foreach.Item: fn cannot be nil")
	}
	o, err := resolveOptions(options)
	if err != nil {
		return func(yield func(K, stream.Result[R]) bool) {
			var zero K
			yield(zero, stream.Result[R]{Err: err})
		}
	}
	if o.ordered {
		return orderedSeq(ctx, in, fn, o)
	}
	return unorderedSeq(ctx, in, fn, o)
}

// pool returns the Context's worker pool, bounded to runtime.NumCPU() * 10 workers when unlimited.
func pool(ctx context.Context) *worker.Pool {
	p := context.Pool(ctx)
	if p.Limit() == 0 {
		p = p.Limited(ctx, "", runtime.NumCPU()*(10))
	}
	return p
}

// input carries one pair from the puller to the dispatch loop.
type input[K, V any] struct {
	k K
	v V
}

// unorderedSeq streams responses in completion order through a bounded channel; the channel is the
// backpressure, so no order engine or checkpoint is involved.
func unorderedSeq[K, V, R any](ctx context.Context, seq iter.Seq2[K, V], fn ItemFunc[K, V, R], o options) iter.Seq2[K, stream.Result[R]] {
	return func(yield func(K, stream.Result[R]) bool) {
		// Every range gets its own copy of the resolved options and its own wiring (the gate), so
		// ranging the returned sequence again — even concurrently — shares nothing with a prior range.
		o := o
		if o.boff != nil {
			o.gate = &gate{}
		}

		ctx, cancel := context.WithCancel(ctx)

		p := pool(ctx)
		out := make(chan keyed[K, R], o.held(p))
		// gone signals that the consumer abandoned the range: it is the only thing that may drop a
		// delivered response. Cancellation alone must not — the consumer may still be draining.
		gone := make(chan struct{})
		d := &dispatcher[K, V, R]{
			seq:     seq,
			fn:      fn,
			o:       o,
			cancel:  cancel,
			pull:    make(chan input[K, V]),
			done:    make(chan struct{}),
			workers: p,
			deliver: func(_ int, k K, resp stream.Result[R]) {
				select {
				case out <- keyed[K, R]{k: k, resp: resp}:
				case <-gone:
				}
			},
			finish: func() { close(out) },
		}
		d.launch(ctx, context.Tasks(ctx))
		// The join: returning from the range — normally or by breaking — releases blocked senders,
		// cancels remaining work and waits for the dispatched ItemFuncs to finish, so their side
		// effects happen before the range returns.
		defer func() {
			close(gone)
			cancel()
			<-d.done
		}()

		for kv := range out {
			if !yield(kv.k, kv.resp) {
				return
			}
		}
	}
}

// orderedSeq streams responses in input order through the order engine, keyed by the dispatch counter
// so K stays fully generic. WithMaxHeld (or its worker-count default) pauses dispatch via the
// checkpoint while the engine holds too many undelivered responses.
func orderedSeq[K, V, R any](ctx context.Context, seq iter.Seq2[K, V], fn ItemFunc[K, V, R], o options) iter.Seq2[K, stream.Result[R]] {
	return func(yield func(K, stream.Result[R]) bool) {
		// Every range gets its own copy of the resolved options and its own wiring (gate, orderWait),
		// so ranging the returned sequence again — even concurrently — shares nothing with a prior
		// range.
		o := o
		if o.boff != nil {
			o.gate = &gate{}
		}

		ctx, cancel := context.WithCancel(ctx)

		p := pool(ctx)
		ord := newOrder[keyed[K, R]]()
		held := o.held(p)
		o.orderWait = func(ctx context.Context) error { return ord.waitBelow(ctx, held) }
		d := &dispatcher[K, V, R]{
			seq:     seq,
			fn:      fn,
			o:       o,
			cancel:  cancel,
			pull:    make(chan input[K, V]),
			done:    make(chan struct{}),
			workers: p,
			deliver: func(i int, k K, resp stream.Result[R]) {
				ord.add(i, keyed[K, R]{k: k, resp: resp})
			},
			finish: ord.finish,
		}
		d.launch(ctx, context.Tasks(ctx))
		// The join: returning from the range — normally or by breaking — cancels remaining work and
		// waits for the dispatched ItemFuncs to finish, so their side effects happen before the range
		// returns.
		defer func() {
			cancel()
			<-d.done
		}()

		for _, kv := range ord.all(ctx) {
			if !yield(kv.k, kv.resp) {
				return
			}
		}
	}
}

// errOrderFinished stops dispatch if waitBelow ever observes a finished order. The dispatcher is the
// only finisher and finishes only after all in-flight work completes, so observing it means an
// internal bug; returning an error just ends dispatch cleanly instead of hanging or panicking in add.
var errOrderFinished = errors.New("foreach: internal order finished while dispatch was paused")
