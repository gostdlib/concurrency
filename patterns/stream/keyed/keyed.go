/*
Package keyed runs a function over a stream of key/value pairs with per-key ordering: pairs that
share a partition key are processed serially in input order, while different keys run in parallel.
It is the parallel analog of a "for range" loop whose body must not run concurrently for the same
entity — applying a per-account changelog, driving a per-connection state machine, consuming a
partitioned source (Kafka/EventHubs) whose per-partition order a plain fan-out would throw away.

Item takes an iter.Seq2 of pairs, a KeyFunc that extracts each pair's partition key, and an
ItemFunc, and returns a lazy iter.Seq2 yielding one stream.Result per pair under its input key.
Adapt a channel, slice, or map into the input with stream.Chan, stream.Slice, or stream.Map:

	fn := func(ctx context.Context, _ int, order Order) (Receipt, error) {
		return apply(ctx, order) // never runs concurrently for the same order.Account
	}

	for k, resp := range keyed.Item(ctx, stream.Slice(orders), keyFn, fn) {
		if resp.Err != nil {
			// Handle the error.
			continue
		}
		// Use resp.V.
	}

where keyFn returns the partition key, e.g. func(_ int, o Order) string { return o.Account }.

Work is routed to lanes; a lane processes its pairs serially, so pairs sharing a lane run one at a
time. The lane strategy is selectable: the default (or WithFixedLanes) hashes keys onto a fixed set
of lanes — constant memory, but two keys that hash to the same lane block each other. A single hot
key can never exceed one lane's throughput; that is inherent to per-key ordering, not a flaw.

Results arrive in completion order under each pair's input key; per-key execution order is always
preserved regardless of delivery order. WithOrdered instead delivers results in input order across all
keys, at the cost of buffering finished results behind a slow key. Errors arrive in-band as
stream.Result.Err. Item is lazy:
no work starts until the returned sequence is ranged, and breaking out of the range (or cancelling
ctx) stops dispatch of pairs not yet started and waits for those already running.
*/
package keyed

import (
	"fmt"
	"hash/maphash"
	"iter"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream"
)

// ErrPermanent marks an error that cannot succeed on retry, such as an invalid option. Check for it
// with errors.Is(err, ErrPermanent). It is the same sentinel as exponential.ErrPermanent, re-exported
// here as foreach and fanout do so callers need not import the retry package.
var ErrPermanent = exponential.ErrPermanent

// ErrTornDown is set on a pair's Result once the run is tearing down after an ItemFunc panicked. It
// is permanent — the run does not recover. The original panic re-raises on the consumer's goroutine
// when the range ends, as a PanicError.
var ErrTornDown = fmt.Errorf("keyed: run torn down after an ItemFunc panic: %w", ErrPermanent)

// PanicError is the value Item re-panics with, on the consumer's goroutine, after an ItemFunc
// panicked. Value is the original panic and Stack is the stack captured in the lane at recover time —
// the live re-panic stack is the consumer's, not the lane's, so the captured Stack is how the origin
// is preserved. Recover it with a type assertion to PanicError.
type PanicError struct {
	// Value is the value the ItemFunc passed to panic().
	Value any
	// Stack is the stack captured in the lane goroutine when the panic was recovered.
	Stack []byte
}

// Error implements error.
func (e PanicError) Error() string {
	return fmt.Sprintf("keyed: panic in ItemFunc: %v\n\noriginating stack:\n%s", e.Value, e.Stack)
}

// teardown records the first ItemFunc panic and tears the run down. once makes the first panic win;
// info is written inside once.Do (so it is written at most once) and read by the consumer only after
// close(out), which happens-after every lane returns — that chain gives the read its visibility
// without an atomic.
type teardown struct {
	once   sync.Once
	info   *PanicError
	cancel context.CancelFunc
}

// onPanic records rec as the first panic (capturing the lane's stack) and cancels the run. Later
// panics during teardown are dropped.
func (t *teardown) onPanic(rec any) {
	t.once.Do(func() {
		t.info = &PanicError{Value: rec, Stack: debug.Stack()}
		t.cancel()
	})
}

// raise re-panics with the recorded panic, if any. The caller must have drained out first so every
// lane has returned and info is visible. It runs on the consumer's goroutine when the range ends,
// whether the range finished naturally or was broken — a broken range must still surface a panic.
func (t *teardown) raise() {
	if t.info != nil {
		panic(*t.info)
	}
}

// call runs fn under a recover so a panic in user code does not crash the process. On a panic it
// records the origin and tears the run down (td.onPanic) and returns the zero result with
// ErrTornDown; the real panic re-raises from the consumer's range end.
func call[K, V, R any](ctx context.Context, td *teardown, fn ItemFunc[K, V, R], k K, v V) (out R, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			td.onPanic(rec)
			err = ErrTornDown
		}
	}()
	return fn(ctx, k, v)
}

// laneBuffer is the per-lane inbox capacity. It must be > 0 so the single dispatcher can enqueue to
// one lane and move on to others instead of blocking on a busy lane; a full inbox (a hot lane)
// applies backpressure that pauses the dispatcher rather than growing memory without bound.
const laneBuffer = 16

// defaultIdle is the WithLanePerKey idle timeout used when the caller passes 0: a key's lane retires
// after this long with no new pair, freeing its goroutine.
const defaultIdle = 500 * time.Millisecond

// Result is the result of processing one pair: a value or an error. It is an alias of the stream
// package's Result so callers share one result type across the stream patterns.
type Result[T any] = stream.Result[T]

// Seq is what Item returns: a lazy iterator yielding one Result per input pair, keyed by the pair's
// input key. It is an alias of iter.Seq2, so range it like any other sequence.
type Seq[K, R any] = iter.Seq2[K, Result[R]]

// KeyFunc extracts the partition key from a key/value pair. Pairs that return the same key are
// processed serially in input order; pairs with different keys may run in parallel. Keys are strings
// — stringify whatever identifies the partition (account, tenant, entity id). The value is passed by
// copy, so KeyFunc must not mutate it.
type KeyFunc[K, V any] func(k K, v V) string

// ItemFunc processes one key/value pair into a result. ItemFuncs for the same partition key never
// run concurrently, so per-key state they touch needs no locking; ItemFuncs for different keys do
// run concurrently, so anything they share must be safe for that.
type ItemFunc[K, V, R any] func(ctx context.Context, k K, v V) (R, error)

// options holds Item's settings, built by applying each Option over the zero defaults.
type options struct {
	// fixedLanes is the WithFixedLanes count; 0 means use the default lane count.
	fixedLanes int
	// perKey selects the WithLanePerKey strategy.
	perKey bool
	// idle is WithLanePerKey's idle-retire timeout; 0 resolves to defaultIdle.
	idle time.Duration
	// ordered selects WithOrdered: deliver results in input order rather than completion order.
	ordered bool
}

// validate checks option combinations that no single With* can. The two lane strategies are
// mutually exclusive; asking for both is a permanent configuration error.
func (o options) validate() error {
	if o.perKey && o.fixedLanes > 0 {
		return fmt.Errorf("keyed.Item: WithFixedLanes and WithLanePerKey are mutually exclusive: %w", ErrPermanent)
	}
	return nil
}

// resolveOptions applies opts in order over the zero defaults, then validates the combination.
func resolveOptions(opts []Option) (options, error) {
	o := options{}
	for _, opt := range opts {
		var err error
		o, err = opt(o)
		if err != nil {
			return o, err
		}
	}
	return o, o.validate()
}

// Option configures an Item run. Options are applied in order, so a later option overrides an
// earlier one; every option is optional and the zero value asks for the documented default.
type Option func(o options) (options, error)

// WithFixedLanes sets the number of lanes keys are hashed onto. More lanes means fewer unrelated
// keys collide on one lane (less head-of-line blocking) at the cost of more concurrent ItemFuncs;
// the right value is roughly the parallelism you want, near the core count if no activity leaves
// memory (IO of any type changes this equation). Keys are distributed by
// hash, so a lane may serve many keys and a key always maps to the same lane for the run. The
// default is runtime.NumCPU(). n must be > 0.
func WithFixedLanes(n int) Option {
	return func(o options) (options, error) {
		if n < 1 {
			return o, fmt.Errorf("keyed.WithFixedLanes: n must be > 0, got %d: %w", n, ErrPermanent)
		}
		o.fixedLanes = n
		return o, nil
	}
}

// WithLanePerKey selects the per-key lane strategy: every distinct key gets its own lane (one
// goroutine and inbox), so no two keys ever block each other — the actor model, where a lane owns
// per-key state that its ItemFuncs touch without locking. A lane retires after idle with no new pair
// (idle 0 uses a default of 500ms), freeing its goroutine; a later pair for that key starts a fresh lane, so
// state does not survive an idle gap. Use this for bounded, long-lived keys (a session, a connection,
// an owned shard); for high-cardinality transient keys prefer WithFixedLanes. It is mutually
// exclusive with WithFixedLanes. idle must be >= 0.
func WithLanePerKey(idle time.Duration) Option {
	return func(o options) (options, error) {
		if idle < 0 {
			return o, fmt.Errorf("keyed.WithLanePerKey: idle must be >= 0, got %s: %w", idle, ErrPermanent)
		}
		o.perKey = true
		o.idle = idle
		return o, nil
	}
}

// WithOrdered delivers results in input order: the Nth pair yielded by in is the Nth Result yielded by
// Item, regardless of which pair finishes first. Without it, results arrive in completion order under
// each pair's input key. Per-key execution order is preserved either way — WithOrdered composes an
// input-order delivery on top of it and works with both lane strategies. The cost is memory: a result
// whose input position has not yet been reached is buffered until the gap fills, so one slow key can
// hold a growing set of finished results behind it. Prefer completion order unless a downstream stage
// truly needs input order across keys.
func WithOrdered() Option {
	return func(o options) (options, error) {
		o.ordered = true
		return o, nil
	}
}

// emit carries a pair's input key alongside its result from a lane to the consumer. seq is the pair's
// 0-based input position, used only by WithOrdered to reorder results into input order at delivery.
type emit[K, R any] struct {
	k    K
	seq  int
	resp Result[R]
}

// laneItem is one pair handed to a lane for processing. seq is the pair's 0-based input position,
// carried through so WithOrdered can reorder results into input order at delivery.
type laneItem[K, V any] struct {
	k   K
	seq int
	v   V
}

// Item runs fn over every key/value pair yielded by in, serializing pairs by the partition key that
// key returns and running different keys in parallel, and returns a lazy iterator that yields one
// stream.Result per pair under the pair's input key. No work starts until the returned sequence is
// ranged, and ranging it again processes the input again. Adapt a channel, slice, or map into in
// with stream.Chan, stream.Slice, or stream.Map. A nil in, key, or fn panics. If an option is
// invalid, the sequence yields a single Result whose Err reports it under K's zero value.
//
// Breaking out of the range (or cancelling ctx) stops dispatch of pairs not yet started and waits
// for the ItemFuncs already running to finish, so their side effects happen before the range
// returns; results from pairs that were in flight may still be delivered before it ends.
func Item[K, V, R any](ctx context.Context, in iter.Seq2[K, V], key KeyFunc[K, V], fn ItemFunc[K, V, R], options ...Option) Seq[K, R] {
	if in == nil {
		panic("keyed.Item: in cannot be nil")
	}
	if key == nil {
		panic("keyed.Item: key cannot be nil")
	}
	if fn == nil {
		panic("keyed.Item: fn cannot be nil")
	}

	o, err := resolveOptions(options)
	if err != nil {
		return func(yield func(K, Result[R]) bool) {
			var zero K
			yield(zero, Result[R]{Err: err})
		}
	}

	r := run[K, V, R]{ctx: ctx, in: in, key: key, fn: fn, ordered: o.ordered}
	if o.perKey {
		idle := o.idle
		if idle <= 0 {
			idle = defaultIdle
		}
		return r.perKeyLanes(idle)
	}

	lanes := o.fixedLanes
	if lanes < 1 {
		lanes = runtime.NumCPU()
	}
	return r.fixedLanes(lanes)
}

// run bundles one Item call's inputs so the lane-strategy methods keep a short signature. K and V
// are the input pair types and R is the ItemFunc result type.
type run[K, V, R any] struct {
	ctx     context.Context
	in      iter.Seq2[K, V]
	key     KeyFunc[K, V]
	fn      ItemFunc[K, V, R]
	ordered bool
}

// deliver drains out to the consumer and is shared by both lane strategies. In completion order
// (ordered false) it yields each emit as it arrives. In input order (ordered true) it buffers emits by
// their seq and releases the contiguous run starting at the next-expected position, so results yield in
// input order across keys. On a broken range it cancels the run, drains out so the lanes unblock and
// the closer can close it, and re-raises any recorded panic; it also re-raises once out is exhausted.
func deliver[K, R any](out chan emit[K, R], ordered bool, td *teardown, yield func(K, Result[R]) bool) {
	stop := func() {
		td.cancel()
		for range out { //nolint:revive // drain so the lanes unblock and the closer can close out
		}
		td.raise() // a broken range must still surface a panic
	}

	if !ordered {
		for it := range out {
			if !yield(it.k, it.resp) {
				stop()
				return
			}
		}
		td.raise()
		return
	}

	// Ordered: pending holds emits whose input position has not yet been reached; next is the position
	// to yield next. Each arrival fills pending, then the loop releases every contiguous position from
	// next onward. A slow key stalls next, so pending can grow until that key's result arrives.
	pending := map[int]emit[K, R]{}
	next := 0
	for it := range out {
		pending[it.seq] = it
		for {
			e, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			next++
			if !yield(e.k, e.resp) {
				stop()
				return
			}
		}
	}
	td.raise()
}

// fixedLanes builds the lazy sequence for the fixed-lane strategy: n lane goroutines each drain a
// buffered inbox and run fn serially, a dispatcher hashes each input pair to a lane, and a closer
// closes the output once every lane has finished. Cancelling ctx or breaking the range stops
// dispatch and drains the in-flight work without leaking a goroutine.
func (r run[K, V, R]) fixedLanes(n int) Seq[K, R] {
	return func(yield func(K, Result[R]) bool) {
		ctx, cancel := context.WithCancel(r.ctx)
		defer cancel()

		td := &teardown{cancel: cancel}
		out := make(chan emit[K, R], n)
		inboxes := make([]chan laneItem[K, V], n)

		// Each lane drains its inbox and runs fn serially. On cancellation it drains its inbox so the
		// dispatcher's send unblocks, then exits; the closer waits for every lane before closing out.
		g := sync.Group{}
		for i := range inboxes {
			inboxes[i] = make(chan laneItem[K, V], laneBuffer)
			inbox := inboxes[i]
			g.Go(ctx, func(ctx context.Context) error {
				for it := range inbox {
					v, ferr := call(ctx, td, r.fn, it.k, it.v)
					select {
					case out <- emit[K, R]{k: it.k, seq: it.seq, resp: Result[R]{V: v, Err: ferr}}:
					case <-ctx.Done():
						for range inbox { //nolint:revive // drain so the dispatcher's send unblocks
						}
						return nil
					}
				}
				return nil
			})
		}

		// One coordinator goroutine dispatches every pair, closes the inboxes, waits for the lanes, then
		// closes out. It runs on the default pool, never the Context's pool, which may be Limited: the
		// coordinator lives for the whole run, so a slot it held would be one fewer slot for real work, and
		// against an already-saturated Limited pool it would wait for a slot that never frees while the run
		// delivered nothing. The lanes run on g's own goroutines (a sync.Group with no Pool), so they need
		// no slot either — a keyed run consumes none of the caller's limit. The seed is fixed for the run,
		// so a key always maps to the same lane. Submitted on a WithoutCancel ctx so Submit never declines;
		// the closure honors cancellation through the captured ctx.
		seed := maphash.MakeSeed()
		submitCtx := context.WithoutCancel(ctx)
		_ = context.Pool(ctx).Default().Submit(submitCtx, func() {
			dispatch := func() {
				// Closing every inbox on the way out ends the lanes whether the input was exhausted or ctx
				// was cancelled, so it must run before the g.Wait below.
				defer func() {
					for _, inbox := range inboxes {
						close(inbox)
					}
				}()
				seq := 0
				for k, v := range r.in {
					idx := int(maphash.String(seed, r.key(k, v)) % uint64(n))
					select {
					case inboxes[idx] <- laneItem[K, V]{k: k, seq: seq, v: v}:
					case <-ctx.Done():
						return
					}
					seq++
				}
			}
			dispatch()
			// Wait on a WithoutCancel ctx so a lane still delivering is not abandoned before out is closed
			// (which would panic a send on a closed channel); every lane exits once its inbox closes.
			_ = g.Wait(context.WithoutCancel(ctx))
			close(out)
		})

		// Drain out to the consumer, reordering into input order if WithOrdered was set. A lane panic is
		// re-raised on the consumer's goroutine once out is exhausted (or the range is broken).
		deliver(out, r.ordered, td, yield)
	}
}

// perLane is one key's lane in the per-key strategy: a buffered inbox its goroutine drains serially,
// plus an in-flight count guarded by keyReg.mu so the reap-vs-dispatch race is safe.
type perLane[K, V any] struct {
	inbox    chan laneItem[K, V]
	inflight int
}

// keyReg is the shared registry for one per-key Item run: the live lanes, the result channel, and
// the idle timeout. Bundling these lets the lane goroutine method keep a short signature. mu guards
// lanes and every perLane.inflight.
type keyReg[K, V, R any] struct {
	fn   ItemFunc[K, V, R]
	out  chan emit[K, R]
	idle time.Duration
	td   *teardown

	mu    sync.Mutex
	lanes map[string]*perLane[K, V]
}

// perKeyLanes builds the lazy sequence for the per-key lane strategy: a lane (one goroutine and
// inbox) is created on demand for each key and retires once it has been idle for the idle timeout
// with nothing in flight. The dispatcher get-or-creates a lane under mu and bumps its in-flight
// count before sending, so a lane it just reserved is never retired out from under it. Cancelling
// ctx or breaking the range stops dispatch and drains in-flight work without leaking a goroutine.
func (r run[K, V, R]) perKeyLanes(idle time.Duration) Seq[K, R] {
	return func(yield func(K, Result[R]) bool) {
		ctx, cancel := context.WithCancel(r.ctx)
		defer cancel()

		reg := &keyReg[K, V, R]{
			fn:    r.fn,
			out:   make(chan emit[K, R], runtime.NumCPU()),
			idle:  idle,
			td:    &teardown{cancel: cancel},
			lanes: map[string]*perLane[K, V]{},
		}
		g := sync.Group{}
		submitCtx := context.WithoutCancel(ctx)

		// One coordinator goroutine dispatches every pair, closes the live lanes' inboxes, waits for the
		// lanes, then closes out. It runs on the default pool, never the Context's pool, which may be
		// Limited: the coordinator lives for the whole run, so a slot it held would be one fewer slot for
		// real work, and against an already-saturated Limited pool it would wait for a slot that never
		// frees while the run delivered nothing. The lanes run on g's own goroutines (a sync.Group with no
		// Pool), so they need no slot either — a keyed run consumes none of the caller's limit. Every g.Go
		// therefore happens before the g.Wait below, on this one goroutine. The dispatcher get-or-creates a
		// lane per key and hands it the pair; inflight is bumped under mu before the send so a concurrent
		// reap sees the reservation and keeps the lane alive.
		_ = context.Pool(ctx).Default().Submit(submitCtx, func() {
			dispatch := func() {
				seq := 0
				for k, v := range r.in {
					p := r.key(k, v)
					reg.mu.Lock()
					lane, ok := reg.lanes[p]
					if !ok {
						lane = &perLane[K, V]{inbox: make(chan laneItem[K, V], laneBuffer)}
						reg.lanes[p] = lane
						g.Go(ctx, func(ctx context.Context) error {
							reg.runLane(ctx, p, lane)
							return nil
						})
					}
					lane.inflight++
					reg.mu.Unlock()

					select {
					case lane.inbox <- laneItem[K, V]{k: k, seq: seq, v: v}:
					case <-ctx.Done():
						return
					}
					seq++
				}
			}
			dispatch()
			// Close every live lane's inbox so they drain and exit at once rather than each waiting out the
			// idle timeout; this must run before the g.Wait.
			reg.mu.Lock()
			for _, lane := range reg.lanes {
				close(lane.inbox)
			}
			reg.mu.Unlock()
			// Wait on a WithoutCancel ctx so a lane still delivering is not abandoned before out is closed.
			_ = g.Wait(context.WithoutCancel(ctx))
			close(reg.out)
		})

		// Drain out to the consumer, reordering into input order if WithOrdered was set. A lane panic is
		// re-raised on the consumer's goroutine once out is exhausted (or the range is broken).
		deliver(reg.out, r.ordered, reg.td, yield)
	}
}

// runLane is one per-key lane's goroutine: it drains inbox and runs fn serially, and retires when it
// has been idle for the idle timeout with nothing in flight. Retiring deletes the lane under mu, so a
// dispatcher that reserved it (inflight > 0) keeps it alive and a later pair for the key gets a fresh
// lane. It exits at once when the dispatcher closes inbox (end of input) or ctx is cancelled.
func (reg *keyReg[K, V, R]) runLane(ctx context.Context, key string, lane *perLane[K, V]) {
	timer := time.NewTimer(reg.idle)
	defer timer.Stop()

	for {
		select {
		case it, ok := <-lane.inbox:
			if !ok {
				return
			}
			v, ferr := call(ctx, reg.td, reg.fn, it.k, it.v)
			select {
			case reg.out <- emit[K, R]{k: it.k, seq: it.seq, resp: Result[R]{V: v, Err: ferr}}:
			case <-ctx.Done():
				return
			}
			reg.mu.Lock()
			lane.inflight--
			reg.mu.Unlock()
			timer.Reset(reg.idle)
		case <-timer.C:
			reg.mu.Lock()
			if lane.inflight == 0 {
				delete(reg.lanes, key)
				reg.mu.Unlock()
				return
			}
			reg.mu.Unlock()
			timer.Reset(reg.idle)
		case <-ctx.Done():
			return
		}
	}
}
