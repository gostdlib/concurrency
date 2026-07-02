/*
Package foreach runs an operation for its side effects on every key/value pair in an iter.Seq2, in
parallel. It is the parallel analog of a "for range" loop whose body returns only an error: it does NOT
transform values or hand any back. The only thing returned is the combined error of the run.

foreach is useful when each value needs an expensive, independent side effect: writing to a database,
calling an API, publishing to a queue, and so on. For cheap operations a plain "for" loop in a single
goroutine will be faster, as the cost of parallelism will outweigh the gain.

Item applies a Func to each key/value pair yielded by the input sequence. Any iter.Seq2 works — over a
slice (stream.Slice, keyed by index), a map (stream.Map, keyed by key), or a channel (stream.Chan, keyed
by receive index). The value is passed by copy, so mutating it inside Func is not observable to the
caller unless the sequence yields a pointer or reference type (e.g. a *Record whose pointed-at struct
Func mutates); because Funcs run concurrently, that is only safe when each item is independent and
nothing else reads it during the pass. By default Item processes until the sequence is exhausted or the
Context is cancelled, using the worker pool attached to the Context. Errors returned by the Func are
collected and returned together (each wrapped in the final error) but do not stop processing unless
WithStopOnErr is passed.

Say you want to read numbers from a channel and print each multiplied by 5:

	input := make(chan int, 1)
	go func() {
		defer close(input)
		for i := 0; i < 1000; i++ {
			input <- i
		}
	}()

	fn := func(ctx context.Context, _ int, i int) error {
		fmt.Println(i * 5)
		return nil
	}

	if err := foreach.Item(ctx, stream.Chan(ctx, input), fn); err != nil {
		// Do something.
	}

Because no options were provided, this processes until the sequence is exhausted or the Context is
cancelled and uses a worker pool with runtime.NumCPU() * 10 workers. Pass WithStopOnErr to cancel on the
first error.

To get transformed results back out — the fan-out/fan-in pattern — pair Item with an Order: the Func
Adds each result under its input key and a consumer ranges Order.All(), which streams the results in
input order while processing is still running. See the example on Order.
*/
package foreach

import (
	"iter"
	"runtime"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/values/generics/queue"
	"github.com/gostdlib/internals/otel/span"
)

// Func is called for its side effects on every key/value pair in the input sequence. It returns only an
// error; any result must be produced as a side effect.
type Func[K, V any] func(context.Context, K, V) error

type opts struct {
	stopOnErr bool
}

// Option is an option for Item.
type Option func(o opts) (opts, error)

// WithStopOnErr causes the first error to cancel processing of the remaining values.
func WithStopOnErr() Option {
	return func(o opts) (opts, error) {
		o.stopOnErr = true
		return o, nil
	}
}

// Item runs fn for its side effects on each key/value pair yielded by seq, using the worker pool attached
// to ctx. It returns no values, only the combined error. Adapt a channel, slice, or map into seq with
// stream.Chan, stream.Slice, or stream.Map. If the pool is unlimited, up to runtime.NumCPU() * 10 workers are
// used. Errors returned by fn are collected and returned together (each wrapped in the final error) but
// do not stop processing unless WithStopOnErr is provided. Cancel ctx to stop early. A nil sequence is a
// no-op.
func Item[K, V any](ctx context.Context, seq iter.Seq2[K, V], fn Func[K, V], options ...Option) error {
	spanner := span.Get(ctx)

	if seq == nil {
		return nil
	}

	o := opts{}
	var err error
	for _, opt := range options {
		o, err = opt(o)
		if err != nil {
			return err
		}
	}

	p := context.Pool(ctx)
	if p.Limit() == 0 {
		p = p.Limited(ctx, "", runtime.NumCPU()*(10))
	}

	cancel := func() {}
	if o.stopOnErr {
		ctx, cancel = context.WithCancel(ctx)
	}
	g := p.Group()
	g.CancelOnErr = cancel

	for k, v := range seq {
		if ctx.Err() != nil {
			break
		}
		g.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, k, v)
		})
	}
	if err := g.Wait(ctx); err != nil {
		spanner.Error(err)
		return err
	}
	return ctx.Err()
}

type item[V any] struct {
	insertNum uint64
	value     V
}

// Order reorders values produced concurrently (usually by Funcs running under Item) back into input
// order. Workers call Add with the value's key from the input sequence; a single consumer ranges All to
// receive the values sorted by that key, each one streamed as soon as every earlier key has been yielded.
// This lets foreach act as an order-retaining fan-out/fan-in: the Func does the parallel work and Adds
// the result, while the consumer receives results in input order as they become ready.
//
// Keys must be unique, 0-based and dense, which is what stream.Chan, stream.Slice and stream.Seq yield.
// Values are held until all earlier keys arrive, so a slow item causes completed later results to
// accumulate in memory.
type Order[V any] struct {
	q         *queue.Queue[queue.Value[item[V]]]
	next      uint64
	added     chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	consuming atomic.Bool
}

// New creates a new Order for use with a Context.
func New[V any](ctx context.Context) (*Order[V], error) {
	backing, err := queue.NewBTreePriority[queue.Value[item[V]]]()
	if err != nil {
		return nil, err
	}
	// The queue must be unbounded: values arrive in completion order and are held until all earlier
	// keys arrive, so any bound deadlocks once out-of-order values fill it.
	q, err := queue.New[queue.Value[item[V]]](ctx, "", backing, queue.Unlimited)
	if err != nil {
		return nil, err
	}
	return &Order[V]{q: q, added: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func itemEqual[V any](a, b item[V]) bool {
	return a.insertNum == b.insertNum
}

func itemHash[V any](a item[V]) uint64 {
	return a.insertNum
}

func newValue[V any](insertNum uint64, v V) queue.Value[item[V]] {
	it := item[V]{insertNum: insertNum, value: v}
	return queue.Value[item[V]]{V: it, Equaler: itemEqual[V], Hasher: itemHash[V], P: insertNum}
}

// Close signals that no more values will be Added. All() drains the values still held and then stops.
// Close is safe to call multiple times and from a different goroutine than Add or All. Calls to Add()
// after Close will cause a panic.
func (o *Order[V]) Close() {
	o.closeOnce.Do(func() { close(o.done) })
}

func (o *Order[V]) closed() bool {
	select {
	case <-o.done:
		return true
	default:
		return false
	}
}

// Len returns the number of values held in Order that All() has not yet yielded.
func (o *Order[V]) Len() int {
	return int(o.q.Len())
}

// Add adds the value v under key k, which must be the value's key in the input sequence. Add is safe to
// call from multiple goroutines. Calling Add after Close panics, as does a negative k.
func (o *Order[V]) Add(ctx context.Context, k int, v V) {
	if k < 0 {
		panic("Order.Add() called with a negative key")
	}
	if o.closed() {
		panic("cannot call Order.Add() after Close() has been called")
	}
	// The insert number is k+1 because the priority queue backing rejects priority 0.
	val := newValue(uint64(k)+1, v)
	if _, err := o.q.Push(context.WithoutCancel(ctx), []queue.Value[item[V]]{val}); err != nil {
		// The queue is unbounded, in-memory and never closed, so Push cannot fail.
		panic(err)
	}
	// Wake a consumer blocked in All(). The buffer of one coalesces signals; All() re-examines the
	// queue after every wakeup, so a coalesced signal cannot strand a value.
	select {
	case o.added <- struct{}{}:
	default:
	}
}

// All yields the Added values in key order along with their keys, streaming each value as soon as every
// earlier key has been yielded. It waits for missing keys until Close() is called; after Close the
// values still held are drained (in key order, skipping keys that were never Added) and the iteration
// stops. Cancelling ctx also stops the iteration. All supports a single consumer at a time: ranging a
// second sequence while another is still being consumed panics. Once an iteration ends, All can be
// ranged again.
func (o *Order[V]) All(ctx context.Context) iter.Seq2[int, V] {
	// Queue operations use a non-cancellable Context so a ctx cancellation ends the iteration at the
	// checks below instead of surfacing as queue errors mid-operation.
	qctx := context.WithoutCancel(ctx)
	return func(yield func(int, V) bool) {
		if !o.consuming.CompareAndSwap(false, true) {
			panic("Order.All() is already being consumed; All() supports a single consumer at a time")
		}
		defer o.consuming.Store(false)
		for {
			if ctx.Err() != nil {
				return
			}
			// closed is read before Peek: every Add happens before Close, so a false read here means
			// any value racing with this Peek triggers another pass (wait returns at once via o.done).
			closed := o.closed()
			v, ok, err := o.q.Peek(qctx)
			if err != nil {
				return
			}
			if !ok {
				if closed {
					return
				}
				o.wait(ctx)
				continue
			}
			// While open, wait for the next key so output retains input order. Once closed, missing
			// keys can never arrive, so drain what remains (the backing keeps it sorted by key).
			if !closed && v.V.insertNum > o.next+1 {
				o.wait(ctx)
				continue
			}
			if _, err := o.q.Pop(qctx, 1); err != nil {
				return
			}
			if v.V.insertNum > o.next {
				o.next = v.V.insertNum
			}
			if !yield(int(v.V.insertNum-1), v.V.value) {
				return
			}
		}
	}
}

// wait blocks until a value is Added, Close() is called or ctx is cancelled.
func (o *Order[V]) wait(ctx context.Context) {
	select {
	case <-o.added:
	case <-o.done:
	case <-ctx.Done():
	}
}
