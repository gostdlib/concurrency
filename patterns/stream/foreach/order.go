package foreach

import (
	"iter"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
)

// order is the engine behind WithOrdered: it reorders values produced concurrently back into dispatch
// order. Workers add each value under its 0-based dispatch index; a single consumer ranges all to
// receive the values sorted by that index, each one streamed as soon as every earlier index has been
// yielded. The buffer is a plain map keyed by dispatch index — indexes are dense, contiguous and
// unique by construction (a single dispatcher assigns them), so ordering needs only "is the next
// index here yet", not a priority queue. Every dispatched index is always added (a response is
// delivered even when the ItemFunc errors), so a missing index can only be one still in flight.
type order[T any] struct {
	mu sync.Mutex
	// buf holds added values that all has not yet yielded, keyed by dispatch index.
	buf map[int]T
	// next is the dispatch index all yields next.
	next int
	// added wakes all when a value arrives; yielded wakes a waitBelow dispatcher when all removes one.
	// Both have a buffer of one so signals coalesce and their single waiter re-checks after every
	// wakeup.
	added     chan struct{}
	yielded   chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	// consuming is true while all() is being ranged; a second concurrent consumer panics. Guarded by
	// mu.
	consuming bool
}

// newOrder creates an order.
func newOrder[T any]() *order[T] {
	return &order[T]{buf: map[int]T{}, added: make(chan struct{}, 1), yielded: make(chan struct{}, 1), done: make(chan struct{})}
}

// finish signals that no more values will be added. all drains the values still held and then stops.
// finish is safe to call multiple times and from a different goroutine than add or all. It takes o.mu
// around the close so it linearizes with add's closed check (both under o.mu): an add either writes
// its value before finish closes done or observes the closed order and panics, never a torn state
// where a non-panicking add's value is silently lost. The lock-free closed() reads in all/waitBelow
// are unaffected — their re-check loops tolerate the race by design.
func (o *order[T]) finish() {
	o.mu.Lock()
	o.closeOnce.Do(func() { close(o.done) })
	o.mu.Unlock()
}

func (o *order[T]) closed() bool {
	select {
	case <-o.done:
		return true
	default:
		return false
	}
}

// held returns the number of values held that all has not yet yielded.
func (o *order[T]) held() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.buf)
}

// add adds the value v under the 0-based dispatch index i and wakes a consumer blocked in all. The
// wake buffer of one coalesces signals; all re-examines the buffer after every wakeup, so a coalesced
// signal cannot strand a value. add is safe to call from multiple goroutines. Calling add after
// finish panics: the dispatcher finishes only after every in-flight worker has added, so it indicates
// an internal bug.
func (o *order[T]) add(i int, v T) {
	// The closed check and the buf write are under one o.mu hold so they linearize with finish (which
	// closes done under the same lock): a concurrent finish either lands before this lock — the check
	// sees it and panics without writing — or after this unlock — the write is already recorded and not
	// lost. Unlock before panicking so a recovering caller does not deadlock on the held mutex.
	o.mu.Lock()
	if o.closed() {
		o.mu.Unlock()
		panic("bug: order.add() called after finish()")
	}
	o.buf[i] = v
	o.mu.Unlock()
	select {
	case o.added <- struct{}{}:
	default:
	}
}

// pop removes and returns the next value in dispatch-index order. While open, only the exact next
// index pops, so output retains dispatch order; once closed, a missing index can never arrive, so pop
// advances to the smallest index still held. ok is false when nothing is poppable — a gap while open,
// or nothing held.
func (o *order[T]) pop(closed bool) (int, T, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	v, ok := o.buf[o.next]
	if !ok {
		if !closed || len(o.buf) == 0 {
			var zero T
			return 0, zero, false
		}
		next := -1
		for k := range o.buf {
			if next == -1 || k < next {
				next = k
			}
		}
		o.next = next
		v = o.buf[o.next]
	}
	i := o.next
	delete(o.buf, i)
	o.next++
	return i, v, true
}

// all yields the added values in dispatch-index order along with their indexes, streaming each value
// as soon as every earlier index has been yielded. It waits for missing indexes until finish is
// called; after finish the values still held are drained (in index order, skipping indexes that were
// never added) and the iteration stops. Cancelling ctx does not truncate: all waits for finish —
// which the dispatcher guarantees follows every cancellation, after the in-flight work has added —
// and then drains, so every delivered value is still yielded. all supports a single consumer at a
// time and panics if a second range starts while one is in progress; its only caller, orderedSeq,
// ranges it exactly once per Item range.
func (o *order[T]) all(ctx context.Context) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		o.mu.Lock()
		if o.consuming {
			o.mu.Unlock()
			panic("order.all() is already being consumed; all() supports one consumer at a time")
		}
		o.consuming = true
		o.mu.Unlock()
		defer func() {
			o.mu.Lock()
			o.consuming = false
			o.mu.Unlock()
		}()
		for {
			// closed is read before pop: every add happens before finish, so a false read here means
			// any value racing with this pop triggers another pass (wait returns at once via o.done).
			closed := o.closed()
			if !closed && ctx.Err() != nil {
				// Cancelled: the dispatcher stops dispatching, the in-flight work still adds, and
				// finish always follows. Wait for it, then drain below — ending here instead would
				// truncate responses that already completed (with WithStopOnErr that includes the
				// very error response the caller was promised).
				<-o.done
				closed = true
			}
			i, v, ok := o.pop(closed)
			if !ok {
				if closed {
					return
				}
				o.wait(ctx)
				continue
			}
			// Wake a dispatcher blocked in waitBelow. Same coalescing scheme as o.added: it re-checks
			// held after every wakeup.
			select {
			case o.yielded <- struct{}{}:
			default:
			}
			if !yield(i, v) {
				return
			}
		}
	}
}

// wait blocks until a value is added, finish is called or ctx is cancelled.
func (o *order[T]) wait(ctx context.Context) {
	select {
	case <-o.added:
	case <-o.done:
	case <-ctx.Done():
	}
}

// waitBelow blocks until the order holds fewer than max values, re-checking each time all removes one.
// It returns nil once below max, errOrderFinished once finish has been called, and ctx.Err() if ctx is
// cancelled first. If no consumer is draining all, it blocks until finish or cancellation.
func (o *order[T]) waitBelow(ctx context.Context, max int) error {
	for {
		if o.closed() {
			return errOrderFinished
		}
		if o.held() < max {
			return nil
		}
		select {
		case <-o.yielded:
		case <-o.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
