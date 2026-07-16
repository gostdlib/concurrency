/*
Package broadcast contains a single type called Value that allows you to broadcast values to listeners.
This package prevents slow listeners from holding up subscribers, though this can build up memory usage
if a subscriber is slower than all the other value reference holders. Setting Value.Name records metrics
for the Value, including how many values have been sent but not yet delivered.

Usage:

	v := &broadcast.Value[int]{Name: "prices"}
	defer v.Close(ctx)

	seq := v.Subscribe(ctx)
	context.Pool(ctx).Submit(ctx, func() {
		for value := range seq {
			fmt.Println(value)
		}
	})

	v.Send(ctx, 1)
	v.Send(ctx, 2)
*/
package broadcast

import (
	"iter"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
)

// store is a single value in the broadcast chain. done is closed once the value is set, at which point
// v, next and end are safe to read. end marks the final store in the chain, which is set by Close().
// idx is the store's position in the chain, which lets a subscriber work out how far behind it is.
type store[T any] struct {
	done chan struct{}
	v    T
	next *store[T]
	idx  int64
	end  bool
}

func newStore[T any](idx int64) *store[T] {
	return &store[T]{done: make(chan struct{}), idx: idx}
}

// holder is a subscription that has been handed out but has not started iterating. release gives its holder
// slot back and stop hands the Context back the callback that would have called release. Only the iterator
// can give the slot back once it is running, so until then the callback on the subscriber's Context is the
// only thing that will, and it is the only thing Close() cannot reach on its own. Keeping the pair here is
// what lets Close() reclaim a subscription that is never ranged.
type holder struct {
	stop    func() bool
	release func()
}

// Value allows you to broadcast values out to subscribers. The zero value is ready to use.
// A Value must not be copied after first use.
type Value[T any] struct {
	// Name namespaces the metrics this Value records. If empty, no metrics are recorded. Name must be
	// set before the first call to Send(), Subscribe() or Close() and must not change after that.
	Name string

	once sync.Once

	// mu gates the read-modify-write of the chain that Send() and Close() do. Subscribe() does not take it,
	// it reads the tail out of result.
	mu     sync.Mutex
	closed bool

	// unstarted is every subscription that has been handed out but has not been ranged. An entry leaves when
	// its iterator starts, when its Context is canceled, or when Close() reclaims it. Guarded by mu.
	unstarted map[*holder]struct{}

	result atomic.Pointer[store[T]]
	// subs is the number of subscribers currently iterating. holders is the number of subscriptions that have
	// been handed out but have not started iterating yet. Send() drops a value only when both are 0, as that
	// is the only state where no one can ever see it.
	subs    atomic.Int64
	holders atomic.Int64
	metrics *metrics
}

// init prepares the Value on first use. The Context of the first caller supplies the MeterProvider.
func (b *Value[T]) init(ctx context.Context) {
	b.once.Do(func() {
		b.result.Store(newStore[T](0))
		b.unstarted = map[*holder]struct{}{}
		if b.Name != "" {
			b.metrics = newMetrics(context.MeterProvider(ctx).Meter(meterName + "/" + b.Name))
		}
	})
}

// Send sends a value out to all subscribers. This is non-blocking and thread-safe. If there are no subscribers,
// or if Close() has been called, the value is dropped.
//
// Every subscriber is handed the same value, it is not copied. If T is a map, a slice or a pointer, one
// subscriber's writes are seen by the others, so use an immutable.Map or immutable.Slice (or send a value
// type) when subscribers must not see each other.
func (b *Value[T]) Send(ctx context.Context, v T) {
	b.init(ctx)

	if b.metrics != nil {
		b.metrics.Sends.Add(ctx, 1)
	}

	// No one is iterating and no one is holding a subscription that has yet to start, so no one can ever
	// see this value. Dropping it here keeps a Value with no subscribers from allocating a store per Send().
	//
	// The read order is load-bearing and is the reverse of the write order in Subscribe(), which does
	// subs.Add(1) then holders.Add(-1). Reading holders first means that seeing holders == 0 guarantees the
	// paired subs.Add(1) is already visible, so the subs.Load() below cannot also read 0 while a subscription
	// is alive. Reading subs first would let a subscription hand off between the two loads and lose a value.
	if b.holders.Load() == 0 && b.subs.Load() == 0 {
		return
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	n := b.result.Load()
	n.v = v
	n.next = newStore[T](n.idx + 1)
	b.result.Store(n.next)
	close(n.done)
	// Read subs inside the critical section. It has to be the count that goes with this exact chain state:
	// a subscriber that is halfway out the door (it has decremented subs but not yet read the tail to work
	// out its lag) would otherwise subtract this value from Pending without anyone having added it.
	subs := b.subs.Load()
	b.mu.Unlock()

	if b.metrics != nil && subs > 0 {
		b.metrics.Pending.Add(ctx, subs)
	}
}

// Close ends the broadcast. Every subscriber delivers the values that were already sent and then its
// iteration ends, which releases the subscriber even if its Context is never canceled. Close() is
// thread-safe and safe to call more than once.
func (b *Value[T]) Close(ctx context.Context) {
	b.init(ctx)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true

	n := b.result.Load()
	n.end = true
	close(n.done)

	unstarted := b.unstarted
	b.unstarted = map[*holder]struct{}{}
	b.mu.Unlock()

	// A subscription that was never ranged has a callback sitting on its Context waiting to give the holder
	// slot back. This Value is done, so that callback has nothing left to do, and leaving it there holds this
	// Value and every value in its chain until a Context that may never be canceled is canceled. Take the
	// callback back and give the slot back here instead. The stops are taken outside mu, as they are
	// reclamation and not correctness, and a stop() does not wait for a callback that is already running.
	// release() is guarded by a CAS, so a subscription that is ranged after this still releases exactly once.
	for h := range unstarted {
		h.stop()
		h.release()
	}
}

// Subscribe returns an iterator over every value sent after Subscribe() returns. Iteration ends when ctx is
// canceled or Close() is called. Values sent before Close() are delivered first, but cancelling ctx can drop a
// value that was ready to be delivered, so a canceled subscriber cannot assume it saw everything. Breaking out
// of the iteration releases every value the subscription is holding. The returned iterator can only be ranged
// over once, a second range yields nothing. If you decide not to range a subscription at all, cancel ctx, as
// until you do every Send() on the Value stores a value for a subscriber that is never going to read it.
func (b *Value[T]) Subscribe(ctx context.Context) iter.Seq[T] {
	b.init(ctx)

	// Registering before the load is what makes Send()'s drop safe: any Send() that lands after this point
	// sees a holder and stores its value, so nothing can be dropped out from under us.
	b.holders.Add(1)

	// This load must happen here and not inside the iterator, otherwise a value sent between Subscribe()
	// returning and the range statement starting would never be seen by this subscriber.
	start := b.result.Load()
	startIdx := start.idx

	// The holder slot has to come back exactly once, whether this subscription gets ranged or is abandoned.
	// Only the iterator can hand it back once it is running, so a subscription that is never ranged relies on
	// the callback below to release it.
	released := atomic.Bool{}
	release := func() {
		if released.CompareAndSwap(false, true) {
			b.holders.Add(-1)
		}
	}

	h := &holder{release: release}

	// drop is what ctx calls back. It takes this subscription out of unstarted as well as giving the slot
	// back, as a Context that is canceled before the subscription is ever ranged is done with it: releasing
	// without dropping would leave the record for Close() to find, and a Value that is never closed would
	// grow one for every subscription that came and went.
	drop := func() {
		b.mu.Lock()
		delete(b.unstarted, h)
		b.mu.Unlock()
		release()
	}

	// Registering the callback under mu is what lets Close() reclaim it. AfterFunc() runs its function in its
	// own goroutine, never on this one, so a ctx that is already done cannot call back into drop() here and
	// deadlock on mu, it only parks until this returns.
	b.mu.Lock()
	closed := b.closed
	if !closed {
		h.stop = context.AfterFunc(ctx, drop)
		b.unstarted[h] = struct{}{}
	}
	b.mu.Unlock()

	// Close() has already drained unstarted, so nothing will ever come back for this one. There is nothing for
	// a callback to wait on either, as the iterator ends as soon as it is ranged, so give the slot back now
	// rather than leaving one on a Value that is done.
	if closed {
		release()
	}

	started := atomic.Bool{}

	return func(yield func(T) bool) {
		// Ranging a second time would take another subs slot while giving back a holder slot it never had,
		// which drives holders negative and permanently disables Send()'s drop for this Value.
		if !started.CompareAndSwap(false, true) {
			return
		}

		delivered := int64(0)

		// Join the subscriber count and read the tail that goes with it in one critical section. Send() reads
		// subs under the same lock, so every value is either counted as pending on us or is behind us in the
		// chain and picked up by backlog, never both and never neither. Doing this with bare atomics lets a
		// Send() land between the two and drift Pending permanently. This is twice per subscription, not
		// per value, so it stays off the hot path.
		b.mu.Lock()
		b.subs.Add(1)
		backlog := b.result.Load().idx - startIdx
		// We are running, so we hand the slot back ourselves and Close() has nothing left to reclaim for us.
		delete(b.unstarted, h)
		b.mu.Unlock()

		// Become a subscriber before giving up the holder slot, so the two counts are never both 0 while
		// this subscription is alive. Send() relies on this order.
		release()
		if h.stop != nil {
			h.stop() // We released the slot ourselves, so ctx no longer needs to hold onto the callback.
		}

		defer func() {
			b.mu.Lock()
			b.subs.Add(-1)
			// Whatever we did not get to is no longer pending on anyone once we stop iterating.
			lag := b.result.Load().idx - startIdx - delivered
			b.mu.Unlock()

			if b.metrics != nil {
				b.metrics.Subscribers.Add(ctx, -1)
				if lag > 0 {
					b.metrics.Pending.Add(ctx, -lag)
				}
			}
		}()

		if b.metrics != nil {
			b.metrics.Subscribers.Add(ctx, 1)
			// Values sent between Subscribe() returning and this iterator starting are already pending on us,
			// but Send() did not count us when it recorded them.
			if backlog > 0 {
				b.metrics.Pending.Add(ctx, backlog)
			}
		}

		// Hand the chain head to a local and drop the closure's reference to it. The closure outlives every
		// value it walks past, so holding start here would pin every value ever sent to this subscription,
		// even for a subscriber that is keeping up.
		s := start
		start = nil

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.done:
			}

			if s.end {
				return
			}

			// The value stops being pending the moment it is handed to yield, not when yield returns. An
			// iterator paused inside yield (iter.Pull) has already given the value to its consumer.
			delivered++
			if b.metrics != nil {
				b.metrics.Delivered.Add(ctx, 1)
				b.metrics.Pending.Add(ctx, -1)
			}

			if !yield(s.v) {
				return
			}
			s = s.next
		}
	}
}
