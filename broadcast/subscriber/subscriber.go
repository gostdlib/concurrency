// Package subscriber contains a Value that broadcasts values on topics, where a topic is a filesystem like
// path. A subscriber may use the wildcards * and ** to subscribe to more than one topic, a sender may not.
//
// A * matches exactly one segment and a ** matches zero or more segments. Both must make up an entire
// segment, so "prices/*/nyse" is a pattern while "prices/us*" is not. A * may appear at any point in the
// pattern and as often as it is wanted, but a ** may only be the last segment, which means a pattern holds
// at most one. This is the rule MQTT and NATS hold their multi-level wildcards to, and it is what keeps a
// pattern from making Send() pay for every way the topic could be split between one ** and the next. As a
// ** may match zero segments, "prices/**" also matches "prices" itself. A leading / is optional, so
// "/prices/us" and "prices/us" are the same topic. Every pattern that matches a sent topic receives the
// value, so a value can be delivered to more than one subscription.
//
// Usage:
//
//	v := &subscriber.Value[int]{Name: "prices"}
//	defer v.Close(ctx)
//
//	// Everything under prices/us, including prices/us itself.
//	seq, err := v.Subscribe(ctx, "prices/us/**")
//	if err != nil {
//		// Handle error.
//	}
//	context.Pool(ctx).Submit(ctx, func() {
//		for value := range seq {
//			fmt.Println(value)
//		}
//	})
//
//	if err := v.Send(ctx, "prices/us/nyse", 1); err != nil {
//		// Handle error.
//	}
package subscriber

import (
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/broadcast"
	"github.com/gostdlib/concurrency/broadcast/subscriber/internal/trie"
)

// ErrClosed is returned by Send() and Subscribe() once Close() has been called. A Value never reopens, so
// this is permanent: a caller retrying through base/retry sees it for what it is and gives up rather than
// spending its budget on a call that cannot start working.
var ErrClosed = fmt.Errorf("subscriber: Value is closed: %w", exponential.ErrPermanent)

// ErrInvalidPath is returned by Send() and Subscribe() when the topic or pattern they were given is not
// one, which a caller checks for with errors.Is(). It says the caller made a mistake, so there is nothing
// to be gained by trying it again. The errors returned wrapping it are permanent for that reason
// (errors.Is(err, exponential.ErrPermanent)); they are built only through invalidPath(), which is what
// carries the permanence, so this sentinel is not itself permanent. What is wrong with the path is in the
// message wrapped around this.
var ErrInvalidPath = errors.New("subscriber: invalid path")

// sub is every subscription to a single pattern. All of them read from one broadcast.Value, so a value
// sent to a topic is stored once no matter how many subscribers the pattern has. refs is the number of
// subscriptions handed out for the pattern that have not been given back, and is guarded by Value.mu. It
// tracks Value.subs exactly: a subscription is counted here and recorded there in the same critical
// section, and leaves both in the same one.
//
// key is the pattern in its canonical form and is the only form kept. The map wants it as a string and the
// trie wants it as segments, but a segment can hold neither a / nor nothing at all, so the two are the same
// key written two ways and segments() turns one back into the other. Keeping both would be keeping an
// invariant that nothing enforces.
type sub[T any] struct {
	key   string
	value *broadcast.Value[T]
	refs  int
}

// subscription is one call to Subscribe(). A pattern can have many subscribers, each with its own Context,
// so a subscription and not a pattern is what a release is keyed by. stop hands the subscriber's Context
// back the callback that releases this subscription, which is the only way to take it off a Context that is
// never canceled. Both fields are written under Value.mu.
type subscription[T any] struct {
	t    *sub[T]
	stop func() bool
}

// segments is the pattern as the trie keys it. canonical() joined these, and a segment can hold neither a
// / nor nothing, so this hands back exactly what was joined.
func (s *sub[T]) segments() []string {
	return strings.Split(s.key, "/")
}

// Value provides a subscription based Value where values can be sent on topics which are represented
// by file system like paths. You can use file system wildcards * and ** to Subscribe to paths or
// single paths. The zero value is ready to use. A Value must not be copied after first use.
type Value[T any] struct {
	// Name namespaces the metrics this Value records. If empty, no metrics are recorded. Name must be
	// set before the first call to Send(), Subscribe() or Close() and must not change after that.
	Name string

	once sync.Once

	// mu gates topics, subs and the rebuild of the trie that goes with them. Send() does not take it, it reads
	// the trie out of tree.
	mu     sync.Mutex
	topics map[string]*sub[T]

	// subs is every subscription that has been handed out and not given back. Membership is what says a
	// subscription is still owed a release, so it is the one token release() and Close() both spend, and it is
	// what lets Close() reach the callback each subscription left on its Context.
	subs map[*subscription[T]]struct{}

	// tree holds the patterns in topics keyed by their segments. This is what Send() walks, so the send path
	// takes no lock. The trie is immutable and copies only the path a change touches, so a subscribe or a
	// release publishes a new one rather than rebuilding it. It is never nil once init() has run.
	tree atomic.Pointer[trie.Tree[string, *sub[T]]]

	// closed is read by Send() outside of mu. A Send() that gets past it while Close() is running is
	// still safe, as a broadcast.Value drops what is sent to it after it is closed.
	closed  atomic.Bool
	metrics *metrics
}

// init prepares the Value on first use. The Context of the first caller supplies the MeterProvider.
func (v *Value[T]) init(ctx context.Context) {
	v.once.Do(func() {
		v.topics = map[string]*sub[T]{}
		v.subs = map[*subscription[T]]struct{}{}
		v.store(trie.Tree[string, *sub[T]]{})
		if v.Name != "" {
			v.metrics = newMetrics(context.MeterProvider(ctx).Meter(meterName + "/" + v.Name))
		}
	})
}

// Send sends value to topic. Topic must be fully qualified and not have a * or **. This may not
// end in a /. Every subscription whose pattern matches topic receives value. A topic that no pattern
// matches is not an error, the value is dropped. Send() is non-blocking and thread-safe. It returns
// ErrClosed if Close() has been called.
//
// Every subscriber that matches is handed the same value, it is not copied. If T is a map, a slice or a
// pointer, one subscriber's writes are seen by the others, so use an immutable.Map or immutable.Slice
// (or send a value type) when subscribers must not see each other.
func (v *Value[T]) Send(ctx context.Context, topic string, value T) error {
	v.init(ctx)

	// Stack space for the segments of the topic and for the topics it matches. A topic deeper than this or
	// one matching more patterns than this still works, the buffer grows onto the heap. These do not escape,
	// so the common send neither splits onto the heap nor allocates a result.
	var segBuf [16]string
	var matchBuf [8]*sub[T]

	// Checked before the topic is validated so that a closed Value reports being closed rather than picking
	// over the caller's argument, which is what the doc promises: once Close() has been called, ErrClosed.
	if v.closed.Load() {
		return ErrClosed
	}

	segs, err := topicSegs(topic, segBuf[:])
	if err != nil {
		return err
	}

	if v.metrics != nil {
		v.metrics.Sends.Add(ctx, 1)
	}

	matched := match(*v.tree.Load(), segs, matchBuf[:])
	if len(matched) == 0 {
		if v.metrics != nil {
			v.metrics.Unmatched.Add(ctx, 1)
		}
		return nil
	}

	for _, t := range matched {
		t.value.Send(ctx, value)
	}

	if v.metrics != nil {
		v.metrics.Matches.Add(ctx, int64(len(matched)))
	}
	return nil
}

// Subscribe subscribes to a topic. You may use ** and * to subscribe to multiple topics, where a ** may
// only be the last segment. This may not end in a / and returns ErrInvalidPath if it does or if a ** is
// anywhere but last. Iteration ends when ctx is canceled or Close() is called, and breaking out of it ends
// the subscription too: the pattern is given back, so a subscriber that walks away does not have to cancel
// ctx to stop being matched. A subscription that is never ranged at all is given back when ctx is canceled.
// Subscribe() returns ErrClosed if Close() has been called. See broadcast.Value.Subscribe() for what the
// returned iterator guarantees.
func (v *Value[T]) Subscribe(ctx context.Context, topic string) (iter.Seq[T], error) {
	v.init(ctx)

	// Checked before the pattern is validated so that a closed Value reports being closed rather than picking
	// over the caller's argument, matching the doc: once Close() has been called, ErrClosed. sub() rechecks
	// this under mu, which is what actually settles a Subscribe() racing a Close(); this is only about which
	// error a caller that is plainly past Close() sees.
	if v.closed.Load() {
		return nil, ErrClosed
	}

	segs, err := patternSegs(topic)
	if err != nil {
		return nil, err
	}

	return v.sub(ctx, segs)
}

// sub subscribes to the pattern segs, creating the entry if this is the first subscription to that pattern,
// counts this subscription against it and records it in v.subs. It returns the subscription to the
// broadcast.Value behind the pattern.
//
// Everything a subscription needs is done here, under mu, and nothing can fail after the first line of it.
// The release callback in particular is registered here and not by the caller, as a caller that registered
// it would have to do so after mu was given up, and a Close() landing in that window would leave the
// callback on a Context that nothing would ever take it off of.
func (v *Value[T]) sub(ctx context.Context, segs []string) (iter.Seq[T], error) {
	key := canonical(segs)

	v.mu.Lock()
	defer v.mu.Unlock()

	// Rechecked under mu. Close() takes mu to tear the topics down, so this is what keeps a subscription
	// from being registered into a map that has already been emptied and never being closed, and is what
	// makes the ErrClosed a Subscribe() racing a Close() gets back reliable rather than best effort.
	if v.closed.Load() {
		return nil, ErrClosed
	}

	// fresh is the only thing keeping one pattern from being registered twice. The trie replaces the value
	// at a path it already holds rather than complaining, so a second sub[T] for a key already in topics
	// would take the first one's place there and leave its subscribers on a broadcast.Value that nothing
	// sends to any more.
	t := v.topics[key]
	fresh := t == nil
	if fresh {
		t = &sub[T]{key: key, value: &broadcast.Value[T]{}}
	}

	// The holder slot is taken before the pattern is published into the trie. A broadcast.Value with no
	// holders and no subscribers drops what is sent to it, so publishing first would leave a window where a
	// matching Send() finds the pattern, hands the value to a broadcast.Value nobody has joined yet, and the
	// value is thrown away behind the subscriber's back.
	seq := t.value.Subscribe(ctx)

	if fresh {
		v.topics[key] = t
		// The trie copies the segments it is handed, so segs does not outlive this call and the caller keeps
		// what it came in with.
		v.store(v.tree.Load().Insert(segs, t))

		if v.metrics != nil {
			v.metrics.Topics.Add(ctx, 1)
		}
	}
	t.refs++

	// The callback is registered under mu so that Close() can never miss it. AfterFunc() runs its function in
	// its own goroutine and never on this one, so a ctx that is already canceled cannot call back into
	// release() here and deadlock on mu, it only parks until this returns, by which time s is whole.
	s := &subscription[T]{t: t}
	v.subs[s] = struct{}{}
	s.stop = context.AfterFunc(ctx, func() { v.release(ctx, s) })

	if v.metrics != nil {
		v.metrics.Subscribers.Add(ctx, 1)
	}
	return v.ranged(ctx, seq, s), nil
}

// ranged wraps seq so the subscription gives itself back the moment iteration ends, however it ends: the
// caller breaks out, the values run out, or Close() stops it. A subscriber that walks away no longer has to
// cancel its Context to release the pattern, so a broken-out subscription no longer sits in the trie being
// matched on every Send() until then. The AfterFunc that sub() put on the Context stays for a subscription
// that is never ranged at all; release() and stop() are both idempotent, so whichever path fires first wins
// and the other does nothing.
func (v *Value[T]) ranged(ctx context.Context, seq iter.Seq[T], s *subscription[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		// Iteration is under way, so the Context callback has nothing left to do. Take it back here rather than
		// leave it on a Context that may outlive the subscription by a long way, then give the subscription back.
		defer func() {
			s.stop()
			v.release(ctx, s)
		}()

		for value := range seq {
			if !yield(value) {
				return
			}
		}
	}
}

// release gives back the subscription s. The last subscription to a pattern takes the pattern out of the
// trie and closes the broadcast.Value behind it, so a Value that a subscriber has walked away from stops
// storing values for it.
//
// Membership in v.subs is what says s is still owed a release, and it is spent under mu, so this runs at
// most once for a subscription no matter how many things call it. That is what makes it safe for a Context
// to be canceled after Close() has already ended the subscription, or twice, or during a Close().
func (v *Value[T]) release(ctx context.Context, s *subscription[T]) {
	v.mu.Lock()

	// Close() drained subs, or this subscription was already given back. Either way it has been accounted for
	// and this must not count it, its pattern or its metrics a second time.
	if _, ok := v.subs[s]; !ok {
		v.mu.Unlock()
		return
	}
	delete(v.subs, s)

	t := s.t
	t.refs--

	// A subscription in subs holds a ref on t, so refs reaching 0 says no other subscription points at t and
	// taking it out strands nobody. The identity check cannot fail today, it is here so that a change that
	// broke that invariant would leave the wrong pattern in place rather than silently evicting a live one.
	dropped := false
	if t.refs == 0 && v.topics[t.key] == t {
		delete(v.topics, t.key)
		v.store(v.tree.Load().Delete(t.segments()))
		dropped = true
	}
	v.mu.Unlock()

	if dropped {
		t.value.Close(ctx)
	}

	if v.metrics != nil {
		v.metrics.Subscribers.Add(ctx, -1)
		if dropped {
			v.metrics.Topics.Add(ctx, -1)
		}
	}
}

// store publishes tree as the one Send() walks. The trie is immutable and mutating it copies only the
// path that changed, so publishing a change costs a pointer store and never disturbs a Send() already
// walking the tree it has. The caller must have the load-mutate-store to itself: mu for every change
// after init(), and once for the empty tree init() puts there.
func (v *Value[T]) store(tree trie.Tree[string, *sub[T]]) {
	v.tree.Store(&tree)
}

// Close ends every subscription to this Value. Each subscriber delivers the values that were already sent
// to its topic and then its iteration ends, which releases the subscriber even if its Context is never
// canceled. Close() is thread-safe and safe to call more than once.
func (v *Value[T]) Close(ctx context.Context) {
	v.init(ctx)

	v.mu.Lock()
	if v.closed.Load() {
		v.mu.Unlock()
		return
	}
	v.closed.Store(true)

	topics := v.topics
	v.topics = map[string]*sub[T]{}
	v.store(trie.Tree[string, *sub[T]]{})

	// Close() ends every subscription it takes here, whether or not its Context is ever canceled, so it is
	// the one that counts them back. release() gives back the ones that cancel before this runs, and taking
	// them out of subs is what stops the two from counting the same subscription twice.
	subs := v.subs
	v.subs = map[*subscription[T]]struct{}{}
	v.mu.Unlock()

	// Every subscription left a callback on its subscriber's Context to release it. This has just released
	// all of them, so those callbacks have nothing left to do, and each one holds this Value and the pattern
	// it names until a Context that may never be canceled is canceled. Hand them back. The stops are taken
	// outside mu, as they are reclamation and not correctness: a subscription is accounted for by having been
	// taken out of subs above, so a callback that is already running finds nothing left to do.
	for s := range subs {
		s.stop()
	}

	for _, t := range topics {
		t.value.Close(ctx)
	}

	if v.metrics != nil {
		if len(topics) > 0 {
			v.metrics.Topics.Add(ctx, -int64(len(topics)))
		}
		if len(subs) > 0 {
			v.metrics.Subscribers.Add(ctx, -int64(len(subs)))
		}
	}
}
