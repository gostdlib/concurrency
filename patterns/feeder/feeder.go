// Package feeder is a helper package for goroutines that are going to do complex work and then mutate some complex
// value. That value may need to do various side effects before or after setting a value. Typically this is going to be
// something that can error due to network problems. We want to be able to do
// retries and have a set of actions that determine what happens if a key already exists for the value.
// The package exists to try and cut down on potential concurrency bugs by giving structure and keeping retries
// in the right boxes.
//
// Basic usage feeds a pipeline of operations into a Value such as Map:
//
//	m := &feeder.Map[string, int]{M: map[string]int{}}
//
//	f, err := feeder.NewFeeder[string, int](ctx, m)
//	if err != nil {
//		return err
//	}
//
//	// A KVPipeline supplies the operations to apply, then closes the channel when done.
//	pipe := func(ctx context.Context) (chan feeder.KeyVal[string, int], chan struct{}, error) {
//		ch := make(chan feeder.KeyVal[string, int], 2)
//		ch <- feeder.KeyVal[string, int]{Op: feeder.Add, K: "a", V: 1}
//		ch <- feeder.KeyVal[string, int]{Op: feeder.Delete, K: "old"}
//		close(ch)
//		return ch, make(chan struct{}), nil
//	}
//
//	// Feed runs in the background; the returned channel yields the terminal error (nil on success).
//	if err := <-f.Feed(ctx, pipe); err != nil {
//		return err
//	}
//
// Pass WithRetry to re-establish a pipeline that fails transiently; wrap an error with ErrPermanent
// to stop retries:
//
//	f, err := feeder.NewFeeder[string, int](ctx, m, feeder.WithRetry[string, int](backoff))
package feeder

import (
	"cmp"
	"fmt"
	"iter"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/base/values/generics/result"
)

// ErrPermanent prevents a retry from happening. Usually added to an existing error with
// fmt.Errorf("%w: %w", origErr, ErrPermanent).
var ErrPermanent = exponential.ErrPermanent

// Value is a generic constraint that is used to define the behavior of some type of value that is going to be
// mutated.
type Value[K cmp.Ordered, V any] interface {
	// Set attempts to set the value at key k to value of v. This may handle conflicts in any way it chooses,
	// such as replacing a value, ignoring the new value or giving an error. If the error that is being returned
	// should stop the operation from being retried, it should wrap ErrPermanent.
	Set(k K, v V) error
	// Delete deletes a value. A non-existent value does not return an error.
	Delete(k K) error
}

// Map provides a Value type that uses a map[K]V protected by a sync.RWMutex.
type Map[K cmp.Ordered, V any] struct {
	// M is the map to set. You should not access the map via M, use the accessor methods.
	M map[K]V
	// SetAccept is called with the key, the incoming value, the previous value, and whether an existing value
	// is being replaced. It returns whether we accept the change and an error. Errors always prevent the change
	// and are retried unless they wrap ErrPermanent. If nil, all changes are accepted. Because it runs while the
	// write lock is held, it is the place to perform side effects (such as a database write) that must be atomic
	// with the in-memory change.
	SetAccept func(key K, val, prev V, replaced bool) (bool, error)
	// DeleteAccept is called with the key, the value being deleted, and whether the value was found. If it returns
	// true then the value can be deleted. If nil, all deletes are accepted. Like SetAccept, it runs while the write
	// lock is held and is the place for side effects that must be atomic with the delete.
	DeleteAccept func(key K, prev V, found bool) (bool, error)

	mu sync.RWMutex
}

// Set implements Value.Set().
func (m *Map[K, V]) Set(k K, v V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prev, exists := m.M[k]
	accept, err := true, error(nil)
	if m.SetAccept != nil {
		accept, err = m.SetAccept(k, v, prev, exists)
	}
	if err != nil {
		return err
	}
	if !accept {
		return nil
	}
	m.M[k] = v
	return nil
}

// Delete deletes k in the map. It is a no-op on a non-found keys.
func (m *Map[K, V]) Delete(k K) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prev, exists := m.M[k]
	accept, err := true, error(nil)
	if m.DeleteAccept != nil {
		accept, err = m.DeleteAccept(k, prev, exists)
	}
	if err != nil {
		return err
	}
	if !accept {
		return nil
	}
	delete(m.M, k)
	return nil
}

// Get gets value at key k. ok indicates if the key was found. Protected with RLock().
func (m *Map[K, V]) Get(k K) (v V, ok bool) {
	m.mu.RLock()
	v, ok = m.M[k]
	m.mu.RUnlock()
	return v, ok
}

// All read locks the map and ranges over all items in it. Only use this if the map is small.
// For larger maps, consider ShardedMap.
func (m *Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		for k, v := range m.M {
			if !yield(k, v) {
				return
			}
		}
	}
}

// ShardedMap provides a Value type that uses a sync.ShardedMap internally.
type ShardedMap[K cmp.Ordered, V any] struct {
	// m is the map to set.  You should not access the map via m, use the accessor methods.
	m sync.ShardedMap[K, V]
	// SetAccept is called with the key, the incoming value, the previous value, and whether an existing value
	// is being replaced. It returns whether we accept the change and an error. Errors always prevent the change
	// and are retried unless they wrap ErrPermanent. If nil, all changes are accepted. Because it runs while the
	// write lock is held, it is the place to perform side effects (such as a database write) that must be atomic
	// with the in-memory change.
	SetAccept func(key K, val, prev V, replaced bool) (bool, error)
	// DeleteAccept is called with the key, the value being deleted, and whether the value was found. If it returns
	// true then the value can be deleted. If nil, all deletes are accepted. Like SetAccept, it runs while the write
	// lock is held and is the place for side effects that must be atomic with the delete.
	DeleteAccept func(key K, prev V, found bool) (bool, error)
}

// Set implements Value.Set().
func (m *ShardedMap[K, V]) Set(k K, v V) error {
	var err error
	m.m.SetAccept(k, v, func(prev V, replaced bool) bool {
		if m.SetAccept == nil {
			return true
		}
		var b bool
		b, err = m.SetAccept(k, v, prev, replaced)
		if err != nil {
			return false
		}
		return b
	})
	return err
}

// Delete implements Value.Delete().
func (m *ShardedMap[K, V]) Delete(k K) error {
	var err error
	m.m.DeleteAccept(k, func(prev V, found bool) bool {
		if m.DeleteAccept == nil {
			return true
		}
		var b bool
		b, err = m.DeleteAccept(k, prev, found)
		if err != nil {
			return false
		}
		return b
	})
	return err
}

// All read locks various shards in the map and ranges over all items in them.
func (m *ShardedMap[K, V]) All() iter.Seq2[K, V] {
	return m.m.All(sync.WithLock())
}

// Op is an operation to do on a key in our Value.
//
//go:generate go tool github.com/gostdlib/base/values/generators/stringer -type=Op
type Op uint8

const (
	// UnknownOp indicates a bug in the calling code.
	UnknownOp Op = iota
	// Add adds a value at key.
	Add
	// Delete removes a value at key.
	Delete
)

// KeyVal is used to do an operation on a key.
type KeyVal[K cmp.Ordered, V any] struct {
	// Op is the operation to perform.
	Op Op
	// K is the key. This is always set.
	K K
	// V is the value. Should not be set if Op is Delete.
	V V
	// Result is the result of the operation. If nil this will not be set.
	Result *result.Value[struct{}]

	// Err is set by KVPipeline if it ran into an error when streaming the values. If set,
	// Result can be ignored and everything else should be the zero value.
	Err error
}

// KVPipeline returns a channel that is used to feed a Value type. Once the returned channel is closed
// the feeder will close, unless the final value is KeyVal.Err != nil. In that case if Feeder has
// WithRetry() set, it will call KVPipeline again to re-establish the connection. A KVPipeline should honor
// a Context timeout until it returns the channel, after which that timeout is not honored. A caller
// can call stop() in order to stop the pipeline. If there is some error to startup, it should be returned.
// If that should not be retried, then ErrPermanent should be included.
type KVPipeline[K cmp.Ordered, V any] func(context.Context) (pipe chan KeyVal[K, V], stop chan struct{}, err error)

// Feeder is used add or delete key values into our Value type.
type Feeder[K cmp.Ordered, V any] struct {
	v    Value[K, V]
	back *exponential.Backoff
}

// FeederOption is an option to the NewFeeder constructor.
type FeederOption[K cmp.Ordered, V any] func(*Feeder[K, V]) error

// WithRetry sets a retry policy applied by Feed().
func WithRetry[K cmp.Ordered, V any](b *exponential.Backoff) FeederOption[K, V] {
	return func(o *Feeder[K, V]) error {
		o.back = b
		return nil
	}
}

// NewFeeder is a constructor for Feeder.
func NewFeeder[K cmp.Ordered, V any](ctx context.Context, v Value[K, V], options ...FeederOption[K, V]) (*Feeder[K, V], error) {
	f := &Feeder[K, V]{
		v: v,
	}

	for _, o := range options {
		if err := o(f); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// Feed takes an input pipeline and starts feeding it into the Feeder's Value. This can be called multiple times for
// different pipe constructors you want to feed into our value. When the feed is done, finished will return an error
// or nil. This starts 1 goroutine per Feed call. Consider ShardedMap Value if doing many feeds.
func (f *Feeder[K, V]) Feed(ctx context.Context, pipe KVPipeline[K, V]) (finished chan error) {
	finished = make(chan error, 1)

	_ = context.Tasks(ctx).Once(ctx, "feeder", func(ctx context.Context) error {
		return f.pump(ctx, pipe, finished)
	})

	return finished
}

// pump establishes the pipeline and feeds it into the Value until it completes or errors. With a retry policy set,
// a retryable error re-establishes the pipeline via the backoff while an error wrapping ErrPermanent stops it. pump
// owns finished: it sends the terminal result and closes finished exactly once.
func (f *Feeder[K, V]) pump(ctx context.Context, pipe KVPipeline[K, V], finished chan error) error {
	var err error
	if f.back == nil {
		err = f.attempt(ctx, pipe)
	} else {
		err = f.back.Retry(ctx, func(ctx context.Context, _ exponential.Record) error {
			return f.attempt(ctx, pipe)
		})
	}
	finished <- err
	close(finished)
	return err
}

// attempt establishes the pipeline once and feeds it into the Value until the pipe closes or an error occurs.
func (f *Feeder[K, V]) attempt(ctx context.Context, pipe KVPipeline[K, V]) error {
	p, stop, err := pipe(ctx)
	if err != nil {
		return err
	}
	return f.feed(ctx, p, stop)
}

func (f *Feeder[K, V]) feed(ctx context.Context, p chan KeyVal[K, V], stop chan struct{}) error {
	for {
		select {
		case <-stop:
			return nil
		case kv, ok := <-p:
			if !ok {
				return nil
			}
			if kv.Err != nil {
				return kv.Err
			}
			switch kv.Op {
			case Add:
				err := f.set(ctx, kv.K, kv.V)
				if kv.Result != nil {
					kv.Result.Set(struct{}{}, err)
				}
				if err != nil {
					return err
				}
			case Delete:
				err := f.delete(ctx, kv.K)
				if kv.Result != nil {
					kv.Result.Set(struct{}{}, err)
				}
				if err != nil {
					return err
				}
			default:
				panic(fmt.Sprintf("cannot send an Op with code %v", kv.Op))
			}
		}
	}
}

func (f *Feeder[K, V]) set(ctx context.Context, k K, v V) error {
	if f.back == nil {
		return f.v.Set(k, v)
	}
	return f.back.Retry(
		ctx,
		func(ctx context.Context, r exponential.Record) error {
			return f.v.Set(k, v)
		},
	)
}

func (f *Feeder[K, V]) delete(ctx context.Context, k K) error {
	if f.back == nil {
		return f.v.Delete(k)
	}
	return f.back.Retry(
		ctx,
		func(ctx context.Context, r exponential.Record) error {
			return f.v.Delete(k)
		},
	)
}
