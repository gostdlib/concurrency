// Package atomics provides other atomic types that are not provided by the sync/atomic package.
package atomics

import (
	"sync"
	"sync/atomic"
)

// RWValue is a replacement for sync.RWMutex or atomic.Pointer when protecting a value. This version allows
// updating a value without blocking all readers. This is useful when you never want stop the world for a read.
// Because replacing values must always been done with a copy of the value and not a modification of the
// existing value, this is not a good choice for situations with high write contention or large values.
// T is the type of the value being protected and will be stored as a *T. T should not be a pointer.
type RWValue[T any] struct {
	mu sync.Mutex
	v  atomic.Pointer[T]
}

// Load returns the value.
func (r *RWValue[T]) Load() T {
	v := r.v.Load()
	if v == nil {
		var zero T
		return zero
	}
	return *v
}

// Store sets the value to v. This blocks any other writers until the Store is complete.
// If you are only using this and not LoadReplace, you should use sync/atomic.Pointer directly.
func (r *RWValue[T]) Store(v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.v.Store(&v)
}

// LoadReplacer is a function that will receive the current value and return the new value to be stored.
// This is useful when you want to update a value based on the current value. Note that you must make a copy
// and not modify the value in place.
type LoadReplacer[T any] func(v T) (T, error)

// LoadReplace will call the LoadReplacer function with the current value and store the new value. If the
// current value is nil, it will pass a zero value of T to the LoadReplacer function. If the LoadReplacer
// returns an error, it will be returned and the value will not be updated. LoadReplace will block all other
// writers while the LoadReplacer function is running.
func (r *RWValue[T]) LoadReplace(lr LoadReplacer[T]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	v := r.v.Load()
	var n T
	var err error
	if v == nil {
		var zero T
		n, err = lr(zero)
		if err != nil {
			return err
		}
	}
	n, err = lr(*v)
	if err != nil {
		return err
	}
	r.v.Store(&n)
	return nil
}
