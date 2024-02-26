// Package queue provides a simple queue primitive that blocks on Pop() until an
// entry is available and can block on Push() when a size limit is applied.
package queue

import (
	"sync/atomic"
	"time"
)

// Queue is a simple generic queue that uses a backing array.
type Queue[A any] struct {
	queue *simple[A]

	closed atomic.Bool

	// limit limits the number of entries we can hold.
	limit chan struct{}
}

// New creates a new Queue that can hold size entries. size of <=0 is infinite.
func New[A any](size int) *Queue[A] {
	var limit chan struct{}
	if size > 0 {
		limit = make(chan struct{}, size)
	}
	return &Queue[A]{limit: limit, queue: newSimple[A]()}
}

// Push adds an entry to the queue.
func (q *Queue[A]) Push(a A) {
	if q.limit != nil {
		q.limit <- struct{}{}
	}
	q.queue.push(a)
}

// Close closes the queue. This should be called by the sender after all Add() calls
// have finished. Calling Add() after Close() is considered an error.
func (q *Queue[A]) Close() {
	q.closed.Store(true)
}

// Pop pops the next value off the Queue. This will block until either Close() is called
// or a value in the queue can be pulled. If ok is nil, this means that val is the zero
// value (which was not added to the queue), the Queue had Closed() called and the Queue
// is empty.
func (q *Queue[A]) Pop() (val A, ok bool) {
	defer func() {
		if q.limit != nil {
			select {
			case <-q.limit:
			default:
				// Normally when you have a limiter, you don't need a select{}, its
				// 1 for 1. In this case a Pop() can be called when Close() has been called.
				// If you don't have a default, this blocks forever.
			}
		}
	}()

	var s time.Duration
	for {
		a, ok := q.queue.pop()
		if ok {
			return a, ok
		}

		if q.closed.Load() {
			return a, ok
		}

		s += 100 * time.Nanosecond
		if s > 10*time.Millisecond {
			s = 10 * time.Millisecond
		}
		time.Sleep(s)
	}
}

// simple is a simple lock free queue using 1.19's atomic.Pointer.
type simple[A any] struct {
	head  atomic.Pointer[node[A]]
	tail  atomic.Pointer[node[A]]
	dummy node[A]
}

func newSimple[A any]() *simple[A] {
	var s simple[A]
	s.head.Store(&s.dummy)
	s.tail.Store(s.head.Load())
	return &s
}

func (s *simple[A]) pop() (A, bool) {
	for {
		h := s.head.Load()
		n := h.next.Load()
		if n != nil {
			if s.head.CompareAndSwap(h, n) {
				return n.val, true
			} else {
				continue
			}
		} else {
			var v A
			return v, false
		}
	}
}

func (s *simple[A]) push(val A) {
	n := &node[A]{val: val}
	for {
		rt := s.tail.Load()

		if rt.next.CompareAndSwap(nil, n) {
			s.tail.Store(n)
			return
		} else {
			continue
		}
	}
}

type node[A any] struct {
	val  A
	next atomic.Pointer[node[A]]
}

// Keeping the old version around in case I run into any problems.
/*
// simple provides a simple queue mechanism. This one doesn't block or have a size limit.
// Our Queue uses this underneath and adds the other mechanisms on top of this.
type simple[A any] struct {
	data []A
	mu   sync.Mutex
}

func (s *simple[A]) pop() (val A, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		var x A
		return x, false
	}
	x := s.data[0]
	s.data = s.data[1:]
	return x, true
}

func (s *simple[A]) push(a A) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = append(s.data, a)
}
*/
