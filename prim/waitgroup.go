package prim

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gostdlib/concurrency/goroutines"
)

// FunCall is a function call that can be used in various functions or methods
// in this package.
type FuncCall func(ctx context.Context) error

// WaitGroup provides a WaitGroup implementation that allows launching
// goroutines in safer way by handling the .Add() and .Done() methods. This prevents
// problems where you forget to increment or decrement the sync.WaitGroup. In
// addition you can use a goroutines.Pool object to allow concurrency control
// and goroutine reuse. It provides a Running() method that keeps track of
// how many GoRoutines are running. This can be used with the goroutines.Pool stats
// to understand what goroutines are in use. It has a CancelOnErr() method to
// allow mimicing of the golang.org/x/sync/errgroup package.
// Finally we provide OTEL support in the WaitGroup that can
// be named via the WaitGroup.Name string.
type WaitGroup struct {
	// Name provides an optional name for a WaitGroup for the purpose of
	// OTEL logging information.
	Name string
	// Pool is an optional goroutines.Pool for concurrency control and reuse.
	Pool goroutines.Pool
	// CancelOnErr holds a CancelFunc that will be called if any goroutine
	// returns an error. This will automatically be called when Wait() is
	// finishecd and then reset to nil.
	CancelOnErr context.CancelFunc

	count  atomic.Int64
	errors atomic.Pointer[error]
	wg     sync.WaitGroup
}

// Go spins off a goroutine that executes f(ctx). This will use the underlying
// goroutines.Pool if provided.
func (w *WaitGroup) Go(ctx context.Context, f FuncCall) {
	w.count.Add(1)

	if w.Pool == nil {
		w.wg.Add(1)
		go func() {
			defer w.count.Add(-1)
			defer w.wg.Done()

			if ctx.Err() != nil {
				return
			}

			if err := f(ctx); err != nil {
				applyErr(&w.errors, err)
				if w.CancelOnErr != nil {
					w.CancelOnErr()
				}
			}
		}()
		return
	}

	w.Pool.Submit(
		ctx,
		func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}

			if err := f(ctx); err != nil {
				if err := f(ctx); err != nil {
					applyErr(&w.errors, err)
					if w.CancelOnErr != nil {
						w.CancelOnErr()
					}
				}
			}
		},
	)
}

// Running returns the number of goroutines that are currently running.
func (w *WaitGroup) Running() int {
	return int(w.count.Load())
}

// Wait blocks until all goroutines are finshed.
func (w *WaitGroup) Wait() error {
	if w.Pool == nil {
		w.wg.Wait()
	} else {
		w.Pool.Wait()
	}
	if w.CancelOnErr != nil {
		w.CancelOnErr()
		w.CancelOnErr = nil
	}
	err := w.errors.Load()
	if err != nil {
		return *err
	}
	return nil
}

/*
// A WaitGroup waits for a collection of goroutines to finish.
// The main goroutine calls Add to set the number of
// goroutines to wait for. Then each of the goroutines
// runs and calls Done when finished. At the same time,
// Wait can be used to block until all goroutines have finished.
//
// A WaitGroup must not be copied after first use.
//
// In the terminology of the Go memory model, a call to Done
// “synchronizes before” the return of any Wait call that it unblocks.
type waitGroup struct {
	noCopy noCopy

	// 64-bit value: high 32 bits are counter, low 32 bits are waiter count.
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers only guarantee that 64-bit fields are 32-bit aligned.
	// For this reason on 32 bit architectures we need to check in state()
	// if state1 is aligned or not, and dynamically "swap" the field order if
	// needed.
	state1 uint64
	state2 uint32
}

// state returns pointers to the state and sema fields stored within wg.state*.
func (wg *waitGroup) state() (statep *uint64, semap *uint32) {
	if unsafe.Alignof(wg.state1) == 8 || uintptr(unsafe.Pointer(&wg.state1))%8 == 0 {
		// state1 is 64-bit aligned: nothing to do.
		return &wg.state1, &wg.state2
	} else {
		// state1 is 32-bit aligned but not 64-bit aligned: this means that
		// (&state1)+4 is 64-bit aligned.
		state := (*[3]uint32)(unsafe.Pointer(&wg.state1))
		return (*uint64)(unsafe.Pointer(&state[1])), &state[0]
	}
}

// Add adds delta, which may be negative, to the WaitGroup counter.
// If the counter becomes zero, all goroutines blocked on Wait are released.
// If the counter goes negative, Add panics.
//
// Note that calls with a positive delta that occur when the counter is zero
// must happen before a Wait. Calls with a negative delta, or calls with a
// positive delta that start when the counter is greater than zero, may happen
// at any time.
// Typically this means the calls to Add should execute before the statement
// creating the goroutine or other event to be waited for.
// If a WaitGroup is reused to wait for several independent sets of events,
// new Add calls must happen after all previous Wait calls have returned.
// See the WaitGroup example.
func (wg *waitGroup) Add(delta int) {
	statep, semap := wg.state()
	if race.Enabled {
		_ = *statep // trigger nil deref early
		if delta < 0 {
			// Synchronize decrements with Wait.
			race.ReleaseMerge(unsafe.Pointer(wg))
		}
		race.Disable()
		defer race.Enable()
	}
	state := atomic.AddUint64(statep, uint64(delta)<<32)
	v := int32(state >> 32)
	w := uint32(state)
	if race.Enabled && delta > 0 && v == int32(delta) {
		// The first increment must be synchronized with Wait.
		// Need to model this as a read, because there can be
		// several concurrent wg.counter transitions from 0.
		race.Read(unsafe.Pointer(semap))
	}
	if v < 0 {
		panic("sync: negative WaitGroup counter")
	}
	if w != 0 && delta > 0 && v == int32(delta) {
		panic("sync: WaitGroup misuse: Add called concurrently with Wait")
	}
	if v > 0 || w == 0 {
		return
	}
	// This goroutine has set counter to 0 when waiters > 0.
	// Now there can't be concurrent mutations of state:
	// - Adds must not happen concurrently with Wait,
	// - Wait does not increment waiters if it sees counter == 0.
	// Still do a cheap sanity check to detect WaitGroup misuse.
	if *statep != state {
		panic("sync: WaitGroup misuse: Add called concurrently with Wait")
	}
	// Reset waiters count to 0.
	*statep = 0
	for ; w != 0; w-- {
		runtime_Semrelease(semap, false, 0)
	}
}

// Done decrements the WaitGroup counter by one.
func (wg *waitGroup) Done() {
	wg.Add(-1)
}

// Wait blocks until the WaitGroup counter is zero or the Context is cancelled.
// The returned bool indicates if the Wait() succeeded.
func (wg *waitGroup) Wait(ctx context.Context) bool {
	statep, semap := wg.state()
	if race.Enabled {
		_ = *statep // trigger nil deref early
		race.Disable()
	}
	for {
		log.Println("this is happening")
		if ctx.Err() != nil {
			log.Println("this happened")
			if race.Enabled {
				race.Enable()
				race.Acquire(unsafe.Pointer(wg))
			}
			return false
		}
		state := atomic.LoadUint64(statep)
		v := int32(state >> 32)
		w := uint32(state)
		if v == 0 {
			// Counter is 0, no need to wait.
			if race.Enabled {
				race.Enable()
				race.Acquire(unsafe.Pointer(wg))
			}
			return true
		}
		// Increment waiters count.
		if atomic.CompareAndSwapUint64(statep, state, state+1) {
			if race.Enabled && w == 0 {
				// Wait must be synchronized with the first Add.
				// Need to model this is as a write to race with the read in Add.
				// As a consequence, can do the write only for the first waiter,
				// otherwise concurrent Waits will race with each other.
				race.Write(unsafe.Pointer(semap))
			}
			runtime_Semacquire(semap)
			if *statep != 0 {
				panic("sync: WaitGroup is reused before previous Wait has returned")
			}
			if race.Enabled {
				race.Enable()
				race.Acquire(unsafe.Pointer(wg))
			}
			return true
		}
	}
}

type noCopy struct{ sync.Mutex }

func (n *noCopy) Lock()   { n.Lock() }
func (n *noCopy) Unlock() { n.Unlock() }

// Semacquire waits until *s > 0 and then atomically decrements it.
// It is intended as a simple sleep primitive for use by the synchronization
// library and should not be used directly.
//
//go:linkname runtime_Semacquire sync.runtime_Semacquire
func runtime_Semacquire(s *uint32)

// SemacquireMutex is like Semacquire, but for profiling contended Mutexes.
// If lifo is true, queue waiter at the head of wait queue.
// skipframes is the number of frames to omit during tracing, counting from
// runtime_SemacquireMutex's caller.
//
//go:linkname runtime_SemacquireMutex sync.runtime_SemacquireMutex
func runtime_SemacquireMutex(s *uint32, lifo bool, skipframes int)

// Semrelease atomically increments *s and notifies a waiting goroutine
// if one is blocked in Semacquire.
// It is intended as a simple wakeup primitive for use by the synchronization
// library and should not be used directly.
// If handoff is true, pass count directly to the first waiter.
// skipframes is the number of frames to omit during tracing, counting from
// runtime_Semrelease's caller.
//
//go:linkname runtime_Semrelease sync.runtime_Semrelease
func runtime_Semrelease(s *uint32, handoff bool, skipframes int)
*/
