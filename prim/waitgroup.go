package prim

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/internals/otel/span"
)

// FunCall is a function call that can be used in various functions or methods
// in this package.
type FuncCall func(ctx context.Context) error

// WaitGroup provides a WaitGroup implementation that allows launching
// goroutines in safer way by handling the .Add() and .Done() methods. This prevents
// problems where you forget to increment or decrement the sync.WaitGroup. In
// addition you can use a goroutines.Pool object to allow concurrency control
// and goroutine reuse (if you don't, it just uses a goroutine per call).
// It provides a Running() method that keeps track of
// how many goroutines are running. This can be used with the goroutines.Pool stats
// to understand what goroutines are in use. It has a CancelOnErr() method to
// allow mimicing of the golang.org/x/sync/errgroup package.
// Finally we provide OTEL support in the WaitGroup that can
// be named via the WaitGroup.Name string. This will provide span messages on the
// current span when Wait() is called and record any errors in the span.
type WaitGroup struct {
	count  atomic.Int64
	total  atomic.Int64
	errors atomic.Pointer[error]
	wg     sync.WaitGroup
	// Pool is an optional goroutines.Pool for concurrency control and reuse.
	Pool goroutines.Pool
	// CancelOnErr holds a CancelFunc that will be called if any goroutine
	// returns an error. This will automatically be called when Wait() is
	// finishecd and then reset to nil.
	CancelOnErr context.CancelFunc
	// Name provides an optional name for a WaitGroup for the purpose of
	// OTEL logging information.
	Name string
}

// Go spins off a goroutine that executes f(ctx). This will use the underlying
// goroutines.Pool if provided. If you pass a goroutines.SubmitOption but have
// not supplied a pool or the pool doesn't support the option, this may panic.
func (w *WaitGroup) Go(ctx context.Context, f FuncCall, options ...goroutines.SubmitOption) {
	w.count.Add(1)
	w.total.Add(1)

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
		options...,
	)
}

// Running returns the number of goroutines that are currently running.
func (w *WaitGroup) Running() int {
	return int(w.count.Load())
}

// Wait blocks until all goroutines are finshed. The passed Context cannot be cancelled.
func (w *WaitGroup) Wait(ctx context.Context) error {
	if w.Name == "" {
		w.Name = "unspecified"
	}

	// OTEL stuff.
	now := time.Now()
	spanner := span.Get(ctx)
	w.waitOTELStart(spanner)
	defer w.waitOTELEnd(spanner, now)

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
		spanner.Error(*err)
		return *err
	}
	return nil
}

func (w *WaitGroup) waitOTELStart(spanner span.Span) {
	hasPool := w.Pool != nil
	cancelOnErr := w.CancelOnErr != nil
	spanner.Event(
		"WaitGroup.Wait() called",
		"name", w.Name,
		"total goroutines", w.total.Load(),
		"cancelOnErr", cancelOnErr,
		"using pool", hasPool,
	)
}

func (w *WaitGroup) waitOTELEnd(spanner span.Span, t time.Time) {
	spanner.Event("WaitGroup(%s).Wait() done", "elapsed_ns", w.Name, time.Since(t))
	// Reset waitgroup counters.
	w.count.Store(0)
	w.total.Store(0)
	w.errors.Store(nil)
}
