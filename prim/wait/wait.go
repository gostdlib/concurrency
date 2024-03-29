/*
Package wait provides a safer alternative to sync.WaitGroup. It is an alternative to the errgroup
package, but does not implement streaming as that package can. We provide a better alternative to that
in our stagepipe framework.

This package can leverage our groutines.Pool types for more control over concurrency and implements
OTEL spans to record information around what is happening in your goroutines.

Here is a basic example:

	g := wait.Group{Name: "Print  me"}

	for i := 0; i < 100; i++ {
		i := i
		g.Go(func(ctx context.Context) error{
			fmt.Println(i)
		}
	}

	if err := g.Wait(ctx); err != nil {
		// Handle error
	}
*/
package wait

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gostdlib/concurrency/goroutines"
	"github.com/gostdlib/internals/otel/span"

	"go.opentelemetry.io/otel/trace"
)

// FuncCall is a function call that can be used in various functions or methods
// in this package.
type FuncCall func(ctx context.Context) error

// Group provides a Group implementation that allows launching
// goroutines in safer way by handling the .Add() and .Done() methods in a standard
// sync.WaitGroup. This prevents problems where you forget to increment or
// decrement the sync.WaitGroup. In addition you can use a goroutines.Pool object
// to allow concurrency control and goroutine reuse (if you don't, it just uses
// a goroutine per call). It provides a Running() method that keeps track of
// how many goroutines are running. This can be used with the goroutines.Pool stats
// to understand what goroutines are in use. It has a CancelOnErr() method to
// allow mimicing of the golang.org/x/sync/errgroup package.
// Finally we provide OTEL support in the Group that can
// be named via the Group.Name string. This will provide span messages on the
// current span when Wait() is called and record any errors in the span.
type Group struct {
	count  atomic.Int64
	total  atomic.Int64
	errors atomic.Pointer[error]
	wg     sync.WaitGroup

	start    time.Time
	otelOnce sync.Once
	span     span.Span

	noCopy noCopy // Flag govet to prevent copying

	// Pool is an optional goroutines.Pool for concurrency control and reuse.
	Pool goroutines.Pool
	// CancelOnErr holds a CancelFunc that will be called if any goroutine
	// returns an error. This will automatically be called when Wait() is
	// finished and then reset to nil to allow reuse.
	CancelOnErr context.CancelFunc
	// Name provides an optional name for a WaitGroup for the purpose of
	// OTEL logging information.
	Name string
	// PoolOptions are the options to use when submitting jobs to the Pool.
	// If you have set PoolOptions but have not supplied a pool or the pool
	// doesn't support the option, the result is undefined.
	PoolOptions []goroutines.SubmitOption
}

// reset resets various internal state of the Group to allow reuse.
func (w *Group) reset() {
	w.start = time.Time{}
	w.otelOnce = sync.Once{}
	w.count.Store(0)
	w.total.Store(0)
	w.errors.Store(nil)
	w.CancelOnErr = nil
}

// Go spins off a goroutine that executes f(ctx). This will use the underlying
// goroutines.Pool if provided. If you have set PoolOptions but have
// not supplied a pool or the pool doesn't support the option, the result is
// undefined.
func (w *Group) Go(ctx context.Context, f FuncCall) {
	w.otelOnce.Do(func() {
		spanner := span.Get(ctx)
		if spanner.IsRecording() {
			if w.Name == "" {
				w.Name = "unspecified"
			}
			w.start = time.Now().UTC()

			ctx, w.span = span.New(ctx, fmt.Sprintf("wait(%s)", w.Name))
			w.span.Event(
				fmt.Sprintf("wait(%s) start", w.Name),
				"name", w.Name,
				"start", w.start,
			)
		}
	})

	t := time.Now().UTC()
	if w.span.Span != nil {
		w.span.Event("Go", "start", t)
		trace.ContextWithSpan(ctx, w.span.Span)
	}

	w.count.Add(1)
	w.total.Add(1)

	if w.Pool == nil {
		w.wg.Add(1)
		go func() {
			defer w.count.Add(-1)
			defer w.wg.Done()

			if ctx.Err() != nil {
				applyErr(&w.errors, ctx.Err())
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

	w.wg.Add(1)
	w.Pool.Submit(
		ctx,
		func(ctx context.Context) {
			defer w.count.Add(-1)
			defer w.wg.Done()

			if ctx.Err() != nil {
				applyErr(&w.errors, ctx.Err())
				return
			}

			if err := f(ctx); err != nil {
				applyErr(&w.errors, err)
				if w.CancelOnErr != nil {
					w.CancelOnErr()
				}
			}
		},
		w.PoolOptions...,
	)
}

// Running returns the number of goroutines that are currently running.
func (w *Group) Running() int {
	return int(w.count.Load())
}

// Wait blocks until all goroutines are finshed. The passed Context cannot be cancelled.
func (w *Group) Wait(ctx context.Context) error {
	defer w.reset()

	if w.Name == "" {
		w.Name = "unspecified"
	}

	// OTEL stuff.
	now := time.Now().UTC()
	defer func() {
		end := time.Now().UTC()
		w.span.Event(
			fmt.Sprintf("wait(%s) end", w.Name),
			"name", w.Name,
			"end", end.UTC(),
			"elapsed_ns", end.Sub(now),
		)
	}()

	w.waitOTELStart()
	defer w.waitOTELEnd(now)

	// Now do the actual waiting.
	w.wg.Wait()

	if w.CancelOnErr != nil {
		w.CancelOnErr()
		w.CancelOnErr = nil
	}
	err := w.errors.Load()
	if err != nil {
		w.span.Error(*err)
		return *err
	}
	return nil
}

// waitOTELStart is called when Wait() is called and will log information to the span.
func (w *Group) waitOTELStart() {
	if !w.span.IsRecording() {
		return
	}

	w.span.Event(
		"WaitGroup.Wait() called",
		"name", w.Name,
		"total goroutines", w.total.Load(),
		"cancelOnErr", w.CancelOnErr != nil,
		"using pool", w.Pool != nil,
		"start:", time.Now().UTC(),
	)
}

// waitOTELEnd is called when Wait() is finished and will log information to the span.
func (w *Group) waitOTELEnd(t time.Time) {
	if w.span.IsRecording() {
		w.span.Event(
			"wait.Group.Wait() done",
			"name", w.Name,
			"end", t,
			"elapsed_ns", time.Now().UTC().Sub(t),
		)
	}
}

// applyErr sets the error to be returned. If an error already exists, it wraps this error in that one.
// If the error is some context cancellation, that is only recorded if it is the first error.
// This uses atomic compare and swap operations to avoid mutex.
func applyErr(ptr *atomic.Pointer[error], err error) {
	for {
		existing := ptr.Load()
		if existing == nil {
			if ptr.CompareAndSwap(nil, &err) {
				return
			}
		} else {
			switch err {
			case context.Canceled, context.DeadlineExceeded:
				return
			}
			err = fmt.Errorf("%w", err)
			if ptr.CompareAndSwap(existing, &err) {
				return
			}
		}
	}
}

type noCopy struct{}

func (*noCopy) Lock() {}
