/*
Package stream holds adapters shared by the parallel stream-processing packages in its sub-directories
(currently foreach). Those packages operate on an iter.Seq2 of key/value pairs, and this package bridges
common data sources into one:

  - Chan turns a receive channel into an iter.Seq2 keyed by a zero-based receive index.
  - Slice turns a slice into an iter.Seq2 of index and element (via slices.All).
  - Map turns a map into an iter.Seq2 of key and value (via maps.All).
  - Seq turns a single-value iter.Seq into an iter.Seq2 keyed by a zero-based index.

For example, to process each value received on a channel in parallel and stream the results back:

	ch := make(chan int, 1)
	context.Pool(ctx).Submit(ctx, func() {
		defer close(ch)
		for i := 0; i < 10; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	})

	for k, resp := range foreach.Item(ctx, stream.Chan(ctx, ch), fn) {
		if resp.Err != nil {
			// Handle the error.
			continue
		}
		// Use resp.V.
	}
*/
package stream

import (
	"iter"
	"maps"
	"slices"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/values/generics/promises"
)

// Result is a result of some stream that can also be an error.
type Result[T any] = promises.Response[T]

// Chan adapts a receive channel into an iter.Seq2 whose key is a zero-based index counting the values
// received and whose value is the value received on c. Iteration ends when c is closed, when ctx is
// cancelled, or when the consumer stops early. It is Context-aware so an idle channel does not keep the
// iteration open after ctx is cancelled. A nil channel yields nothing and ends only when ctx is
// cancelled.
//
// Cancellation always wins deterministically over channel activity. ctx.Err() is checked at the top of
// every iteration, so an already-cancelled ctx returns before consuming any value from c: buffered,
// closed-with-buffered, or continuously-fed channels do not yield after cancellation. ctx.Err() is also
// re-checked after a successful receive but before yielding, so a cancellation that raced the receive
// ends the iteration. As a consequence, a value received in the same instant as cancellation may be
// consumed off c and dropped without being yielded (at most one such value per iteration).
func Chan[V any](ctx context.Context, c <-chan V) iter.Seq2[int, V] {
	return func(yield func(int, V) bool) {
		i := 0
		for {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				if ctx.Err() != nil {
					return
				}
				if !yield(i, v) {
					return
				}
				i++
			}
		}
	}
}

// Slice adapts a slice into an iter.Seq2 of index and element. It is a thin wrapper around slices.All.
func Slice[S ~[]E, E any](s S) iter.Seq2[int, E] {
	return slices.All(s)
}

// Map adapts a map into an iter.Seq2 of key and value. It is a thin wrapper around maps.All. Iteration
// order is unspecified, as with any Go map range.
func Map[M ~map[K]V, K comparable, V any](m M) iter.Seq2[K, V] {
	return maps.All(m)
}

// Seq adapts a single-value iter.Seq into an iter.Seq2 whose key is a zero-based index counting the
// values yielded and whose value is the value from s.
func Seq[V any](s iter.Seq[V]) iter.Seq2[int, V] {
	return func(yield func(int, V) bool) {
		i := 0
		for v := range s {
			if !yield(i, v) {
				return
			}
			i++
		}
	}
}
