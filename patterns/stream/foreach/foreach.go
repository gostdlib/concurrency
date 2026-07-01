/*
Package foreach runs an operation for its side effects on every key/value pair in an iter.Seq2, in
parallel. It is the parallel analog of a "for range" loop whose body returns only an error: it does NOT
transform values or hand any back. The only thing returned is the combined error of the run.

foreach is useful when each value needs an expensive, independent side effect: writing to a database,
calling an API, publishing to a queue, and so on. For cheap operations a plain "for" loop in a single
goroutine will be faster, as the cost of parallelism will outweigh the gain.

Item applies a Func to each key/value pair yielded by the input sequence. Any iter.Seq2 works — over a
slice (stream.Slice, keyed by index), a map (stream.Map, keyed by key), or a channel (stream.Chan, keyed
by receive index). The value is passed by copy, so mutating it inside Func is not observable to the
caller unless the sequence yields a pointer or reference type (e.g. a *Record whose pointed-at struct
Func mutates); because Funcs run concurrently, that is only safe when each item is independent and
nothing else reads it during the pass. By default Item processes until the sequence is exhausted or the
Context is cancelled, using the worker pool attached to the Context. Errors returned by the Func are
collected and returned together (each wrapped in the final error) but do not stop processing unless
WithStopOnErr is passed.

Say you want to read numbers from a channel and print each multiplied by 5:

	input := make(chan int, 1)
	go func() {
		defer close(input)
		for i := 0; i < 1000; i++ {
			input <- i
		}
	}()

	fn := func(ctx context.Context, _ int, i int) error {
		fmt.Println(i * 5)
		return nil
	}

	if err := foreach.Item(ctx, stream.Chan(ctx, input), fn); err != nil {
		// Do something.
	}

Because no options were provided, this processes until the sequence is exhausted or the Context is
cancelled and uses a worker pool with runtime.NumCPU() * 10 workers. Pass WithStopOnErr to cancel on the
first error.
*/
package foreach

import (
	"iter"
	"runtime"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/internals/otel/span"
)

// Func is called for its side effects on every key/value pair in the input sequence. It returns only an
// error; any result must be produced as a side effect.
type Func[K, V any] func(context.Context, K, V) error

type opts struct {
	stopOnErr bool
}

// Option is an option for Item.
type Option func(o opts) (opts, error)

// WithStopOnErr causes the first error to cancel processing of the remaining values.
func WithStopOnErr() Option {
	return func(o opts) (opts, error) {
		o.stopOnErr = true
		return o, nil
	}
}

// Item runs fn for its side effects on each key/value pair yielded by seq, using the worker pool attached
// to ctx. It returns no values, only the combined error. Adapt a channel, slice, or map into seq with
// stream.Chan, stream.Slice, or stream.Map. If the pool is unlimited, up to runtime.NumCPU() * 10 workers are
// used. Errors returned by fn are collected and returned together (each wrapped in the final error) but
// do not stop processing unless WithStopOnErr is provided. Cancel ctx to stop early. A nil sequence is a
// no-op.
func Item[K, V any](ctx context.Context, seq iter.Seq2[K, V], fn Func[K, V], options ...Option) error {
	spanner := span.Get(ctx)

	if seq == nil {
		return nil
	}

	o := opts{}
	var err error
	for _, opt := range options {
		o, err = opt(o)
		if err != nil {
			return err
		}
	}

	p := context.Pool(ctx)
	if p.Limit() == 0 {
		p = p.Limited(ctx, "", runtime.NumCPU()*(10))
	}

	cancel := func() {}
	if o.stopOnErr {
		ctx, cancel = context.WithCancel(ctx)
	}
	g := p.Group()
	g.CancelOnErr = cancel

	for k, v := range seq {
		if ctx.Err() != nil {
			break
		}
		g.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, k, v)
		})
	}
	if err := g.Wait(ctx); err != nil {
		spanner.Error(err)
		return err
	}
	return ctx.Err()
}
