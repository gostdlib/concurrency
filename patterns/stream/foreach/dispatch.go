package foreach

import (
	"errors"
	"fmt"
	"iter"

	"github.com/gostdlib/base/concurrency/background"
	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/concurrency/worker"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/concurrency/patterns/stream"
	"github.com/gostdlib/internals/otel/span"
)

// dispatcher runs the fan-out side of Item: a puller goroutine ranges seq and hands pairs over, a
// dispatch goroutine runs fn on the pool for each pair and hands every response to deliver. Both run
// as background tasks (worker.Default's unbounded pool), never on the possibly-Limited pool the
// ItemFuncs use — a coordinator occupying a worker slot deadlocks a Limit(1) pool and steals a worker
// from every other.
type dispatcher[K, V, R any] struct {
	seq iter.Seq2[K, V]
	fn  ItemFunc[K, V, R]
	o   options
	// cancel cancels Item's internal Context; the dispatch wrapper calls it on the first ItemFunc error
	// when WithStopOnErr is set, before delivering that pair's response.
	cancel func()
	// deliver hands off the i'th dispatched pair's response; it may block until the consumer takes it
	// or abandons the range.
	deliver func(i int, k K, resp stream.Result[R])
	// finish signals that no more responses will be delivered.
	finish func()
	// pull hands pairs from the puller to the dispatch loop.
	pull chan input[K, V]
	// done closes after finish: the end-of-range join point for consumers.
	done chan struct{}
	// workers is the pool the ItemFuncs run on.
	workers *worker.Pool
}

// launch starts Item's fan-out and returns at once: it takes a Tasks manager (the caller passes the
// Context's) and submits the puller (pullSeq) and the dispatch loop (run) as two one-shot background tasks, off that manager
// rather than the possibly-Limited ItemFunc pool so a coordinator never occupies a worker slot. It
// runs once per range, on the range goroutine; the caller then ranges the delivery side and joins on
// d.done.
//
// On the normal path run owns teardown — pullSeq closes d.pull, and run calls d.finish then closes
// d.done — so launch only has to cover a task that fails to start. A failed start must not vanish:
// with nothing delivered, a dispatch that did start drains the (closed) pull and finishes with zero
// responses, a run indistinguishable from empty input. That is the silent-empty-range hazard, and each
// branch closes it. If the puller fails to start, launch closes d.pull in its place and, while ctx is
// alive, delivers exactly one in-band Response wrapping ErrPermanent — permanent because a Tasks
// manager that will not start (a Closed one) does not recover on a retry, matching how invalid options
// report. If the dispatcher fails to start, run is not there to finish or close done, so launch calls
// d.finish and closes d.done itself, and reports the same in-band ErrPermanent unless the puller branch
// already did — the reported guard stops one Closed manager, which fails both Once calls, from
// double-delivering.
//
// A start failure while ctx is already cancelled delivers nothing on purpose: the caller tells a
// truncated run from a complete one by checking ctx.Err() after the range, so an in-band error there
// would misattribute cancellation to a startup fault.
func (d *dispatcher[K, V, R]) launch(ctx context.Context, tasks *background.Tasks) {
	reported := false
	if err := tasks.Once(ctx, "pull", func(ctx context.Context) error { d.pullSeq(ctx); return nil }); err != nil {
		close(d.pull)
		// Mirror the dispatch-failure branch: a non-ctx pull-startup failure must surface in-band, or a
		// dispatch that then drains the closed pull finishes with no response and the run looks identical
		// to empty input. (Not reachable today: the only failure mode, a Closed Tasks manager, fails both
		// Once calls, so this is contract-hardening; the reported flag stops it double-delivering.)
		if ctx.Err() == nil {
			var zero K
			d.deliver(0, zero, stream.Result[R]{Err: fmt.Errorf("foreach: could not start pull: %w: %w", err, ErrPermanent)})
			reported = true
		}
	}
	if err := tasks.Once(ctx, "dispatch", func(ctx context.Context) error { d.run(ctx); return nil }); err != nil {
		if ctx.Err() == nil && !reported {
			var zero K
			d.deliver(0, zero, stream.Result[R]{Err: fmt.Errorf("foreach: could not start dispatch: %w: %w", err, ErrPermanent)})
		}
		d.finish()
		close(d.done)
	}
}

// call runs fn for a pair; under WithGate every attempt runs inside the backoff, and the gate closes
// at the first retryable failure — from inside the retry, so the backoff's sleep separates the first
// and second calls to the struggling dependency and the whole recovery, sleeps included, keeps
// dispatch paused. A healthy pair's single attempt never touches the gate, an error wrapping
// ErrPermanent never closes it, and the backoff's own permanence classification (ErrTransformer,
// attempt limits) ends the retry without another invocation. The gate reopens on every exit, panics
// included.
func (d *dispatcher[K, V, R]) call(ctx context.Context, k K, v V) (R, error) {
	if d.o.gate == nil {
		return d.fn(ctx, k, v)
	}
	var r R
	closed := false
	defer func() {
		if closed {
			d.o.gate.resume()
		}
	}()
	err := d.o.boff.Retry(ctx, func(ctx context.Context, _ exponential.Record) error {
		var rerr error
		r, rerr = d.fn(ctx, k, v)
		if rerr != nil && !closed && !errors.Is(rerr, ErrPermanent) {
			d.o.gate.pause()
			closed = true
		}
		return rerr
	})
	return r, err
}

// pullSeq ranges seq, handing each pair to the dispatch loop. It exists so the dispatch loop can
// always be interrupted by ctx: a sequence blocked on an external source (say stream.Chan on an idle
// channel) parks only this puller, keeping cancellation and the end-of-range join bounded. A parked
// puller unwinds when its source yields, closes or the Context the sequence captured ends; at most
// one pulled pair is discarded when that happens after cancellation.
func (d *dispatcher[K, V, R]) pullSeq(ctx context.Context) {
	defer close(d.pull)
	for k, v := range d.seq {
		select {
		case d.pull <- input[K, V]{k: k, v: v}:
		case <-ctx.Done():
			return
		}
	}
}

// run dispatches until the puller is exhausted, ctx is cancelled or the checkpoint fails, waits for
// the in-flight work, then finishes delivery and closes done.
//
// Each dispatched pair is submitted with the real ctx so the pool's slot acquire is cancellable — a
// caller's cancel or early break must unpark a dispatch blocked waiting for a busy slot on a shared
// Limited pool, which a non-cancellable Context could not do. The cost of the cancellable submit is
// that a cancelled acquire (worker.Pool declines the job) or a cancelled Group (sync.Group.executeFn
// skips the fn when context.Cause(ctx) is non-nil) never runs the fn wrapper, which would silently
// lose a dispatched pair's Response. A ledger closes that gap: every dispatched pair is recorded
// before g.Go and the wrapper removes its own entry once it has delivered, so a pair whose fn ran is
// never in the ledger when g.Wait returns. Whatever remains is a pair whose fn never ran; the sweep
// below delivers a cancellation Response for each, so every dispatched pair still yields exactly one
// Response on every path.
func (d *dispatcher[K, V, R]) run(ctx context.Context) {
	defer close(d.done)
	spanner := span.Get(ctx)

	g := d.workers.Group()

	// ledger tracks dispatched pairs whose fn has not yet delivered, keyed by dispatch index and
	// holding the pair's input key for the sweep. One lock + insert per dispatch, one lock + delete per
	// delivery — trivial against the submit floor.
	var mu sync.Mutex
	ledger := map[int]K{}

	i := 0
loop:
	for {
		if err := d.o.wait(ctx); err != nil {
			// The checkpoint fails on cancellation (routine) or an internal invariant breach; only
			// the latter is worth recording.
			if ctx.Err() == nil {
				spanner.Error(err)
			}
			break
		}
		select {
		case <-ctx.Done():
			break loop
		case in, ok := <-d.pull:
			if !ok {
				break loop
			}
			insert := i
			i++
			mu.Lock()
			ledger[insert] = in.k
			mu.Unlock()
			g.Go(ctx, func(ctx context.Context) error {
				err := ctx.Err()
				var r R
				if err == nil {
					r, err = d.call(ctx, in.k, in.v)
				}
				// With WithStopOnErr, cancel the moment the error is known — before delivery. In
				// unordered mode deliver can block on the full out channel behind a slow consumer, and
				// deferring the cancel to the fn's return (Group.CancelOnErr fires only after fn
				// returns) would strand the first errored worker in delivery while the dispatcher kept
				// dispatching, weakening cancel-on-first-error. A dead ctx already implies the cancel,
				// so this only newly fires on an error from call; cancel is idempotent regardless.
				if err != nil && d.o.stopOnErr {
					d.cancel()
				}
				// Remove the entry before delivering so the post-Wait sweep never re-delivers this
				// pair; the delete happens-before this wrapper's wg.Done, which g.Wait observes.
				mu.Lock()
				delete(ledger, insert)
				mu.Unlock()
				d.deliver(insert, in.k, stream.Result[R]{V: r, Err: err})
				return err
			})
		}
	}
	// Errors already reached the consumer in-band as Response.Err; the span records them for
	// observability.
	if err := g.Wait(ctx); err != nil {
		spanner.Error(err)
	}
	// Sweep the ledger: entries left are pairs whose fn never ran (their submit was declined or their
	// Group execution skipped after ctx was cancelled). Deliver a cancellation Response for each so the
	// "every dispatched pair yields exactly one Response" invariant holds. g.Wait has returned, so no
	// wrapper is still touching the ledger; this runs on the dispatch goroutine before finish, so in
	// ordered mode all() yields the swept pairs in index order. The cause is preferred over Err() so a
	// deadline or a WithStopOnErr cancel surfaces its real reason.
	cause := context.Cause(ctx)
	if cause == nil {
		cause = ctx.Err()
	}
	mu.Lock()
	for insert, k := range ledger {
		d.deliver(insert, k, stream.Result[R]{Err: cause})
	}
	clear(ledger)
	mu.Unlock()
	d.finish()
}
