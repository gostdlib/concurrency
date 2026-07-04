package foreach

import (
	"fmt"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/kylelemons/godebug/pretty"
)

// TestWithGateRetries pins WithGate's retry contract: transient errors retry until fn succeeds,
// permanent errors are never retried, and exhausted retries deliver the final error in-band.
func TestWithGateRetries(t *testing.T) {
	t.Parallel()

	boff := testBoff()
	policy := exponential.Policy{
		InitialInterval:     time.Millisecond,
		Multiplier:          2,
		RandomizationFactor: 0.5,
		MaxInterval:         10 * time.Millisecond,
		MaxAttempts:         2,
	}
	twoAttempts := exponential.Must(exponential.New(exponential.WithTesting(), exponential.WithPolicy(policy)))

	permanent := fmt.Errorf("bad request: %w", ErrPermanent)
	transient := fmt.Errorf("unavailable")

	tests := []struct {
		name string
		boff *exponential.Backoff
		// errs are returned by successive fn calls for key 1; a call past the end succeeds.
		errs []error
		// wantCalls is the number of fn invocations for key 1.
		wantCalls int64
		// wantErrKey asserts key 1's Response carries an Err.
		wantErrKey bool
	}{
		{
			name:      "Success: a healthy value is called exactly once",
			boff:      boff,
			wantCalls: 1,
		},
		{
			name:      "Success: transient errors retry until fn succeeds",
			boff:      boff,
			errs:      []error{transient, transient},
			wantCalls: 3,
		},
		{
			name:       "Error: an error wrapping ErrPermanent is not retried",
			boff:       boff,
			errs:       []error{permanent},
			wantCalls:  1,
			wantErrKey: true,
		},
		{
			name:       "Error: exhausted retries deliver the final error in-band",
			boff:       twoAttempts,
			errs:       []error{transient, transient, transient, transient},
			wantCalls:  2, // Every attempt runs inside the backoff, so MaxAttempts bounds them all.
			wantErrKey: true,
		},
	}

	for _, test := range tests {
		var calls atomic.Int64
		fn := func(_ context.Context, k int, v int) (int, error) {
			if k != 1 {
				return v * 2, nil
			}
			n := calls.Add(1)
			if int(n) <= len(test.errs) {
				return 0, test.errs[n-1]
			}
			return v * 2, nil
		}

		var errKeys []int
		vals := map[int]int{}
		for k, resp := range Item(t.Context(), seqOf(10, 11, 12), fn, WithGate(test.boff)) {
			if resp.Err != nil {
				errKeys = append(errKeys, k)
				continue
			}
			vals[k] = resp.V
		}

		if n := calls.Load(); n != test.wantCalls {
			t.Errorf("TestWithGateRetries(%s): got %d fn calls for key 1, want %d", test.name, n, test.wantCalls)
		}

		var wantErrKeys []int
		wantVals := map[int]int{0: 20, 1: 22, 2: 24}
		if test.wantErrKey {
			wantErrKeys = []int{1}
			delete(wantVals, 1)
		}
		if diff := pretty.Compare(wantErrKeys, errKeys); diff != "" {
			t.Errorf("TestWithGateRetries(%s): error keys: -want/+got:\n%s", test.name, diff)
		}
		if diff := pretty.Compare(wantVals, vals); diff != "" {
			t.Errorf("TestWithGateRetries(%s): values: -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestWithGatePausesDispatch proves dispatch pauses while a pair is retrying: the sequence withholds
// keys 1..total-1 until key 0's retry has closed the gate, so at most the one pair already past the
// checkpoint may dispatch while the gate is closed and the rest must wait for the retry to resolve.
func TestWithGatePausesDispatch(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	boff := testBoff()

	const total = 50

	gateClosed := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)

	var seq iter.Seq2[int, int] = func(yield func(int, int) bool) {
		if !yield(0, 0) {
			return
		}
		<-gateClosed // Later pairs only become available once the gate is closed.
		for i := 1; i < total; i++ {
			if !yield(i, i) {
				return
			}
		}
	}

	var attempts atomic.Int64
	var started atomic.Int64
	fn := func(ctx context.Context, k int, v int) (int, error) {
		if k != 0 {
			started.Add(1)
			return v, nil
		}
		if attempts.Add(1) == 1 {
			return 0, fmt.Errorf("dependency unavailable")
		}
		// The retry attempt: the gate is closed for as long as this blocks.
		close(gateClosed)
		select {
		case <-release:
			return v, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	var respErrs atomic.Int64
	itemDone := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(itemDone)
		for _, resp := range Item(ctx, seq, fn, WithGate(boff)) {
			if resp.Err != nil {
				respErrs.Add(1)
			}
		}
	})

	<-gateClosed
	// The dispatcher checks the gate before pulling the next pair, so exactly one pair may have
	// passed the checkpoint before the gate closed; everything else must freeze until release.
	time.Sleep(50 * time.Millisecond)
	frozen := started.Load()
	if frozen > 1 {
		t.Fatalf("TestWithGatePausesDispatch: got %d pairs dispatched while the gate was closed, want at most 1", frozen)
	}
	time.Sleep(50 * time.Millisecond)
	if n := started.Load(); n != frozen {
		t.Fatalf("TestWithGatePausesDispatch: got %d new dispatches while the gate stayed closed, want none", n-frozen)
	}

	releaseOnce()
	<-itemDone
	if n := respErrs.Load(); n != 0 {
		t.Fatalf("TestWithGatePausesDispatch: got %d error responses, want none", n)
	}
	if n := started.Load(); n != total-1 {
		t.Errorf("TestWithGatePausesDispatch: got %d pairs dispatched in the end, want %d", n, total-1)
	}
}

// TestWithGateContextCancel proves cancelling the Context ends an Item whose dispatch is parked at a
// closed gate, provided the retrying ItemFunc honors its Context.
func TestWithGateContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	boff := testBoff()

	gateClosed := make(chan struct{})
	closedOnce := sync.OnceFunc(func() { close(gateClosed) })

	var attempts atomic.Int64
	fn := func(ctx context.Context, k int, v int) (int, error) {
		if k != 0 {
			return v, nil
		}
		if attempts.Add(1) == 1 {
			return 0, fmt.Errorf("dependency unavailable")
		}
		closedOnce()
		<-ctx.Done()
		return 0, ctx.Err()
	}

	itemDone := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(itemDone)
		for range Item(ctx, seqOf(ints(50)...), fn, WithGate(boff)) {
		}
	})

	<-gateClosed
	cancel()
	<-itemDone // Reaching here is the assertion: cancellation unwound the retry, the gate and the join.
}
