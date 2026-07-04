package foreach

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gostdlib/base/concurrency/background"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/values/generics/promises"
)

// TestCall pins the gate lifecycle around the retry: the gate closes only for retryable failures and
// reopens on every exit path, including fn panicking mid-retry.
func TestCall(t *testing.T) {
	t.Parallel()

	permanent := fmt.Errorf("bad request: %w", ErrPermanent)
	transient := fmt.Errorf("unavailable")

	tests := []struct {
		name string
		// errs are returned by successive fn calls; a call past the end succeeds or panics.
		errs     []error
		fnPanics bool
		wantErr  bool
	}{
		{
			name: "Success: a healthy call never touches the gate",
		},
		{
			name: "Success: a transient error closes the gate and it reopens after recovery",
			errs: []error{transient},
		},
		{
			name:    "Error: a permanent error is returned without another attempt",
			errs:    []error{permanent},
			wantErr: true,
		},
		{
			name:     "Success: the gate reopens even when fn panics mid-retry",
			errs:     []error{transient},
			fnPanics: true,
		},
	}

	for _, test := range tests {
		g := &gate{}
		calls := 0
		d := &dispatcher[int, int, int]{o: options{boff: testBoff(), gate: g}}
		d.fn = func(_ context.Context, _ int, v int) (int, error) {
			n := calls
			calls++
			if n < len(test.errs) {
				return 0, test.errs[n]
			}
			if test.fnPanics {
				panic("boom")
			}
			return v * 2, nil
		}

		var err error
		func() {
			defer func() {
				recover()
			}()
			_, err = d.call(t.Context(), 0, 21)
		}()

		if !test.fnPanics {
			switch {
			case err == nil && test.wantErr:
				t.Errorf("TestCall(%s): got err == nil, want err != nil", test.name)
				continue
			case err != nil && !test.wantErr:
				t.Errorf("TestCall(%s): got err == %s, want err == nil", test.name, err)
				continue
			}
		}

		if !g.open() {
			t.Errorf("TestCall(%s): got a paused gate after call returned, want it open", test.name)
		}
	}
}

// TestLaunchStartupFailure pins launch's startup-failure contract: when the Tasks manager cannot start
// the puller and dispatcher, a non-ctx failure must surface as exactly one in-band Response wrapping
// ErrPermanent — never zero (the old silent-empty gap that looks like empty input) nor two (the
// double-delivery hazard when a Closed manager fails both Once calls) — and the join on d.done must
// still close so the range terminates. Injection passes a closed background.New manager straight to
// launch's tasks parameter; ctx stays alive so the failure is the manager, not the Context (a dead ctx
// suppresses the in-band report on purpose).
func TestLaunchStartupFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ordered bool
	}{
		{name: "Success: unordered startup failure yields exactly one ErrPermanent response"},
		{name: "Success: ordered startup failure yields exactly one ErrPermanent response", ordered: true},
	}

	for _, test := range tests {
		ctx := t.Context()

		// A Closed Tasks manager fails both of launch's Once calls, forcing the startup-failure branch
		// through the injected parameter seam.
		closedTasks := background.New(ctx)
		if err := closedTasks.Close(ctx); err != nil {
			t.Fatalf("TestLaunchStartupFailure(%s): closing the Tasks manager: %v", test.name, err)
		}

		fn := func(_ context.Context, _ int, v int) (int, error) { return v, nil }

		d := &dispatcher[int, int, int]{
			seq:     seqOf(1, 2, 3), // Non-empty: a silent-empty gap is indistinguishable from success on empty input.
			fn:      fn,
			cancel:  func() {},
			pull:    make(chan input[int, int]),
			done:    make(chan struct{}),
			workers: pool(ctx),
		}

		var resps []promises.Response[int]
		if test.ordered {
			ord := newOrder[keyed[int, int]]()
			d.deliver = func(i int, k int, resp promises.Response[int]) { ord.add(i, keyed[int, int]{k: k, resp: resp}) }
			d.finish = ord.finish
			d.launch(ctx, closedTasks)
			<-d.done
			for _, kv := range ord.all(ctx) {
				resps = append(resps, kv.resp)
			}
		} else {
			out := make(chan keyed[int, int], 2) // Cap 2 so a double delivery surfaces as a count, not a deadlock.
			d.deliver = func(_ int, k int, resp promises.Response[int]) { out <- keyed[int, int]{k: k, resp: resp} }
			d.finish = func() { close(out) }
			d.launch(ctx, closedTasks)
			<-d.done
			for kv := range out {
				resps = append(resps, kv.resp)
			}
		}

		switch {
		case len(resps) != 1:
			t.Errorf("TestLaunchStartupFailure(%s): got %d responses, want exactly 1 (0 = silent-empty gap, 2 = double delivery)", test.name, len(resps))
		case resps[0].Err == nil:
			t.Errorf("TestLaunchStartupFailure(%s): got a nil-Err response, want an error wrapping ErrPermanent", test.name)
		case !errors.Is(resps[0].Err, ErrPermanent):
			t.Errorf("TestLaunchStartupFailure(%s): got Err %v, want it to wrap ErrPermanent", test.name, resps[0].Err)
		}
	}
}
