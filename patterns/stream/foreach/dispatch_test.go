package foreach

import (
	"fmt"
	"testing"

	"github.com/gostdlib/base/context"
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
