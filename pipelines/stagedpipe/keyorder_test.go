package stagedpipe

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/errors"
)

// koData is the Request payload for keyed-ordering tests; Key is its partition key.
type koData struct {
	Key string
	N   int
}

// koSM is a minimal three-stage StateMachine (Start -> A -> B) for exercising keyed-ordering
// configuration. The stage bodies do nothing; the tests care only about how the stages are marked.
type koSM struct{}

func (s *koSM) Start(r Request[koData]) Request[koData] {
	r.Next = s.A
	return r
}

func (s *koSM) A(r Request[koData]) Request[koData] {
	r.Next = s.B
	return r
}

func (s *koSM) B(r Request[koData]) Request[koData] {
	r.Next = nil
	return r
}

func (s *koSM) Close() {}

func koKey(d koData) string { return d.Key }

// TestNewKeyed pins the keyed-ordering configuration resolved by New: WithKey and WithOrderedStages
// must appear together, ordered stages resolve to a deduplicated barrier set, and WithStopKeyOnErr
// is recorded. The success cases sit alongside the error cases so the test pins both that a good
// configuration builds a keyOrder and that each bad one is rejected.
func TestNewKeyed(t *testing.T) {
	t.Parallel()

	sm := &koSM{}

	tests := []struct {
		name    string
		options []Option
		wantErr bool
		// wantKeyOrder asserts whether New built a keyOrder (nil means keyed ordering is off).
		wantKeyOrder bool
		// wantBarriers is the expected number of distinct ordered stages.
		wantBarriers int
		// wantStopKeyOnErr asserts the poison-on-error flag carried onto keyOrder.
		wantStopKeyOnErr bool
		// wantAdmitDepth asserts the admission depth carried onto keyOrder.
		wantAdmitDepth int
	}{
		{
			name:         "Success: no keyed-ordering options leaves keyOrder nil",
			options:      nil,
			wantKeyOrder: false,
		},
		{
			name:           "Success: WithKey and WithOrderedStages together build a keyOrder with the default depth",
			options:        []Option{WithKey(koKey), WithOrderedStages(sm.A, sm.B)},
			wantKeyOrder:   true,
			wantBarriers:   2,
			wantAdmitDepth: defaultAdmitDepth,
		},
		{
			name:           "Success: duplicate ordered stages dedupe to one barrier",
			options:        []Option{WithKey(koKey), WithOrderedStages(sm.A, sm.A)},
			wantKeyOrder:   true,
			wantBarriers:   1,
			wantAdmitDepth: defaultAdmitDepth,
		},
		{
			name:             "Success: WithStopKeyOnErr is recorded on the keyOrder",
			options:          []Option{WithKey(koKey), WithOrderedStages(sm.A), WithStopKeyOnErr()},
			wantKeyOrder:     true,
			wantBarriers:     1,
			wantStopKeyOnErr: true,
			wantAdmitDepth:   defaultAdmitDepth,
		},
		{
			name:           "Success: WithAdmissionDepth is recorded on the keyOrder",
			options:        []Option{WithKey(koKey), WithOrderedStages(sm.A), WithAdmissionDepth(3)},
			wantKeyOrder:   true,
			wantBarriers:   1,
			wantAdmitDepth: 3,
		},
		{
			name:           "Success: WithAdmissionDepth zero is unbounded",
			options:        []Option{WithKey(koKey), WithOrderedStages(sm.A), WithAdmissionDepth(0)},
			wantKeyOrder:   true,
			wantBarriers:   1,
			wantAdmitDepth: 0,
		},
		{
			name:    "Error: WithKey without WithOrderedStages",
			options: []Option{WithKey(koKey)},
			wantErr: true,
		},
		{
			name:    "Error: WithAdmissionDepth without WithKey",
			options: []Option{WithAdmissionDepth(2)},
			wantErr: true,
		},
		{
			name:    "Error: WithAdmissionDepth negative",
			options: []Option{WithKey(koKey), WithOrderedStages(sm.A), WithAdmissionDepth(-1)},
			wantErr: true,
		},
		{
			name:    "Error: WithOrderedStages without WithKey",
			options: []Option{WithOrderedStages(sm.A)},
			wantErr: true,
		},
		{
			name:    "Error: WithOrderedStages with no stages",
			options: []Option{WithKey(koKey), WithOrderedStages[koData]()},
			wantErr: true,
		},
		{
			name:    "Error: WithKey with a nil keyFunc",
			options: []Option{WithKey[koData](nil), WithOrderedStages(sm.A)},
			wantErr: true,
		},
	}

	for _, test := range tests {
		p, err := New[koData]("test", 1, sm, test.options...)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestNewKeyed(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestNewKeyed(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		if gotKeyOrder := p.keyOrder != nil; gotKeyOrder != test.wantKeyOrder {
			t.Errorf("TestNewKeyed(%s): got (keyOrder != nil) == %v, want %v", test.name, gotKeyOrder, test.wantKeyOrder)
		}
		if test.wantKeyOrder && p.keyOrder != nil {
			if got := len(p.keyOrder.barrierIdx); got != test.wantBarriers {
				t.Errorf("TestNewKeyed(%s): got %d barriers, want %d", test.name, got, test.wantBarriers)
			}
			if got := p.keyOrder.stopKeyOnErr; got != test.wantStopKeyOnErr {
				t.Errorf("TestNewKeyed(%s): stopKeyOnErr == %v, want %v", test.name, got, test.wantStopKeyOnErr)
			}
			if got := p.keyOrder.admitDepth; got != test.wantAdmitDepth {
				t.Errorf("TestNewKeyed(%s): admitDepth == %d, want %d", test.name, got, test.wantAdmitDepth)
			}
		}
		p.Close()
	}
}

// ordData is a keyed payload: Key is the partition key, N the 0-based submit index within that key.
type ordData struct {
	Key string
	N   int
}

func ordKey(d ordData) string { return d.Key }

// ordSM has one ordered stage, Work, that sleeps longer for earlier sequences. Without the barrier a
// later, faster Request would overtake an earlier, slower one for the same key; with it, each key's
// Requests reach Work in submit order. Work records the order it observed per key.
type ordSM struct {
	perKey int
	unit   time.Duration

	mu       sync.Mutex
	recorded map[string][]int
}

func (s *ordSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Work
	return r
}

func (s *ordSM) Work(r Request[ordData]) Request[ordData] {
	// Earlier sequences sleep longer, so completion order would reverse submit order without the
	// barrier serializing Work per key.
	time.Sleep(time.Duration(s.perKey-r.Data.N) * s.unit)
	s.mu.Lock()
	s.recorded[r.Data.Key] = append(s.recorded[r.Data.Key], r.Data.N)
	s.mu.Unlock()
	r.Next = nil
	return r
}

func (s *ordSM) Close() {}

// TestKeyedOrdering verifies that Requests sharing a key reach an ordered stage in submit order even
// when work-stealing across many pipelines and adversarial per-Request delays would otherwise let
// later Requests overtake earlier ones. Different keys run concurrently.
func TestKeyedOrdering(t *testing.T) {
	t.Parallel()

	const perKey = 12
	keys := []string{"a", "b", "c", "d"}

	sm := &ordSM{perKey: perKey, unit: 500 * time.Microsecond, recorded: map[string][]int{}}

	p, err := New[ordData]("keyed", 6, sm, WithKey(ordKey), WithOrderedStages(sm.Work))
	if err != nil {
		t.Fatalf("TestKeyedOrdering: New: %s", err)
	}

	rg := p.NewRequestGroup()
	done := make(chan struct{})
	context.Pool(t.Context()).Submit(t.Context(), func() {
		defer close(done)
		for range rg.Out() {
		}
	})

	ctx := t.Context()
	for n := 0; n < perKey; n++ {
		for _, k := range keys {
			if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
				t.Fatalf("TestKeyedOrdering: Submit: %s", err)
			}
		}
	}

	rg.Close()
	<-done
	p.Close()

	for _, k := range keys {
		got := sm.recorded[k]
		if len(got) != perKey {
			t.Fatalf("TestKeyedOrdering(key %s): got %d observations, want %d", k, len(got), perKey)
		}
		for i, n := range got {
			if n != i {
				t.Fatalf("TestKeyedOrdering(key %s): observation %d == %d, want %d (out of order): %v", k, i, n, i, got)
			}
		}
	}
}

// TestKeyedReap verifies that a key's state is reclaimed once its last in-flight Request exits, so a
// stream of transient keys does not leak keyState entries.
func TestKeyedReap(t *testing.T) {
	t.Parallel()

	sm := &ordSM{perKey: 1, unit: 0, recorded: map[string][]int{}}

	p, err := New[ordData]("reap", 4, sm, WithKey(ordKey), WithOrderedStages(sm.Work))
	if err != nil {
		t.Fatalf("TestKeyedReap: New: %s", err)
	}

	rg := p.NewRequestGroup()
	done := make(chan struct{})
	context.Pool(t.Context()).Submit(t.Context(), func() {
		defer close(done)
		for range rg.Out() {
		}
	})

	ctx := t.Context()
	// Every Request has a distinct key, so the registry would grow unbounded without reaping.
	for n := 0; n < 200; n++ {
		key := string(rune('a'+n%26)) + string(rune('0'+n/26))
		if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: key, N: 0}}); err != nil {
			t.Fatalf("TestKeyedReap: Submit: %s", err)
		}
	}

	rg.Close()
	<-done
	p.Close()

	p.keyOrder.mu.Lock()
	n := len(p.keyOrder.keys)
	p.keyOrder.mu.Unlock()
	if n != 0 {
		t.Fatalf("TestKeyedReap: got %d live keyStates after all Requests drained, want 0", n)
	}
}

// branchSM routes even-N Requests through the ordered stage Work and odd-N Requests through Skip,
// which never reaches Work. It is the divergent-path case: an odd Request must still clear Work's
// barrier when it exits, or the next even Request waits forever for a turn that never comes.
type branchSM struct {
	perKey int
	unit   time.Duration

	mu        sync.Mutex
	workOrder map[string][]int
}

func (s *branchSM) Start(r Request[ordData]) Request[ordData] {
	if r.Data.N%2 == 0 {
		r.Next = s.Work
		return r
	}
	r.Next = s.Skip
	return r
}

func (s *branchSM) Work(r Request[ordData]) Request[ordData] {
	time.Sleep(time.Duration(s.perKey-r.Data.N) * s.unit)
	s.mu.Lock()
	s.workOrder[r.Data.Key] = append(s.workOrder[r.Data.Key], r.Data.N)
	s.mu.Unlock()
	r.Next = nil
	return r
}

func (s *branchSM) Skip(r Request[ordData]) Request[ordData] {
	r.Next = nil
	return r
}

func (s *branchSM) Close() {}

// TestKeyedDivergentPaths verifies the exit-clear protocol on a non-linear pipeline: Requests that
// branch past the ordered stage still release its barrier at exit, so the Requests that do reach it
// stay ordered and nothing stalls. A broken exit-clear would deadlock this test.
func TestKeyedDivergentPaths(t *testing.T) {
	t.Parallel()

	const perKey = 16
	keys := []string{"a", "b", "c"}

	sm := &branchSM{perKey: perKey, unit: 400 * time.Microsecond, workOrder: map[string][]int{}}

	p, err := New[ordData]("branch", 6, sm, WithKey(ordKey), WithOrderedStages(sm.Work))
	if err != nil {
		t.Fatalf("TestKeyedDivergentPaths: New: %s", err)
	}

	rg := p.NewRequestGroup()
	got := make(chan int, 1)
	context.Pool(t.Context()).Submit(t.Context(), func() {
		count := 0
		for range rg.Out() {
			count++
		}
		got <- count
	})

	ctx := t.Context()
	for n := 0; n < perKey; n++ {
		for _, k := range keys {
			if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
				t.Fatalf("TestKeyedDivergentPaths: Submit: %s", err)
			}
		}
	}

	rg.Close()
	count := <-got
	p.Close()

	if want := perKey * len(keys); count != want {
		t.Fatalf("TestKeyedDivergentPaths: got %d completed Requests, want %d (a stall would lose some)", count, want)
	}

	// The even Requests are the only ones that reach Work, and they must have reached it in order.
	for _, k := range keys {
		order := sm.workOrder[k]
		prev := -1
		for _, n := range order {
			if n <= prev {
				t.Fatalf("TestKeyedDivergentPaths(%s): Work saw %d after %d, want increasing: %v", k, n, prev, order)
			}
			prev = n
		}
	}
}

// panicSM panics in its ordered stage Work for one target (key, N) and passes everything else
// through. It drives the panic-teardown tests both keyed (WithOrderedStages(sm.Work)) and not.
type panicSM struct {
	panicKey string
	panicN   int
}

func (s *panicSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Work
	return r
}

func (s *panicSM) Work(r Request[ordData]) Request[ordData] {
	if r.Data.Key == s.panicKey && r.Data.N == s.panicN {
		panic(fmt.Sprintf("boom at %s/%d", r.Data.Key, r.Data.N))
	}
	r.Next = nil
	return r
}

func (s *panicSM) Close() {}

// TestPanicTeardown verifies stage-panic teardown on both a non-keyed and a keyed pipeline: the
// panic is recovered so the process does not crash, every Request drains with ErrTornDown, parked
// barrier waiters (keyed case) are woken so nothing stalls, and the original panic re-raises from
// RequestGroup.Close as a PanicError carrying the panic value and the stack captured at the site.
func TestPanicTeardown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// num is the pipeline count; keyed adds WithKey + WithOrderedStages(sm.Work).
		num   int
		keyed bool
		// panicKey/panicN select which (key, N) panics inside Work.
		panicKey string
		panicN   int
		// keys x perKey is the submit grid.
		keys   []string
		perKey int
		// wantValue is the expected PanicError.Value; wantStack asserts a non-empty captured stack.
		wantValue string
		wantStack bool
	}{
		{
			name:      "Success: non-keyed stage panic re-raises PanicError with value and stack",
			num:       4,
			panicKey:  "x",
			panicN:    3,
			keys:      []string{"x"},
			perKey:    20,
			wantValue: "boom at x/3",
			wantStack: true,
		},
		{
			name:      "Success: keyed panic on a key's leader wakes parked barrier waiters and re-raises",
			num:       6,
			keyed:     true,
			panicKey:  "a",
			panicN:    0,
			keys:      []string{"a", "b", "c"},
			perKey:    8,
			wantValue: "boom at a/0",
		},
	}

	for _, test := range tests {
		sm := &panicSM{panicKey: test.panicKey, panicN: test.panicN}
		var opts []Option
		if test.keyed {
			opts = []Option{WithKey(ordKey), WithOrderedStages(sm.Work)}
		}
		p, err := New[ordData]("panic", test.num, sm, opts...)
		if err != nil {
			t.Fatalf("TestPanicTeardown(%s): New: %s", test.name, err)
		}

		rg := p.NewRequestGroup()
		done := make(chan struct{})
		context.Pool(t.Context()).Submit(t.Context(), func() {
			defer close(done)
			for range rg.Out() {
			}
		})

		ctx := t.Context()
		for n := 0; n < test.perKey; n++ {
			for _, k := range test.keys {
				if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
					t.Fatalf("TestPanicTeardown(%s): Submit: %s", test.name, err)
				}
			}
		}

		// rg.Close re-raises the panic on this goroutine; recover it to assert the surfaced value.
		var got any
		func() {
			defer func() { got = recover() }()
			rg.Close()
		}()
		<-done
		p.Close()

		pe, ok := got.(PanicError)
		if !ok {
			t.Fatalf("TestPanicTeardown(%s): recovered %T, want PanicError (a stall would hang instead)", test.name, got)
		}
		if s, _ := pe.Value.(string); s != test.wantValue {
			t.Fatalf("TestPanicTeardown(%s): PanicError.Value == %v, want %q", test.name, pe.Value, test.wantValue)
		}
		if test.wantStack && len(pe.Stack) == 0 {
			t.Fatalf("TestPanicTeardown(%s): PanicError.Stack is empty, want the captured origin stack", test.name)
		}
	}
}

// errSM fails one target Request in its ordered stage Work. The target is the key's leader (errN 0)
// and blocks on release until the test closes it, so every later Request for that key is already in
// flight — parked behind it at the barrier — when it fails. That keeps the whole cohort alive
// through the failure, so the cohort-scoped poison is observed deterministically rather than being
// reset by a mid-cohort reap. It drives the WithStopKeyOnErr test.
type errSM struct {
	errKey  string
	errN    int
	release chan struct{}
}

func (s *errSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Work
	return r
}

func (s *errSM) Work(r Request[ordData]) Request[ordData] {
	if r.Data.Key == s.errKey && r.Data.N == s.errN {
		<-s.release
		r.Err = fmt.Errorf("work failed at %s/%d", r.Data.Key, r.Data.N)
		return r
	}
	r.Next = nil
	return r
}

func (s *errSM) Close() {}

// TestStopKeyOnErr verifies WithStopKeyOnErr: once a Request for a key fails, that key's later
// Requests in the same in-flight cohort are skipped with an error while every other key completes
// normally. The failing Request is key "a"'s leader; it blocks until every later "a" Request is in
// flight (parked at the barrier), then fails, so the poison is observed deterministically.
func TestStopKeyOnErr(t *testing.T) {
	t.Parallel()

	const perKey = 6
	keys := []string{"a", "b"}

	sm := &errSM{errKey: "a", errN: 0, release: make(chan struct{})}
	// num exceeds the total Request count so every Request is pulled into a worker at once, and
	// WithAdmissionDepth(0) keeps the whole cohort admissible — a's leader blocks in Work while a/1..
	// park behind it and b/* complete — so no Submit blocks and the cohort stays in flight until release.
	p, err := New[ordData]("stopkey", 16, sm, WithKey(ordKey), WithOrderedStages(sm.Work), WithStopKeyOnErr(), WithAdmissionDepth(0))
	if err != nil {
		t.Fatalf("TestStopKeyOnErr: New: %s", err)
	}

	results := map[string][]error{}
	for _, k := range keys {
		results[k] = make([]error, perKey)
	}
	var mu sync.Mutex
	emitted := 0

	rg := p.NewRequestGroup()
	done := make(chan struct{})
	context.Pool(t.Context()).Submit(t.Context(), func() {
		defer close(done)
		for out := range rg.Out() {
			mu.Lock()
			results[out.Data.Key][out.Data.N] = out.Err
			emitted++
			mu.Unlock()
		}
	})

	ctx := t.Context()
	for n := 0; n < perKey; n++ {
		for _, k := range keys {
			if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
				t.Fatalf("TestStopKeyOnErr: Submit: %s", err)
			}
		}
	}

	// Every "a" Request is now in flight behind the blocked leader; let the leader fail.
	close(sm.release)

	rg.Close()
	<-done
	p.Close()

	// Every submitted Request must come out, so a nil result means "completed", not "dropped".
	if want := len(keys) * perKey; emitted != want {
		t.Fatalf("TestStopKeyOnErr: %d Requests emitted, want %d (some were dropped)", emitted, want)
	}

	// Key "a": the leader carries its own failure; every later Request is skipped with ErrKeyFailed.
	if err := results["a"][0]; err == nil || errors.Is(err, ErrKeyFailed) {
		t.Errorf("TestStopKeyOnErr: a/0 got err == %v, want the leader's own non-ErrKeyFailed failure", err)
	}
	for n := 1; n < perKey; n++ {
		if err := results["a"][n]; !errors.Is(err, ErrKeyFailed) {
			t.Errorf("TestStopKeyOnErr: a/%d got err == %v, want errors.Is(err, ErrKeyFailed)", n, err)
		}
	}
	// Key "b" is independent and must be untouched.
	for n := 0; n < perKey; n++ {
		if results["b"][n] != nil {
			t.Errorf("TestStopKeyOnErr: b/%d got err == %v, want nil (other key unaffected)", n, results["b"][n])
		}
	}
}

// admitSM measures, per key, the peak number of Requests concurrently inside its non-ordered stage
// Fanout. Commit is an ordered barrier. With WithAdmissionDepth(d) at most d Requests for a key are
// in flight, so at most d are ever in Fanout at once; unbounded, many more can be.
type admitSM struct {
	unit time.Duration

	mu  sync.Mutex
	cur map[string]int
	max map[string]int
}

func (s *admitSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Fanout
	return r
}

func (s *admitSM) Fanout(r Request[ordData]) Request[ordData] {
	s.mu.Lock()
	s.cur[r.Data.Key]++
	if s.cur[r.Data.Key] > s.max[r.Data.Key] {
		s.max[r.Data.Key] = s.cur[r.Data.Key]
	}
	s.mu.Unlock()

	time.Sleep(s.unit)

	s.mu.Lock()
	s.cur[r.Data.Key]--
	s.mu.Unlock()

	r.Next = s.Commit
	return r
}

func (s *admitSM) Commit(r Request[ordData]) Request[ordData] {
	r.Next = nil
	return r
}

func (s *admitSM) Close() {}

// TestAdmissionDepth verifies WithAdmissionDepth caps the Requests in flight per key: at most depth
// of a key's Requests are ever concurrent in the non-ordered Fanout stage. The unbounded run shows
// the same workload exceeds that cap, so the bounded assertion is not vacuous.
func TestAdmissionDepth(t *testing.T) {
	t.Parallel()

	const perKey = 30
	const depth = 2
	keys := []string{"a", "b", "c"}

	run := func(opts ...Option) map[string]int {
		// A generous per-stage dwell keeps same-key Requests overlapping in Fanout, so the unbounded
		// run reliably exceeds depth even under a loaded machine; the bounded run's <= depth check is
		// a hard invariant regardless of timing.
		sm := &admitSM{unit: 10 * time.Millisecond, cur: map[string]int{}, max: map[string]int{}}
		options := append([]Option{WithKey(ordKey), WithOrderedStages(sm.Commit)}, opts...)
		p, err := New[ordData]("admit", 16, sm, options...)
		if err != nil {
			t.Fatalf("TestAdmissionDepth: New: %s", err)
		}

		rg := p.NewRequestGroup()
		done := make(chan struct{})
		context.Pool(t.Context()).Submit(t.Context(), func() {
			defer close(done)
			for range rg.Out() {
			}
		})

		ctx := t.Context()
		for n := 0; n < perKey; n++ {
			for _, k := range keys {
				if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
					t.Fatalf("TestAdmissionDepth: Submit: %s", err)
				}
			}
		}

		rg.Close()
		<-done
		p.Close()
		return sm.max
	}

	bounded := run(WithAdmissionDepth(depth))
	for _, k := range keys {
		if bounded[k] > depth {
			t.Errorf("TestAdmissionDepth: key %s peaked at %d concurrent in Fanout, want <= %d", k, bounded[k], depth)
		}
	}

	// Without the cap the same workload must exceed depth for at least one key, or the check above is
	// vacuous (16 workers over 3 keys allow far more than depth concurrent).
	unbounded := run(WithAdmissionDepth(0))
	over := false
	for _, k := range keys {
		if unbounded[k] > depth {
			over = true
		}
	}
	if !over {
		t.Errorf("TestAdmissionDepth: unbounded run never exceeded %d concurrent in Fanout; assertion is vacuous", depth)
	}
}

// gateSM blocks the leader (N==0) of one key in its ordered stage Work until release is closed, so
// that key's later Requests pile up behind it — parked at the barrier (unbounded) or waiting at
// Submit for an admission slot (WithAdmissionDepth). It drives the metrics test.
type gateSM struct {
	gateKey string
	release chan struct{}
}

func (s *gateSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Work
	return r
}

func (s *gateSM) Work(r Request[ordData]) Request[ordData] {
	if r.Data.Key == s.gateKey && r.Data.N == 0 {
		<-s.release
	}
	r.Next = nil
	return r
}

func (s *gateSM) Close() {}

// awaitKeyed polls p.Stats().Keyed until pred holds, failing after a generous deadline. It is how the
// metrics test observes a live gauge while Requests are held blocked.
func awaitKeyed(t *testing.T, p *Pipelines[ordData], pred func(KeyedStats) bool, desc string) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		if pred(p.Stats().Keyed) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("TestKeyedMetrics: timed out waiting for %s; last = %+v", desc, p.Stats().Keyed)
}

// TestKeyedMetrics verifies the KeyedStats gauges move while Requests are blocked and reset once they
// drain, and that barrier wait time accumulates. Part A holds a key's leader so its successors park
// at the barrier (ParkedWorkers); Part B does the same with WithAdmissionDepth(1) so its successors
// wait at Submit for a slot (AdmissionWaiters).
func TestKeyedMetrics(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Part A: parked workers + barrier wait. WithAdmissionDepth(0) so the held leader's successors all
	// park at the barrier (rather than some waiting at Submit for a slot) and inline Submit never blocks.
	smA := &gateSM{gateKey: "a", release: make(chan struct{})}
	pA, err := New[ordData]("metrics-parked", 16, smA, WithKey(ordKey), WithOrderedStages(smA.Work), WithAdmissionDepth(0))
	if err != nil {
		t.Fatalf("TestKeyedMetrics: New (part A): %s", err)
	}
	rgA := pA.NewRequestGroup()
	doneA := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(doneA)
		for range rgA.Out() {
		}
	})
	for n := 0; n < 6; n++ {
		for _, k := range []string{"a", "b"} {
			if err := rgA.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
				t.Fatalf("TestKeyedMetrics: Submit (part A): %s", err)
			}
		}
	}
	// a/0 is blocked in Work; a/1.. park at the barrier behind it.
	awaitKeyed(t, pA, func(k KeyedStats) bool { return k.ParkedWorkers > 0 }, "ParkedWorkers > 0")
	close(smA.release)
	rgA.Close()
	<-doneA
	pA.Close()

	if got := pA.Stats().Keyed; got.ParkedWorkers != 0 {
		t.Errorf("TestKeyedMetrics: after drain ParkedWorkers == %d, want 0", got.ParkedWorkers)
	}
	if got := pA.Stats().Keyed; got.BarrierWaitMax <= 0 {
		t.Errorf("TestKeyedMetrics: BarrierWaitMax == %v, want > 0 (successors waited at the barrier)", got.BarrierWaitMax)
	}

	// Part B: admission waiters, depth 1. Later Requests block at Submit, so submit from a goroutine.
	smB := &gateSM{gateKey: "a", release: make(chan struct{})}
	pB, err := New[ordData]("metrics-admit", 16, smB, WithKey(ordKey), WithOrderedStages(smB.Work), WithAdmissionDepth(1))
	if err != nil {
		t.Fatalf("TestKeyedMetrics: New (part B): %s", err)
	}
	rgB := pB.NewRequestGroup()
	doneB := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(doneB)
		for range rgB.Out() {
		}
	})
	subDone := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(subDone)
		for n := 0; n < 6; n++ {
			if err := rgB.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: "a", N: n}}); err != nil {
				return
			}
		}
	})
	// a/0 holds the only slot (blocked in Work); a/1's Submit waits for admission.
	awaitKeyed(t, pB, func(k KeyedStats) bool { return k.AdmissionWaiters > 0 }, "AdmissionWaiters > 0")
	close(smB.release)
	<-subDone
	rgB.Close()
	<-doneB
	pB.Close()

	if got := pB.Stats().Keyed; got.AdmissionWaiters != 0 {
		t.Errorf("TestKeyedMetrics: after drain AdmissionWaiters == %d, want 0", got.AdmissionWaiters)
	}
}

// TestKeyedOrderingAutoScale verifies that per-key ordering holds while the pipeline's worker count
// changes mid-stream. Ordering is by sequence, not by which worker runs a Request, so adding or
// removing workers must not reorder a key. A live autoscale governor runs, and the test also forces
// deterministic scale-ups (p.scaler.spawn) while a keyed backlog with adversarial delays drains.
func TestKeyedOrderingAutoScale(t *testing.T) {
	t.Parallel()

	const perKey = 25
	keys := []string{"a", "b", "c"}

	sm := &ordSM{perKey: perKey, unit: 300 * time.Microsecond, recorded: map[string][]int{}}
	p, err := New[ordData]("autoscale", 2, sm, WithKey(ordKey), WithOrderedStages(sm.Work), WithAutoScale(1, 6))
	if err != nil {
		t.Fatalf("TestKeyedOrderingAutoScale: New: %s", err)
	}
	if p.scaler == nil {
		t.Fatalf("TestKeyedOrderingAutoScale: scaler is nil, want autoscaling enabled")
	}

	ctx := t.Context()
	rg := p.NewRequestGroup()
	done := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(done)
		for range rg.Out() {
		}
	})

	// Submit the whole backlog from a goroutine so the main goroutine can force scale-ups while it
	// processes.
	subDone := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(subDone)
		for n := 0; n < perKey; n++ {
			for _, k := range keys {
				if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
					return
				}
			}
		}
	})

	// Add workers mid-stream: spawn is only ever additive, so it cannot starve the pool. New workers
	// pull from the shared input and must honor the same barriers as the originals.
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Millisecond)
		p.scaler.spawn(2)
	}

	<-subDone
	rg.Close()
	<-done
	p.Close()

	for _, k := range keys {
		got := sm.recorded[k]
		if len(got) != perKey {
			t.Fatalf("TestKeyedOrderingAutoScale(key %s): got %d observations, want %d", k, len(got), perKey)
		}
		for i, n := range got {
			if n != i {
				t.Fatalf("TestKeyedOrderingAutoScale(%s): observation %d == %d, want %d (reordered): %v", k, i, n, i, got)
			}
		}
	}
}

// visitKey identifies a Request by (key, N) so loopSM can tell a stage's first visit from its second.
type visitKey struct {
	k string
	n int
}

// loopSM has an ordered stage Work that each Request visits twice (Work loops back to itself once,
// no WithDAG). The barrier must gate only the first visit — the second must pass through, since the
// Request already cleared it. If re-entry were gated, the second Wait would block on a turn that has
// already advanced and panic. Work records the order of first visits per key.
type loopSM struct {
	unit time.Duration

	mu      sync.Mutex
	visited map[visitKey]bool
	order   map[string][]int
}

func (s *loopSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Work
	return r
}

func (s *loopSM) Work(r Request[ordData]) Request[ordData] {
	id := visitKey{k: r.Data.Key, n: r.Data.N}
	s.mu.Lock()
	first := !s.visited[id]
	if first {
		s.visited[id] = true
		s.order[r.Data.Key] = append(s.order[r.Data.Key], r.Data.N)
	}
	s.mu.Unlock()

	if first {
		time.Sleep(s.unit)
		r.Next = s.Work // loop back to Work for a second, ungated visit
		return r
	}
	r.Next = nil
	return r
}

func (s *loopSM) Close() {}

// TestKeyedRepeatedStage verifies the at-most-once barrier rule: a Request that re-enters an ordered
// stage (a loop, no WithDAG) is gated only on its first visit; the second passes through. A broken
// guard would panic on the second visit (a Wait for a turn that already advanced). Ordering of first
// visits still holds per key.
func TestKeyedRepeatedStage(t *testing.T) {
	t.Parallel()

	const perKey = 15
	keys := []string{"a", "b"}

	sm := &loopSM{unit: 300 * time.Microsecond, visited: map[visitKey]bool{}, order: map[string][]int{}}
	p, err := New[ordData]("loop", 6, sm, WithKey(ordKey), WithOrderedStages(sm.Work))
	if err != nil {
		t.Fatalf("TestKeyedRepeatedStage: New: %s", err)
	}

	ctx := t.Context()
	rg := p.NewRequestGroup()
	done := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(done)
		for range rg.Out() {
		}
	})

	for n := 0; n < perKey; n++ {
		for _, k := range keys {
			if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: k, N: n}}); err != nil {
				t.Fatalf("TestKeyedRepeatedStage: Submit: %s", err)
			}
		}
	}

	rg.Close()
	<-done
	p.Close()

	for _, k := range keys {
		got := sm.order[k]
		if len(got) != perKey {
			t.Fatalf("TestKeyedRepeatedStage(key %s): got %d first visits, want %d", k, len(got), perKey)
		}
		for i, n := range got {
			if n != i {
				t.Fatalf("TestKeyedRepeatedStage(%s): first visit %d == %d, want %d (out of order): %v", k, i, n, i, got)
			}
		}
	}
}

// benchSM has one ordered stage Work that simulates a fixed unit of work. Under a skewed key
// distribution a hot key without an admission cap parks many workers at Work's barrier, starving
// cold keys; WithAdmissionDepth bounds that. It drives BenchmarkKeyedAdmissionDepth.
type benchSM struct {
	unit time.Duration
}

func (s *benchSM) Start(r Request[ordData]) Request[ordData] {
	r.Next = s.Work
	return r
}

func (s *benchSM) Work(r Request[ordData]) Request[ordData] {
	time.Sleep(s.unit)
	r.Next = nil
	return r
}

func (s *benchSM) Close() {}

// BenchmarkKeyedAdmissionDepth measures keyed-ordering throughput across admission depths (1, 2, 4,
// and unbounded) under a uniform and a Zipfian (skewed) key distribution. It informs the default
// admission depth: under skew a small depth should keep a hot key from monopolizing the pool.
func BenchmarkKeyedAdmissionDepth(b *testing.B) {
	const numKeys = 64
	depths := []int{1, 2, 4, 0} // 0 == unbounded

	dists := []struct {
		name string
		keys func(n int) []string
	}{
		{
			name: "uniform",
			keys: func(n int) []string {
				out := make([]string, n)
				for i := range out {
					out[i] = fmt.Sprintf("k%d", i%numKeys)
				}
				return out
			},
		},
		{
			name: "zipfian",
			keys: func(n int) []string {
				r := rand.New(rand.NewSource(1))
				z := rand.NewZipf(r, 1.2, 1, numKeys-1)
				out := make([]string, n)
				for i := range out {
					out[i] = fmt.Sprintf("k%d", z.Uint64())
				}
				return out
			},
		},
	}

	for _, dist := range dists {
		for _, depth := range depths {
			label := "unbounded"
			if depth > 0 {
				label = fmt.Sprintf("%d", depth)
			}
			b.Run(fmt.Sprintf("%s/depth=%s", dist.name, label), func(b *testing.B) {
				keyList := dist.keys(b.N)
				sm := &benchSM{unit: 25 * time.Microsecond}
				// depth 0 means unbounded, which WithAdmissionDepth(0) selects explicitly (New would
				// otherwise apply the finite default).
				opts := []Option{WithKey(ordKey), WithOrderedStages(sm.Work), WithAdmissionDepth(depth)}
				p, err := New[ordData]("bench", 8, sm, opts...)
				if err != nil {
					b.Fatalf("BenchmarkKeyedAdmissionDepth: New: %s", err)
				}
				ctx := b.Context()
				rg := p.NewRequestGroup()
				done := make(chan struct{})
				context.Pool(ctx).Submit(ctx, func() {
					defer close(done)
					for range rg.Out() {
					}
				})

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := rg.Submit(Request[ordData]{Ctx: ctx, Data: ordData{Key: keyList[i]}}); err != nil {
						b.Fatalf("BenchmarkKeyedAdmissionDepth: Submit: %s", err)
					}
				}
				rg.Close()
				<-done
				b.StopTimer()
				p.Close()
			})
		}
	}
}
