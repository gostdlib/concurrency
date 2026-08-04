package stagedpipe

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// eventually polls cond until it is true or a deadline passes.
func eventually(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// parkedWorkers returns a spawn1 for s. Like pipeline.start it launches a worker and returns at
// once; the worker then parks until it is handed a quit token or the governor stops.
func parkedWorkers[T any](s *scaler[T]) func(quit <-chan struct{}, live *atomic.Int64) {
	return func(quit <-chan struct{}, live *atomic.Int64) {
		go func() {
			defer live.Add(-1)
			select {
			case <-quit:
			case <-s.stop:
			}
		}()
	}
}

func smpl(tsec int, completed, running int64, workers int) sample {
	return sample{
		t:         time.Unix(int64(1000+tsec), 0),
		completed: completed,
		running:   running,
		workers:   workers,
	}
}

// TestAutoScaleDecide exercises the pure control policy. Each case pre-seeds the control state and
// asserts the single worker-count change decide returns for one (prev, cur) transition. With
// ema=baseEMA=100 and a 1s window, a completed delta of 200 is a clear improvement, 100 is a
// plateau, and 0 is a clear regression.
func TestAutoScaleDecide(t *testing.T) {
	t.Parallel()

	// width=3 (a 3-stage pipeline); default bounds 1..8 pipelines => 3..24 workers. Every step is
	// one pipeline, i.e. ±3 workers.
	const w3 = 3

	tests := []struct {
		name       string
		width      int
		minW, maxW int
		seeded     bool
		ema, base  float64
		climbing   bool
		ceiling    int
		holdTicks  int
		sinceProbe int
		prev, cur  sample
		want       int
	}{
		{
			name:  "Success: first call seeds and holds",
			width: w3, minW: 3, maxW: 24, seeded: false,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 100, 9, 9),
			want: 0,
		},
		{
			name:  "Success: zero elapsed time holds",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100,
			prev: smpl(0, 0, 9, 9), cur: smpl(0, 100, 9, 9),
			want: 0,
		},
		{
			name:  "Success: saturated with headroom probes up one pipeline",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 100, 9, 9),
			want: 3,
		},
		{
			name:  "Success: outstanding step that improved keeps climbing one pipeline",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, climbing: true,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 200, 9, 9),
			want: 3,
		},
		{
			name:  "Success: outstanding step that plateaued settles down one pipeline",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, climbing: true,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 100, 9, 9),
			want: -3,
		},
		{
			name:  "Success: outstanding step that regressed settles down one pipeline",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, climbing: true,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 0, 9, 9),
			want: -3,
		},
		{
			name:  "Success: improving climb at max keeps the gain and holds",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, climbing: true,
			prev: smpl(0, 0, 24, 24), cur: smpl(1, 200, 24, 24),
			want: 0,
		},
		{
			name:  "Success: settled at a ceiling holds",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, ceiling: 9,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 100, 9, 9),
			want: 0,
		},
		{
			name:  "Success: re-probe clears the ceiling and probes up",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, ceiling: 9, sinceProbe: asProbeEvery - 1,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 100, 9, 9),
			want: 3,
		},
		{
			name:  "Success: idle releases pipelines toward what is in use",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100,
			prev: smpl(0, 0, 1, 24), cur: smpl(1, 100, 1, 24),
			want: -21,
		},
		{
			name:  "Success: idle never shrinks below min pipelines",
			width: w3, minW: 9, maxW: 24, seeded: true, ema: 100, base: 100,
			prev: smpl(0, 0, 0, 12), cur: smpl(1, 0, 0, 12),
			want: -3,
		},
		{
			name:  "Success: not saturated and not idle holds",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100,
			prev: smpl(0, 0, 6, 9), cur: smpl(1, 100, 6, 9),
			want: 0,
		},
		{
			name:  "Success: hold ticks force a hold",
			width: w3, minW: 3, maxW: 24, seeded: true, ema: 100, base: 100, holdTicks: 2,
			prev: smpl(0, 0, 9, 9), cur: smpl(1, 200, 9, 9),
			want: 0,
		},
	}

	for _, test := range tests {
		c := ctrl{
			width: test.width, minW: test.minW, maxW: test.maxW,
			seeded: test.seeded, ema: test.ema, baseEMA: test.base,
			climbing: test.climbing, ceiling: test.ceiling,
			holdTicks: test.holdTicks, sinceProbe: test.sinceProbe,
		}
		got := c.decide(test.prev, test.cur).delta
		if got != test.want {
			t.Errorf("TestAutoScaleDecide(%s): got delta %d, want %d", test.name, got, test.want)
		}
	}
}

func TestAutoScaleValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     autoScaleCfg
		wantErr bool
	}{
		{name: "Success: valid bounds", cfg: autoScaleCfg{min: 2, max: 64}},
		{name: "Error: min below 1", cfg: autoScaleCfg{min: 0, max: 64}, wantErr: true},
		{name: "Error: max below min", cfg: autoScaleCfg{min: 2, max: 1}, wantErr: true},
	}

	for _, test := range tests {
		err := test.cfg.validate()
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestAutoScaleValidate(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestAutoScaleValidate(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

// TestScalerSpawnRemove verifies the elastic mechanism directly with idle fake workers.
func TestScalerSpawnRemove(t *testing.T) {
	t.Parallel()

	// sendGrace is set high because this test asserts on the live worker count: a worker here is always
	// parked and ready to take a quit token, so a delivery that "times out" would only mean the
	// scheduler was slow, which must not be mistaken for the mechanism failing.
	s := &scaler[int]{quit: make(chan struct{}), stop: make(chan struct{}), sendGrace: 30 * time.Second}
	s.spawn1 = parkedWorkers(s)

	if got := s.spawn(5); got != 5 {
		t.Fatalf("TestScalerSpawnRemove: spawn returned %d, want 5", got)
	}
	if !eventually(t, func() bool { return s.live.Load() == 5 }) {
		t.Fatalf("TestScalerSpawnRemove: live = %d, want 5", s.live.Load())
	}

	if got := s.remove(3); got != 3 {
		t.Fatalf("TestScalerSpawnRemove: remove returned %d, want 3", got)
	}
	if !eventually(t, func() bool { return s.live.Load() == 2 }) {
		t.Fatalf("TestScalerSpawnRemove: after remove(3) live = %d, want 2", s.live.Load())
	}

	// With the governor stopped, remove gives up rather than blocking. The surviving workers are
	// drained first: while one is still parked on quit, remove's send and its stop case are both ready
	// and the select may pick either, so a delivery there would say nothing about whether stop was
	// observed. Once nothing can receive a token, a non-zero return can only mean stop was ignored.
	close(s.stop)
	if !eventually(t, func() bool { return s.live.Load() == 0 }) {
		t.Fatalf("TestScalerSpawnRemove: after close(stop) live = %d, want 0", s.live.Load())
	}
	if got := s.remove(10); got != 0 {
		t.Errorf("TestScalerSpawnRemove: remove after stop returned %d, want 0", got)
	}
}

// fakeClock drives the governor deterministically: the test advances now() and fires ticks by hand.
type fakeClock struct {
	mu  sync.Mutex
	cur time.Time
	c   chan time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur
}

func (f *fakeClock) tick(time.Duration) (<-chan time.Time, func()) {
	return f.c, func() {}
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cur = f.cur.Add(d)
}

// TestGovernorLoop drives the full governor with a fake clock: under sustained saturation with
// rising throughput the worker count grows; when the pool goes idle it shrinks back to min.
func TestGovernorLoop(t *testing.T) {
	t.Parallel()

	fc := &fakeClock{cur: time.Unix(1000, 0), c: make(chan time.Time)}
	st := newStats()
	// width=3 (3 workers per pipeline), 1..8 pipelines => 3..24 workers; start at 2 pipelines.
	const width, minW = 3, 3
	// applied makes each tick observable as complete rather than merely delivered, and sendGrace is
	// set high because the fake workers are always parked: a delivery that lost the race with the
	// default 50ms timer would leave the pool larger than the governor intended, for no reason other
	// than scheduler latency.
	s := &scaler[int]{
		stats: st,
		quit:  make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
		applied: make(chan struct{}), sendGrace: 30 * time.Second,
		size: 2 * width, ctrl: &ctrl{width: width, minW: minW, maxW: 8 * width}, clk: fc,
	}
	s.spawn1 = parkedWorkers(s)
	s.spawn(2 * width)
	go s.loop()
	defer close(s.stop)

	// fire delivers one tick and waits for the governor to finish applying it. Waiting is what makes
	// this test deterministic: the send alone only proves the governor received the tick, so without
	// the handshake the test could race ahead and mutate the clock and stats that the tick it just
	// sent is still about to read. The receive also orders the governor's writes to s.size before the
	// reads below.
	fire := func() {
		fc.advance(asInterval)
		fc.c <- fc.cur
		<-s.applied
	}

	// Saturated + accelerating completions => throughput keeps improving => climb by whole pipelines.
	var completed int64
	fire() // seed
	for i := 1; i <= 12; i++ {
		completed += int64(500 * i)
		st.completed.Store(completed)
		st.running.Store(1000) // always >= size => saturated
		fire()
	}
	if s.size <= 2*width {
		t.Fatalf("TestGovernorLoop: under saturation+improvement size did not grow above %d, got %d", 2*width, s.size)
	}

	// Idle => shrink to min pipelines. Ticks are driven until the governor converges rather than a
	// fixed number of them: the controller's hysteresis (asHoldN) legitimately absorbs several ticks
	// before the shrink, and how many depends on the state the climb left behind. The cap keeps a
	// controller that never converges a bounded failure instead of a hang.
	st.running.Store(0)
	for i := 0; i < 20 && s.size != minW; i++ {
		fire()
	}
	if s.size != minW {
		t.Fatalf("TestGovernorLoop: when idle size did not shrink to min %d, got %d", minW, s.size)
	}
	// The workers themselves exit on their own goroutines after taking the quit token, so live is the
	// one thing here that is legitimately asynchronous and still worth waiting on.
	if !eventually(t, func() bool { return s.live.Load() == int64(minW) }) {
		t.Fatalf("TestGovernorLoop: when idle live did not shrink to min %d, got %d", minW, s.live.Load())
	}
}

// TestGovernorStopWithPendingHandshake is a regression test for the tick-completion handshake: the
// governor must still exit when stop is closed while a handshake send is outstanding. A receiver
// that has stopped receiving — a test that finished with the scaler, or one that failed early and
// returned — must not be able to wedge the governor on that send, because Close waits on the
// governor before it joins the workers, so a wedged loop hangs teardown rather than just leaking.
func TestGovernorStopWithPendingHandshake(t *testing.T) {
	t.Parallel()

	fc := &fakeClock{cur: time.Unix(1000, 0), c: make(chan time.Time)}
	s := &scaler[int]{
		stats: newStats(),
		quit:  make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
		applied: make(chan struct{}),
		size:    3, ctrl: &ctrl{width: 3, minW: 3, maxW: 24}, clk: fc,
	}
	s.spawn1 = parkedWorkers(s)
	go s.loop()

	// One complete tick, handshake included, to establish that the governor is running normally.
	fc.advance(asInterval)
	fc.c <- fc.cur
	<-s.applied

	// A second tick that nobody acknowledges. The governor applies it and then has a handshake send
	// with no receiver.
	fc.advance(asInterval)
	fc.c <- fc.cur

	close(s.stop)
	select {
	case <-s.done:
	case <-time.After(3 * time.Second):
		t.Fatalf("TestGovernorStopWithPendingHandshake: governor did not exit after stop with a handshake outstanding")
	}
}

// TestAutoScalePipeline is an end-to-end check that a WithAutoScale pipeline processes correctly,
// keeps its live worker count within bounds, and releases all workers on Close.
func TestAutoScalePipeline(t *testing.T) {
	t.Parallel()

	p, err := New[int](t.Context(), "autoscale", 2, &statsSM{}, WithAutoScale(2, 16))
	if err != nil {
		t.Fatalf("TestAutoScalePipeline: cannot create pipeline: %s", err)
	}

	rg := p.NewRequestGroup()
	var got int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range rg.Out() {
			atomic.AddInt64(&got, 1)
		}
	}()

	const n = 500
	for i := 0; i < n; i++ {
		if err := rg.Submit(Request[int]{Ctx: t.Context(), Data: i}); err != nil {
			t.Fatalf("TestAutoScalePipeline: problem submitting request: %s", err)
		}
	}

	if l := p.scaler.live.Load(); l < 2 || l > 16 {
		t.Errorf("TestAutoScalePipeline: live = %d, want within [2,16]", l)
	}

	rg.Close()
	<-done
	p.Close()

	if got != n {
		t.Errorf("TestAutoScalePipeline: processed %d requests, want %d", got, n)
	}
	if !eventually(t, func() bool { return p.scaler.live.Load() == 0 }) {
		t.Errorf("TestAutoScalePipeline: workers did not exit after Close, live = %d", p.scaler.live.Load())
	}
}
