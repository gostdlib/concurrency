package stagedpipe

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Autoscale tuning constants. These are intentionally not caller-tunable: WithAutoScale only
// exposes the [min, max] bounds; the control cadence and gains are fixed here.
const (
	// asInterval is how often the governor samples throughput and adjusts the worker count.
	asInterval = 500 * time.Millisecond
	// asEMAAlpha is the smoothing factor for the throughput EMA (higher = more reactive).
	asEMAAlpha = 0.3
	// asMargin is the deadband: throughput must change by more than this fraction to count as a
	// real improvement or regression. Smaller changes are treated as a plateau (no benefit).
	asMargin = 0.05
	// asProbeEvery is how many ticks pass between exploratory upward probes that look for a moved
	// optimum after the pool has settled.
	asProbeEvery = 20
	// asHoldN is how many ticks the governor holds steady after reversing direction, to let the
	// change settle before measuring again.
	asHoldN = 3
	// asSendGrace bounds how long remove() will wait to hand a quit token to a busy worker before
	// giving up for this tick, so a saturated pool never stalls the governor.
	asSendGrace = 50 * time.Millisecond
)

// autoScaleCfg is the validated configuration produced by WithAutoScale.
type autoScaleCfg struct {
	min, max int
}

// validate reports whether the bounds are usable.
func (c autoScaleCfg) validate() error {
	if c.min < 1 {
		return fmt.Errorf("stagedpipe.WithAutoScale: min must be >= 1 pipeline, got %d", c.min)
	}
	if c.max < c.min {
		return fmt.Errorf("stagedpipe.WithAutoScale: max (%d) must be >= min (%d) pipelines", c.max, c.min)
	}
	return nil
}

// WithAutoScale enables dynamic pipeline autoscaling. min and max are pipeline counts; one pipeline
// is C = numStages(sm)+subStages workers, so the live worker count ranges over [min*C, max*C] and
// each scale step adds or removes one whole pipeline (C workers). Instead of a fixed num pipelines,
// the pool starts at num (clamped into [min, max]) and continuously adjusts to maximize throughput:
// it adds a pipeline while throughput keeps improving under load, holds once more pipelines stop
// helping, backs off if throughput regresses, and re-probes periodically to track a changing
// workload.
func WithAutoScale(min, max int) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		cfg := autoScaleCfg{min: min, max: max}
		if err := cfg.validate(); err != nil {
			return o, err
		}
		o.autoScale = &cfg
		return o, nil
	}
}

// sample is one measurement the governor takes each control tick.
type sample struct {
	t         time.Time
	completed int64 // stats.completed snapshot (monotonic)
	running   int64 // stats.running snapshot (in-flight request gauge)
	workers   int   // the governor's current target worker count (scaler.size)
}

// decision is the governor's chosen change in worker count for a tick.
type decision struct {
	delta int // >0 spawn, <0 remove, 0 hold
}

// ctrl carries the hill-climb control state. It is touched only by the governor goroutine, so it
// needs no synchronization. decide is a pure function of (prev, cur) plus this state, which makes
// the control policy unit-testable without goroutines or real time.
//
// The policy is a probe-and-evaluate climber. A steady, saturated pool has flat throughput, so we
// cannot tell whether more workers would help without trying: when saturated with headroom the
// controller speculatively adds a step (climbing=true) and, on the next tick, keeps the gain if
// throughput improved or undoes it and settles (recording a ceiling) if it did not. A settled pool
// holds until the periodic re-probe clears the ceiling to re-test a possibly-moved optimum.
// The controller reasons in worker units, but every change is one whole pipeline: width workers
// (width = C = numStages+subStages), bounded by [minW, maxW] = [min*C, max*C]. size therefore stays
// a multiple of width.
type ctrl struct {
	width      int     // one pipeline's worth of workers; the scale step size
	minW, maxW int     // worker bounds = min/max pipelines * width
	ema        float64 // smoothed throughput (req/s)
	baseEMA    float64 // throughput baseline captured at the last up-step
	seeded     bool
	climbing   bool // an up-step is outstanding, awaiting evaluation
	ceiling    int  // worker count learned to give no further benefit (0 = none)
	holdTicks  int  // ticks remaining to hold after settling (hysteresis)
	sinceProbe int  // ticks since the ceiling was last cleared
}

// decide returns the worker-count change for this tick, always a multiple of width. It is pure (no
// goroutines, no time.Now).
func (c *ctrl) decide(prev, cur sample) decision {
	dt := cur.t.Sub(prev.t).Seconds()
	if dt <= 0 {
		return decision{}
	}
	tput := float64(cur.completed-prev.completed) / dt

	if !c.seeded {
		c.ema, c.baseEMA = tput, tput
		c.seeded = true
		return decision{}
	}
	c.ema = asEMAAlpha*tput + (1-asEMAAlpha)*c.ema

	w := cur.workers
	busy := cur.running >= int64(w) // every worker occupied => another pipeline may help

	// Hysteresis: hold a few ticks after settling so the change can take effect.
	if c.holdTicks > 0 {
		c.holdTicks--
		return decision{}
	}

	// Periodically forget the learned ceiling so a settled pool re-probes for a moved optimum.
	c.sinceProbe++
	if c.sinceProbe >= asProbeEvery {
		c.sinceProbe = 0
		c.ceiling = 0
	}

	// Over-provisioned: fewer than half the workers are busy. Release excess pipelines quickly, down
	// to enough whole pipelines to cover what is in use, never below min.
	if int64(w) > 2*cur.running && w > c.minW {
		c.climbing = false
		c.ceiling = 0
		needPipelines := (int(cur.running) + c.width) / c.width // ceil((running+1)/width)
		target := clamp(needPipelines*c.width, c.minW, w)
		return decision{delta: target - w}
	}

	// Evaluate an outstanding up-step (one pipeline added last tick).
	if c.climbing {
		improved := c.ema > c.baseEMA*(1+asMargin)
		switch {
		case improved && busy && w < c.maxW:
			c.baseEMA = c.ema // it helped: keep climbing
			return decision{delta: c.clampDelta(c.width, w)}
		case improved:
			c.climbing = false // helped but can't/needn't grow more: keep the gain, settle
			c.ceiling = w
			c.holdTicks = asHoldN
			return decision{}
		default:
			c.climbing = false // no gain: undo the pipeline and settle just below
			c.ceiling = w
			c.holdTicks = asHoldN
			return decision{delta: c.clampDelta(-c.width, w)}
		}
	}

	// Saturated with headroom and not settled: probe up one pipeline and evaluate next tick.
	if busy && c.ceiling == 0 && w < c.maxW {
		c.baseEMA = c.ema
		c.climbing = true
		return decision{delta: c.clampDelta(c.width, w)}
	}

	// Not saturated (offered load, not workers, is the bottleneck) or already settled: hold.
	return decision{}
}

// clampDelta returns the delta that moves w toward w+delta but keeps the result within [minW, maxW].
func (c *ctrl) clampDelta(delta, w int) int {
	return clamp(w+delta, c.minW, c.maxW) - w
}

// clamp bounds v to [lo, hi].
func clamp(v, lo, hi int) int {
	switch {
	case v < lo:
		return lo
	case v > hi:
		return hi
	default:
		return v
	}
}

// clock is a small seam over time so the governor loop can be driven deterministically in tests.
type clock interface {
	now() time.Time
	tick(d time.Duration) (<-chan time.Time, func())
}

type realClock struct{}

func (realClock) now() time.Time { return time.Now() }

func (realClock) tick(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// scaler owns the elastic worker pool for a Pipelines. All of its state except live is touched only
// by the governor goroutine (loop); live is atomic because runners decrement it as they exit.
type scaler[T any] struct {
	stats *stats
	// spawn1 starts one worker bound to the shared quit channel and live counter. It is a
	// pipeline runner method value; any pipeline works since all share in/out/sm/stats.
	spawn1 func(quit <-chan struct{}, live *atomic.Int64)
	quit   chan struct{} // unbuffered: one token removes exactly one worker
	stop   chan struct{} // closed by Close to stop the governor
	live   atomic.Int64  // actual live worker goroutines (for introspection/tests)
	size   int           // governor's target worker count (authoritative for decisions)
	ctrl   *ctrl
	clk    clock
}

// spawn starts n new workers and returns n.
func (s *scaler[T]) spawn(n int) int {
	for i := 0; i < n; i++ {
		s.live.Add(1)
		go s.spawn1(s.quit, &s.live)
	}
	return n
}

// remove asks up to n workers to exit and returns how many quit tokens it actually delivered. A
// delivered token is received by exactly one worker, which exits and decrements live. It gives up
// early on stop or after asSendGrace so a fully-busy pool cannot stall the governor.
func (s *scaler[T]) remove(n int) int {
	removed := 0
	for i := 0; i < n; i++ {
		select {
		case s.quit <- struct{}{}:
			removed++
		case <-s.stop:
			return removed
		case <-time.After(asSendGrace):
			return removed
		}
	}
	return removed
}

// snapshot captures the current control inputs.
func (s *scaler[T]) snapshot() sample {
	return sample{
		t:         s.clk.now(),
		completed: s.stats.completed.Load(),
		running:   s.stats.running.Load(),
		workers:   s.size,
	}
}

// loop is the governor: each tick it samples, decides, and applies the worker-count change. It
// exits when stop is closed.
func (s *scaler[T]) loop() {
	ticks, stop := s.clk.tick(asInterval)
	defer stop()

	prev := s.snapshot()
	for {
		select {
		case <-s.stop:
			return
		case <-ticks:
			cur := s.snapshot()
			switch d := s.ctrl.decide(prev, cur); {
			case d.delta > 0:
				s.size += s.spawn(d.delta)
			case d.delta < 0:
				s.size -= s.remove(-d.delta)
			}
			prev = cur
		}
	}
}
