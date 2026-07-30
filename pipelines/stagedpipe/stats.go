package stagedpipe

import (
	"math"
	"sync/atomic"
	"time"
)

// Stats are the stats for the Pipeline.
type Stats struct {
	// Running is the number of currently running Request(s).
	Running int64
	// Completed is the number of completed Request(s).
	Completed int64
	// Min is the minimum running time for a Request.
	Min time.Duration
	// Avg is the avg running time for a Request.
	Avg time.Duration
	// Max is the maximim running time for a Request.
	Max time.Duration

	// IngestStats contains information on Pipeline ingestion.
	IngestStats IngestStats

	// Keyed contains keyed-ordering metrics. Its fields are zero unless WithKey is set.
	Keyed KeyedStats
}

// KeyedStats reports the operational signals of keyed ordering: how much of the pool a hot key is
// tying up, and how long Requests spend blocked. All fields are zero when keyed ordering is off.
type KeyedStats struct {
	// ParkedWorkers is the number of pipeline workers currently blocked at an ordered-stage barrier
	// waiting their key's turn. A high value against the pool size means one or more hot keys are
	// tying up workers; WithAdmissionDepth bounds it.
	ParkedWorkers int64
	// AdmissionWaiters is the number of Requests currently blocked at Submit waiting for a per-key
	// admission slot (WithAdmissionDepth). Zero when WithAdmissionDepth is not set.
	AdmissionWaiters int64
	// BarrierWaitMin, BarrierWaitAvg and BarrierWaitMax are how long Requests spend blocked at
	// ordered-stage barriers. A near-zero minimum is the leaders that never wait; a large maximum is a
	// Request that sat behind a slow predecessor.
	BarrierWaitMin time.Duration
	BarrierWaitAvg time.Duration
	BarrierWaitMax time.Duration
}

// stats is used to atomically calculate our Pipeline stats.
type stats struct {
	ingestStats *ingestStats

	running   atomic.Int64
	completed atomic.Int64
	min       atomic.Int64
	max       atomic.Int64
	avgTotal  atomic.Int64

	// Keyed-ordering metrics. parked and admissionWait are gauges; the barrier fields accumulate a
	// min/avg/max of barrier wait durations, like ingestStats.
	parked          atomic.Int64
	admissionWait   atomic.Int64
	barrierCount    atomic.Int64
	barrierAvgTotal atomic.Int64
	barrierMin      atomic.Int64
	barrierMax      atomic.Int64
}

func newStats() *stats {
	s := &stats{ingestStats: &ingestStats{}}
	// Seed the minimums to MaxInt64 so the first recorded value wins in setMin. Left at 0,
	// no positive duration is ever smaller, so Min would always report 0.
	s.min.Store(math.MaxInt64)
	s.ingestStats.min.Store(math.MaxInt64)
	s.barrierMin.Store(math.MaxInt64)
	return s
}

// recordBarrierWait folds one barrier wait of duration d into the barrier min/avg/max.
func (s *stats) recordBarrierWait(d time.Duration) {
	ns := int64(d)
	s.barrierCount.Add(1)
	s.barrierAvgTotal.Add(ns)
	setMin(&s.barrierMin, ns)
	setMax(&s.barrierMax, ns)
}

// keyedStats snapshots the keyed-ordering metrics.
func (s *stats) keyedStats() KeyedStats {
	ks := KeyedStats{
		ParkedWorkers:    s.parked.Load(),
		AdmissionWaiters: s.admissionWait.Load(),
		BarrierWaitMax:   time.Duration(s.barrierMax.Load()),
	}
	if m := s.barrierMin.Load(); m != math.MaxInt64 {
		ks.BarrierWaitMin = time.Duration(m)
	}
	if c := s.barrierCount.Load(); c != 0 {
		ks.BarrierWaitAvg = time.Duration(s.barrierAvgTotal.Load() / c)
	}
	return ks
}

func (s *stats) toStats() Stats {
	stats := Stats{
		Running:   s.running.Load(),
		Completed: s.completed.Load(),
		Max:       time.Duration(s.max.Load()),
	}
	// Report Min only once a real value has been recorded; the seed value means "unset".
	if m := s.min.Load(); m != math.MaxInt64 {
		stats.Min = time.Duration(m)
	}
	if stats.Completed != 0 {
		stats.Avg = time.Duration(s.avgTotal.Load() / stats.Completed)
	}
	stats.IngestStats = s.ingestStats.toIngestStats(stats.Completed)
	stats.Keyed = s.keyedStats()
	return stats
}

// IngestStats detail how long a request waits for a Pipeline to be ready.
type IngestStats struct {
	// Min is the minimum running time for a Request.
	Min time.Duration
	// Avg is the avg running time for a Request.
	Avg time.Duration
	// Max is the maximim running time for a Request.
	Max time.Duration
}

type ingestStats struct {
	min      atomic.Int64
	max      atomic.Int64
	avgTotal atomic.Int64
}

func (i *ingestStats) toIngestStats(completed int64) IngestStats {
	stats := IngestStats{
		Max: time.Duration(i.max.Load()),
	}
	// Report Min only once a real value has been recorded; the seed value means "unset".
	if m := i.min.Load(); m != math.MaxInt64 {
		stats.Min = time.Duration(m)
	}
	if completed != 0 {
		stats.Avg = time.Duration(i.avgTotal.Load() / completed)
	}
	return stats
}

// setMin will set current to v if v is smaller that current.
func setMin(current *atomic.Int64, v int64) {
	for {
		c := (*current).Load()
		if v >= c {
			return
		}
		if (*current).CompareAndSwap(c, v) {
			return
		}
	}
}

// setMax will set current to v if v is bigger than current.
func setMax(current *atomic.Int64, v int64) {
	for {
		c := (*current).Load()
		if v <= c {
			return
		}
		if (*current).CompareAndSwap(c, v) {
			return
		}
	}
}
