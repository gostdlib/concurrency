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
}

// stats is used to atomically calculate our Pipeline stats.
type stats struct {
	ingestStats *ingestStats

	running   atomic.Int64
	completed atomic.Int64
	min       atomic.Int64
	max       atomic.Int64
	avgTotal  atomic.Int64
}

func newStats() *stats {
	s := &stats{ingestStats: &ingestStats{}}
	// Seed the minimums to MaxInt64 so the first recorded value wins in setMin. Left at 0,
	// no positive duration is ever smaller, so Min would always report 0.
	s.min.Store(math.MaxInt64)
	s.ingestStats.min.Store(math.MaxInt64)
	return s
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
