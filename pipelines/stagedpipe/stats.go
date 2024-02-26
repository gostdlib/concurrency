package stagedpipe

import (
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
	return &stats{ingestStats: &ingestStats{}}
}

func (s *stats) toStats() Stats {
	stats := Stats{
		Running:   s.running.Load(),
		Completed: s.completed.Load(),
		Min:       time.Duration(s.min.Load()),
		Max:       time.Duration(s.max.Load()),
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
		Min: time.Duration(i.min.Load()),
		Max: time.Duration(i.max.Load()),
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
