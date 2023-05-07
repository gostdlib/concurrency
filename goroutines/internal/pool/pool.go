package pool

import (
	"sync/atomic"
)

// PoolType is for internal use. Please ignore.
type PoolType uint8

const (
	PTUnknown PoolType = 0
	PTPooled  PoolType = 1
	PTLimited PoolType = 2
)

// SubmitOptions is used internally. Please ignore.
type SubmitOptions struct {
	// Caller is a name of the supposed calling function so that metrics can differentiate
	// who is using the goroutines in the pool.
	Caller string
	// Type is the type of pool this option is mean for.
	Type PoolType
	// NonBlocking indicates this call is non-blocking, which means if that the
	// pool does not have enough capacity it spins off a naked goroutine.
	NonBlocking bool
}

// Metrics contains stats about a goroutine pool.
type Metrics struct {
	// Static is the number of goroutines that currently exist
	// that are reused.
	Static atomic.Int64
	// StaticRunning is the number of static goroutines that are currently
	// running.
	StaticRunning atomic.Int64
	// StaticTotal is the total number of static goroutines have been created.
	StaticTotal atomic.Int64
	// StaticMin is the minimum number of static goroutines that have been
	// allocated.
	StaticMin atomic.Int64
	// StaticMax is the maximum number of static goroutines that have been
	// allocated.
	StaticMax atomic.Int64
	// DynamicRunning is the number of no-reusable goroutines that have been created.
	DynamicRunning atomic.Int64
	// DynamicTotal is the total number of goroutines that have been created.
	DynamicTotal atomic.Int64
}

// Preventer is an interface that prevents implementations of our pools from outside packages.
type Preventer interface {
	pool()
}

// Pool implements Preventer.
type Pool struct{}

//lint:ignore U1000 This is for internal use only.
func (p *Pool) pool() {}

/*
func (m *Metrics) ToStaticMetrics() metrics.Metrics {
	return metrics.Metrics{
		Static:         m.Static.Load(),
		StaticRunning:  m.StaticRunning.Load(),
		StaticTotal:    m.StaticTotal.Load(),
		StaticMin:      m.StaticMin.Load(),
		StaticMax:      m.StaticMax.Load(),
		DynamicRunning: m.DynamicRunning.Load(),
		DynamicTotal:   m.DynamicTotal.Load(),
	}
}
*/
