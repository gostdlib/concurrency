package concrete

import (
	"context"
	"fmt"
	"sync"
)

/* Code that makes up a statemachine pipeline */

// Stage is a stage in a statemachine.
type Stage func(r Request) Request

// StateMachine represents a statemachine where Start() is the entry Stage
// and you are directed to the next stage to run via Request[T].Next .
type StateMachine interface {
	Start(r Request) Request
}

// Request is a Request to execute in the Pipeline.
type Request struct {
	// Ctx is the context for this request.
	Ctx context.Context
	// Err is used to store an error that occurred in the Pipeline.
	// If Err is set in any Stage, everything stops processing.
	Err error
	// Next is the next Stage to execute. If set to nil, processing stops.
	Next Stage

	// Data is whatever data to use in the Pipeline.
	Data Data
}

// Data will implement T in our Request[T] above
type Data struct {
	// For simplicity here we are going to have each of these have exactly 3 values
	// and 5 slots, the 4th slot will calculate the total and the 5th slot the average.
	Data []int
}

// Pipeline sets up a concurrent (kindof anyways) Pipeline.
type Pipeline struct {
	In  chan Request
	Out chan Request

	wg sync.WaitGroup
	sm StateMachine
}

// New creates a new Pipeline that utilzes the Stage(s) in "sm".
func New(sm StateMachine) *Pipeline {
	p := &Pipeline{In: make(chan Request, 1), Out: make(chan Request, 1), sm: sm}
	// Uses the number of stages in a StateMachine. That logic isn't important for the
	// example, so I'm cheating and just setting it to the StateMachine number of
	// stages I know I'm going to pass here.
	for i := 0; i < 2; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.runner()
		}()
	}
	go func() {
		p.wg.Wait()
		close(p.Out)
	}()
	return p
}

// runner processes a Request by sending it through the StateMachine.
func (p *Pipeline) runner() {
	for r := range p.In {
		r = p.processRequest(r)
		p.Out <- r
	}
}

// processRequest moves through all the stages of the StateMachine that our
// Request directs us to.
func (p *Pipeline) processRequest(r Request) Request {
	stage := p.sm.Start

	for {
		r = stage(r)
		if r.Err != nil || r.Next == nil {
			return r
		}
		stage = r.Next
	}
}

/* End: Code that makes up a statemachine pipeline */

// sm is going to implement our StateMachine. We are simply doing some math inside
// some slices. Non-practical, but this isn't the point.
type sm struct{}

func (s *sm) Start(r Request) Request {
	if len(r.Data.Data) != 5 {
		r.Err = fmt.Errorf("well this is broke")
		r.Next = nil
		return r
	}
	// Calculate the sum.
	r.Data.Data[3] = r.Data.Data[0] + r.Data.Data[1] + r.Data.Data[2]
	r.Next = s.End
	return r
}

func (s *sm) End(r Request) Request {
	// Calculate the average.
	r.Data.Data[4] = r.Data.Data[0] + r.Data.Data[1] + r.Data.Data[2]/3
	r.Next = nil
	return r
}
