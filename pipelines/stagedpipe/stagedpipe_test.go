package stagedpipe

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/concurrency/pipelines/stagedpipe/testing/client"
)

// SM implements StateMachine.
type SM struct {
	// idClient is a client for querying for information based on an ID.
	idClient *client.ID
}

// NewSM creates a new stagepipe.StateMachine.
func NewSM(cli *client.ID) *SM {
	sm := &SM{
		idClient: cli,
	}
	return sm
}

// Close stops all running goroutines. This is only safe after all entries have
// been processed.
func (s *SM) Close() {}

// Start implements StateMachine.Start().
func (s *SM) Start(req Request[[]client.Record]) Request[[]client.Record] {
	// This trims any excess space off of some string attributes.
	// Because "x" is not a pointer, x.recs are not pointers, I need
	// to reassign the changed entry to x.recs[i] .
	for i, rec := range req.Data {
		rec.First = strings.TrimSpace(rec.First)
		rec.Last = strings.TrimSpace(rec.Last)
		rec.ID = strings.TrimSpace(rec.ID)

		switch {
		case rec.First == "":
			log.Println("see record with error")
			req.Err = fmt.Errorf("Record.First cannot be empty")
			return req
		case rec.Last == "":
			req.Err = fmt.Errorf("Record.Last cannot be empty")
			return req
		case rec.ID == "":
			req.Err = fmt.Errorf("Record.ID cannot be empty")
			return req
		}
		req.Data[i] = rec
	}

	req.Next = s.IdVerifier
	return req
}

// IdVerifier takes a Request and adds it to a bulk request to be sent to the
// identity service. This is the last stage of this pipeline.
func (s *SM) IdVerifier(req Request[[]client.Record]) Request[[]client.Record] {
	ctx, cancel := context.WithTimeout(req.Ctx, 2*time.Second)
	defer cancel()

	recs, err := s.idClient.Call(ctx, req.Data)
	if err != nil {
		req.Err = err
		return req
	}
	req.Data = recs
	req.Next = nil
	return req
}

type gen struct {
	lastID int
	errAt  int
}

func (g *gen) genRecord(n int, withErr bool) []client.Record {
	recs := make([]client.Record, n)

	for i := 0; i < n; i++ {
		s := strconv.Itoa(g.lastID + 1)
		g.lastID++
		if withErr && i == 0 {
			log.Println("generated record with error")
			rec := client.Record{Last: s, ID: s} // No First, which is an error
			recs[i] = rec
			continue
		}
		rec := client.Record{First: s, Last: s, ID: s}
		recs[i] = rec
	}
	return recs
}

func (g *gen) genRequests(n int) []Request[[]client.Record] {
	reqs := make([]Request[[]client.Record], n)

	for i, req := range reqs {
		withErr := false
		if g.errAt == i && i != 0 {
			withErr = true
		}
		req.Data = g.genRecord(10, withErr) // 10 items per requests, n requests will be generated
		reqs[i] = req
	}
	return reqs
}

const day = 24 * time.Hour

func TestPipelines(t *testing.T) {
	t.Parallel()

	g := gen{}
	rs1 := g.genRequests(1)
	g = gen{}
	rs1000 := g.genRequests(1000)
	g = gen{errAt: 500}
	rsErr := g.genRequests(1000)

	tests := []struct {
		desc     string
		requests []Request[[]client.Record]
		err      bool
	}{
		{
			desc:     "1 entry only",
			requests: rs1,
		},

		{
			desc:     "1000 entries",
			requests: rs1000,
		},

		{
			desc:     "1000 entries with an error at 500",
			requests: rsErr,
			err:      true,
		},
	}

	sm := NewSM(&client.ID{})
	p, err := New("test statemachine", 10, StateMachine[[]client.Record](sm))
	if err != nil {
		panic(err)
	}
	defer p.Close()

	for _, test := range tests {
		rg := p.NewRequestGroup()
		reqCtx, reqCancel := context.WithCancel(context.Background())
		defer reqCancel()

		expectedRecs := make([]bool, len(test.requests)*10)

		done := make(chan error, 1)
		go func() {
			var err error
			defer func() {
				defer close(done)
				if err != nil {
					done <- err
				}
			}()

			// A RequestGroup must always drain its .Out() channel. If we receive an error and
			// want to stop processing, we can cancel the Context and wait for everything to stop.
			// Here we capture the error so that we can report it. If we get an error, we also
			// rollback the transaction.
			for out := range rg.Out() {
				if err != nil {
					continue
				}
				if out.Err != nil {
					reqCancel()
					err = out.Err
					log.Printf("pipeline had error in stream: %s", out.Err)
				}

				for _, rec := range out.Data {
					if !test.err {
						id, _ := strconv.Atoi(rec.ID)
						expectedRecs[id-1] = true
						if rec.Birth.IsZero() {
							log.Fatalf("TestPipeline(%s): requests are not being processed", test.desc)
						}
						wantBirth := time.Time{}.Add(time.Duration(id) * day)
						if !rec.Birth.Equal(wantBirth) {
							log.Fatalf("TestPipeline(%s): requests are not being processed correctly, ID %d gave Birthday of %v, want %v", test.desc, id, rec.Birth, wantBirth)
						}
					}
				}
			}
		}()

		for _, req := range test.requests {
			if req.Err != nil {
				if req.Err == context.Canceled {
					log.Println("received context.Canceled")
					break
				}
				log.Fatalf("problem reading request block: %s", req.Err)
			}
			req.Ctx = reqCtx
			if err := rg.Submit(req); err != nil {
				t.Logf("Test(%s): problem submitting request to pipeline: %s", test.desc, err)
			}
		}
		// Tell the pipeline that this request group is done.
		rg.Close()

		// We have processed all output.
		processingErr := <-done

		switch {
		case processingErr == nil && test.err:
			t.Errorf("Test(%s): got err == nil, want err != nil", test.desc)
			continue
		case processingErr != nil && !test.err:
			t.Errorf("Test(%s): got err == %s, want err == nil", test.desc, processingErr)
			continue
		case test.err:
			continue
		}

		for i := 0; i < len(expectedRecs); i++ {
			if !expectedRecs[i] {
				t.Errorf("TestPipelines(%s): an expected client.Record[%d] was not set", test.desc, i)
			}
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()

	gen := gen{}
	reqs := gen.genRequests(100000)
	sm := NewSM(&client.ID{})

	p, err := New("test", runtime.NumCPU(), StateMachine[[]client.Record](sm))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := p.NewRequestGroup()
		go func() {
			// For this exercise we need to drain the Out channel so things continue
			// to process.
			for range g.Out() {
			}
		}()

		for _, req := range reqs {
			req.Ctx = context.Background()
			if err := g.Submit(req); err != nil {
				panic(err)
			}
		}
		g.Close()
	}
}

type DAGData struct {
	Num int
}

type DAGSM struct{}

func (d *DAGSM) Close() {}

func (d *DAGSM) Start(req Request[DAGData]) Request[DAGData] {
	if req.Data.Num%2 == 0 {
		req.Next = d.RouteBackToStart
		return req
	}
	req.Next = d.End
	return req
}

func (d *DAGSM) RouteBackToStart(req Request[DAGData]) Request[DAGData] {
	req.Next = d.Start
	return req
}

func (d *DAGSM) End(req Request[DAGData]) Request[DAGData] {
	req.Next = nil
	return req
}

func TestDAG(t *testing.T) {
	t.Parallel()

	sm := &DAGSM{}
	p, err := New[DAGData](
		"test statemachine",
		10,
		sm,
		WithDAG(),
	)

	if err != nil {
		panic(err)
	}
	defer p.Close()

	rg := p.NewRequestGroup()
	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	done := make(chan error, 1)
	got := []Request[DAGData]{}

	go func() {
		defer close(done)

		for out := range rg.Out() {
			got = append(got, out)
		}
	}()

	requests := []Request[DAGData]{
		{Data: DAGData{Num: 0}},
		{Data: DAGData{Num: 1}},
		{Data: DAGData{Num: 2}},
		{Data: DAGData{Num: 3}},
	}

	for _, req := range requests {
		if req.Err != nil {
			if req.Err == context.Canceled {
				log.Println("received context.Canceled")
				break
			}
			log.Fatalf("problem reading request block: %s", req.Err)
		}
		req.Ctx = reqCtx
		if err := rg.Submit(req); err != nil {
			t.Logf("problem submitting request to pipeline: %s", err)
		}
	}
	// Tell the pipeline that this request group is done.
	rg.Close()

	// We have processed all output.
	<-done

	sort.Slice(got, func(i, j int) bool {
		return got[i].Data.Num < got[j].Data.Num
	})

	if len(requests) != len(got) {
		t.Fatalf("got %d, want %d", len(got), len(requests))
	}

	for _, rec := range got {
		log.Println(rec.Data.Num)
	}

	for i := 0; i < len(requests); i++ {
		if got[i].Data.Num != i {
			t.Errorf("got %d, want %d", got[i].Data.Num, i)
		}

		if i%2 == 0 {
			if !IsErrCyclic(got[i].Err) {
				t.Errorf("request %d, got %q, want a cyclic error", i, got[i].Err)
			}
		}
	}
}

// closeSM is a minimal StateMachine that records when Close() is called. It is used to
// regression-test that Pipelines.Close() actually completes its shutdown work.
type closeSM struct {
	closed chan struct{}
}

func newCloseSM() *closeSM {
	return &closeSM{closed: make(chan struct{})}
}

func (s *closeSM) Start(req Request[int]) Request[int] {
	req.Next = nil
	return req
}

func (s *closeSM) Close() {
	close(s.closed)
}

// TestClose is a regression test for a bug where p.wg was incremented on every Submit but
// only decremented on the context-cancel path, never when a Request drained out normally.
// That left Pipelines.Close()'s goroutine blocked forever on p.wg.Wait(), so the output
// channel was never closed and sm.Close() was never called. We submit and fully drain a
// batch of Requests, call Close(), and require sm.Close() to run within a timeout.
func TestClose(t *testing.T) {
	t.Parallel()

	sm := newCloseSM()
	p, err := New[int]("close regression", 2, sm)
	if err != nil {
		t.Fatalf("TestClose: cannot create pipeline: %s", err)
	}

	rg := p.NewRequestGroup()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range rg.Out() {
		}
	}()

	for i := 0; i < 10; i++ {
		req := Request[int]{Ctx: t.Context(), Data: i}
		if err := rg.Submit(req); err != nil {
			t.Fatalf("TestClose: problem submitting request: %s", err)
		}
	}
	rg.Close()
	<-drained

	// Close() spawns a goroutine that waits for every Request to finish, then closes the
	// output channel and calls sm.Close(). If p.wg is never decremented on the success
	// path, that goroutine blocks forever and sm.Close() is never reached.
	p.Close()

	select {
	case <-sm.closed:
	case <-time.After(10 * time.Second):
		t.Fatalf("TestClose: sm.Close() was never called; Pipelines.Close() did not complete (p.wg never reached zero)")
	}
}

// statsSM is a minimal StateMachine whose stage takes a small, non-zero amount of time so
// that recorded run durations are reliably > 0.
type statsSM struct{}

func (s *statsSM) Start(req Request[int]) Request[int] {
	time.Sleep(time.Millisecond)
	req.Next = nil
	return req
}

func (s *statsSM) Close() {}

// TestStats is a regression test for a bug where the min stats were seeded at 0, so setMin
// (which only stores a smaller value) could never record a real minimum and Stats.Min was
// always reported as 0 regardless of how long Requests actually took.
func TestStats(t *testing.T) {
	t.Parallel()

	p, err := New[int]("stats regression", 2, &statsSM{})
	if err != nil {
		t.Fatalf("TestStats: cannot create pipeline: %s", err)
	}
	defer p.Close()

	rg := p.NewRequestGroup()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range rg.Out() {
		}
	}()

	for i := 0; i < 20; i++ {
		if err := rg.Submit(Request[int]{Ctx: t.Context(), Data: i}); err != nil {
			t.Fatalf("TestStats: problem submitting request: %s", err)
		}
	}
	rg.Close()
	<-drained

	got := p.Stats()
	switch {
	case got.Completed != 20:
		t.Fatalf("TestStats: Completed = %d, want 20", got.Completed)
	case got.Min <= 0:
		t.Fatalf("TestStats: Min = %v, want > 0 (the minimum run duration was never recorded)", got.Min)
	case got.Min > got.Max:
		t.Fatalf("TestStats: Min (%v) > Max (%v)", got.Min, got.Max)
	}
}

// TestNewCapturesPipelines is a regression test for a bug where New() discarded every
// *pipeline[T] returned by newPipeline(), so p.pipelines was always an empty slice and the
// individual pipeline handles were unreachable.
func TestNewCapturesPipelines(t *testing.T) {
	t.Parallel()

	const num = 4

	p, err := New[int]("pipelines regression", num, &statsSM{})
	if err != nil {
		t.Fatalf("TestNewCapturesPipelines: cannot create pipeline: %s", err)
	}
	defer p.Close()

	if len(p.pipelines) != num {
		t.Fatalf("TestNewCapturesPipelines: len(p.pipelines) = %d, want %d", len(p.pipelines), num)
	}
	for i, pl := range p.pipelines {
		if pl == nil {
			t.Fatalf("TestNewCapturesPipelines: p.pipelines[%d] is nil", i)
		}
	}
}

// TestIsErrCyclic is a regression test for IsErrCyclic not being wrap-aware: a cyclic Error
// hidden behind fmt.Errorf("%w", ...) used to report false.
func TestIsErrCyclic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "a nil error is not cyclic", err: nil, want: false},
		{name: "a plain cyclic Error is cyclic", err: Error{Type: cyclicErr}, want: true},
		{name: "a wrapped cyclic Error is cyclic", err: fmt.Errorf("stage failed: %w", Error{Type: cyclicErr}), want: true},
		{name: "a non-cyclic Error type is not cyclic", err: Error{Type: "other"}, want: false},
		{name: "a plain error is not cyclic", err: errors.New("boom"), want: false},
	}

	for _, test := range tests {
		got := IsErrCyclic(test.err)
		if got != test.want {
			t.Errorf("TestIsErrCyclic(%s): got %v, want %v", test.name, got, test.want)
		}
	}
}

// TestWithPreProcessors covers New()'s resolution of the type-erased WithPreProcessors
// arguments into PreProcesor[T]: it must accept both the named PreProcesor[T] type and the
// bare func(Request[T]) Request[T], reject anything else, and actually run what it accepts.
func TestWithPreProcessors(t *testing.T) {
	t.Parallel()

	named := PreProcesor[int](func(r Request[int]) Request[int] { r.Data++; return r })
	bare := func(r Request[int]) Request[int] { r.Data++; return r }

	tests := []struct {
		name    string
		pp      any
		wantErr bool
	}{
		{name: "Success: a PreProcesor[T] value is accepted", pp: named, wantErr: false},
		{name: "Success: a bare func(Request[T]) Request[T] is accepted", pp: bare, wantErr: false},
		{name: "Error: a value that is not a function is rejected", pp: 42, wantErr: true},
		{name: "Error: a func with the wrong T is rejected", pp: func(r Request[string]) Request[string] { return r }, wantErr: true},
	}

	for _, test := range tests {
		p, err := New[int]("preproc test", 1, &statsSM{}, WithPreProcessors(test.pp))
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestWithPreProcessors(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestWithPreProcessors(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		// Success path: send one Request through and confirm the preprocessor ran (Data++).
		rg := p.NewRequestGroup()
		got := []int{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for out := range rg.Out() {
				got = append(got, out.Data)
			}
		}()
		if err := rg.Submit(Request[int]{Ctx: t.Context(), Data: 5}); err != nil {
			t.Errorf("TestWithPreProcessors(%s): problem submitting request: %s", test.name, err)
			rg.Close()
			<-done
			p.Close()
			continue
		}
		rg.Close()
		<-done
		p.Close()

		if len(got) != 1 || got[0] != 6 {
			t.Errorf("TestWithPreProcessors(%s): got Data %v, want [6] (preprocessor should have incremented 5)", test.name, got)
		}
	}
}

func TestMethodName(t *testing.T) {
	sm := NewSM(&client.ID{})
	mn := methodName(sm.Start)
	if !strings.HasSuffix(mn, ".Start") {
		t.Fatalf("TestMethodName: got %s, it to end with '.Start'", mn)
	}
}
