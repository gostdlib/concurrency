package stagedpipe

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

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
		DAG[DAGData](),
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

func TestMethodName(t *testing.T) {
	sm := NewSM(&client.ID{})
	mn := methodName(sm.Start)
	if !strings.HasSuffix(mn, ".Start") {
		t.Fatalf("TestMethodName: got %s, it to end with '.Start'", mn)
	}
}
