package stagedpipe_test

import (
	"context"
	"log"
	"testing"

	"github.com/gostdlib/concurrency/pipelines/stagedpipe"
)

type OrderedData struct {
	// Item int is simply used for demonstration, replace with your own attributes.
	Item int
}

func NewOrderedRequest(ctx context.Context, data OrderedData) stagedpipe.Request[OrderedData] {
	return stagedpipe.Request[OrderedData]{
		Ctx:  ctx,
		Data: data,
	}
}

type OrderedSM struct{}

func NewOrderedSM() (*OrderedSM, error) {
	sm := &OrderedSM{}
	return sm, nil
}

// Start is the entry point (first stage) of the pipeline. Every stage that you add must
// have the same signature and be public.
// Note: It is best practice to not attempt concurrency within the Pipeline. This ends up
// being not as efficient as dividing a concurrency step as a Stage and allowing the
// pipeline to handle it. It can also create a lot of excess goroutine churn.
func (s *OrderedSM) Start(req stagedpipe.Request[OrderedData]) stagedpipe.Request[OrderedData] {
	if req.Ctx.Err() != nil {
		req.Err = req.Ctx.Err()
		return req
	}

	// Set the next stage to execute in the pipeline. nil exits the pipeline.
	req.Next = nil
	return req
}

// Close implements stagedpipe.StateMachine.Close(). This can be used to shut down shared
// resources in SM.
func (s *OrderedSM) Close() {
	// Nothing to do here.
}

func TestOrdered(t *testing.T) {
	ctx := context.Background()

	// Create the StateMachine implementation that will be used in our pipeline.
	xsm, err := NewOrderedSM()
	if err != nil {
		log.Fatalf("cannot start state machine: %s", err)
	}

	// Create our parallel and concurrent Pipeline from our StateMachine.
	pipeline, err := stagedpipe.New[OrderedData]("ordered test", 10, xsm, stagedpipe.Ordered[OrderedData]())
	if err != nil {
		log.Fatalf("cannot create a pipeline: %s", err)
	}
	defer pipeline.Close()

	// Setup a RequestGroup to send data on.
	rg := pipeline.NewRequestGroup()
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel()

	// Get responses from the pipeline. Many times you do not need to do anything with the
	done := make(chan error, 1) // Signal that we've received all output.
	got := []int{}
	go func() {
		var err error
		defer func() {
			if err != nil {
				done <- err
			}
			close(done)
		}()

		for out := range rg.Out() {
			if err != nil {
				continue
			}
			if out.Err != nil {
				// Stop input and processing. This is not required, but in the most common cases
				// we no longer want to process this request.
				reqCancel()
				err = out.Err
				continue
			}
			got = append(got, out.Data.Item)
		}
	}()

	// Send data into the pipeline.
	for i := 0; i < 1000000; i++ {
		if reqCtx.Err() != nil {
			break
		}
		req := NewOrderedRequest(reqCtx, OrderedData{Item: i})
		if err := rg.Submit(req); err != nil {
			log.Fatalf("problem submitting request to the pipeline: %s", err)
			break
		}
	}
	rg.Close()

	// Wait for everything to finish.
	pipelineErr := <-done
	if pipelineErr != nil {
		t.Fatalf("TestOrdered: pipeline had an error: %s", pipelineErr)
	}

	for i, v := range got {
		if i != v {
			t.Fatalf("TestOrdered: got %d, want %d", v, i)
		}
	}
}
