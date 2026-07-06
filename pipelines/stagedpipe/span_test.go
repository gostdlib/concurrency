package stagedpipe

import (
	"strings"
	"testing"

	"github.com/gostdlib/base/context"
	baseTrace "github.com/gostdlib/base/telemetry/otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// tracedCtx returns a context carrying a recording root span plus the base tracer that
// context.NewSpan reads (via TracerKey) to create child spans, and a recorder collecting ended
// spans. The returned func ends the root span.
func tracedCtx(t *testing.T) (context.Context, *tracetest.SpanRecorder, func()) {
	t.Helper()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test")
	ctx, root := tracer.Start(t.Context(), "root")
	// base's span package resolves the tracer from the context, not from the parent span's
	// provider, so we attach it explicitly for the test.
	ctx = context.WithValue(ctx, baseTrace.TracerKey, tracer)
	return ctx, sr, func() { root.End() }
}

// runOne drives a single Request through a 1-stage pipeline using ctx, and blocks until output is
// fully drained and the pipeline is closed.
func runOne(t *testing.T, ctx context.Context, name string) {
	t.Helper()

	p, err := New[int](name, 1, &statsSM{})
	if err != nil {
		t.Fatalf("%s: cannot create pipeline: %s", name, err)
	}

	rg := p.NewRequestGroup()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range rg.Out() {
		}
	}()
	if err := rg.Submit(Request[int]{Ctx: ctx, Data: 1}); err != nil {
		t.Fatalf("%s: problem submitting request: %s", name, err)
	}
	rg.Close()
	<-done
	p.Close()
}

// TestStageSpansEnded is a regression test that per-stage OTEL spans are ended. execStage creates a
// child span per stage when the Request is recording; if it never calls End() on that span, the
// span is never closed/exported. We drive a pipeline under a recording tracer and require the stage
// span (the SM's ".Start" method) to appear among the ended spans.
func TestStageSpansEnded(t *testing.T) {
	t.Parallel()

	ctx, sr, endRoot := tracedCtx(t)
	runOne(t, ctx, "span test")
	endRoot()

	names := []string{}
	found := false
	for _, s := range sr.Ended() {
		names = append(names, s.Name())
		if strings.HasSuffix(s.Name(), ".Start") {
			found = true
		}
	}
	if !found {
		t.Errorf("TestStageSpansEnded: no ended span for the Start stage; ended spans = %v", names)
	}
}

// TestRequestGroupSpan is a regression test that the RequestGroup span (created in Submit) is ended
// rather than leaked, and that its "Started Submit()" event actually records.
func TestRequestGroupSpan(t *testing.T) {
	t.Parallel()

	ctx, sr, endRoot := tracedCtx(t)
	runOne(t, ctx, "group span test")
	endRoot()

	var group sdktrace.ReadOnlySpan
	names := []string{}
	for _, s := range sr.Ended() {
		names = append(names, s.Name())
		if s.Name() == "stagedpipe.RequestGroup(unnamed)" {
			group = s
		}
	}
	if group == nil {
		t.Fatalf("TestRequestGroupSpan: the RequestGroup span was never ended; ended spans = %v", names)
	}

	foundEvent := false
	for _, e := range group.Events() {
		if e.Name == "Started Submit()" {
			foundEvent = true
		}
	}
	if !foundEvent {
		t.Errorf("TestRequestGroupSpan: the group span has no \"Started Submit()\" event")
	}
}
