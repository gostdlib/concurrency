/*
Package stagedpipe offers a generic, concurrent and parallel pipeline based on a
statemachine to process work. For N number of Stages in the StateMachine, N Stages
can concurrently be processed. You can run pipelines in parallel. So X Pipelines
with N Stages will have X*N Stages processing.

The number of pipelines can be fixed or, with the WithAutoScale() option, automatically
scaled up and down between bounds based on live throughput.

This library requires working knowledge of both the specific type of Go statemachine
implementation and basic Go pipelining.

Full introduction including a hello world example can be found here:
https://vimeo.com/879175351?share=copy

Please view the README.md for more detailed information on how to get started.

Every pipeline will receive a Request, which contains the data to be manipulated.
Each Request is designed to be stack allocated, meaning the data should not be a pointer
unless absolutely necessary.

You define a StateMachine object that satisfies the StateMachine interface. These states
represent the stages of the pipeline. All StateMachine methods that implement a Stage MUST BE PUBLIC.

A RequestGroup represents a set of related Request(s) that should be processed together.
A new RequestGroup can be created with Pipelines.NewRequestGroup().

Requests enter the Pipelines via the RequestGroup.Submit() method. Requests are received
with RequestGroup.Out(), which returns a channel of Request(s).

Multiple RequestGroup(s) can send into the Pipelines for processing, as everything is
muxed into the Pipelines and demuxed out to the RequestGroup.Out() channel.

There is a provided CLI application called `stagedpipe-cli“ located in the `tools/` directory
that can be used to generate all the boilerplate you see below for a working example.  You can
install it like this:

```
go install github.com/gostdlib/concurrency/pipelines/stagedpipe/tools/stagedpipe-cli@latest
```
Simply enter into your new package's directory, and type: `stagedpipe-cli -m -p "[package root]/sm"` to get:

```
├──myPipeline

	├── main.go
	└──sm
	    ├── data.go
	    └── sm.go

```
Run `go mod init <path>`, `go mod tidy` and `go fmt ./...`, to get a running program:
```
├──myPipeline

	├── go.mod
	├── go.sum
	├── main.go
	└──sm
	    ├── data.go
	    └── sm.go

```

Type `go run .` to run the basic pipeline that you can change to fit your needs.

Here is an example that runs inside the playground: https://go.dev/play/p/zaiNU_kbp6_3

Here is an ETL pipeline example: https://github.com/johnsiilver/concurrency/pipelines/tree/main/stagedpipe/examples/etl/bostonFoodViolations/pipelined

A video introduciton to the ETL pipeline: https://player.vimeo.com/video/879203973?h=24035c0a82

Note: This package supports OTEL spans and will record information into OTEL spans if provided.
*/
package stagedpipe

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	// stdlibsync is imported only for WaitGroup. base/concurrency/sync deliberately omits it in favor
	// of sync.Group, but Group cannot express the counting this package needs: the increment happens
	// on the submitting goroutine and the matching decrement on a drain goroutine, which Group.Go/Wait
	// has no way to split. Everything else here comes from base/concurrency/sync.
	stdlibsync "sync"
	"sync/atomic"
	"time"

	"github.com/johnsiilver/dynamics/demux"
	"github.com/johnsiilver/dynamics/method"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/concurrency/worker"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/errors"
	"github.com/gostdlib/base/telemetry/otel/trace/span"

	"github.com/go-json-experiment/json"
)

const (
	// cyclicErr is the error type for cyclic errors.
	cyclicErr = "cyclic"
)

// Error represents a typed error that this package can return.
// Not all errors are of this type.
type Error struct {
	// Type is the type of error.
	Type string
	// Msg is the message of the error.
	Msg string
}

// Error returns the Error type and message.
func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Msg)
}

// IsErrCyclic returns true if the error is a cyclic error. A cyclic error is when
// a stage is called more than once in a single Request. This is only returned
// if the WithDAG() option is set.
func IsErrCyclic(err error) bool {
	if err == nil {
		return false
	}
	var t Error
	if !errors.As(err, &t) {
		return false
	}
	return t.Type == cyclicErr
}

// PanicError is the value a Pipelines re-panics with, on the caller's goroutine, after a stage
// panicked. Value is the original panic and Stack is the stack captured at the panic site — the
// live re-panic stack belongs to the caller, not the worker goroutine that panicked, so the
// captured Stack is how the origin is preserved. Recover it with a type assertion to PanicError.
type PanicError struct {
	// Value is the value the stage passed to panic().
	Value any
	// Stack is the stack trace captured in the worker at the moment of the panic.
	Stack []byte
}

// Error implements error.
func (e PanicError) Error() string {
	return fmt.Sprintf("stagedpipe: panic in pipeline stage: %v\n\noriginating stack:\n%s", e.Value, e.Stack)
}

// ErrTornDown is the per-Request error set on every Request once the pipeline is tearing down after
// a stage panic — both the Requests fast-drained by execStage and the ones whose barrier wait was
// aborted. It is permanent (wraps ErrPermanent), so a retrying caller gives up; match it with
// errors.Is(err, ErrTornDown). The original panic surfaces separately as a PanicError from
// RequestGroup.Close().
var ErrTornDown = fmt.Errorf("stagedpipe: pipeline torn down after a stage panic: %w", ErrPermanent)

// seenStages tracks what stages have been called in a Request. This is used to detect cyclic
// errors. Stages are held as their entry PCs rather than their names: a stage's identity has always
// been its PC (a name is only ever derived from one, see methodName), so comparing PCs is the same
// test without the name lookup, and it keeps the name resolution off the per-stage path entirely --
// callTrace resolves names once, when a cycle has actually been found. Implemented with a slice to
// reduce allocations and is faster to remove elements from than a map (to allow reuse). n is small,
// so the lookup performance is negligible. This is not thread-safe (which is not needed).
type seenStages []uintptr

// seen returns true if the stage has been seen before. If it has not been seen,
// it adds it to the list of seen stages.
func (s *seenStages) seen(pc uintptr) bool {
	for _, st := range *s {
		if st == pc {
			return true
		}
	}

	n := append(*s, pc)
	*s = n
	return false
}

// callTrace returns a string of the stages that have been called. It is only reached when a cycle
// has been detected, so resolving each PC to its name here costs nothing on the success path.
func (s *seenStages) callTrace() string {
	out := strings.Builder{}
	for i, pc := range *s {
		if i != 0 {
			out.WriteString(" -> ")
		}
		out.WriteString(funcName(pc))
	}
	return out.String()
}

// reset truncates the seenStages object in place so it can be reused. It reuses the same
// header the pool handed back rather than allocating a new one, which would defeat the pool.
func (s *seenStages) reset() *seenStages {
	*s = (*s)[:0]
	return s
}

// Requests is a Request to be processed by a pipeline.
type Request[T any] struct {
	span span.Span

	// queueTime and ingestTime hold the times when the Request was queued and ingested.
	queueTime, ingestTime time.Time

	// Ctx is a Context scoped for this requestor set of requests.
	Ctx context.Context

	// Data is data that is processed in this Request.
	Data T

	// Err, if set, is an error for the Request. This type of error is for unrecoverable
	// errors in processing, not errors for the data being processed. For example, if it
	// can't communicate with a database or RPC service. For errors with the data itself,
	// add the error to the underlying data type as a separate error.
	Err error

	// Next is the next stage to be executed. Must be set at each stage of a StateMachine.
	// If set to nil, exits the pipeline.
	Next Stage[T]

	// seenStages tracks what stages have been called in this Request. This is used to
	// detect cyclic errors. If nil, cyclic errors are not checked.
	seenStages *seenStages

	// groupNum is used to track what RequestGroup this Request belongs to for routing.
	groupNum uint64
	// itemNum is used to track the order of the Request in the RequestGroup.
	itemNum uint64

	// ks is the live ordering state for this Request's partition key, assigned at Submit when keyed
	// ordering is on (nil otherwise). It is carried on the Request so the ordering hot path reaches
	// the key's sequencers without a registry lookup.
	ks *keyState
	// seq is the dense, 0-based per-key sequence assigned at Submit. It is the turn a Request takes
	// at each ordered stage for its key. Unused when keyed ordering is off.
	seq uint64
	// clearedMask is the set of ordered barriers this Request has resolved, one bit per barrier
	// indexed by keyOrder.barrierIdx. Used to enforce at-most-once resolution and to drive the exit
	// clear of barriers the Request never reached.
	clearedMask uint64
	// admitted is true once this Request holds one of its key's admission slots (WithAdmissionDepth),
	// so the exit hook releases exactly the slots that were acquired.
	admitted bool
}

func (r Request[T]) otelStart() Request[T] {
	if !r.span.IsRecording() {
		return r
	}

	j, err := json.Marshal(r.Data)
	if err != nil {
		j = []byte(fmt.Sprintf("Error marshaling data: %s", err.Error()))
	}

	r.span.Event(
		"processing start",
		attribute.String("data", string(j)),
		attribute.Int64("queue_wait_ns", int64(time.Since(r.queueTime))),
	)
	return r
}

// Event records an OTEL event into the Request span with name and attrs. This allows stages in your
// statemachine to record events inside each stage. Build attrs with the go.opentelemetry.io/otel/attribute
// helpers, e.g. attribute.String("key", "value") or attribute.Int("count", 3).
//
// Note: This is a no-op if the Request is not recording.
func (r Request[T]) Event(name string, attrs ...attribute.KeyValue) {
	r.span.Event(name, attrs...)
}

func (r Request[T]) otelEnd() {
	if !r.span.IsRecording() {
		return
	}
	if r.Err != nil {
		r.span.Status(codes.Error, r.Err.Error())
	}
	j, err := json.Marshal(r.Data)
	if err != nil {
		j = []byte(fmt.Sprintf("Error marshaling data: %s", err.Error()))
	}
	r.span.Event(
		"processing end",
		attribute.String("data", string(j)),
		attribute.Int64("elapsed_ns", int64(time.Since(r.queueTime))),
	)
	r.span.End()
}

// StateMachine represents a state machine where the methods that implement Stage
// are the States and execution starts with the Start() method.
type StateMachine[T any] interface {
	// Start is the starting Stage of the StateMachine.
	Start(req Request[T]) Request[T]
	// Close stops the StateMachine.
	Close()
}

// Stage represents a function that executes at a given state.
type Stage[T any] func(req Request[T]) Request[T]

// PreProcessor is called before each Stage. If req.Err is set
// execution of the Request in the StateMachine stops.
type PreProcesor[T any] func(req Request[T]) Request[T]

// Pipelines provides access to a set of Pipelines that processes DBD information.
type Pipelines[T any] struct {
	name string

	in  chan Request[T]
	out chan Request[T]

	pipelines     []*pipeline[T]
	preProcessors []PreProcesor[T]
	sm            StateMachine[T]

	// subStages is used to record the number of stages in objects that aren't the
	// StateMachine.
	subStages int
	// delayWarning is used to send a log message when pushing entries to the out channel
	// takes longer than the supplied time.Duration.
	delayWarning time.Duration

	// wg counts Request(s) that have entered the Pipelines and not yet been handed to a caller. It is
	// used to know when it is safe to close the output channel.
	wg *stdlibsync.WaitGroup

	// pool is where this Pipelines' coordinator jobs run: the autoscale governor, the Close reaper and
	// each RequestGroup's output drain loop. It is a Sub of the caller's pool (or of the default pool
	// when the caller's is Limited), named for this Pipelines so its jobs report their own metrics. The
	// stage workers do NOT run here; see Pipelines.workers.
	pool *worker.Pool
	// bg is the Context the coordinators are submitted and waited on. It is New's Context stripped of
	// cancellation, so a cancelled caller Context never causes Submit to decline a coordinator (which
	// would leave the output channel unclosed) while the coordinators still see its values: the
	// MeterProvider, tracer and pool the caller configured.
	bg context.Context
	// workers is the join point for the stage worker goroutines. It is a Group with no Pool on purpose;
	// see newPipeline's runner spawn for why.
	workers *sync.Group

	// requestGroupNum is used to generate the next number for a RequestGroup used to
	// route requests to the correct RequestGroup.
	requestGroupNum atomic.Uint64
	// demux is used to demux the output of the Pipelines to the RequestGroup(s).
	demux *demux.Demux[uint64, Request[T]]

	stats *stats
	// seenPool recycles the seenStages trackers used for cyclic detection. It is built only when
	// WithDAG() is set and is nil otherwise, so a pipeline that does not need it pays no meter.
	seenPool *sync.Pool[*seenStages]
	// ss is true if the WithDAG() option was set.
	ss bool
	// ordered is true if the WithOrdered() option was set.
	ordered bool
	// scaler drives dynamic worker autoscaling when WithAutoScale() is set; nil otherwise.
	scaler *scaler[T]
	// keyOrder holds keyed-ordering config and per-key state when WithKey/WithOrderedStages are set;
	// nil otherwise. When nil, the ordering hooks are skipped entirely and pay nothing.
	keyOrder *keyOrder[T]
	// panicked holds the first stage panic seen by any worker (nil until one happens). Once set, the
	// workers stop running stage bodies and fast-drain the remaining Requests with a torn-down error,
	// and RequestGroup.Close re-panics with it. Shared by every worker via a pointer.
	panicked atomic.Pointer[PanicError]
}

// pipelinesOptions holds the values set by the Option(s) passed to New(). It is type-erased
// (Option is not generic), so preProcessors and subStageObjs are resolved against the concrete
// T inside New().
type pipelinesOptions struct {
	// ss is true if the WithDAG() option was set.
	ss bool
	// ordered is true if the WithOrdered() option was set.
	ordered bool
	// preProcessors are PreProcessors for each stage. Each must be a PreProcesor[T] (or the
	// equivalent func(Request[T]) Request[T]); New() type-asserts them once T is known.
	preProcessors []any
	// delayWarning is used to send a log message when pushing entries to the out channel
	// takes longer than the supplied time.Duration.
	delayWarning time.Duration
	// subStageObjs holds objects whose Stage methods are counted toward concurrency but that
	// do not live on the StateMachine. New() counts their stages once T is known.
	subStageObjs []any
	// autoScale, when non-nil, enables dynamic worker autoscaling within its [min, max] bounds.
	autoScale *autoScaleCfg
	// keyFunc is the WithKey partition-key extractor, type-erased as func(T) string; New() asserts
	// it once T is known. nil when keyed ordering is off.
	keyFunc any
	// orderedStages holds the WithOrderedStages barriers, each a Stage[T] erased to any; New()
	// resolves them to entry PCs once T is known. Empty when keyed ordering is off.
	orderedStages []any
	// stopKeyOnErr is the WithStopKeyOnErr flag: poison a key on its first Request error.
	stopKeyOnErr bool
	// admitDepth is the WithAdmissionDepth value and admitDepthSet marks it as explicitly given. New
	// resolves the effective per-key bound: unset becomes defaultAdmitDepth, an explicit 0 stays 0
	// (unbounded), any other value is used as-is.
	admitDepth    int
	admitDepthSet bool
}

// validate checks option combinations that do not depend on T. Keyed ordering is meaningful only
// when WithKey and WithOrderedStages are set together, and WithStopKeyOnErr only alongside them;
// each lone use is a permanent configuration error, as a bad configuration cannot succeed on retry.
// The T-dependent resolution (asserting keyFunc's type and resolving stage PCs) happens in New once
// T is known.
func (o pipelinesOptions) validate() error {
	hasKey := o.keyFunc != nil
	hasStages := len(o.orderedStages) > 0
	switch {
	case hasKey && !hasStages:
		return fmt.Errorf("stagedpipe: WithKey requires WithOrderedStages, otherwise nothing is ordered: %w", ErrPermanent)
	case hasStages && !hasKey:
		return fmt.Errorf("stagedpipe: WithOrderedStages requires WithKey to supply the partition key: %w", ErrPermanent)
	case o.stopKeyOnErr && !hasKey:
		return fmt.Errorf("stagedpipe: WithStopKeyOnErr requires WithKey: %w", ErrPermanent)
	case o.admitDepthSet && !hasKey:
		return fmt.Errorf("stagedpipe: WithAdmissionDepth requires WithKey: %w", ErrPermanent)
	}
	return nil
}

// Option is an option for the New() constructor. It is not generic, so a single set of Option
// values can be passed to New() for any T; options that depend on T (WithPreProcessors,
// WithCountSubStages) collect their arguments as any and are resolved inside New().
type Option func(o pipelinesOptions) (pipelinesOptions, error)

// WithDAG makes the StateMachine a Directed Acyllic Graph. This means that no Stage
// can be called more than once in a single Request. If a Stage is called more than
// once, the request will exit with a cyclic error that can be detected with IsErrCyclic().
func WithDAG() Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		o.ss = true
		return o, nil
	}
}

// WithOrdered makes the Pipelines output requests in the order they are received by a request group.
// This can slow down output as it stores finished requests until older ones finish processing
// and are output.
func WithOrdered() Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		o.ordered = true
		return o, nil
	}
}

// WithPreProcessors provides a set of functions that are called in order
// at each stage in the StateMachine. This is used to do work that is common to
// each stage instead of having to call the same code. Similar to http.HandleFunc
// wrapping techniques. Each argument must be a PreProcesor[T] (or the equivalent
// func(Request[T]) Request[T]) for the T used in New(); New() returns an error if any is not.
func WithPreProcessors(p ...any) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		o.preProcessors = append(o.preProcessors, p...)
		return o, nil
	}
}

// WithDelayWarning will send a log message whenever pushing entries to the out channel
// takes longer than the supplied time.Duration. Not setting this results will result
// in no warnings. Useful when chaining Pipelines and figuring out where something is stuck.
func WithDelayWarning(d time.Duration) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		if d < 0 {
			return o, fmt.Errorf("cannot provide a WithDelayWarning < 0")
		}
		o.delayWarning = d
		return o, nil
	}
}

// WithCountSubStages is used when the StateMachine object does not hold all the Stage(s).
// This allows you to design multiple pipleines that use the same data object but will
// be executed as a single pipeline. WithCountSubStages is used to correctly calculate
// the concurrency. Without this, only stages in the StateMachine object will be counted
// toward the concurrency count.
func WithCountSubStages(subStageObj ...any) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		o.subStageObjs = append(o.subStageObjs, subStageObj...)
		return o, nil
	}
}

// resetNext is a Preprocessor we use to reset req.Next at each stage. This prevents
// accidental infinite loop scenarios.
func resetNext[T any](req Request[T]) Request[T] {
	req.Next = nil
	return req
}

// New creates a new Pipelines object with "num" pipelines running in parallel.
// Each underlying pipeline runs concurrently for each stage. The first StateMachine.Start()
// in the list is the starting place for executions.
//
// ctx is the Pipelines' lifetime Context, not a per-Request one: it supplies the worker pool the
// coordinators run on and the MeterProvider and tracer their metrics and spans are reported through.
// Cancelling it does not stop the Pipelines — call Close() for that, and cancel an individual
// Request through its own Request.Ctx.
func New[T any](ctx context.Context, name string, num int, sm StateMachine[T], options ...Option) (*Pipelines[T], error) {
	if num < 1 {
		return nil, fmt.Errorf("num must be > 0")
	}
	if sm == nil {
		return nil, fmt.Errorf("must provide a valid StateMachine")
	}

	opts := pipelinesOptions{}
	for _, o := range options {
		var err error
		opts, err = o(opts)
		if err != nil {
			return nil, err
		}
	}
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// Resolve keyed ordering (WithKey / WithOrderedStages) before anything spawns, so a bad
	// configuration returns before a single pipeline goroutine or the scaler is started. validate()
	// has already confirmed the two appear together, so a keyFunc means both are present. The
	// keyFunc assertion and stage-PC resolution are T-dependent, hence resolved here in New[T].
	var keyOrd *keyOrder[T]
	if opts.keyFunc != nil {
		keyFunc, ok := opts.keyFunc.(func(T) string)
		if !ok {
			return nil, fmt.Errorf("stagedpipe.WithKey: keyFunc %T is not func(T) string: %w", opts.keyFunc, ErrPermanent)
		}
		// Apply the default admission depth when the caller did not set one.
		depth := opts.admitDepth
		if !opts.admitDepthSet {
			depth = defaultAdmitDepth
		}
		ko, err := newKeyOrder[T](keyFunc, opts.orderedStages, opts.stopKeyOnErr, depth)
		if err != nil {
			return nil, err
		}
		keyOrd = ko
	}

	// Options are type-erased so Option can be non-generic; resolve the T-dependent ones here.
	// PreProcessors always start with the built-in resetNext.
	preProcessors := []PreProcesor[T]{resetNext[T]}
	for i, pp := range opts.preProcessors {
		switch f := pp.(type) {
		case PreProcesor[T]:
			preProcessors = append(preProcessors, f)
		case func(Request[T]) Request[T]:
			preProcessors = append(preProcessors, f)
		default:
			var want PreProcesor[T]
			return nil, fmt.Errorf("stagedpipe.WithPreProcessors: preprocessor at index %d has type %T, want %T", i, pp, want)
		}
	}

	subStages := 0
	for _, obj := range opts.subStageObjs {
		subStages += numStages[T](obj)
	}

	in := make(chan Request[T], 1)
	out := make(chan Request[T], 1)
	stats := newStats()

	// Keyed ordering records admission-wait through the shared stats; the workers record parked and
	// barrier-wait through the same stats via pipeline.stats.
	if keyOrd != nil {
		keyOrd.stats = stats
	}

	d, err := demux.New(
		out,
		func(r Request[T]) uint64 {
			return r.groupNum
		},
		func(r Request[T], err error) {
			panic(fmt.Sprintf("bug: received %#+v and got demux error: %s", r, err))
		},
	)
	if err != nil {
		return nil, err
	}

	// Coordinators run on the caller's pool so their work is accounted where the caller expects, unless
	// that pool is Limited: a coordinator lives as long as the Pipelines (or the RequestGroup it
	// drains), so a limited slot it took would be held for that whole time instead of doing work, and
	// against a saturated Limited pool the submit would wait for a slot that never frees. Pool.Limit()
	// reports 0 for an unlimited pool, which is the check. Sub() is called directly here, not through a
	// helper, because its meter name is derived from the caller's stack frame.
	base := context.Pool(ctx)
	if base.Limit() != 0 {
		base = base.Default()
	}
	pool := base.Sub(ctx, name)

	// A Context that cannot be cancelled, so a cancelled caller Context never makes Submit decline a
	// coordinator and strand the output channel unclosed. Values (MeterProvider, tracer, pool) survive.
	bg := context.WithoutCancel(ctx)

	var seenPool *sync.Pool[*seenStages]
	if opts.ss {
		seenPool = sync.NewPool[*seenStages](ctx, "seenStages", func() *seenStages { return &seenStages{} })
	}

	p := &Pipelines[T]{
		name:          name,
		in:            in,
		out:           out,
		wg:            &stdlibsync.WaitGroup{},
		pool:          pool,
		bg:            bg,
		workers:       &sync.Group{},
		sm:            sm,
		stats:         stats,
		seenPool:      seenPool,
		demux:         d,
		preProcessors: preProcessors,
		subStages:     subStages,
		delayWarning:  opts.delayWarning,
		ss:            opts.ss,
		ordered:       opts.ordered,
		keyOrder:      keyOrd,
	}

	pipelines := make([]*pipeline[T], 0, num)
	for i := 0; i < num; i++ {
		args := pipelineArgs[T]{
			name:          name,
			id:            i,
			in:            in,
			out:           out,
			num:           num,
			sm:            sm,
			subStages:     p.subStages,
			preProcessors: p.preProcessors,
			stats:         stats,
			seenPool:      seenPool,
			delayWarning:  p.delayWarning,
			ss:            p.ss,
			autoscale:     opts.autoScale != nil,
			keyOrder:      keyOrd,
			panicked:      &p.panicked,
			workers:       p.workers,
			bg:            bg,
		}

		pl, err := newPipeline(args)
		if err != nil {
			close(in)
			return nil, err
		}
		pipelines = append(pipelines, pl)
	}
	p.pipelines = pipelines

	// With autoscaling on, no pipeline spawned runners; the scaler owns the flat worker pool. It
	// starts at the fixed base (num × per-pipeline stage width) clamped into [min, max] and adjusts
	// from there.
	if opts.autoScale != nil {
		// One pipeline is width workers. min/max are pipeline counts; start at num pipelines
		// (clamped) and scale in whole-pipeline (width-worker) steps between [min*width, max*width].
		width := numStages[T](sm) + subStages
		initial := clamp(num, opts.autoScale.min, opts.autoScale.max) * width
		s := &scaler[T]{
			stats:  stats,
			spawn1: pipelines[0].start,
			quit:   make(chan struct{}),
			stop:   make(chan struct{}),
			done:   make(chan struct{}),
			size:   initial,
			ctrl: &ctrl{
				width: width,
				minW:  opts.autoScale.min * width,
				maxW:  opts.autoScale.max * width,
			},
			clk: realClock{},
		}
		s.spawn(initial)
		p.scaler = s
		// The governor is a coordinator: it lives for the whole Pipelines and only ticks, so it runs on
		// the pool rather than holding a worker of its own.
		_ = pool.Submit(bg, s.loop)
	}

	return p, nil
}

// Close closes the ingestion of the Pipeline. No further Submit calls should be made.
// If called more than once Close will panic.
func (p *Pipelines[T]) Close() {
	// Stop the autoscale governor first so it stops issuing quit tokens; then closing p.in exits
	// every worker regardless of how many the governor had spawned.
	if p.scaler != nil {
		close(p.scaler.stop)
	}
	close(p.in)

	_ = p.pool.Submit(p.bg, func() {
		// Every Request has been handed to a RequestGroup drain loop, so no worker still holds one.
		p.wg.Wait()
		// Join the governor before the workers: closing stop only tells it to exit, and a tick already
		// in flight may still be spawning. Once it has returned, the worker set is final.
		if p.scaler != nil {
			<-p.scaler.done
		}
		// p.in is closed and nothing more can be spawned, so every live worker is either exiting or
		// parked on the closed channel. Joining them here means Close's teardown covers the workers
		// themselves and not merely the Requests they carried.
		_ = p.workers.Wait(p.bg)
		close(p.out)
		p.sm.Close()
	})
}

// RequestGroup provides in and out channels to send a group of related data into
// the Pipelines and receive the processed data. This allows multiple callers to
// multiplex onto the same Pipelines. A RequestGroup is created with Pipelines.NewRequestGroup().
type RequestGroup[T any] struct {
	// Name is the name of the RequestGroup. This is used in OTEL tracing only and is not required.
	Name string

	// span is the Open Telemetry span for this Request.
	span span.Span

	// ordered is used to handle ordering of output when the WithOrdered() option is set.
	// If set to nil, the output is not ordered.
	ordered *demux.InOrder[uint64, Request[T]]

	// out is the channel the demuxer will use to send us output.
	out chan Request[T]
	// user is the channel that we give the user to receive output. We do a little
	// processing between receiveing on "out" and sending to "user".
	user chan Request[T]
	// p is the Pipelines object this RequestGroup is tied to.
	p *Pipelines[T]
	// wg counts this group's in-flight Request(s). It is used to know when it is safe to close the
	// output channel.
	wg stdlibsync.WaitGroup
	// id is the ID of the RequestGroup.
	id uint64

	// itemNum is used to track the order of the Request in the RequestGroup.
	itemNum atomic.Uint64

	// startOnce is used to do operations related to telemetry on the first Submit() call.
	startOnce *sync.Once
	// started is when the RequestGroup starts being processed.
	started atomic.Pointer[time.Time]
}

// Close signals that the input is done and will wait for all Request objects to
// finish proceessing, then close the output channel. The owner of the RequestGroup
// is still required to pull all entries out of the RequestGroup via .Out() and until
// that occurs, Close() will not return.
func (r *RequestGroup[T]) Close() {
	r.wg.Wait()

	r.otelEnd()

	r.p.demux.RemoveReceiver(r.id) // This closes the input channel into the Pipelines object

	// If a stage panicked while this group was in flight, re-raise it here — on the caller's
	// goroutine, after every Request has drained — carrying the stack captured at the panic site.
	if info := r.p.panicked.Load(); info != nil {
		panic(*info)
	}
}

// Submit submits a new Request into the Pipelines. A Request with a nil Context will
// cause a panic.
func (r *RequestGroup[T]) Submit(req Request[T]) error {
	if req.Ctx == nil {
		return errors.New("Request.Ctx cannot be nil")
	}

	// If the group isn't named, name it.
	gName := r.Name

	r.startOnce.Do(func() {
		if r.Name == "" {
			gName = "unnamed"
		}

		groupName := fmt.Sprintf("stagedpipe.RequestGroup(%s)", gName)
		req.Ctx, r.span = context.NewSpan(req.Ctx, span.WithName(groupName))

		// Record the time the first request was submitted.
		t := time.Now()
		r.started.CompareAndSwap(nil, &t)
		// Start our OTEL span.
		r.otelStart()
	})

	req.groupNum = r.id
	req.itemNum = r.itemNum.Add(1) - 1 // This must start at 0.
	req.queueTime = time.Now()

	// This let's the Pipelines object know it is receiving a new Request to process.
	r.p.wg.Add(1)
	// This tracks the request in the RequestGroup.
	r.wg.Add(1)

	// Create a child context with a new child span for the request.
	reqName := fmt.Sprintf("stagedpipe.RequestGroup(%s).Request(%d)", gName, req.itemNum)
	ctx, spanner := context.NewSpan(req.Ctx, span.WithName(reqName))
	req.Ctx = ctx
	req.span = spanner

	// With keyed ordering on, enter assigns the partition key and per-key sequence and does the
	// ordered send (sequence order == send order for a key). A cancelled send is resolved inside
	// enter so no successor is stranded; here we only undo the wg accounting.
	if r.p.keyOrder != nil {
		if err := r.p.keyOrder.enter(req.Ctx, r.p.in, req); err != nil {
			r.p.wg.Done()
			r.wg.Done()
			return err
		}
		return nil
	}

	select {
	case <-req.Ctx.Done():
		r.p.wg.Done()
		r.wg.Done()
		return req.Ctx.Err()
	case r.p.in <- req:
	}

	return nil
}

// Out returns a channel to receive Request(s) that have been processed. It is
// unsafe to close the output channel. Instead, use .Close() when all input has
// been sent and the output channel will close once all data has been processed.
// You MUST get all data from Out() until it closes, even if you run into an error.
// Otherwise the pipelines become stuck.
func (r *RequestGroup[T]) Out() chan Request[T] {
	return r.user
}

func (r *RequestGroup[T]) otelStart() {
	if !r.span.IsRecording() {
		return
	}
	// The span timestamps its own events, so we don't attach the start time as an attribute.
	r.span.Event("Started Submit()")
}

func (r *RequestGroup[T]) otelEnd() {
	if !r.span.IsRecording() {
		return
	}
	r.span.Event("RequestGroup finished", attribute.Int64("elapsed_ns", int64(time.Since(*r.started.Load()))))
	r.span.End() // End the RequestGroup span; otherwise it is created but never closed.
}

// NewRequestGroup returns a RequestGroup that can be used to process requests
// in this set of Pipelines.
func (p *Pipelines[T]) NewRequestGroup() *RequestGroup[T] {
	id := p.requestGroupNum.Add(1)
	r := &RequestGroup[T]{
		id:        id,
		out:       make(chan Request[T], 1),
		user:      make(chan Request[T], 1),
		p:         p,
		startOnce: &sync.Once{},
	}
	p.demux.AddReceiver(id, r.out)

	// The drain loop is a coordinator, so it runs on the Pipelines' pool rather than a goroutine of its
	// own. It is submitted on the Pipelines' uncancellable Context: a declined submit would leave the
	// user channel unclosed and every reader of Out() hung, and the loop needs no cancellation of its
	// own since it ends when the demuxer closes r.out.
	if p.ordered { // Output must be returned in the order it was submitted.
		r.ordered = demux.NewInOrder(
			func(r Request[T]) uint64 {
				return r.itemNum
			},
			r.user,
		)
		_ = p.pool.Submit(p.bg, func() {
			defer r.ordered.Close()
			for req := range r.out {
				r.wg.Done()
				r.p.wg.Done()
				req.otelEnd()
				if err := r.ordered.Add(req); err != nil {
					panic(fmt.Sprintf("bug: ordered demuxer: %s", err))
				}
			}
		})
		return r
	}

	// No output order is required.
	_ = p.pool.Submit(p.bg, func() {
		defer close(r.user)
		for req := range r.out {
			r.wg.Done()
			r.p.wg.Done()
			req.otelEnd()
			r.user <- req
		}
	})

	return r
}

// Stats returns stats about all the running Pipelines.
func (p *Pipelines[T]) Stats() Stats {
	return p.stats.toStats()
}

// pipeline processes DBD entries.
type pipeline[T any] struct {
	sm            StateMachine[T]
	stats         *stats
	in            chan Request[T]
	out           chan Request[T]
	name          string
	preProcessors []PreProcesor[T]
	id            int
	concurrency   int
	delayWarning  time.Duration
	ss            bool
	// keyOrder is shared with the parent Pipelines: the same registry serves every worker, since a
	// key's Requests may be processed by different workers. nil when keyed ordering is off.
	keyOrder *keyOrder[T]
	// panicked points at the parent Pipelines' first-panic slot, shared by every worker.
	panicked *atomic.Pointer[PanicError]
	// seenPool is the parent Pipelines' seenStages pool, non-nil only when WithDAG() is set.
	seenPool *sync.Pool[*seenStages]
	// workers is the parent Pipelines' worker join point, shared by every pipeline and the scaler.
	workers *sync.Group
	// bg is the parent Pipelines' uncancellable lifetime Context, used to start workers.
	bg context.Context
}

type pipelineArgs[T any] struct {
	sm            StateMachine[T]
	in            chan Request[T]
	out           chan Request[T]
	stats         *stats
	ss            bool
	name          string
	preProcessors []PreProcesor[T]
	id            int
	num           int
	subStages     int
	delayWarning  time.Duration
	// autoscale, when true, means the Pipelines-level scaler owns worker spawning, so newPipeline
	// starts no runners of its own.
	autoscale bool
	// keyOrder is the shared keyed-ordering registry, or nil when keyed ordering is off.
	keyOrder *keyOrder[T]
	// panicked points at the parent Pipelines' first-panic slot.
	panicked *atomic.Pointer[PanicError]
	// seenPool is the shared seenStages pool, non-nil only when WithDAG() is set.
	seenPool *sync.Pool[*seenStages]
	// workers is the shared worker join point.
	workers *sync.Group
	// bg is the parent Pipelines' uncancellable lifetime Context.
	bg context.Context
}

// newPipeline creates a new Pipeline. A new Pipeline should be created for a new set of related
// requests.
func newPipeline[T any](args pipelineArgs[T]) (*pipeline[T], error) {
	p := &pipeline[T]{
		name:          args.name,
		id:            args.id,
		in:            args.in,
		out:           args.out,
		preProcessors: args.preProcessors,
		stats:         args.stats,
		sm:            args.sm,
		ss:            args.ss,
		delayWarning:  args.delayWarning,
		keyOrder:      args.keyOrder,
		panicked:      args.panicked,
		seenPool:      args.seenPool,
		workers:       args.workers,
		bg:            args.bg,
	}

	p.concurrency = numStages[T](args.sm) + args.subStages

	if p.concurrency == 0 {
		return nil, fmt.Errorf("did not find any Public methods that implement Stages")
	}

	// When autoscaling, the Pipelines-level scaler owns all worker goroutines, so this pipeline
	// spawns none of its own.
	if args.autoscale {
		return p, nil
	}

	for i := 0; i < p.concurrency; i++ {
		p.start(nil, nil)
	}

	return p, nil
}

// start launches one stage worker on the shared Group and returns at once. The Group deliberately has
// no Pool: a worker is a permanently blocked receiver on p.in, not a task, so it would hold a pool
// runner for the life of the Pipelines and never return it — the pool's only benefit, reuse, is
// unreachable. Worse, a worker parks at an ordered-stage barrier while it waits its key's turn
// (execStage), so on a Limited pool enough parked workers deadlock the pool against itself, and on the
// shared default pool they would consume every static runner and degrade every other Submit in the
// process. Running them on the Group's own goroutines costs exactly one goroutine per worker and takes
// no slot from anyone, while still giving Close a join point. Submitted on the uncancellable lifetime
// Context so a cancelled caller Context cannot skip a worker; a worker exits on p.in closing or on a
// quit token from the autoscaler.
func (p *pipeline[T]) start(quit <-chan struct{}, live *atomic.Int64) {
	p.workers.Go(p.bg, func(context.Context) error {
		p.runner(quit, live)
		return nil
	})
}

// runner processes requests until either p.in is closed (Close) or a quit token is received (the
// autoscaler removing this worker). quit is nil in fixed mode, where its select case never fires so
// the loop behaves exactly like ranging p.in. live, when non-nil, tracks the live worker count for
// the autoscaler and is decremented on every exit path.
func (p *pipeline[T]) runner(quit <-chan struct{}, live *atomic.Int64) {
	if live != nil {
		defer live.Add(-1)
	}
	id := fmt.Sprintf("%s-%d", p.name, p.id)
	var tick *time.Ticker
	if p.delayWarning != 0 {
		tick = time.NewTicker(p.delayWarning)
		// Stop on every exit path. Under autoscale, workers are removed continuously, so a leaked
		// ticker would keep firing and accumulate for the life of the process.
		defer tick.Stop()
	}
	for {
		select {
		case r, ok := <-p.in:
			if !ok {
				return
			}
			r = r.otelStart()
			r = p.processReq(r)
			p.calcExitStats(r)
			if p.delayWarning != 0 {
				for {
					tick.Reset(p.delayWarning)
					select {
					case p.out <- r:
					case <-tick.C:
						log.Printf("pipeline(%s) is having output delays exceeding %v", id, p.delayWarning)
						continue
					}
					break
				}
			} else {
				p.out <- r
			}
		case <-quit:
			return
		}
	}
}

// processReq processes a single request through the pipeline. The return is named so the
// deferred cleanup below clears seenStages on the value actually returned to the caller; an
// unnamed return would copy r out before the defer ran, leaking a pooled object to the caller.
func (p *pipeline[T]) processReq(r Request[T]) (out Request[T]) {
	// Stat colllection.
	r.ingestTime = time.Now()
	queuedTime := time.Since(r.queueTime)
	if p.ss {
		r.seenStages = p.seenPool.Get(r.Ctx).reset()
		defer func() {
			p.seenPool.Put(out.Ctx, out.seenStages)
			out.seenStages = nil
		}()
	}

	// Keyed ordering: on every exit path resolve the barriers this Request never cleared and drop its
	// in-flight hold on the key. Deferred so it covers a mid-pipeline error return, a branch that
	// skips a barrier, and normal completion alike.
	if p.keyOrder != nil && r.ks != nil {
		defer func() {
			p.keyOrder.exit(out)
		}()
	}

	p.stats.running.Add(1)
	setMin(&p.stats.ingestStats.min, int64(queuedTime))
	setMax(&p.stats.ingestStats.max, int64(queuedTime))
	p.stats.ingestStats.avgTotal.Add(int64(queuedTime))

	// Loop through all our states starting with p.sms[0].Start until we
	// get either an error or the Request.Next == nil
	// which indicates that the statemachine is done processing.
	stage := p.sm.Start
	for {
		r = p.execStage(r, stage)
		if r.Err != nil {
			return r
		}
		stage = r.Next
		r.Next = nil

		if stage == nil {
			return r
		}
	}
}

// execStage executes a single stage of the pipeline and all preProcessors. When the Request is
// recording it also creates a per-stage OTEL span. The return is named so the deferred span/ctx
// restore below applies to the value the caller receives (an unnamed return would copy out first).
func (p *pipeline[T]) execStage(r Request[T], stage Stage[T]) (out Request[T]) {
	recording := r.span.IsRecording()

	// stageName is only needed for OTEL span/event naming, so it is computed only when recording.
	// Cyclic detection does not need it: it compares entry PCs and resolves names only if it finds a
	// cycle, which keeps methodName's reflection and cache lookup off the hot path.
	var stageName string
	if recording {
		stageName = methodName(stage)
	}

	// All OTEL work is gated behind recording: when tracing is off this whole block is skipped,
	// avoiding a per-stage NewSpan, the event attribute allocations, and two defers.
	if recording {
		parentCtx := r.Ctx
		parentSpan := r.span
		r.Ctx, r.span = context.NewSpan(r.Ctx, span.WithName(stageName))
		// The span timestamps its own events, so we record only the phase.
		r.span.Event(stageName, attribute.String("phase", "start"))
		defer func() {
			out.span.Event(stageName, attribute.String("phase", "end"))
			out.span.End() // End the per-stage span before restoring the parent onto out.
			out.Ctx = parentCtx
			out.span = parentSpan
		}()
	}

	// If the context has been cancelled, stop processing.
	if r.Ctx.Err() != nil {
		r.Err = r.Ctx.Err()
		return r
	}

	// A stage has panicked somewhere in the pipeline: stop running stage bodies and fast-drain this
	// Request with a torn-down error so it still flows out (keeping the wait-group balanced) while no
	// further user code runs. The origin re-raises from RequestGroup.Close.
	if p.panicked.Load() != nil {
		r.Err = ErrTornDown
		return r
	}

	// WithStopKeyOnErr: an earlier Request for this key failed, so skip this one without running any
	// stage. Checked here (every stage) so a Request that has not yet reached a barrier — or is on a
	// non-ordered stage — is skipped too, not only those released while parked at a barrier below.
	if p.keyOrder != nil && p.keyOrder.stopKeyOnErr && r.ks != nil && r.ks.poisonedBefore(r.seq) {
		r.Err = ErrKeyFailed
		return r
	}

	if r.seenStages != nil {
		if r.seenStages.seen(reflect.ValueOf(stage).Pointer()) {
			r.Err = Error{Type: cyclicErr, Msg: r.seenStages.callTrace()}
			return r
		}
	}

	// Keyed ordering: if this stage is a barrier for this Request's key, block until it is this
	// sequence's turn before running the stage body. A failed wait (ctx cancelled or teardown) means
	// the Request does not enter the stage; its sequence is left for the exit hook to resolve, so a
	// successor is not stranded. barrierBit computes the stage PC, so it is reached only with keyed
	// ordering on.
	bit, gated := 0, false
	if p.keyOrder != nil && r.ks != nil {
		if b, ok := p.keyOrder.barrierBit(stage, r.clearedMask); ok {
			bit, gated = b, true
			// Record the wait: parked is a gauge of workers blocked at a barrier right now, and the
			// wait duration folds into the barrier min/avg/max.
			p.stats.parked.Add(1)
			waitStart := time.Now()
			err := r.ks.seqs[b].Wait(r.Ctx, r.seq)
			p.stats.recordBarrierWait(time.Since(waitStart))
			p.stats.parked.Add(-1)
			if err != nil {
				r.Err = classifyWaitErr(err)
				return r
			}
			// The predecessor we waited on may have failed and poisoned the key while we were parked.
			if p.keyOrder.stopKeyOnErr && r.ks.poisonedBefore(r.seq) {
				r.Err = ErrKeyFailed
				return r
			}
		}
	}

	for _, pp := range p.preProcessors {
		r = pp(r)
		if r.Err != nil {
			return r
		}
	}
	r = p.callStage(stage, r)

	// Release the barrier as soon as the stage body has run, so the key's next Request may enter this
	// stage while this one moves on. A stage that set r.Err still ran, so it still releases here; a
	// preProcessor error above returns first and leaves the barrier for the exit hook to resolve.
	if gated {
		// If the stage failed, poison the key before releasing the barrier, so the next Request waiting
		// on it wakes to a poisoned key and is skipped rather than proceeding (WithStopKeyOnErr).
		if r.Err != nil && p.keyOrder.stopKeyOnErr {
			r.ks.poison(r.seq)
		}
		r.ks.seqs[bit].Done(r.seq)
		r.clearedMask |= uint64(1) << uint(bit)
	}
	return r
}

// onPanic records the first stage panic and starts teardown. First panic wins; later panics during
// teardown are dropped. On the first, every live barrier waiter is woken (abortAll) so no worker is
// left blocked, and thereafter execStage fast-drains the remaining Requests.
func (p *pipeline[T]) onPanic(rec any, stack []byte) {
	if p.panicked.CompareAndSwap(nil, &PanicError{Value: rec, Stack: stack}) {
		if p.keyOrder != nil {
			p.keyOrder.abortAll()
		}
	}
}

// callStage runs a single stage body under a recover so a panic in user code does not crash the
// process. On a panic it records the origin (onPanic) and returns the Request carrying ErrTornDown;
// the real panic is re-raised later from RequestGroup.Close. The recover runs before processReq's
// exit hook, so abortAll happens before any barrier is released and no waiter slips through.
func (p *pipeline[T]) callStage(stage Stage[T], r Request[T]) (out Request[T]) {
	defer func() {
		if rec := recover(); rec != nil {
			p.onPanic(rec, debug.Stack())
			out = r
			out.Err = ErrTornDown
		}
	}()
	return stage(r)
}

// calcExitStats calculates the final stats when a Request exits the Pipeline.
func (p *pipeline[T]) calcExitStats(r Request[T]) {
	runTime := time.Since(r.ingestTime)

	p.stats.running.Add(-1)
	p.stats.completed.Add(1)

	setMin(&p.stats.min, int64(runTime))
	setMax(&p.stats.max, int64(runTime))
	p.stats.avgTotal.Add(int64(runTime))
}

func numStages[T any](sm any) int {
	var sig Stage[T]
	count := 0
	for range method.MatchesSignature(reflect.ValueOf(sm), reflect.ValueOf(sig)) {
		count++
	}
	return count
}

// methodNameCache memoizes methodName by a function's entry PC so the reflection and FuncForPC
// lookup run once per distinct stage instead of once per stage per request. Distinct funcs (and
// method values) have distinct, stable entry PCs, so the PC is a valid key. Reads are lock-free
// once warm.
var methodNameCache sync.Map // map[uintptr]string

// methodName takes a function or a method and returns its name.
func methodName(method any) string {
	if method == nil {
		return "<nil>"
	}
	valueOf := reflect.ValueOf(method)
	if valueOf.Kind() != reflect.Func {
		return "<not a function>"
	}
	return funcName(valueOf.Pointer())
}

// funcName resolves a function's entry PC to its name, through methodNameCache. A PC the runtime
// cannot resolve yields a placeholder rather than a panic, since the caller may be building the
// message for an error that is already in flight.
func funcName(pc uintptr) string {
	if v, ok := methodNameCache.Load(pc); ok {
		return v.(string)
	}
	name := "<unknown>"
	if f := runtime.FuncForPC(pc); f != nil {
		name = strings.TrimSuffix(strings.TrimSuffix(f.Name(), "-fm"), "[...]")
	}
	methodNameCache.Store(pc, name)
	return name
}
