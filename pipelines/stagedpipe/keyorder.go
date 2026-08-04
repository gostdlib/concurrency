package stagedpipe

import (
	"fmt"
	"reflect"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/errors"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/base/values/immutable"
	"github.com/gostdlib/concurrency/internal/sequencer"
)

// maxOrderedStages caps how many stages a single Pipelines may mark ordered. A Request tracks the
// barriers it has cleared in a uint64 bitset (clearedMask), so the limit is 64. No real pipeline
// approaches it; the cap exists to keep the bitset representation valid.
const maxOrderedStages = 64

// defaultAdmitDepth is the per-key admission depth applied when WithKey is set but WithAdmissionDepth
// is not. A small finite bound keeps a hot key from parking many workers under a skewed key
// distribution (the benchmark shows depth 2–4 beats unbounded there) while barely affecting a uniform
// one. Callers who want the old unbounded behavior pass WithAdmissionDepth(0).
const defaultAdmitDepth = 4

// ErrPermanent marks an error that cannot succeed on retry, such as a configuration mistake. Check
// for it with errors.Is(err, ErrPermanent). It is the same sentinel as exponential.ErrPermanent,
// re-exported here as feeder and foreach do so callers need not import the retry package.
var ErrPermanent = exponential.ErrPermanent

// ErrKeyFailed is set on a Request that a Pipelines skipped without running any further stage because
// an earlier Request for the same key failed and WithStopKeyOnErr was set. It is permanent (a skipped
// Request will not succeed on retry) so match it with errors.Is(err, ErrKeyFailed). The poison is
// cohort-scoped: it holds only while the key stays in flight, so once the key fully drains a later
// burst starts fresh. Requests for other keys are unaffected.
var ErrKeyFailed = fmt.Errorf("stagedpipe: an earlier Request for this key failed (WithStopKeyOnErr): %w", ErrPermanent)

// WithKey enables keyed ordering: keyFunc extracts a partition key from each Request's Data, and
// Requests sharing a key are admitted through the stages marked by WithOrderedStages in submit
// order. Different keys run in parallel. Keys are strings — stringify whatever identifies the
// partition (account, tenant, entity id). WithKey and WithOrderedStages must be set together;
// either alone is a configuration error from New.
func WithKey[T any](keyFunc func(T) string) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		if keyFunc == nil {
			return o, fmt.Errorf("stagedpipe.WithKey: keyFunc cannot be nil: %w", ErrPermanent)
		}
		o.keyFunc = keyFunc
		return o, nil
	}
}

// WithOrderedStages marks the stages at which keyed ordering is enforced. A stage is named by its
// method value (sm.ApplyBalance), not a string, so a rename or typo is a compile error rather than
// ordering that silently does not happen. For each marked stage a Request sharing a key does not
// enter that stage until the same key's previous Request has cleared it; unmarked stages run fully
// in parallel. Requires WithKey. At most maxOrderedStages stages may be marked.
func WithOrderedStages[T any](stages ...Stage[T]) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		if len(stages) == 0 {
			return o, fmt.Errorf("stagedpipe.WithOrderedStages: at least one stage is required: %w", ErrPermanent)
		}
		erased := make([]any, 0, len(stages))
		for i, s := range stages {
			if s == nil {
				return o, fmt.Errorf("stagedpipe.WithOrderedStages: stage at index %d is nil: %w", i, ErrPermanent)
			}
			erased = append(erased, s)
		}
		o.orderedStages = erased
		return o, nil
	}
}

// WithStopKeyOnErr changes the per-key error behavior from the default. By default an error on one
// Request affects only that Request and the key's later Requests proceed. With this option the key
// is poisoned on the first error — any non-nil Request.Err, including a context cancellation, since
// for a causally dependent stream a gap invalidates what follows — and every later in-flight Request
// for that key exits with ErrKeyFailed without running any stage. The poison is cohort-scoped: it
// lasts only while the key stays in flight, so once the key fully drains a later burst starts fresh.
// Other keys are unaffected. Requires WithKey.
func WithStopKeyOnErr() Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		o.stopKeyOnErr = true
		return o, nil
	}
}

// WithAdmissionDepth bounds how many Requests for a single key are in flight at once, capping the
// pipeline workers a hot key can hold parked at its barriers. Once a key has depth Requests in
// flight, its next Request waits at Submit until one of them completes. depth 1 admits a key's
// Requests one at a time (no intra-key pipelining); a higher depth allows that many at once, trading
// parked workers for throughput; depth 0 is unbounded. depth must be >= 0. When WithKey is set but
// this option is not, the default is 4, a small bound that helps skewed workloads and is near-neutral
// on uniform ones. Different keys are independent, so this never blocks one key behind another.
// Requires WithKey.
func WithAdmissionDepth(depth int) Option {
	return func(o pipelinesOptions) (pipelinesOptions, error) {
		if depth < 0 {
			return o, fmt.Errorf("stagedpipe.WithAdmissionDepth: depth must be >= 0, got %d: %w", depth, ErrPermanent)
		}
		o.admitDepth = depth
		o.admitDepthSet = true
		return o, nil
	}
}

// keyState is the live ordering state for one partition key. It holds one sequencer per barrier,
// all sharing the key's dense sequence space, plus the counters that drive sequence assignment and
// reaping. A keyState is created on the first Submit for a key and reaped once its last in-flight
// Request exits.
type keyState struct {
	// key is the partition key, kept so exit/abandon can delete this state from the registry.
	key string
	// seqs holds one sequencer per barrier, indexed by the barrier's bit position (keyOrder.barrierIdx
	// value). A Request takes its turn at barrier b through seqs[b].
	seqs []*sequencer.Sequencer

	// submitMu is held across the ordered send in enter so that, for this key, sequence-assignment
	// order equals p.in send order — the invariant the leader-admitted-first deadlock-freedom
	// argument relies on. It guards nextSeq.
	submitMu sync.Mutex
	// nextSeq is the next dense, 0-based sequence to hand out. Guarded by submitMu.
	nextSeq uint64

	// admit bounds the Requests in flight for this key (WithAdmissionDepth); nil when unbounded. It
	// is acquired under submitMu in enter (so admission order is sequence order) and released in exit.
	admit *sync.Semaphore

	// inflight counts Requests assigned a sequence for this key that have not yet exited. Guarded by
	// keyOrder.mu. The keyState is reaped when it returns to zero.
	inflight int

	// poisonMu guards poisoned and poisonSeq (WithStopKeyOnErr).
	poisonMu sync.Mutex
	// poisoned is set once a Request for this key has failed. poisonSeq is the lowest failing
	// sequence: every Request whose sequence is greater is then skipped with ErrKeyFailed, while any
	// lower sequence (already ahead in the pipeline) still completes.
	poisoned  bool
	poisonSeq uint64
}

// poison records that the Request at seq failed, so later Requests for the key are skipped. It keeps
// the lowest failing sequence, as an out-of-order failure at a non-ordered stage must not poison a
// lower sequence that is already further along.
func (ks *keyState) poison(seq uint64) {
	ks.poisonMu.Lock()
	if !ks.poisoned || seq < ks.poisonSeq {
		ks.poisoned = true
		ks.poisonSeq = seq
	}
	ks.poisonMu.Unlock()
}

// poisonedBefore reports whether seq comes after a failed Request for the key, so it should be
// skipped with ErrKeyFailed. The failing Request itself (seq == poisonSeq) is not skipped — it keeps
// its own error.
func (ks *keyState) poisonedBefore(seq uint64) bool {
	ks.poisonMu.Lock()
	defer ks.poisonMu.Unlock()
	return ks.poisoned && seq > ks.poisonSeq
}

// keyOrder holds the resolved keyed-ordering configuration for a Pipelines plus its live per-key
// registry. keyFunc and barrierIdx are fixed at construction; keys is mutated as Requests enter and
// exit, guarded by mu.
type keyOrder[T any] struct {
	// keyFunc extracts a Request's partition key from its Data.
	keyFunc func(T) string
	// barrierIdx maps an ordered stage's entry PC to its 0-based bit position in Request.clearedMask.
	// A stage whose PC is absent is not a barrier and runs unordered. It is immutable: newKeyOrder
	// builds it once and every worker then reads it concurrently for the life of the Pipelines.
	barrierIdx immutable.Map[uintptr, int]
	// stopKeyOnErr poisons a key on its first Request error when true (WithStopKeyOnErr).
	stopKeyOnErr bool
	// admitDepth bounds the Requests in flight per key (WithAdmissionDepth). 0 means unbounded; a
	// positive value gives each keyState an admission semaphore of that capacity.
	admitDepth int
	// stats is the Pipelines' shared metrics, set by New. enter records admission-wait here; the
	// per-stage parked and barrier-wait metrics are recorded by the workers via pipeline.stats.
	stats *stats

	// mu guards keys. It is taken only on the entry and exit of a Request (acquire/exit), never per
	// stage — a Request carries its *keyState directly, so the ordering hot path takes no registry
	// lock.
	mu sync.Mutex
	// keys is the live keyState per partition key. An entry appears on a key's first in-flight
	// Request and is deleted when its last one exits.
	keys map[string]*keyState
}

// newKeyOrder resolves the type-erased ordered stages against T and builds the barrier index. Each
// stage resolves to its entry PC via reflection; the same PC listed twice is deduplicated. It
// errors (permanently — a configuration mistake cannot succeed on retry) if a stage is not a
// Stage[T] or if more than maxOrderedStages distinct stages are marked.
func newKeyOrder[T any](keyFunc func(T) string, stages []any, stopKeyOnErr bool, admitDepth int) (*keyOrder[T], error) {
	barrierIdx := map[uintptr]int{}
	for i, s := range stages {
		stage, ok := s.(Stage[T])
		if !ok {
			return nil, fmt.Errorf("stagedpipe.WithOrderedStages: stage %d is not a Stage[T] (got %T): %w", i, s, ErrPermanent)
		}
		pc := reflect.ValueOf(stage).Pointer()
		if _, dup := barrierIdx[pc]; dup {
			continue
		}
		barrierIdx[pc] = len(barrierIdx)
	}
	if len(barrierIdx) > maxOrderedStages {
		return nil, fmt.Errorf("stagedpipe: %d ordered stages over limit %d: %w", len(barrierIdx), maxOrderedStages, ErrPermanent)
	}
	return &keyOrder[T]{
		keyFunc:      keyFunc,
		barrierIdx:   immutable.NewMap(barrierIdx),
		stopKeyOnErr: stopKeyOnErr,
		admitDepth:   admitDepth,
		keys:         map[string]*keyState{},
	}, nil
}

// acquire returns the keyState for key, creating it (with one sequencer per barrier) on first use,
// and records one in-flight Request against it. The caller must balance every acquire with an exit
// so the keyState is eventually reaped. The registry lock is held only for the map lookup and the
// counter bump, never across a send or a stage.
func (ko *keyOrder[T]) acquire(key string) *keyState {
	ko.mu.Lock()
	defer ko.mu.Unlock()

	ks, ok := ko.keys[key]
	if !ok {
		ks = &keyState{key: key, seqs: make([]*sequencer.Sequencer, ko.barrierIdx.Len())}
		for i := range ks.seqs {
			ks.seqs[i] = sequencer.New()
		}
		if ko.admitDepth > 0 {
			s := sync.NewSemaphore(ko.admitDepth)
			ks.admit = &s
		}
		ko.keys[key] = ks
	}
	ks.inflight++
	return ks
}

// enter assigns req its partition key and dense per-key sequence and sends it into in. The sequence
// assignment and the send happen under the key's submitMu so that, for a single key, sequence order
// equals send order — every earlier sequence is on the channel before a later one, which is what
// keeps the sequence leader (never blocked) admitted first. On a cancelled send the Request never
// enters the pipeline, so its sequence is resolved at every barrier and its in-flight hold dropped
// (via exit) — otherwise a successor would wait forever for a turn that never comes.
func (ko *keyOrder[T]) enter(ctx context.Context, in chan Request[T], req Request[T]) error {
	ks := ko.acquire(ko.keyFunc(req.Data))

	ks.submitMu.Lock()
	req.ks = ks
	req.seq = ks.nextSeq
	ks.nextSeq++

	// Bound the Requests in flight for this key. Acquiring under submitMu keeps admission order equal
	// to sequence order, so the sequence leader is still admitted first. A cancelled acquire never
	// entered, so exit resolves its sequence without releasing a slot it does not hold.
	if ks.admit != nil {
		ko.stats.admissionWait.Add(1)
		ok := ks.admit.AcquireContext(ctx)
		ko.stats.admissionWait.Add(-1)
		if !ok {
			ks.submitMu.Unlock()
			ko.exit(req)
			return ctx.Err()
		}
		req.admitted = true
	}

	select {
	case in <- req:
		ks.submitMu.Unlock()
		return nil
	case <-ctx.Done():
		ks.submitMu.Unlock()
		ko.exit(req)
		return ctx.Err()
	}
}

// exit resolves the ordering state a Request leaves behind: every barrier it did not clear is
// resolved at its sequence (so successors branched-past or errored-before that stage are released),
// and its in-flight hold is dropped. When a key's last in-flight Request exits, its keyState is
// reaped — every sequence has resolved at every barrier, so nothing is lost. exit runs once per
// Request, from the processReq exit hook or from enter's cancelled-send path.
func (ko *keyOrder[T]) exit(req Request[T]) {
	ks := req.ks

	// With WithStopKeyOnErr, a failed Request poisons the key before its barriers are released, so a
	// successor woken by the exit-clear below sees the poison and is skipped rather than proceeding.
	if ko.stopKeyOnErr && req.Err != nil {
		ks.poison(req.seq)
	}

	// Resolve the barriers this Request never cleared. clearedMask has a bit set for each barrier it
	// resolved in-stage; the rest are stages it branched past, errored before, or (on the cancelled
	// send path) never reached at all.
	for _, bit := range ko.barrierIdx.All() {
		if req.clearedMask&(uint64(1)<<uint(bit)) == 0 {
			ks.seqs[bit].Done(req.seq)
		}
	}

	// Release the admission slot this Request held (WithAdmissionDepth), letting the key's next
	// waiting Request in. Done before the reap so a slot is never freed on a keyState about to be
	// deleted while a successor still waits on it.
	if req.admitted {
		ks.admit.Release()
	}

	ko.mu.Lock()
	ks.inflight--
	if ks.inflight == 0 {
		delete(ko.keys, ks.key)
	}
	ko.mu.Unlock()
}

// abortAll aborts every sequencer of every live key, waking each parked barrier waiter with
// ErrAborted. It is called once when a stage panics: no waiter must stay blocked while the pipeline
// tears down. After a sequencer is aborted its Wait returns ErrAborted and its Done is a no-op, so
// the exit hooks that still run during teardown neither block nor double-resolve.
func (ko *keyOrder[T]) abortAll() {
	ko.mu.Lock()
	defer ko.mu.Unlock()
	for _, ks := range ko.keys {
		for _, s := range ks.seqs {
			s.Abort()
		}
	}
}

// classifyWaitErr maps a failed barrier Wait to the error surfaced on the Request. A cancelled ctx
// is kept raw so errors.Is(err, context.Canceled) still holds for the caller; a teardown
// (sequencer.ErrAborted) becomes ErrTornDown — the same permanent sentinel the fast-drained Requests
// carry — so callers see one torn-down identity and the internal sequencer sentinel does not leak.
func classifyWaitErr(err error) error {
	if errors.Is(err, sequencer.ErrAborted) {
		return ErrTornDown
	}
	return err
}

// barrierBit reports the barrier bit for stage if it is an ordered stage this Request has not yet
// cleared, so keyed ordering must gate it. It returns ok == false for an unmarked stage or a barrier
// already cleared (a stage re-entered in a loop — at most once per Request). It is the only place
// the ordering hot path computes a stage PC, and only when keyed ordering is on.
func (ko *keyOrder[T]) barrierBit(stage Stage[T], clearedMask uint64) (bit int, ok bool) {
	b, marked := ko.barrierIdx.Get(reflect.ValueOf(stage).Pointer())
	if !marked {
		return 0, false
	}
	if clearedMask&(uint64(1)<<uint(b)) != 0 {
		return 0, false
	}
	return b, true
}
