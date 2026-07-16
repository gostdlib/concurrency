package broadcast

import (
	"iter"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"

	"github.com/kylelemons/godebug/pretty"
)

func TestSubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		preSends       []int
		subscribers    int
		sends          []int
		postCloseSends []int
		want           []int
	}{
		{
			name:        "Success: subscriber receives the sent value",
			subscribers: 1,
			sends:       []int{42},
			want:        []int{42},
		},
		{
			name:        "Success: multiple subscribers receive every value in order",
			subscribers: 2,
			sends:       []int{1, 2, 3},
			want:        []int{1, 2, 3},
		},
		{
			name:        "Success: subscriber receives only the values sent after it subscribed",
			preSends:    []int{1},
			subscribers: 1,
			sends:       []int{2},
			want:        []int{2},
		},
		{
			name:        "Success: no values sent means an empty subscription",
			subscribers: 1,
			want:        []int{},
		},
		{
			name:        "Success: a value sent when no one has subscribed is dropped",
			preSends:    []int{1},
			subscribers: 1,
			want:        []int{},
		},
		{
			name:           "Success: a value sent after Close is dropped",
			subscribers:    1,
			postCloseSends: []int{2},
			want:           []int{},
		},
	}

	for _, test := range tests {
		ctx := t.Context()

		v := &Value[int]{}
		for _, n := range test.preSends {
			v.Send(ctx, n)
		}

		seqs := make([]iter.Seq[int], 0, test.subscribers)
		for i := 0; i < test.subscribers; i++ {
			seqs = append(seqs, v.Subscribe(ctx))
		}

		for _, n := range test.sends {
			v.Send(ctx, n)
		}
		v.Close(ctx)

		for _, n := range test.postCloseSends {
			v.Send(ctx, n)
		}

		for i, seq := range seqs {
			got := []int{}
			for n := range seq {
				got = append(got, n)
			}
			if diff := pretty.Compare(test.want, got); diff != "" {
				t.Errorf("TestSubscribe(%s): subscriber %d: -want/+got:\n%s", test.name, i, diff)
			}
		}
	}
}

// TestSubscribeThenSend pins the invariant that a value sent immediately after Subscribe() returns is seen by
// that subscriber, even though the iterator has not started running yet.
func TestSubscribeThenSend(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	for i := 0; i < 100; i++ {
		v := &Value[int]{}
		seq := v.Subscribe(ctx)

		g := sync.Group{}
		g.Go(ctx, func(ctx context.Context) error {
			v.Send(ctx, 1)
			v.Close(ctx)
			return nil
		})

		got := []int{}
		for n := range seq {
			got = append(got, n)
		}
		if err := g.Wait(ctx); err != nil {
			t.Fatalf("TestSubscribeThenSend(iteration %d): got err == %s, want err == nil", i, err)
		}

		if diff := pretty.Compare([]int{1}, got); diff != "" {
			t.Fatalf("TestSubscribeThenSend(iteration %d): -want/+got:\n%s", i, diff)
		}
	}
}

// TestSubscribeRangedTwice checks that ranging a subscription a second time yields nothing and, more
// importantly, does not corrupt the subscriber counts that Send() uses to decide it can drop a value.
func TestSubscribeRangedTwice(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}
	seq := v.Subscribe(ctx)
	v.Send(ctx, 1)
	v.Close(ctx)

	got := []int{}
	for n := range seq {
		got = append(got, n)
	}
	for n := range seq {
		got = append(got, n)
	}

	if diff := pretty.Compare([]int{1}, got); diff != "" {
		t.Errorf("TestSubscribeRangedTwice: -want/+got:\n%s", diff)
	}

	if holders := v.holders.Load(); holders != 0 {
		t.Errorf("TestSubscribeRangedTwice: got holders == %d, want holders == 0", holders)
	}
	if subs := v.subs.Load(); subs != 0 {
		t.Errorf("TestSubscribeRangedTwice: got subs == %d, want subs == 0", subs)
	}
}

// TestSubscribeAbandoned checks that a subscription that is never ranged over gives its holder slot back when
// its Context is canceled, so an abandoned subscription cannot pin Send() into storing values forever.
func TestSubscribeAbandoned(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	subCtx, cancel := context.WithCancel(ctx)

	v := &Value[int]{}
	_ = v.Subscribe(subCtx)

	if holders := v.holders.Load(); holders != 1 {
		t.Fatalf("TestSubscribeAbandoned: got holders == %d before cancel, want holders == 1", holders)
	}

	cancel()

	// AfterFunc releases the slot on its own goroutine, so give it a moment to land.
	for i := 0; i < 100 && v.holders.Load() != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}

	if holders := v.holders.Load(); holders != 0 {
		t.Errorf("TestSubscribeAbandoned: got holders == %d after cancel, want holders == 0", holders)
	}

	// The Context is done with this subscription, so its record has to go as well as its slot. Only Close()
	// drains what is left in unstarted, so a Value that is never closed would otherwise grow a record for
	// every subscription that was abandoned this way.
	v.mu.Lock()
	unstarted := len(v.unstarted)
	v.mu.Unlock()

	if unstarted != 0 {
		t.Errorf("TestSubscribeAbandoned: got len(unstarted) == %d after cancel, want 0", unstarted)
	}
}

func TestSubscribeCancel(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	subCtx, cancel := context.WithCancel(ctx)

	v := &Value[int]{}
	seq := v.Subscribe(subCtx)

	done := make(chan struct{})
	g := sync.Group{}
	// The Group runs on ctx, not subCtx: a Group does not run a function whose Context is already canceled.
	g.Go(ctx, func(context.Context) error {
		defer close(done)
		for range seq {
		}
		return nil
	})

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("TestSubscribeCancel: timed out waiting for the subscription to end after cancel")
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestSubscribeCancel: got err == %s, want err == nil", err)
	}
}

// TestSendNonBlocking checks that a subscriber that never advances its iterator cannot hold up Send().
func TestSendNonBlocking(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}
	_ = v.Subscribe(ctx)

	done := make(chan struct{})
	g := sync.Group{}
	g.Go(ctx, func(ctx context.Context) error {
		defer close(done)
		for i := 0; i < 1000; i++ {
			v.Send(ctx, i)
		}
		return nil
	})

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("TestSendNonBlocking: Send() blocked on a subscriber that was not draining")
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestSendNonBlocking: got err == %s, want err == nil", err)
	}
}

// TestClose checks that Close() drains the values already sent and then releases a subscriber whose Context
// is never canceled, which is the only way a producer can shut its subscribers down.
func TestClose(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}
	seq := v.Subscribe(ctx)

	for _, n := range []int{1, 2, 3} {
		v.Send(ctx, n)
	}
	v.Close(ctx)
	v.Close(ctx) // Must be safe to call more than once.

	got := []int{}
	done := make(chan struct{})
	g := sync.Group{}
	g.Go(ctx, func(ctx context.Context) error {
		defer close(done)
		for n := range seq {
			got = append(got, n)
		}
		return nil
	})

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("TestClose: timed out waiting for the subscription to end after Close()")
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestClose: got err == %s, want err == nil", err)
	}

	if diff := pretty.Compare([]int{1, 2, 3}, got); diff != "" {
		t.Errorf("TestClose: -want/+got:\n%s", diff)
	}
}

// TestCloseStopsCallbacks checks that Close() hands back the callback Subscribe() puts on a subscriber's
// Context. A subscription that is never ranged gives its holder slot back through that callback, and only
// Close() can reach it, so a Close() that leaves it there strands the callback, this Value and everything it
// holds on a Context that may never be canceled.
func TestCloseStopsCallbacks(t *testing.T) {
	t.Parallel()

	v := &Value[int]{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	v.Subscribe(ctx) // Never ranged, so only the callback on ctx would ever give the slot back.

	v.mu.Lock()
	var h *holder
	for k := range v.unstarted {
		h = k
	}
	v.mu.Unlock()
	if h == nil {
		t.Fatalf("TestCloseStopsCallbacks: Subscribe() did not record the un-ranged subscription")
	}

	// ctx is still live, so the slot coming back is what says Close() reclaimed the callback.
	v.Close(ctx)

	if got := v.holders.Load(); got != 0 {
		t.Errorf("TestCloseStopsCallbacks: got holders == %d after Close(), want 0: Close() left its callback on the Context", got)
	}

	// stop() only reports true if it was the call that took the callback off the Context, so a false here is
	// what says Close() already did.
	if h.stop() {
		t.Errorf("TestCloseStopsCallbacks: got stop() == true after Close(), want false: Close() left its callback on the Context")
	}

	v.mu.Lock()
	unstarted := len(v.unstarted)
	v.mu.Unlock()

	if unstarted != 0 {
		t.Errorf("TestCloseStopsCallbacks: got len(unstarted) == %d after Close(), want 0", unstarted)
	}

	// A ctx that cancels after Close() runs release() anyway, so do here what that stale callback does and
	// check the slot does not come back a second time. The CAS in release() is the only thing stopping that:
	// without it holders would go negative, which permanently disables Send()'s no-subscriber drop.
	h.release()

	if got := v.holders.Load(); got != 0 {
		t.Errorf("TestCloseStopsCallbacks: a stale release() after Close() drove holders to %d, want 0", got)
	}
}

// TestSubscribeReleasesConsumed checks that a subscriber that is keeping up does not pin the values it has
// already consumed. The chain head is only reachable from the iterator, so holding it would retain every value
// ever sent to the subscription, not just the ones a slow subscriber has yet to read.
func TestSubscribeReleasesConsumed(t *testing.T) {
	t.Parallel()

	const sends = 30

	ctx := t.Context()

	v := &Value[*[]byte]{}
	next, stop := iter.Pull(v.Subscribe(ctx))
	defer stop()

	collected := atomic.Int64{}
	for i := 0; i < sends; i++ {
		payload := new([]byte)
		*payload = make([]byte, 64*1024)
		runtime.AddCleanup(payload, func(struct{}) { collected.Add(1) }, struct{}{})

		v.Send(ctx, payload)
		payload = nil

		got, ok := next()
		if !ok {
			t.Fatalf("TestSubscribeReleasesConsumed: subscription ended early at send %d", i)
		}
		got = nil
		_ = got
	}

	// The iterator is parked inside yield holding the value it handed over last, so that one stays reachable.
	want := int64(sends - 1)
	for i := 0; i < 100 && collected.Load() < want; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}

	if got := collected.Load(); got < want {
		t.Errorf("TestSubscribeReleasesConsumed: got %d of %d consumed values collected, want at least %d", got, sends, want)
	}
}

// TestMetricsNoDrift churns subscribers against concurrent producers and checks that pending returns to 0 once
// everyone has stopped. An UpDownCounter that drifts never comes back, so any skew here is permanent.
func TestMetricsNoDrift(t *testing.T) {
	t.Parallel()

	const (
		producers   = 4
		perProducer = 3000
		churners    = 8
		rounds      = 500
		steadySubs  = 16
	)

	ctx := t.Context()
	reader := metricReader(t)

	// The Name is what turns metrics on. Without it there is nothing to drift and this test proves nothing.
	v := &Value[int]{Name: "test"}

	// Steady subscribers keep Send() past its drop fast path for the whole run, so every send does real work.
	steady := sync.Group{}
	for i := 0; i < steadySubs; i++ {
		seq := v.Subscribe(ctx)
		steady.Go(ctx, func(ctx context.Context) error {
			for range seq {
			}
			return nil
		})
	}

	// Meanwhile subscribers come and go while values are in flight. A subscriber leaving at the moment a Send()
	// lands is the interleaving that drifts Pending, so churn hard rather than long.
	readers := sync.Group{}
	for i := 0; i < churners; i++ {
		readers.Go(ctx, func(ctx context.Context) error {
			for round := 0; round < rounds; round++ {
				taken := 0
				for range v.Subscribe(ctx) {
					taken++
					if taken == 2 {
						break
					}
				}
			}
			return nil
		})
	}

	writers := sync.Group{}
	for p := 0; p < producers; p++ {
		writers.Go(ctx, func(ctx context.Context) error {
			for i := 0; i < perProducer; i++ {
				v.Send(ctx, i)
			}
			return nil
		})
	}

	if err := writers.Wait(ctx); err != nil {
		t.Fatalf("TestMetricsNoDrift: writers: got err == %s, want err == nil", err)
	}
	v.Close(ctx)
	if err := readers.Wait(ctx); err != nil {
		t.Fatalf("TestMetricsNoDrift: readers: got err == %s, want err == nil", err)
	}
	if err := steady.Wait(ctx); err != nil {
		t.Fatalf("TestMetricsNoDrift: steady: got err == %s, want err == nil", err)
	}

	if got := sumValue(t, ctx, reader, "TestMetricsNoDrift", "after churn", "pending"); got != 0 {
		t.Errorf("TestMetricsNoDrift: pending: got %d, want 0", got)
	}
	if got := sumValue(t, ctx, reader, "TestMetricsNoDrift", "after churn", "subscribers"); got != 0 {
		t.Errorf("TestMetricsNoDrift: subscribers: got %d, want 0", got)
	}
}

// TestConcurrent sends from several producers to several subscribers at once and checks that no subscriber
// loses a value and that every subscriber sees the same total order.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	const (
		producers   = 4
		perProducer = 500
		subscribers = 8
	)

	ctx := t.Context()

	v := &Value[int]{}

	seqs := make([]iter.Seq[int], 0, subscribers)
	for i := 0; i < subscribers; i++ {
		seqs = append(seqs, v.Subscribe(ctx))
	}

	results := make([][]int, subscribers)
	readers := sync.Group{}
	for i, seq := range seqs {
		readers.Go(ctx, func(ctx context.Context) error {
			got := make([]int, 0, producers*perProducer)
			for n := range seq {
				got = append(got, n)
			}
			results[i] = got
			return nil
		})
	}

	writers := sync.Group{}
	for p := 0; p < producers; p++ {
		writers.Go(ctx, func(ctx context.Context) error {
			for i := 0; i < perProducer; i++ {
				v.Send(ctx, p*perProducer+i)
			}
			return nil
		})
	}
	if err := writers.Wait(ctx); err != nil {
		t.Fatalf("TestConcurrent: writers: got err == %s, want err == nil", err)
	}
	v.Close(ctx)
	if err := readers.Wait(ctx); err != nil {
		t.Fatalf("TestConcurrent: readers: got err == %s, want err == nil", err)
	}

	want := producers * perProducer
	for i, got := range results {
		if len(got) != want {
			t.Errorf("TestConcurrent: subscriber %d: got %d values, want %d", i, len(got), want)
			continue
		}
		if diff := pretty.Compare(results[0], got); diff != "" {
			t.Errorf("TestConcurrent: subscriber %d saw a different order than subscriber 0: -want/+got:\n%s", i, diff)
		}
	}
}
