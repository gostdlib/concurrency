package subscriber

import (
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"

	"github.com/kylelemons/godebug/pretty"
)

// topicCount is the number of patterns the Value currently holds. Used to check that a pattern is torn
// down once its last subscriber goes away.
func (v *Value[T]) topicCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.topics)
}

// treeEmpty reports whether the trie Send() walks holds nothing. The map and the trie are torn down by
// different keys, the map by the pattern and the trie by its segments, so a topicCount of 0 says nothing
// about the trie. A pattern left behind in the trie is a leak that only this can see.
func (v *Value[T]) treeEmpty() bool {
	return v.tree.Load().Empty()
}

// waitTopics waits for the Value to hold want patterns. A subscription is given back from a Context
// callback, so teardown happens after the cancel, not during it.
func waitTopics[T any](t *testing.T, v *Value[T], want int) int {
	t.Helper()

	for i := 0; i < 100; i++ {
		if got := v.topicCount(); got == want {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return v.topicCount()
}

// send is a value sent to a topic.
type send struct {
	topic string
	value int
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// patterns is a subscription each. Two entries holding the same pattern are two subscriptions.
		patterns []string
		sends    []send
		// want is what each subscription in patterns receives, in the same order.
		want    [][]int
		wantErr bool
	}{
		{
			name:     "Success: a literal pattern receives its topic",
			patterns: []string{"prices/us/nyse"},
			sends:    []send{{"prices/us/nyse", 1}},
			want:     [][]int{{1}},
		},
		{
			name:     "Success: a literal pattern does not receive another topic",
			patterns: []string{"prices/us/nyse"},
			sends:    []send{{"prices/us/nasdaq", 1}},
			want:     [][]int{{}},
		},
		{
			name:     "Success: a leading / on either side is the same topic",
			patterns: []string{"/prices/us/nyse"},
			sends:    []send{{"prices/us/nyse", 1}},
			want:     [][]int{{1}},
		},
		{
			name:     "Success: values arrive in the order they were sent",
			patterns: []string{"prices/us/nyse"},
			sends:    []send{{"prices/us/nyse", 1}, {"prices/us/nyse", 2}, {"prices/us/nyse", 3}},
			want:     [][]int{{1, 2, 3}},
		},
		{
			name:     "Success: a * matches one segment",
			patterns: []string{"prices/us/*"},
			sends:    []send{{"prices/us/nyse", 1}, {"prices/us/nasdaq", 2}, {"prices/us/east/nyse", 3}},
			want:     [][]int{{1, 2}},
		},
		{
			name:     "Success: a ** matches any number of segments",
			patterns: []string{"prices/**"},
			sends:    []send{{"prices/us", 1}, {"prices/us/east/nyse", 2}, {"trades/us", 3}},
			want:     [][]int{{1, 2}},
		},
		{
			name:     "Success: a ** matches zero segments, so it matches its own prefix",
			patterns: []string{"prices/**"},
			sends:    []send{{"prices", 1}},
			want:     [][]int{{1}},
		},
		{
			name:     "Success: every pattern that matches a topic receives the value",
			patterns: []string{"prices/us/nyse", "prices/us/*", "prices/**", "**", "trades/**"},
			sends:    []send{{"prices/us/nyse", 1}},
			want:     [][]int{{1}, {1}, {1}, {1}, {}},
		},
		{
			name:     "Success: two subscriptions to the same pattern both receive the value",
			patterns: []string{"prices/**", "prices/**"},
			sends:    []send{{"prices/us", 1}},
			want:     [][]int{{1}, {1}},
		},
		{
			name:     "Success: a topic no pattern matches is dropped",
			patterns: []string{"prices/**"},
			sends:    []send{{"trades/us", 1}},
			want:     [][]int{{}},
		},
		{
			// One row is enough to prove Subscribe() runs the pattern through patternSegs() and hands its error
			// back. Which patterns are invalid is patternSegs()'s contract, and TestPatternSegs tests it in full.
			name:     "Error: an invalid pattern is rejected",
			patterns: []string{"prices/us*"},
			wantErr:  true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()

		v := &Value[int]{}

		seqs := make([]iter.Seq[int], 0, len(test.patterns))
		var err error
		for _, p := range test.patterns {
			var seq iter.Seq[int]
			seq, err = v.Subscribe(ctx, p)
			if err != nil {
				break
			}
			seqs = append(seqs, seq)
		}

		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestSubscribe(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestSubscribe(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		for _, s := range test.sends {
			if err := v.Send(ctx, s.topic, s.value); err != nil {
				t.Fatalf("TestSubscribe(%s): Send(%s): got err == %s, want err == nil", test.name, s.topic, err)
			}
		}
		v.Close(ctx)

		for i, seq := range seqs {
			got := []int{}
			for n := range seq {
				got = append(got, n)
			}
			if diff := pretty.Compare(test.want[i], got); diff != "" {
				t.Errorf("TestSubscribe(%s): subscription %d(%s): -want/+got:\n%s", test.name, i, test.patterns[i], diff)
			}
		}
	}
}

func TestSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:  "Success: a fully qualified topic",
			topic: "prices/us/nyse",
		},
		{
			name:  "Success: a leading / is not part of the topic",
			topic: "/prices/us/nyse",
		},
		{
			// One row is enough to prove Send() runs the topic through topicSegs() and hands its error back.
			// Which topics are invalid is topicSegs()'s contract, and TestTopicSegs tests it in full.
			name:    "Error: an invalid topic is rejected",
			topic:   "prices/*/nyse",
			wantErr: true,
		},
	}

	for _, test := range tests {
		ctx := t.Context()

		v := &Value[int]{}
		seq, err := v.Subscribe(ctx, "**")
		if err != nil {
			t.Fatalf("TestSend(%s): Subscribe(): got err == %s, want err == nil", test.name, err)
		}

		err = v.Send(ctx, test.topic, 1)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestSend(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestSend(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		v.Close(ctx)

		got := []int{}
		for n := range seq {
			got = append(got, n)
		}
		if diff := pretty.Compare([]int{1}, got); diff != "" {
			t.Errorf("TestSend(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestErrors checks that a caller can tell why it was turned away without reading the message. A bad path
// and a closed Value are the two ways in, and they have to be told apart by value.
func TestErrors(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}

	// A path the caller got wrong, on a Value that is open and working. The mistake is the caller's and no
	// retry fixes it, so the error is permanent as well as an ErrInvalidPath.
	badPath := v.Send(ctx, "prices/*/nyse", 1)
	switch {
	case !errors.Is(badPath, ErrInvalidPath):
		t.Errorf("TestErrors: Send() with a wildcard in the topic: got err == %v, want ErrInvalidPath", badPath)
	case !errors.Is(badPath, exponential.ErrPermanent):
		t.Errorf("TestErrors: Send() with a wildcard in the topic: got err == %v, want it to also be ErrPermanent", badPath)
	}
	if _, err := v.Subscribe(ctx, "prices/us*"); !errors.Is(err, ErrInvalidPath) {
		t.Errorf("TestErrors: Subscribe() with a half wildcard: got err == %v, want ErrInvalidPath", err)
	}

	// A good path is not turned away, so the two above are about the path and not about the Value.
	if err := v.Send(ctx, "prices/us/nyse", 1); err != nil {
		t.Errorf("TestErrors: Send() with a good topic: got err == %s, want err == nil", err)
	}

	v.Close(ctx)

	// The same good path now fails, and for the other reason. A closed Value never reopens, so this is
	// permanent too.
	err := v.Send(ctx, "prices/us/nyse", 1)
	switch {
	case !errors.Is(err, ErrClosed):
		t.Errorf("TestErrors: Send() after Close(): got err == %v, want ErrClosed", err)
	case errors.Is(err, ErrInvalidPath):
		t.Errorf("TestErrors: Send() after Close(): got ErrInvalidPath as well, want the two told apart")
	case !errors.Is(err, exponential.ErrPermanent):
		t.Errorf("TestErrors: Send() after Close(): got err == %v, want it to also be ErrPermanent", err)
	}

	// A bad path on a closed Value reports being closed, not the bad path: the lifecycle answer wins, which is
	// what the doc promises. This is checked because the closed test comes before the path is validated, and
	// the reverse order would leak an ErrInvalidPath here.
	if err := v.Send(ctx, "prices/*/nyse", 1); !errors.Is(err, ErrClosed) || errors.Is(err, ErrInvalidPath) {
		t.Errorf("TestErrors: Send() with a bad path after Close(): got err == %v, want ErrClosed and not ErrInvalidPath", err)
	}
	if _, err := v.Subscribe(ctx, "prices/us*"); !errors.Is(err, ErrClosed) || errors.Is(err, ErrInvalidPath) {
		t.Errorf("TestErrors: Subscribe() with a bad path after Close(): got err == %v, want ErrClosed and not ErrInvalidPath", err)
	}
}

// TestClose checks that Close() ends every subscription and that the Value stops taking work.
func TestClose(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}
	seq, err := v.Subscribe(ctx, "prices/**")
	if err != nil {
		t.Fatalf("TestClose: Subscribe(): got err == %s, want err == nil", err)
	}

	if err := v.Send(ctx, "prices/us", 1); err != nil {
		t.Fatalf("TestClose: Send(): got err == %s, want err == nil", err)
	}

	v.Close(ctx)
	v.Close(ctx) // Close() is safe to call more than once.

	// The value sent before Close() is delivered and then the iteration ends, without ctx being canceled.
	got := []int{}
	for n := range seq {
		got = append(got, n)
	}
	if diff := pretty.Compare([]int{1}, got); diff != "" {
		t.Errorf("TestClose: -want/+got:\n%s", diff)
	}

	if err := v.Send(ctx, "prices/us", 2); !errors.Is(err, ErrClosed) {
		t.Errorf("TestClose: Send() after Close(): got err == %v, want err == ErrClosed", err)
	}
	if _, err := v.Subscribe(ctx, "prices/**"); !errors.Is(err, ErrClosed) {
		t.Errorf("TestClose: Subscribe() after Close(): got err == %v, want err == ErrClosed", err)
	}
	if got := v.topicCount(); got != 0 {
		t.Errorf("TestClose: got topicCount == %d, want 0", got)
	}
}

// TestCloseStopsCallbacks checks that Close() hands each subscriber's Context back the callback Subscribe()
// put on it. Close() ends every subscription, so those callbacks have nothing left to do, and leaving one on
// a Context holds the Value and the pattern it names until a Context that may never be canceled is canceled.
func TestCloseStopsCallbacks(t *testing.T) {
	t.Parallel()

	v := &Value[int]{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if _, err := v.Subscribe(ctx, "prices/**"); err != nil {
		t.Fatalf("TestCloseStopsCallbacks: Subscribe(): got err == %s, want err == nil", err)
	}

	v.mu.Lock()
	pattern := v.topics["prices/**"]
	var s *subscription[int]
	for k := range v.subs {
		s = k
	}
	v.mu.Unlock()
	switch {
	case pattern == nil:
		t.Fatalf("TestCloseStopsCallbacks: Subscribe() did not register the pattern")
	case s == nil:
		t.Fatalf("TestCloseStopsCallbacks: Subscribe() did not record the subscription")
	}

	// ctx is still live, so Close() is the only thing that can have taken the callback off it.
	v.Close(ctx)

	// stop() only reports true if it was the call that took the callback off the Context, so a false here is
	// what says Close() already did.
	if s.stop() {
		t.Errorf("TestCloseStopsCallbacks: got stop() == true after Close(), want false: Close() left its callback on the Context")
	}

	v.mu.Lock()
	subs := len(v.subs)
	refs := s.t.refs
	v.mu.Unlock()

	if subs != 0 {
		t.Errorf("TestCloseStopsCallbacks: got len(subs) == %d after Close(), want 0", subs)
	}

	// A ctx that cancels after Close() runs release() anyway, so do here what that stale callback does and
	// check it changes nothing. Membership in subs is the only thing keeping that release from counting a
	// second time: without it refs would go negative and the Subscribers metric would drift below zero.
	v.release(ctx, s)

	v.mu.Lock()
	gotRefs, gotSubs, gotTopics := s.t.refs, len(v.subs), len(v.topics)
	v.mu.Unlock()

	switch {
	case gotRefs != refs:
		t.Errorf("TestCloseStopsCallbacks: a stale release() after Close() changed refs from %d to %d, want it unchanged", refs, gotRefs)
	case gotSubs != 0:
		t.Errorf("TestCloseStopsCallbacks: a stale release() after Close() left len(subs) == %d, want 0", gotSubs)
	case gotTopics != 0:
		t.Errorf("TestCloseStopsCallbacks: a stale release() after Close() left len(topics) == %d, want 0", gotTopics)
	}
}

// TestRelease checks that a pattern is only torn down once its last subscriber cancels, so that a Value
// stops storing values for subscribers that have walked away.
func TestRelease(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}
	defer v.Close(ctx)

	first, cancelFirst := context.WithCancel(ctx)
	second, cancelSecond := context.WithCancel(ctx)
	other, cancelOther := context.WithCancel(ctx)
	defer cancelOther()

	for _, sub := range []struct {
		ctx     context.Context
		pattern string
	}{
		{first, "prices/**"},
		{second, "prices/**"},
		{other, "trades/**"},
	} {
		if _, err := v.Subscribe(sub.ctx, sub.pattern); err != nil {
			t.Fatalf("TestRelease: Subscribe(%s): got err == %s, want err == nil", sub.pattern, err)
		}
	}

	if got := v.topicCount(); got != 2 {
		t.Fatalf("TestRelease: after subscribing: got topicCount == %d, want 2", got)
	}

	// One of the two subscribers to prices/** leaves. The other still holds the pattern.
	cancelFirst()
	if got := waitTopics(t, v, 2); got != 2 {
		t.Errorf("TestRelease: after the first of two subscribers left: got topicCount == %d, want 2", got)
	}

	// The last subscriber to prices/** leaves, so the pattern goes away. trades/** stays.
	cancelSecond()
	if got := waitTopics(t, v, 1); got != 1 {
		t.Errorf("TestRelease: after the last subscriber to a pattern left: got topicCount == %d, want 1", got)
	}

	// A pattern nobody is subscribed to must be out of the trie as well, not just out of the map. The two
	// come down by different keys, so a Send() could otherwise go on matching a pattern that is gone.
	if _, ok := v.tree.Load().Trace([]string{"prices", doubleStar}).Terminal(); ok {
		t.Errorf("TestRelease: the pattern nobody subscribes to is still in the trie Send() walks")
	}

	// The last subscriber of all leaves, so the trie empties out.
	cancelOther()
	waitTopics(t, v, 0)
	if !v.treeEmpty() {
		t.Errorf("TestRelease: after every subscriber left: got a trie still holding patterns, want it empty")
	}
}

// TestSubscribeBreakReleases checks that breaking out of the iteration gives the pattern back on its own,
// so a subscriber that walks away does not have to cancel its Context to stop being matched on every Send().
func TestSubscribeBreakReleases(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	v := &Value[int]{}

	seq, err := v.Subscribe(ctx, "prices/**")
	if err != nil {
		t.Fatalf("TestSubscribeBreakReleases: Subscribe(): got err == %s, want err == nil", err)
	}
	if got := v.topicCount(); got != 1 {
		t.Fatalf("TestSubscribeBreakReleases: after Subscribe(): got topicCount == %d, want 1", got)
	}

	// A value is waiting, so the break happens mid-stream (yield returns false) rather than on an empty
	// iterator. Ranging runs on this goroutine, so the release completes before the loop statement returns,
	// and ctx is never canceled.
	v.Send(ctx, "prices/us", 1)
	for range seq {
		break
	}

	if got := v.topicCount(); got != 0 {
		t.Errorf("TestSubscribeBreakReleases: after breaking out: got topicCount == %d, want 0", got)
	}
	if !v.treeEmpty() {
		t.Errorf("TestSubscribeBreakReleases: after breaking out: got a trie still holding the pattern, want it empty")
	}

	v.mu.Lock()
	subs := len(v.subs)
	v.mu.Unlock()
	if subs != 0 {
		t.Errorf("TestSubscribeBreakReleases: after breaking out: got len(subs) == %d, want 0", subs)
	}
}

// TestConcurrent hammers a Value from several directions at once to shake out data races. It asserts
// nothing about what is delivered, only that concurrent use is safe.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	v := &Value[int]{}

	patterns := []string{"prices/us/nyse", "prices/us/*", "prices/**", "**", "prices/us/**"}
	topics := []string{"prices/us/nyse", "prices/us/nasdaq", "prices/us/east/nyse", "trades/us/nyse"}

	g := context.Pool(ctx).Group()
	for i := 0; i < 10; i++ {
		g.Go(ctx, func(ctx context.Context) error {
			for j, p := range patterns {
				sub, cancel := context.WithCancel(ctx)

				seq, err := v.Subscribe(sub, p)
				if err != nil {
					cancel()
					return err
				}

				// Half the subscribers walk away without ever ranging, which is what a canceled
				// subscription that never started looks like.
				if j%2 == 0 {
					context.Pool(ctx).Submit(ctx, func() {
						for range seq {
						}
					})
				}
				cancel()
			}
			return nil
		})

		g.Go(ctx, func(ctx context.Context) error {
			for j, topic := range topics {
				if err := v.Send(ctx, topic, j); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestConcurrent: got err == %s, want err == nil", err)
	}

	v.Close(ctx)
}
