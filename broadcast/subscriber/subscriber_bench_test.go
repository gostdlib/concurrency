package subscriber

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/gostdlib/base/context"
)

// benchValue returns a Value holding patterns, each with one subscriber that drains everything, along
// with the topic to send to.
func benchValue(b *testing.B, ctx context.Context, patterns []string) *Value[int] {
	b.Helper()

	v := &Value[int]{}
	for _, p := range patterns {
		seq, err := v.Subscribe(ctx, p)
		if err != nil {
			b.Fatalf("benchValue(): Subscribe(%s): got err == %s, want err == nil", p, err)
		}
		context.Pool(ctx).Submit(ctx, func() {
			for range seq {
			}
		})
	}
	return v
}

// literals returns n literal patterns that the sent topic does not match, so the walk has to get past
// them to find what it is looking for.
func literals(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("prices/us/nyse/%d", i))
	}
	return out
}

// BenchmarkSend measures the send path, which is a walk of the trie plus a broadcast Send() to each
// pattern that matched. The trie is what keeps this off the number of patterns and on the depth of the
// topic, so the interesting comparison is subscribers=1 against subscribers=1000 at the same depth.
func BenchmarkSend(b *testing.B) {
	benchmarks := []struct {
		name     string
		patterns []string
		topic    string
	}{
		{
			name:     "one literal pattern, matched",
			patterns: []string{"prices/us/nyse"},
			topic:    "prices/us/nyse",
		},
		{
			name:     "1000 literal patterns, one matched",
			patterns: append(literals(1000), "prices/us/nyse"),
			topic:    "prices/us/nyse",
		},
		{
			name:     "1000 literal patterns, none matched",
			patterns: literals(1000),
			topic:    "prices/us/nyse",
		},
		{
			name:     "one * pattern, matched",
			patterns: []string{"prices/us/*"},
			topic:    "prices/us/nyse",
		},
		{
			name:     "one ** pattern, matched",
			patterns: []string{"prices/**"},
			topic:    "prices/us/nyse",
		},
		{
			name:     "a * before a **, matched",
			patterns: []string{"prices/*/**"},
			topic:    "prices/us/east/nyse",
		},
		{
			name:     "five overlapping patterns, all matched",
			patterns: []string{"prices/us/nyse", "prices/us/*", "prices/*/nyse", "prices/**", "**"},
			topic:    "prices/us/nyse",
		},
		{
			name:     "a deep topic against a bare **",
			patterns: []string{"**"},
			topic:    strings.Join([]string{"a", "b", "c", "d", "e", "f", "g", "h"}, "/"),
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := b.Context()

			v := benchValue(b, ctx, bm.patterns)
			defer v.Close(ctx)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := v.Send(ctx, bm.topic, i); err != nil {
					b.Fatalf("BenchmarkSend(%s): got err == %s, want err == nil", bm.name, err)
				}
			}
		})
	}
}

// BenchmarkTeardown measures what a parent Context dying takes: every subscription under it is given back
// at once, each on its own callback, and each takes the lock to drop its pattern. The trie copies only the
// path a delete touches, so this is the number of subscriptions times the depth of a pattern, not the
// number of subscriptions times the size of the whole topic set.
func BenchmarkTeardown(b *testing.B) {
	benchmarks := []struct {
		name string
		subs int
	}{
		{name: "100 subscriptions", subs: 100},
		{name: "1000 subscriptions", subs: 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := b.Context()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				v := &Value[int]{}
				cancels := make([]context.CancelFunc, 0, bm.subs)
				for j := 0; j < bm.subs; j++ {
					sub, cancel := context.WithCancel(ctx)
					if _, err := v.Subscribe(sub, fmt.Sprintf("prices/us/nyse/%d", j)); err != nil {
						cancel()
						b.Fatalf("BenchmarkTeardown(%s): Subscribe(): got err == %s, want err == nil", bm.name, err)
					}
					cancels = append(cancels, cancel)
				}

				b.StartTimer()

				for _, cancel := range cancels {
					cancel()
				}
				// A subscription is given back from a Context callback, so the teardown is not done when the
				// last cancel returns. It is done when every pattern is gone.
				for v.topicCount() != 0 {
					runtime.Gosched()
				}
			}
		})
	}
}

// BenchmarkSubscribe measures the subscribe path, which inserts the pattern into the trie. The insert
// copies only the path it touches, so this should not grow with the number of patterns already there.
func BenchmarkSubscribe(b *testing.B) {
	benchmarks := []struct {
		name     string
		existing int
	}{
		{name: "no existing patterns", existing: 0},
		{name: "100 existing patterns", existing: 100},
		{name: "1000 existing patterns", existing: 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := b.Context()

			v := benchValue(b, ctx, literals(bm.existing))
			defer v.Close(ctx)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := v.Subscribe(ctx, fmt.Sprintf("trades/us/nyse/%d", i)); err != nil {
					b.Fatalf("BenchmarkSubscribe(%s): got err == %s, want err == nil", bm.name, err)
				}
			}
		})
	}
}
