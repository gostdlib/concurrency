package subscriber

import (
	"slices"
	"testing"

	"github.com/gostdlib/concurrency/broadcast/subscriber/internal/trie"

	"github.com/kylelemons/godebug/pretty"
)

// tree builds a trie holding patterns, where each pattern's value is the pattern itself. This is what
// Value[T] does, except that its values are the topics the patterns belong to.
func tree(t *testing.T, patterns []string) trie.Tree[string, string] {
	t.Helper()

	var out trie.Tree[string, string]
	for _, p := range patterns {
		segs, err := patternSegs(p)
		if err != nil {
			t.Fatalf("tree(): pattern(%s) is not valid: %s", p, err)
		}
		out = out.Insert(segs, p)
	}
	return out
}

func TestMatch(t *testing.T) {
	t.Parallel()

	set := []string{
		"prices/us/nyse",
		"prices/us/*",
		"prices/*/nyse",
		"prices/**",
		"prices/us/**",
		"**",
		"trades/us/nyse",
	}

	tests := []struct {
		name     string
		patterns []string
		topic    string
		want     []string
	}{
		{
			name:     "Success: a literal pattern matches the topic it names",
			patterns: []string{"prices/us/nyse", "trades/us/nyse"},
			topic:    "prices/us/nyse",
			want:     []string{"prices/us/nyse"},
		},
		{
			name:     "Success: a topic that no pattern matches",
			patterns: []string{"prices/us/nyse", "prices/*/nasdaq"},
			topic:    "trades/us/nyse",
		},
		{
			name:  "Success: an empty trie, which is what a Value with no subscribers holds, matches nothing",
			topic: "prices/us/nyse",
		},
		{
			name:     "Success: * matches a single segment",
			patterns: []string{"prices/*/nyse"},
			topic:    "prices/us/nyse",
			want:     []string{"prices/*/nyse"},
		},
		{
			name:     "Success: * does not match more than one segment",
			patterns: []string{"prices/*"},
			topic:    "prices/us/nyse",
		},
		{
			name:     "Success: * does not match zero segments",
			patterns: []string{"prices/*"},
			topic:    "prices",
		},
		{
			name:     "Success: ** matches a single trailing segment",
			patterns: []string{"prices/**"},
			topic:    "prices/us",
			want:     []string{"prices/**"},
		},
		{
			name:     "Success: ** matches many trailing segments",
			patterns: []string{"prices/**"},
			topic:    "prices/us/nyse/aapl",
			want:     []string{"prices/**"},
		},
		{
			name:     "Success: ** matches zero segments, so it matches its own prefix",
			patterns: []string{"prices/**"},
			topic:    "prices",
			want:     []string{"prices/**"},
		},
		{
			name:     "Success: a bare ** matches everything",
			patterns: []string{"**"},
			topic:    "prices/us/nyse",
			want:     []string{"**"},
		},
		{
			name:     "Success: ** does not match a topic that leaves its prefix",
			patterns: []string{"prices/**"},
			topic:    "trades/us/nyse",
		},
		{
			name:     "Success: * and a trailing ** in one pattern",
			patterns: []string{"prices/*/**"},
			topic:    "prices/us/nyse/tech/aapl",
			want:     []string{"prices/*/**"},
		},
		{
			name:     "Success: a * before a ** still matches exactly one segment",
			patterns: []string{"prices/*/**"},
			topic:    "prices",
			want:     nil,
		},
		{
			name:     "Success: every overlapping pattern that matches is reported",
			patterns: set,
			topic:    "prices/us/nyse",
			want:     []string{"prices/us/nyse", "prices/us/*", "prices/*/nyse", "prices/**", "prices/us/**", "**"},
		},
		{
			name:     "Success: overlapping patterns against a deeper topic",
			patterns: set,
			topic:    "prices/us/east/nyse",
			want:     []string{"prices/**", "prices/us/**", "**"},
		},
		{
			name:     "Success: overlapping patterns against another prefix",
			patterns: set,
			topic:    "trades/us/nyse",
			want:     []string{"trades/us/nyse", "**"},
		},
	}

	for _, test := range tests {
		segs, err := topicSegs(test.topic, nil)
		if err != nil {
			t.Fatalf("TestMatch(%s): topic(%s) is not valid: %s", test.name, test.topic, err)
		}

		got := match(tree(t, test.patterns), segs, nil)

		// match() reports patterns in the order it walks the trie, which is an implementation detail.
		slices.Sort(got)
		want := slices.Clone(test.want)
		slices.Sort(want)

		if diff := pretty.Compare(want, got); diff != "" {
			t.Errorf("TestMatch(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}
