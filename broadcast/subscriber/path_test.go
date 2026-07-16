package subscriber

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestTopicSegs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		want    []string
		wantErr bool
	}{
		{
			name:  "Success: a single segment",
			topic: "prices",
			want:  []string{"prices"},
		},
		{
			name:  "Success: multiple segments",
			topic: "prices/us/nyse",
			want:  []string{"prices", "us", "nyse"},
		},
		{
			name:  "Success: a leading / is not part of the canonical form",
			topic: "/prices/us/nyse",
			want:  []string{"prices", "us", "nyse"},
		},
		{
			name:    "Error: the topic is empty",
			topic:   "",
			wantErr: true,
		},
		{
			name:    "Error: the topic ends in a /",
			topic:   "prices/us/",
			wantErr: true,
		},
		{
			name:    "Error: the topic has an empty segment",
			topic:   "prices//nyse",
			wantErr: true,
		},
		{
			name:    "Error: the topic has a * segment",
			topic:   "prices/*/nyse",
			wantErr: true,
		},
		{
			name:    "Error: the topic has a ** segment",
			topic:   "prices/**/nyse",
			wantErr: true,
		},
		{
			name:    "Error: the topic has a * inside a segment",
			topic:   "prices/us*/nyse",
			wantErr: true,
		},
	}

	for _, test := range tests {
		got, err := topicSegs(test.topic, nil)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestTopicSegs(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestTopicSegs(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestTopicSegs(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

func TestPatternSegs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    []string
		wantErr bool
	}{
		{
			name:    "Success: a pattern with no wildcards",
			pattern: "prices/us/nyse",
			want:    []string{"prices", "us", "nyse"},
		},
		{
			name:    "Success: a leading / is not part of the canonical form",
			pattern: "/prices/us/nyse",
			want:    []string{"prices", "us", "nyse"},
		},
		{
			name:    "Success: a * in the middle",
			pattern: "prices/*/nyse",
			want:    []string{"prices", star, "nyse"},
		},
		{
			name:    "Success: a * as the last segment",
			pattern: "prices/us/*",
			want:    []string{"prices", "us", star},
		},
		{
			name:    "Success: a ** as the last segment",
			pattern: "prices/**",
			want:    []string{"prices", doubleStar},
		},
		{
			name:    "Success: a * and a trailing **",
			pattern: "prices/*/**",
			want:    []string{"prices", star, doubleStar},
		},
		{
			name:    "Success: a ** on its own subscribes to everything",
			pattern: "**",
			want:    []string{doubleStar},
		},
		{
			name:    "Error: the pattern is empty",
			pattern: "",
			wantErr: true,
		},
		{
			name:    "Error: the pattern ends in a /",
			pattern: "prices/us/",
			wantErr: true,
		},
		{
			name:    "Error: the pattern has an empty segment",
			pattern: "prices//nyse",
			wantErr: true,
		},
		{
			name:    "Error: a * does not make up an entire segment",
			pattern: "prices/us*/nyse",
			wantErr: true,
		},
		{
			name:    "Error: a ** does not make up an entire segment",
			pattern: "prices/**x/nyse",
			wantErr: true,
		},
		{
			name:    "Error: a segment holds more than a **",
			pattern: "prices/***/nyse",
			wantErr: true,
		},
		{
			name:    "Error: a ** that is not the last segment",
			pattern: "prices/**/nyse",
			wantErr: true,
		},
		{
			name:    "Error: more than one **",
			pattern: "prices/**/**",
			wantErr: true,
		},
	}

	for _, test := range tests {
		got, err := patternSegs(test.pattern)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestPatternSegs(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestPatternSegs(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestPatternSegs(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

func TestCanonical(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		segs []string
		want string
	}{
		{
			name: "Success: a single segment",
			segs: []string{"prices"},
			want: "prices",
		},
		{
			name: "Success: multiple segments",
			segs: []string{"prices", "us", "nyse"},
			want: "prices/us/nyse",
		},
		{
			name: "Success: wildcards are kept as written",
			segs: []string{"prices", star, doubleStar},
			want: "prices/*/**",
		},
	}

	for _, test := range tests {
		got := canonical(test.segs)
		if got != test.want {
			t.Errorf("TestCanonical(%s): got %q, want %q", test.name, got, test.want)
		}
	}
}
