package feeder

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/base/values/generics/result"

	"github.com/kylelemons/godebug/pretty"
)

func TestMapSet(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name      string
		setAccept func(key string, val, prev int, replaced bool) (bool, error)
		key       string
		val       int
		wantErr   bool
		wantVal   int
		wantOK    bool
	}{
		{
			name:    "Success: nil SetAccept accepts the set",
			key:     "a",
			val:     1,
			wantVal: 1,
			wantOK:  true,
		},
		{
			name:      "Success: SetAccept returning true accepts the set",
			setAccept: func(key string, val, prev int, replaced bool) (bool, error) { return true, nil },
			key:       "a",
			val:       1,
			wantVal:   1,
			wantOK:    true,
		},
		{
			name:      "Success: SetAccept returning false rejects the set",
			setAccept: func(key string, val, prev int, replaced bool) (bool, error) { return false, nil },
			key:       "a",
			val:       1,
			wantVal:   0,
			wantOK:    false,
		},
		{
			name:      "Error: SetAccept returning an error rejects the set",
			setAccept: func(key string, val, prev int, replaced bool) (bool, error) { return true, boom },
			key:       "a",
			val:       1,
			wantErr:   true,
			wantVal:   0,
			wantOK:    false,
		},
	}

	for _, test := range tests {
		m := &Map[string, int]{M: map[string]int{}, SetAccept: test.setAccept}
		err := m.Set(test.key, test.val)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestMapSet(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestMapSet(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		got, ok := m.Get(test.key)
		if got != test.wantVal || ok != test.wantOK {
			t.Errorf("TestMapSet(%s): got (%v, %v), want (%v, %v)", test.name, got, ok, test.wantVal, test.wantOK)
		}
	}
}

func TestMapDelete(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name         string
		deleteAccept func(key string, prev int, found bool) (bool, error)
		key          string
		wantErr      bool
		wantOK       bool
	}{
		{
			name:   "Success: nil DeleteAccept deletes the key",
			key:    "a",
			wantOK: false,
		},
		{
			name:         "Success: DeleteAccept returning true deletes the key",
			deleteAccept: func(key string, prev int, found bool) (bool, error) { return true, nil },
			key:          "a",
			wantOK:       false,
		},
		{
			name:         "Success: DeleteAccept returning false keeps the key",
			deleteAccept: func(key string, prev int, found bool) (bool, error) { return false, nil },
			key:          "a",
			wantOK:       true,
		},
		{
			name:   "Success: deleting a non-existent key is a no-op",
			key:    "missing",
			wantOK: true,
		},
		{
			name:         "Error: DeleteAccept returning an error keeps the key",
			deleteAccept: func(key string, prev int, found bool) (bool, error) { return true, boom },
			key:          "a",
			wantErr:      true,
			wantOK:       true,
		},
	}

	for _, test := range tests {
		m := &Map[string, int]{M: map[string]int{"a": 1}, DeleteAccept: test.deleteAccept}
		err := m.Delete(test.key)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestMapDelete(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestMapDelete(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		_, ok := m.Get("a")
		if ok != test.wantOK {
			t.Errorf("TestMapDelete(%s): key %q present == %v, want %v", test.name, "a", ok, test.wantOK)
		}
	}
}

// TestMapAllReleasesLock is a regression test: All() must release the read lock once iteration ends,
// whether the consumer ranges to completion or breaks early. A leaked lock would deadlock the Set below.
func TestMapAllReleasesLock(t *testing.T) {
	tests := []struct {
		name    string
		consume func(m *Map[string, int])
	}{
		{
			name:    "Success: read lock is released after a full iteration",
			consume: func(m *Map[string, int]) { for range m.All() { } },
		},
		{
			name: "Success: read lock is released after an early break",
			consume: func(m *Map[string, int]) {
				for range m.All() {
					break
				}
			},
		},
	}

	for _, test := range tests {
		m := &Map[string, int]{M: map[string]int{"a": 1, "b": 2, "c": 3}}
		test.consume(m)

		done := make(chan error, 1)
		go func() { done <- m.Set("z", 26) }()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("TestMapAllReleasesLock(%s): got err == %s, want err == nil", test.name, err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("TestMapAllReleasesLock(%s): All() did not release the read lock (deadlock)", test.name)
		}
	}
}

func TestShardedMapSet(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name      string
		setAccept func(key string, val, prev int, replaced bool) (bool, error)
		key       string
		val       int
		wantErr   bool
		want      map[string]int
	}{
		{
			name: "Success: nil SetAccept accepts the set",
			key:  "a",
			val:  1,
			want: map[string]int{"a": 1},
		},
		{
			name:      "Success: SetAccept returning true accepts the set",
			setAccept: func(key string, val, prev int, replaced bool) (bool, error) { return true, nil },
			key:       "a",
			val:       1,
			want:      map[string]int{"a": 1},
		},
		{
			name:      "Success: SetAccept returning false rejects the set",
			setAccept: func(key string, val, prev int, replaced bool) (bool, error) { return false, nil },
			key:       "a",
			val:       1,
			want:      map[string]int{},
		},
		{
			name:      "Error: SetAccept returning an error rejects the set",
			setAccept: func(key string, val, prev int, replaced bool) (bool, error) { return true, boom },
			key:       "a",
			val:       1,
			wantErr:   true,
			want:      map[string]int{},
		},
	}

	for _, test := range tests {
		m := &ShardedMap[string, int]{SetAccept: test.setAccept}
		err := m.Set(test.key, test.val)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestShardedMapSet(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestShardedMapSet(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		if diff := pretty.Compare(test.want, collectSharded(m)); diff != "" {
			t.Errorf("TestShardedMapSet(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

func TestShardedMapDelete(t *testing.T) {
	boom := errors.New("boom")

	tests := []struct {
		name         string
		deleteAccept func(key string, prev int, found bool) (bool, error)
		key          string
		wantErr      bool
		want         map[string]int
	}{
		{
			name: "Success: nil DeleteAccept deletes the key",
			key:  "a",
			want: map[string]int{},
		},
		{
			name:         "Success: DeleteAccept returning true deletes the key",
			deleteAccept: func(key string, prev int, found bool) (bool, error) { return true, nil },
			key:          "a",
			want:         map[string]int{},
		},
		{
			name:         "Success: DeleteAccept returning false keeps the key",
			deleteAccept: func(key string, prev int, found bool) (bool, error) { return false, nil },
			key:          "a",
			want:         map[string]int{"a": 1},
		},
		{
			name:         "Error: DeleteAccept returning an error keeps the key",
			deleteAccept: func(key string, prev int, found bool) (bool, error) { return true, boom },
			key:          "a",
			wantErr:      true,
			want:         map[string]int{"a": 1},
		},
	}

	for _, test := range tests {
		m := &ShardedMap[string, int]{DeleteAccept: test.deleteAccept}
		if err := m.Set("a", 1); err != nil {
			t.Errorf("TestShardedMapDelete(%s): seeding Set: %s", test.name, err)
			continue
		}

		err := m.Delete(test.key)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestShardedMapDelete(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestShardedMapDelete(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		if diff := pretty.Compare(test.want, collectSharded(m)); diff != "" {
			t.Errorf("TestShardedMapDelete(%s): -want/+got:\n%s", test.name, diff)
		}
	}
}

func TestFeed(t *testing.T) {
	startup := fmt.Errorf("pipeline startup failed: %w", ErrPermanent)

	tests := []struct {
		name     string
		ops      []KeyVal[string, int]
		startErr error
		wantErr  bool
		want     map[string]int
	}{
		{
			name: "Success: adds and deletes are applied to the value",
			ops: []KeyVal[string, int]{
				{Op: Add, K: "a", V: 1},
				{Op: Add, K: "b", V: 2},
				{Op: Delete, K: "a"},
			},
			want: map[string]int{"b": 2},
		},
		{
			name:     "Error: a pipeline startup error is delivered on finished",
			startErr: startup,
			wantErr:  true,
			want:     map[string]int{},
		},
	}

	for _, test := range tests {
		m := &Map[string, int]{M: map[string]int{}}
		f, err := NewFeeder[string, int](t.Context(), m)
		if err != nil {
			t.Errorf("TestFeed(%s): NewFeeder: %s", test.name, err)
			continue
		}

		err = <-f.Feed(t.Context(), staticPipe(test.ops, test.startErr))
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestFeed(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestFeed(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		if diff := pretty.Compare(test.want, m.M); diff != "" {
			t.Errorf("TestFeed(%s): map -want/+got:\n%s", test.name, diff)
		}
	}
}

// TestFeedRetry verifies that, with a retry policy, a retryable pipeline error is retried until the
// pipeline succeeds, while a permanent error stops retrying. Both cases fail exactly once before the
// pipeline would succeed, so the permanent case proves retries were NOT attempted.
func TestFeedRetry(t *testing.T) {
	back, err := exponential.New(exponential.WithTesting())
	if err != nil {
		t.Fatalf("TestFeedRetry: exponential.New: %s", err)
	}

	permanent := fmt.Errorf("permanent failure: %w", ErrPermanent)

	tests := []struct {
		name     string
		failErr  error
		ops      []KeyVal[string, int]
		wantErr  bool
		want     map[string]int
	}{
		{
			name:    "Success: a retryable startup error is retried until the pipeline succeeds",
			failErr: errors.New("temporary failure"),
			ops:     []KeyVal[string, int]{{Op: Add, K: "a", V: 1}},
			want:    map[string]int{"a": 1},
		},
		{
			name:    "Error: a permanent startup error stops retries and is delivered",
			failErr: permanent,
			wantErr: true,
			want:    map[string]int{},
		},
	}

	for _, test := range tests {
		m := &Map[string, int]{M: map[string]int{}}
		f, err := NewFeeder[string, int](t.Context(), m, WithRetry[string, int](back))
		if err != nil {
			t.Errorf("TestFeedRetry(%s): NewFeeder: %s", test.name, err)
			continue
		}

		err = <-f.Feed(t.Context(), flakyPipe(1, test.failErr, test.ops))
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestFeedRetry(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestFeedRetry(%s): got err == %s, want err == nil", test.name, err)
			continue
		}

		if diff := pretty.Compare(test.want, m.M); diff != "" {
			t.Errorf("TestFeedRetry(%s): map -want/+got:\n%s", test.name, diff)
		}
	}
}

func TestFeedResult(t *testing.T) {
	m := &Map[string, int]{M: map[string]int{}}
	f, err := NewFeeder[string, int](t.Context(), m)
	if err != nil {
		t.Fatalf("TestFeedResult: NewFeeder: %s", err)
	}

	res := result.New[struct{}]()
	if err := <-f.Feed(t.Context(), staticPipe([]KeyVal[string, int]{{Op: Add, K: "a", V: 1, Result: res}}, nil)); err != nil {
		t.Fatalf("TestFeedResult: feed: %s", err)
	}

	if _, err := res.Wait(t.Context()); err != nil {
		t.Errorf("TestFeedResult: got Result err == %s, want err == nil", err)
	}
}

func TestFeedUnknownOpPanics(t *testing.T) {
	f := &Feeder[string, int]{v: &Map[string, int]{M: map[string]int{}}}

	p := make(chan KeyVal[string, int], 1)
	p <- KeyVal[string, int]{Op: UnknownOp, K: "a"}
	close(p)

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestFeedUnknownOpPanics: got no panic, want a panic on an unknown Op")
		}
	}()
	f.feed(t.Context(), p, make(chan struct{}))
}

// staticPipe returns a KVPipeline that, unless startErr is set, delivers ops on a buffered channel and
// closes it so the feed completes after draining.
func staticPipe(ops []KeyVal[string, int], startErr error) KVPipeline[string, int] {
	return func(ctx context.Context) (chan KeyVal[string, int], chan struct{}, error) {
		if startErr != nil {
			return nil, nil, startErr
		}
		ch := make(chan KeyVal[string, int], len(ops))
		for _, kv := range ops {
			ch <- kv
		}
		close(ch)
		return ch, make(chan struct{}), nil
	}
}

// flakyPipe returns a KVPipeline that fails startup with failErr the first failures times, then behaves
// like staticPipe. The pipeline is invoked sequentially by the backoff, so the counter needs no locking.
func flakyPipe(failures int, failErr error, ops []KeyVal[string, int]) KVPipeline[string, int] {
	var n int
	return func(ctx context.Context) (chan KeyVal[string, int], chan struct{}, error) {
		if n < failures {
			n++
			return nil, nil, failErr
		}
		ch := make(chan KeyVal[string, int], len(ops))
		for _, kv := range ops {
			ch <- kv
		}
		close(ch)
		return ch, make(chan struct{}), nil
	}
}

func collectSharded(m *ShardedMap[string, int]) map[string]int {
	out := map[string]int{}
	for k, v := range m.All() {
		out[k] = v
	}
	return out
}
