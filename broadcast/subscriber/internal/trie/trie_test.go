package trie

import (
	"fmt"
	"strings"
	"testing"
)

// build returns a Tree holding each path in paths, where a path's value is the path itself joined by /,
// so a value says which path it was stored under and a lookup landing on the wrong node is visible.
func build(paths ...string) Tree[string, string] {
	var t Tree[string, string]
	for _, p := range paths {
		t = t.Insert(strings.Split(p, "/"), p)
	}
	return t
}

// wide returns a Tree whose "a" node holds enough children to put find() on its binary search, along with
// a narrow sibling. The children are inserted in descending order, so every insert lands in front of the
// ones already there. Building it in ascending order would put every insert at the end and never ask
// find() for an insertion point in the middle, which is most of what the search has to get right.
func wide(t *testing.T) Tree[string, string] {
	t.Helper()

	paths := make([]string, 0, wideChildren+1)
	for i := wideChildren - 1; i >= 0; i-- {
		paths = append(paths, fmt.Sprintf("a/c%02d", i))
	}
	paths = append(paths, "b/zzz")

	return build(paths...)
}

// wideChildren is comfortably past the point find() stops scanning and starts searching.
const wideChildren = 45

func TestInsert(t *testing.T) {
	t.Parallel()

	set := []string{"prices/us/nyse", "prices/us", "prices/eu/lse", "trades/us/nyse"}

	tests := []struct {
		name string
		// insert is what the tree holds, path is what is looked up in it.
		insert []string
		path   string
		want   string
		wantOK bool
	}{
		{
			name:   "Success: a whole path holds its value",
			insert: set,
			path:   "prices/us/nyse",
			want:   "prices/us/nyse",
			wantOK: true,
		},
		{
			name:   "Success: a path that is also the prefix of a longer one holds its value",
			insert: set,
			path:   "prices/us",
			want:   "prices/us",
			wantOK: true,
		},
		{
			name:   "Success: the longer path survives inserting its prefix",
			insert: []string{"prices/us/nyse", "prices/us"},
			path:   "prices/us/nyse",
			want:   "prices/us/nyse",
			wantOK: true,
		},
		{
			name:   "Success: a path branching off an existing one",
			insert: set,
			path:   "prices/eu/lse",
			want:   "prices/eu/lse",
			wantOK: true,
		},
		{
			name:   "Success: a single segment path",
			insert: []string{"prices"},
			path:   "prices",
			want:   "prices",
			wantOK: true,
		},
		{
			name:   "Success: a prefix that was never inserted holds nothing",
			insert: set,
			path:   "prices",
		},
		{
			name:   "Success: a path that is not in the tree holds nothing",
			insert: set,
			path:   "prices/us/nasdaq",
		},
		{
			name:   "Success: a path below a whole path holds nothing",
			insert: set,
			path:   "prices/us/nyse/aapl",
		},
		{
			name: "Success: an empty tree holds nothing",
			path: "prices/us",
		},
	}

	for _, test := range tests {
		got, ok := build(test.insert...).Trace(strings.Split(test.path, "/")).Terminal()

		switch {
		case ok != test.wantOK:
			t.Errorf("TestInsert(%s): got ok == %v, want ok == %v (value %q)", test.name, ok, test.wantOK, got)
		case got != test.want:
			t.Errorf("TestInsert(%s): got %q, want %q", test.name, got, test.want)
		}
	}
}

// TestInsertReplaces checks that inserting a path that is already there replaces its value. The values
// have to differ for this to say anything, which is why it does not go through build().
func TestInsertReplaces(t *testing.T) {
	t.Parallel()

	path := []string{"prices", "us"}

	var tree Tree[string, string]
	tree = tree.Insert(path, "first")
	tree = tree.Insert(path, "second")

	got, ok := tree.Trace(path).Terminal()
	switch {
	case !ok:
		t.Fatalf("TestInsertReplaces: the path is gone, want it holding a value")
	case got != "second":
		t.Errorf("TestInsertReplaces: got %q, want %q", got, "second")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// insert is what the tree holds and delete is the path removed from it. Then every path in gone
		// must hold nothing, every path in kept must still hold its own value, and every path in pruned
		// must no longer be a node at all, which is what keeps churn from growing the tree forever.
		insert []string
		delete string
		gone   []string
		kept   []string
		pruned []string
	}{
		{
			name:   "Success: the only path leaves an empty tree",
			insert: []string{"prices/us"},
			delete: "prices/us",
			gone:   []string{"prices/us"},
			pruned: []string{"prices"},
		},
		{
			name:   "Success: a sibling is untouched",
			insert: []string{"prices/us/nyse", "prices/eu/lse"},
			delete: "prices/us/nyse",
			gone:   []string{"prices/us/nyse"},
			kept:   []string{"prices/eu/lse"},
			pruned: []string{"prices/us"},
		},
		{
			name:   "Success: the nodes that only existed to reach the deleted path go with it",
			insert: []string{"prices/us/east/nyse", "prices/eu/lse"},
			delete: "prices/us/east/nyse",
			gone:   []string{"prices/us/east/nyse"},
			kept:   []string{"prices/eu/lse"},
			pruned: []string{"prices/us", "prices/us/east"},
		},
		{
			name:   "Success: deleting a path keeps the longer path below it",
			insert: []string{"prices/us", "prices/us/nyse"},
			delete: "prices/us",
			gone:   []string{"prices/us"},
			kept:   []string{"prices/us/nyse"},
		},
		{
			name:   "Success: deleting a path keeps the prefix above it",
			insert: []string{"prices/us", "prices/us/nyse"},
			delete: "prices/us/nyse",
			gone:   []string{"prices/us/nyse"},
			kept:   []string{"prices/us"},
		},
		{
			name:   "Success: deleting the middle child of a node leaves the others mapped to their own values",
			insert: []string{"prices/a", "prices/b", "prices/c", "prices/d"},
			delete: "prices/b",
			gone:   []string{"prices/b"},
			kept:   []string{"prices/a", "prices/c", "prices/d"},
		},
		{
			name:   "Success: deleting a path that is not in the tree changes nothing",
			insert: []string{"prices/us/nyse"},
			delete: "prices/eu/lse",
			kept:   []string{"prices/us/nyse"},
		},
		{
			name:   "Success: deleting a prefix that holds no value changes nothing",
			insert: []string{"prices/us/nyse"},
			delete: "prices/us",
			kept:   []string{"prices/us/nyse"},
		},
	}

	for _, test := range tests {
		tree := build(test.insert...).Delete(strings.Split(test.delete, "/"))

		for _, path := range test.gone {
			if v, ok := tree.Trace(strings.Split(path, "/")).Terminal(); ok {
				t.Errorf("TestDelete(%s): path(%s) still holds %q, want it gone", test.name, path, v)
			}
		}
		for _, path := range test.kept {
			v, ok := tree.Trace(strings.Split(path, "/")).Terminal()
			switch {
			case !ok:
				t.Errorf("TestDelete(%s): path(%s) is gone, want it kept", test.name, path)
			case v != path:
				t.Errorf("TestDelete(%s): path(%s): got %q, want %q", test.name, path, v, path)
			}
		}
		for _, path := range test.pruned {
			if !tree.Trace(strings.Split(path, "/")).Empty() {
				t.Errorf("TestDelete(%s): node(%s) is still there, want it pruned", test.name, path)
			}
		}
	}
}

// TestDeleteWide deletes out of a node holding enough children to be searched rather than scanned. A
// delete has to take the label and the child it belongs to out at the same index, so this checks that
// every child left over still maps to its own value rather than to a neighbor's.
func TestDeleteWide(t *testing.T) {
	t.Parallel()

	tree := wide(t).Delete([]string{"a", "c22"})

	if v, ok := tree.Trace([]string{"a", "c22"}).Terminal(); ok {
		t.Errorf("TestDeleteWide: the deleted path still holds %q, want it gone", v)
	}

	for i := 0; i < wideChildren; i++ {
		if i == 22 {
			continue
		}

		path := fmt.Sprintf("a/c%02d", i)
		v, ok := tree.Trace(strings.Split(path, "/")).Terminal()
		switch {
		case !ok:
			t.Errorf("TestDeleteWide: path(%s) is gone, want it kept", path)
		case v != path:
			t.Errorf("TestDeleteWide: path(%s): got %q, want %q. A label and its child are out of step.", path, v, path)
		}
	}

	if v, ok := tree.Trace([]string{"b", "zzz"}).Terminal(); !ok || v != "b/zzz" {
		t.Errorf("TestDeleteWide: the narrow sibling got %q(%v), want %q", v, ok, "b/zzz")
	}
}

// TestDeleteNothing checks that deleting a path that is not there hands back the Tree it was given rather
// than a copy of it, which is what keeps a delete that has nothing to do from copying nodes for no reason.
func TestDeleteNothing(t *testing.T) {
	t.Parallel()

	before := build("prices/us/nyse", "prices/eu/lse")
	after := before.Delete([]string{"trades", "us"})

	if before.root != after.root {
		t.Errorf("TestDeleteNothing: got a new tree, want the one that was passed in")
	}
}

// TestMutationShares checks the property the lock free read path rests on: a mutation copies the path it
// touches and shares everything else, so a Tree already handed out never changes under the reader holding
// it. This has to hold for Delete as much as Insert, as a Delete writing through to a published node would
// corrupt the Tree a Send() is walking.
func TestMutationShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// mutate returns the new Tree, path is what it added or removed, and wantAfter is whether the new
		// Tree holds path once mutate is done. shared is a subtree that the mutation does not touch.
		mutate    func(Tree[string, string]) Tree[string, string]
		path      []string
		wantAfter bool
	}{
		{
			name: "Success: an insert copies the path it touches and shares the rest",
			mutate: func(t Tree[string, string]) Tree[string, string] {
				return t.Insert([]string{"prices", "eu", "lse"}, "prices/eu/lse")
			},
			path:      []string{"prices", "eu", "lse"},
			wantAfter: true,
		},
		{
			name: "Success: a delete copies the path it touches and shares the rest",
			mutate: func(t Tree[string, string]) Tree[string, string] {
				return t.Delete([]string{"prices", "us", "nyse"})
			},
			path: []string{"prices", "us", "nyse"},
		},
	}

	for _, test := range tests {
		before := build("prices/us/nyse", "trades/us/nyse")
		after := test.mutate(before)

		// The Tree that was mutated does not move. This is the whole contract: a reader holding it sees
		// what it saw before, whichever way the tree was changed.
		if _, ok := before.Trace([]string{"prices", "us", "nyse"}).Terminal(); !ok {
			t.Errorf("TestMutationShares(%s): the Tree that was mutated changed, want it left alone", test.name)
		}
		if _, ok := after.Trace(test.path).Terminal(); ok != test.wantAfter {
			t.Errorf("TestMutationShares(%s): the new Tree holds the path == %v, want %v", test.name, ok, test.wantAfter)
		}

		// The subtree off the path of the mutation is the same nodes, not a copy of them.
		if before.Child("trades").root != after.Child("trades").root {
			t.Errorf("TestMutationShares(%s): got the untouched subtree copied, want it shared", test.name)
		}
		// The path of the mutation is copied, not written through.
		if before.Child("prices").root == after.Child("prices").root {
			t.Errorf("TestMutationShares(%s): got the path of the mutation shared, want it copied", test.name)
		}
	}
}

// TestWideNode pins the child lookup on a node with enough children to take the binary search. A search
// for a label above every child used to run off the end of the children and match a node belonging to
// another parent, which handed back a subtree the path never led to.
func TestWideNode(t *testing.T) {
	t.Parallel()

	tree := wide(t)

	tests := []struct {
		name   string
		path   string
		want   string
		wantOK bool
	}{
		{
			name:   "Success: a child in the middle of a wide node is found",
			path:   "a/c07",
			want:   "a/c07",
			wantOK: true,
		},
		{
			name:   "Success: the first child of a wide node is found",
			path:   "a/c00",
			want:   "a/c00",
			wantOK: true,
		},
		{
			name:   "Success: the last child of a wide node is found",
			path:   "a/c44",
			want:   "a/c44",
			wantOK: true,
		},
		{
			name:   "Success: a path under a narrow sibling of a wide node is found",
			path:   "b/zzz",
			want:   "b/zzz",
			wantOK: true,
		},
		{
			name: "Success: a path sorting above every child of a wide node is not found",
			path: "a/zzz",
		},
		{
			name: "Success: a path sorting below every child of a wide node is not found",
			path: "a/aaa",
		},
		{
			name: "Success: a path between two children of a wide node is not found",
			path: "a/c07x",
		},
	}

	for _, test := range tests {
		got, ok := tree.Trace(strings.Split(test.path, "/")).Terminal()

		switch {
		case ok != test.wantOK:
			t.Errorf("TestWideNode(%s): got ok == %v, want ok == %v (value %q)", test.name, ok, test.wantOK, got)
		case got != test.want:
			t.Errorf("TestWideNode(%s): got %q, want %q", test.name, got, test.want)
		}
	}
}
