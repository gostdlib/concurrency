/*
Package trie provides an immutable trie with copy-on-write mutation. A Tree is never changed in place:
Insert() and Delete() return a new Tree that shares every node they did not have to touch with the old one.
Only the path from the root to the changed node is copied, so a mutation costs the fanout along that path
rather than a rebuild of the whole structure.

That is what lets a reader hold a Tree with no lock while a writer mutates: the reader keeps walking the
Tree it has, which no longer changes, and the writer publishes its new one when it is done.

Usage:

	var t trie.Tree[string, int]

	t = t.Insert([]string{"prices", "us"}, 1)
	t = t.Insert([]string{"prices", "eu"}, 2)

	if v, ok := t.Trace([]string{"prices", "us"}).Terminal(); ok {
		fmt.Println(v) // 1
	}

	t = t.Delete([]string{"prices", "us"})
*/
package trie

import (
	"cmp"
	"slices"
)

// Tree is an immutable trie keyed by a path of K. The zero value is an empty Tree that is ready to use.
// A Tree is a value: copying one is copying a pointer to the shared, immutable nodes below it.
type Tree[K cmp.Ordered, V any] struct {
	root *node[K, V]
}

// node is one label in a path. Nodes are immutable once published: labels and children are sorted by
// label, run in step with each other, and are never written to after the node reaches a Tree that anyone
// else can see. match reports whether a value was stored at this node, which is what tells a key apart
// from the prefix of a longer one.
//
// The labels of the children are held here rather than on the children themselves so that the search for
// one walks a run of contiguous labels instead of chasing a pointer into a scattered node to read each
// one. On a node with many children that is the difference between touching a handful of cache lines and
// taking a cache miss per step of the search, and the search is on the hot path of every Send().
type node[K cmp.Ordered, V any] struct {
	labels   []K
	children []*node[K, V]
	value    V
	match    bool
}

// clone is a shallow copy with its own labels and children, which is what makes a node on the path of a
// mutation safe to change without disturbing a reader walking the Tree it came from.
func (n *node[K, V]) clone() *node[K, V] {
	c := *n
	c.labels = slices.Clone(n.labels)
	c.children = slices.Clone(n.children)
	return &c
}

// linear is the number of children up to which find() scans rather than searching. A scan gets to lead
// with an equality test, which for a string is a length check and a memequal, where a search has to order
// the two labels and pay for a comparison whether or not it has found what it wants. Most nodes hold a
// handful of children, so the scan is what the hot path usually takes.
const linear = 8

// find returns the index of the child labeled label. If there is no such child, it returns the index the
// child would be inserted at to keep labels and children sorted.
func (n *node[K, V]) find(label K) (int, bool) {
	if len(n.labels) <= linear {
		for i, l := range n.labels {
			switch {
			case l == label:
				return i, true
			case l > label:
				return i, false
			}
		}
		return len(n.labels), false
	}

	lo, hi := 0, len(n.labels)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch l := n.labels[mid]; {
		case l < label:
			lo = mid + 1
		case l > label:
			hi = mid
		default:
			return mid, true
		}
	}
	return lo, false
}

// Empty reports whether t holds nothing.
func (t Tree[K, V]) Empty() bool {
	return t.root == nil
}

// Child returns the subtree below the child of t's root labeled label. It returns an empty Tree if there
// is no such child.
func (t Tree[K, V]) Child(label K) Tree[K, V] {
	if t.root == nil {
		return Tree[K, V]{}
	}

	i, ok := t.root.find(label)
	if !ok {
		return Tree[K, V]{}
	}
	return Tree[K, V]{root: t.root.children[i]}
}

// Trace returns the subtree of t reached by walking path from t's root. It returns an empty Tree if path
// is not in t.
func (t Tree[K, V]) Trace(path []K) Tree[K, V] {
	for _, label := range path {
		t = t.Child(label)
		if t.root == nil {
			return Tree[K, V]{}
		}
	}
	return t
}

// Terminal returns the value stored at t's root. The second return reports whether a value was stored
// there at all, which is false for a node that only exists as the prefix of a longer path.
func (t Tree[K, V]) Terminal() (V, bool) {
	if t.root == nil {
		var zero V
		return zero, false
	}
	return t.root.value, t.root.match
}

// Insert returns a Tree holding everything in t plus v stored at path. It does not modify t. A value
// already at path is replaced rather than reported, so a caller that must not overwrite one has to check
// for it first. An empty path stores v at the root.
func (t Tree[K, V]) Insert(path []K, v V) Tree[K, V] {
	root := t.root
	if root == nil {
		root = &node[K, V]{}
	}
	return Tree[K, V]{root: root.insert(path, v)}
}

// insert returns a copy of n with v stored at path below it.
func (n *node[K, V]) insert(path []K, v V) *node[K, V] {
	c := n.clone()

	if len(path) == 0 {
		c.value = v
		c.match = true
		return c
	}

	// Everything off the path is left alone, so the new node shares it with the node it was copied from.
	i, ok := c.find(path[0])
	if ok {
		c.children[i] = c.children[i].insert(path[1:], v)
		return c
	}
	c.labels = slices.Insert(c.labels, i, path[0])
	c.children = slices.Insert(c.children, i, chain[K, V](path[1:], v))
	return c
}

// chain builds the nodes for a path that is not in the Tree yet, holding v at its end. The node it
// returns is the one its caller files under the label it took off the front of the path.
func chain[K cmp.Ordered, V any](path []K, v V) *node[K, V] {
	n := &node[K, V]{value: v, match: true}
	for i := len(path) - 1; i >= 0; i-- {
		n = &node[K, V]{labels: []K{path[i]}, children: []*node[K, V]{n}}
	}
	return n
}

// Delete returns a Tree holding everything in t except what was stored at path, along with any node that
// only existed to reach it. It does not modify t. Deleting a path that is not in t returns t itself,
// copying nothing. An empty path deletes what is stored at the root.
func (t Tree[K, V]) Delete(path []K) Tree[K, V] {
	if t.root == nil {
		return t
	}

	root, changed := t.root.remove(path)
	if !changed {
		return t
	}
	return Tree[K, V]{root: root}
}

// remove returns a copy of n without the value at path below it. It returns nil for a node left holding
// neither a value nor a child, which tells the parent to prune it, so a deleted path does not leave the
// nodes that only existed to reach it behind. The bool reports whether anything was removed at all, which
// keeps a delete of a path that is not there from copying nodes for no reason.
func (n *node[K, V]) remove(path []K) (*node[K, V], bool) {
	if len(path) == 0 {
		if !n.match {
			return n, false
		}

		c := n.clone()
		var zero V
		c.value = zero
		c.match = false

		if len(c.children) == 0 {
			return nil, true
		}
		return c, true
	}

	i, ok := n.find(path[0])
	if !ok {
		return n, false
	}

	child, changed := n.children[i].remove(path[1:])
	if !changed {
		return n, false
	}

	c := n.clone()
	if child == nil {
		c.labels = slices.Delete(c.labels, i, i+1)
		c.children = slices.Delete(c.children, i, i+1)
	} else {
		c.children[i] = child
	}

	if len(c.children) == 0 && !c.match {
		return nil, true
	}
	return c, true
}
