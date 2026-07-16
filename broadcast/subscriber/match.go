package subscriber

import (
	"github.com/gostdlib/concurrency/broadcast/subscriber/internal/trie"
)

// match returns the value of every pattern in t that matches segs, which are the segments of a fully
// qualified topic. Patterns are stored in t keyed by their segments, with * and ** held as ordinary
// labels, so matching is a walk of t that branches into the wildcard labels at each segment. Matches are
// appended to buf, which may be nil, but the send path hands it an array so that a topic matching a
// handful of patterns does not reach the heap. The walk threads the result by value rather than through a
// pointer, as escape analysis gives up on a pointer handed to a recursive call and would push buf onto
// the heap.
func match[V comparable](t trie.Tree[string, V], segs []string, buf []V) []V {
	return matchInto(t, segs, buf[:0])
}

// matchInto walks t against segs and returns out with what it found appended. Child() on a label that is
// not there returns an empty Tree and Terminal() on that reports no value, so a branch that leads nowhere
// costs a nil check. Every call descends a level of t, so this terminates.
//
// A node is reached by exactly one path from the root and every step of that path eats one segment, so a
// node is only ever walked with the offset into segs that its depth names. It is therefore visited at most
// once and the walk costs the nodes it touches, with no split of the topic to guess at. That is what
// patternSegs() buys by holding ** to the last segment.
func matchInto[V comparable](t trie.Tree[string, V], segs []string, out []V) []V {
	if t.Empty() {
		return out
	}

	// A ** is the last segment of its pattern, so a ** below this node is a pattern that ends there and it
	// takes whatever is left of the topic, down to nothing at all.
	if v, ok := t.Child(doubleStar).Terminal(); ok {
		out = append(out, v)
	}

	// The topic is used up, so a pattern that ends here matches.
	if len(segs) == 0 {
		if v, ok := t.Terminal(); ok {
			out = append(out, v)
		}
		return out
	}

	out = matchInto(t.Child(segs[0]), segs[1:], out)
	return matchInto(t.Child(star), segs[1:], out)
}
