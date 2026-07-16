# trie

An immutable trie with copy-on-write mutation, used by `subscriber.Value` to hold the topic patterns that
`Send()` matches against.

`Insert()` and `Delete()` return a new `Tree` that shares every node they did not have to touch with the
one they came from. Only the path from the root to the changed node is copied. That is what lets `Send()`
walk the trie with no lock at all: it holds a `Tree` that can no longer change, while a writer builds its
own and publishes it with a single atomic store.

Each node holds the labels of its children inline, next to the child pointers, so looking one up walks a
run of contiguous labels rather than chasing a pointer per step. Small nodes are scanned, wide ones are
binary searched.

## History

This started as the array-based trie from https://github.com/acomagu/trie (MIT licensed), which was in
turn based on https://engineering.linecorp.com/ja/blog/simple-tries. That structure was a flat `[]node`
built by a single `New(keys, values)` constructor, with no insert and no delete, so every change to the
pattern set meant sorting every key and rebuilding the whole array: 403µs and 261KB per subscription at
1000 patterns, and once per subscriber on teardown. It was replaced by the copy-on-write trie here, which
does not rebuild.

Two bugs in the original are worth recording, as their tests remain:

- `Trace()` binary searched the children of a node with more than 40 of them, and its bounds check let the
  search land one past the last child, which is the first node of an unrelated parent. A label match there
  returned a subtree the traced path never led to — for the subscriber, a value delivered to the wrong
  subscription. `TestWideNode` pins it.
- `Predict()` and `Children()` were never used by the subscriber and are gone.
