package subscriber

import (
	"fmt"
	"strings"

	"github.com/gostdlib/base/retry/exponential"
)

// invalidPath returns an ErrInvalidPath saying what is wrong with a path. Every one of them is a caller
// that wrote the path wrong, which no amount of retrying turns into a path that parses, so they are all
// permanent. Going through here rather than calling fmt.Errorf() at each site is what keeps that true of a
// reason added later.
func invalidPath(reason string, args ...any) error {
	return fmt.Errorf("%w: %s: %w", ErrInvalidPath, fmt.Sprintf(reason, args...), exponential.ErrPermanent)
}

// star matches exactly one segment and may appear anywhere in a pattern, as often as it is wanted.
// doubleStar matches zero or more segments and may only be the last segment of a pattern. Both are only
// wildcards when they make up an entire segment, so a topic like "a/b*" holds no wildcard, it is just an
// invalid pattern.
const (
	star       = "*"
	doubleStar = "**"
)

// segments splits a path into its segments, appending them to buf. A leading / is optional and is not
// part of the canonical form, so "/a/b" and "a/b" are the same path. The path may not be empty, end in a
// /, or hold an empty segment. buf may be nil, but the send path hands it an array so that splitting a
// topic does not reach the heap, which is why this does not simply call strings.Split().
func segments(path string, buf []string) ([]string, error) {
	switch {
	case path == "":
		return nil, invalidPath("a path cannot be empty")
	case strings.HasSuffix(path, "/"):
		return nil, invalidPath("path(%s) cannot end in a /", path)
	}

	segs := buf[:0]
	rest := strings.TrimPrefix(path, "/")
	for {
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			break
		}
		if i == 0 {
			return nil, invalidPath("path(%s) cannot have an empty segment", path)
		}
		segs = append(segs, rest[:i])
		rest = rest[i+1:]
	}
	return append(segs, rest), nil
}

// topicSegs splits a topic that is being sent to, appending its segments to buf. A topic must be fully
// qualified, so it may not hold a wildcard. * is rejected anywhere in a segment, not just as a whole
// segment, as no valid pattern can match it literally.
func topicSegs(topic string, buf []string) ([]string, error) {
	segs, err := segments(topic, buf)
	if err != nil {
		return nil, err
	}

	for _, seg := range segs {
		if strings.Contains(seg, star) {
			return nil, invalidPath("topic(%s) cannot contain a wildcard", topic)
		}
	}
	return segs, nil
}

// patternSegs splits a pattern that is being subscribed to. A pattern may use * to match a single segment
// and ** to match zero or more segments, but a wildcard must make up an entire segment. A ** may only be
// the last segment, which is the rule MQTT and NATS hold their multi-level wildcards to. A ** anywhere else
// makes matching cost grow with the number of ways the topic can be split between the **s, which a
// subscriber could otherwise turn into an arbitrarily expensive Send(). It also implies that a pattern
// holds at most one **, as a second one cannot also be last. This is on the subscribe path, so it does not
// bother with a buffer.
func patternSegs(pattern string) ([]string, error) {
	segs, err := segments(pattern, nil)
	if err != nil {
		return nil, err
	}

	for i, seg := range segs {
		switch {
		case strings.Contains(seg, star) && seg != star && seg != doubleStar:
			return nil, invalidPath("pattern(%s) has segment(%s): a wildcard must be an entire segment", pattern, seg)
		case seg == doubleStar && i != len(segs)-1:
			return nil, invalidPath("pattern(%s): a ** must be the last segment, so a pattern can hold only one", pattern)
		}
	}
	return segs, nil
}

// canonical is the form a path takes as a key: no leading /, segments joined by /.
func canonical(segs []string) string {
	return strings.Join(segs, "/")
}
