/*
Package sequencer provides a per-(key, barrier) turn gate that admits work in a dense sequence
order. It is the primitive under keyed ordering: items sharing a key are assigned a dense,
0-based sequence, and a Sequencer lets each item enter a critical section only after every
lower sequence has resolved. Different keys use different Sequencers and never contend.

A Sequencer is used in two ways, which correspond to the two ways an item resolves its sequence
at a barrier:

  - Visit: the item reaches the barrier. It calls Wait to block until its turn, runs the critical
    section, then calls Done. Wait returns nil only when it is exactly this sequence's turn.
  - Exit: the item leaves without ever reaching the barrier (a branch, an error, cancellation). It
    calls Done alone to clear its slot so successors are not stranded.

Both paths converge on Done: it advances the turn when the resolving sequence is the current one,
and records it for later when it resolves out of order. Every sequence assigned for a key MUST be
resolved exactly once by exactly one of these paths, or a successor waits forever.

The lowest unresolved sequence is always the turn, and the item holding it never blocks in Wait,
so a Sequencer cannot deadlock as long as every assigned sequence is eventually resolved. That is
the caller's contract; it is what the exit path exists to guarantee.

Contract (violations panic, as they indicate a bug in the caller, not a runtime condition):

  - Each sequence is resolved at most once. Calling Done twice for one sequence, or calling Wait
    for a sequence the turn has already passed (a re-entered barrier), panics. The caller enforces
    at-most-once — a Sequencer does not track which items have already cleared it.
  - The goroutine that Waits a sequence is the one that later Dones it (on the visit path).

Abort is the teardown path: it wakes every blocked Wait with ErrAborted and makes all future Waits
return ErrAborted. It is used when a panic tears the pipeline down; a Sequencer does no recovery of
its own.
*/
package sequencer

import (
	"fmt"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/errors"
)

// ErrAborted is returned by Wait once Abort has been called. It is permanent: an aborted Sequencer
// never reopens.
var ErrAborted = errors.New("sequencer: aborted")

// Sequencer admits work in dense sequence order for a single key/barrier. The zero value is not
// usable; construct one with New. A Sequencer must not be copied after first use.
type Sequencer struct {
	mu sync.Mutex
	// turn is the next sequence permitted to enter. It is also the lowest unresolved sequence, so the
	// item holding it never blocks.
	turn uint64
	// ahead holds sequences greater than turn that have already resolved out of order. When turn
	// reaches one, Done drains it and keeps advancing.
	ahead map[uint64]struct{}
	// changed is closed and replaced whenever turn advances or Abort runs. Waiters observe the
	// channel they read, block on it, then re-check — the gate.go broadcast pattern.
	changed chan struct{}
	aborted bool
}

// New constructs a Sequencer whose first admitted sequence is 0.
func New() *Sequencer {
	return &Sequencer{ahead: map[uint64]struct{}{}, changed: make(chan struct{})}
}

// signal wakes every current waiter by closing the observed channel and installing a fresh one for
// the next round. The caller must hold s.mu.
func (s *Sequencer) signal() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// Wait blocks until it is seq's turn to enter the critical section. It returns nil when the turn
// reaches seq — the caller may then run the critical section and MUST call Done(seq) afterward. It
// returns ctx.Err() if ctx is cancelled first, or ErrAborted if the Sequencer is torn down; in
// both error cases the caller did NOT enter and must NOT call Done from the visit path — the exit
// path is what resolves seq. ctx is checked before the turn, so an already-cancelled ctx returns
// deterministically without entering even when it is seq's turn.
//
// Wait panics if the turn has already passed seq, which means seq was resolved twice or reused —
// a caller bug (for example a barrier re-entered without the at-most-once guard).
func (s *Sequencer) Wait(ctx context.Context, seq uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		switch {
		case s.aborted:
			s.mu.Unlock()
			return ErrAborted
		case seq < s.turn:
			s.mu.Unlock()
			panic(fmt.Sprintf("bug: sequencer.Wait(%d) but turn is already %d; sequence resolved twice or reused", seq, s.turn))
		case seq == s.turn:
			s.mu.Unlock()
			return nil
		}
		ch := s.changed
		s.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Done resolves seq and advances the turn as far as the contiguously resolved sequences allow,
// waking the waiter for the new turn. When seq is the current turn it advances immediately and
// drains any higher sequences already resolved into ahead; when seq is higher than the turn it is
// recorded in ahead until the turn reaches it. Recording into ahead wakes no one, as no visit
// waiter can exist for a sequence resolved by the exit path.
//
// Once Abort has been called Done is a no-op: torn-down work is abandoned, not resolved, so an
// exit-clear that races Abort during teardown neither advances the turn nor panics.
//
// Done panics if seq is below the turn, or is already recorded in ahead — either means seq was
// resolved twice, a caller bug.
func (s *Sequencer) Done(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return
	}

	switch {
	case seq < s.turn:
		panic(fmt.Sprintf("bug: sequencer.Done(%d) but turn is already %d; sequence resolved twice", seq, s.turn))
	case seq > s.turn:
		if _, dup := s.ahead[seq]; dup {
			panic(fmt.Sprintf("bug: sequencer.Done(%d) called twice while awaiting turn %d", seq, s.turn))
		}
		s.ahead[seq] = struct{}{}
		return
	default:
		s.turn++
		for {
			if _, ok := s.ahead[s.turn]; !ok {
				break
			}
			delete(s.ahead, s.turn)
			s.turn++
		}
		s.signal()
	}
}

// Abort tears the Sequencer down: every blocked Wait returns ErrAborted and every later Wait
// returns ErrAborted at once. It does not advance the turn — torn-down work is abandoned, not
// resolved. Abort is idempotent and safe to call from a goroutine other than the waiters'.
func (s *Sequencer) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.aborted {
		return
	}
	s.aborted = true
	s.signal()
}

// Turn reports the next sequence permitted to enter, which is also the lowest unresolved sequence.
// A key's Sequencer is fully drained — and may be reaped — once Turn passes the highest sequence
// assigned for that key. It is a snapshot, valid only until the next Done.
func (s *Sequencer) Turn() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turn
}
