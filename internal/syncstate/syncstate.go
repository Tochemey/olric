/*
 * Copyright 2018-2024 Burak Sezer
 * Copyright 2025-2026 Arsene Tochemey Gandote
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package syncstate

import (
	"sync"
	"sync/atomic"
	"time"
)

// State tracks which partitions a node is waiting to receive data for.
// Used to delay rebalance ACK until initial sync is complete.
//
// Each tracked partition records when it first became pending. A partition
// counts as pending only while its escape deadline has not elapsed: one that
// never receives data (legitimately empty on the source) escapes once its
// deadline passes and stays escaped, no matter how often the routing table is
// reconciled, because Reconcile preserves the original timestamps. Cluster
// churn faster than the escape delay therefore cannot starve the rebalance
// ACK, and expired entries are kept (not deleted) so a subsequent Reconcile
// cannot resurrect them with a fresh clock.
type State struct {
	mu      sync.Mutex
	pending map[uint64]time.Time
	// receivedAt records when each partition last received data since the
	// previous Reconcile. A routing table install scans the empty partitions,
	// asks their owners over the network, and only then reconciles; a
	// fragment that lands in between would otherwise be marked received
	// before the partition is tracked, and the partition would wait out the
	// whole escape for data it already holds. Reconcile skips partitions
	// received after the scan it reconciles for, then forgets the marks.
	receivedAt map[uint64]time.Time
	escape     time.Duration
	timer      *time.Timer
	done       uint32
	doneCh     chan struct{}
	doneOnce   sync.Once
}

// New creates a new sync state.
func New() *State {
	return &State{
		pending:    make(map[uint64]time.Time),
		receivedAt: make(map[uint64]time.Time),
		doneCh:     make(chan struct{}),
	}
}

// Reconcile rebuilds the tracked set from the given partition IDs, which were
// found empty by a scan taken at scannedAt. Call when the routing table is
// updated. Partitions already tracked keep their original timestamp (their
// escape clock never restarts, even after it has expired), new ones start now,
// entries not in partitionIDs are dropped, and a partition that received data
// after scannedAt is not tracked: it was delivered while the table was being
// installed, see receivedAt.
//
// escape is the per-partition deadline after which a tracked partition that
// received no data stops counting as pending; escape <= 0 disables expiry,
// so only MarkReceived clears entries.
//
// Closes the previous Done channel to wake any waiters so they can re-check
// or wait on the new cycle. If nothing is effectively pending, signals done
// immediately.
func (x *State) Reconcile(partitionIDs []uint64, escape time.Duration, scannedAt time.Time) {
	now := time.Now()

	x.mu.Lock()
	defer x.mu.Unlock()

	x.escape = escape
	next := make(map[uint64]time.Time, len(partitionIDs))
	for _, id := range partitionIDs {
		if received, ok := x.receivedAt[id]; ok && received.After(scannedAt) {
			continue
		}

		first, ok := x.pending[id]
		if !ok {
			first = now
		}
		next[id] = first
	}

	x.pending = next
	// Every mark so far is either consumed above or predates the scan, whose
	// view of the partitions already reflects it.
	x.receivedAt = make(map[uint64]time.Time)

	atomic.StoreUint32(&x.done, 0)
	x.doneOnce.Do(func() { close(x.doneCh) })
	x.doneCh = make(chan struct{})
	x.doneOnce = sync.Once{}
	x.refreshLocked(now)
}

// Expired reports whether partID is tracked and has outlived its escape
// deadline. Such a partition no longer holds back PendingEmpty.
func (x *State) Expired(partID uint64) bool {
	now := time.Now()

	x.mu.Lock()
	defer x.mu.Unlock()

	first, ok := x.pending[partID]
	if !ok || x.escape <= 0 {
		return false
	}

	return now.Sub(first) >= x.escape
}

// MarkReceived marks partition partID as having received data, whether or not
// it is tracked yet: a mark that precedes the Reconcile of an ongoing install
// keeps that Reconcile from tracking the partition.
func (x *State) MarkReceived(partID uint64) {
	now := time.Now()

	x.mu.Lock()
	defer x.mu.Unlock()

	delete(x.pending, partID)
	x.receivedAt[partID] = now
	x.refreshLocked(now)
}

// unexpiredCountLocked returns how many tracked partitions still count as
// pending, i.e. their escape deadline has not elapsed. Must be called with mu
// locked.
func (s *State) unexpiredCountLocked(now time.Time) int {
	if s.escape <= 0 {
		return len(s.pending)
	}
	n := 0
	for _, first := range s.pending {
		if now.Sub(first) < s.escape {
			n++
		}
	}
	return n
}

// refreshLocked recomputes the done state and (re)arms the single backstop
// timer to the earliest unexpired deadline so Done fires even when nothing
// polls PendingEmpty. Must be called with mu locked.
func (s *State) refreshLocked(now time.Time) {
	if s.unexpiredCountLocked(now) == 0 {
		if s.timer != nil {
			s.timer.Stop()
		}
		s.signalDoneLocked()
		return
	}

	if s.escape <= 0 {
		// No deadlines to watch; only MarkReceived clears entries.
		return
	}

	var earliest time.Time
	for _, first := range s.pending {
		if now.Sub(first) >= s.escape {
			continue
		}
		if earliest.IsZero() || first.Before(earliest) {
			earliest = first
		}
	}

	d := earliest.Add(s.escape).Sub(now)
	if d < time.Millisecond {
		d = time.Millisecond
	}

	if s.timer == nil {
		s.timer = time.AfterFunc(d, s.expire)
	} else {
		s.timer.Reset(d)
	}
}

func (s *State) expire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked(time.Now())
}

func (s *State) signalDoneLocked() {
	atomic.StoreUint32(&s.done, 1)
	s.doneOnce.Do(func() { close(s.doneCh) })
}

// PendingEmpty returns true if no partition is effectively pending receive:
// every tracked partition either received data or outlived its escape
// deadline.
func (s *State) PendingEmpty() bool {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unexpiredCountLocked(now) > 0 {
		return false
	}
	s.signalDoneLocked()
	return true
}

// IsDone returns true if initial sync is complete.
func (s *State) IsDone() bool {
	if atomic.LoadUint32(&s.done) == 1 {
		return true
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unexpiredCountLocked(now) == 0 {
		s.signalDoneLocked()
		return true
	}
	return false
}

// Done returns a channel that closes when initial sync is complete.
func (s *State) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doneCh
}
