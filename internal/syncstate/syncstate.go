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
type State struct {
	mu      sync.RWMutex
	pending map[uint64]struct{}
	done    uint32
	doneCh  chan struct{}
	doneOnce sync.Once
}

// New creates a new sync state.
func New() *State {
	return &State{
		pending: make(map[uint64]struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Reset rebuilds the pending set from the given partition IDs.
// Call when the routing table is updated. Closes the previous Done channel
// to wake any waiters so they can re-check or wait on the new cycle.
// If partitionIDs is empty, signals done immediately.
func (s *State) Reset(partitionIDs []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pending = make(map[uint64]struct{}, len(partitionIDs))
	for _, id := range partitionIDs {
		s.pending[id] = struct{}{}
	}
	atomic.StoreUint32(&s.done, 0)
	s.doneOnce.Do(func() { close(s.doneCh) })
	s.doneCh = make(chan struct{})
	s.doneOnce = sync.Once{}
	if len(s.pending) == 0 {
		s.signalDoneLocked()
	}
}

// MarkReceived marks partition partID as having received data.
func (s *State) MarkReceived(partID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.pending, partID)
	if len(s.pending) == 0 {
		s.signalDoneLocked()
	}
}

// StartEmptyPartitionTimeout starts a timer that signals done when it fires.
// Partitions that never receive data (empty on source) would otherwise block
// WaitForInitialSync forever. After timeout, any remaining pending partitions
// are considered "received" (empty).
func (s *State) StartEmptyPartitionTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			s.mu.Lock()
			if len(s.pending) > 0 {
				s.pending = make(map[uint64]struct{})
				s.signalDoneLocked()
			}
			s.mu.Unlock()
		}
	}()
}

func (s *State) signalDoneLocked() {
	atomic.StoreUint32(&s.done, 1)
	s.doneOnce.Do(func() { close(s.doneCh) })
}

// PendingEmpty returns true if there are no partitions pending receive.
func (s *State) PendingEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pending) == 0
}

// IsDone returns true if initial sync is complete.
func (s *State) IsDone() bool {
	return atomic.LoadUint32(&s.done) == 1
}

// Done returns a channel that closes when initial sync is complete.
func (s *State) Done() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doneCh
}
