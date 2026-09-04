/*
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

package dmap

import "sync"

const (
	// keyLockStripes is the number of mutexes the key locks are spread over.
	// A power of two, so a hashed key picks its stripe with a mask. 1024
	// stripes cost 8 KB per service, and two concurrent writes to different
	// keys share a stripe with probability 1/1024.
	keyLockStripes = 1024
)

// keyLocks serializes the operations on one key end to end, replication
// included, so that a key's writes reach its replicas in the order the primary
// applied them, while the fragment lock is held only around the storage
// accesses and never across a network call: a replica that does not answer
// stalls the writes of that key alone, not the reads, key counts and moves of
// the whole fragment. Keys are mapped onto a fixed set of stripes by their
// hash, so the structure is allocated once with the service and an operation
// acquires a single, usually uncontended, mutex.
//
// The order guarantee holds for synchronous replication, where the replica
// writes complete under the key lock. With AsyncReplicationMode the replica
// writes run in the background after the lock is released, so two writes of
// one key may reach a replica in either order; the replica keeps the last
// one it receives until a read repairs it.
type keyLocks [keyLockStripes]sync.Mutex

// keyLock returns the stripe that serializes the operations on hkey. The lock
// order is key lock first, fragment lock second, everywhere.
func (x *Service) keyLock(hkey uint64) *sync.Mutex {
	return &x.keys[hkey&(keyLockStripes-1)]
}
