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

package routingtable

import (
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/consistent"
	"github.com/tochemey/olric/internal/discovery"
)

// memberIndex is the membership snapshot a table computation works from: the
// live members by name, read from memberlist once per computation instead of
// once per owner per partition, which decoded every member's metadata each
// time.
type memberIndex map[string]discovery.Member

// snapshotMembers reads the live members from memberlist once.
func (x *RoutingTable) snapshotMembers() memberIndex {
	members := x.discovery.GetMembers()
	index := make(memberIndex, len(members))
	for _, member := range members {
		index[member.Name] = member
	}

	return index
}

// ownerCounts holds the key counts probed for a table computation, by owner
// and query; -1 marks a query the owner did not answer.
type ownerCounts map[discovery.Member]map[countQuery]int64

// count returns the probed count for owner and query, -1 when the query was
// not probed or not answered.
func (x ownerCounts) count(owner discovery.Member, query countQuery) int64 {
	counts, ok := x[owner]
	if !ok {
		return -1
	}

	count, ok := counts[query]
	if !ok {
		return -1
	}

	return count
}

// liveOwners returns a copy of owners, the recorded owners of a partition's
// primary or backup copies, without the members that left the cluster or
// rejoined under the same name with a new identity, as index reports them.
func (x *RoutingTable) liveOwners(partID uint64, owners []discovery.Member, index memberIndex, kind partitions.Kind) []discovery.Member {
	live := make([]discovery.Member, 0, len(owners))

	for _, owner := range owners {
		current, ok := index[owner.Name]
		if !ok {
			x.log.V(6).Printf("[DEBUG] Failed to find %s in the cluster: member not found", owner)
			x.log.V(3).Printf("[INFO] Member: %s has been deleted from the %s owners list of PartID: %v", owner, kind, partID)
			continue
		}

		if !owner.CompareByID(current) {
			x.log.V(3).Printf("[WARN] One of the partitions owners is probably re-joined: %s", current)
			continue
		}

		live = append(live, owner)
	}

	return live
}

// probeOwners asks every owner in queries for its key counts in the
// partitions it is a recorded owner of, in one pipelined round trip per owner
// and concurrently across owners, and returns the counts. An owner that does
// not answer is left in place by the callers: if the node is down, memberlist
// sends a leave event.
func (x *RoutingTable) probeOwners(queries map[discovery.Member][]countQuery) ownerCounts {
	var mtx sync.Mutex
	counts := make(ownerCounts, len(queries))

	var g errgroup.Group
	g.SetLimit(clusterCallConcurrency)

	for owner, ownerQueries := range queries {
		g.Go(func() error {
			probed, _ := x.keyCounts(owner, ownerQueries)
			byQuery := make(map[countQuery]int64, len(ownerQueries))
			for i, query := range ownerQueries {
				byQuery[query] = probed[i]
			}

			mtx.Lock()
			counts[owner] = byQuery
			mtx.Unlock()

			return nil
		})
	}

	// The probes report through counts; the group only bounds concurrency.
	_ = g.Wait()

	return counts
}

// placePrimaryOwner returns the primary owners of partID: the live recorded
// owners that still hold data, in order, with the owner the hash ring
// designates last. A recorded owner whose primary copy is empty is dropped;
// one that did not answer is kept.
func (x *RoutingTable) placePrimaryOwner(partID uint64, owners []discovery.Member, counts ownerCounts) []discovery.Member {
	newOwner := x.consistent.GetPartitionOwner(int(partID)).(discovery.Member)

	kept := make([]discovery.Member, 0, len(owners)+1)
	for _, owner := range owners {
		if counts.count(owner, countQuery{partID: partID}) == 0 {
			// Empty partition. Delete it from ownership list.
			continue
		}

		kept = append(kept, owner)
	}

	// Here add the new partition owner.
	for i, owner := range kept {
		if owner.CompareByID(newOwner) {
			// Remove it from the current position
			kept = append(kept[:i], kept[i+1:]...)
			// Append it again to head
			return append(kept, newOwner)
		}
	}

	return append(kept, newOwner)
}

func (r *RoutingTable) getReplicaOwners(partID uint64) ([]consistent.Member, error) {
	for i := r.config.ReplicaCount; i > 0; i-- {
		newOwners, err := r.consistent.GetClosestNForPartition(int(partID), i)
		if errors.Is(err, consistent.ErrInsufficientMemberCount) {
			continue
		}
		if err != nil {
			// Fail early
			return nil, err
		}
		return newOwners, nil
	}
	return nil, consistent.ErrInsufficientMemberCount
}

func isOwner(member discovery.Member, owners []consistent.Member) bool {
	for _, owner := range owners {
		if member.Name == owner.String() {
			return true
		}
	}
	return false
}

// placeBackupOwners returns the backup owners of partID: the live recorded
// owners that still hold a replica, in order, followed by the owners the hash
// ring designates, each moved to the tail when already present. A recorded
// owner whose replica is empty is dropped; one that did not answer is kept.
func (x *RoutingTable) placeBackupOwners(partID uint64, owners []discovery.Member, counts ownerCounts) []discovery.Member {
	newOwners, err := x.getReplicaOwners(partID)
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to get replica owners for PartID: %d: %v",
			partID, err)
		return nil
	}

	// Remove the primary owner
	newOwners = newOwners[1:]

	kept := make([]discovery.Member, 0, len(owners)+len(newOwners))
	for _, backup := range owners {
		count := counts.count(backup, countQuery{partID: partID, replica: true})
		if count == 0 {
			// Empty node, delete it.
			continue
		}

		if count > 0 && !isOwner(backup, newOwners) {
			// About this scenario:
			//
			// * ReplicaCount = 3
			// * Create three nodes and insert some keys
			// * Kill one of the nodes
			// * Now we have replicas that it's impossible to transfer its ownership
			// * Since we cannot drop a healthy replica, we prefer to keep it until
			//   a new node joined. Then, we transfer the ownership safely.
			// * During this incident, a node owns a primary and backup replicas at the same time.
			x.log.V(3).Printf("[WARN] %s hosts primary and replica copies "+
				"for PartID: %d", backup, partID)
		}

		kept = append(kept, backup)
	}

	// Here add the new backup owners.
	for _, newOwner := range newOwners {
		member := newOwner.(discovery.Member)
		exists := false

		for i, owner := range kept {
			if owner.CompareByID(member) {
				exists = true
				// Remove it from the current position
				kept = append(kept[:i], kept[i+1:]...)
				// Append it again to head
				kept = append(kept, member)
				break
			}
		}

		if !exists {
			kept = append(kept, member)
		}
	}

	return kept
}
