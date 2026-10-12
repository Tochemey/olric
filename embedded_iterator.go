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

package olric

import (
	"sync"

	"github.com/tochemey/olric/internal/dmap"
	"github.com/tochemey/olric/internal/protocol"
)

// EmbeddedIterator implements distributed query on DMaps.
type EmbeddedIterator struct {
	mtx sync.Mutex

	client          *EmbeddedClient
	dm              *dmap.DMap
	clusterIterator *ClusterIterator
}

// scanOnOwners runs one scan step of the current partition on every owner the
// routing table lists for it, scanning this member's own data in place. Owners
// memberlist has confirmed dead are skipped without a dial, and an owner whose
// death memberlist has not confirmed yet is dropped once a scan command cannot
// reach it, so one dead member does not end the whole scan; any other error is
// returned.
func (x *EmbeddedIterator) scanOnOwners() error {
	owners := x.clusterIterator.getOwners()

	// The embedded member has authoritative, seconds-fresh membership through
	// memberlist. Build the live set once per pass so a hard-dead owner is
	// skipped instead of dialed: a deleted pod IP drops packets, so each dial
	// against it burns the full dial timeout for nothing, and the dead owner's
	// data is served by the promoted replica anyway. GetMembers reports members
	// that are confirmed removed from memberlist (not merely suspected), so a
	// flapping-but-alive member is never skipped mid-scan.
	live := x.liveMembers()

	for idx, owner := range owners {
		cursor := x.clusterIterator.loadCursor(owner)

		if x.client.db.rt.This().String() == owner {
			keys, newCursor, err := x.dm.Scan(x.clusterIterator.partID, cursor, x.clusterIterator.config)
			if err != nil {
				return err
			}
			x.clusterIterator.updateIterator(keys, newCursor, owner)
			if newCursor == 0 {
				x.clusterIterator.removeScannedOwner(idx)
			}
			continue
		}

		// Skip owners memberlist has confirmed dead, and owners an earlier scan
		// command in this scan could not reach. Dropping the owner from the
		// route lets the scan converge without dialing the corpse again; without
		// this removal the route would never empty and the iterator would spin.
		if _, alive := live[owner]; !alive || x.clusterIterator.isUnreachable(owner) {
			x.clusterIterator.removeScannedOwner(idx)
			continue
		}

		// Build a scan command here
		s := protocol.NewScan(x.clusterIterator.partID, x.clusterIterator.dm.Name(), cursor)
		if x.clusterIterator.config.HasCount {
			s.SetCount(x.clusterIterator.config.Count)
		}
		if x.clusterIterator.config.HasMatch {
			s.SetMatch(x.clusterIterator.config.Match)
		}
		if x.clusterIterator.config.Replica {
			s.SetReplica()
		}

		scanCmd := s.Command(x.clusterIterator.ctx)
		// Fetch a Redis client for the given owner.
		rc := x.clusterIterator.clusterClient.client.Get(owner)
		err := rc.Process(x.clusterIterator.ctx, scanCmd)
		if err != nil {
			if !x.clusterIterator.markUnreachable(owner, err) {
				return err
			}
			x.clusterIterator.removeScannedOwner(idx)
			continue
		}

		keys, newCursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		x.clusterIterator.updateIterator(keys, newCursor, owner)
		if newCursor == 0 {
			x.clusterIterator.removeScannedOwner(idx)
		}
	}
	return nil
}

// liveMembers returns the set of member addresses memberlist currently reports
// as live. Membership is keyed on confirmed removal, not suspicion: a member
// that is merely suspected is still returned here, so its data is never skipped
// mid-scan on a false positive.
func (e *EmbeddedIterator) liveMembers() map[string]struct{} {
	members := e.client.db.rt.Discovery().GetMembers()
	live := make(map[string]struct{}, len(members))
	for _, m := range members {
		live[m.Name] = struct{}{}
	}
	return live
}

// Next returns true if there is more key in the iterator implementation.
// Otherwise, it returns false.
func (e *EmbeddedIterator) Next() bool {
	return e.clusterIterator.Next()
}

// Key returns a key name from the distributed map.
func (e *EmbeddedIterator) Key() string {
	return e.clusterIterator.Key()
}

// Close stops the iteration and releases allocated resources.
//
// The cluster client behind the iteration is shared and owned by the member, not
// by this iterator, so it is deliberately left open: Olric.Shutdown closes it.
func (e *EmbeddedIterator) Close() {
	e.clusterIterator.Close()
}
