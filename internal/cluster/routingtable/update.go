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
	"bytes"
	"runtime"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
)

type leftOverDataReport struct {
	Partitions []uint64
	Backups    []uint64
}

// countQuery asks an owner for its key count in a partition, in its backup
// copy when replica is set.
type countQuery struct {
	partID  uint64
	replica bool
}

// buildRoutingTablePayload encodes the routing table as a msgpack map whose
// entries are written in ascending partition id order, and returns the payload
// together with its signature.
//
// The signature is the xxhash of the payload and doubles as the rebalance
// epoch id, so it must be a pure function of the table content. Members derive
// their signature from the received bytes (see applyRoutingTablePayload) and
// the balancer acks that value, so the payload itself has to be canonical:
// msgpack.Marshal follows Go's randomized map iteration order and yields a
// different signature for the same table on almost every push, which
// supersedes in-flight epochs on a stable cluster. The wire format is still a
// plain msgpack map, so members running an older version keep decoding it.
func (r *RoutingTable) buildRoutingTablePayload() ([]byte, uint64, error) {
	partIDs := make([]uint64, 0, len(r.table))
	for partID := range r.table {
		partIDs = append(partIDs, partID)
	}
	slices.Sort(partIDs)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeMapLen(len(partIDs)); err != nil {
		return nil, 0, err
	}

	for _, partID := range partIDs {
		if err := enc.EncodeUint64(partID); err != nil {
			return nil, 0, err
		}
		if err := enc.Encode(r.table[partID]); err != nil {
			return nil, 0, err
		}
	}
	data := buf.Bytes()
	return data, xxhash.Sum64(data), nil
}

func (r *RoutingTable) prepareLeftOverDataReport() ([]byte, error) {
	res := leftOverDataReport{}
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		part := r.primary.PartitionByID(partID)
		if part.Length() != 0 {
			res.Partitions = append(res.Partitions, partID)
		}

		backup := r.backup.PartitionByID(partID)
		if backup.Length() != 0 {
			res.Backups = append(res.Backups, partID)
		}
	}
	return msgpack.Marshal(res)
}

// partitionsPendingReceive returns partition IDs where this node is an owner
// but has no data (needs to receive from others).
func (r *RoutingTable) partitionsPendingReceive() []uint64 {
	var out []uint64
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		primaryPart := r.primary.PartitionByID(partID)
		backupPart := r.backup.PartitionByID(partID)

		// Primary: we need receive if we're the owner and have no data
		if owners := primaryPart.Owners(); len(owners) > 0 {
			last := owners[len(owners)-1]
			if last.CompareByID(r.this) && primaryPart.Length() == 0 {
				out = append(out, partID)
				continue
			}
		}

		// Backup: we need receive if we're in owner list and have no data
		for _, m := range backupPart.Owners() {
			if m.CompareByID(r.this) && backupPart.Length() == 0 {
				out = append(out, partID)
				break
			}
		}
	}
	return out
}

// partitionsAwaitingData returns the partitions this member has to wait on
// before acking a rebalance: the owned-but-empty partitions reported by
// partitionsPendingReceive, minus those no live owner holds data for. A
// partition nobody holds data for never receives a fragment, so waiting on it
// only delays the ack by InitialSyncEmptyPartitionTimeout. Partitions whose
// escape deadline has already elapsed are kept as they are: the sync state no
// longer waits on them, so asking their owners would buy nothing. Every other
// owner is asked over the network, once, in a single pipelined round trip.
func (x *RoutingTable) partitionsAwaitingData() []uint64 {
	pending := x.partitionsPendingReceive()
	if len(pending) == 0 || x.syncState == nil {
		return pending
	}

	awaiting := make(map[uint64]struct{}, len(pending))
	queries := make(map[discovery.Member][]countQuery)

	for _, partID := range pending {
		if x.syncState.Expired(partID) {
			awaiting[partID] = struct{}{}
			continue
		}

		for _, owner := range x.primary.PartitionByID(partID).Owners() {
			if !owner.CompareByID(x.this) {
				queries[owner] = append(queries[owner], countQuery{partID: partID})
			}
		}

		for _, owner := range x.backup.PartitionByID(partID).Owners() {
			if !owner.CompareByID(x.this) {
				queries[owner] = append(queries[owner], countQuery{partID: partID, replica: true})
			}
		}
	}

	var mtx sync.Mutex
	var g errgroup.Group
	g.SetLimit(runtime.NumCPU())

	for owner, ownerQueries := range queries {
		g.Go(func() error {
			held := x.partitionsHeldBy(owner, ownerQueries)

			mtx.Lock()
			defer mtx.Unlock()

			for _, partID := range held {
				awaiting[partID] = struct{}{}
			}

			return nil
		})
	}

	// The probes report through awaiting; the group only bounds concurrency.
	_ = g.Wait()

	out := make([]uint64, 0, len(awaiting))
	for _, partID := range pending {
		if _, ok := awaiting[partID]; ok {
			out = append(out, partID)
		}
	}

	if skipped := len(pending) - len(out); skipped > 0 {
		x.log.V(6).Printf("[DEBUG] %d of %d empty partitions have no live source and are not awaited", skipped, len(pending))
	}

	return out
}

// partitionsHeldBy asks owner, in one pipelined round trip, for its key counts
// in the queried partitions and returns the ids of those it holds data for. A
// query that is not answered counts as held: memberlist removes a dead owner,
// and until then its data may still arrive, so the escape deadline keeps
// covering the partition.
func (x *RoutingTable) partitionsHeldBy(owner discovery.Member, queries []countQuery) []uint64 {
	rc := x.client.Get(owner.String())
	pipe := rc.Pipeline()
	cmds := make([]*redis.IntCmd, len(queries))

	for i, query := range queries {
		lengthOfPart := protocol.NewLengthOfPart(query.partID)
		if query.replica {
			lengthOfPart.SetReplica()
		}

		cmds[i] = lengthOfPart.Command(x.ctx)
		_ = pipe.Process(x.ctx, cmds[i])
	}

	if _, err := pipe.Exec(x.ctx); err != nil {
		x.log.V(6).Printf("[DEBUG] Failed to check key counts on %s: %v", owner, err)
	}

	// Left nil on purpose: on an empty cluster nothing is held, so nothing
	// needs to be allocated.
	var held []uint64

	for i, query := range queries {
		count, err := cmds[i].Result()
		if err != nil || count != 0 {
			held = append(held, query.partID)
		}
	}

	return held
}

func (r *RoutingTable) updateRoutingTableOnMember(data []byte, member discovery.Member) (*leftOverDataReport, error) {
	cmd := protocol.NewUpdateRouting(data, r.this.ID).Command(r.ctx)
	rc := r.client.Get(member.String())
	err := rc.Process(r.ctx, cmd)
	if err != nil {
		return nil, err
	}

	result, err := cmd.Bytes()
	if err != nil {
		return nil, err
	}

	report := leftOverDataReport{}
	err = msgpack.Unmarshal(result, &report)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to call decode ownership report from %s: %v", member, err)
		return nil, err
	}
	return &report, nil
}

// fetchRoutingTableFromCoordinator pulls the committed routing table from the
// cluster coordinator and applies it if this node is not bootstrapped yet.
// It is the delivery guarantee behind the push in updateRoutingTableOnCluster.
func (r *RoutingTable) fetchRoutingTableFromCoordinator() error {
	coordinator := r.discovery.GetCoordinator()
	if coordinator.ID == 0 {
		return ErrNotJoinedYet
	}
	if coordinator.CompareByID(r.this) {
		// The coordinator bootstraps itself.
		return nil
	}

	cmd := protocol.NewFetchRouting(r.this.ID).Command(r.ctx)
	rc := r.client.Get(coordinator.String())
	if err := rc.Process(r.ctx, cmd); err != nil {
		return err
	}
	payload, err := cmd.Bytes()
	if err != nil {
		return err
	}

	_, err = r.applyRoutingTablePayload(payload, coordinator.ID, true)
	return err
}

func (r *RoutingTable) updateRoutingTableOnCluster(data []byte) (map[discovery.Member]*leftOverDataReport, error) {
	var mtx sync.Mutex
	var g errgroup.Group
	var unreachable []discovery.Member
	reports := make(map[discovery.Member]*leftOverDataReport)
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)

	r.Members().RLock()
	r.Members().Range(func(id uint64, tmp discovery.Member) bool {
		member := tmp
		g.Go(func() error {
			if err := sem.Acquire(r.ctx, 1); err != nil {
				r.log.V(3).Printf("[ERROR] Failed to acquire semaphore to update routing table on %s: %v", member, err)
				return err
			}
			defer sem.Release(1)

			report, err := r.updateRoutingTableOnMember(data, member)
			// TODO: temporary diagnostic logging for the routing table push
			// investigation. Remove once the silent push failure is understood.
			if err != nil {
				r.log.V(1).Printf("[WARN] Failed to push routing table to %s: %v", member, err)
			} else {
				r.log.V(1).Printf("[DEBUG] Routing table pushed to %s", member)
			}
			if err != nil {
				// The coordinator must apply its own table, otherwise the
				// committed epoch would reference a table this node never
				// installed.
				if member.CompareByID(r.this) {
					return err
				}
				// An unreachable member must not block the commit for the
				// rest of the cluster. It receives the table on the next
				// push, or memberlist removes it.
				mtx.Lock()
				unreachable = append(unreachable, member)
				mtx.Unlock()
				return nil
			}

			mtx.Lock()
			defer mtx.Unlock()
			reports[member] = report
			return nil
		})
		return true
	})
	r.Members().RUnlock()

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if len(unreachable) > 0 {
		r.log.V(2).Printf("[WARN] Routing table could not be pushed to %d member(s): %v. "+
			"They will receive it on the next update", len(unreachable), unreachable)
	}
	return reports, nil
}
