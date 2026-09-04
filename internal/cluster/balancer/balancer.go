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

package balancer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/service"
	"github.com/tochemey/olric/pkg/flog"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/cluster/routingtable"
)

const (
	// balanceAttempts caps how many cycles one balancer invocation runs back
	// to back. A cycle that moved data is followed by another one right away,
	// so the rebalance ack lands as soon as the member has nothing left to
	// move instead of on the next TriggerBalancerInterval tick, and a cycle
	// whose ack the coordinator reported stale re-runs against the fresh
	// table. The cap keeps heavy churn from spinning.
	balanceAttempts = 3
)

type Balancer struct {
	sync.Mutex

	log       *flog.Logger
	config    *config.Config
	primary   *partitions.Partitions
	backup    *partitions.Partitions
	rt        *routingtable.RoutingTable
	syncState SyncState
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc

	// lastAckedGeneration records the routing table generation (see
	// RoutingTable.Version) this member last acked. Generations, unlike
	// signatures, never repeat, so a table that returns to an earlier state
	// is treated as the new install it is.
	lastAckedGeneration uint64
	// pushedSignature records, per partition, the signature of the routing
	// table under which this member last pushed the partition's primary copy
	// to its replica owners. The signature, not the generation, is the key:
	// the replica owners are part of the table content, so a table with the
	// same signature has the same replica owners and needs no new push, while
	// a generation that advanced for the same content, after a pull or a
	// coordinator change, must not re-copy every partition. A partition is
	// pushed once per table, as soon as it holds data: one that is empty
	// when a cycle runs, because its data is still being moved in, is left
	// unmarked and pushed by a later cycle, and one whose push failed is
	// retried likewise. Sized once from PartitionCount so the step allocates
	// nothing per cycle.
	pushedSignature []uint64
	// lastOwners records, per partition, the IDs of the primary owners,
	// current and previous, listed by the routing table at the last balance
	// cycle, nil before the first cycle. An owner that drops out of the list
	// schedules a restore, see trackPrimaryOwners. Replaced only when the
	// list changed, so a stable table allocates nothing per cycle.
	lastOwners [][]uint64
	// departedOwner records, per scheduled partition, the owner whose
	// leaving the owner list scheduled the restore. Whether it left the
	// cluster or only drained its copy is decided when the restore falls
	// due, once memberlist has had the delay to converge, see
	// restorePrimaryCopies.
	departedOwner []uint64
	// restoreDueAt records, per partition, when a scheduled restore of the
	// primary copy becomes due, in Unix nanoseconds, zero when none is
	// scheduled. Sized once from PartitionCount so the step allocates
	// nothing per cycle.
	restoreDueAt []int64
	// restoreAttempts counts the attempts of the pending restore of each
	// partition, so that only the first one is logged above debug level.
	restoreAttempts []uint32
}

// SyncState is the minimal interface for sync completion tracking.
type SyncState interface {
	PendingEmpty() bool
}

// New builds a Balancer from the environment, sizing its per-partition
// bookkeeping from the configured partition count.
func New(e *environment.Environment) *Balancer {
	c := e.Get("config").(*config.Config)
	log := e.Get("logger").(*flog.Logger)
	ctx, cancel := context.WithCancel(context.Background())
	b := &Balancer{
		config:          c,
		primary:         e.Get("primary").(*partitions.Partitions),
		backup:          e.Get("backup").(*partitions.Partitions),
		rt:              e.Get("routingtable").(*routingtable.RoutingTable),
		log:             log,
		ctx:             ctx,
		cancel:          cancel,
		pushedSignature: make([]uint64, c.PartitionCount),
		lastOwners:      make([][]uint64, c.PartitionCount),
		departedOwner:   make([]uint64, c.PartitionCount),
		restoreDueAt:    make([]int64, c.PartitionCount),
		restoreAttempts: make([]uint32, c.PartitionCount),
	}
	if v := e.Get("syncstate"); v != nil {
		b.syncState = v.(SyncState)
	}
	return b
}

func (b *Balancer) isAlive() bool {
	select {
	case <-b.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

// ownersString joins the address forms of owners with commas, for the
// balancer's transfer log lines.
func ownersString(owners []discovery.Member) string {
	names := make([]string, len(owners))
	for i, owner := range owners {
		names[i] = owner.String()
	}

	return strings.Join(names, ",")
}

// movePartition moves every non-empty fragment of part to owners, keeping each
// fragment's own kind. It reports moved when at least one fragment was moved,
// failed when at least one move returned an error, and aborted when the cycle
// has to stop because the routing table changed or the node is shutting down.
func (x *Balancer) movePartition(sign uint64, part *partitions.Partition, owners ...discovery.Member) (moved, failed, aborted bool) {
	return x.movePartitionWithTargetKind(sign, part, 0, owners...)
}

// rangeFragments applies apply to every non-empty fragment of part and reports
// moved when at least one apply succeeded, failed when at least one returned an
// error, and aborted when the routing table changed or the node began shutting
// down mid-cycle. It is the shared skeleton of the move and replicate paths,
// which differ only in the transfer apply runs and its log lines.
func (x *Balancer) rangeFragments(sign uint64, part *partitions.Partition, apply func(f partitions.Fragment, name string) error) (moved, failed, aborted bool) {
	part.Map().Range(func(rawName, rawFragment any) bool {
		f := rawFragment.(partitions.Fragment)
		if f.Stats().Length == 0 {
			// An empty fragment has nothing to move; the others may.
			return true
		}

		name := strings.TrimPrefix(rawName.(string), "dmap.")
		if err := apply(f, name); err != nil {
			failed = true
		} else {
			moved = true
		}

		// if this returns true, the iteration continues
		if x.breakLoop(sign) {
			aborted = true
			return false
		}

		return true
	})

	return moved, failed, aborted
}

// movePartitionWithTargetKind is movePartition with the receiver merging every
// fragment into targetKind, for example BACKUP when a primary fragment is
// pushed to its replica owners; a zero targetKind keeps the fragment's kind.
// moved and failed are reported separately so a cycle can tell progress from
// targets that are not ready: only a successful move counts as progress.
func (x *Balancer) movePartitionWithTargetKind(sign uint64, part *partitions.Partition, targetKind partitions.Kind, owners ...discovery.Member) (moved, failed, aborted bool) {
	ownersStr := ownersString(owners)

	return x.rangeFragments(sign, part, func(f partitions.Fragment, name string) error {
		x.log.V(2).Printf("[INFO] Moving %s fragment: %s (kind: %s) on PartID: %d to %s",
			f.Name(), name, part.Kind(), part.ID(), ownersStr)

		var err error
		if targetKind != 0 {
			err = f.MoveWithTargetKind(part, name, owners, targetKind)
		} else {
			err = f.Move(part, name, owners)
		}

		if err != nil {
			x.log.V(2).Printf("[ERROR] Failed to move %s fragment: %s on PartID: %d to %s: %v",
				f.Name(), name, part.ID(), ownersStr, err)
		}

		return err
	})
}

// replicatePartition copies every non-empty fragment of part to owners, to be
// merged into their partition of targetKind, and keeps the local copies. It
// reports moved, failed and aborted as movePartition does.
func (x *Balancer) replicatePartition(sign uint64, part *partitions.Partition, targetKind partitions.Kind, owners ...discovery.Member) (moved, failed, aborted bool) {
	ownersStr := ownersString(owners)

	return x.rangeFragments(sign, part, func(f partitions.Fragment, name string) error {
		x.log.V(2).Printf("[INFO] Replicating %s fragment: %s (kind: %s) on PartID: %d to %s as %s",
			f.Name(), name, part.Kind(), part.ID(), ownersStr, targetKind)

		if err := f.Replicate(part, name, owners, targetKind); err != nil {
			x.log.V(2).Printf("[ERROR] Failed to replicate %s fragment: %s on PartID: %d to %s: %v",
				f.Name(), name, part.ID(), ownersStr, err)
			return err
		}

		return nil
	})
}

// restoreEnabled reports whether this member restores primary copies after a
// departure: the proactive replication flag is set and there are replicas to
// restore from.
func (x *Balancer) restoreEnabled() bool {
	return x.config.EnableProactiveSyncOnJoin && x.config.ReplicaCount > config.MinimumReplicaCount
}

// isMemberID reports whether a member with the given ID is in the cluster.
func (x *Balancer) isMemberID(id uint64) bool {
	members := x.rt.Members()
	members.RLock()
	defer members.RUnlock()

	_, err := members.Get(id)
	return err == nil
}

// sameOwnerIDs reports whether owners lists exactly the members whose IDs
// are ids, in the same order.
func sameOwnerIDs(ids []uint64, owners []discovery.Member) bool {
	if len(ids) != len(owners) {
		return false
	}

	for i, owner := range owners {
		if owner.ID != ids[i] {
			return false
		}
	}

	return true
}

// containsOwnerID reports whether owners lists the member whose ID is id.
func containsOwnerID(owners []discovery.Member, id uint64) bool {
	for _, owner := range owners {
		if owner.ID == id {
			return true
		}
	}

	return false
}

// trackPrimaryOwners compares each partition's primary owner list, current
// and previous owners, with the one the previous cycle saw and schedules a
// restore for every partition an owner dropped out of: a member the
// coordinator pruned as dead, whose primary copy died with it, or one that
// drained its copy to the new owner and was dropped as a previous owner.
// Which of the two it was is not decided here: this member's memberlist may
// learn of a death after the coordinator's table arrives, so the restore is
// scheduled ReplicaRestoreDelay ahead, per partition, and decided when due,
// see restorePrimaryCopies. A later table change does not move the deadline
// of a pending departure, but a change that follows a pending drain by a
// member still in the cluster replaces it, so a drain scheduled shortly before
// a death never hides the death. The first cycle of a process only records
// the owners.
func (x *Balancer) trackPrimaryOwners(now time.Time) {
	for partID := range x.config.PartitionCount {
		owners := x.primary.PartitionByID(partID).Owners()
		last := x.lastOwners[partID]
		if sameOwnerIDs(last, owners) {
			continue
		}

		if len(last) > 0 {
			for _, id := range last {
				if containsOwnerID(owners, id) {
					continue
				}

				if x.restoreDueAt[partID] != 0 && !x.isMemberID(x.departedOwner[partID]) {
					// A departure is already pending for this partition; the
					// restore it leads to covers this change as well.
					break
				}

				// Either nothing is pending, or what is pending was a drain by
				// a member still in the cluster: this change is the one to
				// decide on, and its own delay applies.
				x.restoreDueAt[partID] = now.Add(x.config.ReplicaRestoreDelay).UnixNano()
				x.departedOwner[partID] = id
				x.restoreAttempts[partID] = 0
				break
			}
		}

		ids := make([]uint64, len(owners))
		for i, owner := range owners {
			ids[i] = owner.ID
		}

		x.lastOwners[partID] = ids
	}
}

// restorePrimaryCopies re-creates, from the backup copies this member holds,
// the primary copies whose restore trackPrimaryOwners scheduled once the delay
// has passed. A due partition is dropped from the schedule when the owner
// that left its owner list is still a cluster member, since it drained its
// copy to the new owner rather than dying with it, when this member owns the
// partition now, since promoteBackupCopies merges its own copy, or when it
// holds no backup copy for it any more. Otherwise every live table of the backup
// fragments is copied to the current primary owner as PRIMARY and the local
// copy is kept; the receiver's merge is version-aware, so a copy that lands
// after a previous owner already moved the data in changes nothing. A failed
// copy stays scheduled and is retried by the next cycle, logged in full the
// first time and at debug level afterwards.
//
// The step is off the convergence path: it runs only in a cycle that had
// nothing to move, and its outcome never holds the rebalance ack, so it
// reports only whether the cycle has to be abandoned.
func (x *Balancer) restorePrimaryCopies(sign uint64, now time.Time) (aborted bool) {
	for partID := range x.config.PartitionCount {
		due := x.restoreDueAt[partID]
		if due == 0 || now.UnixNano() < due {
			continue
		}

		if x.breakLoop(sign) {
			return true
		}

		primaryPart := x.primary.PartitionByID(partID)
		if primaryPart.OwnerCount() == 0 {
			continue
		}

		owner := primaryPart.Owner()
		backupPart := x.backup.PartitionByID(partID)
		if x.isMemberID(x.departedOwner[partID]) || backupPart.Length() == 0 || owner.CompareByName(x.rt.This()) {
			x.restoreDueAt[partID] = 0
			continue
		}

		if x.restoreAttempts[partID] == 0 {
			x.log.V(2).Printf("[INFO] Restoring the primary copy of PartID: %d on %s from the backup copy on %s", partID, owner, x.rt.This())
		} else {
			x.log.V(6).Printf("[DEBUG] Retrying the restore of the primary copy of PartID: %d on %s, attempt %d", partID, owner, x.restoreAttempts[partID]+1)
		}

		x.restoreAttempts[partID]++

		moved, failed, partitionAborted := x.replicatePartition(sign, backupPart, partitions.PRIMARY, owner)
		if partitionAborted {
			return true
		}

		if moved && !failed {
			x.restoreDueAt[partID] = 0
		}
	}

	return false
}

// primaryCopies moves the primary fragments this member still holds for
// partitions another member owns to that owner. It reports moved, failed and
// aborted as movePartition does, accumulated over every partition.
func (x *Balancer) primaryCopies(sign uint64) (moved, failed, aborted bool) {
	for partID := uint64(0); partID < x.config.PartitionCount; partID++ {
		if x.breakLoop(sign) {
			return moved, failed, true
		}

		part := x.primary.PartitionByID(partID)
		if part.Length() == 0 {
			// Empty partition. Skip it.
			continue
		}

		owner := part.Owner()
		// Here we don't use CompareByID function because the routing table is an
		// eventually consistent data structure and a node can try to move data
		// to previous instance(the same name but a different birthdate)
		// of itself. So just check the name.
		if owner.CompareByName(x.rt.This()) {
			// Already belongs to me.
			continue
		}

		// This is a previous owner. Move the keys.
		partitionMoved, partitionFailed, partitionAborted := x.movePartition(sign, part, owner)
		moved = moved || partitionMoved
		failed = failed || partitionFailed
		if partitionAborted {
			return moved, failed, true
		}
	}
	return moved, failed, false
}

func (b *Balancer) breakLoop(sign uint64) bool {
	if !b.isAlive() {
		return true
	}

	if sign != b.rt.Signature() {
		// Routing table is updated. Just quit. Another balancer goroutine
		// will work on the new table immediately.
		return true
	}

	return false
}

// backupCopies moves the backup fragments this member holds for partitions it
// is no longer a replica owner of to the current replica owners. It reports
// moved, failed and aborted as movePartition does, accumulated over every
// partition.
func (x *Balancer) backupCopies(sign uint64) (moved, failed, aborted bool) {
LOOP:
	for partID := uint64(0); partID < x.config.PartitionCount; partID++ {
		if x.breakLoop(sign) {
			return moved, failed, true
		}

		part := x.backup.PartitionByID(partID)
		if part.Length() == 0 || part.OwnerCount() == 0 {
			continue
		}

		primaryPart := x.primary.PartitionByID(partID)
		if primaryPart.OwnerCount() > 0 && primaryPart.Owner().CompareByName(x.rt.This()) {
			// This member owns the partition: promoteBackupCopies merges the
			// copy into the primary fragment. Relocating it instead, after a
			// promotion that failed, would move the only copy off the member
			// that never failed.
			continue
		}

		var (
			counter       = 1
			currentOwners []discovery.Member
		)

		owners := part.Owners()
		for i := len(owners) - 1; i >= 0; i-- {
			if counter > x.config.ReplicaCount-1 {
				break
			}

			counter++
			owner := owners[i]
			// Here we don't use CompareById function because the routing table
			// is an eventually consistent data structure and a node can try to
			// move data to previous instance(the same name but a different birthdate)
			// of itself. So just check the name.
			if x.rt.This().CompareByName(owner) {
				// Already belongs to me.
				continue LOOP
			}
			currentOwners = append(currentOwners, owner)
		}

		if len(currentOwners) == 0 {
			continue LOOP
		}

		partitionMoved, partitionFailed, partitionAborted := x.movePartition(sign, part, currentOwners...)
		moved = moved || partitionMoved
		failed = failed || partitionFailed
		if partitionAborted {
			return moved, failed, true
		}
	}
	return moved, failed, false
}

// promoteBackupCopies merges this node's backup fragments into its primary
// fragments for partitions it has become the primary owner of. After a node
// failure, the survivor's backup fragment may hold the only copy of a
// partition; without local promotion that sole copy stays in the backup
// fragment, and backupCopies would relocate it (transfer + local drop) to the
// newly assigned replica owner. If that node dies too, the data is destroyed
// even though this node never failed. Promotion must therefore run before
// backupCopies in every balancing cycle.
//
// The merge reuses the fragment-move machinery: the backup fragment is moved
// to this node itself with the PRIMARY target kind, which routes it through
// mergeFragments and its version-aware conflict resolution, then drops the
// backup fragment. It reports moved, failed and aborted as movePartition does.
func (x *Balancer) promoteBackupCopies(sign uint64) (moved, failed, aborted bool) {
	if x.config.ReplicaCount <= config.MinimumReplicaCount {
		return false, false, false
	}

	for partID := uint64(0); partID < x.config.PartitionCount; partID++ {
		if x.breakLoop(sign) {
			return moved, failed, true
		}

		primaryPart := x.primary.PartitionByID(partID)
		if primaryPart.OwnerCount() == 0 || !primaryPart.Owner().CompareByName(x.rt.This()) {
			continue
		}

		backupPart := x.backup.PartitionByID(partID)
		length := backupPart.Length()
		if length == 0 {
			continue
		}

		x.log.V(2).Printf("[INFO] Promoting backup copy of PartID: %d to primary on %s", partID, x.rt.This())

		// A fragment is transferred one storage table at a time, so drain the
		// backup fragment completely before backupCopies may relocate what's
		// left. Bail out if a cycle makes no progress (e.g. persistent move
		// errors) to avoid spinning.
		for length > 0 {
			partitionMoved, partitionFailed, partitionAborted := x.movePartitionWithTargetKind(sign, backupPart, partitions.PRIMARY, x.rt.This())
			moved = moved || partitionMoved
			failed = failed || partitionFailed
			if partitionAborted {
				return moved, failed, true
			}

			current := backupPart.Length()
			if current >= length {
				break
			}
			length = current
		}
	}
	return moved, failed, false
}

// pushPrimaryToBackups copies the primary copies this member owns to their
// replica owners, every live table of each fragment, keeping the primary copy.
// It is the proactive replication behind EnableProactiveSyncOnJoin: a replica
// owner that just joined, or that was designated after a departure, receives
// the data it lacks without waiting for the next write. Each partition is
// pushed once per table, see pushedSignature: a partition that is
// still empty, because its primary copy is being moved in from a previous
// owner, and a partition whose push failed, because a replica owner has not
// installed the table yet, are left unmarked and pushed again by a later
// cycle. It reports moved, failed and aborted as movePartition does.
func (x *Balancer) pushPrimaryToBackups(sign uint64) (moved, failed, aborted bool) {
	if !x.config.EnableProactiveSyncOnJoin || x.config.ReplicaCount <= config.MinimumReplicaCount {
		return false, false, false
	}

	for partID := range x.config.PartitionCount {
		if x.pushedSignature[partID] == sign {
			continue
		}

		if x.breakLoop(sign) {
			return moved, failed, true
		}

		primaryPart := x.primary.PartitionByID(partID)
		if primaryPart.OwnerCount() == 0 || !primaryPart.Owner().CompareByName(x.rt.This()) {
			continue
		}

		if primaryPart.Length() == 0 {
			// Nothing to push yet; the data may still be on its way.
			continue
		}

		// The owner list keeps previous replica owners at its head while they
		// still hold data; the current replica owners are its last
		// ReplicaCount-1 entries. A previous owner is draining, not a target.
		owners := x.backup.PartitionByID(partID).Owners()
		first := len(owners) - (x.config.ReplicaCount - 1)
		if first < 0 {
			first = 0
		}

		var targets []discovery.Member
		for _, m := range owners[first:] {
			if !x.rt.This().CompareByName(m) {
				targets = append(targets, m)
			}
		}

		if len(targets) == 0 {
			x.pushedSignature[partID] = sign
			continue
		}

		partitionMoved, partitionFailed, partitionAborted := x.replicatePartition(sign, primaryPart, partitions.BACKUP, targets...)
		moved = moved || partitionMoved
		failed = failed || partitionFailed
		if partitionAborted {
			return moved, failed, true
		}

		if partitionMoved && !partitionFailed {
			x.pushedSignature[partID] = sign
		}
	}
	return moved, failed, false
}

// triggerBalancer runs one balance pass now, outside the periodic tick.
func (x *Balancer) triggerBalancer() {
	x.runBalance()
}

// runBalance executes balance cycles under the balancer lock, up to
// balanceAttempts of them back to back: a cycle that moved data is followed
// by another one right away, so the member acks as soon as it has nothing
// left to move instead of on the next tick, and a cycle whose ack turned out
// to be stale re-runs against the fresh routing table.
func (x *Balancer) runBalance() {
	x.Lock()
	defer x.Unlock()

	if err := x.rt.CheckBootstrap(); err != nil {
		x.log.V(2).Printf("[WARN] Balancer awaits for bootstrapping")
		return
	}

	for attempt := 0; attempt < balanceAttempts; attempt++ {
		if !x.balanceOnce() {
			return
		}
	}
}

// balanceOnce runs a single balance cycle and reports whether the caller
// should run another one right away. That is the case after a cycle that
// moved data, so the ack follows the last move without waiting for the next
// tick, and after an ack the coordinator reported stale while the routing
// table changed since the cycle started. A cycle whose moves only failed
// reports false: its targets are not ready, and the next tick retries them.
func (x *Balancer) balanceOnce() bool {
	sign, generation := x.rt.Version()
	now := time.Now()
	restore := x.restoreEnabled()
	if restore {
		x.trackPrimaryOwners(now)
	}

	moved, failed, aborted := x.promoteBackupCopies(sign)
	if aborted {
		return false
	}

	primaryMoved, primaryFailed, aborted := x.primaryCopies(sign)
	moved = moved || primaryMoved
	failed = failed || primaryFailed
	if aborted {
		return false
	}

	if x.config.ReplicaCount > config.MinimumReplicaCount {
		pushed, pushFailed, aborted := x.pushPrimaryToBackups(sign)
		moved = moved || pushed
		failed = failed || pushFailed
		if aborted {
			return false
		}

		backupMoved, backupFailed, aborted := x.backupCopies(sign)
		moved = moved || backupMoved
		failed = failed || backupFailed
		if aborted {
			return false
		}
	}

	if moved {
		return true
	}

	if restore && x.restorePrimaryCopies(sign, now) {
		return false
	}

	if failed {
		return false
	}

	return x.tryAckRebalance(sign, generation) && x.rt.Signature() != sign
}

// tryAckRebalance sends the rebalance ack for sign, the signature of the
// routing table installed as generation; both come from one Version snapshot.
// It returns true when the coordinator reported the ack as stale, meaning the
// epoch was superseded.
//
// The ack is sent once per installed generation rather than once per
// signature value: signatures derive from the table content and repeat when
// the table returns to an earlier state (a member that joined and left before
// any data moved), and the coordinator then runs a new epoch under the same
// id. Keying on the signature alone would skip that ack and leave the epoch
// open until the next table change.
func (x *Balancer) tryAckRebalance(sign, generation uint64) bool {
	if sign == 0 || generation == x.lastAckedGeneration {
		return false
	}

	if x.syncState != nil && !x.syncState.PendingEmpty() {
		x.log.V(6).Printf("[DEBUG] Rebalance ack for signature %d (generation %d) deferred: partitions are still awaiting data", sign, generation)
		return false
	}

	// Don't try to send ACK if node is shutting down
	select {
	case <-x.ctx.Done():
		return false
	default:
	}

	if err := x.rt.SendRebalanceAck(sign); err != nil {
		if errors.Is(err, routingtable.ErrStaleRebalanceAck) {
			// The coordinator has moved to a newer epoch. Re-balance against
			// the current table instead of retrying this signature.
			x.log.V(6).Printf("[DEBUG] Rebalance ack for signature %d (generation %d) reported stale by the coordinator", sign, generation)
			return true
		}
		// Don't log if context was canceled (expected during graceful shutdown)
		if errors.Is(err, context.Canceled) {
			return false
		}
		// Convert protocol error and check if it's ErrNotCoordinator
		// This is expected during coordinator transitions and will retry automatically
		convertedErr := protocol.ConvertError(err)
		if errors.Is(convertedErr, protocol.ErrNotCoordinator) || errors.Is(err, protocol.ErrNotCoordinator) {
			// Silently ignore - this is expected during coordinator transitions
			return false
		}
		// Log other errors as they indicate actual problems
		x.log.V(3).Printf("[WARN] Failed to send rebalance ack for signature %d: %v", sign, err)
		return false
	}
	x.lastAckedGeneration = generation
	return false
}

// BalanceEagerly runs a full balance cycle right away. It is registered as the
// routing table callback when EnableProactiveSyncOnJoin is set, and then runs
// on every member after each installed routing table and, on the coordinator,
// after a node-join push, so the proactive primary-to-backup push starts as
// soon as a table is installed instead of on the next tick. The push itself
// runs on every cycle, once per partition per installed table as soon as the
// partition holds data, see pushedSignature, and holds the fragment
// write-lock for the duration of the network transfer, which blocks concurrent
// reads of that fragment; that cost is why the flag is opt-in.
func (x *Balancer) BalanceEagerly() {
	x.runBalance()
}

func (b *Balancer) balance() {
	defer b.wg.Done()

	timer := time.NewTimer(b.config.TriggerBalancerInterval)
	defer timer.Stop()

	for {
		timer.Reset(b.config.TriggerBalancerInterval)
		select {
		case <-timer.C:
			b.triggerBalancer()
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Balancer) Start() error {
	b.wg.Add(1)
	go b.balance()
	return nil
}

func (b *Balancer) RegisterHandlers() {}

func (b *Balancer) Shutdown(ctx context.Context) error {
	select {
	case <-b.ctx.Done():
		// already closed
		return nil
	default:
	}

	b.cancel()
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}

	return nil
}

var _ service.Service = (*Balancer)(nil)
