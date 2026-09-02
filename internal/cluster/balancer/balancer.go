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

	// lastAckedGeneration and lastProactiveSyncGeneration record the routing
	// table generation (see RoutingTable.Version) this member last acked and
	// last ran the proactive primary-to-backup push for. Generations, unlike
	// signatures, never repeat, so a table that returns to an earlier state
	// is treated as the new install it is.
	lastAckedGeneration         uint64
	lastProactiveSyncGeneration uint64
}

// SyncState is the minimal interface for sync completion tracking.
type SyncState interface {
	PendingEmpty() bool
}

func New(e *environment.Environment) *Balancer {
	c := e.Get("config").(*config.Config)
	log := e.Get("logger").(*flog.Logger)
	ctx, cancel := context.WithCancel(context.Background())
	b := &Balancer{
		config:  c,
		primary: e.Get("primary").(*partitions.Partitions),
		backup:  e.Get("backup").(*partitions.Partitions),
		rt:      e.Get("routingtable").(*routingtable.RoutingTable),
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
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

func (b *Balancer) movePartition(sign uint64, part *partitions.Partition, owners ...discovery.Member) (bool, bool) {
	return b.movePartitionWithTargetKind(sign, part, 0, owners...)
}

func (b *Balancer) movePartitionWithTargetKind(sign uint64, part *partitions.Partition, targetKind partitions.Kind, owners ...discovery.Member) (bool, bool) {
	var (
		moved   bool
		aborted bool
	)
	ownersStr := func() string {
		var names []string
		for _, owner := range owners {
			names = append(names, owner.String())
		}
		return strings.Join(names, ",")
	}()

	part.Map().Range(func(rawName, rawFragment any) bool {
		f := rawFragment.(partitions.Fragment)
		if f.Stats().Length == 0 {
			return false
		}
		name := strings.TrimPrefix(rawName.(string), "dmap.")

		b.log.V(2).Printf("[INFO] Moving %s fragment: %s (kind: %s) on PartID: %d to %s",
			f.Name(), name, part.Kind(), part.ID(), ownersStr)

		moved = true
		var err error
		if targetKind != 0 {
			err = f.MoveWithTargetKind(part, name, owners, targetKind)
		} else {
			err = f.Move(part, name, owners)
		}
		if err != nil {
			b.log.V(2).Printf("[ERROR] Failed to move %s fragment: %s on PartID: %d to %s: %v",
				f.Name(), name, part.ID(), ownersStr, err)
		}

		// if this returns true, the iteration continues
		if b.breakLoop(sign) {
			aborted = true
			return false
		}
		return true
	})
	return moved, aborted
}

func (b *Balancer) primaryCopies(sign uint64) (bool, bool) {
	var moved bool
	for partID := uint64(0); partID < b.config.PartitionCount; partID++ {
		if b.breakLoop(sign) {
			return moved, true
		}

		part := b.primary.PartitionByID(partID)
		if part.Length() == 0 {
			// Empty partition. Skip it.
			continue
		}

		owner := part.Owner()
		// Here we don't use CompareByID function because the routing table is an
		// eventually consistent data structure and a node can try to move data
		// to previous instance(the same name but a different birthdate)
		// of itself. So just check the name.
		if owner.CompareByName(b.rt.This()) {
			// Already belongs to me.
			continue
		}

		// This is a previous owner. Move the keys.
		partitionMoved, aborted := b.movePartition(sign, part, owner)
		moved = moved || partitionMoved
		if aborted {
			return moved, true
		}
	}
	return moved, false
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

func (b *Balancer) backupCopies(sign uint64) (bool, bool) {
	var moved bool
LOOP:
	for partID := uint64(0); partID < b.config.PartitionCount; partID++ {
		if b.breakLoop(sign) {
			return moved, true
		}

		part := b.backup.PartitionByID(partID)
		if part.Length() == 0 || part.OwnerCount() == 0 {
			continue
		}

		var (
			counter       = 1
			currentOwners []discovery.Member
		)

		owners := part.Owners()
		for i := len(owners) - 1; i >= 0; i-- {
			if counter > b.config.ReplicaCount-1 {
				break
			}

			counter++
			owner := owners[i]
			// Here we don't use CompareById function because the routing table
			// is an eventually consistent data structure and a node can try to
			// move data to previous instance(the same name but a different birthdate)
			// of itself. So just check the name.
			if b.rt.This().CompareByName(owner) {
				// Already belongs to me.
				continue LOOP
			}
			currentOwners = append(currentOwners, owner)
		}

		if len(currentOwners) == 0 {
			continue LOOP
		}

		partitionMoved, aborted := b.movePartition(sign, part, currentOwners...)
		moved = moved || partitionMoved
		if aborted {
			return moved, true
		}
	}
	return moved, false
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
// backup fragment.
func (b *Balancer) promoteBackupCopies(sign uint64) (bool, bool) {
	if b.config.ReplicaCount <= config.MinimumReplicaCount {
		return false, false
	}

	var moved bool
	for partID := uint64(0); partID < b.config.PartitionCount; partID++ {
		if b.breakLoop(sign) {
			return moved, true
		}

		primaryPart := b.primary.PartitionByID(partID)
		if primaryPart.OwnerCount() == 0 || !primaryPart.Owner().CompareByName(b.rt.This()) {
			continue
		}

		backupPart := b.backup.PartitionByID(partID)
		length := backupPart.Length()
		if length == 0 {
			continue
		}

		b.log.V(2).Printf("[INFO] Promoting backup copy of PartID: %d to primary on %s", partID, b.rt.This())

		// A fragment is transferred one storage table at a time, so drain the
		// backup fragment completely before backupCopies may relocate what's
		// left. Bail out if a cycle makes no progress (e.g. persistent move
		// errors) to avoid spinning.
		for length > 0 {
			partitionMoved, aborted := b.movePartitionWithTargetKind(sign, backupPart, partitions.PRIMARY, b.rt.This())
			moved = moved || partitionMoved
			if aborted {
				return moved, true
			}
			current := backupPart.Length()
			if current >= length {
				break
			}
			length = current
		}
	}
	return moved, false
}

// pushPrimaryToBackups pushes primary partition data to backup owners that may
// not yet have it (e.g. newly joined nodes). Enables proactive sync on node join.
func (b *Balancer) pushPrimaryToBackups(sign uint64) (bool, bool) {
	if b.config.ReplicaCount <= config.MinimumReplicaCount {
		return false, false
	}

	var moved bool
	for partID := uint64(0); partID < b.config.PartitionCount; partID++ {
		if b.breakLoop(sign) {
			return moved, true
		}

		primaryPart := b.primary.PartitionByID(partID)
		if primaryPart.Length() == 0 {
			continue
		}

		owner := primaryPart.Owner()
		if !owner.CompareByName(b.rt.This()) {
			continue
		}

		backupPart := b.backup.PartitionByID(partID)
		backupOwners := backupPart.Owners()
		if len(backupOwners) == 0 {
			continue
		}

		var targets []discovery.Member
		for _, m := range backupOwners {
			if !b.rt.This().CompareByName(m) {
				targets = append(targets, m)
			}
		}
		if len(targets) == 0 {
			continue
		}

		partitionMoved, aborted := b.movePartitionWithTargetKind(sign, primaryPart, partitions.BACKUP, targets...)
		moved = moved || partitionMoved
		if aborted {
			return moved, true
		}
	}
	return moved, false
}

func (b *Balancer) triggerBalancer() {
	b.runBalance(false)
}

// runBalance executes balance cycles under the balancer lock. A cycle whose
// rebalance ack turns out to be stale re-runs immediately against the fresh
// routing table instead of waiting for the next tick, with a small cap to
// avoid spinning under heavy membership churn.
func (b *Balancer) runBalance(proactiveSync bool) {
	b.Lock()
	defer b.Unlock()

	if err := b.rt.CheckBootstrap(); err != nil {
		b.log.V(2).Printf("[WARN] Balancer awaits for bootstrapping")
		return
	}

	for attempts := 0; attempts < 3; attempts++ {
		if !b.balanceOnce(proactiveSync) {
			return
		}
	}
}

// balanceOnce runs a single balance cycle. It returns true when the cycle
// acked a stale epoch and the routing table has changed since the cycle
// started, i.e. the caller should re-run against the fresh table.
func (b *Balancer) balanceOnce(proactiveSync bool) bool {
	sign, generation := b.rt.Version()
	moved, aborted := b.promoteBackupCopies(sign)
	if aborted {
		return false
	}

	primaryMoved, aborted := b.primaryCopies(sign)
	moved = moved || primaryMoved
	if aborted {
		return false
	}

	if b.config.ReplicaCount > config.MinimumReplicaCount {
		if proactiveSync && generation != b.lastProactiveSyncGeneration {
			primaryPushed, aborted := b.pushPrimaryToBackups(sign)
			moved = moved || primaryPushed
			if aborted {
				return false
			}
			// One push per installed table is sufficient. MoveWithTargetKind
			// for BACKUP intentionally keeps data on the primary, so repeated
			// calls would flood backup nodes with duplicate data.
			b.lastProactiveSyncGeneration = generation
		}

		backupMoved, aborted := b.backupCopies(sign)
		moved = moved || backupMoved
		if aborted {
			return false
		}
	}

	if !moved {
		return b.tryAckRebalance(sign, generation) && b.rt.Signature() != sign
	}
	return false
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
func (b *Balancer) tryAckRebalance(sign, generation uint64) bool {
	if sign == 0 || generation == b.lastAckedGeneration {
		return false
	}
	if b.syncState != nil && !b.syncState.PendingEmpty() {
		return false
	}

	// Don't try to send ACK if node is shutting down
	select {
	case <-b.ctx.Done():
		return false
	default:
	}

	if err := b.rt.SendRebalanceAck(sign); err != nil {
		if errors.Is(err, routingtable.ErrStaleRebalanceAck) {
			// The coordinator has moved to a newer epoch. Re-balance against
			// the current table instead of retrying this signature.
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
		b.log.V(3).Printf("[WARN] Failed to send rebalance ack for signature %d: %v", sign, err)
		return false
	}
	b.lastAckedGeneration = generation
	return false
}

// BalanceEagerly is the callback registered for node-join events. It runs the
// full balance cycle AND the proactive primary-to-backup push for new nodes,
// all under a single lock so tryAckRebalance fires only after the push.
//
// It must NOT be called on node-leave events: the write-lock held during the
// network transfer in pushPrimaryToBackups would block concurrent reads for
// the entire push duration, causing read timeouts. The routing table ensures
// this by only triggering runCallbacks on rebalanceReasonNodeJoin.
func (b *Balancer) BalanceEagerly() {
	b.runBalance(true)
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
