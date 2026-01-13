/*
 * Copyright 2025 Arsene Tochemey Gandote
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

	log     *flog.Logger
	config  *config.Config
	primary *partitions.Partitions
	backup  *partitions.Partitions
	rt      *routingtable.RoutingTable
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	lastAckedSignature uint64
}

func New(e *environment.Environment) *Balancer {
	c := e.Get("config").(*config.Config)
	log := e.Get("logger").(*flog.Logger)
	ctx, cancel := context.WithCancel(context.Background())
	return &Balancer{
		config:  c,
		primary: e.Get("primary").(*partitions.Partitions),
		backup:  e.Get("backup").(*partitions.Partitions),
		rt:      e.Get("routingtable").(*routingtable.RoutingTable),
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
	}
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
		err := f.Move(part, name, owners)
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

func (b *Balancer) triggerBalancer() {
	b.Lock()
	defer b.Unlock()

	if err := b.rt.CheckBootstrap(); err != nil {
		b.log.V(2).Printf("[WARN] Balancer awaits for bootstrapping")
		return
	}

	sign := b.rt.Signature()
	moved, aborted := b.primaryCopies(sign)
	if aborted {
		return
	}

	if b.config.ReplicaCount > config.MinimumReplicaCount {
		backupMoved, aborted := b.backupCopies(sign)
		moved = moved || backupMoved
		if aborted {
			return
		}
	}

	if !moved {
		b.tryAckRebalance(sign)
	}
}

func (b *Balancer) tryAckRebalance(sign uint64) {
	if sign == 0 || sign == b.lastAckedSignature {
		return
	}

	// Don't try to send ACK if node is shutting down
	select {
	case <-b.ctx.Done():
		return
	default:
	}

	if err := b.rt.SendRebalanceAck(sign); err != nil {
		// Convert protocol error and check if it's ErrNotCoordinator
		// This is expected during coordinator transitions and will retry automatically
		convertedErr := protocol.ConvertError(err)
		if errors.Is(convertedErr, protocol.ErrNotCoordinator) || errors.Is(err, protocol.ErrNotCoordinator) {
			// Silently ignore - this is expected during coordinator transitions
			return
		}
		// Log other errors as they indicate actual problems
		b.log.V(3).Printf("[WARN] Failed to send rebalance ack for signature %d: %v", sign, err)
		return
	}
	b.lastAckedSignature = sign
}

func (b *Balancer) BalanceEagerly() {
	b.triggerBalancer()
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
