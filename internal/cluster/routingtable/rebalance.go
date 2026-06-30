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
	"github.com/hashicorp/memberlist"

	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
)

type rebalanceReason string

const (
	rebalanceReasonUnknown    rebalanceReason = "unknown"
	rebalanceReasonBootstrap  rebalanceReason = "bootstrap"
	rebalanceReasonPeriodic   rebalanceReason = "periodic"
	rebalanceReasonManual     rebalanceReason = "manual"
	rebalanceReasonNodeJoin   rebalanceReason = "node-join"
	rebalanceReasonNodeLeft   rebalanceReason = "node-left"
	rebalanceReasonNodeUpdate rebalanceReason = "node-update"
)

type rebalanceState struct {
	epoch     uint64
	pending   map[uint64]struct{}
	acked     map[uint64]struct{}
	completed bool
}

func rebalanceReasonFromEvent(event *discovery.ClusterEvent) (rebalanceReason, string) {
	member, _ := discovery.NewMemberFromMetadata(event.NodeMeta)
	switch event.Event {
	case memberlist.NodeJoin:
		return rebalanceReasonNodeJoin, member.String()
	case memberlist.NodeLeave:
		return rebalanceReasonNodeLeft, member.String()
	case memberlist.NodeUpdate:
		return rebalanceReasonNodeUpdate, member.String()
	default:
		return rebalanceReasonUnknown, ""
	}
}

func (r *RoutingTable) startRebalanceEpoch(epoch uint64, reason rebalanceReason, node string) {
	if epoch == 0 {
		return
	}

	pending := make(map[uint64]struct{})
	r.Members().RLock()
	r.Members().Range(func(id uint64, _ discovery.Member) bool {
		pending[id] = struct{}{}
		return true
	})
	r.Members().RUnlock()
	if len(pending) == 0 {
		return
	}

	r.rebalanceMtx.Lock()
	r.rebalanceState = rebalanceState{
		epoch:   epoch,
		pending: pending,
		acked:   make(map[uint64]struct{}),
	}
	r.rebalanceMtx.Unlock()

	if r.config.EnableClusterEventsChannel {
		r.wg.Add(1)
		go r.publishRebalanceStartEvent(epoch, string(reason), node)
	}
}

func (r *RoutingTable) handleRebalanceAck(epoch, memberID uint64) bool {
	r.rebalanceMtx.Lock()
	defer r.rebalanceMtx.Unlock()

	if r.rebalanceState.epoch == 0 || r.rebalanceState.epoch != epoch {
		return false
	}
	if r.rebalanceState.completed {
		return true
	}
	if _, ok := r.rebalanceState.pending[memberID]; !ok {
		return true
	}
	if _, ok := r.rebalanceState.acked[memberID]; ok {
		return true
	}

	r.rebalanceState.acked[memberID] = struct{}{}

	// Check completion based on live members only (not departed ones)
	// This implements: "completes only after all live members report"
	// and "node-left-event remains a membership signal, not a rebalance barrier"
	r.checkRebalanceCompletionLocked()

	return true
}

// checkRebalanceCompletionLocked checks if rebalance is complete based on current live members.
// This implements the requirement: "completes only after all live members report".
// Must be called with rebalanceMtx locked.
func (r *RoutingTable) checkRebalanceCompletionLocked() {
	if r.rebalanceState.epoch == 0 || r.rebalanceState.completed {
		return
	}

	// Count current live members that were in the original pending set
	// This ensures departed members don't block completion (node-left is not a barrier)
	livePendingCount := 0
	liveAckedCount := 0

	r.Members().RLock()
	r.Members().Range(func(id uint64, _ discovery.Member) bool {
		// Only count members that were originally in pending
		if _, wasPending := r.rebalanceState.pending[id]; wasPending {
			livePendingCount++
			if _, hasAcked := r.rebalanceState.acked[id]; hasAcked {
				liveAckedCount++
			}
		}
		return true
	})
	r.Members().RUnlock()

	// Complete when all live members (that were in pending) have ACKed
	// This implements: "completes only after all live members report"
	// Note: If all members leave, livePendingCount will be 0, and we won't complete
	// (which is correct - no one to complete for)
	if livePendingCount > 0 && liveAckedCount == livePendingCount {
		r.rebalanceState.completed = true
		completedEpoch := r.rebalanceState.epoch
		// Only publish event if context is still alive (node not shutting down)
		if r.config.EnableClusterEventsChannel {
			select {
			case <-r.ctx.Done():
				// Node is shutting down, don't publish event
			default:
				r.wg.Add(1)
				go r.publishRebalanceCompleteEvent(completedEpoch)
			}
		}
	}
}

func (r *RoutingTable) SendRebalanceAck(epoch uint64) error {
	if epoch == 0 {
		return nil
	}
	coordinator := r.discovery.GetCoordinator()
	if coordinator.ID == 0 {
		return ErrNotJoinedYet
	}
	cmd := protocol.NewRebalanceAck(epoch, r.this.ID).Command(r.ctx)
	rc := r.client.Get(coordinator.String())
	err := rc.Process(r.ctx, cmd)
	if err != nil {
		return err
	}
	return cmd.Err()
}
