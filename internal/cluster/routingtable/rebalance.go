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
	"slices"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hashicorp/memberlist"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
)

const (
	// overdueEpochIntervals is how many balancer intervals an epoch may stay
	// open before the members whose ack it waits for are logged, once per
	// further interval, so a stuck convergence names its cause.
	overdueEpochIntervals = 3
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
	epoch uint64
	// generation is the install generation of the table pushed for this epoch
	// on the coordinator; unlike epoch it never recurs, see
	// RoutingTable.Generation.
	generation uint64
	// members lists, sorted, the addresses of the members the coordinator knew
	// when it computed the pushed table.
	members []string
	// startPublished is closed once the epoch's start event has been
	// published, or right away when it is not published. The completion
	// publisher waits on it, so subscribers never see a completion before
	// its start.
	startPublished chan struct{}
	pending        map[uint64]struct{}
	acked          map[uint64]struct{}
	completed      bool
}

// ackStatus is the coordinator's verdict on a received rebalance ack.
type ackStatus int

const (
	// ackAccepted: the ack was counted for the active epoch (or was an
	// idempotent no-op for it).
	ackAccepted ackStatus = iota
	// ackEarly: the ack references the table this coordinator has already
	// committed, but startRebalanceEpoch has not run yet. The ack is buffered
	// and harvested when the epoch starts.
	ackEarly
	// ackStale: the ack references a superseded epoch. The sender should
	// re-run its balance cycle against its current table.
	ackStale
)

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

// startRebalanceEpoch activates a new rebalance epoch. memberIDs is the set of
// live members the pushed table was computed for, every one of them: a member
// the fan-out could not reach installs the table through the retried push or
// its own pull and acks then, and one that leaves meanwhile stops gating the
// epoch, see checkRebalanceCompletionLocked. Gating on the members the first
// push reached instead let the completion be announced while a member whose
// push was still being retried routed requests on the old table, to a member
// that had died. members lists the addresses of the same members; it is
// reported with the completion of the epoch.
func (x *RoutingTable) startRebalanceEpoch(epoch uint64, reason rebalanceReason, node string, memberIDs []uint64, members []string) {
	if epoch == 0 {
		return
	}

	pending := make(map[uint64]struct{}, len(memberIDs))
	for _, id := range memberIDs {
		pending[id] = struct{}{}
	}

	if len(pending) == 0 {
		return
	}

	// The coordinator installed its own push before starting the epoch, so
	// its generation is the one of the pushed table.
	generation := x.Generation()

	x.rebalanceMtx.Lock()
	startPublished := make(chan struct{})
	x.rebalanceState = rebalanceState{
		epoch:          epoch,
		generation:     generation,
		members:        members,
		startPublished: startPublished,
		pending:        pending,
		acked:          make(map[uint64]struct{}),
	}
	// Harvest acks that raced the epoch start: members that processed the
	// pushed table acked while the coordinator was still fanning it out.
	// Members outside the pending set are recorded too, as handleRebalanceAck
	// does: one that pulled the table during the fan-out joins the pending
	// set when the retried push lands, and its ack must count then.
	for id := range x.earlyAcks[epoch] {
		x.rebalanceState.acked[id] = struct{}{}
	}
	x.earlyAcks = make(map[uint64]map[uint64]struct{})

	// The start is stamped, and its publish started, before the completion
	// check: when every ack was harvested early the completion is published
	// from inside that check, and it must neither carry an earlier timestamp
	// than its start nor reach subscribers before it.
	startedAt := time.Now().UnixNano()
	published := x.config.EnableClusterEventsChannel && x.spawn(func() {
		defer close(startPublished)

		x.publishRebalanceStartEvent(epoch, generation, string(reason), node, startedAt)
	})
	if !published {
		close(startPublished)
	}

	x.checkRebalanceCompletionLocked()
	completed := x.rebalanceState.completed
	x.rebalanceMtx.Unlock()

	if !completed {
		x.spawn(func() { x.watchOverdueEpoch(epoch, generation) })
	}
}

// watchOverdueEpoch logs, once per balancer interval after overdueEpochIntervals
// of them, the members whose ack the epoch started for generation is still
// waiting on. It ends as soon as that epoch completes or is superseded, so no
// goroutine outlives an open epoch and a quiescent cluster runs none.
func (x *RoutingTable) watchOverdueEpoch(epoch, generation uint64) {
	interval := x.config.TriggerBalancerInterval
	if interval <= 0 {
		interval = config.DefaultTriggerBalancerInterval
	}

	started := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for tick := 1; ; tick++ {
		select {
		case <-x.ctx.Done():
			return
		case <-ticker.C:
		}

		missing, open := x.overdueMembers(epoch, generation)
		if !open {
			return
		}

		if tick >= overdueEpochIntervals {
			x.log.V(2).Printf("[WARN] Rebalance epoch %d (generation %d) is still open after %s, waiting for the ack of %v",
				epoch, generation, time.Since(started).Round(time.Millisecond), missing)
		}
	}
}

// overdueMembers returns, sorted, the names of the live members in the pending
// set of the epoch started for generation that have not acked it, and whether
// that epoch is still the active, uncompleted one.
func (x *RoutingTable) overdueMembers(epoch, generation uint64) ([]string, bool) {
	x.rebalanceMtx.Lock()
	defer x.rebalanceMtx.Unlock()

	state := x.rebalanceState
	if state.epoch != epoch || state.generation != generation || state.completed {
		return nil, false
	}

	var missing []string

	x.Members().RLock()
	x.Members().Range(func(id uint64, member discovery.Member) bool {
		if _, pending := state.pending[id]; !pending {
			return true
		}

		if _, acked := state.acked[id]; acked {
			return true
		}

		missing = append(missing, member.String())
		return true
	})
	x.Members().RUnlock()

	slices.Sort(missing)
	return missing, true
}

// memberNames returns the addresses of the current members in sorted order.
// Taken before a routing table is computed, it names the members that table
// was computed for.
func (x *RoutingTable) memberNames() []string {
	_, names := x.memberSnapshot()
	return names
}

// memberSnapshot returns the IDs and the sorted addresses of the current
// members, taken under one read lock so both describe the same membership.
func (x *RoutingTable) memberSnapshot() ([]uint64, []string) {
	x.Members().RLock()
	defer x.Members().RUnlock()

	ids := make([]uint64, 0, x.Members().Length())
	names := make([]string, 0, x.Members().Length())
	x.Members().Range(func(id uint64, member discovery.Member) bool {
		ids = append(ids, id)
		names = append(names, member.String())
		return true
	})

	slices.Sort(names)
	return ids, names
}

// committedSignature returns the signature of the payload this coordinator
// committed last, zero when it committed none. It can run ahead of the
// installed signature while the coordinator's own push is in flight.
func (x *RoutingTable) committedSignature() uint64 {
	payload, ok := x.committedPayload.Load().([]byte)
	if !ok || len(payload) == 0 {
		return 0
	}

	return xxhash.Sum64(payload)
}

// handleRebalanceAck records the ack of memberID for epoch and reports whether
// it was accepted, buffered as early, or stale.
func (x *RoutingTable) handleRebalanceAck(epoch, memberID uint64) ackStatus {
	x.rebalanceMtx.Lock()
	defer x.rebalanceMtx.Unlock()

	if x.rebalanceState.epoch == 0 || x.rebalanceState.epoch != epoch {
		// The push fan-out and startRebalanceEpoch are not atomic: a member
		// that applies the pushed table quickly, or pulls the committed one,
		// acks before the epoch becomes active. Buffer acks matching the
		// installed or the committed table signature; they are harvested
		// when the epoch starts.
		if epoch == x.Signature() || epoch == x.committedSignature() {
			if x.earlyAcks[epoch] == nil {
				x.earlyAcks[epoch] = make(map[uint64]struct{})
			}
			x.earlyAcks[epoch][memberID] = struct{}{}
			return ackEarly
		}
		return ackStale
	}

	if x.rebalanceState.completed {
		return ackAccepted
	}

	if _, ok := x.rebalanceState.acked[memberID]; ok {
		return ackAccepted
	}

	// Recorded whether or not the member is in the pending set: a member that
	// receives the table on a push retry joins the set afterwards, see
	// admitLatePush, and its ack must count then.
	x.rebalanceState.acked[memberID] = struct{}{}

	if _, ok := x.rebalanceState.pending[memberID]; !ok {
		return ackAccepted
	}

	// Check completion based on live members only (not departed ones)
	// This implements: "completes only after all live members report"
	// and "node-left-event remains a membership signal, not a rebalance barrier"
	x.checkRebalanceCompletionLocked()

	return ackAccepted
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
		generation := r.rebalanceState.generation
		members := r.rebalanceState.members
		startPublished := r.rebalanceState.startPublished
		// Only publish event if context is still alive (node not shutting down)
		if r.config.EnableClusterEventsChannel {
			select {
			case <-r.ctx.Done():
				// Node is shutting down, don't publish event
			default:
				r.spawn(func() {
					// Nil only for states built without startRebalanceEpoch.
					if startPublished != nil {
						<-startPublished
					}

					r.publishRebalanceCompleteEvent(completedEpoch, generation, members)
				})
			}
		}
	}
}

// ErrStaleRebalanceAck is returned by SendRebalanceAck when the coordinator
// reports that the acked epoch has been superseded. The caller should re-run
// its balance cycle against the current routing table instead of retrying.
var ErrStaleRebalanceAck = errors.New("rebalance ack references a superseded epoch")

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
	status, err := cmd.Result()
	if err != nil {
		return err
	}
	if status == protocol.StatusStaleRebalanceAck {
		return ErrStaleRebalanceAck
	}
	return nil
}
