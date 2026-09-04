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
	"context"
	"time"

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/discovery"
)

// ClusterEventPublisher delivers an encoded cluster event to the subscribers
// of channel on every cluster member. The routing table has no publishing
// capability of its own: the pubsub service registers one at construction
// time via SetClusterEventPublisher.
type ClusterEventPublisher func(ctx context.Context, channel, message string) error

// SetClusterEventPublisher registers publish as the transport for cluster
// events. Until one is registered, PublishClusterEvent drops events.
func (r *RoutingTable) SetClusterEventPublisher(publish ClusterEventPublisher) {
	r.eventPublisherMtx.Lock()
	defer r.eventPublisherMtx.Unlock()
	r.eventPublisher = publish
}

// PublishClusterEvent delivers an encoded cluster event to the subscribers of
// channel on every cluster member through the registered publisher. Without a
// registered publisher — a deployment without the pubsub service, which only
// happens in partial test setups — the event is dropped.
func (r *RoutingTable) PublishClusterEvent(ctx context.Context, channel, message string) error {
	r.eventPublisherMtx.RLock()
	publish := r.eventPublisher
	r.eventPublisherMtx.RUnlock()

	if publish == nil {
		r.log.V(4).Printf("[DEBUG] No cluster event publisher is registered, dropping event to %s", channel)
		return nil
	}
	return publish(ctx, channel, message)
}

// SetLocalClusterEventPublisher registers publish as the transport for the
// events a member delivers to its own subscribers only, node-join-event and
// node-left-event. Until one is registered those events go through the
// cluster-wide publisher, so a deployment or test that registers only that one
// still receives them.
func (x *RoutingTable) SetLocalClusterEventPublisher(publish ClusterEventPublisher) {
	x.eventPublisherMtx.Lock()
	defer x.eventPublisherMtx.Unlock()
	x.localEventPublisher = publish
}

// PublishLocalClusterEvent delivers an encoded cluster event to this member's
// own subscribers of channel, through the local publisher when one is
// registered and the cluster-wide one otherwise. Without either, the event is
// dropped.
func (x *RoutingTable) PublishLocalClusterEvent(ctx context.Context, channel, message string) error {
	x.eventPublisherMtx.RLock()
	publish := x.localEventPublisher
	if publish == nil {
		publish = x.eventPublisher
	}

	x.eventPublisherMtx.RUnlock()

	if publish == nil {
		x.log.V(4).Printf("[DEBUG] No cluster event publisher is registered, dropping local event to %s", channel)
		return nil
	}

	return publish(ctx, channel, message)
}

// publishMembershipChangeEvent announces, from the coordinator, that m joined,
// left or was updated (change), together with the sorted addresses of the
// members after the change and generation, the install generation this
// coordinator held when it observed the change.
func (x *RoutingTable) publishMembershipChangeEvent(change string, m *discovery.Member, members []string, generation uint64) {
	message := events.MembershipChangeEvent{
		Kind:       events.KindMembershipChangeEvent,
		Source:     x.this.String(),
		Change:     change,
		Node:       m.String(),
		NodeMeta:   m.Meta,
		Members:    members,
		Generation: generation,
		Timestamp:  time.Now().UnixNano(),
	}

	data, err := message.Encode()
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to encode MembershipChangeEvent: %v", err)
		return
	}

	err = x.PublishClusterEvent(x.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to publish MembershipChangeEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

// publishNodeJoinEvent announces the join of m to this member's own
// subscribers, stamped with generation, the install generation this member
// held when it observed the join.
func (x *RoutingTable) publishNodeJoinEvent(m *discovery.Member, generation uint64) {
	message := events.NodeJoinEvent{
		Kind:       events.KindNodeJoinEvent,
		Source:     x.this.String(),
		NodeJoin:   m.String(),
		NodeMeta:   m.Meta,
		Generation: generation,
		Timestamp:  time.Now().UnixNano(),
	}

	data, err := message.Encode()
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to encode NodeJoinEvent: %v", err)
		return
	}

	err = x.PublishLocalClusterEvent(x.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to publish NodeJoinEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

// publishNodeLeftEvent announces the departure of m to this member's own
// subscribers, stamped with generation, the install generation this member
// held when it observed the departure.
func (x *RoutingTable) publishNodeLeftEvent(m *discovery.Member, generation uint64) {
	message := events.NodeLeftEvent{
		Kind:       events.KindNodeLeftEvent,
		Source:     x.this.String(),
		NodeLeft:   m.String(),
		NodeMeta:   m.Meta,
		Generation: generation,
		Timestamp:  time.Now().UnixNano(),
	}

	data, err := message.Encode()
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to encode NodeLeftEvent: %v", err)
		return
	}

	err = x.PublishLocalClusterEvent(x.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to publish NodeLeftEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

// publishRebalanceStartEvent announces the epoch started for the table pushed
// as generation. startedAt is the epoch's start time in nanoseconds: it is
// taken before the epoch could complete, so a completion published right
// away never carries an earlier timestamp than its start.
func (r *RoutingTable) publishRebalanceStartEvent(epoch, generation uint64, reason, node string, startedAt int64) {
	message := events.RebalanceStartEvent{
		Kind:       events.KindRebalanceStartEvent,
		Source:     r.this.String(),
		Epoch:      epoch,
		Generation: generation,
		Reason:     reason,
		Node:       node,
		Timestamp:  startedAt,
	}
	data, err := message.Encode()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to encode RebalanceStartEvent: %v", err)
		return
	}
	err = r.PublishClusterEvent(r.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to publish RebalanceStartEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

// publishRebalanceCompleteEvent announces the completion of the epoch whose
// table was pushed as generation and computed for members.
func (r *RoutingTable) publishRebalanceCompleteEvent(epoch, generation uint64, members []string) {
	// Check if context is already canceled (node is shutting down)
	select {
	case <-r.ctx.Done():
		// Node is shutting down, don't try to publish
		return
	default:
	}

	message := events.RebalanceCompleteEvent{
		Kind:       events.KindRebalanceCompleteEvent,
		Source:     r.this.String(),
		Epoch:      epoch,
		Generation: generation,
		Members:    members,
		Timestamp:  time.Now().UnixNano(),
	}
	data, err := message.Encode()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to encode RebalanceCompleteEvent: %v", err)
		return
	}
	err = r.PublishClusterEvent(r.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		// Don't log if context was canceled (expected during shutdown)
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		r.log.V(3).Printf("[ERROR] Failed to publish RebalanceCompleteEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}
