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

func (r *RoutingTable) publishNodeJoinEvent(m *discovery.Member) {
	defer r.wg.Done()

	message := events.NodeJoinEvent{
		Kind:      events.KindNodeJoinEvent,
		Source:    r.this.String(),
		NodeJoin:  m.String(),
		NodeMeta:  m.Meta,
		Timestamp: time.Now().UnixNano(),
	}
	data, err := message.Encode()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to encode NodeJoinEvent: %v", err)
		return
	}
	err = r.PublishClusterEvent(r.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to publish NodeJoinEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

func (r *RoutingTable) publishNodeLeftEvent(m *discovery.Member) {
	defer r.wg.Done()

	message := events.NodeLeftEvent{
		Kind:      events.KindNodeLeftEvent,
		Source:    r.this.String(),
		NodeLeft:  m.String(),
		NodeMeta:  m.Meta,
		Timestamp: time.Now().UnixNano(),
	}
	data, err := message.Encode()
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to encode NodeLeftEvent: %v", err)
		return
	}
	err = r.PublishClusterEvent(r.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to publish NodeLeftEvent to %s: %v", events.ClusterEventsChannel, err)
	}
}

func (r *RoutingTable) publishRebalanceStartEvent(epoch uint64, reason, node string) {
	defer r.wg.Done()

	message := events.RebalanceStartEvent{
		Kind:      events.KindRebalanceStartEvent,
		Source:    r.this.String(),
		Epoch:     epoch,
		Reason:    reason,
		Node:      node,
		Timestamp: time.Now().UnixNano(),
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

func (r *RoutingTable) publishRebalanceCompleteEvent(epoch uint64) {
	defer r.wg.Done()

	// Check if context is already canceled (node is shutting down)
	select {
	case <-r.ctx.Done():
		// Node is shutting down, don't try to publish
		return
	default:
	}

	message := events.RebalanceCompleteEvent{
		Kind:      events.KindRebalanceCompleteEvent,
		Source:    r.this.String(),
		Epoch:     epoch,
		Timestamp: time.Now().UnixNano(),
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
