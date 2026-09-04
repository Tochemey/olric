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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/testutil"
)

func TestRoutingTable_publishNodeJoinEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		defer cancel()

		require.Equal(t, events.ClusterEventsChannel, channel)

		v := events.NodeJoinEvent{}
		require.NoError(t, json.Unmarshal([]byte(message), &v))
		require.Equal(t, events.KindNodeJoinEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, rt.this.String(), v.NodeJoin)
		require.Equal(t, uint64(7), v.Generation)
		return nil
	})

	m := discovery.NewMember(c)
	go rt.publishNodeJoinEvent(&m, 7)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

func TestRoutingTable_publishNodeLeftEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		defer cancel()

		require.Equal(t, events.ClusterEventsChannel, channel)

		v := events.NodeLeftEvent{}
		require.NoError(t, json.Unmarshal([]byte(message), &v))
		require.Equal(t, events.KindNodeLeftEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, rt.this.String(), v.NodeLeft)
		require.Equal(t, uint64(7), v.Generation)
		return nil
	})

	m := discovery.NewMember(c)
	go rt.publishNodeLeftEvent(&m, 7)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

func TestRoutingTable_publishRebalanceStartEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	startedAt := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		defer cancel()

		require.Equal(t, events.ClusterEventsChannel, channel)

		v := events.RebalanceStartEvent{}
		require.NoError(t, json.Unmarshal([]byte(message), &v))
		require.Equal(t, events.KindRebalanceStartEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, uint64(42), v.Epoch)
		require.Equal(t, uint64(7), v.Generation)
		require.Equal(t, "node-left", v.Reason)
		require.Equal(t, "127.0.0.1:9999", v.Node)
		require.Equal(t, startedAt, v.Timestamp, "the start carries the timestamp taken at the epoch start")
		return nil
	})

	go rt.publishRebalanceStartEvent(42, 7, "node-left", "127.0.0.1:9999", startedAt)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

func TestRoutingTable_publishRebalanceCompleteEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		defer cancel()

		require.Equal(t, events.ClusterEventsChannel, channel)

		v := events.RebalanceCompleteEvent{}
		require.NoError(t, json.Unmarshal([]byte(message), &v))
		require.Equal(t, events.KindRebalanceCompleteEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, uint64(42), v.Epoch)
		require.Equal(t, uint64(7), v.Generation)
		require.Equal(t, []string{"127.0.0.1:1", "127.0.0.1:2"}, v.Members)
		return nil
	})

	go rt.publishRebalanceCompleteEvent(42, 7, []string{"127.0.0.1:1", "127.0.0.1:2"})
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

func TestRoutingTable_publishRebalanceCompleteEvent_Canceled(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	rt.SetClusterEventPublisher(func(context.Context, string, string) error {
		t.Error("nothing must be published after shutdown")
		return nil
	})

	// The node is shutting down: nothing must be published.
	rt.cancel()
	rt.publishRebalanceCompleteEvent(42, 7, nil)
}

func TestRoutingTable_publishRebalanceCompleteEvent_CanceledDuringPublish(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	rt.SetClusterEventPublisher(func(context.Context, string, string) error {
		// Cancel the routing table context before failing the publish: the
		// error must be swallowed silently because the node is shutting down.
		rt.cancel()
		return errors.New("publish failed")
	})

	rt.publishRebalanceCompleteEvent(42, 7, nil)
}

// TestRoutingTable_publishMembershipChangeEvent guards the wire form of the
// coordinator's membership announcement: change, node, the member set after
// the change and the generation held when it was observed.
func TestRoutingTable_publishMembershipChangeEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		defer cancel()

		require.Equal(t, events.ClusterEventsChannel, channel)

		v := events.MembershipChangeEvent{}
		require.NoError(t, json.Unmarshal([]byte(message), &v))
		require.Equal(t, events.KindMembershipChangeEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, events.MembershipChangeLeft, v.Change)
		require.Equal(t, "127.0.0.1:9999", v.Node)
		require.Equal(t, []string{"127.0.0.1:1", "127.0.0.1:2"}, v.Members)
		require.Equal(t, uint64(7), v.Generation)
		return nil
	})

	m := discovery.Member{Name: "127.0.0.1:9999"}
	go rt.publishMembershipChangeEvent(events.MembershipChangeLeft, &m, []string{"127.0.0.1:1", "127.0.0.1:2"}, 7)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

// TestRoutingTable_localObservationsUseLocalPublisher guards where each kind
// of event goes: a member's own node-left observation takes the local
// publisher when one is registered, while the coordinator's membership change
// takes the cluster-wide one.
func TestRoutingTable_localObservationsUseLocalPublisher(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	local := make(chan string, 4)
	wide := make(chan string, 4)
	rt.SetLocalClusterEventPublisher(func(_ context.Context, _ string, message string) error {
		local <- message
		return nil
	})
	rt.SetClusterEventPublisher(func(_ context.Context, _ string, message string) error {
		wide <- message
		return nil
	})

	m := discovery.NewMember(c)
	rt.publishNodeLeftEvent(&m, 1)
	rt.publishNodeJoinEvent(&m, 1)
	rt.publishMembershipChangeEvent(events.MembershipChangeJoin, &m, []string{m.String()}, 1)

	require.Len(t, local, 2, "the observations stay local")
	require.Len(t, wide, 1, "the membership change is announced cluster-wide")
	require.Contains(t, <-wide, events.KindMembershipChangeEvent)
}
