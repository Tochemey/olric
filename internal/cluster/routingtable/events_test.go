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
		return nil
	})

	m := discovery.NewMember(c)
	rt.wg.Add(1)
	go rt.publishNodeJoinEvent(&m)
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
		return nil
	})

	m := discovery.NewMember(c)
	rt.wg.Add(1)
	go rt.publishNodeLeftEvent(&m)
	<-ctx.Done()
	require.ErrorIs(t, context.Canceled, ctx.Err())
}

func TestRoutingTable_publishRebalanceStartEvent(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		defer cancel()

		require.Equal(t, events.ClusterEventsChannel, channel)

		v := events.RebalanceStartEvent{}
		require.NoError(t, json.Unmarshal([]byte(message), &v))
		require.Equal(t, events.KindRebalanceStartEvent, v.Kind)
		require.Equal(t, rt.this.String(), v.Source)
		require.Equal(t, uint64(42), v.Epoch)
		require.Equal(t, "node-left", v.Reason)
		require.Equal(t, "127.0.0.1:9999", v.Node)
		return nil
	})

	rt.wg.Add(1)
	go rt.publishRebalanceStartEvent(42, "node-left", "127.0.0.1:9999")
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
		return nil
	})

	rt.wg.Add(1)
	go rt.publishRebalanceCompleteEvent(42)
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
	rt.wg.Add(1)
	rt.publishRebalanceCompleteEvent(42)
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

	rt.wg.Add(1)
	rt.publishRebalanceCompleteEvent(42)
}
