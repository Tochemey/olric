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
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/testutil"
)

func TestRoutingTable_rebalanceCompleteEventOnAck(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	c.EnableClusterEventsChannel = true
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Unlock()

	done := make(chan events.RebalanceCompleteEvent, 1)
	rt.server.ServeMux().HandleFunc(protocol.PubSub.Publish, func(conn redcon.Conn, cmd redcon.Command) {
		publishCmd, err := protocol.ParsePublishCommand(cmd)
		require.NoError(t, err)
		require.Equal(t, events.ClusterEventsChannel, publishCmd.Channel)

		var envelope struct {
			Kind string `json:"kind"`
		}
		err = json.Unmarshal([]byte(publishCmd.Message), &envelope)
		require.NoError(t, err)
		if envelope.Kind == events.KindRebalanceCompleteEvent {
			var ev events.RebalanceCompleteEvent
			err = json.Unmarshal([]byte(publishCmd.Message), &ev)
			require.NoError(t, err)
			done <- ev
		}

		conn.WriteInt(1)
	})

	rt.startRebalanceEpoch(42, rebalanceReasonNodeLeft, peer.String())
	rt.handleRebalanceAck(42, rt.this.ID)

	select {
	case <-done:
		t.Fatalf("rebalance completion should wait for all acks")
	case <-time.After(50 * time.Millisecond):
	}

	rt.handleRebalanceAck(42, peer.ID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case ev := <-done:
		require.Equal(t, events.KindRebalanceCompleteEvent, ev.Kind)
		require.Equal(t, rt.this.String(), ev.Source)
		require.Equal(t, uint64(42), ev.Epoch)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for rebalance completion event")
	}
}

func TestRoutingTable_bootstrapStartsRebalanceEpoch(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)

	signature := rt.Signature()
	if signature == 0 {
		t.Skip("unexpected zero signature")
	}

	rt.rebalanceMtx.Lock()
	epoch := rt.rebalanceState.epoch
	pendingLen := len(rt.rebalanceState.pending)
	rt.rebalanceMtx.Unlock()

	require.Equal(t, signature, epoch)
	require.NotZero(t, pendingLen)
	require.True(t, rt.handleRebalanceAck(signature, rt.this.ID))

	rt.rebalanceMtx.Lock()
	completed := rt.rebalanceState.completed
	rt.rebalanceMtx.Unlock()
	require.True(t, completed)
}

func TestRebalanceReasonFromEvent(t *testing.T) {
	c := testutil.NewConfig()
	member := discovery.NewMember(c)
	meta, err := member.Encode()
	require.NoError(t, err)

	reason, node := rebalanceReasonFromEvent(&discovery.ClusterEvent{Event: memberlist.NodeJoin, NodeMeta: meta})
	require.Equal(t, rebalanceReasonNodeJoin, reason)
	require.Equal(t, member.String(), node)

	reason, node = rebalanceReasonFromEvent(&discovery.ClusterEvent{Event: memberlist.NodeLeave, NodeMeta: meta})
	require.Equal(t, rebalanceReasonNodeLeft, reason)
	require.Equal(t, member.String(), node)

	reason, node = rebalanceReasonFromEvent(&discovery.ClusterEvent{Event: memberlist.NodeUpdate, NodeMeta: meta})
	require.Equal(t, rebalanceReasonNodeUpdate, reason)
	require.Equal(t, member.String(), node)

	reason, node = rebalanceReasonFromEvent(&discovery.ClusterEvent{Event: memberlist.NodeEventType(42), NodeMeta: meta})
	require.Equal(t, rebalanceReasonUnknown, reason)
	require.Equal(t, "", node)
}

func TestStartRebalanceEpoch_EarlyReturns(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	// A zero epoch is ignored.
	rt.startRebalanceEpoch(0, rebalanceReasonManual, "")
	epoch, pending, _, _ := rt.getRebalanceState()
	require.Zero(t, epoch)
	require.Zero(t, pending)

	// Without any known members there is nothing to track.
	rt.startRebalanceEpoch(42, rebalanceReasonManual, "")
	epoch, pending, _, _ = rt.getRebalanceState()
	require.Zero(t, epoch)
	require.Zero(t, pending)
}

func TestHandleRebalanceAck_EdgeCases(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	// No active epoch.
	require.False(t, rt.handleRebalanceAck(5, 1))

	rt.rebalanceMtx.Lock()
	rt.rebalanceState = rebalanceState{
		epoch:   5,
		pending: map[uint64]struct{}{1: {}},
		acked:   map[uint64]struct{}{},
	}
	rt.rebalanceMtx.Unlock()

	// Mismatched epoch.
	require.False(t, rt.handleRebalanceAck(6, 1))

	// A member that is not in the pending set is acknowledged as a no-op.
	require.True(t, rt.handleRebalanceAck(5, 99))

	// The first ack is recorded. The member is not in the members list, so
	// the epoch cannot complete yet.
	require.True(t, rt.handleRebalanceAck(5, 1))
	// A duplicate ack is idempotent.
	require.True(t, rt.handleRebalanceAck(5, 1))

	// A completed epoch always reports true.
	rt.rebalanceMtx.Lock()
	rt.rebalanceState.completed = true
	rt.rebalanceMtx.Unlock()
	require.True(t, rt.handleRebalanceAck(5, 1))
}

func TestCheckRebalanceCompletionLocked_EarlyReturn(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	// No active epoch: the check is a no-op.
	rt.rebalanceMtx.Lock()
	rt.checkRebalanceCompletionLocked()
	completed := rt.rebalanceState.completed
	rt.rebalanceMtx.Unlock()
	require.False(t, completed)
}

func TestCheckRebalanceCompletionLocked_SkipsEventOnShutdown(t *testing.T) {
	c := testutil.NewConfig()
	c.EnableClusterEventsChannel = true
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Unlock()

	// The node is shutting down: completion is recorded but the rebalance
	// completion event must not be published.
	rt.cancel()

	rt.rebalanceMtx.Lock()
	rt.rebalanceState = rebalanceState{
		epoch:   7,
		pending: map[uint64]struct{}{peer.ID: {}},
		acked:   map[uint64]struct{}{peer.ID: {}},
	}
	rt.checkRebalanceCompletionLocked()
	completed := rt.rebalanceState.completed
	rt.rebalanceMtx.Unlock()

	require.True(t, completed)
}

func TestSendRebalanceAck_ZeroEpoch(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))
	require.NoError(t, rt.SendRebalanceAck(0))
}

func TestSendRebalanceAck_ProcessError(t *testing.T) {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	require.NoError(t, rt.Join())
	require.NoError(t, rt.Start())
	defer func() {
		require.NoError(t, rt.Shutdown(context.Background()))
	}()

	// Stop the TCP server: the ack cannot be delivered to the coordinator.
	require.NoError(t, srv.Shutdown(context.Background()))
	require.Error(t, rt.SendRebalanceAck(42))
}
