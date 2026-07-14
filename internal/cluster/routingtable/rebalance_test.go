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

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/discovery"
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
	rt.SetClusterEventPublisher(func(_ context.Context, channel, message string) error {
		require.Equal(t, events.ClusterEventsChannel, channel)

		var envelope struct {
			Kind string `json:"kind"`
		}
		require.NoError(t, json.Unmarshal([]byte(message), &envelope))
		if envelope.Kind == events.KindRebalanceCompleteEvent {
			var ev events.RebalanceCompleteEvent
			require.NoError(t, json.Unmarshal([]byte(message), &ev))
			done <- ev
		}
		return nil
	})

	rt.startRebalanceEpoch(42, rebalanceReasonNodeLeft, peer.String(), []uint64{rt.this.ID, peer.ID})
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
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(signature, rt.this.ID))

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
	rt.startRebalanceEpoch(0, rebalanceReasonManual, "", []uint64{1})
	epoch, pending, _, _ := rt.getRebalanceState()
	require.Zero(t, epoch)
	require.Zero(t, pending)

	// Without any members that confirmed the table push there is nothing to track.
	rt.startRebalanceEpoch(42, rebalanceReasonManual, "", nil)
	epoch, pending, _, _ = rt.getRebalanceState()
	require.Zero(t, epoch)
	require.Zero(t, pending)
}

func TestHandleRebalanceAck_EdgeCases(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	// No active epoch and no committed table: the ack is stale.
	require.Equal(t, ackStale, rt.handleRebalanceAck(5, 1))

	rt.rebalanceMtx.Lock()
	rt.rebalanceState = rebalanceState{
		epoch:   5,
		pending: map[uint64]struct{}{1: {}},
		acked:   map[uint64]struct{}{},
	}
	rt.rebalanceMtx.Unlock()

	// Mismatched epoch that doesn't match the committed table either.
	require.Equal(t, ackStale, rt.handleRebalanceAck(6, 1))

	// A member that is not in the pending set is acknowledged as a no-op.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(5, 99))

	// The first ack is recorded. The member is not in the members list, so
	// the epoch cannot complete yet.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(5, 1))
	// A duplicate ack is idempotent.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(5, 1))

	// A completed epoch always accepts.
	rt.rebalanceMtx.Lock()
	rt.rebalanceState.completed = true
	rt.rebalanceMtx.Unlock()
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(5, 1))
}

func TestHandleRebalanceAck_EarlyAckIsBufferedAndHarvested(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	otherCfg := testutil.NewConfig()
	otherCfg.MemberlistConfig.Name = "127.0.0.1:9998"
	other := discovery.NewMember(otherCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Add(other)
	rt.Members().Unlock()

	// The table with signature 42 has been committed, but its epoch has not
	// started yet: a fast member acks during the push fan-out.
	rt.setSignature(42)
	require.Equal(t, ackEarly, rt.handleRebalanceAck(42, peer.ID))

	// An ack that matches neither the active epoch nor the committed table.
	require.Equal(t, ackStale, rt.handleRebalanceAck(41, peer.ID))

	// The epoch starts: the buffered ack is harvested.
	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID, other.ID})
	epoch, pending, acked, completed := rt.getRebalanceState()
	require.Equal(t, uint64(42), epoch)
	require.Equal(t, 2, pending)
	require.Equal(t, 1, acked)
	require.False(t, completed)

	// The remaining member completes the epoch.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(42, other.ID))
	_, _, _, completed = rt.getRebalanceState()
	require.True(t, completed)
}

func TestStartRebalanceEpoch_UnpushedMemberDoesNotBlockCompletion(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	joinerCfg := testutil.NewConfig()
	joinerCfg.MemberlistConfig.Name = "127.0.0.1:9998"
	joiner := discovery.NewMember(joinerCfg)

	// Both members are known to memberlist, but the joiner never received the
	// routing table push (it cannot bootstrap, so it can never ack).
	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Add(joiner)
	rt.Members().Unlock()

	rt.setSignature(42)
	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID})

	// The epoch completes with the pushed member's ack alone.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(42, peer.ID))
	_, _, _, completed := rt.getRebalanceState()
	require.True(t, completed)
}

func TestStartRebalanceEpoch_CompletesImmediatelyWhenAllAckedEarly(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Unlock()

	rt.setSignature(42)
	require.Equal(t, ackEarly, rt.handleRebalanceAck(42, peer.ID))

	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID})
	_, _, _, completed := rt.getRebalanceState()
	require.True(t, completed)
}

func TestSendRebalanceAck_StaleEpoch(t *testing.T) {
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

	// The single node is the coordinator and has an active epoch for its own
	// signature. An ack for a superseded epoch is reported as stale, not as an
	// error.
	require.NotEqual(t, uint64(999999999), rt.Signature())
	err = rt.SendRebalanceAck(999999999)
	require.ErrorIs(t, err, ErrStaleRebalanceAck)

	// An ack for the active epoch is accepted.
	require.NoError(t, rt.SendRebalanceAck(rt.Signature()))
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
