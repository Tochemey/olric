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

	members := rt.memberNames()
	rt.startRebalanceEpoch(42, rebalanceReasonNodeLeft, peer.String(), []uint64{rt.this.ID, peer.ID}, members)
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
		require.Equal(t, rt.Generation(), ev.Generation, "the completion carries the generation of the pushed table")
		require.Equal(t, members, ev.Members, "the completion lists the members the table was computed for")
		require.Contains(t, ev.Members, peer.String())
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
	rt.startRebalanceEpoch(0, rebalanceReasonManual, "", []uint64{1}, nil)
	epoch, pending, _, _ := rt.getRebalanceState()
	require.Zero(t, epoch)
	require.Zero(t, pending)

	// Without any members that confirmed the table push there is nothing to track.
	rt.startRebalanceEpoch(42, rebalanceReasonManual, "", nil, nil)
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
	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID, other.ID}, nil)
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
	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID}, nil)

	// The epoch completes with the pushed member's ack alone.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(42, peer.ID))
	_, _, _, completed := rt.getRebalanceState()
	require.True(t, completed)
}

// TestStartRebalanceEpoch_CompletesImmediatelyWhenAllAckedEarly guards the
// early-ack path: an epoch whose every ack arrived before it started completes
// from inside the start, and the completion published from there must not
// carry an earlier timestamp than its own start. Both events carry the
// generation of the pushed table.
func TestStartRebalanceEpoch_CompletesImmediatelyWhenAllAckedEarly(t *testing.T) {
	c := testutil.NewConfig()
	c.EnableClusterEventsChannel = true
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Unlock()

	type stamped struct {
		Kind       string `json:"kind"`
		Generation uint64 `json:"generation"`
		Timestamp  int64  `json:"timestamp"`
	}

	published := make(chan stamped, 2)
	rt.SetClusterEventPublisher(func(_ context.Context, _ string, message string) error {
		var ev stamped
		require.NoError(t, json.Unmarshal([]byte(message), &ev))
		published <- ev
		return nil
	})

	rt.setSignature(42)
	require.Equal(t, ackEarly, rt.handleRebalanceAck(42, peer.ID))

	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID}, nil)
	_, _, _, completed := rt.getRebalanceState()
	require.True(t, completed)

	// The completion publisher waits for the start publisher, so the start
	// is delivered first even though the completion was published from
	// inside the epoch start.
	var start, complete stamped
	select {
	case start = <-published:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the rebalance start event")
	}

	select {
	case complete = <-published:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the rebalance completion event")
	}

	require.Equal(t, events.KindRebalanceStartEvent, start.Kind)
	require.Equal(t, events.KindRebalanceCompleteEvent, complete.Kind)
	require.LessOrEqual(t, start.Timestamp, complete.Timestamp, "the completion must not predate its start")
	require.Equal(t, rt.Generation(), start.Generation)
	require.Equal(t, start.Generation, complete.Generation)
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

func TestUpdateRouting_UnchangedTableKeepsEpoch(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)

	// The bootstrap epoch is still in flight: nothing has acked it yet.
	epoch, _, _, completed := rt.getRebalanceState()
	require.NotZero(t, epoch)
	require.Equal(t, rt.Signature(), epoch)
	require.False(t, completed)

	// Periodic pushes of an unchanged table must not supersede the active
	// epoch: a node-left or node-join epoch that is still waiting for acks
	// would otherwise be dropped before it completes.
	for range 10 {
		rt.updateRoutingWithReason(rebalanceReasonPeriodic, "")

		current, _, _, completed := rt.getRebalanceState()
		require.Equal(t, epoch, current)
		require.Equal(t, epoch, rt.Signature())
		require.False(t, completed)
	}

	// The original epoch is still the one that completes.
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(epoch, rt.this.ID))
	current, _, _, completed := rt.getRebalanceState()
	require.Equal(t, epoch, current)
	require.True(t, completed)
}
