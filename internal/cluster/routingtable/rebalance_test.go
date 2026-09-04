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
	"log"
	"strings"
	"sync"
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
	rt.setVersion(42, 0, 0)
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

// TestStartRebalanceEpoch_HarvestsEarlyAckOutsidePending checks that an early
// ack from a member the fan-out did not reach is kept at the epoch start, so
// that the member counts as acked as soon as the retried push admits it.
func TestStartRebalanceEpoch_HarvestsEarlyAckOutsidePending(t *testing.T) {
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

	// peer pulled the committed table during the fan-out and acked; the push
	// to it failed, so it is not in the pending set when the epoch starts.
	rt.setVersion(42, 0, 0)
	require.Equal(t, ackEarly, rt.handleRebalanceAck(42, peer.ID))
	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{other.ID}, nil)
	_, pending, acked, completed := rt.getRebalanceState()
	require.Equal(t, 1, pending)
	require.Equal(t, 1, acked, "the early ack of a member outside the pending set is kept")
	require.False(t, completed)

	// The retried push lands: peer joins the pending set already acked, and
	// the other member's ack completes the epoch.
	rt.admitLatePush(42, peer, &leftOverDataReport{})
	_, pending, _, completed = rt.getRebalanceState()
	require.Equal(t, 2, pending)
	require.False(t, completed)
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(42, other.ID))
	_, _, _, completed = rt.getRebalanceState()
	require.True(t, completed)
}

// TestStartRebalanceEpoch_UnpushedLiveMemberGatesCompletion checks that a
// live member the push did not reach still gates the epoch: it installs the
// table through the retried push or its own pull and acks then, and until it
// does the completion, which tells consumers every member routes on the new
// table, is not announced.
func TestStartRebalanceEpoch_UnpushedLiveMemberGatesCompletion(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	joinerCfg := testutil.NewConfig()
	joinerCfg.MemberlistConfig.Name = "127.0.0.1:9998"
	joiner := discovery.NewMember(joinerCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Add(joiner)
	rt.Members().Unlock()

	// Both members are known to memberlist; the joiner rejected the push.
	rt.setVersion(42, 0, 0)
	rt.startRebalanceEpoch(42, rebalanceReasonNodeJoin, "", []uint64{peer.ID, joiner.ID}, nil)

	require.Equal(t, ackAccepted, rt.handleRebalanceAck(42, peer.ID))
	_, pending, acked, completed := rt.getRebalanceState()
	require.Equal(t, 2, pending)
	require.Equal(t, 1, acked)
	require.False(t, completed, "a live member still on the old table holds the completion")

	// The retried push lands and the joiner acks.
	rt.admitLatePush(42, joiner, &leftOverDataReport{})
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(42, joiner.ID))
	_, _, _, completed = rt.getRebalanceState()
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

	rt.setVersion(42, 0, 0)
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

// TestHandleRebalanceAck_RecordsAckOutsidePending guards that an ack from a
// member outside the pending set is recorded, so a member admitted to the
// epoch later, after a push retry, does not have to ack again.
func TestHandleRebalanceAck_RecordsAckOutsidePending(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	rt.rebalanceMtx.Lock()
	rt.rebalanceState = rebalanceState{
		epoch:   5,
		pending: map[uint64]struct{}{1: {}},
		acked:   map[uint64]struct{}{},
	}
	rt.rebalanceMtx.Unlock()

	require.Equal(t, ackAccepted, rt.handleRebalanceAck(5, 99))

	rt.rebalanceMtx.Lock()
	_, recorded := rt.rebalanceState.acked[99]
	rt.rebalanceMtx.Unlock()
	require.True(t, recorded, "the ack of a member outside the pending set must be kept")

	// The member is admitted late; both members are live.
	rt.Members().Lock()
	rt.Members().Add(discovery.Member{ID: 1, Name: "127.0.0.1:1"})
	rt.Members().Add(discovery.Member{ID: 99, Name: "127.0.0.1:99"})
	rt.Members().Unlock()

	rt.rebalanceMtx.Lock()
	rt.rebalanceState.pending[99] = struct{}{}
	rt.rebalanceMtx.Unlock()

	require.Equal(t, ackAccepted, rt.handleRebalanceAck(5, 1))
	_, _, _, completed := rt.getRebalanceState()
	require.True(t, completed, "the earlier ack of the late member must count")
}

// TestHandleRebalanceAck_EarlyAckMatchesCommittedSignature guards that an ack
// for the table the coordinator committed but has not installed yet, as a
// member that pulled it during the fan-out sends, is buffered rather than
// reported stale.
func TestHandleRebalanceAck_EarlyAckMatchesCommittedSignature(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	rt.committedPayload.Store([]byte("committed but not installed"))
	epoch := rt.committedSignature()
	require.NotZero(t, epoch)
	require.NotEqual(t, rt.Signature(), epoch)

	require.Equal(t, ackEarly, rt.handleRebalanceAck(epoch, 7))

	rt.rebalanceMtx.Lock()
	_, buffered := rt.earlyAcks[epoch][7]
	rt.rebalanceMtx.Unlock()
	require.True(t, buffered)
}

// lockedLog is an io.Writer safe for the logger and the test to share.
type lockedLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.String()
}

// TestWatchOverdueEpoch_LogsMissingMembers guards the observability of a stuck
// convergence: an epoch still open after overdueEpochIntervals balancer
// intervals is logged with the members whose ack is missing, and the log
// stops once the epoch completes.
func TestWatchOverdueEpoch_LogsMissingMembers(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	logs := &lockedLog{}
	c := testutil.NewConfig()
	c.TriggerBalancerInterval = 50 * time.Millisecond
	c.Logger = log.New(logs, "", 0)
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	silent := discovery.Member{Name: "127.0.0.1:1", ID: 424242}
	rt.Members().Lock()
	rt.Members().Add(silent)
	rt.Members().Unlock()

	rt.startRebalanceEpoch(rt.Signature(), rebalanceReasonManual, "", []uint64{rt.This().ID, silent.ID}, nil)

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "still open") && strings.Contains(logs.String(), silent.Name)
	}, 3*time.Second, 20*time.Millisecond, "the overdue epoch must be logged with the missing member")

	require.Equal(t, ackAccepted, rt.handleRebalanceAck(rt.Signature(), rt.This().ID))
	require.Equal(t, ackAccepted, rt.handleRebalanceAck(rt.Signature(), silent.ID))
	_, _, _, completed := rt.getRebalanceState()
	require.True(t, completed)
}
