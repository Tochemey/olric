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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/internal/testutil/mockfragment"
)

func TestPartitionsPendingReceive(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)
	require.True(t, rt.IsBootstrapped())

	// On a freshly bootstrapped single node this member owns every primary
	// partition but holds no data yet, so all partitions are pending receive.
	pending := rt.partitionsPendingReceive()
	require.NotNil(t, pending)
	require.Len(t, pending, int(rt.config.PartitionCount))
}

func TestPartitionsPendingReceive_WithData(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	// Mark partition 0 as holding data so it is no longer pending receive.
	frag := mockfragment.New()
	frag.Fill()
	part := rt.primary.PartitionByID(0)
	part.Map().Store("dmap.test", frag)

	pending := rt.partitionsPendingReceive()
	for _, id := range pending {
		require.NotEqual(t, uint64(0), id)
	}
}

func TestPartitionsPendingReceive_BackupOwner(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	// Give every primary partition some data: the primary branch no longer
	// reports them as pending.
	for partID := uint64(0); partID < rt.config.PartitionCount; partID++ {
		frag := mockfragment.New()
		frag.Fill()
		rt.primary.PartitionByID(partID).Map().Store("dmap.test", frag)
	}

	// This node is a backup owner for partition 2 but holds no backup data.
	rt.backup.PartitionByID(2).SetOwners([]discovery.Member{rt.This()})

	pending := rt.partitionsPendingReceive()
	require.Equal(t, []uint64{2}, pending)
}

// newLoneNodeWithSyncState starts a single-member routing table that tracks a
// sync state with the given escape timeout, and waits for its bootstrap.
func newLoneNodeWithSyncState(t *testing.T, escape time.Duration) *RoutingTable {
	t.Helper()

	c := testutil.NewConfig()
	c.InitialSyncEmptyPartitionTimeout = escape
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	rt.syncState = syncstate.New()
	require.NoError(t, rt.Join())
	require.NoError(t, rt.Start())

	t.Cleanup(func() {
		require.NoError(t, rt.Shutdown(context.Background()))
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	require.True(t, rt.IsBootstrapped())
	return rt
}

// TestPartitionsAwaitingData_NoLiveSource guards the source probe on the
// install path: a lone member owns every partition, holds no data and has no
// other owner to receive data from, so every partition is pending receive
// but none is worth waiting on, and the install must not have started the
// escape clock at all.
func TestPartitionsAwaitingData_NoLiveSource(t *testing.T) {
	rt := newLoneNodeWithSyncState(t, time.Minute)

	require.Len(t, rt.partitionsPendingReceive(), int(rt.config.PartitionCount))
	require.Empty(t, rt.partitionsAwaitingData())
	require.True(t, rt.syncState.PendingEmpty())
}

// TestPartitionsAwaitingData_KeepsExpired guards that partitions whose escape
// deadline elapsed are awaited as they are: the sync state no longer waits on
// them, so there is nothing to gain from asking their owners.
func TestPartitionsAwaitingData_KeepsExpired(t *testing.T) {
	rt := newLoneNodeWithSyncState(t, time.Minute)

	pending := rt.partitionsPendingReceive()
	rt.syncState.Reconcile(pending, time.Nanosecond, time.Now())
	time.Sleep(time.Millisecond)

	require.Equal(t, pending, rt.partitionsAwaitingData())
	require.True(t, rt.syncState.PendingEmpty())
}

// TestPartitionsHeldBy guards what counts as a source for an owned-but-empty
// partition: an owner answering with a non-zero key count, or an owner that
// cannot be answered for.
func TestPartitionsHeldBy(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt1, err := cluster.addNode(nil)
	require.NoError(t, err)
	rt2, err := cluster.addNode(nil)
	require.NoError(t, err)

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	})

	require.NoError(t, err)

	// Partitions seen from rt2 whose primary owner is rt1. With seven
	// partitions and bounded loads each member owns at least two.
	var byRT1 []uint64
	for partID := uint64(0); partID < rt2.config.PartitionCount; partID++ {
		if rt2.primary.PartitionByID(partID).Owner().CompareByID(rt1.This()) {
			byRT1 = append(byRT1, partID)
		}
	}

	require.GreaterOrEqual(t, len(byRT1), 2)
	withData, empty := byRT1[0], byRT1[1]

	frag := mockfragment.New()
	frag.Fill()
	rt1.primary.PartitionByID(withData).Map().Store("dmap.test", frag)

	queries := []countQuery{
		{partID: withData},
		{partID: empty},
		{partID: withData, replica: true},
	}

	require.Equal(t, []uint64{withData}, rt2.partitionsHeldBy(rt1.This(), queries), "only the primary copy holding data counts")

	// An owner that cannot be reached is assumed to hold data for every
	// queried partition.
	dead := discovery.Member{Name: "127.0.0.1:1"}
	require.Equal(t, []uint64{withData, empty, withData}, rt2.partitionsHeldBy(dead, queries))
}

func TestPrepareLeftOverDataReport_Backup(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	frag := mockfragment.New()
	frag.Fill()
	rt.backup.PartitionByID(3).Map().Store("dmap.test", frag)

	data, err := rt.prepareLeftOverDataReport()
	require.NoError(t, err)

	report := leftOverDataReport{}
	require.NoError(t, msgpack.Unmarshal(data, &report))
	require.Contains(t, report.Backups, uint64(3))
}

func TestUpdateRoutingTableOnMember_InvalidReport(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	// A fake member that responds with a payload that cannot be decoded as
	// a left-over data report.
	fakeCfg := testutil.NewConfig()
	fakeSrv := testutil.NewServer(fakeCfg)
	fakeSrv.ServeMux().HandleFunc(protocol.Internal.UpdateRouting, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteBulkString("this is not msgpack")
	})
	go func() {
		if err := fakeSrv.ListenAndServe(); err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-fakeSrv.StartedCtx.Done()
	defer func() {
		require.NoError(t, fakeSrv.Shutdown(context.Background()))
	}()

	data, _, err := rt.buildRoutingTablePayload()
	require.NoError(t, err)

	fakeMember := discovery.Member{Name: fakeCfg.MemberlistConfig.Name}
	_, err = rt.updateRoutingTableOnMember(context.Background(), data, 0, fakeMember)
	require.Error(t, err)
}

func TestFetchRoutingTableFromCoordinator_Bootstraps(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt1, err := cluster.addNode(nil)
	require.NoError(t, err)
	rt2, err := cluster.addNode(nil)
	require.NoError(t, err)

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	// The coordinator does not pull; it bootstraps itself.
	require.NoError(t, rt1.fetchRoutingTableFromCoordinator(true))

	// Simulate a joiner whose routing table push was lost: it is joined but
	// not bootstrapped. The pull must install the coordinator's committed
	// table and bootstrap the node.
	atomic.StoreInt32(&rt2.bootstrapped, 0)
	require.NoError(t, rt2.fetchRoutingTableFromCoordinator(true))
	require.True(t, rt2.IsBootstrapped())
	require.Equal(t, rt1.Signature(), rt2.Signature())

	// A pull on an already bootstrapped node is a no-op.
	require.NoError(t, rt2.fetchRoutingTableFromCoordinator(true))
}

func TestUpdateRoutingTableOnCluster_Canceled(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	data, _, err := rt.buildRoutingTablePayload()
	require.NoError(t, err)

	// Cancel the routing table context: the semaphore cannot be acquired.
	rt.cancel()
	_, _, err = rt.updateRoutingTableOnCluster(data, 0)
	require.Error(t, err)
}

func TestBuildRoutingTablePayload_Deterministic(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)
	require.True(t, rt.IsBootstrapped())

	// buildRoutingTablePayload runs under the routing table lock in
	// production: the table must not be replaced while it is encoded.
	rt.Lock()
	defer rt.Unlock()

	first, signature, err := rt.buildRoutingTablePayload()
	require.NoError(t, err)
	require.NotZero(t, signature)

	// The signature is the rebalance epoch id, so it must be a pure function
	// of the table content: encoding an unchanged table again has to yield
	// the same bytes and the same signature.
	for range 50 {
		data, sign, err := rt.buildRoutingTablePayload()
		require.NoError(t, err)
		require.Equal(t, first, data)
		require.Equal(t, signature, sign)
	}

	// The canonical payload still decodes through the path members use in
	// applyRoutingTablePayload, so mixed-version clusters keep working.
	table := make(map[uint64]*route)
	require.NoError(t, msgpack.Unmarshal(first, &table))
	require.Equal(t, rt.table, table)
}

// newSourceFixture starts a lone member with a sync state and a second lone
// member, peer, in a cluster of its own, and shapes the first member's routing
// view so that each partition below models one way an owned-but-empty
// partition can have, or lack, a source:
//
//	0: pending as primary; peer holds a backup copy only     -> no source
//	1: pending as primary; peer is a previous primary owner with data -> source
//	2: pending as backup; peer is the primary owner with data          -> source
//	3: pending as backup; peer is a previous backup owner with data    -> source
//	4: pending as backup; peer owns the primary but holds nothing      -> no source
//
// The remaining partitions stay owned by the member alone, pending as primary
// with no other owner.
func newSourceFixture(t *testing.T) (rt, peer *RoutingTable) {
	t.Helper()

	rt = newLoneNodeWithSyncState(t, time.Minute)
	peer = newLoneNodeWithSyncState(t, time.Minute)
	fill := func(parts *partitions.Partitions, partID uint64) {
		frag := mockfragment.New()
		frag.Fill()
		parts.PartitionByID(partID).Map().Store("dmap.test", frag)
	}

	self, other := rt.This(), peer.This()

	rt.backup.PartitionByID(0).SetOwners([]discovery.Member{other})
	fill(peer.backup, 0)

	rt.primary.PartitionByID(1).SetOwners([]discovery.Member{other, self})
	fill(peer.primary, 1)

	rt.primary.PartitionByID(2).SetOwners([]discovery.Member{other})
	rt.backup.PartitionByID(2).SetOwners([]discovery.Member{self})
	fill(peer.primary, 2)

	rt.primary.PartitionByID(3).SetOwners([]discovery.Member{other})
	rt.backup.PartitionByID(3).SetOwners([]discovery.Member{other, self})
	fill(peer.backup, 3)

	rt.primary.PartitionByID(4).SetOwners([]discovery.Member{other})
	rt.backup.PartitionByID(4).SetOwners([]discovery.Member{self})

	return rt, peer
}

// TestPendingPartitions_Roles guards the role reported for each owned-but-empty
// partition, and that partitionsPendingReceive lists the same partitions.
func TestPendingPartitions_Roles(t *testing.T) {
	rt, _ := newSourceFixture(t)

	kinds := make(map[uint64]partitions.Kind)
	for _, p := range rt.pendingPartitions() {
		kinds[p.partID] = p.kind
	}

	require.Len(t, kinds, int(rt.config.PartitionCount))
	require.Equal(t, partitions.PRIMARY, kinds[0])
	require.Equal(t, partitions.PRIMARY, kinds[1])
	require.Equal(t, partitions.BACKUP, kinds[2])
	require.Equal(t, partitions.BACKUP, kinds[3])
	require.Equal(t, partitions.BACKUP, kinds[4])
	require.Equal(t, partitions.PRIMARY, kinds[5])
	require.Equal(t, partitionIDs(rt.pendingPartitions()), rt.partitionsPendingReceive())
}

// TestPartitionsAwaitingData_RoleAwareSources guards which owners count as a
// source: a partition pending as primary is awaited only when a previous
// primary owner holds a primary copy, never for a backup copy held elsewhere,
// which is restored off the convergence path; a partition pending as backup is
// awaited when the primary owner or a previous backup owner holds data.
func TestPartitionsAwaitingData_RoleAwareSources(t *testing.T) {
	rt, _ := newSourceFixture(t)

	require.Equal(t, []uint64{1, 2, 3}, rt.partitionsAwaitingData())
}

// activateEpoch puts rt's coordinator state in an active epoch for its
// installed table, pending on memberIDs, as startRebalanceEpoch leaves it
// after a fan-out that reached only those members.
func activateEpoch(rt *RoutingTable, memberIDs ...uint64) {
	signature, generation := rt.Version()
	pending := make(map[uint64]struct{}, len(memberIDs))
	for _, id := range memberIDs {
		pending[id] = struct{}{}
	}

	started := make(chan struct{})
	close(started)

	rt.rebalanceMtx.Lock()
	rt.rebalanceState = rebalanceState{
		epoch:          signature,
		generation:     generation,
		pending:        pending,
		acked:          map[uint64]struct{}{},
		startPublished: started,
	}
	rt.rebalanceMtx.Unlock()
}

// pendingMembers returns a copy of the pending set of the active epoch.
func pendingMembers(rt *RoutingTable) map[uint64]struct{} {
	rt.rebalanceMtx.Lock()
	defer rt.rebalanceMtx.Unlock()

	out := make(map[uint64]struct{}, len(rt.rebalanceState.pending))
	for id := range rt.rebalanceState.pending {
		out[id] = struct{}{}
	}

	return out
}

// newSettledPair starts a coordinator and a second member and waits until both
// have installed the same table.
func newSettledPair(t *testing.T, cluster *testCluster) (rt1, rt2 *RoutingTable) {
	t.Helper()

	rt1, err := cluster.addNode(nil)
	require.NoError(t, err)
	rt2, err = cluster.addNode(nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return rt1.Signature() != 0 && rt1.Signature() == rt2.Signature()
	}, 10*time.Second, 50*time.Millisecond, "both members must install the same table")

	return rt1, rt2
}

// TestRetryRoutingTablePush_AdmitsLateMember guards the retry: a member the
// fan-out missed receives the committed table on the retry, joins the pending
// set of the active epoch, and its ack then counts towards completion.
func TestRetryRoutingTablePush_AdmitsLateMember(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt1, rt2 := newSettledPair(t, cluster)
	signature := rt1.Signature()
	data, ok := rt1.committedPayload.Load().([]byte)
	require.True(t, ok)

	// As if the fan-out had reached the coordinator only.
	activateEpoch(rt1, rt1.This().ID)

	started := time.Now()
	rt1.retryRoutingTablePush(data, 1, signature, []discovery.Member{rt2.This()})
	require.Less(t, time.Since(started), 3*time.Second, "the first retry must land within its backoff")

	_, admitted := pendingMembers(rt1)[rt2.This().ID]
	require.True(t, admitted, "the late member must gate the epoch")

	require.Equal(t, ackAccepted, rt1.handleRebalanceAck(signature, rt2.This().ID))
	_, _, _, completed := rt1.getRebalanceState()
	require.False(t, completed, "the coordinator has not acked yet")

	require.Equal(t, ackAccepted, rt1.handleRebalanceAck(signature, rt1.This().ID))
	_, _, _, completed = rt1.getRebalanceState()
	require.True(t, completed)
}

// TestRetryRoutingTablePush_StopsWhenSuperseded guards that a retry for a
// table that is no longer the installed one ends without pushing: the newer
// table's own fan-out and retry take over.
func TestRetryRoutingTablePush_StopsWhenSuperseded(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt1, rt2 := newSettledPair(t, cluster)
	data, ok := rt1.committedPayload.Load().([]byte)
	require.True(t, ok)
	activateEpoch(rt1, rt1.This().ID)

	started := time.Now()
	rt1.retryRoutingTablePush(data, 1, rt1.Signature()+1, []discovery.Member{rt2.This()})
	require.Less(t, time.Since(started), 2*time.Second)

	_, admitted := pendingMembers(rt1)[rt2.This().ID]
	require.False(t, admitted, "a superseded retry must not touch the epoch")
}

// TestRetryRoutingTablePush_StopsWhenMemberLeft guards that a member memberlist
// removed is dropped from the retry without being dialed.
func TestRetryRoutingTablePush_StopsWhenMemberLeft(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt1, err := cluster.addNode(nil)
	require.NoError(t, err)
	data, ok := rt1.committedPayload.Load().([]byte)
	require.True(t, ok)
	activateEpoch(rt1, rt1.This().ID)

	gone := discovery.Member{Name: "127.0.0.1:1", ID: 424242}
	started := time.Now()
	rt1.retryRoutingTablePush(data, 1, rt1.Signature(), []discovery.Member{gone})
	require.Less(t, time.Since(started), 2*time.Second)

	_, admitted := pendingMembers(rt1)[gone.ID]
	require.False(t, admitted)
}

// TestRetryRoutingTablePush_GivesUpAtPushInterval guards the bound of the
// retry: a member that stays unreachable is retried with growing backoff only
// until the periodic push is due, which pushes to every member anyway.
func TestRetryRoutingTablePush_GivesUpAtPushInterval(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	c := testutil.NewConfig()
	c.RoutingTablePushInterval = 600 * time.Millisecond
	rt1, err := cluster.addNode(c)
	require.NoError(t, err)
	data, ok := rt1.committedPayload.Load().([]byte)
	require.True(t, ok)
	activateEpoch(rt1, rt1.This().ID)

	// Alive from the member set's point of view, but refusing every dial.
	unreachable := discovery.Member{Name: "127.0.0.1:1", ID: 424242}
	rt1.Members().Lock()
	rt1.Members().Add(unreachable)
	rt1.Members().Unlock()

	started := time.Now()
	rt1.retryRoutingTablePush(data, 1, rt1.Signature(), []discovery.Member{unreachable})
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "the retry must keep trying until the push interval")
	require.Less(t, elapsed, 3*time.Second, "the retry must give up once the periodic push is due")

	_, admitted := pendingMembers(rt1)[unreachable.ID]
	require.False(t, admitted)
}

// TestApplyRoutingTablePayload_SequenceAdvancesGeneration guards that a push
// numbered with a new sequence advances the installed generation even when
// the table content, and so its signature, recurred: the coordinator started
// an epoch this member has to ack. A repeated push with the same sequence, and
// a pull, which carries none, leave the generation alone.
func TestApplyRoutingTablePayload_SequenceAdvancesGeneration(t *testing.T) {
	rt := newLoneNodeWithSyncState(t, time.Minute)
	payload, ok := rt.committedPayload.Load().([]byte)
	require.True(t, ok)

	signature, generation := rt.Version()

	_, err := rt.applyRoutingTablePayload(payload, rt.This().ID, 7, false, nil)
	require.NoError(t, err)
	current, next := rt.Version()
	require.Equal(t, signature, current, "the content did not change")
	require.Equal(t, generation+1, next, "a new sequence is a new table to ack")

	_, err = rt.applyRoutingTablePayload(payload, rt.This().ID, 7, false, nil)
	require.NoError(t, err)
	_, again := rt.Version()
	require.Equal(t, next, again, "a repeated push changes nothing")

	_, err = rt.applyRoutingTablePayload(payload, rt.This().ID, 0, false, nil)
	require.NoError(t, err)
	_, pulled := rt.Version()
	require.Equal(t, next, pulled, "a pull decides by signature alone")
	require.Equal(t, uint64(7), rt.version.Load().sequence, "a confirming pull keeps the sequence of the push")

	_, err = rt.applyRoutingTablePayload(payload, rt.This().ID, 7, false, nil)
	require.NoError(t, err)
	_, confirmed := rt.Version()
	require.Equal(t, next, confirmed, "the push of the pulled table is the same table")

	_, err = rt.applyRoutingTablePayload(payload, rt.This().ID, 8, false, nil)
	require.NoError(t, err)
	_, later := rt.Version()
	require.Equal(t, next+1, later)

	// A retried push of an older sequence than the installed one, from the
	// same coordinator, is answered but not installed.
	report, err := rt.applyRoutingTablePayload(payload, rt.This().ID, 7, false, nil)
	require.NoError(t, err)
	require.NotEmpty(t, report, "the retry needs a report to count the push as delivered")
	require.Equal(t, uint64(8), rt.version.Load().sequence, "an older push never replaces a newer table")
	_, unchanged := rt.Version()
	require.Equal(t, later, unchanged)

	// A pull issued against a version that a push replaced meanwhile is
	// dropped: the pushed table is the newer one.
	stale := &tableVersion{}
	report, err = rt.applyRoutingTablePayload(payload, rt.This().ID, 0, false, stale)
	require.NoError(t, err)
	require.Nil(t, report)
	_, afterStalePull := rt.Version()
	require.Equal(t, later, afterStalePull)

	installed := rt.version.Load()
	_, err = rt.applyRoutingTablePayload(payload, rt.This().ID, 0, false, installed)
	require.NoError(t, err)
	require.NotSame(t, installed, rt.version.Load(), "a pull against the installed version is applied")
}
