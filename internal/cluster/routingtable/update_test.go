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
	rt.syncState.Reconcile(pending, time.Nanosecond)
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
	_, err = rt.updateRoutingTableOnMember(data, fakeMember)
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
	require.NoError(t, rt1.fetchRoutingTableFromCoordinator())

	// Simulate a joiner whose routing table push was lost: it is joined but
	// not bootstrapped. The pull must install the coordinator's committed
	// table and bootstrap the node.
	atomic.StoreInt32(&rt2.bootstrapped, 0)
	require.NoError(t, rt2.fetchRoutingTableFromCoordinator())
	require.True(t, rt2.IsBootstrapped())
	require.Equal(t, rt1.Signature(), rt2.Signature())

	// A pull on an already bootstrapped node is a no-op.
	require.NoError(t, rt2.fetchRoutingTableFromCoordinator())
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
	_, err = rt.updateRoutingTableOnCluster(data)
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
