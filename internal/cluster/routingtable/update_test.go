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
