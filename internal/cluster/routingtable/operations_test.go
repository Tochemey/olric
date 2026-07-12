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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/internal/testutil"
)

func TestRoutingTable_lengthOfPartCommandHandler(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	ctx := context.Background()
	rc := rt.client.Get(rt.This().String())

	// Primary partition length.
	cmd := protocol.NewLengthOfPart(1).Command(ctx)
	require.NoError(t, rc.Process(ctx, cmd))
	length, err := cmd.Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), length)

	// Replica partition length.
	cmd = protocol.NewLengthOfPart(1).SetReplica().Command(ctx)
	require.NoError(t, rc.Process(ctx, cmd))
	length, err = cmd.Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), length)

	// Malformed command: the partition ID is missing.
	require.Error(t, rc.Do(ctx, protocol.Internal.LengthOfPart).Err())
}

func TestRoutingTable_updateRoutingCommandHandler_Errors(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	ctx := context.Background()
	rc := rt.client.Get(rt.This().String())

	// Malformed command: payload and coordinator ID are missing.
	require.Error(t, rc.Do(ctx, protocol.Internal.UpdateRouting).Err())

	// The payload cannot be decoded as a routing table.
	cmd := protocol.NewUpdateRouting([]byte("junk-payload"), rt.This().ID).Command(ctx)
	require.Error(t, rc.Process(ctx, cmd))

	// The coordinator ID doesn't belong to a known cluster member.
	table := make(map[uint64]*route)
	for partID := uint64(0); partID < rt.config.PartitionCount; partID++ {
		table[partID] = &route{}
	}
	data, err := msgpack.Marshal(table)
	require.NoError(t, err)
	cmd = protocol.NewUpdateRouting(data, 987654321).Command(ctx)
	require.Error(t, rc.Process(ctx, cmd))

	// The pushed table has a bogus partition count.
	small, err := msgpack.Marshal(map[uint64]*route{0: {}})
	require.NoError(t, err)
	cmd = protocol.NewUpdateRouting(small, rt.This().ID).Command(ctx)
	require.Error(t, rc.Process(ctx, cmd))
}

func TestRoutingTable_verifyRoutingTable(t *testing.T) {
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

	table := make(map[uint64]*route)
	for partID := uint64(0); partID < rt1.config.PartitionCount; partID++ {
		table[partID] = &route{}
	}

	// Unknown member ID.
	require.Error(t, rt1.verifyRoutingTable(987654321, table))

	// rt2 is a known member but not the cluster coordinator.
	require.Error(t, rt1.verifyRoutingTable(rt2.This().ID, table))

	// Partition count mismatch.
	require.Error(t, rt1.verifyRoutingTable(rt1.This().ID, map[uint64]*route{0: {}}))

	// A well-formed routing table pushed by the coordinator.
	require.NoError(t, rt1.verifyRoutingTable(rt1.This().ID, table))
}

func TestRoutingTable_rebalanceAckCommandHandler_Errors(t *testing.T) {
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

	ctx := context.Background()

	// Malformed command: the member ID is missing.
	rc1 := rt1.client.Get(rt1.This().String())
	require.Error(t, rc1.Do(ctx, protocol.Internal.RebalanceAck, "42").Err())

	// rt2 is not the cluster coordinator: it must refuse the ack.
	rc2 := rt2.client.Get(rt2.This().String())
	cmd := protocol.NewRebalanceAck(42, rt2.This().ID).Command(ctx)
	require.Error(t, rc2.Process(ctx, cmd))

	// An ack for a superseded epoch is not an error: the coordinator reports
	// it as stale so the sender re-runs its balance cycle.
	cmd = protocol.NewRebalanceAck(999999999, rt1.This().ID).Command(ctx)
	require.NoError(t, rc1.Process(ctx, cmd))
	status, err := cmd.Result()
	require.NoError(t, err)
	require.Equal(t, protocol.StatusStaleRebalanceAck, status)
}

func TestRoutingTable_fetchRoutingCommandHandler(t *testing.T) {
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

	ctx := context.Background()

	// Malformed command: the member ID is missing.
	rc1 := rt1.client.Get(rt1.This().String())
	require.Error(t, rc1.Do(ctx, protocol.Internal.FetchRouting).Err())

	// rt2 is not the cluster coordinator: it must refuse the pull.
	rc2 := rt2.client.Get(rt2.This().String())
	cmd := protocol.NewFetchRouting(rt1.This().ID).Command(ctx)
	require.Error(t, rc2.Process(ctx, cmd))

	// The coordinator serves the committed routing table payload.
	cmd = protocol.NewFetchRouting(rt2.This().ID).Command(ctx)
	require.NoError(t, rc1.Process(ctx, cmd))
	payload, err := cmd.Bytes()
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, rt1.committedPayload.Load().([]byte), payload)
}

func newRoutingTableWithSyncState(c *config.Config, srv *server.Server) *RoutingTable {
	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", server.NewClient(c.Client))
	e.Set("server", srv)
	e.Set("syncstate", syncstate.New())

	rt := New(e)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-srv.StartedCtx.Done()

	return rt
}

func TestRoutingTable_updateRouting_WithSyncState(t *testing.T) {
	c := testutil.NewConfig()
	c.InitialSyncEmptyPartitionTimeout = time.Second
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableWithSyncState(c, srv)
	require.NotNil(t, rt.syncState)

	require.NoError(t, rt.Join())
	require.NoError(t, rt.Start())
	defer func() {
		require.NoError(t, rt.Shutdown(context.Background()))
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	require.True(t, rt.IsBootstrapped())

	// Force a routing table update on the coordinator: the sync state is
	// reset with the partitions that are pending receive.
	rt.UpdateEagerly()
	require.True(t, rt.IsBootstrapped())
}
