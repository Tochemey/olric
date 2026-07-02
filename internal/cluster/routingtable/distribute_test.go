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
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/consistent"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/internal/testutil/mockfragment"
)

func TestRoutingTable_distributedBackups(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	rt1, err := cluster.addNode(c1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt1.IsBootstrapped() {
		t.Fatalf("The coordinator node cannot be bootstrapped")
	}

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	rt2, err := cluster.addNode(c2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = rt1.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	c3 := testutil.NewConfig()
	c3.ReplicaCount = 2
	rt3, err := cluster.addNode(c3)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	rt2.UpdateEagerly()

	for partID := uint64(0); partID < c3.PartitionCount; partID++ {
		part := rt3.backup.PartitionByID(partID)
		if part.OwnerCount() != 1 {
			t.Fatalf("Expected backup owners count: 1. Got: %d", part.OwnerCount())
		}

		for _, owner := range part.Owners() {
			if owner.CompareByID(rt1.This()) {
				t.Fatalf("Dead node still a replica owner: %v", rt1.This())
			}
		}
	}

	err = cluster.shutdown()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestIsOwner(t *testing.T) {
	owners := []consistent.Member{
		discovery.Member{Name: "127.0.0.1:3320"},
		discovery.Member{Name: "127.0.0.1:3321"},
	}

	require.True(t, isOwner(discovery.Member{Name: "127.0.0.1:3320"}, owners))
	require.True(t, isOwner(discovery.Member{Name: "127.0.0.1:3321"}, owners))
	require.False(t, isOwner(discovery.Member{Name: "127.0.0.1:9999"}, owners))
	require.False(t, isOwner(discovery.Member{Name: "127.0.0.1:3320"}, nil))
}

func TestGetReplicaOwners_InsufficientMemberCount(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	// The consistent hash ring is empty: there is no member to own a replica.
	owners, err := rt.getReplicaOwners(0)
	require.Nil(t, owners)
	require.ErrorIs(t, err, consistent.ErrInsufficientMemberCount)
}

func TestDistributeBackups_NoReplicaOwners(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	// getReplicaOwners fails on an empty hash ring: no backups are assigned.
	require.Nil(t, rt.distributeBackups(0))
}

func TestRoutingTable_distribute_UnreachableAndFullMembers(t *testing.T) {
	// Two nodes with ReplicaCount=2. The test first fills the backup
	// partitions on both nodes so distributeBackups keeps the existing
	// (non-empty) owners, then stops the TCP server of the second node so
	// the LengthOfPart requests fail while the member is still alive from
	// the memberlist point of view.
	newCfg := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		port, err := testutil.GetFreePort()
		if err != nil {
			panic(err)
		}
		c.MemberlistConfig.BindPort = port
		return c
	}

	c1 := newCfg()
	srv1 := testutil.NewServer(c1)
	rt1 := newRoutingTableForTest(c1, srv1)
	require.NoError(t, rt1.Join())
	require.NoError(t, rt1.Start())

	c2 := newCfg()
	c2.Peers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(c1.MemberlistConfig.BindPort))}
	srv2 := testutil.NewServer(c2)
	rt2 := newRoutingTableForTest(c2, srv2)
	require.NoError(t, rt2.Join())
	require.NoError(t, rt2.Start())

	defer func() {
		_ = rt2.Shutdown(context.Background())
		_ = rt1.Shutdown(context.Background())
		_ = srv2.Shutdown(context.Background())
		_ = srv1.Shutdown(context.Background())
	}()

	err := testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	// Fill the backup partitions on both nodes: the replicas hold data, so
	// the coordinator keeps the existing owners and moves the designated
	// owner to the tail of the list.
	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		for _, rt := range []*RoutingTable{rt1, rt2} {
			frag := mockfragment.New()
			frag.Fill()
			rt.backup.PartitionByID(partID).Map().Store("dmap.test", frag)
		}
	}

	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		require.NotEmpty(t, rt1.distributeBackups(partID))
	}

	// Stop the TCP server of the second node. It remains a live cluster
	// member, but the LengthOfPart requests against it fail now.
	require.NoError(t, srv2.Shutdown(context.Background()))

	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		require.NotEmpty(t, rt1.distributePrimaryCopies(partID))
		rt1.distributeBackups(partID)
	}
}
