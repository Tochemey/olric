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
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/cluster/partitions"
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
	require.Nil(t, rt.placeBackupOwners(0, nil, ownerCounts{}))
}

func TestRoutingTable_distribute_UnreachableAndFullMembers(t *testing.T) {
	// Two nodes with ReplicaCount=2. The test first fills the backup
	// partitions on both nodes so placeBackupOwners keeps the existing
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

	rt1.Lock()
	rt1.fillRoutingTable()
	rt1.Unlock()
	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		require.NotEmpty(t, rt1.table[partID].Backups)
	}

	// Stop the TCP server of the second node. It remains a live cluster
	// member, but the LengthOfPart requests against it fail now.
	require.NoError(t, srv2.Shutdown(context.Background()))

	rt1.Lock()
	rt1.fillRoutingTable()
	rt1.Unlock()
	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		require.NotEmpty(t, rt1.table[partID].Owners)
		// An owner that does not answer keeps the copies it may hold: with
		// two members and two replicas the second node stays an owner,
		// primary or backup, of every partition.
		names := append(memberNamesOf(rt1.table[partID].Owners), memberNamesOf(rt1.table[partID].Backups)...)
		require.Contains(t, names, rt2.This().Name, "PartID %d", partID)
	}
}

// memberNamesOf returns the names of members, for assertions.
func memberNamesOf(members []discovery.Member) []string {
	names := make([]string, 0, len(members))
	for _, m := range members {
		names = append(names, m.Name)
	}

	return names
}

// BenchmarkFillRoutingTable measures the coordinator's table computation on a
// three-member cluster whose partitions all hold data, so every owner is
// probed for its key count, at the default and at a large partition count.
func BenchmarkFillRoutingTable(b *testing.B) {
	for _, partitionCount := range []uint64{271, 4096} {
		b.Run(fmt.Sprintf("partitions=%d", partitionCount), func(b *testing.B) {
			cluster := newTestCluster()
			defer func() { _ = cluster.shutdown() }()

			newConfig := func() *config.Config {
				c := testutil.NewConfig()
				c.PartitionCount = partitionCount
				c.ReplicaCount = 2
				c.Logger = log.New(io.Discard, "", 0)
				return c
			}

			var members []*RoutingTable
			for range 3 {
				rt, err := cluster.addNode(newConfig())
				require.NoError(b, err)
				members = append(members, rt)
			}

			for _, rt := range members {
				require.NoError(b, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
					if !rt.IsBootstrapped() {
						return errors.New("member is not bootstrapped")
					}

					return nil
				}))
			}

			for _, rt := range members {
				for partID := uint64(0); partID < partitionCount; partID++ {
					for _, parts := range []*partitions.Partitions{rt.primary, rt.backup} {
						frag := mockfragment.New()
						frag.Fill()
						parts.PartitionByID(partID).Map().Store("dmap.bench", frag)
					}
				}
			}

			coordinator := members[0]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				coordinator.Lock()
				coordinator.fillRoutingTable()
				coordinator.Unlock()
			}
		})
	}
}

// TestPlaceOwners_PruneRules guards the rules the owner lists are built by,
// given the probed counts: a recorded owner with an empty copy is dropped, one
// with data or one that did not answer is kept, the ring's designated primary
// owner ends the primary list, and the designated backup owners follow the
// kept replicas, moved to the tail when already among them.
func TestPlaceOwners_PruneRules(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	self := discovery.Member{Name: "127.0.0.1:1", ID: 1}
	other := discovery.Member{Name: "127.0.0.1:2", ID: 2}
	rt.this = self
	rt.consistent.Add(self)
	rt.consistent.Add(other)

	partID := uint64(0)
	designated := rt.consistent.GetPartitionOwner(int(partID)).(discovery.Member)
	previous := other
	if designated.CompareByID(other) {
		previous = self
	}

	counts := ownerCounts{
		previous: {
			{partID: partID}:                0,
			{partID: partID, replica: true}: 3,
		},
	}

	// An empty previous owner is dropped; the designated owner ends the list.
	require.Equal(t, []discovery.Member{designated}, rt.placePrimaryOwner(partID, []discovery.Member{previous, designated}, counts))

	// A previous owner with data stays ahead of the designated owner.
	counts[previous][countQuery{partID: partID}] = 5
	require.Equal(t, []discovery.Member{previous, designated}, rt.placePrimaryOwner(partID, []discovery.Member{designated, previous}, counts))

	// An owner that did not answer is kept as well.
	require.Equal(t, []discovery.Member{previous, designated}, rt.placePrimaryOwner(partID, []discovery.Member{previous}, ownerCounts{}))

	// Backups: the ring designates the non-primary member; a recorded replica
	// with data that is also designated is moved to the tail, an empty one is
	// dropped.
	replicaOwner := other
	if designated.CompareByID(other) {
		replicaOwner = self
	}
	require.Equal(t, []discovery.Member{replicaOwner}, rt.placeBackupOwners(partID, []discovery.Member{replicaOwner}, counts))

	counts[replicaOwner] = map[countQuery]int64{{partID: partID, replica: true}: 0}
	require.Equal(t, []discovery.Member{replicaOwner}, rt.placeBackupOwners(partID, []discovery.Member{replicaOwner}, counts))

	// A replica held by the primary owner itself is kept when it holds data,
	// so a healthy copy is never dropped.
	counts[designated] = map[countQuery]int64{{partID: partID, replica: true}: 2}
	require.Equal(t, []discovery.Member{designated, replicaOwner}, rt.placeBackupOwners(partID, []discovery.Member{designated}, counts))
}
