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
	"errors"
	"testing"
	"time"

	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/internal/testutil/mockfragment"
)

func TestProcessLeftOverDataReports_EnsuresOwnership(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)
	require.True(t, rt.IsBootstrapped())

	leftOver := discovery.Member{Name: "127.0.0.1:9999", ID: 987654, Birthdate: 1}
	reports := map[discovery.Member]*leftOverDataReport{
		leftOver: {Partitions: []uint64{0}, Backups: []uint64{1}},
	}
	rt.processLeftOverDataReports(reports)

	// The member with left-over data is prepended to the owners list while
	// the current owner keeps the partition.
	primary := rt.primary.PartitionByID(0)
	require.Len(t, primary.Owners(), 2)
	require.Equal(t, leftOver.ID, primary.Owners()[0].ID)
	require.Equal(t, rt.This().ID, primary.Owner().ID)

	backup := rt.backup.PartitionByID(1)
	require.Len(t, backup.Owners(), 1)
	require.Equal(t, leftOver.ID, backup.Owners()[0].ID)

	// Processing the same report again must not duplicate the ownership.
	rt.processLeftOverDataReports(reports)
	require.Len(t, rt.primary.PartitionByID(0).Owners(), 2)
	require.Len(t, rt.backup.PartitionByID(1).Owners(), 1)
}

func TestRoutingTable_LeftOverData(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt1, err := cluster.addNode(c1)
		require.NoError(t, err)

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		for partID := uint64(0); partID < c1.PartitionCount; partID++ {
			part := rt1.primary.PartitionByID(partID)
			ts := mockfragment.New()
			ts.Fill()
			part.Map().Store("test-data", ts)
		}

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt2, err := cluster.addNode(c2)
		require.NoError(t, err)

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		for partID := uint64(0); partID < c2.PartitionCount; partID++ {
			part := rt2.primary.PartitionByID(partID)
			ts := mockfragment.New()
			ts.Fill()
			part.Map().Store("test-data", ts)
		}

		rt1.UpdateEagerly()

		for partID := uint64(0); partID < c1.PartitionCount; partID++ {
			part := rt1.primary.PartitionByID(partID)
			if len(part.Owners()) != 2 {
				t.Fatalf("Expected partition owners count: 2. Got: %d, PartID: %d", part.OwnerCount(), partID)
			}
		}
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfig()
		rt1, err := cluster.addNode(c1)
		require.NoError(t, err)

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		for partID := uint64(0); partID < c1.PartitionCount; partID++ {
			part := rt1.primary.PartitionByID(partID)
			ts := mockfragment.New()
			ts.Fill()
			part.Map().Store("test-data", ts)
		}

		c2 := testutil.NewConfig()
		rt2, err := cluster.addNode(c2)
		require.NoError(t, err)

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		for partID := uint64(0); partID < c2.PartitionCount; partID++ {
			part := rt2.primary.PartitionByID(partID)
			ts := mockfragment.New()
			ts.Fill()
			part.Map().Store("test-data", ts)
		}

		rt1.UpdateEagerly()

		for partID := uint64(0); partID < c1.PartitionCount; partID++ {
			part := rt1.primary.PartitionByID(partID)
			if len(part.Owners()) != 2 {
				t.Fatalf("Expected partition owners count: 2. Got: %d, PartID: %d", part.OwnerCount(), partID)
			}
		}
	})
}
