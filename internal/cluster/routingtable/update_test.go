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
	"testing"

	"github.com/stretchr/testify/require"

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
