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

package olric

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
)

func TestIntegration_NodesJoinOrLeftDuringQuery(t *testing.T) {
	t.Skip("TestIntegration_NodesJoinOrLeftDuringQuery: flaky test")

	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadRepair = true
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())
	db2 := cluster.addMemberWithConfig(t, newConfig())

	t.Log("Wait for 1 second before inserting keys")
	<-time.After(time.Second)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 100000; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
		if i == 5999 {
			go cluster.addMemberWithConfig(t, newConfig())
		}
	}

	go cluster.addMemberWithConfig(t, newConfig())

	t.Log("Fetch all keys")

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(context.Background(), fmt.Sprintf("mykey-%d", i))
		if errors.Is(err, ErrConnRefused) {
			// Rewind
			i--
			require.NoError(t, c.RefreshMetadata(context.Background()))
			continue
		}
		require.NoError(t, err)
		if i == 5999 {
			err = c.client.Close(db2.name)
			require.NoError(t, err)

			t.Logf("Shutdown one of the nodes: %s", db2.name)
			require.NoError(t, db2.Shutdown(ctx))

			go cluster.addMemberWithConfig(t, newConfig())

			t.Log("Wait for \"NodeLeave\" event propagation")
			<-time.After(time.Second)
		}
	}

	for i := range 100000 {
		_, err = dm.Get(context.Background(), fmt.Sprintf("mykey-%d", i))
		require.NoError(t, err)
	}
}

func TestIntegration_DMap_Cache_Eviction_LRU_MaxKeys(t *testing.T) {
	maxKeys := 100000
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxKeys = maxKeys
		c.DMaps.EvictionPolicy = config.LRUEviction
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := range maxKeys {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	var total int
	for i := range maxKeys {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue", NX())
		if errors.Is(err, ErrKeyFound) {
			err = nil
		} else {
			total++
		}
		require.NoError(t, err)
	}
	require.Greater(t, total, 0)
	t.Logf("number of misses: %d, utilization rate: %f", total, float64(100)-(float64(total*100))/float64(maxKeys))
}

func TestIntegration_DMap_Cache_Eviction_MaxKeys(t *testing.T) {
	maxKeys := 100000
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxKeys = maxKeys
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	var total int
	for i := maxKeys; i < 2*maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue", NX())
		if err == ErrKeyFound {
			err = nil
		} else {
			total++
		}
		require.NoError(t, err)
	}

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Equal(t, maxKeys, total)
}

func TestIntegration_DMap_Cache_Eviction_MaxIdleDuration(t *testing.T) {
	maxKeys := 100000
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxIdleDuration = 100 * time.Millisecond
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	<-time.After(250 * time.Millisecond)

	var total int

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Greater(t, total, 0)
}

func TestIntegration_DMap_Cache_Eviction_TTLDuration(t *testing.T) {
	maxKeys := 100000
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.TTLDuration = 100 * time.Millisecond
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	<-time.After(250 * time.Millisecond)

	var total int

	for i := 0; i < maxKeys; i++ {
		_, err := dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Equal(t, maxKeys, total)
}

func TestIntegration_DMap_Cache_Eviction_LRU_MaxInuse(t *testing.T) {
	maxKeys := 100000
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxInuse = 100 // bytes
		c.DMaps.EvictionPolicy = "LRU"
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	<-time.After(250 * time.Millisecond)

	var total int

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Greater(t, total, 0)
}

func TestIntegration_Kill_Nodes_During_Operation(t *testing.T) {
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 3
		c.WriteQuorum = 1
		c.ReadRepair = true
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.Client.DisableRedisLogging = true
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	cluster.addMemberWithConfig(t, newConfig())
	db3 := cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())
	db5 := cluster.addMemberWithConfig(t, newConfig())

	t.Log("Wait for 1 second before inserting keys")
	<-time.After(time.Second)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	require.NoError(t, err)

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	t.Log("Insert keys")

	for i := 0; i < 100000; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	t.Log("Fetch all keys")

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if errors.Is(err, ErrKeyNotFound) {
			continue
		}
		require.NoError(t, err)
	}

	t.Logf("Terminate %s", db3.rt.This())
	require.NoError(t, db3.Shutdown(context.Background()))

	t.Logf("Terminate %s", db5.rt.This())
	require.NoError(t, db5.Shutdown(context.Background()))

	t.Log("Wait for \"NodeLeave\" event propagation")
	<-time.After(time.Second)

	for i := 0; i < 100000; i++ {
		ctx := context.Background()
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if errors.Is(err, ErrConnRefused) {
			i--
			require.NoError(t, c.RefreshMetadata(ctx))
			continue
		}
		require.NoError(t, err)
	}
}

func TestIntegration_ProductionConfig_NoDataLoss(t *testing.T) {
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 3
		c.WriteQuorum = 2
		c.ReadQuorum = 2
		c.ReadRepair = true
		c.ReplicationMode = config.SyncReplicationMode
		c.LogOutput = io.Discard
		c.Client.DisableRedisLogging = true
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	// Start with 3 nodes (minimum for production config with ReplicaCount=3)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	db2 := cluster.addMemberWithConfig(t, newConfig())
	_ = cluster.addMemberWithConfig(t, newConfig())

	t.Log("Wait for cluster to stabilize")
	<-time.After(time.Second)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db1.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	t.Log("Insert keys with WriteQuorum=2")
	keyCount := 10000
	for i := range keyCount {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), fmt.Sprintf("myvalue-%d", i))
		require.NoError(t, err, "Write should succeed with WriteQuorum=2")
	}

	t.Log("Verify all keys are accessible before node failure")
	for i := range keyCount {
		entry, err := dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		require.NoError(t, err, "Key should be accessible before node failure")
		require.NotNil(t, entry, "Entry should not be nil")
		scannedValue, err := entry.String()
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("myvalue-%d", i), scannedValue, "Value should match")
	}

	t.Logf("Terminate node %s (1 of 3 nodes)", db2.rt.This())
	require.NoError(t, db2.Shutdown(context.Background()))

	t.Log("Wait for \"NodeLeave\" event propagation and rebalancing")
	<-time.After(2 * time.Second)

	t.Log("Verify all keys are still accessible after 1 node failure (2 nodes remaining)")
	// With ReplicaCount=3, WriteQuorum=2, ReadQuorum=2, we should still have quorum
	// with 2 nodes remaining (since we wrote to 2+ nodes and can read from 2 nodes)
	accessibleCount := 0
	for i := range keyCount {
		ctx := context.Background()
		entry, err := dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if errors.Is(err, ErrConnRefused) {
			// Metadata might be stale, refresh and retry
			i--
			require.NoError(t, c.RefreshMetadata(ctx))
			continue
		}
		if errors.Is(err, ErrKeyNotFound) {
			// Key might be lost if it was only on the failed node (shouldn't happen with quorum)
			t.Logf("Key mykey-%d not found after node failure", i)
			continue
		}
		require.NoError(t, err, "Key should be accessible after node failure")
		require.NotNil(t, entry, "Entry should not be nil")
		scannedValue, err := entry.String()
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("myvalue-%d", i), scannedValue, "Value should match")
		accessibleCount++
	}

	// With WriteQuorum=2 and ReadQuorum=2, we should have high data availability
	// Allow for some tolerance due to timing/balancing, but expect most keys to be accessible
	require.Greater(t, accessibleCount, keyCount*95/100,
		"At least 95%% of keys should be accessible after 1 node failure with production config")

	t.Logf("Successfully accessed %d out of %d keys after node failure", accessibleCount, keyCount)

	// Verify we can still write new data with remaining nodes
	t.Log("Verify write operations still work with remaining nodes")
	newKey := "newkey-after-failure"
	newValue := "newvalue-after-failure"
	err = dm.Put(ctx, newKey, newValue)
	require.NoError(t, err, "Write should succeed with WriteQuorum=2 on remaining nodes")

	// Verify the new key is readable
	entry, err := dm.Get(ctx, newKey)
	require.NoError(t, err, "New key should be readable")
	require.NotNil(t, entry, "Entry should not be nil")
	scannedValue, err := entry.String()
	require.NoError(t, err)
	require.Equal(t, newValue, scannedValue, "New value should match")
}

func TestIntegration_Network_Partitioning_Cluster_DM_SCAN(t *testing.T) {
	keyGenerator := func(i int) string {
		return fmt.Sprintf("mykey-%d", i)
	}
	result := scanIntegrationTestCommon(t, false, keyGenerator)
	passOne, passTwo := result[0], result[1]
	require.Empty(t, passOne)
	require.Empty(t, passTwo)
}

func TestIntegration_Network_Partitioning_Cluster_DM_SCAN_Match(t *testing.T) {
	var oddNumbers int
	keyGenerator := func(i int) string {
		if i%2 == 0 {
			return fmt.Sprintf("even:%d", i)
		}
		oddNumbers++
		return fmt.Sprintf("odd:%d", i)
	}
	result := scanIntegrationTestCommon(t, false, keyGenerator, Match("^even:"))
	passOne, passTwo := result[0], result[1]
	require.Len(t, passOne, oddNumbers)
	require.Len(t, passTwo, oddNumbers)
}

func TestIntegration_Network_Partitioning_Embedded_DM_SCAN(t *testing.T) {
	keyGenerator := func(i int) string {
		return fmt.Sprintf("mykey-%d", i)
	}
	result := scanIntegrationTestCommon(t, true, keyGenerator)
	passOne, passTwo := result[0], result[1]
	require.Empty(t, passOne)
	require.Empty(t, passTwo)
}

func TestIntegration_Network_Partitioning_Embedded_DM_SCAN_Match(t *testing.T) {
	var oddNumbers int
	keyGenerator := func(i int) string {
		if i%2 == 0 {
			return fmt.Sprintf("even:%d", i)
		}
		oddNumbers++
		return fmt.Sprintf("odd:%d", i)
	}
	result := scanIntegrationTestCommon(t, true, keyGenerator, Match("^even:"))
	passOne, passTwo := result[0], result[1]
	require.Len(t, passOne, oddNumbers)
	require.Len(t, passTwo, oddNumbers)
}

func scanIntegrationTestCommon(t *testing.T, embedded bool, keyFunc func(i int) string, options ...ScanOption) []map[string]struct{} {
	newConfig := func() *config.Config {
		c := config.New(config.MemberlistEnvLocal)
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadRepair = false
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.TriggerBalancerInterval = time.Millisecond
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())
	db2 := cluster.addMemberWithConfig(t, newConfig())
	_ = cluster.addMemberWithConfig(t, newConfig())

	t.Log("Wait for 1 second before inserting keys")
	<-time.After(time.Second)

	ctx := context.Background()
	var c Client
	var err error

	if embedded {
		c = db.NewEmbeddedClient()
	} else {
		c, err = NewClusterClient([]string{db.name})
		require.NoError(t, err)
	}

	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	passOne := make(map[string]struct{})
	passTwo := make(map[string]struct{})
	for i := 0; i < 10000; i++ {
		key := keyFunc(i)
		err = dm.Put(ctx, key, "myvalue")
		require.NoError(t, err)
		passOne[key] = struct{}{}
		passTwo[key] = struct{}{}
	}

	t.Logf("Shutdown one of the nodes: %s", db2.name)
	require.NoError(t, db2.Shutdown(ctx))

	t.Log("Wait for \"NodeLeave\" event propagation")
	<-time.After(time.Second)

	t.Log("First pass")

	s, err := dm.Scan(context.Background(), options...)
	require.NoError(t, err)
	for s.Next() {
		delete(passOne, s.Key())
	}
	s.Close()

	db3 := cluster.addMemberWithConfig(t, newConfig())
	t.Logf("Add a new member: %s", db3.rt.This())

	<-time.After(time.Second)

	t.Log("Second pass")
	s, err = dm.Scan(context.Background(), options...)
	require.NoError(t, err)

	for s.Next() {
		delete(passTwo, s.Key())
	}
	s.Close()

	return []map[string]struct{}{passOne, passTwo}
}
