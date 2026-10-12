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
	"log"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/pkg/flog"
)

func TestClusterClient_ScanMatch(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	evenKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		var key string
		if i%2 == 0 {
			key = fmt.Sprintf("even:%s", testutil.ToKey(i))
			evenKeys[key] = false
		} else {
			key = fmt.Sprintf("odd:%s", testutil.ToKey(i))
		}
		err = dm.Put(ctx, key, i)
		require.NoError(t, err)
	}
	i, err := dm.Scan(ctx, Match("^even:"))
	require.NoError(t, err)
	var count int
	defer i.Close()

	for i.Next() {
		count++
		require.Contains(t, evenKeys, i.Key())
	}
	require.Equal(t, 50, count)
}

func TestClusterClient_Scan(t *testing.T) {
	cl := newTestCluster(t)
	db := cl.addMember(t)
	cl.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i)
		require.NoError(t, err)
		allKeys[testutil.ToKey(i)] = false
	}

	i, err := dm.Scan(ctx)
	require.NoError(t, err)

	var count int
	defer i.Close()

	for i.Next() {
		count++
		require.Contains(t, allKeys, i.Key())
	}
	require.Equal(t, 100, count)
}

// newClusterWithUndetectedCrash starts a three-member cluster that keeps two
// copies of every partition, writes keyCount keys, and crashes a member that
// owns primary partitions without a leave broadcast. It returns a survivor and
// the written keys. Right after it returns, the survivor's memberlist still
// lists the crashed member as alive, so a scan still routes to it.
func newClusterWithUndetectedCrash(t *testing.T, keyCount int) (*Olric, map[string]struct{}) {
	t.Helper()

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		return c
	}

	cl := newTestCluster(t)
	db := cl.addMemberWithConfig(t, newConfig())
	crashed := cl.addMemberWithConfig(t, newConfig())
	members := []*Olric{db, crashed, cl.addMemberWithConfig(t, newConfig())}
	waitForClusterSize(t, members, 3)
	waitForPartitionOwners(t, members, 1)

	require.Eventually(t, func() bool {
		for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
			if db.primary.PartitionByID(partID).Owner().CompareByID(crashed.rt.This()) {
				return true
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond, "the crashed member must own primary partitions for the scan to reach it")

	dm, err := db.NewEmbeddedClient().NewDMap("mydmap")
	require.NoError(t, err)

	keys := make(map[string]struct{}, keyCount)
	for i := range keyCount {
		require.NoError(t, dm.Put(context.Background(), testutil.ToKey(i), i))
		keys[testutil.ToKey(i)] = struct{}{}
	}

	cl.crashMember(t, crashed)
	require.EqualValues(t, 3, db.rt.NumMembers(), "the crash must not be detected yet")
	return db, keys
}

// Regression test: a scan that reaches an owner which died before memberlist
// confirmed it must skip that owner and read its keys from the backup copies.
// It used to end the whole scan silently after the dial retries, returning
// almost no keys.
func TestClusterClient_Scan_SkipsUndetectedDeadOwner(t *testing.T) {
	db, keys := newClusterWithUndetectedCrash(t, 1000)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	it, err := dm.Scan(ctx)
	require.NoError(t, err)
	defer it.Close()

	scanned := make(map[string]struct{}, len(keys))
	for it.Next() {
		scanned[it.Key()] = struct{}{}
	}
	require.Equal(t, keys, scanned)
}

// TestClusterIterator_MarkUnreachable verifies which scan errors drop an owner
// for the rest of the scan: only errors showing the command never reached it,
// and none once the scan's context is done.
func TestClusterIterator_MarkUnreachable(t *testing.T) {
	const owner = "127.0.0.1:1"

	newIterator := func(ctx context.Context) *ClusterIterator {
		return &ClusterIterator{
			ctx:         ctx,
			logger:      flog.New(log.New(io.Discard, "", 0)),
			unreachable: make(map[string]struct{}),
		}
	}

	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	readErr := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "dial refused", err: dialErr, want: true},
		{name: "wrapped dial failure", err: fmt.Errorf("scan: %w", dialErr), want: true},
		{name: "connection pool closed", err: redis.ErrClosed, want: true},
		{name: "read failure", err: readErr, want: false},
		{name: "protocol error", err: errors.New("ERR invalid argument"), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			it := newIterator(context.Background())
			require.Equal(t, tc.want, it.markUnreachable(owner, tc.err))
			require.Equal(t, tc.want, it.isUnreachable(owner))
		})
	}

	t.Run("context done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		it := newIterator(ctx)
		require.False(t, it.markUnreachable(owner, dialErr))
		require.False(t, it.isUnreachable(owner))
	})
}

// TestClusterIterator_ConcurrentRoutingTableAccess tests that concurrent access
// to routingTable is safe when routing table updates happen in the background.
// This test validates that the routingTableMtx lock properly protects routingTable
// reads during normal iteration operations. Note: We test through the public API
// (Next()) rather than calling internal methods directly, as internal methods
// may have additional synchronization requirements.
func TestClusterIterator_ConcurrentRoutingTableAccess(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)
	cluster.addMember(t) // Add second member to enable routing table updates

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	// Populate with data
	for i := 0; i < 200; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i)
		require.NoError(t, err)
	}

	// Create iterator - this starts the background routing table updater
	iter, err := dm.Scan(ctx)
	require.NoError(t, err)
	defer iter.Close()

	// Cast to ClusterIterator to access internal methods for testing
	clusterIter, ok := iter.(*ClusterIterator)
	require.True(t, ok, "Expected ClusterIterator, got %T", iter)
	require.NotNil(t, clusterIter)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutine 1: Manually trigger routing table updates (writes routingTable)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-done:
				return
			default:
				// Force routing table refresh
				_ = clusterIter.fetchRoutingTable()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Note: We don't directly call getOwners() from a separate goroutine
	// because it's an internal method that reads config.Replica, which would
	// create a different race. Instead, we validate the routingTable protection
	// by ensuring normal iteration (which calls getOwners internally) works
	// safely alongside routing table updates.

	// Goroutine 3: Iterate normally (reads routingTable via next)
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for count < 50 {
			select {
			case <-done:
				return
			default:
				if iter.Next() {
					_ = iter.Key()
					count++
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}()

	// Run for a short duration to stress test
	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()
}

// TestClusterIterator_PartitionCountRace tests that concurrent access to
// partitionCount in next() is safe when routing table is being updated.
// This validates the fix for the partitionCount data race.
func TestClusterIterator_PartitionCountRace(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)
	cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	// Populate with data across multiple partitions
	for i := 0; i < 300; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i)
		require.NoError(t, err)
	}

	iter, err := dm.Scan(ctx)
	require.NoError(t, err)
	defer iter.Close()

	clusterIter, ok := iter.(*ClusterIterator)
	require.True(t, ok, "Expected ClusterIterator, got %T", iter)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutine 1: Continuously call next() which reads partitionCount
	// The fix ensures partitionCount read is protected
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for count < 100 {
			select {
			case <-done:
				return
			default:
				if iter.Next() {
					_ = iter.Key()
					count++
				} else {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}
	}()

	// Goroutine 2: Update routing table (writes partitionCount)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-done:
				return
			default:
				_ = clusterIter.fetchRoutingTable()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Run for a short duration
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()
}

// TestClusterIterator_RouteCopySafety tests that loadRoute() makes a proper
// copy and modifications to route don't affect the original routing table.
func TestClusterIterator_RouteCopySafety(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	// Populate with data
	for i := 0; i < 50; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), i)
		require.NoError(t, err)
	}

	iter, err := dm.Scan(ctx)
	require.NoError(t, err)
	defer iter.Close()

	clusterIter, ok := iter.(*ClusterIterator)
	require.True(t, ok, "Expected ClusterIterator, got %T", iter)

	// Load route
	clusterIter.loadRoute()
	originalRoute := clusterIter.route
	require.NotNil(t, originalRoute)

	// Store original owner count
	originalPrimaryCount := len(originalRoute.PrimaryOwners)
	require.Greater(t, originalPrimaryCount, 0)

	// Modify the route (this should only affect the copy, not the routing table)
	clusterIter.removeScannedOwner(0)

	// Reload route - should get fresh copy from routing table
	clusterIter.loadRoute()
	newRoute := clusterIter.route

	// Verify we got a fresh copy (original count should be restored)
	require.Equal(t, originalPrimaryCount, len(newRoute.PrimaryOwners),
		"Route should be a fresh copy, not affected by previous modifications")
}
