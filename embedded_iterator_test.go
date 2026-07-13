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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/testutil"
)

func TestEmbeddedClient_ScanMatch(t *testing.T) {
	cl := newTestCluster(t)
	db := cl.addMember(t)
	cl.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()

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

func TestEmbeddedClient_Scan(t *testing.T) {
	cl := newTestCluster(t)
	db := cl.addMember(t)
	cl.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
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

// Regression test for https://github.com/Tochemey/olric/issues/22. When the
// iterator's routing-table snapshot still names a hard-dead owner (a member
// removed from memberlist after a crash), the embedded scan path must skip it
// instead of dialing the corpse: the dead address is filtered against live
// membership, the scan converges over the surviving owners, and no doomed dial
// is ever made.
func TestEmbeddedClient_Scan_SkipsDeadOwner(t *testing.T) {
	cl := newTestCluster(t)
	db := cl.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), i))
		allKeys[testutil.ToKey(i)] = false
	}

	it, err := dm.Scan(ctx)
	require.NoError(t, err)
	defer it.Close()

	// Splice an owner that memberlist has never seen into every partition's
	// primary owner list. It points at a refused port: if the scan path dialed
	// it, the scan would error out and yield fewer than 100 keys. Because the
	// address is absent from live membership, the embedded scan path skips it
	// and still returns everything the surviving (local) owner holds.
	const deadOwner = "127.0.0.1:1"
	ci := it.(*EmbeddedIterator).clusterIterator
	ci.routingTableMtx.Lock()
	for pid, route := range ci.routingTable {
		route.PrimaryOwners = append(route.PrimaryOwners, deadOwner)
		ci.routingTable[pid] = route
	}
	ci.routingTableMtx.Unlock()

	var count int
	for it.Next() {
		count++
		require.Contains(t, allKeys, it.Key())
	}
	require.Equal(t, 100, count)
}

// Regression test: Close must never panic, even when the scan context has
// already been canceled by the caller before the iterator is closed. Failing
// to tear down the internal cluster client's connection pools is not a fatal
// condition.
func TestEmbeddedClient_Scan_CloseWithCanceledContext(t *testing.T) {
	cl := newTestCluster(t)
	db := cl.addMember(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	require.NoError(t, dm.Put(context.Background(), "key", "value"))

	ctx, cancel := context.WithCancel(context.Background())
	it, err := dm.Scan(ctx)
	require.NoError(t, err)

	// The caller cancels the scan context first (e.g. a supervisor shutting
	// down), then closes the iterator. Close must not panic.
	cancel()
	require.NotPanics(t, func() {
		it.Close()
	})
}
