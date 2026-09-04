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

package dmap

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/testcluster"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/pkg/storage"
)

func TestDMap_Fragment(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	t.Run("loadFragment", func(t *testing.T) {
		part := s.primary.PartitionByID(1)
		_, err = dm.loadFragment(part)
		if !errors.Is(err, errFragmentNotFound) {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("newFragment", func(t *testing.T) {
		_, err := dm.newFragment()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	t.Run("loadFragment -- errFragmentNotFound", func(t *testing.T) {
		part := dm.getPartitionByHKey(123, partitions.PRIMARY)
		_, err := dm.loadFragment(part)
		if !errors.Is(err, errFragmentNotFound) {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("loadOrCreateFragment", func(t *testing.T) {
		part := dm.getPartitionByHKey(123, partitions.PRIMARY)
		_, err = dm.loadOrCreateFragment(part)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, err := dm.loadFragment(part)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

func TestDMap_Fragment_Concurrent_Access(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	part := dm.getPartitionByHKey(123, partitions.PRIMARY)

	var mtx sync.RWMutex
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			f, err := dm.loadOrCreateFragment(part)
			if err != nil {
				t.Errorf("Expected nil. Got: %v", err)
			}

			e := f.storage.NewEntry()
			e.SetKey(testutil.ToKey(idx))

			mtx.Lock()
			// storage engine is not thread-safe
			err = f.storage.Put(uint64(idx), e)
			mtx.Unlock()

			if err != nil {
				t.Errorf("Expected nil. Got: %v", err)
			}
		}(i)
	}

	wg.Wait()

	f, err := dm.loadFragment(part)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 1000; i++ {
		entry, err := f.storage.Get(uint64(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if entry.Key() != testutil.ToKey(i) {
			t.Fatalf("Expected key: %s. Got: %s", testutil.ToKey(i), entry.Key())
		}
	}
}

// TestDMap_Fragment_Replicate guards the copy path of a fragment: every live
// table is sent to the owners and merged into their partition of the target
// kind, and the local copy is kept. MoveWithTargetKind reaches only the first
// table, so a fragment spanning several tables tells the two apart.
func TestDMap_Fragment_Replicate(t *testing.T) {
	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		// Tiny tables, so a partition's fragment spans several of them.
		if c.DMaps.Engine.Config == nil {
			c.DMaps.Engine.Config = make(map[string]any)
		}
		c.DMaps.Engine.Config["tableSize"] = 4096
		// The engine was built when the config was sanitized; rebuild it with
		// the new table size.
		c.DMaps.Engine.Storage = nil
		require.NoError(t, c.DMaps.Engine.Sanitize())
		return c
	}

	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s1 := cluster.AddMember(testcluster.NewEnvironment(newConfig())).(*Service)
	s2 := cluster.AddMember(testcluster.NewEnvironment(newConfig())).(*Service)

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)
	value := make([]byte, 200)
	for i := range 3000 {
		require.NoError(t, dm1.Put(ctx, testutil.ToKey(i), value, nil))
	}

	// A partition s1 owns and s2 replicates.
	var partID uint64
	found := false
	for id := range s1.config.PartitionCount {
		part := s1.primary.PartitionByID(id)
		if !part.Owner().CompareByID(s1.rt.This()) || part.Length() == 0 {
			continue
		}

		for _, owner := range s1.backup.PartitionByID(id).Owners() {
			if owner.CompareByID(s2.rt.This()) {
				partID, found = id, true
				break
			}
		}

		if found {
			break
		}
	}
	require.True(t, found, "no partition owned by s1 and replicated by s2")

	part := s1.primary.PartitionByID(partID)
	f, err := dm1.loadFragment(part)
	require.NoError(t, err)

	tables := 0
	f.RLock()
	replicator, ok := f.storage.TransferIterator().(storage.Replicator)
	require.True(t, ok)
	for index := 0; ; {
		_, next, err := replicator.ExportFrom(index)
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		tables++
		index = next
	}
	f.RUnlock()
	require.Greater(t, tables, 1, "the fragment must span several tables")

	// Sync replication filled the replica already; empty it so the copy is
	// observable.
	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)
	replica, err := dm2.loadFragment(s2.backup.PartitionByID(partID))
	require.NoError(t, err)
	require.Equal(t, f.Stats().Length, replica.Stats().Length)

	replica.Lock()
	for i := range 3000 {
		hkey := partitions.HKey("mydmap", testutil.ToKey(i))
		if s2.backup.PartitionByHKey(hkey).ID() != partID {
			continue
		}

		if err := replica.storage.Delete(hkey); err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			replica.Unlock()
			require.NoError(t, err)
		}
	}
	replica.Unlock()
	require.Zero(t, replica.Stats().Length)

	length := f.Stats().Length
	require.NoError(t, f.Replicate(part, "mydmap", []discovery.Member{s2.rt.This()}, partitions.BACKUP))

	require.Equal(t, length, f.Stats().Length, "the local copy must be kept")
	require.Equal(t, length, replica.Stats().Length, "every table must have been copied")
}

// TestDMap_Fragment_MoveToDepartedOwnerFailsWithoutDialing guards that a
// transfer to an owner memberlist has removed fails at once, without a dial,
// and keeps the fragment, so the balancer retries it against the next table.
func TestDMap_Fragment_MoveToDepartedOwnerFailsWithoutDialing(t *testing.T) {
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s := cluster.AddMember(nil).(*Service)

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)
	key := ownedKey(t, s, "mydmap")
	require.NoError(t, dm.Put(context.Background(), key, []byte("value"), nil))

	part := s.primary.PartitionByHKey(partitions.HKey("mydmap", key))
	f, err := dm.loadFragment(part)
	require.NoError(t, err)
	length := f.Stats().Length

	gone := discovery.Member{Name: silentListener(t), ID: 424242}
	started := time.Now()
	err = f.Move(part, "mydmap", []discovery.Member{gone})
	require.ErrorIs(t, err, errOwnerDeparted)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.Equal(t, length, f.Stats().Length, "a failed transfer keeps the fragment")
}
