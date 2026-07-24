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
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/kvstore"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/testcluster"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/pkg/storage"
)

func checkEmptyStorageEngine(t *testing.T, s *Service) {
	maximum := 50
	check := func(current int) (bool, error) {
		for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
			part := s.primary.PartitionByID(partID)
			tmp, ok := part.Map().Load("dmap.mymap")
			if !ok {
				continue
			}

			f := tmp.(*fragment)
			f.RLock()
			numTables := f.storage.Stats().NumTables
			f.RUnlock()

			if numTables != 1 && current < maximum-1 {
				return false, nil
			}
			if numTables != 1 && current >= maximum-1 {
				return false, fmt.Errorf("numTables=%d PartID: %d", numTables, partID)
			}
		}
		return true, nil
	}

	for i := 0; i < maximum; i++ {
		done, err := check(i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if done {
			return
		}
		<-time.After(100 * time.Millisecond)
	}
	t.Fatalf("Failed to control compaction status")
}

func TestDMap_Delete_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	dm2, err := s2.NewDMap("mymap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, _, err = dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Delete_MultiKeyDifferentRemoteOwners(t *testing.T) {
	// Ported from the same fix on olric-data/olric (upstream PR
	// https://github.com/olric-data/olric/pull/287): deleteKeys groups keys
	// by partition owner and previously returned unconditionally after
	// handling the first remote owner, silently skipping every remote owner
	// after that. This reproduces the bug on a 3-node cluster by asserting
	// the key set actually hashes to at least two different remote owners
	// before calling Delete, so the multi-owner fan-out path is exercised.
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	const keyCount = 30
	for i := 0; i < keyCount; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	// Sanity check: the keys must hash to at least two different owners other
	// than s1 itself. Otherwise, deleteKeys' per-owner fan-out only has a
	// single remote entry and this test wouldn't exercise the multi-owner
	// code path where a bug could silently drop keys past the first remote
	// owner.
	remoteOwners := make(map[string]struct{})
	for i := 0; i < keyCount; i++ {
		hkey := partitions.HKey("mymap", testutil.ToKey(i))
		owner := s1.primary.PartitionByHKey(hkey).Owner()
		if !owner.CompareByName(s1.rt.This()) {
			remoteOwners[owner.String()] = struct{}{}
		}
	}
	require.GreaterOrEqual(t, len(remoteOwners), 2,
		"test setup must distribute keys across at least two remote owners")

	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = testutil.ToKey(i)
	}

	count, err := dm1.Delete(ctx, keys...)
	require.NoError(t, err)
	require.Equal(t, keyCount, count)

	for i := 0; i < keyCount; i++ {
		_, _, err = dm1.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

// newUnreachableMember returns a member whose address is a free local port with
// nothing listening on it, so every attempt to dial it fails immediately with
// connection refused. It is used to drive the remote failure branches of the
// delete path without tearing a live member down.
func newUnreachableMember(t *testing.T) discovery.Member {
	t.Helper()

	port, err := testutil.GetFreePort()
	require.NoError(t, err)

	name := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	return discovery.Member{
		Name:      name,
		NameHash:  xxhash.Sum64([]byte(name)),
		ID:        discovery.MemberID(name, 1),
		Birthdate: 1,
	}
}

func TestDMap_Delete_RemoteOwnerUnreachable(t *testing.T) {
	// deleteKeys' remote branch has to surface a dial failure instead of
	// swallowing it, and g.Wait's error has to reach the caller.
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	key := testutil.ToKey(1)
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(1), nil))

	// Hand the partition to a member that isn't listening, so the key is
	// routed to a remote owner that cannot be dialed.
	hkey := partitions.HKey("mymap", key)
	s1.primary.PartitionByHKey(hkey).SetOwners([]discovery.Member{newUnreachableMember(t)})

	count, err := dm.Delete(ctx, key)
	require.Error(t, err)
	require.Zero(t, count)
}

func TestDMap_Delete_PreviousOwnerUnreachable(t *testing.T) {
	// The local owner branch delegates to deleteKey, which asks every previous
	// owner to drop the key first. A previous owner that cannot be dialed must
	// fail the whole delete rather than be skipped.
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	key := testutil.ToKey(1)
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(1), nil))

	// Owner() is the last entry, so this node stays the current owner while the
	// unreachable member becomes a previous owner.
	hkey := partitions.HKey("mymap", key)
	s1.primary.PartitionByHKey(hkey).SetOwners([]discovery.Member{
		newUnreachableMember(t),
		s1.rt.This(),
	})

	count, err := dm.Delete(ctx, key)
	require.Error(t, err)
	require.Zero(t, count)
}

func TestDMap_Delete_BackupOwnerUnreachable(t *testing.T) {
	// ReplicaCount is 1 by default, so deleteOnCluster fans the deletion out to
	// the backup owners. A backup owner that cannot be dialed must fail the
	// delete instead of leaving a stale replica behind silently.
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	key := testutil.ToKey(1)
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(1), nil))

	hkey := partitions.HKey("mymap", key)
	s1.backup.PartitionByHKey(hkey).SetOwners([]discovery.Member{newUnreachableMember(t)})

	count, err := dm.Delete(ctx, key)
	require.Error(t, err)
	require.Zero(t, count)
}

func TestDMap_Delete_PanicsWhenPartitionHasNoOwner(t *testing.T) {
	// An empty owners list is a programming error: deleteOnCluster panics
	// rather than deleting data whose replication targets are unknown.
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	key := testutil.ToKey(1)
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(1), nil))

	hkey := partitions.HKey("mymap", key)
	s1.primary.PartitionByHKey(hkey).SetOwners([]discovery.Member{})

	// Delete cannot be used here: grouping the keys by owner would panic in
	// Partition.Owner before deleteOnCluster is ever reached.
	require.Panics(t, func() {
		_ = dm.deleteKey(key)
	})
}

func TestDMap_Delete_MixedOwnersOneUnreachable(t *testing.T) {
	// deleteKeys fans out to every owner. When one of them fails, the error has
	// to win over the successful owners so the caller never reads a partial
	// delete as a complete one.
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	const keyCount = 10
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = testutil.ToKey(i)
		require.NoError(t, dm.Put(ctx, keys[i], testutil.ToVal(i), nil))
	}

	// Only the first key moves to an unreachable owner. The rest stay local, so
	// the fan-out has both a healthy and a failing group.
	hkey := partitions.HKey("mymap", keys[0])
	s1.primary.PartitionByHKey(hkey).SetOwners([]discovery.Member{newUnreachableMember(t)})

	count, err := dm.Delete(ctx, keys...)
	require.Error(t, err)
	require.Zero(t, count)
}

func TestDMap_Delete_Lookup(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	s3 := cluster.AddMember(nil).(*Service)

	dm2, err := s3.NewDMap("mymap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, _, err = dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Delete_StaleFragments(t *testing.T) {
	cluster := testcluster.New(NewService)
	c1 := testutil.NewConfig()
	c1.DMaps.CheckEmptyFragmentsInterval = time.Millisecond
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.DMaps.CheckEmptyFragmentsInterval = time.Millisecond
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := s2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, _, err = dm2.Get(ctx, testutil.ToKey(i))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}

	s1.wg.Add(1)
	go s1.janitorWorker()
	s2.wg.Add(1)
	go s2.janitorWorker()

	var dc int32
	for i := 0; i < 1000; i++ {
		dc = 0
		for partID := uint64(0); partID < s1.config.PartitionCount; partID++ {
			for _, instance := range []*Service{s1, s2} {
				part := instance.primary.PartitionByID(partID)
				part.Map().Range(func(name, dm interface{}) bool { dc++; return true })

				bpart := instance.backup.PartitionByID(partID)
				bpart.Map().Range(func(name, dm interface{}) bool { dc++; return true })
			}
		}
		if dc == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dc != 0 {
		t.Fatalf("Expected dmap count is 0. Got: %d", dc)
	}
}

func TestDMap_Delete_PreviousOwner(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(context.Background(), "mykey", "myvalue", nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	cmd := protocol.NewDelEntry("mydmap", "mykey").Command(context.Background())
	rc := s.client.Get(s.rt.This().String())
	err = rc.Process(context.Background(), cmd)
	require.NoError(t, err)
	require.NoError(t, cmd.Err())

	_, _, err = dm.Get(context.Background(), "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Delete_DeleteKeyValFromPreviousOwners(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(context.Background(), "mykey", "myvalue", nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Prepare fragmented partition owners list
	hkey := partitions.HKey("mydmap", "mykey")
	owners := s.primary.PartitionOwnersByHKey(hkey)
	owner := owners[len(owners)-1]

	var data []discovery.Member
	for _, member := range s.rt.Discovery().GetMembers() {
		if member.CompareByID(owner) {
			continue
		}
		data = append(data, member)
	}
	// this has to be the last one
	data = append(data, owner)
	err = dm.deleteFromPreviousOwners("mykey", data)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_Delete_Backup(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReadRepair = true
	c1.ReplicaCount = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReadRepair = true
	c2.ReplicaCount = 2
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := s2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 10; i++ {
		_, err = dm2.Delete(ctx, testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, _, err = dm2.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Delete_Compaction(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.ReadRepair = true
	c.ReplicaCount = 2
	c.DMaps.TriggerCompactionInterval = time.Millisecond
	c.DMaps.Engine.Name = config.DefaultStorageEngine

	c.DMaps.Engine.Config = map[string]interface{}{
		"tableSize":           uint64(100), // overwrite tableSize to trigger compaction.
		"maxIdleTableTimeout": time.Millisecond,
	}

	kv, err := kvstore.New(storage.NewConfig(c.DMaps.Engine.Config))
	require.NoError(t, err)
	c.DMaps.Engine.Storage = kv

	e := testcluster.NewEnvironment(c)

	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm.Delete(ctx, testutil.ToKey(i))
		require.NoError(t, err)

		_, _, err = dm.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
	checkEmptyStorageEngine(t, s)
}

func TestDMap_delEntryCommandHandler_Replica(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	rc := s.client.Get(s.rt.This().String())

	// DelEntry with the replica flag deletes from the BACKUP partition kind.
	cmd := protocol.NewDelEntry("mydmap", "missing").SetReplica().Command(s.ctx)
	err := rc.Process(s.ctx, cmd)
	if err == nil {
		err = cmd.Err()
	}
	require.NoError(t, err)
}

func TestDMap_delCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil))
	}

	keys := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		keys = append(keys, testutil.ToKey(i))
	}

	rc := s.client.Get(s.rt.This().String())
	cmd := protocol.NewDel("mydmap", keys...).Command(s.ctx)
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)
	require.NoError(t, cmd.Err())
}
