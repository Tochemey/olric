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
	"encoding/hex"
	"errors"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/testcluster"
	"github.com/tochemey/olric/internal/testutil"
)

func TestDMap_Put_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		gr, _, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		gr, _, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_AsyncReplicationMode(t *testing.T) {
	cluster := testcluster.New(NewService)
	// Create DMap services with custom configuration
	c1 := testutil.NewConfig()
	c1.ReplicationMode = config.AsyncReplicationMode
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicationMode = config.AsyncReplicationMode
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	// Wait some time for async replication
	<-time.After(100 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		gr, _, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_WriteQuorum(t *testing.T) {
	cluster := testcluster.New(NewService)
	// Create DMap services with custom configuration
	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.WriteQuorum = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	var hit bool
	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)

		hkey := partitions.HKey(dm.name, key)
		host := dm.s.primary.PartitionByHKey(hkey).Owner()
		if s1.rt.This().CompareByID(host) {
			err = dm.Put(ctx, key, testutil.ToVal(i), nil)
			if err != ErrWriteQuorum {
				t.Fatalf("Expected ErrWriteQuorum. Got: %v", err)
			}
			hit = true
		}
	}
	if !hit {
		t.Fatalf("No keys checked on %v", s1)
	}
}

func TestDMap_Put_PX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasPX: true,
		PX:    time.Millisecond,
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(10 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, _, err := dm2.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Put_NX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	pc := &PutConfig{
		HasNX: true,
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i*2), pc)
		if err == ErrKeyFound {
			err = nil
		}
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		gr, _, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Put_XX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasXX: true,
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i*2), pc)
		if errors.Is(err, ErrKeyNotFound) {
			err = nil
		}
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		_, _, err = dm.Get(ctx, testutil.ToKey(i))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Put_EX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasEX: true,
		EX:    time.Second / 4,
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(time.Second)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, _, err := dm2.Get(ctx, testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_Put_EXAT(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasEXAT: true,
		EXAT:    time.Duration(time.Now().Add(time.Second).UnixNano()),
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(time.Second)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, _, err := dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Put_PXAT(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasPXAT: true,
		PXAT:    time.Duration(time.Now().Add(time.Millisecond).UnixNano()),
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(10 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, _, err := dm2.Get(ctx, testutil.ToKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestDMap_Put_ErrKeyTooLarge(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	data := make([]byte, 300)
	_, err = rand.Read(data)
	require.NoError(t, err)
	key := hex.EncodeToString(data)
	err = dm.Put(ctx, key, "value", nil)
	require.ErrorIs(t, err, ErrKeyTooLarge)
}

func TestDMap_Put_ErrEntryTooLarge(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	data := make([]byte, 1<<21)
	_, err = rand.Read(data)
	require.NoError(t, err)

	err = dm.Put(ctx, "key", data, nil)
	require.ErrorIs(t, err, ErrEntryTooLarge)
}

func TestDMap_Put_PX_With_NX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	pc := &PutConfig{
		HasPX: true,
		PX:    time.Minute,
		HasNX: true,
	}
	for i := range 10 {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), pc)
		require.NoError(t, err)
	}

	<-time.After(10 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := range 10 {
		gr, _, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		assert.NotZero(t, gr.TTL())
	}
}

func TestDMap_Put_AsyncReplicationMode_WithReplicas(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.ReplicationMode = config.AsyncReplicationMode
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c2.ReplicationMode = config.AsyncReplicationMode
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	// Give the fire-and-forget backup goroutines time to replicate.
	<-time.After(200 * time.Millisecond)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		gr, _, err := dm2.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_putEntryCommandHandler_EntryTooLarge(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// A value larger than the configured table size triggers the error path in
	// putOnReplicaFragment.
	bigValue := make([]byte, 1<<21)
	cmd := protocol.NewPutEntry("mydmap", "key", bigValue).Command(s.ctx)
	rc := s.client.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	if err == nil {
		err = cmd.Err()
	}
	require.Error(t, err)
}

// newBenchmarkCluster starts members with the given replica count and returns
// the cluster, which the caller shuts down, together with its first member.
func newBenchmarkCluster(b *testing.B, members, replicaCount int) (*testcluster.TestCluster, *Service) {
	b.Helper()

	cluster := testcluster.New(NewService)

	var first *Service
	for range members {
		c := testutil.NewConfig()
		c.ReplicaCount = replicaCount
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.Logger = log.New(io.Discard, "", 0)
		s := cluster.AddMember(testcluster.NewEnvironment(c)).(*Service)
		if first == nil {
			first = s
		}
	}

	return cluster, first
}

// benchmarkKeys returns count keys whose primary owner is s. When samePartition
// is set they all fall into the partition of the first such key, so parallel
// writers contend on one fragment.
func benchmarkKeys(b *testing.B, s *Service, name string, count int, samePartition bool) []string {
	b.Helper()

	keys := make([]string, 0, count)
	var partID uint64
	for i := 0; len(keys) < count; i++ {
		if i > 1_000_000 {
			b.Fatal("not enough keys owned by the member")
		}

		key := testutil.ToKey(i)
		part := s.primary.PartitionByHKey(partitions.HKey(name, key))
		if !part.Owner().CompareByID(s.rt.This()) {
			continue
		}

		if samePartition {
			if len(keys) == 0 {
				partID = part.ID()
			} else if part.ID() != partID {
				continue
			}
		}

		keys = append(keys, key)
	}

	return keys
}

func BenchmarkDMap_Put_ReplicaCount1(b *testing.B) {
	cluster, s := newBenchmarkCluster(b, 1, 1)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("bench")
	require.NoError(b, err)
	keys := benchmarkKeys(b, s, "bench", 1024, false)
	value := []byte("value")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dm.Put(ctx, keys[i%len(keys)], value, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDMap_Put_SyncReplication_Serial(b *testing.B) {
	cluster, s := newBenchmarkCluster(b, 2, 2)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("bench")
	require.NoError(b, err)
	keys := benchmarkKeys(b, s, "bench", 1024, false)
	value := []byte("value")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dm.Put(ctx, keys[i%len(keys)], value, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDMap_Put_SyncReplication_Parallel(b *testing.B) {
	cluster, s := newBenchmarkCluster(b, 2, 2)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("bench")
	require.NoError(b, err)
	keys := benchmarkKeys(b, s, "bench", 64, true)
	value := []byte("value")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Intn(len(keys))
		for pb.Next() {
			if err := dm.Put(ctx, keys[i%len(keys)], value, nil); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

// silentListener accepts connections and never answers, like a peer whose
// packets are dropped. It is closed when the test ends.
func silentListener(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go func() {
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
	})

	return ln.Addr().String()
}

// departedMember removes member from the member set of s, as memberlist does
// when it confirms a death, while the routing table still lists it as an owner.
func departedMember(s *Service, member discovery.Member) {
	s.rt.Members().Lock()
	defer s.rt.Members().Unlock()

	s.rt.Members().Delete(member.ID)
}

// ownedKey returns a key of the DMap name whose primary owner is s.
func ownedKey(t *testing.T, s *Service, name string) string {
	t.Helper()

	for i := range 100000 {
		key := testutil.ToKey(i)
		part := s.primary.PartitionByHKey(partitions.HKey(name, key))
		if part.OwnerCount() > 0 && part.Owner().CompareByID(s.rt.This()) {
			return key
		}
	}

	t.Fatal("no key owned by the member")
	return ""
}

// newReplicatedPair starts two members with two copies per partition and
// returns their services and environments.
func newReplicatedPair(t *testing.T) (*testcluster.TestCluster, *Service, *Service, *environment.Environment, *environment.Environment) {
	t.Helper()

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		return c
	}

	cluster := testcluster.New(NewService)
	e1 := testcluster.NewEnvironment(newConfig())
	s1 := cluster.AddMember(e1).(*Service)
	e2 := testcluster.NewEnvironment(newConfig())
	s2 := cluster.AddMember(e2).(*Service)
	return cluster, s1, s2, e1, e2
}

// TestDMap_Put_SkipsDepartedReplica guards that a write does not dial a
// replica owner memberlist has already removed, even though the routing table
// still lists it: the write succeeds on the quorum the local copy provides
// instead of failing on the dead peer.
func TestDMap_Put_SkipsDepartedReplica(t *testing.T) {
	cluster, s1, s2, _, e2 := newReplicatedPair(t)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)
	key := ownedKey(t, s1, "mydmap")

	// The replica dies: its server is gone and memberlist has removed it,
	// but the routing table still names it as backup owner.
	require.NoError(t, e2.Get("server").(*server.Server).Shutdown(context.Background()))
	departedMember(s1, s2.rt.This())

	started := time.Now()
	require.NoError(t, dm.Put(context.Background(), key, []byte("value"), nil))
	require.Less(t, time.Since(started), time.Second, "a departed replica must not be dialed")

	gr, _, err := dm.Get(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), gr.Value())
}

// TestDMap_Put_BoundedByRequestDeadline guards that the replica call of a
// write runs under the request's deadline: a replica owner that accepts the
// connection and never answers costs the request its deadline, not the
// client's retry chain.
func TestDMap_Put_BoundedByRequestDeadline(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	c.WriteQuorum = 1
	c.ReadQuorum = 1
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s := cluster.AddMember(testcluster.NewEnvironment(c)).(*Service)

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)
	key := ownedKey(t, s, "mydmap")

	// A live member, as far as the member set knows, that never answers.
	silent := discovery.Member{Name: silentListener(t), ID: 424242}
	s.rt.Members().Lock()
	s.rt.Members().Add(silent)
	s.rt.Members().Unlock()
	s.backup.PartitionByHKey(partitions.HKey("mydmap", key)).SetOwners([]discovery.Member{silent})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	started := time.Now()
	err = dm.Put(ctx, key, []byte("value"), nil)
	require.Less(t, time.Since(started), 1500*time.Millisecond, "the replica call must stop at the request deadline")
	require.Error(t, err, "the replica did not answer within the deadline")
}

// sameFragmentKeys returns two distinct keys of the DMap name that fall into
// the same partition owned by s.
func sameFragmentKeys(t *testing.T, s *Service, name string) (string, string) {
	t.Helper()

	first := ownedKey(t, s, name)
	partID := s.primary.PartitionByHKey(partitions.HKey(name, first)).ID()
	for i := range 100000 {
		key := testutil.ToKey(i)
		if key != first && s.primary.PartitionByHKey(partitions.HKey(name, key)).ID() == partID {
			return first, key
		}
	}

	t.Fatal("no second key in the partition")
	return "", ""
}

// TestDMap_Put_StuckReplicaDoesNotBlockFragment guards the lock split: a
// write whose replica never answers holds up that key only. A read of another
// key in the same fragment, and the fragment's key count, return at once
// instead of waiting behind the stuck replication.
func TestDMap_Put_StuckReplicaDoesNotBlockFragment(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	c.WriteQuorum = 1
	c.ReadQuorum = 1
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s := cluster.AddMember(testcluster.NewEnvironment(c)).(*Service)

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)
	stuck, other := sameFragmentKeys(t, s, "mydmap")
	require.NoError(t, dm.Put(context.Background(), other, []byte("other"), nil))

	part := s.primary.PartitionByHKey(partitions.HKey("mydmap", stuck))
	silent := discovery.Member{Name: silentListener(t), ID: 424242}
	s.rt.Members().Lock()
	s.rt.Members().Add(silent)
	s.rt.Members().Unlock()
	s.backup.PartitionByID(part.ID()).SetOwners([]discovery.Member{silent})

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- dm.Put(ctx, stuck, []byte("stuck"), nil)
	}()

	// Let the write reach its replica call before measuring.
	time.Sleep(200 * time.Millisecond)

	// The fragment's key count needs the fragment lock only.
	started := time.Now()
	require.Equal(t, 1, part.Length())
	require.Less(t, time.Since(started), 300*time.Millisecond, "a stuck replica must not block the fragment")

	// A read of the other key consults the same silent replica, so it is
	// bounded by its own deadline rather than by the stuck write.
	readCtx, readCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer readCancel()

	started = time.Now()
	gr, _, err := dm.Get(readCtx, other)
	require.NoError(t, err)
	require.Equal(t, []byte("other"), gr.Value())
	require.Less(t, time.Since(started), 700*time.Millisecond, "a read is bounded by its own deadline, not by the stuck write")

	require.Error(t, <-done, "the stuck write itself fails at its deadline")
}

// TestDMap_Put_ConcurrentWritesToOneKeyStayOrdered guards what the key lock
// preserves: writes of one key from many goroutines reach the replica in the
// order the primary applied them, so both end with the same value.
func TestDMap_Put_ConcurrentWritesToOneKeyStayOrdered(t *testing.T) {
	cluster, s1, s2, _, _ := newReplicatedPair(t)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)
	key := ownedKey(t, s1, "mydmap")

	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dm1.Put(context.Background(), key, testutil.ToVal(i), nil)
		}()
	}
	wg.Wait()

	gr, _, err := dm1.Get(context.Background(), key)
	require.NoError(t, err)
	primary := gr.Value()

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)
	hkey := partitions.HKey("mydmap", key)
	replica, err := dm2.loadFragment(s2.backup.PartitionByHKey(hkey))
	require.NoError(t, err)

	replica.RLock()
	entry, err := replica.storage.Get(hkey)
	replica.RUnlock()
	require.NoError(t, err)
	require.Equal(t, primary, entry.Value(), "the replica must hold the primary's last write")
}
