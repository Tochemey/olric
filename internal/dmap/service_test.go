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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/internal/testcluster"
	"github.com/tochemey/olric/internal/testutil"
)

func TestDMapService(t *testing.T) {
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s, ok := cluster.AddMember(nil).(*Service)
	if !ok {
		t.Fatal("AddMember returned a different service.Service implementation")
	}

	err := s.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_getType(t *testing.T) {
	// Pointer kind: returns the element type name.
	require.Equal(t, "InitialSyncCompleteEvent", getType(&events.InitialSyncCompleteEvent{}))
	// Non-pointer kind: returns the type name directly.
	require.Equal(t, "string", getType("hello"))
}

func TestDMap_isAlive_AfterCancel(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	require.True(t, s.isAlive())
	s.cancel()
	require.False(t, s.isAlive())
}

func TestDMap_Shutdown_ContextDeadline(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Add a never-completing entry to the wait group so that wg.Wait blocks
	// and Shutdown is forced to honor the context deadline.
	s.wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := s.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Release the wait group so the cluster can shut down cleanly.
	s.wg.Done()
}

func TestDMap_initialSyncCompletePublisher(t *testing.T) {
	cluster := testcluster.New(NewService)

	// Wire up a sync state marked as complete. It must be injected through the
	// environment before the member starts: assigning s.syncState afterwards
	// races with Service.Start reading the field from its own goroutine.
	st := syncstate.New()
	st.Reconcile(nil, time.Minute, time.Now()) // empty pending set signals "done" immediately
	require.True(t, st.IsDone())

	e := testcluster.NewEnvironment(nil)
	e.Set("syncstate", st)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	s.wg.Add(1)
	go s.initialSyncCompletePublisher()

	// Allow the publisher (and the spawned publishEvent goroutine) to run.
	<-time.After(200 * time.Millisecond)
}

func TestDMap_initialSyncCompletePublisher_ContextDone(t *testing.T) {
	cluster := testcluster.New(NewService)

	// Sync state that never completes so the publisher blocks until the
	// service context is cancelled. Injected through the environment before
	// the member starts: assigning s.syncState afterwards races with
	// Service.Start reading the field from its own goroutine.
	st := syncstate.New()
	st.Reconcile([]uint64{1, 2, 3}, time.Minute, time.Now())
	require.False(t, st.IsDone())

	e := testcluster.NewEnvironment(nil)
	e.Set("syncstate", st)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	done := make(chan struct{})
	s.wg.Add(1)
	go func() {
		s.initialSyncCompletePublisher()
		close(done)
	}()

	// Cancel the service context to trigger the ctx.Done() branch.
	s.cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("initialSyncCompletePublisher did not return after context cancellation")
	}
}

// primaryKeyWithBackupOn returns the first key from start on whose primary
// copy dm's member owns and whose backup copy backup owns, and its hashed key.
func primaryKeyWithBackupOn(dm *DMap, backup *Service, start int) (string, uint64, bool) {
	for i := start; i < start+1000; i++ {
		key := testutil.ToKey(i)
		hkey := dm.s.hkey(dm.name, key)

		if !dm.s.primary.PartitionByHKey(hkey).Owner().CompareByID(dm.s.rt.This()) {
			continue
		}

		for _, owner := range dm.s.backup.PartitionOwnersByHKey(hkey) {
			if owner.CompareByID(backup.rt.This()) {
				return key, hkey, true
			}
		}
	}

	return "", 0, false
}

func TestDMap_Shutdown_WaitsForAsyncBackupWrites(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.ReplicationMode = config.AsyncReplicationMode
	// One connection to each peer: a backup write that holds it leaves the
	// next one waiting in the pool, where a cancelled context drops it before
	// anything is sent.
	c1.Client.PoolSize = 1
	s1 := cluster.AddMember(testcluster.NewEnvironment(c1)).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c2.ReplicationMode = config.AsyncReplicationMode
	s2 := cluster.AddMember(testcluster.NewEnvironment(c2)).(*Service)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	var blockedKey, pendingKey string
	var blockedHKey, pendingHKey uint64

	require.Eventually(t, func() bool {
		var ok bool
		blockedKey, blockedHKey, ok = primaryKeyWithBackupOn(dm1, s2, 0)
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	// Searched past the range of the first key, so the two keys differ.
	pendingKey, pendingHKey, ok := primaryKeyWithBackupOn(dm1, s2, 1000)
	require.True(t, ok)

	// Hold the backup fragment of the first key on s2, so its backup write
	// keeps s1's only connection to s2 busy.
	blocked, err := dm2.loadOrCreateFragment(s2.backup.PartitionByHKey(blockedHKey))
	require.NoError(t, err)

	blocked.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			blocked.Unlock()
		}
	}()

	ctx := context.Background()
	require.NoError(t, dm1.Put(ctx, blockedKey, testutil.ToVal(1), nil))

	rc := s1.client.Get(s2.rt.This().String())
	require.Eventually(t, func() bool {
		st := rc.PoolStats()
		return st.TotalConns == 1 && st.IdleConns == 0
	}, 5*time.Second, 10*time.Millisecond)

	// Acknowledged to the caller; its backup write waits for the connection.
	require.NoError(t, dm1.Put(ctx, pendingKey, testutil.ToVal(2), nil))

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s1.Shutdown(shutdownCtx)
	}()

	// Let Shutdown start while both backup writes are in flight, then let
	// them through.
	<-time.After(200 * time.Millisecond)
	blocked.Unlock()
	unlocked = true

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Shutdown did not return")
	}

	pending, err := dm2.loadFragment(s2.backup.PartitionByHKey(pendingHKey))
	require.NoError(t, err, "the backup write acknowledged before Shutdown never reached the backup owner")

	pending.Lock()
	defer pending.Unlock()

	require.True(t, pending.storage.Check(pendingHKey), "the backup write acknowledged before Shutdown never reached the backup owner")
}

func TestDMap_Shutdown_AsyncBackupWritesBoundedByContext(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// A backup write that never finishes.
	s.asyncWrites.Add(1)
	defer s.asyncWrites.Add(-1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := s.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, s.isAlive())
}

func TestDMap_Shutdown_RefusesAsyncWritesWhileDraining(t *testing.T) {
	cluster := testcluster.New(NewService)

	c := testutil.NewConfig()
	c.ReplicaCount = 2
	c.ReplicationMode = config.AsyncReplicationMode
	s := cluster.AddMember(testcluster.NewEnvironment(c)).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	// An async write in flight keeps Shutdown draining. Released by a defer
	// that runs before cluster.Shutdown, so a failing assertion cannot leave
	// that Shutdown waiting on it.
	s.asyncWrites.Add(1)
	release := sync.OnceFunc(func() { s.asyncWrites.Add(-1) })
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.Shutdown(ctx)
	}()

	require.Eventually(t, func() bool {
		s.asyncWritesMtx.RLock()
		defer s.asyncWritesMtx.RUnlock()

		return s.draining
	}, 5*time.Second, time.Millisecond)

	// Shutdown has not cancelled the service yet, so without the gate this
	// write would be stored and acknowledged with its backup write about to
	// be dropped.
	require.True(t, s.isAlive())

	key := testutil.ToKey(1)
	err = dm.Put(context.Background(), key, testutil.ToVal(1), nil)
	require.ErrorIs(t, err, ErrServerGone)

	hkey := s.hkey(dm.name, key)
	f, err := dm.loadFragment(s.primary.PartitionByHKey(hkey))
	if err == nil {
		f.Lock()
		stored := f.storage.Check(hkey)
		f.Unlock()

		require.False(t, stored, "a refused write was stored")
	} else {
		require.ErrorIs(t, err, errFragmentNotFound)
	}

	release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Shutdown did not return")
	}
}

func TestDMap_asyncPutOnCluster_BackupWriteNotStarted(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.ReplicationMode = config.AsyncReplicationMode
	s1 := cluster.AddMember(testcluster.NewEnvironment(c1)).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c2.ReplicationMode = config.AsyncReplicationMode
	s2 := cluster.AddMember(testcluster.NewEnvironment(c2)).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	var key string
	require.Eventually(t, func() bool {
		var ok bool
		key, _, ok = primaryKeyWithBackupOn(dm, s2, 0)
		return ok
	}, 5*time.Second, 50*time.Millisecond)

	// Runs before cluster.Shutdown, so a count the code under test leaks
	// fails the assertion below instead of hanging that Shutdown.
	defer s1.asyncWrites.Store(0)

	// The state once Shutdown gave up waiting at its deadline: background
	// goroutines are refused while the service context is still alive.
	s1.shutdownMtx.Lock()
	s1.closed = true
	s1.shutdownMtx.Unlock()

	err = dm.Put(context.Background(), key, testutil.ToVal(1), nil)
	require.ErrorIs(t, err, ErrServerGone)
	require.Zero(t, s1.asyncWrites.Load())
}
