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

package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/internal/testutil/mockfragment"
	"github.com/tochemey/olric/pkg/storage"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/cluster/routingtable"
)

func TestBalance_Primary_Move(t *testing.T) {
	t.Run("With TSL", func(t *testing.T) {
		cluster := newMockCluster(t)
		defer cluster.shutdown()

		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		e1 := newTestEnvironment(c1)
		cluster.addNode(e1)

		fragments := make(map[uint64]*mockfragment.MockFragment)

		// Create a MockFragment and insert some fake data
		c := e1.Get("config").(*config.Config)
		part := e1.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
		for partID := uint64(0); partID < c.PartitionCount; partID++ {
			part := part.PartitionByID(partID)
			s := mockfragment.New()
			s.Fill()
			part.Map().Store("dmap.test-data", s)
			fragments[partID] = s
		}

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		e2 := newTestEnvironment(c2)
		b2 := cluster.addNode(e2)

		err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !b2.rt.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		for partID, f := range fragments {
			result := f.Result()
			if len(result) == 0 {
				continue
			}

			require.Len(t, result, 1)
			require.NotNil(t, result[partitions.PRIMARY])
			r := result[partitions.PRIMARY]
			require.NotNil(t, r[partID])
			require.Equal(t, "test-data", r[partID].Name)
			require.Equal(t, []discovery.Member{b2.rt.This()}, r[partID].Owners)
		}
	})
	t.Run("Without TSL", func(t *testing.T) {
		cluster := newMockCluster(t)
		defer cluster.shutdown()

		e1 := newTestEnvironment(nil)
		cluster.addNode(e1)

		fragments := make(map[uint64]*mockfragment.MockFragment)

		// Create a MockFragment and insert some fake data
		c := e1.Get("config").(*config.Config)
		part := e1.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
		for partID := uint64(0); partID < c.PartitionCount; partID++ {
			part := part.PartitionByID(partID)
			s := mockfragment.New()
			s.Fill()
			part.Map().Store("dmap.test-data", s)
			fragments[partID] = s
		}

		e2 := newTestEnvironment(nil)
		b2 := cluster.addNode(e2)

		err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !b2.rt.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		for partID, f := range fragments {
			result := f.Result()
			if len(result) == 0 {
				continue
			}

			require.Len(t, result, 1)
			require.NotNil(t, result[partitions.PRIMARY])
			r := result[partitions.PRIMARY]
			require.NotNil(t, r[partID])
			require.Equal(t, "test-data", r[partID].Name)
			require.Equal(t, []discovery.Member{b2.rt.This()}, r[partID].Owners)
		}
	})
}

func TestBalance_Empty_Backup_Move(t *testing.T) {
	t.Run("With TSL", func(t *testing.T) {
		cluster := newMockCluster(t)
		defer cluster.shutdown()

		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		c1.ReplicaCount = 2
		e1 := newTestEnvironment(c1)
		b1 := cluster.addNode(e1)

		b1.rt.UpdateEagerly()

		err := checkBackupOwnership(e1)
		require.NoError(t, err)

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		c2.ReplicaCount = 2
		e2 := newTestEnvironment(c2)
		b2 := cluster.addNode(e2)

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !b2.rt.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		b1.rt.UpdateEagerly()

		err = checkBackupOwnership(e2)
		require.NoError(t, err)
	})
	t.Run("Without TSL", func(t *testing.T) {
		cluster := newMockCluster(t)
		defer cluster.shutdown()

		c1 := testutil.NewConfig()
		c1.ReplicaCount = 2
		e1 := newTestEnvironment(c1)
		b1 := cluster.addNode(e1)

		b1.rt.UpdateEagerly()

		err := checkBackupOwnership(e1)
		require.NoError(t, err)

		c2 := testutil.NewConfig()
		c2.ReplicaCount = 2
		e2 := newTestEnvironment(c2)
		b2 := cluster.addNode(e2)

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !b2.rt.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		b1.rt.UpdateEagerly()

		err = checkBackupOwnership(e2)
		require.NoError(t, err)
	})
}

func TestBalance_Backup_Move(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		cluster := newMockCluster(t)
		defer cluster.shutdown()

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		c1.ReplicaCount = 2
		e1 := newTestEnvironment(c1)
		b1 := cluster.addNode(e1)

		fragments := make(map[uint64]*mockfragment.MockFragment)

		c := e1.Get("config").(*config.Config)
		part := e1.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)
		for partID := uint64(0); partID < c.PartitionCount; partID++ {
			part := part.PartitionByID(partID)
			s := mockfragment.New()
			s.Fill()
			part.Map().Store("dmap.test-data", s)
			fragments[partID] = s
		}

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		c2.ReplicaCount = 2
		e2 := newTestEnvironment(c2)
		b2 := cluster.addNode(e2)

		err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !b2.rt.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		for i := 0; i < 5; i++ {
			b1.rt.UpdateEagerly()
			err = checkBackupOwnership(e2)
			require.NoError(t, err)
		}

		for partID, f := range fragments {
			result := f.Result()
			if len(result) == 0 {
				continue
			}

			require.Len(t, result, 1)
			// The node is the primary owner of these partitions, so the data
			// in its backup fragments is promoted into the local primary
			// (see promoteBackupCopies) instead of being relocated to the
			// other node, which would drop the only copy from this node.
			require.NotNil(t, result[partitions.PRIMARY])
			r := result[partitions.PRIMARY]
			require.NotNil(t, r[partID])
			require.Equal(t, "test-data", r[partID].Name)
			require.Equal(t, []discovery.Member{b1.rt.This()}, r[partID].Owners)
		}
	})
	t.Run("Without TLS", func(t *testing.T) {
		cluster := newMockCluster(t)
		defer cluster.shutdown()

		c1 := testutil.NewConfig()
		c1.ReplicaCount = 2
		e1 := newTestEnvironment(c1)
		b1 := cluster.addNode(e1)

		fragments := make(map[uint64]*mockfragment.MockFragment)

		c := e1.Get("config").(*config.Config)
		part := e1.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)
		for partID := uint64(0); partID < c.PartitionCount; partID++ {
			part := part.PartitionByID(partID)
			s := mockfragment.New()
			s.Fill()
			part.Map().Store("dmap.test-data", s)
			fragments[partID] = s
		}

		c2 := testutil.NewConfig()
		c2.ReplicaCount = 2
		e2 := newTestEnvironment(c2)
		b2 := cluster.addNode(e2)

		err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !b2.rt.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		require.NoError(t, err)

		for range 5 {
			b1.rt.UpdateEagerly()
			err = checkBackupOwnership(e2)
			require.NoError(t, err)
		}

		for partID, f := range fragments {
			result := f.Result()
			if len(result) == 0 {
				continue
			}

			require.Len(t, result, 1)
			// The node is the primary owner of these partitions, so the data
			// in its backup fragments is promoted into the local primary
			// (see promoteBackupCopies) instead of being relocated to the
			// other node, which would drop the only copy from this node.
			require.NotNil(t, result[partitions.PRIMARY])
			r := result[partitions.PRIMARY]
			require.NotNil(t, r[partID])
			require.Equal(t, "test-data", r[partID].Name)
			require.Equal(t, []discovery.Member{b1.rt.This()}, r[partID].Owners)
		}
	})
}

func newTestEnvironment(c *config.Config) *environment.Environment {
	if c == nil {
		c = testutil.NewConfig()
	}

	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", server.NewClient(c.Client))
	return e
}

func newBalancerForTest(e *environment.Environment) *Balancer {
	rt := routingtable.New(e)
	srv := e.Get("server").(*server.Server)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-srv.StartedCtx.Done()

	e.Set("routingtable", rt)
	b := New(e)
	return b
}

type mockCluster struct {
	t         *testing.T
	peerPorts []int
	errGr     errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
	// noLoop keeps the balancer's periodic loop from starting, so a test
	// drives every cycle itself.
	noLoop bool
}

func newMockCluster(t *testing.T) *mockCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockCluster{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (mc *mockCluster) addNode(e *environment.Environment) *Balancer {
	if e == nil {
		e = newTestEnvironment(nil)
	}
	c := e.Get("config").(*config.Config)
	c.TriggerBalancerInterval = time.Millisecond
	c.DMaps.CheckEmptyFragmentsInterval = time.Millisecond

	port, err := testutil.GetFreePort()
	if err != nil {
		require.NoError(mc.t, err)
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range mc.peerPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	srv := testutil.NewServer(c)
	e.Set("server", srv)
	b := newBalancerForTest(e)

	if !mc.noLoop {
		require.NoError(mc.t, b.Start())
	}

	err = b.rt.Join()
	require.NoError(mc.t, err)

	err = b.rt.Start()
	if err != nil {
		require.NoError(mc.t, err)
	}

	mc.errGr.Go(func() error {
		<-mc.ctx.Done()
		return srv.Shutdown(context.Background())
	})

	mc.errGr.Go(func() error {
		<-mc.ctx.Done()
		return b.rt.Shutdown(context.Background())
	})

	mc.peerPorts = append(mc.peerPorts, port)

	mc.t.Cleanup(func() {
		require.NoError(mc.t, b.Shutdown(context.Background()))
	})

	return b
}

func (mc *mockCluster) shutdown() {
	mc.cancel()
	require.NoError(mc.t, mc.errGr.Wait())
}

func checkBackupOwnership(e *environment.Environment) error {
	c := e.Get("config").(*config.Config)
	primary := e.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
	backup := e.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)

	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		primaryOwner := primary.PartitionByID(partID).Owner()
		part := backup.PartitionByID(partID)
		for _, owner := range part.Owners() {
			if primaryOwner.CompareByID(owner) {
				return fmt.Errorf("%s is the primary and backup owner of partID: %d at the same time", primaryOwner, partID)
			}
		}
	}

	return nil
}

func TestRegisterHandlers(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	e := newTestEnvironment(nil)
	b := cluster.addNode(e)

	// RegisterHandlers is a no-op but must remain callable.
	require.NotPanics(t, func() {
		b.RegisterHandlers()
	})
}

func TestShutdown_Idempotent(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	e := newTestEnvironment(nil)
	b := cluster.addNode(e)

	// First shutdown stops the balance goroutine.
	require.NoError(t, b.Shutdown(context.Background()))
	// Second shutdown must be a no-op and return nil.
	require.NoError(t, b.Shutdown(context.Background()))

	// isAlive must now report false because the context was cancelled.
	require.False(t, b.isAlive())
}

func TestNew_WithSyncState(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	e := newTestEnvironment(nil)
	state := syncstate.New()
	e.Set("syncstate", state)

	b := cluster.addNode(e)
	require.NotNil(t, b.syncState)
	// The same state instance is shared with the balancer; PendingEmpty is
	// driven by the routing table lifecycle, so just assert it is wired up.
	require.NotPanics(t, func() {
		_ = b.syncState.PendingEmpty()
	})
}

func TestTryAckRebalance_Guards(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	e := newTestEnvironment(nil)
	b := cluster.addNode(e)

	// Signature 0 must be a no-op (no panic, lastAckedGeneration stays 0).
	b.tryAckRebalance(0, 1)
	require.Equal(t, uint64(0), b.lastAckedGeneration)

	// When syncState reports pending data, ack is skipped.
	pending := syncstate.New()
	pending.Reconcile([]uint64{1}, time.Minute, time.Now())
	b.syncState = pending
	require.False(t, b.syncState.PendingEmpty())
	b.tryAckRebalance(1234, 1)
	require.Equal(t, uint64(0), b.lastAckedGeneration)

	// When the context is cancelled, ack is skipped even with empty pending.
	b.syncState = syncstate.New()
	b.cancel()
	b.tryAckRebalance(5678, 1)
	require.Equal(t, uint64(0), b.lastAckedGeneration)
}

// TestTryAckRebalance_AcksOncePerGeneration guards the ack marker against
// content-derived signatures that repeat: the balancer acks once per installed
// routing table generation, not once per signature value, so a table that
// returns to an earlier state (same signature, new generation) is acked
// again. Keying on the signature would leave the marker at the first
// generation.
func TestTryAckRebalance_AcksOncePerGeneration(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	e := newTestEnvironment(nil)
	b := cluster.addNode(e)

	sign, generation := b.rt.Version()
	require.NotZero(t, sign)
	require.NotZero(t, generation)

	// The first ack for this generation is sent and recorded.
	b.tryAckRebalance(sign, generation)
	require.Equal(t, generation, b.lastAckedGeneration)

	// The same signature under a new generation is acked again.
	b.tryAckRebalance(sign, generation+1)
	require.Equal(t, generation+1, b.lastAckedGeneration)
}

// TestBalanceEagerly exercises the proactive node-join balance path including
// pushPrimaryToBackups by filling the primary partitions and triggering an
// eager balance on a two-node cluster with backups enabled.
func TestBalanceEagerly(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.EnableProactiveSyncOnJoin = true
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

	c := e1.Get("config").(*config.Config)
	primary := e1.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := primary.PartitionByID(partID)
		s := mockfragment.New()
		s.Fill()
		part.Map().Store("dmap.test-data", s)
	}

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c2.EnableProactiveSyncOnJoin = true
	e2 := newTestEnvironment(c2)
	b2 := cluster.addNode(e2)

	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	// Drive the eager balance path several times. The first push records the
	// proactive sync signature; later calls hit the already-synced branch.
	for i := 0; i < 5; i++ {
		b1.rt.UpdateEagerly()
		require.NotPanics(t, func() {
			b1.BalanceEagerly()
		})
	}
}

// TestPromoteBackupCopies verifies that data sitting in a backup fragment of a
// partition this node is the primary owner of gets merged into the primary
// fragment (a move to itself with the PRIMARY target kind) instead of being
// relocated to another node. A single-node cluster with ReplicaCount=2 models
// the post-failure state: the survivor owns every partition while its backup
// fragments still hold the only copy of the data.
func TestPromoteBackupCopies(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.EnableProactiveSyncOnJoin = true
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

	b1.rt.UpdateEagerly()
	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b1.rt.IsBootstrapped() {
			return errors.New("the node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	fragments := make(map[uint64]*mockfragment.MockFragment)

	c := e1.Get("config").(*config.Config)
	backup := e1.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		part := backup.PartitionByID(partID)
		s := mockfragment.New()
		s.Fill()
		part.Map().Store("dmap.test-data", s)
		fragments[partID] = s
	}

	// Stats is guarded by the fragment mutex, so once every fragment reports
	// empty, the balancer's writes to the move results are visible too.
	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		for partID, f := range fragments {
			if f.Stats().Length != 0 {
				return fmt.Errorf("backup fragment of PartID: %d has not been promoted yet", partID)
			}
		}
		return nil
	})
	require.NoError(t, err)

	for partID, f := range fragments {
		r := f.Result()[partitions.PRIMARY][partID]
		require.Equal(t, "test-data", r.Name)
		// Promotion targets the node itself, never another member.
		require.Equal(t, []discovery.Member{b1.rt.This()}, r.Owners)
		// The backup fragment must be drained afterwards so backupCopies has
		// nothing left to relocate.
		require.Equal(t, 0, f.Stats().Length)
		// The data must not have been relocated as a backup copy anywhere.
		require.Nil(t, f.Result()[partitions.BACKUP])
	}
}

// TestPromoteBackupCopies_NotPrimaryOwner verifies that backup fragments of
// partitions owned by another node are left alone by the promotion pass.
func TestPromoteBackupCopies_NotPrimaryOwner(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.EnableProactiveSyncOnJoin = true
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c2.EnableProactiveSyncOnJoin = true
	e2 := newTestEnvironment(c2)
	b2 := cluster.addNode(e2)

	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)
	b1.rt.UpdateEagerly()

	// Fill b1's backup fragments only for partitions whose primary owner is b2.
	fragments := make(map[uint64]*mockfragment.MockFragment)
	c := e1.Get("config").(*config.Config)
	primary := e1.Get(strings.ToLower(partitions.PRIMARY.String())).(*partitions.Partitions)
	backup := e1.Get(strings.ToLower(partitions.BACKUP.String())).(*partitions.Partitions)
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		if primary.PartitionByID(partID).Owner().CompareByName(b1.rt.This()) {
			continue
		}
		part := backup.PartitionByID(partID)
		s := mockfragment.New()
		s.Fill()
		part.Map().Store("dmap.test-data", s)
		fragments[partID] = s
	}
	require.NotEmpty(t, fragments)

	b1.BalanceEagerly()

	for partID, f := range fragments {
		require.Nilf(t, f.Result()[partitions.PRIMARY],
			"backup fragment of PartID: %d owned by another node must not be promoted", partID)
	}
}

func TestPromoteBackupCopies_SingleReplica(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	// ReplicaCount defaults to MinimumReplicaCount, so there are no backup
	// copies to promote and the pass must be a no-op.
	e := newTestEnvironment(nil)
	b := cluster.addNode(e)

	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b.rt.IsBootstrapped() {
			return errors.New("node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	moved, _, aborted := b.promoteBackupCopies(b.rt.Signature())
	require.False(t, moved)
	require.False(t, aborted)
}

func TestBalanceEagerly_SingleReplica(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	// ReplicaCount defaults to MinimumReplicaCount so the backup branches are
	// skipped, covering the early-skip path of pushPrimaryToBackups indirectly.
	e := newTestEnvironment(nil)
	b := cluster.addNode(e)

	err := testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !b.rt.IsBootstrapped() {
			return errors.New("node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.BalanceEagerly()
	})

	// pushPrimaryToBackups returns immediately when replicas are at minimum.
	sign, _ := b.rt.Version()
	moved, _, aborted := b.pushPrimaryToBackups(sign)
	require.False(t, moved)
	require.False(t, aborted)
}

// failingFragment is a Fragment whose moves fail the first failures attempts
// and succeed afterwards, counting every attempt. Like the real fragment it
// keeps its data on a push to a backup and drops it on any other move.
type failingFragment struct {
	mu       sync.Mutex
	failures int
	calls    int
	// replications counts the Replicate calls among calls, so a test can tell
	// a restore from a move or a promotion.
	replications int
	length       int
}

func newFailingFragment(failures int) *failingFragment {
	return &failingFragment{failures: failures, length: 1}
}

func (f *failingFragment) Name() string { return "failing" }

func (f *failingFragment) Stats() storage.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()

	return storage.Stats{Length: f.length}
}

func (f *failingFragment) Move(part *partitions.Partition, name string, owners []discovery.Member) error {
	return f.MoveWithTargetKind(part, name, owners, part.Kind())
}

func (f *failingFragment) MoveWithTargetKind(_ *partitions.Partition, _ string, _ []discovery.Member, targetKind partitions.Kind) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.calls <= f.failures {
		return errors.New("move failed")
	}

	// A successful move drops the local table whatever the target kind, as
	// the real fragment does; only Replicate keeps it.
	f.length = 0

	return nil
}

func (f *failingFragment) Compaction() (bool, error) { return false, nil }

func (f *failingFragment) Destroy() error { return nil }

func (f *failingFragment) Close() error { return nil }

// attempts returns how many moves were attempted on the fragment.
func (f *failingFragment) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// replicated returns how many of the attempts were Replicate calls.
func (f *failingFragment) replicated() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.replications
}

// partitionsOwnedBy returns the ids of the partitions whose primary owner, in
// primary's view, is member.
func partitionsOwnedBy(c *config.Config, primary *partitions.Partitions, member discovery.Member) []uint64 {
	var out []uint64
	for partID := range c.PartitionCount {
		part := primary.PartitionByID(partID)
		if part.OwnerCount() > 0 && part.Owner().CompareByName(member) {
			out = append(out, partID)
		}
	}

	return out
}

// TestRunBalance_AcksAfterMovingWithoutWaitingForTick guards that one balancer
// invocation acks as soon as it has nothing left to move: the cycle that
// hands the joiner its partitions is followed by another one right away, so
// the ack does not wait for the next TriggerBalancerInterval tick.
func TestRunBalance_AcksAfterMovingWithoutWaitingForTick(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	e1 := newTestEnvironment(nil)
	b1 := cluster.addNode(e1)

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	for partID := range c.PartitionCount {
		frag := mockfragment.New()
		frag.Fill()
		primary.PartitionByID(partID).Map().Store("dmap.test-data", frag)
	}

	b2 := cluster.addNode(newTestEnvironment(nil))
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	// The cycle needs something to move: wait until the table hands at least
	// one partition to the joiner.
	require.Eventually(t, func() bool {
		return len(partitionsOwnedBy(c, primary, b2.rt.This())) > 0
	}, 10*time.Second, 50*time.Millisecond)

	_, generation := b1.rt.Version()
	require.Zero(t, b1.lastAckedGeneration)

	b1.triggerBalancer()

	require.Equal(t, generation, b1.lastAckedGeneration, "the ack must follow the moves in the same invocation")
	for _, partID := range partitionsOwnedBy(c, primary, b2.rt.This()) {
		require.Zero(t, primary.PartitionByID(partID).Length(), "the joiner's partitions must have been moved")
	}
}

// TestRunBalance_FailedMovesDoNotSpinOrAck guards the other half of the
// re-run rule: a cycle whose moves only failed neither acks nor re-runs, so
// each failing fragment is attempted once per invocation and the next tick
// retries it.
func TestRunBalance_FailedMovesDoNotSpinOrAck(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	e1 := newTestEnvironment(nil)
	b1 := cluster.addNode(e1)
	b2 := cluster.addNode(newTestEnvironment(nil))
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	var owned []uint64
	require.Eventually(t, func() bool {
		owned = partitionsOwnedBy(c, primary, b2.rt.This())
		return len(owned) > 0
	}, 10*time.Second, 50*time.Millisecond)

	frags := make(map[uint64]*failingFragment, len(owned))
	for _, partID := range owned {
		frag := newFailingFragment(1000)
		primary.PartitionByID(partID).Map().Store("dmap.test-data", frag)
		frags[partID] = frag
	}

	b1.triggerBalancer()

	require.Zero(t, b1.lastAckedGeneration, "failed moves must hold the ack")
	for partID, frag := range frags {
		require.Equal(t, 1, frag.attempts(), "partition %d must be attempted once per invocation", partID)
	}
}

// TestBalanceEagerly_RetriesFailedProactivePush guards that a proactive
// primary-to-backup push that failed is not recorded as done for the installed
// table: the next invocation pushes again, and only a successful push ends the
// retries for that table.
func TestBalanceEagerly_RetriesFailedProactivePush(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.EnableProactiveSyncOnJoin = true
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c2.EnableProactiveSyncOnJoin = true
	b2 := cluster.addNode(newTestEnvironment(c2))
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	// Partitions this member owns and the joiner replicates: the proactive
	// push targets exactly these.
	primary := e1.Get("primary").(*partitions.Partitions)
	backup := e1.Get("backup").(*partitions.Partitions)
	var replicated []uint64
	require.Eventually(t, func() bool {
		replicated = replicated[:0]
		for _, partID := range partitionsOwnedBy(c1, primary, b1.rt.This()) {
			for _, owner := range backup.PartitionByID(partID).Owners() {
				if owner.CompareByName(b2.rt.This()) {
					replicated = append(replicated, partID)
					break
				}
			}
		}

		return len(replicated) > 0
	}, 10*time.Second, 50*time.Millisecond)

	frags := make(map[uint64]*failingFragment, len(replicated))
	for _, partID := range replicated {
		frag := newFailingFragment(1)
		primary.PartitionByID(partID).Map().Store("dmap.test-data", frag)
		frags[partID] = frag
	}

	b1.BalanceEagerly()
	for partID := range frags {
		require.Zero(t, b1.pushedSignature[partID], "a failed push must not be recorded as done")
	}
	for partID, frag := range frags {
		require.Equal(t, 1, frag.attempts(), "partition %d", partID)
	}

	b1.BalanceEagerly()
	signature, _ := b1.rt.Version()
	for partID := range frags {
		require.Equal(t, signature, b1.pushedSignature[partID], "a successful push is recorded for the installed table")
	}
	for partID, frag := range frags {
		require.Equal(t, 2, frag.attempts(), "partition %d", partID)
	}

	b1.BalanceEagerly()
	for partID, frag := range frags {
		require.Equal(t, 2, frag.attempts(), "partition %d must not be pushed again for the same table", partID)
	}
}

// Replicate counts an attempt like MoveWithTargetKind and always keeps the
// data, as a copy does.
func (f *failingFragment) Replicate(_ *partitions.Partition, _ string, _ []discovery.Member, _ partitions.Kind) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	f.replications++
	if f.calls <= f.failures {
		return errors.New("replicate failed")
	}

	return nil
}

// restoreFixture starts two members, the second replicating the first, with
// re-replication configured by flag and delay, and returns both balancers,
// the first member's environment and the partitions the second member owns
// as primary while the first holds their backup copies. Each of those backup
// copies is a failingFragment so the test can count restore attempts.
func restoreFixture(t *testing.T, cluster *mockCluster, proactive bool, delay time.Duration) (b1, b2 *Balancer, e1, e2 *environment.Environment, frags map[uint64]*failingFragment) {
	t.Helper()

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.EnableProactiveSyncOnJoin = proactive
		c.ReplicaRestoreDelay = delay
		return c
	}

	e1 = newTestEnvironment(newConfig())
	b1 = cluster.addNode(e1)
	e2 = newTestEnvironment(newConfig())
	b2 = cluster.addNode(e2)
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	backup := e1.Get("backup").(*partitions.Partitions)

	var candidates []uint64
	require.Eventually(t, func() bool {
		candidates = candidates[:0]
		for _, partID := range partitionsOwnedBy(c, primary, b2.rt.This()) {
			if primary.PartitionByID(partID).OwnerCount() != 1 {
				continue
			}

			for _, owner := range backup.PartitionByID(partID).Owners() {
				if owner.CompareByName(b1.rt.This()) {
					candidates = append(candidates, partID)
					break
				}
			}
		}

		return len(candidates) > 0
	}, 10*time.Second, 50*time.Millisecond)

	frags = make(map[uint64]*failingFragment, len(candidates))
	for _, partID := range candidates {
		frag := newFailingFragment(0)
		backup.PartitionByID(partID).Map().Store("dmap.test-data", frag)
		frags[partID] = frag
	}

	return b1, b2, e1, e2, frags
}

// scheduleDeparture makes b see the primary owner of every partition in
// frags as gone: one cycle records the current owners, the recorded owner is
// replaced with an ID no member has, and the next cycle schedules the restore
// of those partitions, due ReplicaRestoreDelay later.
func scheduleDeparture(b *Balancer, frags map[uint64]*failingFragment) {
	b.triggerBalancer()
	for partID := range frags {
		b.lastOwners[partID] = []uint64{0xDEAD}
	}

	b.triggerBalancer()
}

// TestRestorePrimaryCopies_RestoresAfterOwnerDeparture guards the restore
// itself: once the primary owner of a partition is gone and the delay has
// passed, the member holding its backup copy copies it to the new owner,
// keeps its own copy, leaves the schedule and does not copy it again.
func TestRestorePrimaryCopies_RestoresAfterOwnerDeparture(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, _, _, frags := restoreFixture(t, cluster, true, time.Millisecond)
	scheduleDeparture(b1, frags)
	for partID := range frags {
		require.NotZero(t, b1.restoreDueAt[partID], "partition %d must be scheduled", partID)
	}

	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Equal(t, 1, frag.replicated(), "partition %d must be restored", partID)
		require.Equal(t, 1, frag.Stats().Length, "the backup copy must be kept")
		require.Zero(t, b1.restoreDueAt[partID], "a restored partition leaves the schedule")
	}

	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Equal(t, 1, frag.replicated(), "partition %d must not be restored twice", partID)
	}
}

// TestRestorePrimaryCopies_WaitsForDelay guards the delay: a scheduled
// partition is not restored before ReplicaRestoreDelay has passed since its
// owner departed.
func TestRestorePrimaryCopies_WaitsForDelay(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, _, _, frags := restoreFixture(t, cluster, true, time.Hour)
	scheduleDeparture(b1, frags)
	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Zero(t, frag.replicated(), "partition %d must not be restored before the delay", partID)
		require.NotZero(t, b1.restoreDueAt[partID], "the schedule is kept")
	}
}

// TestRestorePrimaryCopies_DisabledWithoutProactiveSync guards the opt-in:
// with EnableProactiveSyncOnJoin off, owners are not tracked, nothing is
// scheduled and the default lazy repair stays.
func TestRestorePrimaryCopies_DisabledWithoutProactiveSync(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, _, _, frags := restoreFixture(t, cluster, false, time.Millisecond)
	scheduleDeparture(b1, frags)
	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Zero(t, frag.replicated(), "partition %d must not be restored with the flag off", partID)
		require.Zero(t, b1.restoreDueAt[partID])
	}
}

// TestRestorePrimaryCopies_OwnerChangeBetweenLiveMembersRestoresNothing
// guards that a partition whose previous owner drained its copy and is still
// a member is left to primaryCopies: the restore is scheduled, since the
// balancer cannot tell a drain from a death when the owner list changes, and
// dropped when due because the owner is still in the cluster.
func TestRestorePrimaryCopies_OwnerChangeBetweenLiveMembersRestoresNothing(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, _, _, frags := restoreFixture(t, cluster, true, time.Millisecond)
	b1.triggerBalancer()
	for partID := range frags {
		// As far as the balancer can tell, this live member drained the
		// partition to the current owner.
		b1.lastOwners[partID] = []uint64{b1.rt.This().ID}
	}

	b1.triggerBalancer()
	for partID := range frags {
		require.NotZero(t, b1.restoreDueAt[partID], "partition %d is scheduled until the owner's fate is known", partID)
	}

	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Zero(t, b1.restoreDueAt[partID], "partition %d: its previous owner is alive", partID)
		require.Zero(t, frag.replicated())
	}
}

// TestRestorePrimaryCopies_DepartureReplacesPendingDrain guards that a real
// departure scheduled while a drain by a live member is still pending is not
// hidden by it: the pending schedule is replaced and the restore happens.
func TestRestorePrimaryCopies_DepartureReplacesPendingDrain(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, _, _, frags := restoreFixture(t, cluster, true, time.Hour)
	b1.triggerBalancer()
	for partID := range frags {
		b1.lastOwners[partID] = []uint64{b1.rt.This().ID}
	}

	b1.triggerBalancer()
	for partID := range frags {
		require.Equal(t, b1.rt.This().ID, b1.departedOwner[partID], "partition %d: the drain is pending", partID)
	}

	// The owner list changes again, this time because the owner died.
	b1.config.ReplicaRestoreDelay = time.Millisecond
	for partID := range frags {
		b1.lastOwners[partID] = []uint64{0xDEAD}
	}

	b1.triggerBalancer()
	for partID := range frags {
		require.Equal(t, uint64(0xDEAD), b1.departedOwner[partID], "partition %d: the departure replaces the drain", partID)
	}

	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Equal(t, 1, frag.replicated(), "partition %d must be restored", partID)
		require.Zero(t, b1.restoreDueAt[partID])
	}
}

// TestRestorePrimaryCopies_FailedRestoreIsRetried guards that a restore
// whose copy failed stays scheduled and is attempted again by the next cycle.
func TestRestorePrimaryCopies_FailedRestoreIsRetried(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, e1, _, frags := restoreFixture(t, cluster, true, time.Millisecond)
	backup := e1.Get("backup").(*partitions.Partitions)
	for partID := range frags {
		frag := newFailingFragment(1)
		backup.PartitionByID(partID).Map().Store("dmap.test-data", frag)
		frags[partID] = frag
	}

	scheduleDeparture(b1, frags)
	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Equal(t, 1, frag.replicated(), "partition %d", partID)
		require.NotZero(t, b1.restoreDueAt[partID], "a failed restore stays scheduled")
	}

	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Equal(t, 2, frag.replicated(), "partition %d must be attempted again", partID)
		require.Zero(t, b1.restoreDueAt[partID])
	}
}

// TestRestorePrimaryCopies_SkipsPartitionsOwnedByThisMember checks that a
// backup copy of a partition this member owns as primary is neither restored
// anywhere nor relocated: promoteBackupCopies merges it, and until that
// succeeds the copy stays where it is.
func TestRestorePrimaryCopies_SkipsPartitionsOwnedByThisMember(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, e1, _, _ := restoreFixture(t, cluster, true, time.Millisecond)

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	backup := e1.Get("backup").(*partitions.Partitions)
	own := make(map[uint64]*failingFragment)
	for _, partID := range partitionsOwnedBy(c, primary, b1.rt.This()) {
		if primary.PartitionByID(partID).OwnerCount() != 1 {
			continue
		}

		// The promotion fails in the two cycles scheduleDeparture runs, so
		// the copy is still in the backup fragment when the rest of each
		// cycle runs.
		frag := newFailingFragment(2)
		backup.PartitionByID(partID).Map().Store("dmap.own-data", frag)
		own[partID] = frag
	}
	require.NotEmpty(t, own)

	scheduleDeparture(b1, own)

	// The failed promotions are the only attempts: backupCopies must not
	// move the sole copy off this member.
	for partID, frag := range own {
		require.Equal(t, 2, frag.attempts(), "partition %d: only the promotion is attempted", partID)
		require.Equal(t, 1, frag.Stats().Length, "partition %d: the copy stays on this member", partID)
	}

	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range own {
		require.Zero(t, frag.replicated(), "partition %d is owned by this member", partID)
		require.Zero(t, b1.restoreDueAt[partID], "an owned partition leaves the schedule")
		require.Zero(t, frag.Stats().Length, "the copy is promoted")
	}
}

// TestRestorePrimaryCopies_SkipsEmptyFragments checks that an empty fragment
// in a restored partition does not stop the restore of the fragments after
// it, and is not copied itself.
func TestRestorePrimaryCopies_SkipsEmptyFragments(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1, _, e1, _, frags := restoreFixture(t, cluster, true, time.Millisecond)

	backup := e1.Get("backup").(*partitions.Partitions)
	empties := make(map[uint64]*mockfragment.MockFragment, len(frags))
	for partID := range frags {
		empty := mockfragment.New()
		// Stored under a name that sorts before the filled fragment so the
		// empty one is met first when the partition map is ranged.
		backup.PartitionByID(partID).Map().Store("dmap.a-empty", empty)
		empties[partID] = empty
	}

	scheduleDeparture(b1, frags)
	time.Sleep(5 * time.Millisecond)
	b1.triggerBalancer()

	for partID, frag := range frags {
		require.Equal(t, 1, frag.replicated(), "partition %d must still be restored", partID)
		require.Zero(t, b1.restoreDueAt[partID])
		require.Empty(t, empties[partID].Result(), "an empty fragment is not copied")
	}
}

// TestPushPrimaryToBackups_TargetsCurrentReplicaOwnersOnly guards that the
// proactive push goes to the current replica owners, the tail of the owner
// list, and not to a previous replica owner that is still draining.
func TestPushPrimaryToBackups_TargetsCurrentReplicaOwnersOnly(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.EnableProactiveSyncOnJoin = true
		return c
	}

	e1 := newTestEnvironment(newConfig())
	b1 := cluster.addNode(e1)
	b2 := cluster.addNode(newTestEnvironment(newConfig()))
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	backup := e1.Get("backup").(*partitions.Partitions)
	var target uint64
	require.Eventually(t, func() bool {
		for _, partID := range partitionsOwnedBy(c, primary, b1.rt.This()) {
			owners := backup.PartitionByID(partID).Owners()
			if len(owners) == 1 && owners[0].CompareByName(b2.rt.This()) {
				target = partID
				return true
			}
		}

		return false
	}, 10*time.Second, 50*time.Millisecond)

	frag := mockfragment.New()
	frag.Fill()
	primary.PartitionByID(target).Map().Store("dmap.test-data", frag)

	staleCfg := testutil.NewConfig()
	staleCfg.MemberlistConfig.Name = "127.0.0.1:1"
	stale := discovery.NewMember(staleCfg)
	backup.PartitionByID(target).SetOwners([]discovery.Member{stale, b2.rt.This()})

	b1.triggerBalancer()

	result := frag.Result()[partitions.BACKUP][target]
	require.Len(t, result.Owners, 1, "only the current replica owner is a target")
	require.True(t, result.Owners[0].CompareByName(b2.rt.This()))
	signature, _ := b1.rt.Version()
	require.Equal(t, signature, b1.pushedSignature[target])
}

// TestPushPrimaryToBackups_PushesOncePartitionHoldsData guards that the
// proactive push is per partition and tick-driven: a partition that holds
// data is pushed by the first cycle after a table is installed and marked
// done; one that is still empty then, because its data is being moved in, is
// left unmarked, so the periodic cycle that finds it filled pushes it, once.
func TestPushPrimaryToBackups_PushesOncePartitionHoldsData(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.EnableProactiveSyncOnJoin = true
		return c
	}

	e1 := newTestEnvironment(newConfig())
	b1 := cluster.addNode(e1)
	b2 := cluster.addNode(newTestEnvironment(newConfig()))
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	backup := e1.Get("backup").(*partitions.Partitions)
	var replicated []uint64
	require.Eventually(t, func() bool {
		replicated = replicated[:0]
		for _, partID := range partitionsOwnedBy(c, primary, b1.rt.This()) {
			for _, owner := range backup.PartitionByID(partID).Owners() {
				if owner.CompareByName(b2.rt.This()) {
					replicated = append(replicated, partID)
					break
				}
			}
		}

		return len(replicated) >= 2
	}, 10*time.Second, 50*time.Millisecond)

	filled, late := replicated[0], replicated[1]
	first := newFailingFragment(0)
	primary.PartitionByID(filled).Map().Store("dmap.test-data", first)
	signature, _ := b1.rt.Version()

	// The install-time cycle pushes the partition that holds data and leaves
	// the empty one unmarked.
	b1.BalanceEagerly()
	require.Equal(t, 1, first.attempts())
	require.Equal(t, signature, b1.pushedSignature[filled])
	require.Zero(t, b1.pushedSignature[late])

	// Data arrives later; the periodic cycle pushes it, and only it.
	second := newFailingFragment(0)
	primary.PartitionByID(late).Map().Store("dmap.test-data", second)
	b1.triggerBalancer()
	require.Equal(t, 1, second.attempts())
	require.Equal(t, signature, b1.pushedSignature[late])
	require.Equal(t, 1, first.attempts(), "a pushed partition is not pushed again for the same table")

	b1.triggerBalancer()
	require.Equal(t, 1, first.attempts())
	require.Equal(t, 1, second.attempts())
}

// TestRunBalance_SkipsEmptyFragmentsInMove checks that an empty fragment in a
// partition handed to another member is skipped while the filled fragments
// of the same partition are moved.
func TestRunBalance_SkipsEmptyFragmentsInMove(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	e1 := newTestEnvironment(nil)
	b1 := cluster.addNode(e1)

	c := e1.Get("config").(*config.Config)
	primary := e1.Get("primary").(*partitions.Partitions)
	empties := make(map[uint64]*mockfragment.MockFragment, c.PartitionCount)
	for partID := range c.PartitionCount {
		frag := mockfragment.New()
		frag.Fill()
		primary.PartitionByID(partID).Map().Store("dmap.test-data", frag)

		empty := mockfragment.New()
		primary.PartitionByID(partID).Map().Store("dmap.a-empty", empty)
		empties[partID] = empty
	}

	b2 := cluster.addNode(newTestEnvironment(nil))
	require.NoError(t, testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if !b2.rt.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}

		return nil
	}))

	require.Eventually(t, func() bool {
		return len(partitionsOwnedBy(c, primary, b2.rt.This())) > 0
	}, 10*time.Second, 50*time.Millisecond)

	b1.triggerBalancer()

	for _, partID := range partitionsOwnedBy(c, primary, b2.rt.This()) {
		require.Zero(t, primary.PartitionByID(partID).Length(), "the filled fragment must have been moved")
		require.Empty(t, empties[partID].Result(), "an empty fragment is not moved")
	}
}

// TestTryAckRebalance_StaleAckReportsSupersededEpoch checks that an ack the
// coordinator reports as stale makes the cycle re-run against the current
// table instead of recording the generation as acked.
func TestTryAckRebalance_StaleAckReportsSupersededEpoch(t *testing.T) {
	cluster := newMockCluster(t)
	cluster.noLoop = true
	defer cluster.shutdown()

	b1 := cluster.addNode(newTestEnvironment(nil))
	sign, generation := b1.rt.Version()
	acked := b1.lastAckedGeneration

	require.True(t, b1.tryAckRebalance(sign+1, generation+1), "an epoch the coordinator does not know is stale")
	require.Equal(t, acked, b1.lastAckedGeneration, "a stale ack records nothing")
}
