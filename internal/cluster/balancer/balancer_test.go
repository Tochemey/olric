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

	err = b.Start()
	if err != nil {
		require.NoError(mc.t, err)
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

	// Signature 0 must be a no-op (no panic, lastAckedSignature stays 0).
	b.tryAckRebalance(0)
	require.Equal(t, uint64(0), b.lastAckedSignature)

	// When syncState reports pending data, ack is skipped.
	pending := syncstate.New()
	pending.Reset([]uint64{1})
	b.syncState = pending
	require.False(t, b.syncState.PendingEmpty())
	b.tryAckRebalance(1234)
	require.Equal(t, uint64(0), b.lastAckedSignature)

	// When the context is cancelled, ack is skipped even with empty pending.
	b.syncState = syncstate.New()
	b.cancel()
	b.tryAckRebalance(5678)
	require.Equal(t, uint64(0), b.lastAckedSignature)
}

// TestBalanceEagerly exercises the proactive node-join balance path including
// pushPrimaryToBackups by filling the primary partitions and triggering an
// eager balance on a two-node cluster with backups enabled.
func TestBalanceEagerly(t *testing.T) {
	cluster := newMockCluster(t)
	defer cluster.shutdown()

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
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
	e1 := newTestEnvironment(c1)
	b1 := cluster.addNode(e1)

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

	moved, aborted := b.promoteBackupCopies(b.rt.Signature())
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
	moved, aborted := b.pushPrimaryToBackups(b.rt.Signature())
	require.False(t, moved)
	require.False(t, aborted)
}
