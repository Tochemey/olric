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
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/testutil"

	"github.com/tochemey/olric/internal/cluster/partitions"
)

func newRoutingTableForTest(c *config.Config, srv *server.Server) *RoutingTable {
	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", server.NewClient(c.Client))
	e.Set("server", srv)

	rt := New(e)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-srv.StartedCtx.Done()

	return rt
}

type testCluster struct {
	peerPorts []int
	errGr     errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
}

func newTestCluster() *testCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &testCluster{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *testCluster) addNode(c *config.Config) (*RoutingTable, error) {
	if c == nil {
		c = testutil.NewConfig()
	}
	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range t.peerPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	err = rt.Join()
	if err != nil {
		return nil, err
	}
	err = rt.Start()
	if err != nil {
		return nil, err
	}

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return srv.Shutdown(context.Background())
	})

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return rt.Shutdown(context.Background())
	})

	t.peerPorts = append(t.peerPorts, port)
	return rt, err
}

func (t *testCluster) shutdown() error {
	t.cancel()
	return t.errGr.Wait()
}

func TestRoutingTable_IsJoined(t *testing.T) {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	// The join has not been attempted yet.
	require.False(t, rt.IsJoined())

	require.NoError(t, rt.Join())
	require.True(t, rt.IsJoined())

	require.NoError(t, rt.Start())
	defer func() {
		require.NoError(t, rt.Shutdown(context.Background()))
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	require.True(t, rt.IsJoined())
}

func TestRoutingTable_SingleNode(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		c := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt, err := cluster.addNode(c)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt.This().CompareByID(rt.Discovery().GetCoordinator()) {
			t.Fatalf("Coordinator is different")
		}

		if !rt.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		for partID := uint64(0); partID < c.PartitionCount; partID++ {
			part := rt.primary.PartitionByID(partID)
			if !part.Owner().CompareByID(rt.This()) {
				t.Fatalf("PartID: %d has a different owner", partID)
			}
		}

		if rt.Signature() == 0 {
			t.Fatalf("routingTable.signature is zero")
		}

		if rt.OwnedPartitionCount() != c.PartitionCount {
			t.Fatalf("Expected owned partition count: %d. Got: %d", rt.OwnedPartitionCount(), c.PartitionCount)
		}
	})
	t.Run("Without TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		c := testutil.NewConfig()
		rt, err := cluster.addNode(c)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt.This().CompareByID(rt.Discovery().GetCoordinator()) {
			t.Fatalf("Coordinator is different")
		}

		if !rt.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		for partID := uint64(0); partID < c.PartitionCount; partID++ {
			part := rt.primary.PartitionByID(partID)
			if !part.Owner().CompareByID(rt.This()) {
				t.Fatalf("PartID: %d has a different owner", partID)
			}
		}

		if rt.Signature() == 0 {
			t.Fatalf("routingTable.signature is zero")
		}

		if rt.OwnedPartitionCount() != c.PartitionCount {
			t.Fatalf("Expected owned partition count: %d. Got: %d", rt.OwnedPartitionCount(), c.PartitionCount)
		}
	})
}

func TestRoutingTable_Cluster(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		firstSignature := rt1.Signature()

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt2.Discovery().GetCoordinator().CompareByID(rt1.Discovery().GetCoordinator()) {
			t.Fatalf("Coordinator is different")
		}

		if firstSignature == rt2.Signature() {
			t.Fatalf("routingTable signature did not changed after node join")
		}

		if rt1.OwnedPartitionCount() == c1.PartitionCount {
			t.Fatalf("rt1 has all the partitions")
		}

		if rt2.OwnedPartitionCount() == c2.PartitionCount {
			t.Fatalf("rt2 has all the partitions")
		}

		totalPartitionCount := rt1.OwnedPartitionCount() + rt2.OwnedPartitionCount()
		if totalPartitionCount != c1.PartitionCount {
			t.Fatalf("Total partition count is wrong: %d", totalPartitionCount)
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfig()
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		firstSignature := rt1.Signature()

		c2 := testutil.NewConfig()
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt2.Discovery().GetCoordinator().CompareByID(rt1.Discovery().GetCoordinator()) {
			t.Fatalf("Coordinator is different")
		}

		if firstSignature == rt2.Signature() {
			t.Fatalf("routingTable signature did not changed after node join")
		}

		if rt1.OwnedPartitionCount() == c1.PartitionCount {
			t.Fatalf("rt1 has all the partitions")
		}

		if rt2.OwnedPartitionCount() == c2.PartitionCount {
			t.Fatalf("rt2 has all the partitions")
		}

		totalPartitionCount := rt1.OwnedPartitionCount() + rt2.OwnedPartitionCount()
		if totalPartitionCount != c1.PartitionCount {
			t.Fatalf("Total partition count is wrong: %d", totalPartitionCount)
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

func TestRoutingTable_CheckPartitionOwnership(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		for partID := uint64(0); partID < c1.PartitionCount; partID++ {
			ownerOne := rt1.primary.PartitionByID(partID).Owner()
			ownerTwo := rt2.primary.PartitionByID(partID).Owner()
			if !ownerOne.CompareByID(ownerTwo) {
				t.Fatalf("Different partition: %d owner: %s != %s", partID, ownerOne, ownerTwo)
			}
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfig()
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		c2 := testutil.NewConfig()
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		for partID := uint64(0); partID < c1.PartitionCount; partID++ {
			ownerOne := rt1.primary.PartitionByID(partID).Owner()
			ownerTwo := rt2.primary.PartitionByID(partID).Owner()
			if !ownerOne.CompareByID(ownerTwo) {
				t.Fatalf("Different partition: %d owner: %s != %s", partID, ownerOne, ownerTwo)
			}
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

func TestRoutingTable_NodeLeave(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		signatureWithTwoNode := rt1.Signature()
		err = rt1.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
			if rt2.Signature() == signatureWithTwoNode {
				return errors.New("still has the same signature")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt2.Discovery().GetCoordinator().CompareByID(rt2.This()) {
			t.Fatalf("Coordinator is different")
		}

		for partID := uint64(0); partID < c2.PartitionCount; partID++ {
			part := rt2.primary.PartitionByID(partID)
			if !part.Owner().CompareByID(rt2.This()) {
				t.Fatalf("PartID: %d has a different owner", partID)
			}
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfig()
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		c2 := testutil.NewConfig()
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		signatureWithTwoNode := rt1.Signature()
		err = rt1.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
			if rt2.Signature() == signatureWithTwoNode {
				return errors.New("still has the same signature")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt2.Discovery().GetCoordinator().CompareByID(rt2.This()) {
			t.Fatalf("Coordinator is different")
		}

		for partID := uint64(0); partID < c2.PartitionCount; partID++ {
			part := rt2.primary.PartitionByID(partID)
			if !part.Owner().CompareByID(rt2.This()) {
				t.Fatalf("PartID: %d has a different owner", partID)
			}
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

// The NodeLeave branch of processClusterEvent must hand the departed member's
// name to the registered node-leave callbacks: the member relies on this to
// evict the address from connection pools the routing table does not own,
// such as the embedded cluster client's.
func TestRoutingTable_NodeLeave_FiresNodeLeaveCallbacks(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt1, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)
	require.True(t, rt1.IsBootstrapped())

	rt2, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)
	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	var mtx sync.Mutex
	var left []string
	rt2.AddNodeLeaveCallback(func(nodeName string) {
		mtx.Lock()
		defer mtx.Unlock()
		left = append(left, nodeName)
	})

	rt1Name := rt1.This().String()
	require.NoError(t, rt1.Shutdown(context.Background()))

	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, name := range left {
			if name == rt1Name {
				return nil
			}
		}
		return errors.New("the node-leave callback has not fired for the departed member")
	})
	require.NoError(t, err)
}

// The NodeJoin branch of processClusterEvent must hand the joined member's
// name to the registered node-join callbacks: the member relies on this to
// lift the ban a node-leave callback placed on connection pools the routing
// table does not own.
func TestRoutingTable_NodeJoin_FiresNodeJoinCallbacks(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt1, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)
	require.True(t, rt1.IsBootstrapped())

	var mtx sync.Mutex
	var joined []string
	rt1.AddNodeJoinCallback(func(nodeName string) {
		mtx.Lock()
		defer mtx.Unlock()
		joined = append(joined, nodeName)
	})

	rt2, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)
	rt2Name := rt2.This().String()

	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, name := range joined {
			if name == rt2Name {
				return nil
			}
		}
		return errors.New("the node-join callback has not fired for the joined member")
	})
	require.NoError(t, err)
}

// The NodeUpdate branch must fire the node-join callbacks: an update proves
// the member is alive, so any ban placed on its address has to be lifted.
func TestRoutingTable_NodeUpdate_FiresNodeJoinCallbacks(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt1, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)
	require.True(t, rt1.IsBootstrapped())

	c2 := testutil.NewConfig()
	rt2, err := cluster.addNode(c2)
	require.NoError(t, err)
	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	var mtx sync.Mutex
	var confirmed []string
	rt1.AddNodeJoinCallback(func(nodeName string) {
		mtx.Lock()
		defer mtx.Unlock()
		confirmed = append(confirmed, nodeName)
	})

	// Deliver a fabricated NodeUpdate for rt2 to rt1, the same way memberlist
	// gossip would. See TestRoutingTable_NodeUpdate for the copy rationale.
	n := rt2.Discovery().LocalNode()
	meta, err := discovery.NewMember(c2).Encode()
	require.NoError(t, err)
	updated := *n
	updated.Meta = meta
	event := memberlist.NodeEvent{Event: memberlist.NodeUpdate, Node: &updated}
	rt1.Discovery().ClusterEvents <- discovery.ToClusterEvent(event)

	rt2Name := rt2.This().String()
	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, name := range confirmed {
			if name == rt2Name {
				return nil
			}
		}
		return errors.New("the node-join callback has not fired for the updated member")
	})
	require.NoError(t, err)
}

func TestRoutingTable_NodeUpdate(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		c1 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		c2 := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		n := rt2.Discovery().LocalNode()
		meta, err := discovery.NewMember(c2).Encode()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		// Mutate a copy: LocalNode returns memberlist's live object, and
		// writing its Meta races with concurrent metadata reads. The event is
		// delivered to both nodes' event channels directly — the same way
		// memberlist gossip would fan it out — so each routing table processes
		// the NodeUpdate.
		updated := *n
		updated.Meta = meta
		event := memberlist.NodeEvent{Event: memberlist.NodeUpdate, Node: &updated}
		rt2.Discovery().ClusterEvents <- discovery.ToClusterEvent(event)
		rt1.Discovery().ClusterEvents <- discovery.ToClusterEvent(event)

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			rt1.Members().RLock()
			_, err = rt1.Members().Get(rt2.This().ID)
			rt1.Members().RUnlock()
			if err == nil {
				// node id is updated.
				return errors.New("rt2 could not be updated")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster()
		defer cluster.cancel()

		c1 := testutil.NewConfig()
		rt1, err := cluster.addNode(c1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if !rt1.IsBootstrapped() {
			t.Fatalf("The coordinator node cannot be bootstrapped")
		}

		c2 := testutil.NewConfig()
		rt2, err := cluster.addNode(c2)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			if !rt2.IsBootstrapped() {
				return errors.New("the second node cannot be bootstrapped")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		n := rt2.Discovery().LocalNode()
		meta, err := discovery.NewMember(c2).Encode()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		// Mutate a copy: LocalNode returns memberlist's live object, and
		// writing its Meta races with concurrent metadata reads. The event is
		// delivered to both nodes' event channels directly — the same way
		// memberlist gossip would fan it out — so each routing table processes
		// the NodeUpdate.
		updated := *n
		updated.Meta = meta
		event := memberlist.NodeEvent{Event: memberlist.NodeUpdate, Node: &updated}
		rt2.Discovery().ClusterEvents <- discovery.ToClusterEvent(event)
		rt1.Discovery().ClusterEvents <- discovery.ToClusterEvent(event)

		err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
			rt1.Members().RLock()
			_, err = rt1.Members().Get(rt2.This().ID)
			rt1.Members().RUnlock()
			if err == nil {
				// node id is updated.
				return errors.New("rt2 could not be updated")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		err = cluster.shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

func TestRoutingTable_ConcurrentStartupRebalanceAck(t *testing.T) {
	const nodeCount = 3

	nodes, servers, err := startConcurrentCluster(nodeCount)
	require.NoError(t, err)
	defer shutdownCluster(nodes, servers)

	require.NoError(t, waitForClusterMembers(nodes, nodeCount, 5*time.Second))

	// The active epoch can be superseded between reading it and the acks
	// arriving (a late gossip update starts a new rebalance, which discards
	// acks sent for the previous epoch). Retry the ack cycle against the
	// current epoch until one completes.
	deadline := time.Now().Add(10 * time.Second)
	for {
		coordinator, epoch, err := waitForActiveEpoch(nodes, 5*time.Second)
		require.NoError(t, err)

		var ackErr error
		for _, rt := range nodes {
			if err := rt.SendRebalanceAck(epoch); err != nil {
				ackErr = err
				break
			}
		}
		if ackErr == nil {
			if err := waitForRebalanceComplete(coordinator, epoch, 2*time.Second); err == nil {
				return
			}
		}
		require.Truef(t, time.Now().Before(deadline),
			"rebalance did not complete for any epoch; last ack error: %v", ackErr)
	}
}

func TestRoutingTable_RebalanceCompletesWhenNodesLeave(t *testing.T) {
	// This test validates the critical requirement: "node departures don't block rebalance completion"
	// It ensures that when nodes leave during rebalance, completion is based on live members only.
	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadRepair = true
		c.ReadQuorum = 1
		c.EnableClusterEventsChannel = true
		return c
	}

	cluster := newTestCluster()
	defer cluster.shutdown()

	// Start with 3 nodes
	rt1, err := cluster.addNode(newConfig())
	require.NoError(t, err)
	rt2, err := cluster.addNode(newConfig())
	require.NoError(t, err)
	rt3, err := cluster.addNode(newConfig())
	require.NoError(t, err)

	t.Log("Wait for cluster to stabilize")
	<-time.After(2 * time.Second)

	// Get the coordinator
	coordinator := rt1
	if !rt1.Discovery().IsCoordinator() {
		if rt2.Discovery().IsCoordinator() {
			coordinator = rt2
		} else {
			coordinator = rt3
		}
	}

	// Capture the initial signature (epoch)
	initialSignature := coordinator.Signature()
	t.Logf("Initial signature: %d", initialSignature)

	// Add a 4th node - this will trigger a new rebalance epoch
	t.Log("Adding 4th node to trigger rebalance")
	rt4, err := cluster.addNode(newConfig())
	require.NoError(t, err)

	// Wait for rebalance epoch to start and capture it
	var rebalanceEpoch uint64
	err = func() error {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			epoch, pendingCount, _, completed := coordinator.getRebalanceState()
			if pendingCount == 4 && !completed {
				rebalanceEpoch = epoch
				t.Logf("Rebalance epoch started: %d with %d pending members", epoch, pendingCount)
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
		return fmt.Errorf("timeout waiting for rebalance epoch to start")
	}()
	require.NoError(t, err, "Rebalance epoch should start after adding node")

	// Immediately kill one of the original nodes (rt2) before it can ACK
	// This simulates a node leaving during rebalance
	t.Logf("Terminating node %s before it can ACK", rt2.This())
	require.NoError(t, rt2.Shutdown(context.Background()))

	// Wait for node leave event to propagate
	<-time.After(2 * time.Second)

	// Verify rt2 is no longer in the members list
	coordinator.Members().RLock()
	_, err = coordinator.Members().Get(rt2.This().ID)
	coordinator.Members().RUnlock()
	require.Error(t, err, "rt2 should be removed from members list")

	// Get the current epoch (might be the same or a new one if node leave triggered rebalance)
	currentEpoch, _, _, _ := coordinator.getRebalanceState()
	t.Logf("Current epoch after node leave: %d (started with: %d)", currentEpoch, rebalanceEpoch)

	// Manually send ACKs from the remaining live nodes (rt1, rt3, rt4)
	// The routingtable test setup doesn't have a balancer running, so we need to send ACKs manually
	// This simulates what the balancer would do automatically
	t.Log("Sending ACKs from remaining live nodes")
	remainingNodes := []*RoutingTable{rt1, rt3, rt4}
	for _, rt := range remainingNodes {
		if rt != nil {
			err = rt.SendRebalanceAck(currentEpoch)
			// Ignore errors if epoch changed (new rebalance started) - we'll retry with new epoch
			if err != nil {
				// If epoch changed, get the new epoch and retry
				newEpoch, _, _, _ := coordinator.getRebalanceState()
				if newEpoch != currentEpoch {
					t.Logf("Epoch changed from %d to %d, retrying ACKs", currentEpoch, newEpoch)
					currentEpoch = newEpoch
					err = rt.SendRebalanceAck(currentEpoch)
				}
				if err != nil {
					t.Logf("Node %s ACK error: %v", rt.This(), err)
				}
			}
		}
	}

	// Wait for rebalance to complete - it should complete with only the 3 remaining live nodes
	// This validates that departed nodes don't block completion
	// Use the current epoch (which might be rebalanceEpoch or a newer one)
	t.Log("Waiting for rebalance to complete with remaining live nodes")
	err = waitForRebalanceComplete(coordinator, currentEpoch, 15*time.Second)
	require.NoError(t, err, "Rebalance should complete even though rt2 left before ACKing")

	// Verify completion state
	_, _, ackedCount, completed := coordinator.getRebalanceState()

	require.True(t, completed, "Rebalance should be marked as completed")
	t.Logf("Rebalance completed with %d ACKs (expected 3, rt2 left before ACKing)", ackedCount)
	require.Equal(t, 3, ackedCount, "Should have ACKs from 3 live nodes (rt1, rt3, rt4)")
}

func startConcurrentCluster(nodeCount int) ([]*RoutingTable, []*server.Server, error) {
	configs := make([]*config.Config, nodeCount)
	memberPorts := make([]int, nodeCount)

	for i := range nodeCount {
		c := testutil.NewConfig()
		c.JoinRetryInterval = 50 * time.Millisecond
		c.MaxJoinAttempts = 100

		memberPort, err := testutil.GetFreePort()
		if err != nil {
			return nil, nil, err
		}
		c.MemberlistConfig.BindPort = memberPort

		configs[i] = c
		memberPorts[i] = memberPort
	}

	for i := range nodeCount {
		var peers []string
		for j := range nodeCount {
			if i == j {
				continue
			}
			peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(memberPorts[j])))
		}
		configs[i].Peers = peers
	}

	nodes := make([]*RoutingTable, nodeCount)
	servers := make([]*server.Server, nodeCount)
	var mtx sync.Mutex
	var g errgroup.Group

	for i := range nodeCount {
		g.Go(func() error {
			srv := testutil.NewServer(configs[i])
			rt := newRoutingTableForTest(configs[i], srv)
			if err := rt.Join(); err != nil {
				return err
			}
			if err := rt.Start(); err != nil {
				return err
			}
			mtx.Lock()
			nodes[i] = rt
			servers[i] = srv
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		shutdownCluster(nodes, servers)
		return nil, nil, err
	}
	return nodes, servers, nil
}

func shutdownCluster(nodes []*RoutingTable, servers []*server.Server) {
	for _, rt := range nodes {
		if rt == nil {
			continue
		}
		_ = rt.Shutdown(context.Background())
	}
	for _, srv := range servers {
		if srv == nil {
			continue
		}
		_ = srv.Shutdown(context.Background())
	}
}

func waitForClusterMembers(nodes []*RoutingTable, expected int, timeout time.Duration) error {
	return waitForCondition(timeout, func() bool {
		for _, rt := range nodes {
			if rt == nil {
				return false
			}
			if rt.NumMembers() != int32(expected) {
				return false
			}
			if membersLen(rt) != expected {
				return false
			}
		}
		return true
	})
}

func waitForActiveEpoch(nodes []*RoutingTable, timeout time.Duration) (*RoutingTable, uint64, error) {
	var coordinator *RoutingTable
	var epoch uint64

	err := waitForCondition(timeout, func() bool {
		coordinator = nil
		epoch = 0

		var coordID uint64
		for _, rt := range nodes {
			if rt == nil {
				return false
			}
			coord := rt.Discovery().GetCoordinator()
			if coord.ID == 0 {
				return false
			}
			if coordID == 0 {
				coordID = coord.ID
			}
			if coord.ID != coordID {
				return false
			}
			if rt.This().CompareByID(coord) {
				coordinator = rt
			}
		}
		if coordinator == nil {
			return false
		}

		signature := coordinator.Signature()
		if signature == 0 {
			return false
		}

		for _, rt := range nodes {
			if rt.Signature() != signature {
				return false
			}
		}

		coordinator.rebalanceMtx.Lock()
		epoch = coordinator.rebalanceState.epoch
		coordinator.rebalanceMtx.Unlock()
		if epoch != signature {
			return false
		}
		return true
	})

	if err != nil {
		return nil, 0, err
	}
	return coordinator, epoch, nil
}

func waitForRebalanceComplete(rt *RoutingTable, epoch uint64, timeout time.Duration) error {
	return waitForCondition(timeout, func() bool {
		rt.rebalanceMtx.Lock()
		defer rt.rebalanceMtx.Unlock()
		return rt.rebalanceState.epoch == epoch && rt.rebalanceState.completed
	})
}

// getRebalanceState is a test helper that returns the current rebalance state.
func (r *RoutingTable) getRebalanceState() (epoch uint64, pendingCount int, ackedCount int, completed bool) {
	r.rebalanceMtx.Lock()
	defer r.rebalanceMtx.Unlock()
	return r.rebalanceState.epoch, len(r.rebalanceState.pending), len(r.rebalanceState.acked), r.rebalanceState.completed
}

func waitForCondition(timeout time.Duration, f func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if f() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout while waiting for condition")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func membersLen(rt *RoutingTable) int {
	rt.Members().RLock()
	defer rt.Members().RUnlock()
	return rt.Members().Length()
}

func TestRoutingTable_RejoinLoop_NotStartedWithDefaultQuorum(t *testing.T) {
	// MemberCountQuorum defaults to 1 (MinimumMemberCountQuorum).
	// The rejoin loop must not be started in this case — it is only
	// meaningful when quorum > 1 because a single-node cluster is always
	// trivially above quorum.
	cluster := newTestCluster()
	defer cluster.shutdown()

	c := testutil.NewConfig()
	// MemberCountQuorum == 1 (default) → loop guard fires, no goroutine started.
	rt, err := cluster.addNode(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !rt.IsBootstrapped() {
		t.Fatalf("Node should be bootstrapped")
	}
	if rt.CheckMemberCountQuorum() != nil {
		t.Fatalf("Expected quorum to be satisfied with one member and quorum=1")
	}
}

func TestRoutingTable_RejoinLoop_QuorumSatisfiedNoSpuriousRejoin(t *testing.T) {
	// With MemberCountQuorum=2 and two live nodes the quorum check in
	// rejoinLoop returns nil, so Join is never called. Verify the cluster
	// remains stable after the loop has had time to tick several times.
	//
	// Both nodes must be started concurrently because Start() blocks until
	// MemberCountQuorum is satisfied; with quorum=2, neither node can
	// complete Start() on its own.
	newCfg := func(memberPort int, peers []string) *config.Config {
		c := testutil.NewConfig()
		c.MemberCountQuorum = 2
		c.RejoinInterval = 50 * time.Millisecond
		c.MemberlistConfig.BindPort = memberPort
		c.Peers = peers
		return c
	}

	port1, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort: %v", err)
	}
	port2, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort: %v", err)
	}

	c1 := newCfg(port1, []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port2))})
	c2 := newCfg(port2, []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port1))})

	srv1 := testutil.NewServer(c1)
	srv2 := testutil.NewServer(c2)
	rt1 := newRoutingTableForTest(c1, srv1)
	rt2 := newRoutingTableForTest(c2, srv2)

	if err := rt1.Join(); err != nil {
		t.Fatalf("rt1.Join: %v", err)
	}
	if err := rt2.Join(); err != nil {
		t.Fatalf("rt2.Join: %v", err)
	}

	var g errgroup.Group
	g.Go(func() error { return rt1.Start() })
	g.Go(func() error { return rt2.Start() })
	if err := g.Wait(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	defer func() {
		_ = rt1.Shutdown(context.Background())
		_ = rt2.Shutdown(context.Background())
		_ = srv1.Shutdown(context.Background())
		_ = srv2.Shutdown(context.Background())
	}()

	// Let the rejoin loop tick several times with quorum satisfied.
	time.Sleep(300 * time.Millisecond)

	if rt1.CheckMemberCountQuorum() != nil {
		t.Fatalf("rt1: quorum should be satisfied with two members")
	}
	if rt2.CheckMemberCountQuorum() != nil {
		t.Fatalf("rt2: quorum should be satisfied with two members")
	}
	if rt1.NumMembers() != 2 {
		t.Fatalf("Expected 2 members on rt1. Got: %d", rt1.NumMembers())
	}
	if rt2.NumMembers() != 2 {
		t.Fatalf("Expected 2 members on rt2. Got: %d", rt2.NumMembers())
	}
}

func TestRoutingTable_RejoinLoop_RejoinsAfterPartitionHeals(t *testing.T) {
	// Scenario:
	//   1. node1 and node2 form a 2-node cluster (MemberCountQuorum=2).
	//   2. node1 is shut down — node2 drops below quorum.
	//   3. node1 is restarted at the SAME memberlist address with no peers
	//      configured, so it cannot contact node2 on its own.
	//   4. node2's rejoin loop calls discovery.Join() → memberlist.Join(peers)
	//      where peers includes node1's address. This reconnects the two nodes.
	//   5. node2 regains quorum.

	// Pre-allocate fixed ports for node1 so node2 can reference them in
	// its peers list before node1 is restarted.
	node1MemberPort, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort: %v", err)
	}
	node1TCPPort, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort: %v", err)
	}

	// Build node1 config with the pre-allocated ports.
	c1 := testutil.NewConfig()
	c1.MemberCountQuorum = 2
	c1.RejoinInterval = 100 * time.Millisecond
	c1.MemberlistConfig.BindPort = node1MemberPort
	c1.BindAddr = "127.0.0.1"
	c1.BindPort = node1TCPPort
	c1.MemberlistConfig.Name = net.JoinHostPort("127.0.0.1", strconv.Itoa(node1TCPPort))

	// node2 knows about node1's memberlist address.
	c2 := testutil.NewConfig()
	c2.MemberCountQuorum = 2
	c2.RejoinInterval = 100 * time.Millisecond
	c2.Peers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(node1MemberPort))}

	srv1 := testutil.NewServer(c1)
	srv2 := testutil.NewServer(c2)
	rt1 := newRoutingTableForTest(c1, srv1)
	rt2 := newRoutingTableForTest(c2, srv2)

	if err := rt1.Join(); err != nil {
		t.Fatalf("rt1.Join: %v", err)
	}
	if err := rt2.Join(); err != nil {
		t.Fatalf("rt2.Join: %v", err)
	}

	// Both nodes must start concurrently: Start() blocks until quorum is met.
	var startG errgroup.Group
	startG.Go(func() error { return rt1.Start() })
	startG.Go(func() error { return rt2.Start() })
	if err := startG.Wait(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for both nodes to see each other.
	if err := waitForClusterMembers([]*RoutingTable{rt1, rt2}, 2, 5*time.Second); err != nil {
		t.Fatalf("Cluster did not stabilise with 2 members: %v", err)
	}

	// Partition: shut down node1.
	if err := rt1.Shutdown(context.Background()); err != nil {
		t.Fatalf("rt1.Shutdown: %v", err)
	}
	if err := srv1.Shutdown(context.Background()); err != nil {
		t.Fatalf("srv1.Shutdown: %v", err)
	}

	// Verify node2 is now below quorum.
	if err := waitForCondition(5*time.Second, func() bool {
		return rt2.CheckMemberCountQuorum() != nil
	}); err != nil {
		t.Fatalf("node2 should be below quorum after node1 left: %v", err)
	}

	// Allow OS to release the ports before rebinding.
	time.Sleep(200 * time.Millisecond)

	// Heal: restart node1 at the SAME addresses, but with NO peers — it
	// cannot reach node2 on its own. Only node2's rejoin loop can reconnect
	// the two nodes.
	c1New := testutil.NewConfig()
	c1New.MemberCountQuorum = 2
	c1New.RejoinInterval = 100 * time.Millisecond
	c1New.MemberlistConfig.BindPort = node1MemberPort
	c1New.BindAddr = "127.0.0.1"
	c1New.BindPort = node1TCPPort
	c1New.MemberlistConfig.Name = c1.MemberlistConfig.Name
	c1New.Peers = []string{} // intentionally empty — rejoin loop on node2 must do the work

	srv1New := testutil.NewServer(c1New)
	rt1New := newRoutingTableForTest(c1New, srv1New)
	if err := rt1New.Join(); err != nil {
		t.Fatalf("rt1New.Join: %v", err)
	}

	// Start rt1New in a goroutine: its Start() blocks until MemberCountQuorum=2
	// is met. node2's rejoin loop will call discovery.Join() which contacts
	// node1New's memberlist address; gossip then syncs the two nodes, allowing
	// both Start() calls to proceed.
	startErrCh := make(chan error, 1)
	go func() { startErrCh <- rt1New.Start() }()

	defer func() {
		_ = rt1New.Shutdown(context.Background())
		_ = srv1New.Shutdown(context.Background())
		_ = rt2.Shutdown(context.Background())
		_ = srv2.Shutdown(context.Background())
	}()

	// node2's rejoin loop calls discovery.Join() → memberlist.Join([node1_addr]).
	// node1New is listening; gossip syncs; node2 fires NodeJoin; quorum restored.
	if err := waitForCondition(10*time.Second, func() bool {
		return rt2.CheckMemberCountQuorum() == nil
	}); err != nil {
		t.Fatalf("node2 did not regain quorum after node1 restarted: %v", err)
	}

	// Ensure rt1New.Start() also completed without error.
	select {
	case err := <-startErrCh:
		if err != nil {
			t.Fatalf("rt1New.Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("rt1New.Start did not complete in time")
	}

	if rt2.NumMembers() < 2 {
		t.Fatalf("Expected at least 2 members on node2 after rejoin. Got: %d", rt2.NumMembers())
	}
}

func TestSetNumMembersEagerly(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)

	rt.SetNumMembersEagerly(42)
	require.Equal(t, int32(42), rt.NumMembers())

	rt.SetNumMembersEagerly(7)
	require.Equal(t, int32(7), rt.NumMembers())
}

func TestCheckBootstrap_Bootstrapped(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	rt, err := cluster.addNode(nil)
	require.NoError(t, err)
	require.True(t, rt.IsBootstrapped())

	// A bootstrapped node returns immediately with no error.
	require.NoError(t, rt.CheckBootstrap())
}

func TestCheckBootstrap_Timeout(t *testing.T) {
	c := testutil.NewConfig()
	// Keep the wait short: the node is never bootstrapped so CheckBootstrap
	// must exhaust its interval loop and return an error.
	c.BootstrapTimeout = 250 * time.Millisecond

	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	defer func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	require.False(t, rt.IsBootstrapped())
	err = rt.CheckBootstrap()
	require.Error(t, err)
}

func TestCheckBootstrap_BootstrappedLater(t *testing.T) {
	c := testutil.NewConfig()
	c.BootstrapTimeout = 5 * time.Second

	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	defer func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	// The coordinator bootstraps the node while CheckBootstrap is polling.
	go func() {
		<-time.After(300 * time.Millisecond)
		rt.markBootstrapped()
	}()

	require.NoError(t, rt.CheckBootstrap())
}

func TestRoutingTable_Start_NotJoined(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	defer func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	require.ErrorIs(t, rt.Start(), ErrNotJoinedYet)
}

func TestRoutingTable_Join_DiscoveryStartError(t *testing.T) {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	require.NoError(t, err)

	// Occupy the memberlist TCP port: discovery cannot start.
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, ln.Close())
	}()

	c.MemberlistConfig.BindPort = port
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	defer func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	require.Error(t, rt.Join())
}

func TestRoutingTable_Start_BootstrapError(t *testing.T) {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	// The Olric server is intentionally NOT started: the coordinator cannot
	// push the initial routing table to itself.
	srv := testutil.NewServer(c)
	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	e.Set("client", server.NewClient(c.Client))
	e.Set("server", srv)
	rt := New(e)

	require.NoError(t, rt.Join())
	require.Error(t, rt.Start())
	require.NoError(t, rt.Shutdown(context.Background()))
}

func TestRoutingTable_Start_CanceledWhileWaitingQuorum(t *testing.T) {
	c := testutil.NewConfig()
	c.MemberCountQuorum = 3

	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	require.NoError(t, rt.Join())

	// A single node can never satisfy a quorum of three members. Cancel the
	// routing table context while Start is waiting for the quorum.
	go func() {
		<-time.After(250 * time.Millisecond)
		rt.cancel()
	}()

	require.Error(t, rt.Start())

	require.NoError(t, rt.discovery.Shutdown())
	require.NoError(t, srv.Shutdown(context.Background()))
}

func TestRoutingTable_Shutdown_ContextExpired(t *testing.T) {
	c := testutil.NewConfig()
	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	c.MemberlistConfig.BindPort = port

	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	require.NoError(t, rt.Join())
	require.NoError(t, rt.Start())

	// Occupy the wait group: Shutdown cannot finish before its context expires.
	release := make(chan struct{})
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		<-release
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.Error(t, rt.Shutdown(ctx))

	close(release)
	require.NoError(t, srv.Shutdown(context.Background()))
}

func TestRoutingTable_PushPeriodically(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	c := testutil.NewConfig()
	c.RoutingTablePushInterval = 50 * time.Millisecond
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	// Let the periodic push tick a few times.
	<-time.After(300 * time.Millisecond)
	require.True(t, rt.IsBootstrapped())
}

func TestProcessClusterEvent_EdgeCases(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)
	defer func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	}()

	meta, err := discovery.NewMember(testutil.NewConfig()).Encode()
	require.NoError(t, err)

	// A NodeLeave event for a member that was never registered is ignored.
	rt.processClusterEvent(&discovery.ClusterEvent{
		Event:    memberlist.NodeLeave,
		NodeName: "127.0.0.1:9999",
		NodeMeta: meta,
	})
	require.Zero(t, rt.Members().Length())

	// Events of an unknown type are ignored as well.
	rt.processClusterEvent(&discovery.ClusterEvent{
		Event:    memberlist.NodeEventType(42),
		NodeMeta: meta,
	})
	require.Zero(t, rt.Members().Length())
}

func TestRoutingTable_ProactiveSyncOnJoin(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.shutdown()

	newCfg := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.EnableProactiveSyncOnJoin = true
		return c
	}

	rt1, err := cluster.addNode(newCfg())
	require.NoError(t, err)

	var calls int32
	rt1.AddCallback(func() {
		atomic.AddInt32(&calls, 1)
	})

	_, err = cluster.addNode(newCfg())
	require.NoError(t, err)

	err = testutil.TryWithInterval(50, 100*time.Millisecond, func() error {
		if atomic.LoadInt32(&calls) == 0 {
			return errors.New("routing table callbacks did not run")
		}
		return nil
	})
	require.NoError(t, err)
}

// Regression test for the routing table commit aborting when any member is
// unreachable. A member whose RESP server is dead (but whose memberlist is
// still alive, e.g. a pod that just crashed) must not block the coordinator
// from committing routing updates: a joiner arriving while the dead member
// is still in the member list must receive a table and bootstrap.
func TestRoutingTable_CommitWithUnreachableMember(t *testing.T) {
	cluster := newTestCluster()
	defer func() {
		require.NoError(t, cluster.shutdown())
	}()

	c1 := testutil.NewConfig()
	c1.EnableClusterEventsChannel = true
	rt1, err := cluster.addNode(c1)
	require.NoError(t, err)
	_, err = cluster.addNode(nil)
	require.NoError(t, err)
	rt3, err := cluster.addNode(nil)
	require.NoError(t, err)
	require.NoError(t, rt3.CheckBootstrap())

	// Kill rt3's RESP server hard. Its memberlist stays alive, so the
	// coordinator still counts it as a member but cannot push tables to it.
	require.NoError(t, rt3.server.Shutdown(context.Background()))

	rt4, err := cluster.addNode(nil)
	require.NoError(t, err)

	// The unreachable member must still be part of the cluster while the
	// joiner bootstraps. The coordinator learns about the joiner through
	// asynchronous memberlist gossip, so poll instead of asserting instantly.
	require.Eventually(t, func() bool {
		return rt1.NumMembers() == 4
	}, 15*time.Second, 100*time.Millisecond,
		"coordinator must observe all four members, including the unreachable one")
	require.NoError(t, rt4.CheckBootstrap())

	// The coordinator must commit the routing table even though one member
	// could not be reached, and the joiner must install exactly that table:
	// both signatures are derived from the committed payload bytes. The
	// active epoch is the committed table's epoch. Whether the join changed
	// partition ownership is not asserted: with seven partitions a fourth
	// member may own none, in which case the table, its signature and the
	// epoch legitimately stay the same.
	require.Eventually(t, func() bool {
		epoch, _, _, _ := rt1.getRebalanceState()
		signature := rt1.Signature()
		return signature != 0 && rt4.Signature() == signature && epoch == signature
	}, 15*time.Second, 100*time.Millisecond,
		"coordinator must commit the routing update and the joiner must install it despite an unreachable member")
}
