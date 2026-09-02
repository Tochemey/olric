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
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/stats"
)

// newTestOlricWithConfig creates a new Olric instance with the given configuration.
// This function is intended for internal use. Please use testOlricCluster and its
// methods to form a cluster in tests.
func newTestWithConfig(t *testing.T, c *config.Config) *Olric {
	port, err := testutil.GetFreePort()
	require.NoError(t, err)

	if c.MemberlistConfig == nil {
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}
	c.MemberlistConfig.BindPort = 0

	c.BindAddr = "127.0.0.1"
	c.BindPort = port

	err = c.Sanitize()
	require.NoError(t, err)

	err = c.Validate()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		cancel()
	}

	db, err := New(c)
	require.NoError(t, err)

	go func() {
		if err := db.Start(); err != nil {
			panic(fmt.Sprintf("Failed to run Olric: %v", err))
		}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("Olric cannot be started in one second")
	case <-ctx.Done():
		// everything is fine
	}

	return db
}

type testCluster struct {
	mtx     sync.Mutex
	members map[string]*Olric
}

func newTestCluster(t *testing.T) *testCluster {
	cl := &testCluster{members: make(map[string]*Olric)}
	t.Cleanup(func() {
		cl.mtx.Lock()
		defer cl.mtx.Unlock()
		for _, member := range cl.members {
			// Generous deadline: shutdown waits for leave broadcasts and
			// background goroutines, which get markedly slower on starved CI
			// runners and under the race detector.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := member.Shutdown(ctx)
			cancel()
			require.NoError(t, err)
		}
	})
	return cl
}

func (cl *testCluster) addMemberWithConfig(t *testing.T, c *config.Config) *Olric {
	cl.mtx.Lock()
	defer cl.mtx.Unlock()

	if c == nil {
		c = testutil.NewConfig()
	}

	for _, member := range cl.members {
		c.Peers = append(c.Peers, member.rt.Discovery().LocalNode().Address())
	}

	db := newTestWithConfig(t, c)
	cl.members[db.rt.This().String()] = db
	t.Logf("A new cluster member has been created: %s", db.rt.This())
	return db
}

func (cl *testCluster) addMember(t *testing.T) *Olric {
	return cl.addMemberWithConfig(t, nil)
}

func TestStartAndShutdown(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	err := db.Shutdown(context.Background())
	require.NoError(t, err)
}

// Regression test for https://github.com/Tochemey/olric/issues/20. A failed
// node start must not block the Started callback of instances created later
// in the same process.
func TestOlric_StartedCallback_AfterFailedStart(t *testing.T) {
	// Occupy a TCP port so memberlist cannot bind it and the discovery
	// subsystem fails to start, making Start return an error. At that point
	// the TCP server checkpoint has passed but the routing table checkpoint
	// has not, which used to leave the process-global counters unbalanced
	// forever.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, blocker.Close())
	}()

	c := testutil.NewConfig()
	c.MemberlistConfig.BindAddr = "127.0.0.1"
	c.MemberlistConfig.BindPort = blocker.Addr().(*net.TCPAddr).Port

	db, err := New(c)
	require.NoError(t, err)
	require.Error(t, db.Start())
	require.NoError(t, db.Shutdown(context.Background()))

	// A healthy instance in the same process must still fire Started.
	// newTestWithConfig fails the test if the callback does not fire within
	// a second.
	cluster := newTestCluster(t)
	cluster.addMember(t)
}

// Regression test for rebalance ACK starvation: routing updates arriving
// faster than the empty-partition escape delay used to rebuild the pending
// set and restart its escape clock from zero on every update, so no member
// ever ACKed and no rebalance epoch completed. With per-partition deadlines
// preserved across reconciles, an epoch started mid-storm must still
// complete: every owned-but-empty partition escapes once its own deadline
// elapses, regardless of how often the routing table is pushed.
func TestOlric_RebalanceCompletes_UnderRoutingUpdateChurn(t *testing.T) {
	const escape = 1500 * time.Millisecond

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.EnableClusterEventsChannel = true
		c.InitialSyncEmptyPartitionTimeout = escape
		c.TriggerBalancerInterval = 100 * time.Millisecond
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())

	// Cluster events are published on the coordinator's Pub/Sub. db1 is the
	// oldest member, so it stays coordinator for the whole test.
	ctx := context.Background()
	ps, err := db1.NewEmbeddedClient().NewPubSub(ToAddress(db1.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	defer func() {
		require.NoError(t, rp.Close())
	}()
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	var completes atomic.Int64
	ch := rp.Channel()
	go func() {
		for msg := range ch {
			if strings.Contains(msg.Payload, events.KindRebalanceCompleteEvent) {
				completes.Add(1)
			}
		}
	}()

	// Let the bootstrap epochs settle and the initial empty partitions
	// escape once, then start counting from zero.
	<-time.After(escape + time.Second)
	completes.Store(0)

	// Storm: force a routing table push on the whole cluster far more often
	// than the escape delay. Every push reconciles the pending set on every
	// member; before per-partition deadlines this restarted the escape clock
	// each time and starved the ACKs.
	stormDone := make(chan struct{})
	stormStopped := make(chan struct{})
	go func() {
		defer close(stormStopped)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stormDone:
				return
			case <-ticker.C:
				db1.rt.UpdateEagerly()
			}
		}
	}()
	defer func() {
		close(stormDone)
		<-stormStopped
	}()

	// A member joining mid-storm starts a new rebalance epoch. It must
	// complete while the storm keeps reconciling the pending sets: each
	// member's owned-but-empty partitions escape once their own deadline
	// elapses, the balancer ACKs on its next tick, and the coordinator
	// publishes RebalanceComplete.
	<-time.After(time.Second)
	cluster.addMemberWithConfig(t, newConfig())

	require.Eventually(t, func() bool {
		return completes.Load() >= 1
	}, 15*time.Second, 100*time.Millisecond,
		"rebalance epochs must complete while routing updates arrive faster than the escape delay")
}

func TestClusterStartAndShutdown(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.addMember(t)
	db := cluster.addMember(t)
	require.Len(t, cluster.members, 2)

	e := db.NewEmbeddedClient()
	st, err := e.Stats(context.Background(), db.rt.This().String())
	require.NoError(t, err)
	require.Len(t, st.ClusterMembers, 2)
	for _, member := range cluster.members {
		require.Contains(t, st.ClusterMembers, stats.MemberID(member.rt.This().ID))
	}
}

func TestOlric_WaitForInitialSync_ReplicaCount1(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 1
	db, err := New(c)
	require.NoError(t, err)
	require.NotNil(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.WaitForInitialSync(ctx)
	require.NoError(t, err)
}

func TestOlric_InitialSyncComplete_ReplicaCount1(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicaCount = 1
	db, err := New(c)
	require.NoError(t, err)
	require.NotNil(t, db)

	ch := db.InitialSyncComplete()
	select {
	case <-ch:
		// Channel closed immediately for ReplicaCount=1
	default:
		t.Fatal("InitialSyncComplete should return closed channel for ReplicaCount=1")
	}
}

func TestOlric_EnableProactiveSyncOnJoin_Default(t *testing.T) {
	c := config.New("local")
	require.False(t, c.EnableProactiveSyncOnJoin)
}

// TestOlric_ClusterEvents_NodeJoinDelivery drives the real cluster-event
// pipeline end to end: the routing table has no event transport of its own,
// so this only passes if pubsub.NewService registered itself as the
// publisher. Every other events test injects a fake publisher; this is the
// wiring's only guard.
func TestOlric_ClusterEvents_NodeJoinDelivery(t *testing.T) {
	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.EnableClusterEventsChannel = true
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	ps, err := db1.NewEmbeddedClient().NewPubSub(ToAddress(db1.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	defer func() {
		require.NoError(t, rp.Close())
	}()
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	ch := rp.Channel()
	db2 := cluster.addMemberWithConfig(t, newConfig())

	deadline := time.After(10 * time.Second)
	for {
		select {
		case msg := <-ch:
			if strings.Contains(msg.Payload, events.KindNodeJoinEvent) &&
				strings.Contains(msg.Payload, db2.rt.This().String()) {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for NodeJoinEvent on cluster.events")
		}
	}
}

// TestOlric_PeriodicPush_UnchangedTableStartsNoEpoch guards the end-to-end
// contract behind https://github.com/Tochemey/olric/issues/40. The routing
// table signature is the rebalance epoch id and must be a pure function of the
// table content, so periodic pushes (RoutingTablePushInterval) of an unchanged
// table must leave the signature untouched and start no rebalance epoch: a
// stable cluster publishes no rebalance-start-event on cluster.events. Before
// the fix msgpack encoded the table in Go's randomized map order, the
// signature changed on almost every push, and each push superseded the epoch
// that a node join or leave had started.
func TestOlric_PeriodicPush_UnchangedTableStartsNoEpoch(t *testing.T) {
	const pushInterval = 200 * time.Millisecond

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.EnableClusterEventsChannel = true
		c.RoutingTablePushInterval = pushInterval
		c.TriggerBalancerInterval = 100 * time.Millisecond
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())

	// Cluster events are published on the coordinator's Pub/Sub. db1 is the
	// oldest member, so it stays coordinator for the whole test.
	ctx := context.Background()
	ps, err := db1.NewEmbeddedClient().NewPubSub(ToAddress(db1.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	defer func() {
		require.NoError(t, rp.Close())
	}()
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	var starts atomic.Int64
	ch := rp.Channel()
	go func() {
		for msg := range ch {
			if strings.Contains(msg.Payload, events.KindRebalanceStartEvent) {
				starts.Add(1)
			}
		}
	}()

	// Let the join epochs settle. Membership is final and the cluster holds
	// no data, so the table converges on the first push after the last join.
	time.Sleep(5 * pushInterval)
	signature := db1.rt.Signature()
	require.NotZero(t, signature)
	starts.Store(0)

	// Many periodic pushes of the unchanged table: the signature must not
	// move and no rebalance epoch may start.
	time.Sleep(10 * pushInterval)
	require.Equal(t, signature, db1.rt.Signature())
	require.Zero(t, starts.Load(), "periodic pushes of an unchanged routing table started rebalance epochs")
}

// TestOlric_NodeLeftEpochCompletes_WhenTableReturnsToEarlierState guards the
// balancer's ack marker reset against content-derived epoch ids that repeat.
// On an empty cluster a member that joins and leaves before any data moves
// returns the routing table to its previous state, so the node-left epoch
// runs under the same id as the epoch the survivors acked before the join.
// The join hands the survivors new backup partitions whose sync escape has
// not elapsed when the member leaves, so they never ack the join epoch and
// still remember the initial signature as their last ack. Unless that marker
// is cleared when the table is installed again, they skip the ack and the
// node-left epoch never completes.
func TestOlric_NodeLeftEpochCompletes_WhenTableReturnsToEarlierState(t *testing.T) {
	const escape = 4 * time.Second

	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		// Enough partitions that the joiner always takes ownership, so both
		// the join and the leave change the table.
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.EnableClusterEventsChannel = true
		c.InitialSyncEmptyPartitionTimeout = escape
		c.TriggerBalancerInterval = 100 * time.Millisecond
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())

	// Cluster events are published on the coordinator's Pub/Sub. db1 is the
	// oldest member, so it stays coordinator for the whole test.
	ctx := context.Background()
	ps, err := db1.NewEmbeddedClient().NewPubSub(ToAddress(db1.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	defer func() {
		require.NoError(t, rp.Close())
	}()
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	var mtx sync.Mutex
	completed := make(map[uint64]struct{})
	completedFor := func(epoch uint64) bool {
		mtx.Lock()
		defer mtx.Unlock()
		_, ok := completed[epoch]
		return ok
	}
	ch := rp.Channel()
	go func() {
		for msg := range ch {
			if !strings.Contains(msg.Payload, events.KindRebalanceCompleteEvent) {
				continue
			}
			var ev events.RebalanceCompleteEvent
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				continue
			}
			mtx.Lock()
			completed[ev.Epoch] = struct{}{}
			mtx.Unlock()
		}
	}()

	// Let the initial table settle: its empty partitions escape once and
	// every member acks it, so each survivor now remembers the initial
	// signature as its last ack.
	require.Eventually(t, func() bool {
		return completedFor(db1.rt.Signature())
	}, escape+15*time.Second, 100*time.Millisecond,
		"initial rebalance epoch must complete")
	initial := db1.rt.Signature()
	mtx.Lock()
	completed = make(map[uint64]struct{})
	mtx.Unlock()

	// The joiner takes ownership and hands the survivors new backup
	// partitions whose escape has not elapsed when it leaves again, so no
	// survivor acks the join epoch.
	db4 := cluster.addMemberWithConfig(t, newConfig())
	require.Eventually(t, func() bool {
		return db1.rt.Signature() != initial
	}, 5*time.Second, 50*time.Millisecond, "joiner must change the routing table")
	require.NoError(t, db4.Shutdown(context.Background()))

	// Without the joiner the table is back in its initial state and the
	// node-left epoch runs under the initial signature. It must complete.
	require.Eventually(t, func() bool {
		return db1.rt.Signature() == initial && completedFor(initial)
	}, escape+15*time.Second, 100*time.Millisecond,
		"node-left epoch must complete although its id matches an epoch acked before the join")
}
