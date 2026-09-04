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
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/stats"
)

const (
	// repeatedSignatureEscape is the sync escape of the cluster in
	// TestOlric_NodeLeftEpochCompletes_WhenTableReturnsToEarlierState.
	repeatedSignatureEscape = 4 * time.Second
	// emptyPartitionsEscape is the sync escape of the cluster in
	// TestOlric_NodeLeftEpochCompletes_WithoutWaitingForEmptyPartitions. It is
	// far above that test's assertion window, so a regression to waiting on
	// empty partitions fails the test.
	emptyPartitionsEscape = 20 * time.Second
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

	// The coordinator stamps the join with the generation it held before the
	// table changed, and the epoch it then starts for the join carries a
	// higher one. Only db1's own events are considered: every member
	// publishes the join it observes, each with its own generation.
	var joinGeneration, startGeneration uint64
	var sawJoin, sawStart bool
	deadline := time.After(10 * time.Second)

	for !sawJoin || !sawStart {
		select {
		case msg := <-ch:
			switch {
			case strings.Contains(msg.Payload, events.KindNodeJoinEvent):
				var ev events.NodeJoinEvent
				require.NoError(t, json.Unmarshal([]byte(msg.Payload), &ev))
				if ev.Source == db1.rt.This().String() && ev.NodeJoin == db2.rt.This().String() {
					joinGeneration, sawJoin = ev.Generation, true
				}
			case strings.Contains(msg.Payload, events.KindRebalanceStartEvent):
				var ev events.RebalanceStartEvent
				require.NoError(t, json.Unmarshal([]byte(msg.Payload), &ev))
				if ev.Node == db2.rt.This().String() {
					startGeneration, sawStart = ev.Generation, true
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for the join and its rebalance start on cluster.events")
		}
	}

	require.Greater(t, startGeneration, joinGeneration)
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

// waitForSettledEpoch waits until every member has installed the same routing
// table and completedFor reports that table's rebalance epoch complete, then
// returns the table's signature. A member's Started callback fires once it
// installed the pushed table, which can be before the coordinator installed
// that table itself, so reading the coordinator's signature right after
// adding members can observe the previous table.
func waitForSettledEpoch(t *testing.T, timeout time.Duration, completedFor func(uint64) bool, members ...*Olric) uint64 {
	t.Helper()

	var signature uint64
	require.Eventually(t, func() bool {
		signature = members[0].rt.Signature()
		if signature == 0 || !completedFor(signature) {
			return false
		}

		for _, member := range members[1:] {
			if member.rt.Signature() != signature {
				return false
			}
		}

		return true
	}, timeout, 100*time.Millisecond, "members must agree on a routing table whose rebalance epoch completed")

	return signature
}

// completedEpochs subscribes to the cluster events of coordinator and records
// the epoch of every rebalance-complete event it receives. It returns a
// predicate reporting whether an epoch completed and a reset that forgets the
// epochs seen so far. The subscription is closed when the test ends.
func completedEpochs(t *testing.T, coordinator *Olric) (completedFor func(epoch uint64) bool, reset func()) {
	t.Helper()

	ctx := context.Background()
	ps, err := coordinator.NewEmbeddedClient().NewPubSub(ToAddress(coordinator.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	t.Cleanup(func() {
		require.NoError(t, rp.Close())
	})

	// The first message confirms the subscription.
	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	var mtx sync.Mutex
	completed := make(map[uint64]struct{})
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

	completedFor = func(epoch uint64) bool {
		mtx.Lock()
		defer mtx.Unlock()

		_, ok := completed[epoch]
		return ok
	}

	reset = func() {
		mtx.Lock()
		defer mtx.Unlock()

		clear(completed)
	}

	return completedFor, reset
}

// TestOlric_NodeLeftEpochCompletes_WhenTableReturnsToEarlierState guards,
// end to end, the balancer acking once per installed table generation rather
// than once per signature value. On an empty cluster a member that joins and
// leaves before any data moves returns the routing table to its previous
// state, so the node-left epoch runs under the same id as the epoch the
// survivors acked before the join. Keyed on the signature alone, a survivor
// whose last ack was that signature would skip the ack and the epoch would
// never complete. The discriminating checks live in
// TestTryAckRebalance_AcksOncePerGeneration and
// TestApplyRoutingTablePayload_Generation; this test asserts the property on
// a live cluster.
func TestOlric_NodeLeftEpochCompletes_WhenTableReturnsToEarlierState(t *testing.T) {
	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		// Enough partitions that the joiner always takes ownership, so both
		// the join and the leave change the table.
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.EnableClusterEventsChannel = true
		c.InitialSyncEmptyPartitionTimeout = repeatedSignatureEscape
		c.TriggerBalancerInterval = 100 * time.Millisecond
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	db2 := cluster.addMemberWithConfig(t, newConfig())
	db3 := cluster.addMemberWithConfig(t, newConfig())

	// Cluster events are published on the coordinator's Pub/Sub. db1 is the
	// oldest member, so it stays coordinator for the whole test.
	completedFor, reset := completedEpochs(t, db1)

	// Let the initial table settle on every member and its epoch complete,
	// so each survivor now remembers the initial signature as acked.
	initial := waitForSettledEpoch(t, repeatedSignatureEscape+15*time.Second, completedFor, db1, db2, db3)
	reset()

	// The joiner takes ownership, then leaves before any data moved.
	db4 := cluster.addMemberWithConfig(t, newConfig())
	require.Eventually(t, func() bool {
		return db1.rt.Signature() != initial
	}, 5*time.Second, 50*time.Millisecond, "joiner must change the routing table")

	require.NoError(t, db4.Shutdown(context.Background()))

	// Without the joiner the table is back in its initial state and the
	// node-left epoch runs under the initial signature. It must complete.
	require.Eventually(t, func() bool {
		return db1.rt.Signature() == initial && completedFor(initial)
	}, repeatedSignatureEscape+15*time.Second, 100*time.Millisecond, "node-left epoch must complete although its id matches an epoch acked before the join")
}

// TestOlric_NodeLeftEpochCompletes_WithoutWaitingForEmptyPartitions guards
// the source probe end to end. On an empty cluster no owner holds data for
// any partition, so after a member leaves the survivors must ack the
// node-left epoch on their next balancer tick instead of holding it for
// InitialSyncEmptyPartitionTimeout, which emptyPartitionsEscape sets far
// above the assertion window.
func TestOlric_NodeLeftEpochCompletes_WithoutWaitingForEmptyPartitions(t *testing.T) {
	newConfig := func() *config.Config {
		c := testutil.NewConfig()
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.EnableClusterEventsChannel = true
		c.InitialSyncEmptyPartitionTimeout = emptyPartitionsEscape
		c.TriggerBalancerInterval = 100 * time.Millisecond
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	db2 := cluster.addMemberWithConfig(t, newConfig())
	db3 := cluster.addMemberWithConfig(t, newConfig())

	// Cluster events are published on the coordinator's Pub/Sub. db1 is the
	// oldest member, so it stays coordinator for the whole test.
	completedFor, _ := completedEpochs(t, db1)

	// The join epochs complete without waiting either: no member holds
	// data, so nothing is in flight.
	initial := waitForSettledEpoch(t, 5*time.Second, completedFor, db1, db2, db3)

	require.NoError(t, db3.Shutdown(context.Background()))

	// The departure changes the table and starts the node-left epoch, which
	// must complete well inside the escape timeout.
	require.Eventually(t, func() bool {
		signature := db1.rt.Signature()
		return signature != initial && completedFor(signature)
	}, 5*time.Second, 100*time.Millisecond, "node-left epoch must complete without waiting for empty partitions")
}

// startMemberAt starts a member of the cluster on the given RESP and
// memberlist ports, so a member that left can come back under its own name,
// as a restarted process does. Everything else follows newTestWithConfig.
func (cl *testCluster) startMemberAt(t *testing.T, c *config.Config, bindAddr string, bindPort, memberlistPort int) *Olric {
	t.Helper()

	cl.mtx.Lock()
	defer cl.mtx.Unlock()

	for _, member := range cl.members {
		if member.rt.Discovery().LocalNode().Address() == net.JoinHostPort(bindAddr, strconv.Itoa(memberlistPort)) {
			continue
		}

		c.Peers = append(c.Peers, member.rt.Discovery().LocalNode().Address())
	}

	c.MemberlistConfig.BindPort = memberlistPort
	c.BindAddr = bindAddr
	c.BindPort = bindPort
	require.NoError(t, c.Sanitize())
	require.NoError(t, c.Validate())

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
	case <-time.After(30 * time.Second):
		t.Fatalf("Olric cannot be restarted in thirty seconds")
	case <-ctx.Done():
	}

	cl.members[db.rt.This().String()] = db
	return db
}

// primaryTransfers subscribes to the cluster events of member and returns a
// function counting the fragment-migration events received after since that
// carried a primary copy to another member: a restore or an ownership move,
// as opposed to a backup push or a local promotion.
func primaryTransfers(t *testing.T, member *Olric) func(since time.Time) int {
	t.Helper()

	ctx := context.Background()
	ps, err := member.NewEmbeddedClient().NewPubSub(ToAddress(member.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	t.Cleanup(func() {
		require.NoError(t, rp.Close())
	})

	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	var mtx sync.Mutex
	var seen []time.Time
	var details []string
	ch := rp.Channel()

	go func() {
		for msg := range ch {
			if !strings.Contains(msg.Payload, events.KindFragmentMigrationEvent) {
				continue
			}

			var ev events.FragmentMigrationEvent
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				continue
			}

			if ev.IsBackup || ev.Target == ev.Source {
				continue
			}

			mtx.Lock()
			seen = append(seen, time.Now())
			details = append(details, fmt.Sprintf("%s -> %s partition %d", ev.Source, ev.Target, ev.PartitionID))
			mtx.Unlock()
		}
	}()

	return func(since time.Time) int {
		mtx.Lock()
		defer mtx.Unlock()

		count := 0
		for i, at := range seen {
			if at.After(since) {
				count++
				t.Logf("primary transfer after %s: %s", since.Format(time.StampMilli), details[i])
			}
		}

		return count
	}
}

// copiesOf reports how many of members hold data for partition partID in
// either copy, and whether the partition's primary owner, in view's routing
// view, holds a primary copy.
func copiesOf(members []*Olric, view *Olric, partID uint64) (copies int, primaryHeld bool) {
	owned := view.primary.PartitionByID(partID)
	for _, member := range members {
		primary := member.primary.PartitionByID(partID).Length()
		backup := member.backup.PartitionByID(partID).Length()
		if primary > 0 || backup > 0 {
			copies++
		}

		if owned.OwnerCount() > 0 && owned.Owner().CompareByName(member.rt.This()) && primary > 0 {
			primaryHeld = true
		}
	}

	return copies, primaryHeld
}

// newDepartureConfig is the configuration of the departure tests: two copies
// per partition, proactive sync on, events on, a fast balancer and an escape
// far above the assertion windows, so any ack that waits on it fails the test.
func newDepartureConfig() *config.Config {
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	c.WriteQuorum = 1
	c.ReadQuorum = 1
	c.EnableClusterEventsChannel = true
	c.EnableProactiveSyncOnJoin = true
	c.InitialSyncEmptyPartitionTimeout = emptyPartitionsEscape
	c.TriggerBalancerInterval = 100 * time.Millisecond
	return c
}

// TestOlric_NodeLeftEpochCompletes_WithDataInBackups guards that a departure
// converges without waiting on the escape when the departed member's data is
// held in the survivors' backup copies: a new primary owner that only has a
// backup copy elsewhere to receive from is not awaited, because that copy is
// restored off the convergence path.
func TestOlric_NodeLeftEpochCompletes_WithDataInBackups(t *testing.T) {
	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newDepartureConfig())
	completedFor, _ := completedEpochs(t, db1)
	db2 := cluster.addMemberWithConfig(t, newDepartureConfig())
	db3 := cluster.addMemberWithConfig(t, newDepartureConfig())

	initial := waitForSettledEpoch(t, 5*time.Second, completedFor, db1, db2, db3)

	ctx := context.Background()
	dm, err := db1.NewEmbeddedClient().NewDMap("mydmap")
	require.NoError(t, err)
	for i := range 300 {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i)))
	}

	require.NoError(t, db3.Shutdown(ctx))

	require.Eventually(t, func() bool {
		signature := db1.rt.Signature()
		return signature != initial && completedFor(signature)
	}, 5*time.Second, 100*time.Millisecond, "the node-left epoch must complete without waiting on the escape")

	for i := range 300 {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

// TestOlric_RestoresPrimaryCopiesAfterDeparture guards the delayed restore end
// to end: once ReplicaRestoreDelay has passed after a departure, every
// partition that holds data has a primary copy on its owner and ReplicaCount
// copies in total, without a write having touched it.
func TestOlric_RestoresPrimaryCopiesAfterDeparture(t *testing.T) {
	newConfig := func() *config.Config {
		c := newDepartureConfig()
		c.ReplicaRestoreDelay = 500 * time.Millisecond
		return c
	}

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig())
	completedFor, _ := completedEpochs(t, db1)
	db2 := cluster.addMemberWithConfig(t, newConfig())
	db3 := cluster.addMemberWithConfig(t, newConfig())

	waitForSettledEpoch(t, 5*time.Second, completedFor, db1, db2, db3)

	ctx := context.Background()
	dm, err := db1.NewEmbeddedClient().NewDMap("mydmap")
	require.NoError(t, err)
	for i := range 300 {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i)))
	}

	require.NoError(t, db3.Shutdown(ctx))

	survivors := []*Olric{db1, db2}
	require.Eventually(t, func() bool {
		for partID := range db1.config.PartitionCount {
			copies, primaryHeld := copiesOf(survivors, db1, partID)
			if copies == 0 {
				continue
			}

			if copies < db1.config.ReplicaCount || !primaryHeld {
				return false
			}
		}

		return true
	}, 15*time.Second, 100*time.Millisecond, "every partition with data must regain a primary copy and its replica set")
}

// TestOlric_RollingRestartMovesNoPrimaryCopyWithinRestoreDelay guards the
// rolling-restart guarantee: a member that leaves and comes back under its
// own name within ReplicaRestoreDelay causes no restore and no primary copy to
// move; only the join sync that runs today happens.
func TestOlric_RollingRestartMovesNoPrimaryCopyWithinRestoreDelay(t *testing.T) {
	// Every member logs into one buffer: a restore announces itself there.
	// The logger is rebuilt from LogOutput when the member starts, so the
	// buffer is set as the output rather than as the logger.
	logs := &lockedLog{}
	newConfig := func(index int) *config.Config {
		c := newDepartureConfig()
		c.LogOutput = &prefixedWriter{prefix: fmt.Sprintf("[m%d] ", index), w: logs}
		c.Logger = log.New(c.LogOutput, "", log.LstdFlags)
		c.LogVerbosity = 6
		return c
	}

	t.Cleanup(func() {
		if t.Failed() {
			path := filepath.Join(os.TempDir(), "olric-rolling-restart.log")
			_ = os.WriteFile(path, []byte(logs.String()), 0o644)
			t.Logf("member logs written to %s", path)
		}
	})

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig(1))
	completedFor, _ := completedEpochs(t, db1)
	db2 := cluster.addMemberWithConfig(t, newConfig(2))
	db3 := cluster.addMemberWithConfig(t, newConfig(3))

	initial := waitForSettledEpoch(t, 5*time.Second, completedFor, db1, db2, db3)

	ctx := context.Background()
	dm, err := db1.NewEmbeddedClient().NewDMap("mydmap")
	require.NoError(t, err)
	for i := range 300 {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i)))
	}

	transfers := primaryTransfers(t, db1)
	bindAddr, bindPort := db3.config.BindAddr, db3.config.BindPort
	memberlistPort := int(db3.rt.Discovery().LocalNode().Port)
	departed := time.Now()

	require.NoError(t, db3.Shutdown(ctx))
	require.Eventually(t, func() bool {
		signature := db1.rt.Signature()
		return signature != initial && completedFor(signature)
	}, 5*time.Second, 100*time.Millisecond)

	// Several balancer ticks pass with the member gone: nothing may be
	// restored in that window. Primary transfers are logged for diagnosis
	// only: memberlist may flap on a departure and move a promoted copy
	// back, which is not a restore.
	time.Sleep(time.Second)
	t.Logf("primary transfers in the departure window: %d", transfers(departed))
	require.NotContains(t, logs.String(), "Restoring the primary copy", "a member back within the restore delay must not have its share restored")

	db3 = cluster.startMemberAt(t, newConfig(4), bindAddr, bindPort, memberlistPort)
	waitForSettledEpoch(t, 10*time.Second, completedFor, db1, db2, db3)
	require.NotContains(t, logs.String(), "Restoring the primary copy")

	for i := range 300 {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

// lockedLog is an io.Writer the members' loggers and the test can share.
type lockedLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.String()
}

// prefixedWriter tags every line a member logs with a prefix, so the shared
// log of a multi-member test can be attributed to a member.
type prefixedWriter struct {
	prefix string
	w      io.Writer
}

func (p *prefixedWriter) Write(b []byte) (int, error) {
	if _, err := p.w.Write(append([]byte(p.prefix), b...)); err != nil {
		return 0, err
	}

	return len(b), nil
}

// capturedEvents subscribes to the cluster events of member and returns a
// function listing the decoded events of one kind received so far.
func capturedEvents(t *testing.T, member *Olric) func(kind string) []map[string]any {
	t.Helper()

	ctx := context.Background()
	ps, err := member.NewEmbeddedClient().NewPubSub(ToAddress(member.rt.This().String()))
	require.NoError(t, err)
	rp := ps.Subscribe(ctx, events.ClusterEventsChannel)
	t.Cleanup(func() {
		require.NoError(t, rp.Close())
	})

	_, err = rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	var mtx sync.Mutex
	var seen []map[string]any
	ch := rp.Channel()

	go func() {
		for msg := range ch {
			ev := map[string]any{}
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				continue
			}

			mtx.Lock()
			seen = append(seen, ev)
			mtx.Unlock()
		}
	}()

	return func(kind string) []map[string]any {
		mtx.Lock()
		defer mtx.Unlock()

		var out []map[string]any
		for _, ev := range seen {
			if ev["kind"] == kind {
				out = append(out, ev)
			}
		}

		return out
	}
}

// TestOlric_MembershipChange_AnnouncedOnceByCoordinator guards the membership
// announcement model end to end: a subscriber on a member that is not the
// coordinator receives one membership-change-event per departure and per join,
// from the coordinator, carrying the member set after the change, while the
// node-left-event copies it receives come from its own member only.
func TestOlric_MembershipChange_AnnouncedOnceByCoordinator(t *testing.T) {
	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newDepartureConfig())
	db2 := cluster.addMemberWithConfig(t, newDepartureConfig())
	db3 := cluster.addMemberWithConfig(t, newDepartureConfig())

	waitForClusterSize(t, []*Olric{db1, db2, db3}, 3)
	captured := capturedEvents(t, db2)
	departed := db3.rt.This().String()

	require.NoError(t, db3.Shutdown(context.Background()))

	require.Eventually(t, func() bool {
		return len(captured(events.KindMembershipChangeEvent)) == 1
	}, 5*time.Second, 50*time.Millisecond, "the departure must be announced exactly once")

	ev := captured(events.KindMembershipChangeEvent)[0]
	require.Equal(t, events.MembershipChangeLeft, ev["change"])
	require.Equal(t, departed, ev["node"])
	require.Equal(t, db1.rt.This().String(), ev["source"], "the coordinator announces")
	require.ElementsMatch(t, []any{db1.rt.This().String(), db2.rt.This().String()}, ev["members"])

	// The coordinator may announce before this member's own memberlist has
	// processed the leave, so the local observation can trail the announcement.
	require.Eventually(t, func() bool {
		return len(captured(events.KindNodeLeftEvent)) > 0
	}, 5*time.Second, 50*time.Millisecond, "the local observation is still delivered")

	for _, left := range captured(events.KindNodeLeftEvent) {
		require.Equal(t, db2.rt.This().String(), left["source"], "observations come from the subscriber's own member only")
	}

	db4 := cluster.addMemberWithConfig(t, newDepartureConfig())
	require.Eventually(t, func() bool {
		return len(captured(events.KindMembershipChangeEvent)) == 2
	}, 5*time.Second, 50*time.Millisecond, "the join must be announced exactly once")

	joined := captured(events.KindMembershipChangeEvent)[1]
	require.Equal(t, events.MembershipChangeJoin, joined["change"])
	require.Equal(t, db4.rt.This().String(), joined["node"])
	require.ElementsMatch(t, []any{db1.rt.This().String(), db2.rt.This().String(), db4.rt.This().String()}, joined["members"])

	// No second announcement arrives late.
	time.Sleep(500 * time.Millisecond)
	require.Len(t, captured(events.KindMembershipChangeEvent), 2)
}

// crashMember stops member the way a SIGKILL does: its memberlist is shut
// down without a leave broadcast, its RESP server and balancer are stopped,
// and it is taken out of the cluster's cleanup, since a member whose
// memberlist is gone cannot be shut down gracefully afterwards.
func (cl *testCluster) crashMember(t *testing.T, member *Olric) {
	t.Helper()

	cl.mtx.Lock()
	delete(cl.members, member.rt.This().String())
	cl.mtx.Unlock()

	field := reflect.ValueOf(member.rt.Discovery()).Elem().FieldByName("memberlist")
	ml := *(**memberlist.Memberlist)(unsafe.Pointer(field.UnsafeAddr()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, member.balancer.Shutdown(ctx))
	require.NoError(t, ml.Shutdown())
	require.NoError(t, member.server.Shutdown(ctx))
}

// TestOlric_CoordinatorCrash_ConvergesWithinDetection guards the reported
// scenario end to end: the coordinator dies without a leave, with data in the
// cluster and with the gossip of the member that becomes coordinator slowed,
// so its first table push races its own dead-node gossip and can be rejected.
// The survivors must announce the departure once, from the new coordinator,
// and publish a rebalance-complete-event without the dead member within a few
// seconds of memberlist confirming the death, not at the next periodic push,
// and every key must stay readable.
func TestOlric_CoordinatorCrash_ConvergesWithinDetection(t *testing.T) {
	logs := &lockedLog{}
	newConfig := func(index int, slowGossip bool) *config.Config {
		c := newDepartureConfig()
		c.LogOutput = &prefixedWriter{prefix: fmt.Sprintf("[m%d] ", index), w: logs}
		c.Logger = log.New(c.LogOutput, "", log.LstdFlags)
		c.LogVerbosity = 6
		if slowGossip {
			c.MemberlistConfig.GossipInterval = 2 * time.Second
		}

		return c
	}

	t.Cleanup(func() {
		if t.Failed() {
			path := filepath.Join(os.TempDir(), "olric-coordinator-crash.log")
			_ = os.WriteFile(path, []byte(logs.String()), 0o644)
			t.Logf("member logs written to %s", path)
		}
	})

	cluster := newTestCluster(t)
	db1 := cluster.addMemberWithConfig(t, newConfig(1, false))
	db2 := cluster.addMemberWithConfig(t, newConfig(2, true))
	db3 := cluster.addMemberWithConfig(t, newConfig(3, false))
	db4 := cluster.addMemberWithConfig(t, newConfig(4, false))
	waitForClusterSize(t, []*Olric{db1, db2, db3, db4}, 4)

	ctx := context.Background()
	dm, err := db1.NewEmbeddedClient().NewDMap("mydmap")
	require.NoError(t, err)
	for i := range 300 {
		require.NoError(t, dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i)))
	}

	captured := capturedEvents(t, db4)
	victim := db1.rt.This().String()
	survivors := []*Olric{db2, db3, db4}

	crashed := time.Now()
	cluster.crashMember(t, db1)

	require.Eventually(t, func() bool {
		for _, survivor := range survivors {
			if survivor.rt.NumMembers() != 3 {
				return false
			}
		}

		return true
	}, 30*time.Second, 50*time.Millisecond, "memberlist must confirm the death")
	detected := time.Now()

	convergedWithout := func() bool {
		for _, ev := range captured(events.KindRebalanceCompleteEvent) {
			members, _ := ev["members"].([]any)
			if len(members) == 0 {
				continue
			}

			gone := true
			for _, member := range members {
				if member == victim {
					gone = false
				}
			}

			if gone {
				return true
			}
		}

		return false
	}
	require.Eventually(t, convergedWithout, 10*time.Second, 50*time.Millisecond, "the survivors must converge without waiting for the periodic push")
	t.Logf("death confirmed %s after the crash, converged %s after that", detected.Sub(crashed).Round(time.Millisecond), time.Since(detected).Round(time.Millisecond))
	require.Less(t, time.Since(detected), 5*time.Second)

	announced := false
	for _, ev := range captured(events.KindMembershipChangeEvent) {
		if ev["change"] == events.MembershipChangeLeft && ev["node"] == victim {
			announced = true
			require.Equal(t, db2.rt.This().String(), ev["source"], "the new coordinator announces the departure")
		}
	}
	require.True(t, announced, "the departure must be announced")

	dm, err = db2.NewEmbeddedClient().NewDMap("mydmap")
	require.NoError(t, err)
	for i := range 300 {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}
