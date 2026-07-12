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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/config"
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
