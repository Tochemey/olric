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
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tochemey/olric/internal/testutil"
)

// fakeServiceDiscovery is a minimal, concurrency-safe ServiceDiscovery plugin
// used to simulate a peer that becomes resolvable after Start() has already
// begun waiting on the quorum gate, without racing on config.Peers.
type fakeServiceDiscovery struct {
	mu    sync.Mutex
	peers []string
}

func (f *fakeServiceDiscovery) Initialize() error                      { return nil }
func (f *fakeServiceDiscovery) SetConfig(map[string]interface{}) error { return nil }
func (f *fakeServiceDiscovery) SetLogger(*log.Logger)                  {}
func (f *fakeServiceDiscovery) Register() error                        { return nil }
func (f *fakeServiceDiscovery) Deregister() error                      { return nil }
func (f *fakeServiceDiscovery) Close() error                           { return nil }

func (f *fakeServiceDiscovery) DiscoverPeers() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.peers...), nil
}

func (f *fakeServiceDiscovery) setPeers(peers []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.peers = peers
}

func TestRoutingTable_tryWithInterval(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	var foobarError = errors.New("foobar")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := rt.tryWithInterval(ctx, time.Millisecond, func() error {
		return foobarError
	})

	if err != foobarError {
		t.Fatalf("Expected foobarError. Got: %v", foobarError)
	}
}

func TestRoutingTable_attemptToJoin(t *testing.T) {
	c := testutil.NewConfig()
	c.MaxJoinAttempts = 3
	c.JoinRetryInterval = 100 * time.Millisecond
	c.Peers = []string{"127.0.0.1:0"} // An invalid peer
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	err := rt.discovery.Start()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = rt.discovery.Shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	err = rt.attemptToJoin()
	if err != ErrClusterJoin {
		t.Fatalf("Expected ErrClusterJoin. Got: %v", err)
	}
}

func TestRoutingTable_attemptToJoin_ServerGone(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	// The node is gone: the join loop must give up immediately.
	rt.cancel()
	if err := rt.attemptToJoin(); err != ErrServerGone {
		t.Fatalf("Expected ErrServerGone. Got: %v", err)
	}
}

func TestRoutingTable_attemptToJoin_BootstrappedByCoordinator(t *testing.T) {
	c := testutil.NewConfig()
	c.MaxJoinAttempts = 5
	c.JoinRetryInterval = 50 * time.Millisecond
	c.Peers = []string{"127.0.0.1:0"} // An invalid peer
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	err := rt.discovery.Start()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = rt.discovery.Shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	// The join attempt fails, but the cluster coordinator has already
	// bootstrapped this node: attemptToJoin must report success.
	rt.markBootstrapped()
	if err := rt.attemptToJoin(); err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestRoutingTable_tryWithInterval_SucceedsOnRetry(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var calls int
	err := rt.tryWithInterval(ctx, 10*time.Millisecond, func() error {
		calls++
		if calls == 1 {
			return errors.New("try again")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if calls < 2 {
		t.Fatalf("Expected at least 2 calls. Got: %d", calls)
	}
}

func TestRoutingTable_Start_QuorumGateActivelyRejoinsDuringWait(t *testing.T) {
	// Cold-start scenario: node1 boots with MemberCountQuorum=2, but its
	// ServiceDiscovery backend has no peers to offer yet at Join() time, so
	// it ends up alone. Before this fix, the quorum gate in Start() only
	// waited passively for gossip to surface a peer, which never happens
	// here since node2 doesn't know about node1 either. With the fix, the
	// gate actively calls discovery.Join() (via tryRejoin, the same call
	// rejoinLoop makes) at RejoinInterval cadence, re-querying the
	// ServiceDiscovery backend on every attempt. Once node2 becomes
	// resolvable — simulated here by making the fake backend return its
	// address — the gate must pick it up and exit well before the 1-hour
	// ceiling.
	node2Port, err := testutil.GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort: %v", err)
	}

	sd := &fakeServiceDiscovery{}

	c1 := testutil.NewConfig()
	c1.MemberCountQuorum = 2
	c1.RejoinInterval = 200 * time.Millisecond
	c1.MaxJoinAttempts = 1
	c1.JoinRetryInterval = 10 * time.Millisecond
	c1.ServiceDiscovery = map[string]any{"plugin": sd} // empty peer list at bootstrap

	c2 := testutil.NewConfig()
	c2.MemberlistConfig.BindPort = node2Port
	// node2 does not know about node1: only node1's active rejoin, once its
	// ServiceDiscovery backend starts returning node2's address, reconnects them.

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
	if err := rt2.Start(); err != nil {
		t.Fatalf("rt2.Start: %v", err)
	}

	defer func() {
		_ = rt1.Shutdown(context.Background())
		_ = rt2.Shutdown(context.Background())
		_ = srv1.Shutdown(context.Background())
		_ = srv2.Shutdown(context.Background())
	}()

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- rt1.Start() }()

	// Give the gate a couple of ticks while node2 is still unknown to it.
	time.Sleep(500 * time.Millisecond)
	if n := rt1.NumMembers(); n != 1 {
		t.Fatalf("Expected rt1 to still be alone before node2 is resolvable. Got: %d members", n)
	}

	// node2 "becomes resolvable": the ServiceDiscovery backend now returns it.
	sd.setPeers([]string{net.JoinHostPort("127.0.0.1", strconv.Itoa(node2Port))})

	select {
	case err := <-startErrCh:
		if err != nil {
			t.Fatalf("rt1.Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("rt1.Start did not exit the quorum gate after the peer became resolvable")
	}

	if n := rt1.NumMembers(); n < 2 {
		t.Fatalf("Expected rt1 to see 2 members after the gate actively rejoined. Got: %d", n)
	}
}

func TestRoutingTable_Start_NoActiveJoinWhenQuorumDefault(t *testing.T) {
	// MemberCountQuorum defaults to 1: the quorum check inside the gate is
	// satisfied on the very first invocation, before the active-rejoin branch
	// added by this fix even gets a chance to run. This guards against the
	// fix introducing superfluous Join() calls in the common single-node/
	// default-quorum case, by asserting Start() still returns essentially
	// immediately rather than waiting out any rejoin cadence.
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	if err := rt.Join(); err != nil {
		t.Fatalf("rt.Join: %v", err)
	}
	defer func() {
		_ = rt.Shutdown(context.Background())
		_ = srv.Shutdown(context.Background())
	}()

	start := time.Now()
	if err := rt.Start(); err != nil {
		t.Fatalf("rt.Start: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Start took %s with default quorum; the gate should be satisfied immediately", elapsed)
	}
}

func TestRoutingTable_tryWithInterval_Canceled(t *testing.T) {
	c := testutil.NewConfig()
	srv := testutil.NewServer(c)
	rt := newRoutingTableForTest(c, srv)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	// The interval is too long for the ticker to ever fire: canceling the
	// context is the only way out and it maps to ErrServerGone.
	err := rt.tryWithInterval(ctx, time.Hour, func() error {
		return errors.New("never succeeds")
	})
	if err != ErrServerGone {
		t.Fatalf("Expected ErrServerGone. Got: %v", err)
	}
}
