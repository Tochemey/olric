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
	"testing"
	"time"

	"github.com/tochemey/olric/internal/testutil"
)

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
