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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/testutil"
)

func TestRoutingTable_Callback(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))
	var num int32
	increase := func() {
		atomic.AddInt32(&num, 1)
	}
	rt.AddCallback(increase)
	go rt.runCallbacks()
	<-time.After(100 * time.Millisecond)
	modified := atomic.LoadInt32(&num)
	if modified != 1 {
		t.Fatalf("Expected number: 1. Got: %v", modified)
	}
}

func TestRoutingTable_NodeLeaveCallback(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	var got []string
	rt.AddNodeLeaveCallback(func(nodeName string) {
		got = append(got, nodeName)
	})

	// Callbacks fire synchronously on the event goroutine, in order.
	rt.notifyNodeLeaveCallbacks("127.0.0.1:3320")
	rt.notifyNodeLeaveCallbacks("127.0.0.1:3321")
	require.Equal(t, []string{"127.0.0.1:3320", "127.0.0.1:3321"}, got)
}

func TestRoutingTable_NodeJoinCallback(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	var got []string
	rt.AddNodeJoinCallback(func(nodeName string) {
		got = append(got, nodeName)
	})

	rt.notifyNodeJoinCallbacks("127.0.0.1:3320")
	require.Equal(t, []string{"127.0.0.1:3320"}, got)
}

func TestRoutingTable_NodeLeaveCallback_CanceledContext(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))

	var num int32
	rt.AddNodeLeaveCallback(func(string) {
		atomic.AddInt32(&num, 1)
	})

	// Cancel the routing table context: the notifier must return without
	// invoking any callback.
	rt.cancel()
	rt.notifyNodeLeaveCallbacks("127.0.0.1:3320")

	require.Zero(t, atomic.LoadInt32(&num))
}

func TestRoutingTable_Callback_CanceledContext(t *testing.T) {
	c := testutil.NewConfig()
	rt := newRoutingTableForTest(c, testutil.NewServer(c))
	var num int32
	rt.AddCallback(func() {
		atomic.AddInt32(&num, 1)
	})

	// Cancel the routing table context: runCallbacks must return without
	// invoking any callback.
	rt.cancel()
	rt.runCallbacks()

	if modified := atomic.LoadInt32(&num); modified != 0 {
		t.Fatalf("Expected no callback run. Got: %v", modified)
	}
}
