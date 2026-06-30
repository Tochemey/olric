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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/internal/testcluster"
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
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Wire up a sync state and mark the initial sync as complete.
	st := syncstate.New()
	st.Reset(nil) // empty pending set signals "done" immediately
	require.True(t, st.IsDone())
	s.syncState = st

	s.wg.Add(1)
	go s.initialSyncCompletePublisher()

	// Allow the publisher (and the spawned publishEvent goroutine) to run.
	<-time.After(200 * time.Millisecond)
}

func TestDMap_initialSyncCompletePublisher_ContextDone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Sync state that never completes so the publisher blocks until the
	// service context is cancelled.
	st := syncstate.New()
	st.Reset([]uint64{1, 2, 3})
	require.False(t, st.IsDone())
	s.syncState = st

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
