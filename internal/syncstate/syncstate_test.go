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

package syncstate

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestState_Reconcile_Empty(t *testing.T) {
	s := New()
	s.Reconcile(nil, time.Second)
	require.True(t, s.PendingEmpty())
	require.True(t, s.IsDone())
	select {
	case <-s.Done():
	default:
		t.Fatal("Done() channel should be closed when reconciled with empty")
	}
}

func TestState_Reconcile_WithPartitions(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1, 2, 3}, time.Second)
	require.False(t, s.PendingEmpty())
	require.False(t, s.IsDone())
}

// TestState_Expired guards the expiry query: only a tracked partition whose
// escape deadline elapsed is expired, and a reconcile keeps the deadline clock
// of partitions already tracked.
func TestState_Expired(t *testing.T) {
	s := New()
	require.False(t, s.Expired(1), "an untracked partition is not expired")

	s.Reconcile([]uint64{1}, time.Hour)
	require.False(t, s.Expired(1), "a tracked partition inside its escape window is not expired")

	// Partition 1 keeps its original deadline clock, partition 2 starts now;
	// with a one nanosecond escape both outlive it immediately.
	s.Reconcile([]uint64{1, 2}, time.Nanosecond)
	time.Sleep(time.Millisecond)
	require.True(t, s.Expired(1))
	require.True(t, s.Expired(2))
	require.False(t, s.Expired(3), "a partition that was never tracked is not expired")
}

func TestState_MarkReceived(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1, 2, 3}, time.Second)

	s.MarkReceived(2)
	require.False(t, s.PendingEmpty())

	s.MarkReceived(1)
	s.MarkReceived(3)
	require.True(t, s.PendingEmpty())
	require.True(t, s.IsDone())
}

func TestState_MarkReceived_UnknownPartition(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1}, time.Second)
	s.MarkReceived(99) // not in pending
	require.False(t, s.PendingEmpty())
	s.MarkReceived(1)
	require.True(t, s.PendingEmpty())
}

func TestState_Reconcile_AfterDone(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1}, time.Second)
	s.MarkReceived(1)
	require.True(t, s.IsDone())

	s.Reconcile([]uint64{2, 3}, time.Second)
	require.False(t, s.IsDone())
	require.False(t, s.PendingEmpty())
}

func TestState_Reconcile_PreservesDeadlines(t *testing.T) {
	// The escape clock of an already-pending partition must not restart when
	// the pending set is reconciled again: churn faster than the escape delay
	// must not starve expiry.
	s := New()
	escape := 100 * time.Millisecond
	s.Reconcile([]uint64{1, 2}, escape)

	deadline := time.Now().Add(escape)
	// Reconcile repeatedly, always faster than the escape delay.
	for time.Now().Before(deadline.Add(50 * time.Millisecond)) {
		s.Reconcile([]uint64{1, 2}, escape)
		if s.PendingEmpty() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, s.PendingEmpty(),
		"partitions must escape after their original deadline despite continuous reconciles")
	require.True(t, s.IsDone())

	// An escaped partition must stay escaped: a later Reconcile with the same
	// IDs must not resurrect it with a fresh clock.
	s.Reconcile([]uint64{1, 2}, escape)
	require.True(t, s.PendingEmpty(),
		"reconcile must not restart the escape clock of an already-escaped partition")
	require.True(t, s.IsDone())
}

func TestState_Reconcile_DropsStaleEntries(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1, 2, 3}, time.Second)
	// Partition 3 is no longer pending receive.
	s.Reconcile([]uint64{1, 2}, time.Second)
	s.MarkReceived(1)
	s.MarkReceived(2)
	require.True(t, s.PendingEmpty())
	require.True(t, s.IsDone())
}

func TestState_LazyExpiry_PendingEmpty(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1, 2}, 20*time.Millisecond)
	require.False(t, s.PendingEmpty())

	time.Sleep(40 * time.Millisecond)
	require.True(t, s.PendingEmpty(), "expired partitions must be pruned lazily")
	require.True(t, s.IsDone())
}

func TestState_Expiry_SignalsDoneWithoutPolling(t *testing.T) {
	// Done must fire even when nothing polls PendingEmpty: WaitForInitialSync
	// and the dmap initial-sync publisher only wait on the channel.
	s := New()
	s.Reconcile([]uint64{1, 2, 3}, 20*time.Millisecond)
	require.False(t, s.IsDone())

	select {
	case <-s.Done():
		require.True(t, s.IsDone())
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Done() should close once the escape deadline elapses")
	}
}

func TestState_Expiry_MixedDeadlines(t *testing.T) {
	// A later Reconcile adds a new partition with a later deadline; the
	// backstop timer must fire for both, in order.
	s := New()
	s.Reconcile([]uint64{1}, 30*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	s.Reconcile([]uint64{1, 2}, 30*time.Millisecond)

	select {
	case <-s.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Done() should close once every escape deadline has elapsed")
	}
	require.True(t, s.PendingEmpty())
}

func TestState_EscapeIsPerPartition_NoPrematureClear(t *testing.T) {
	// Deadlines are per partition: a partition that just became pending must
	// not be cleared when an older partition's deadline elapses. The previous
	// implementation cleared the whole pending set with a single timer, so a
	// genuinely-transferring partition could be acked before it received any
	// data.
	s := New()
	escape := 100 * time.Millisecond
	s.Reconcile([]uint64{1}, escape)
	time.Sleep(80 * time.Millisecond)
	s.Reconcile([]uint64{1, 2}, escape)

	// Partition 1 expires; partition 2 is only ~40ms old and must survive.
	time.Sleep(40 * time.Millisecond)
	require.False(t, s.PendingEmpty(),
		"a fresh partition must not be cleared by an older partition's deadline")
	require.False(t, s.IsDone())

	// Partition 2 escapes once its own deadline elapses.
	require.Eventually(t, func() bool { return s.IsDone() }, time.Second, 5*time.Millisecond)
}

func TestState_ZeroEscape_NeverExpires(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1}, 0)
	time.Sleep(20 * time.Millisecond)
	require.False(t, s.PendingEmpty())
	require.False(t, s.IsDone())
	s.MarkReceived(1)
	require.True(t, s.IsDone())
}

func TestState_Done_ConcurrentWithReconcile(t *testing.T) {
	s := New()
	s.Reconcile([]uint64{1, 2, 3}, time.Second)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				case <-s.Done():
					s.IsDone()
				}
			}
		})
	}

	for i := range 1000 {
		s.Reconcile([]uint64{uint64(i)}, time.Second)
		s.MarkReceived(uint64(i))
	}
	close(stop)
	wg.Wait()
	require.True(t, s.IsDone())
}
