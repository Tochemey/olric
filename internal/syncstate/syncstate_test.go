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

func TestState_Reset_Empty(t *testing.T) {
	s := New()
	s.Reset(nil)
	require.True(t, s.PendingEmpty())
	require.True(t, s.IsDone())
	select {
	case <-s.Done():
	default:
		t.Fatal("Done() channel should be closed when reset with empty")
	}
}

func TestState_Reset_WithPartitions(t *testing.T) {
	s := New()
	s.Reset([]uint64{1, 2, 3})
	require.False(t, s.PendingEmpty())
	require.False(t, s.IsDone())
}

func TestState_MarkReceived(t *testing.T) {
	s := New()
	s.Reset([]uint64{1, 2, 3})

	s.MarkReceived(2)
	require.False(t, s.PendingEmpty())

	s.MarkReceived(1)
	s.MarkReceived(3)
	require.True(t, s.PendingEmpty())
	require.True(t, s.IsDone())
}

func TestState_MarkReceived_UnknownPartition(t *testing.T) {
	s := New()
	s.Reset([]uint64{1})
	s.MarkReceived(99) // not in pending
	require.False(t, s.PendingEmpty())
	s.MarkReceived(1)
	require.True(t, s.PendingEmpty())
}

func TestState_Reset_AfterDone(t *testing.T) {
	s := New()
	s.Reset([]uint64{1})
	s.MarkReceived(1)
	require.True(t, s.IsDone())

	s.Reset([]uint64{2, 3})
	require.False(t, s.IsDone())
	require.False(t, s.PendingEmpty())
}

func TestState_StartEmptyPartitionTimeout(t *testing.T) {
	s := New()
	s.Reset([]uint64{1, 2, 3})
	require.False(t, s.IsDone())

	s.StartEmptyPartitionTimeout(10 * time.Millisecond)
	select {
	case <-s.Done():
		require.True(t, s.IsDone())
		require.True(t, s.PendingEmpty())
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Done() should close within timeout")
	}
}

func TestState_Done_ConcurrentWithReset(t *testing.T) {
	s := New()
	s.Reset([]uint64{1, 2, 3})

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
		s.Reset([]uint64{uint64(i)})
		s.MarkReceived(uint64(i))
	}
	close(stop)
	wg.Wait()
	require.True(t, s.IsDone())
}

func TestState_StartEmptyPartitionTimeout_ZeroNoOp(t *testing.T) {
	s := New()
	s.Reset([]uint64{1})
	s.StartEmptyPartitionTimeout(0)
	require.False(t, s.IsDone())
}
