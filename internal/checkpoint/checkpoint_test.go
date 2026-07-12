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

package checkpoint

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpoint(t *testing.T) {
	cp := New()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cp.Add()
		}()
	}

	wg.Wait()
	require.False(t, cp.AllPassed())

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cp.Pass()
		}()
	}

	wg.Wait()
	require.True(t, cp.AllPassed())
}

func TestCheckpoint_Isolated(t *testing.T) {
	// A checkpoint with an unmet requirement must not affect a fresh one.
	failed := New()
	failed.Add()
	require.False(t, failed.AllPassed())

	healthy := New()
	healthy.Add()
	healthy.Pass()
	require.True(t, healthy.AllPassed())
}
