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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDMaps_Sanitize_Defaults(t *testing.T) {
	dm := &DMaps{
		MaxInuse:   -5,
		MaxKeys:    -5,
		LRUSamples: -5,
		Custom: map[string]DMap{
			"cache": {MaxInuse: -1, MaxKeys: -1, LRUSamples: -1},
		},
	}
	require.NoError(t, dm.Sanitize())
	require.NoError(t, dm.Validate())

	require.Equal(t, 0, dm.MaxInuse)
	require.Equal(t, 0, dm.MaxKeys)
	require.Equal(t, DefaultLRUSamples, dm.LRUSamples)
	require.Equal(t, EvictionPolicy("NONE"), dm.EvictionPolicy)
	require.Greater(t, dm.NumEvictionWorkers, int64(0))
	require.Equal(t, DefaultCheckEmptyFragmentsInterval, dm.CheckEmptyFragmentsInterval)
	require.Equal(t, DefaultTriggerCompactionInterval, dm.TriggerCompactionInterval)
	require.NotNil(t, dm.Engine)

	// Custom entries are sanitized via a copy, so the stored map value is
	// untouched. This documents the current behaviour.
	custom := dm.Custom["cache"]
	require.Equal(t, -1, custom.LRUSamples)
}

func TestDMaps_Sanitize_PreservesValues(t *testing.T) {
	dm := &DMaps{
		NumEvictionWorkers:          4,
		LRUSamples:                  9,
		EvictionPolicy:              LRUEviction,
		CheckEmptyFragmentsInterval: 2 * time.Minute,
		TriggerCompactionInterval:   3 * time.Minute,
	}
	require.NoError(t, dm.Sanitize())
	require.Equal(t, int64(4), dm.NumEvictionWorkers)
	require.Equal(t, 9, dm.LRUSamples)
	require.Equal(t, LRUEviction, dm.EvictionPolicy)
	require.Equal(t, 2*time.Minute, dm.CheckEmptyFragmentsInterval)
	require.Equal(t, 3*time.Minute, dm.TriggerCompactionInterval)
}
