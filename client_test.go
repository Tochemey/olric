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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/dmap"
	"github.com/tochemey/olric/pkg/storage"
)

func TestStorageEntryImplementation_Option(t *testing.T) {
	cfg := &dmapConfig{}
	require.Nil(t, cfg.storageEntryImplementation)

	opt := StorageEntryImplementation(func() storage.Entry { return nil })
	opt(cfg)

	require.NotNil(t, cfg.storageEntryImplementation)
}

func TestCount_Option(t *testing.T) {
	cfg := &dmap.ScanConfig{}
	require.False(t, cfg.HasCount)

	opt := Count(42)
	opt(cfg)

	require.True(t, cfg.HasCount)
	require.Equal(t, 42, cfg.Count)
}

func TestPipelineConcurrency_Option(t *testing.T) {
	dp := &DMapPipeline{}
	opt := PipelineConcurrency(7)
	opt(dp)

	require.Equal(t, 7, dp.concurrency)
}
