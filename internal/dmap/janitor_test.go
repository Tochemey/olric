/*
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/testcluster"
)

// TestJanitor_SkipsFragmentWithWriteInFlight guards that the janitor leaves
// an empty fragment alone while a write is between its preparation and its
// store: wiping it then would detach the store the write lands in.
func TestJanitor_SkipsFragmentWithWriteInFlight(t *testing.T) {
	cluster := testcluster.New(NewService)
	defer cluster.Shutdown()
	s := cluster.AddMember(nil).(*Service)

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)
	part := s.primary.PartitionByID(0)
	f, err := dm.loadOrCreateFragment(part)
	require.NoError(t, err)

	f.inFlight.Add(1)
	s.janitor(part)
	_, ok := part.Map().Load("dmap.mydmap")
	require.True(t, ok, "a fragment with a write in flight is kept")

	f.inFlight.Add(-1)
	s.janitor(part)
	_, ok = part.Map().Load("dmap.mydmap")
	require.False(t, ok, "an empty fragment with no write in flight is wiped")
}
