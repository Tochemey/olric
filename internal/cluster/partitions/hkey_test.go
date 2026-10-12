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

package partitions

import (
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/hasher"
)

// fnvHasher hashes with FNV-1a so tests can compare it with the default hasher.
type fnvHasher struct{}

// Sum64 returns the 64-bit FNV-1a hash of key.
func (fnvHasher) Sum64(key []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(key)
	return h.Sum64()
}

func TestPartitions_HKey(t *testing.T) {
	hkey := HKey(hasher.NewDefaultHasher(), "storage-unit-name", "some-key")
	require.NotEqualf(t, 0, hkey, "HKey is zero. This shouldn't be normal")
}

// Regression test for https://github.com/Tochemey/olric/issues/53. HKey must
// hash with the hasher it is given, so instances with different hashers in one
// process stay independent.
func TestPartitions_HKey_UsesGivenHasher(t *testing.T) {
	defaultHasher := hasher.NewDefaultHasher()
	custom := fnvHasher{}

	require.Equal(t, defaultHasher.Sum64([]byte("storage-unit-namesome-key")), HKey(defaultHasher, "storage-unit-name", "some-key"))
	require.Equal(t, custom.Sum64([]byte("storage-unit-namesome-key")), HKey(custom, "storage-unit-name", "some-key"))
	require.NotEqual(t, HKey(defaultHasher, "storage-unit-name", "some-key"), HKey(custom, "storage-unit-name", "some-key"))
}
