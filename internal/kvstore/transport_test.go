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

package kvstore

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/kvstore/entry"
	"github.com/tochemey/olric/internal/kvstore/table"
	"github.com/tochemey/olric/pkg/storage"
)

func TestTransferIterator_Drop_Empty(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	it := kv.TransferIterator()
	require.False(t, it.Next())

	err = it.Drop(0)
	require.Error(t, err)
}

func TestTransferIterator_Export_Empty(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	it := kv.TransferIterator()
	_, _, err = it.Export()
	require.Error(t, err)
}

// TestTransferIterator_ExportFrom_KeepsTables checks that the tables are
// walked one at a time by position, skipping recycled ones, that the store
// keeps every table, and that the copies import in full.
func TestTransferIterator_ExportFrom_KeepsTables(t *testing.T) {
	kv := testKVStore(t, nil).(*KVStore)

	put := func(i int) {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		require.NoError(t, kv.Put(xxhash.Sum64([]byte(e.Key())), e))
	}

	for i := range 10 {
		put(i)
	}

	// A second table, so the copy has to walk past the first one.
	require.NoError(t, kv.makeTable())
	for i := 10; i < 20; i++ {
		put(i)
	}

	require.Len(t, kv.tables, 2)

	it := kv.TransferIterator()
	r, ok := it.(storage.Replicator)
	require.True(t, ok, "the kvstore transfer iterator must implement storage.Replicator")

	var payloads [][]byte
	for index := 0; ; {
		data, next, err := r.ExportFrom(index)
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		require.Greater(t, next, index, "the position must advance")
		payloads = append(payloads, data)
		index = next
	}
	require.Len(t, payloads, 2, "one payload per live table")

	require.Len(t, kv.tables, 2, "the tables must stay in the store")
	require.True(t, it.Next())
	require.Equal(t, 20, kv.Stats().Length)

	// A recycled table is skipped and the walk ends after the last one.
	kv.tables[0].SetState(table.RecycledState)
	data, next, err := r.ExportFrom(0)
	require.NoError(t, err)
	require.Equal(t, 2, next, "the recycled first table is skipped")
	require.NotEmpty(t, data)
	_, _, err = r.ExportFrom(next)
	require.ErrorIs(t, err, io.EOF)

	target := testKVStore(t, nil)
	imported := 0
	for _, data := range payloads {
		require.NoError(t, target.Import(data, func(hkey uint64, e storage.Entry) error {
			imported++
			return target.Put(hkey, e)
		}))
	}
	require.Equal(t, 20, imported)
	require.Equal(t, 20, target.Stats().Length)
}

// TestKVStore_Import_ReturnsCallbackError checks that an import whose merge
// callback fails reports the failure instead of answering as if every entry
// had been merged.
func TestKVStore_Import_ReturnsCallbackError(t *testing.T) {
	kv := testKVStore(t, nil).(*KVStore)
	for i := range 5 {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		require.NoError(t, kv.Put(xxhash.Sum64([]byte(e.Key())), e))
	}

	data, _, err := kv.TransferIterator().Export()
	require.NoError(t, err)

	failure := errors.New("merge failed")
	calls := 0
	err = testKVStore(t, nil).Import(data, func(uint64, storage.Entry) error {
		calls++
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, calls, "the import stops at the first failure")
}
