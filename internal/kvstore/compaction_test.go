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
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/kvstore/entry"
	"github.com/tochemey/olric/internal/kvstore/table"
)

func TestKVStore_Compaction(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 750; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	var compacted bool
	for _, tb := range s.(*KVStore).tables {
		stats := tb.Stats()
		if stats.Inuse == 0 {
			require.Equal(t, table.RecycledState, tb.State())
			compacted = true
		} else {
			require.Equal(t, 750, stats.Length)
			require.Equal(t, table.ReadWriteState, tb.State())
		}
	}

	require.Truef(t, compacted, "Compaction could not work properly")
}

func TestKVStore_Compaction_MaxIdleTableDuration(t *testing.T) {
	c := DefaultConfig()
	c.Add("maxIdleTableTimeout", time.Millisecond)

	s := testKVStore(t, c)

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 1500; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	require.Equal(t, 2, len(s.(*KVStore).tables))

	for i := 0; i < 800; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)
	}

	// It's still two because we have not triggered the compaction yet.
	require.Equal(t, 2, len(s.(*KVStore).tables))

	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	<-time.After(100 * time.Millisecond)

	// Be sure deletion of the idle table.
	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	require.Equal(t, 1, len(s.(*KVStore).tables))
}

func compactUntilDone(t *testing.T, s *KVStore) {
	t.Helper()
	for i := 0; i < 100; i++ {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			return
		}
	}
	t.Fatal("compaction did not finish in 100 passes")
}

func TestKVStore_Compaction_HeadTableKeepsLiveKeys(t *testing.T) {
	s := testKVStore(t, nil).(*KVStore)

	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		require.NoError(t, s.Put(xxhash.Sum64([]byte(bkey(i))), e))
	}

	// Overwrite one key until the only table is garbage-heavy enough to be
	// compacted while it is still the head.
	hot := xxhash.Sum64([]byte("hot"))
	for i := 0; i < 5000; i++ {
		e := entry.New()
		e.SetKey("hot")
		e.SetValue([]byte(fmt.Sprintf("%0100d", i)))
		require.NoError(t, s.Put(hot, e))
	}
	require.Len(t, s.tables, 1)
	require.True(t, s.isCompactionOK(s.tables[0]))

	compactUntilDone(t, s)

	for i := 0; i < 10; i++ {
		e, err := s.Get(xxhash.Sum64([]byte(bkey(i))))
		require.NoError(t, err)
		require.Equal(t, bval(i), e.Value())
	}
	e, err := s.Get(hot)
	require.NoError(t, err)
	require.Equal(t, []byte(fmt.Sprintf("%0100d", 4999)), e.Value())
	require.Equal(t, 11, s.Stats().Length)
}

func TestKVStore_Compaction_DoesNotRestoreOverwrittenValue(t *testing.T) {
	s := testKVStore(t, nil).(*KVStore)

	put := func(key string, value []byte) {
		e := entry.New()
		e.SetKey(key)
		e.SetValue(value)
		require.NoError(t, s.Put(xxhash.Sum64([]byte(key)), e))
	}

	put("a", []byte("old"))
	for i := 0; len(s.tables) < 2; i++ {
		put(bkey(i), []byte(fmt.Sprintf("%01000d", i)))
	}
	// "a" now lives in a read-only table; the new version goes to the head.
	put("a", []byte("new"))

	// Make the old table garbage-heavy so compaction evicts it.
	for i := 0; i < 600; i++ {
		require.NoError(t, s.Delete(xxhash.Sum64([]byte(bkey(i)))))
	}

	compactUntilDone(t, s)

	e, err := s.Get(xxhash.Sum64([]byte("a")))
	require.NoError(t, err)
	require.Equal(t, []byte("new"), e.Value())
}

func TestKVStore_PutRaw_OverwriteAcrossTablesLeavesOneVersion(t *testing.T) {
	s := testKVStore(t, nil).(*KVStore)
	src := testKVStore(t, nil).(*KVStore)

	hkey := xxhash.Sum64([]byte("k"))
	for i := 0; i < 20000; i++ {
		e := entry.New()
		e.SetKey("k")
		e.SetValue([]byte(fmt.Sprintf("%0100d", i)))
		require.NoError(t, src.Put(hkey, e))
		raw, err := src.GetRaw(hkey)
		require.NoError(t, err)
		require.NoError(t, s.PutRaw(hkey, raw))
	}
	// ~2.6 MB written into 1 MB tables: the key crossed table boundaries.
	require.Greater(t, len(s.tables), 1)

	var versions int
	s.RangeHKey(func(h uint64) bool {
		if h == hkey {
			versions++
		}
		return true
	})
	require.Equal(t, 1, versions)
	require.Equal(t, 1, s.Stats().Length)

	compactUntilDone(t, s)

	e, err := s.Get(hkey)
	require.NoError(t, err)
	require.Equal(t, []byte(fmt.Sprintf("%0100d", 19999)), e.Value())
	require.Equal(t, 1, s.Stats().Length)
}
