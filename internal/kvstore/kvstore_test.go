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
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/pkg/storage"

	"github.com/tochemey/olric/internal/kvstore/entry"
	"github.com/tochemey/olric/internal/kvstore/table"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func testKVStore(t *testing.T, c *storage.Config) storage.Engine {
	kv, err := New(c)
	require.NoError(t, err)

	child, err := kv.Fork(nil)
	require.NoError(t, err)

	err = child.Start()
	require.NoError(t, err)

	return child
}

func TestKVStore_Put(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTTL(int64(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}
}

func TestKVStore_Get(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := s.Get(hkey)
		require.NoError(t, err)

		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, int64(i), e.TTL())
		require.Equal(t, bval(i), e.Value())
		require.Equal(t, timestamp, e.Timestamp())
	}
}

func TestKVStore_Delete(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	garbage := make(map[int]uint64)
	for i, tb := range s.(*KVStore).tables {
		s := tb.Stats()
		garbage[i] = s.Inuse
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)

		_, err = s.Get(hkey)
		require.ErrorIs(t, err, storage.ErrKeyNotFound)
	}

	for i, tb := range s.(*KVStore).tables {
		s := tb.Stats()
		require.Equal(t, uint64(0), s.Inuse)
		require.Equal(t, 0, s.Length)
		require.Equal(t, garbage[i], s.Garbage)
	}
}

func TestKVStore_ExportImport(t *testing.T) {
	timestamp := time.Now().UnixNano()
	s := testKVStore(t, nil)

	for i := 0; i < 1000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	fresh := testKVStore(t, nil)

	ti := s.TransferIterator()
	for ti.Next() {
		data, index, err := ti.Export()
		require.NoError(t, err)

		err = fresh.Import(data, func(u uint64, e storage.Entry) error {
			return fresh.Put(u, e)
		})
		require.NoError(t, err)

		err = ti.Drop(index)
		require.NoError(t, err)
	}

	_, _, err := ti.Export()
	require.ErrorIs(t, err, io.EOF)

	for i := 0; i < 1000; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := fresh.Get(hkey)
		require.NoError(t, err)
		require.Equal(t, bkey(i), e.Key())
		require.Equal(t, int64(i), e.TTL())
		require.Equal(t, bval(i), e.Value())
		require.Equal(t, timestamp, e.Timestamp())
	}
}

func TestKVStore_Stats_Length(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	require.Equal(t, 100, s.Stats().Length)
}

func TestKVStore_Range(t *testing.T) {
	s := testKVStore(t, nil)

	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)

		hkeys[hkey] = struct{}{}
	}

	s.Range(func(hkey uint64, entry storage.Entry) bool {
		_, ok := hkeys[hkey]
		require.Truef(t, ok, "Invalid hkey: %d", hkey)
		return true
	})
}

func TestKVStore_Check(t *testing.T) {
	s := testKVStore(t, nil)

	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)

		hkeys[hkey] = struct{}{}
	}

	for hkey := range hkeys {
		require.Truef(t, s.Check(hkey), "hkey could not be found: %d", hkey)
	}
}

func TestKVStore_UpdateTTL(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(10)
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.UpdateTTL(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := s.Get(hkey)
		require.NoError(t, err)

		if e.Key() != bkey(i) {
			t.Fatalf("Expected key: %s. Got %s", bkey(i), e.Key())
		}
		if e.TTL() != 10 {
			t.Fatalf("Expected ttl: %d. Got %v", i, e.TTL())
		}
	}
}

func TestKVStore_GetKey(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))
	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	key, err := s.GetKey(hkey)
	require.NoError(t, err)

	if key != bkey(1) {
		t.Fatalf("Expected %s. Got %v", bkey(1), key)
	}
}

func TestKVStore_PutRawGetRaw(t *testing.T) {
	s := testKVStore(t, nil)

	value := []byte("value")
	hkey := xxhash.Sum64([]byte("key"))
	err := s.PutRaw(hkey, value)
	require.NoError(t, err)

	rawval, err := s.GetRaw(hkey)
	require.NoError(t, err)

	if bytes.Equal(value, rawval) {
		t.Fatalf("Expected %s. Got %v", value, rawval)
	}
}

func TestKVStore_GetTTL(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))

	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	ttl, err := s.GetTTL(hkey)
	require.NoError(t, err)

	if ttl != e.TTL() {
		t.Fatalf("Expected TTL %d. Got %d", ttl, e.TTL())
	}
}

func TestKVStore_GetLastAccess(t *testing.T) {
	s := testKVStore(t, nil)

	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))

	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	lastAccess, err := s.GetLastAccess(hkey)
	require.NoError(t, err)
	require.NotEqual(t, 0, lastAccess)
}

func TestKVStore_Fork(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	child, err := s.Fork(nil)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		_, err = child.Get(hkey)
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Expected storage.ErrKeyNotFound. Got %v", err)
		}
	}

	stats := child.Stats()
	if uint64(stats.Allocated) != defaultTableSize {
		t.Fatalf("Expected Stats.Allocated: %d. Got: %d", defaultTableSize, stats.Allocated)
	}

	if stats.Inuse != 0 {
		t.Fatalf("Expected Stats.Inuse: 0. Got: %d", stats.Inuse)
	}

	if stats.Garbage != 0 {
		t.Fatalf("Expected Stats.Garbage: 0. Got: %d", stats.Garbage)
	}

	if stats.Length != 0 {
		t.Fatalf("Expected Stats.Length: 0. Got: %d", stats.Length)
	}

	if stats.NumTables != 1 {
		t.Fatalf("Expected Stats.NumTables: 1. Got: %d", stats.NumTables)
	}
}

func TestKVStore_StateChange(t *testing.T) {
	s := testKVStore(t, nil)

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 100000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i, tb := range s.(*KVStore).tables {
		if tb.State() == table.ReadWriteState {
			require.Equalf(t, len(s.(*KVStore).tables)-1, i, "Writable table has to be the latest table")
		} else if tb.State() == table.ReadOnlyState {
			require.True(t, i < len(s.(*KVStore).tables)-1)
		}
	}
}

func TestKVStore_NewEntry(t *testing.T) {
	s := testKVStore(t, nil)

	i := s.NewEntry()
	_, ok := i.(*entry.Entry)
	require.True(t, ok)
}

func TestKVStore_Name(t *testing.T) {
	s := testKVStore(t, nil)
	require.Equal(t, "kvstore", s.Name())
}

func TestKVStore_CloseDestroy(t *testing.T) {
	s := testKVStore(t, nil)
	require.NoError(t, s.Close())
	require.NoError(t, s.Destroy())
}

func TestStorage_Scan(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 1000000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	var (
		count  int
		cursor uint64
		err    error
	)
	k := s.(*KVStore)
	for {
		cursor, err = k.Scan(cursor, 10, func(e storage.Entry) bool {
			count++
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 1000000, count)
}

func TestStorage_ScanRegexMatch(t *testing.T) {
	s := testKVStore(t, nil)

	var key string
	for i := 0; i < 1000000; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}

		e := entry.New()
		e.SetKey(key)
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	var (
		count  int
		cursor uint64
		err    error
	)
	k := s.(*KVStore)
	for {
		cursor, err = k.ScanRegexMatch(cursor, "even:", 10, func(entry storage.Entry) bool {
			count++
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 500000, count)
}

func TestStorage_ScanRegexMatch_OnlyOneEntry(t *testing.T) {
	s := testKVStore(t, nil)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	e := entry.New()
	e.SetKey("even:200")
	e.SetTTL(123123)
	e.SetValue([]byte("my-value"))
	e.SetTimestamp(time.Now().UnixNano())
	hkey := xxhash.Sum64([]byte(e.Key()))
	err := s.Put(hkey, e)
	require.NoError(t, err)

	var (
		num    int
		count  int
		cursor uint64
	)
	k := s.(*KVStore)
	for {
		num += 1
		cursor, err = k.ScanRegexMatch(cursor, "even:", 10, func(entry storage.Entry) bool {
			count++
			require.Equal(t, "even:200", e.Key())
			require.Equal(t, "my-value", string(e.Value()))
			return true
		})
		require.NoError(t, err)
		if cursor == 0 {
			break
		}
	}

	require.Equal(t, 1, num)
	require.Equal(t, 1, count)
}

func TestKVStore_Put_ErrEntryTooLarge(t *testing.T) {
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testKVStore(t, c)
	value := make([]byte, 2048)
	e := entry.New()
	e.SetKey("key")
	e.SetValue(value)
	e.SetTTL(10)
	e.SetTimestamp(time.Now().UnixNano())
	hkey := xxhash.Sum64([]byte(e.Key()))

	err := s.Put(hkey, e)
	require.ErrorIs(t, err, storage.ErrEntryTooLarge)
}

func TestKVStore_New_MissingTableSize(t *testing.T) {
	c := storage.NewConfig(nil)
	// No "tableSize" key configured -> Get must fail.
	_, err := New(c)
	require.Error(t, err)
}

func TestKVStore_New_InvalidTableSizeType(t *testing.T) {
	c := storage.NewConfig(nil)
	c.Add("tableSize", "not-a-number")
	_, err := New(c)
	require.Error(t, err)
}

func TestKVStore_New_NilConfigUsesDefault(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)
	require.NotNil(t, kv)
	require.Equal(t, defaultTableSize, kv.tableSize)
}

func TestKVStore_SetConfig(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	newCfg := DefaultConfig()
	kv.SetConfig(newCfg)
	require.Same(t, newCfg, kv.config)
}

func TestKVStore_SetLogger(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)
	// SetLogger is a no-op but must remain callable without panicking.
	require.NotPanics(t, func() {
		kv.SetLogger(log.Default())
	})
}

func TestKVStore_Start_NilConfig(t *testing.T) {
	kv := &KVStore{}
	err := kv.Start()
	require.Error(t, err)
}

func TestKVStore_Start_OK(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)
	require.NoError(t, kv.Start())
}

func TestKVStore_PrepareTableSize(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want uint64
	}{
		{"uint", uint(10), 10},
		{"uint8", uint8(10), 10},
		{"uint16", uint16(10), 10},
		{"uint32", uint32(10), 10},
		{"uint64", uint64(10), 10},
		{"int", int(10), 10},
		{"int8", int8(10), 10},
		{"int16", int16(10), 10},
		{"int32", int32(10), 10},
		{"int64", int64(10), 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := prepareTableSize(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("invalid", func(t *testing.T) {
		_, err := prepareTableSize("bad")
		require.Error(t, err)
	})
}

func TestKVStore_DefaultTableSize(t *testing.T) {
	require.Equal(t, defaultTableSize, DefaultTableSize())
}

func TestKVStore_PrepareTableSize_Exported(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		got, err := PrepareTableSize(int(4096))
		require.NoError(t, err)
		require.Equal(t, uint64(4096), got)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := PrepareTableSize("bad")
		require.Error(t, err)
	})
}

func TestKVStore_PutRaw_ErrEntryTooLarge(t *testing.T) {
	c := DefaultConfig()
	c.Add("tableSize", 1024)
	s := testKVStore(t, c)

	err := s.PutRaw(xxhash.Sum64([]byte("key")), make([]byte, 2048))
	require.ErrorIs(t, err, storage.ErrEntryTooLarge)
}

func TestKVStore_PutRaw_EmptyTables(t *testing.T) {
	// New KVStore with no tables yet: PutRaw must create the first table.
	kv, err := New(nil)
	require.NoError(t, err)
	require.Len(t, kv.tables, 0)

	err = kv.PutRaw(xxhash.Sum64([]byte("key")), []byte("value"))
	require.NoError(t, err)
	require.Len(t, kv.tables, 1)
}

func TestKVStore_Put_EmptyTables(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	e := entry.New()
	e.SetKey("key")
	e.SetValue([]byte("value"))
	e.SetTimestamp(time.Now().UnixNano())
	require.NoError(t, kv.Put(xxhash.Sum64([]byte("key")), e))
	require.Len(t, kv.tables, 1)
}

func TestKVStore_NotFound_Errors(t *testing.T) {
	s := testKVStore(t, nil)
	missing := xxhash.Sum64([]byte("does-not-exist"))

	_, err := s.GetRaw(missing)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	_, err = s.Get(missing)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	_, err = s.GetTTL(missing)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	_, err = s.GetLastAccess(missing)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	_, err = s.GetKey(missing)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	e := entry.New()
	e.SetKey("does-not-exist")
	e.SetTTL(10)
	err = s.UpdateTTL(missing, e)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	// Delete of a missing key must not return an error.
	require.NoError(t, s.Delete(missing))

	require.False(t, s.Check(missing))
}

func TestKVStore_RangeHKey(t *testing.T) {
	s := testKVStore(t, nil)

	hkeys := make(map[uint64]struct{})
	for i := 0; i < 50; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		require.NoError(t, s.Put(hkey, e))
		hkeys[hkey] = struct{}{}
	}

	var visited int
	s.(*KVStore).RangeHKey(func(hkey uint64) bool {
		_, ok := hkeys[hkey]
		require.True(t, ok)
		visited++
		return true
	})
	require.Equal(t, len(hkeys), visited)
}

func TestKVStore_RangeHKey_EarlyStop(t *testing.T) {
	s := testKVStore(t, nil)
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		require.NoError(t, s.Put(xxhash.Sum64([]byte(e.Key())), e))
	}

	var visited int
	s.(*KVStore).RangeHKey(func(hkey uint64) bool {
		visited++
		return false
	})
	require.Equal(t, 1, visited)
}

func TestKVStore_Scan_EmptyTables(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)
	// No tables yet: scanCommon must short-circuit and return cursor 0.
	cursor, err := kv.Scan(0, 10, func(e storage.Entry) bool { return true })
	require.NoError(t, err)
	require.Equal(t, uint64(0), cursor)
}

func TestKVStore_Scan_InvalidCursor(t *testing.T) {
	s := testKVStore(t, nil)
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		require.NoError(t, s.Put(xxhash.Sum64([]byte(e.Key())), e))
	}

	k := s.(*KVStore)
	// A cursor far beyond the highest coefficient resolves to an invalid
	// coefficient and returns 0 without error.
	cursor, err := k.Scan(k.tableSize*1000, 10, func(e storage.Entry) bool { return true })
	require.NoError(t, err)
	require.Equal(t, uint64(0), cursor)
}

func TestKVStore_FindCoefficient_EOF(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)
	require.NoError(t, kv.makeTable())

	// Coefficient 0 exists; searching beyond it must return io.EOF.
	_, err = kv.findCoefficient(100)
	require.Error(t, err)
}

// TestKVStore_MakeTable_ReuseRecycled drives a compaction that recycles an
// emptied table and then writes more data so makeTable reuses the recycled
// table instead of allocating a new one.
func TestKVStore_MakeTable_ReuseRecycled(t *testing.T) {
	s := testKVStore(t, nil)
	k := s.(*KVStore)

	put := func(i int) {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTimestamp(time.Now().UnixNano())
		require.NoError(t, s.Put(xxhash.Sum64([]byte(e.Key())), e))
	}

	// Fill enough to allocate more than one table.
	for i := 0; i < 1500; i++ {
		put(i)
	}
	require.GreaterOrEqual(t, len(k.tables), 2)

	// Empty the first table.
	for i := 0; i < 750; i++ {
		require.NoError(t, s.Delete(xxhash.Sum64([]byte(bkey(i)))))
	}

	for {
		done, err := s.Compaction()
		require.NoError(t, err)
		if done {
			break
		}
	}

	// A recycled table must now exist.
	var recycled bool
	for _, tb := range k.tables {
		if tb.State() == table.RecycledState {
			recycled = true
		}
	}
	require.True(t, recycled)

	tablesBefore := len(k.tables)
	// Write more data; makeTable should reuse the recycled table.
	for i := 1500; i < 4000; i++ {
		put(i)
	}
	// Reuse means the table count should not grow unbounded past reuse.
	require.LessOrEqual(t, len(k.tables), tablesBefore+2)
}

func TestKVStore_Fork_InvalidConfig(t *testing.T) {
	kv, err := New(nil)
	require.NoError(t, err)

	bad := storage.NewConfig(nil)
	bad.Add("tableSize", "invalid")
	_, err = kv.Fork(bad)
	require.Error(t, err)
}
