/*
 * Copyright 2025 Arsene Tochemey Gandote
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 */

package buntstore

import (
    "bytes"
    "errors"
    "fmt"
    "io"
    "log"
    "github.com/tidwall/buntdb"
    "strconv"
    "strings"
    "testing"
    "time"

    "github.com/cespare/xxhash/v2"
    "github.com/stretchr/testify/require"

    "github.com/tochemey/olric/internal/kvstore/entry"
    "github.com/tochemey/olric/pkg/storage"
)

func bkey(i int) string { return fmt.Sprintf("%09d", i) }
func bval(i int) []byte { return []byte(fmt.Sprintf("%025d", i)) }

func testBuntStore(t *testing.T, c *storage.Config) storage.Engine {
    t.Helper()
    bs, err := New(c)
    require.NoError(t, err)
    child, err := bs.Fork(nil)
    require.NoError(t, err)
    require.NoError(t, child.Start())
    return child
}

func TestBuntStore_Put_Get(t *testing.T) {
    s := testBuntStore(t, nil)
    timestamp := time.Now().UnixNano()
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(timestamp)
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    for i := range 100 {
        hkey := xxhash.Sum64([]byte(bkey(i)))
        e, err := s.Get(hkey)
        require.NoError(t, err)
        require.Equal(t, bkey(i), e.Key())
        require.Equal(t, int64(i), e.TTL())
        require.Equal(t, bval(i), e.Value())
        require.Equal(t, timestamp, e.Timestamp())
    }
}

func TestBuntStore_Setters_StartTwice(t *testing.T) {
    bs, err := New(nil)
    require.NoError(t, err)
    // exercise setters
    bs.SetConfig(DefaultConfig())
    bs.SetLogger(log.New(io.Discard, "", 0))
    // start twice
    require.NoError(t, bs.Start())
    require.NoError(t, bs.Start())
    require.NoError(t, bs.Close())
}

func TestBuntStore_PutRaw_NotStarted(t *testing.T) {
    bs, err := New(nil)
    require.NoError(t, err)
    // don't start
    err = bs.PutRaw(1, []byte("x"))
    require.Error(t, err)
}

func TestBuntStore_Put_KeyTooLarge(t *testing.T) {
    s := testBuntStore(t, nil)
    bigKey := strings.Repeat("a", 256)
    e := entry.New()
    e.SetKey(bigKey)
    e.SetValue([]byte("v"))
    e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key()))
    err := s.Put(h, e)
    require.ErrorIs(t, err, storage.ErrKeyTooLarge)
}

func TestBuntStore_GetRaw_NotFound(t *testing.T) {
    bs, err := New(nil)
    require.NoError(t, err)
    require.NoError(t, bs.Start())
    _, err = bs.GetRaw(42)
    require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestBuntStore_GetTTL(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New()
    e.SetKey("k")
    e.SetTTL(42)
    e.SetValue([]byte("v"))
    e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key()))
    require.NoError(t, s.Put(h, e))
    ttl, err := s.GetTTL(h)
    require.NoError(t, err)
    require.Equal(t, int64(42), ttl)
}

func TestBuntStore_GetLastAccess_Fallback(t *testing.T) {
    s := testBuntStore(t, nil)
    // PutRaw without meta to force fallback path
    e := entry.New()
    e.SetKey("fallback")
    e.SetTTL(0)
    e.SetValue([]byte("v"))
    ts := time.Now().UnixNano()
    e.SetTimestamp(ts)
    la := time.Now().UnixNano() - 12345
    e.SetLastAccess(la)
    h := xxhash.Sum64([]byte(e.Key()))
    require.NoError(t, s.PutRaw(h, e.Encode()))
    got, err := s.GetLastAccess(h)
    require.NoError(t, err)
    require.Equal(t, la, got)
}

func TestBuntStore_Delete_NotFound_OK(t *testing.T) {
    s := testBuntStore(t, nil)
    require.NoError(t, s.Delete(999))
}

func TestBuntStore_UpdateTTL_KeyNotFound(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New()
    e.SetKey("missing")
    e.SetTTL(1)
    e.SetTimestamp(time.Now().UnixNano())
    err := s.UpdateTTL(12345, e)
    require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestBuntStore_TransferIterator_Empty(t *testing.T) {
    s := testBuntStore(t, nil)
    ti := s.TransferIterator()
    require.False(t, ti.Next())
}

func TestBuntStore_Import_BadPayload(t *testing.T) {
    s := testBuntStore(t, nil)
    err := s.Import([]byte("not-msgpack"), func(u uint64, e storage.Entry) error { return nil })
    require.Error(t, err)
}

func TestBuntStore_Compaction(t *testing.T) {
    s := testBuntStore(t, nil)
    done, err := s.Compaction()
    require.NoError(t, err)
    require.True(t, done)
}

func TestBuntStore_Delete(t *testing.T) {
    s := testBuntStore(t, nil)
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    for i := range 100 {
        hkey := xxhash.Sum64([]byte(bkey(i)))
        require.NoError(t, s.Delete(hkey))
        _, err := s.Get(hkey)
        require.ErrorIs(t, err, storage.ErrKeyNotFound)
    }
}

func TestBuntStore_ExportImport(t *testing.T) {
    s := testBuntStore(t, nil)
    timestamp := time.Now().UnixNano()
    for i := range 1000 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(timestamp)
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    fresh := testBuntStore(t, nil)
    ti := s.TransferIterator()
    for ti.Next() {
        data, index, err := ti.Export()
        require.NoError(t, err)
        err = fresh.Import(data, func(u uint64, e storage.Entry) error { return fresh.Put(u, e) })
        require.NoError(t, err)
        require.NoError(t, ti.Drop(index))
    }
    _, _, err := ti.Export()
    require.ErrorIs(t, err, io.EOF)
    for i := range 1000 {
        hkey := xxhash.Sum64([]byte(bkey(i)))
        e, err := fresh.Get(hkey)
        require.NoError(t, err)
        require.Equal(t, bkey(i), e.Key())
        require.Equal(t, int64(i), e.TTL())
        require.Equal(t, bval(i), e.Value())
        require.Equal(t, timestamp, e.Timestamp())
    }
}

func TestBuntStore_TransferIterator_ChunkSize(t *testing.T) {
    // set a small chunk size to force multiple chunks
    cfg := DefaultConfig()
    cfg.Add("transferChunkSize", 100)

    s0, err := New(cfg)
    require.NoError(t, err)
    s, err := s0.Fork(nil)
    require.NoError(t, err)

    // load 250 entries
    for i := 0; i < 250; i++ {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetValue(bval(i))
        e.SetTTL(0)
        e.SetTimestamp(time.Now().UnixNano())
        h := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(h, e))
    }

    ti := s.TransferIterator()
    chunks := 0
    for ti.Next() {
        data, idx, err := ti.Export()
        require.NoError(t, err)
        require.GreaterOrEqual(t, idx, 0)
        require.NotEmpty(t, data)
        require.NoError(t, ti.Drop(idx))
        chunks++
    }
    require.Equal(t, 3, chunks)
    require.Equal(t, 0, s.Stats().Length)
}

func TestBuntStore_Stats_Length(t *testing.T) {
    s := testBuntStore(t, nil)
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    require.Equal(t, 100, s.Stats().Length)
}

func TestBuntStore_Range(t *testing.T) {
    s := testBuntStore(t, nil)
    hkeys := make(map[uint64]struct{})
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
        hkeys[hkey] = struct{}{}
    }
    s.Range(func(hkey uint64, e storage.Entry) bool {
        _, ok := hkeys[hkey]
        require.Truef(t, ok, "Invalid hkey: %d", hkey)
        return true
    })
}

func TestBuntStore_Put_PreservesLastAccess(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New()
    e.SetKey("ka")
    e.SetValue([]byte("v"))
    preset := time.Now().UnixNano() - 9876
    e.SetLastAccess(preset)
    e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key()))
    require.NoError(t, s.Put(h, e))
    la, err := s.GetLastAccess(h)
    require.NoError(t, err)
    require.Equal(t, preset, la)
}

func TestBuntStore_RangeHKey(t *testing.T) {
    s := testBuntStore(t, nil)
    expected := make(map[uint64]struct{})
    for i := 0; i < 200; i++ {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
        expected[hkey] = struct{}{}
    }
    seen := make(map[uint64]struct{})
    s.RangeHKey(func(hkey uint64) bool {
        seen[hkey] = struct{}{}
        return true
    })
    require.Equal(t, len(expected), len(seen))
    for h := range expected {
        _, ok := seen[h]
        require.True(t, ok, "missing hkey: %d", h)
    }
    // Short-circuit after 10 items
    var count int
    s.RangeHKey(func(hkey uint64) bool {
        count++
        return count < 10
    })
    require.Equal(t, 10, count)
}

func TestBuntStore_Check(t *testing.T) {
    s := testBuntStore(t, nil)
    hkeys := make(map[uint64]struct{})
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
        hkeys[hkey] = struct{}{}
    }
    for hkey := range hkeys {
        require.Truef(t, s.Check(hkey), "hkey could not be found: %d", hkey)
    }

    // Now delete one and ensure Check=false
    var deleted uint64
    for hkey := range hkeys { deleted = hkey; break }
    require.NoError(t, s.Delete(deleted))
    require.False(t, s.Check(deleted))
}

func TestBuntStore_UpdateTTL(t *testing.T) {
    s := testBuntStore(t, nil)
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    for i := range 100 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(10)
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.UpdateTTL(hkey, e))
    }
    for i := range 100 {
        hkey := xxhash.Sum64([]byte(bkey(i)))
        e, err := s.Get(hkey)
        require.NoError(t, err)
        require.Equal(t, bkey(i), e.Key())
        require.Equal(t, int64(10), e.TTL())
    }
}

func TestBuntStore_GetKey(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New()
    e.SetKey(bkey(1))
    e.SetTTL(int64(1))
    e.SetValue(bval(1))
    hkey := xxhash.Sum64([]byte(e.Key()))
    require.NoError(t, s.Put(hkey, e))
    key, err := s.GetKey(hkey)
    require.NoError(t, err)
    require.Equal(t, bkey(1), key)
}

func TestBuntStore_PutRawGetRaw(t *testing.T) {
    s := testBuntStore(t, nil)
    // Put raw-encoded entry, expect to get back exactly the same bytes
    ent := entry.New()
    ent.SetKey("rawkey")
    ent.SetTTL(123)
    ent.SetTimestamp(time.Now().UnixNano())
    ent.SetValue([]byte("value"))
    raw := ent.Encode()
    hkey := xxhash.Sum64([]byte(ent.Key()))
    require.NoError(t, s.PutRaw(hkey, raw))
    rawval, err := s.GetRaw(hkey)
    require.NoError(t, err)
    require.True(t, bytes.Equal(raw, rawval))
}

func TestBuntStore_Fork(t *testing.T) {
    s := testBuntStore(t, nil)
    for i := 0; i < 10; i++ {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    child, err := s.Fork(nil)
    require.NoError(t, err)
    // child is empty by definition
    for i := range 100 {
        hkey := xxhash.Sum64([]byte(bkey(i)))
        _, err = child.Get(hkey)
        if !errors.Is(err, storage.ErrKeyNotFound) {
            t.Fatalf("Expected storage.ErrKeyNotFound. Got %v", err)
        }
    }
    stats := child.Stats()
    require.Equal(t, 0, stats.Length)
    require.Equal(t, 1, stats.NumTables)
}

func TestBuntStore_Name_NewEntry_CloseDestroy(t *testing.T) {
    s := testBuntStore(t, nil)
    require.Equal(t, "buntdb", s.Name())
    i := s.NewEntry()
    _, ok := i.(*entry.Entry)
    require.True(t, ok)
    require.NoError(t, s.Close())
    require.NoError(t, s.Destroy())
}

func TestBuntStore_Scan(t *testing.T) {
    s := testBuntStore(t, nil)
    for i := range 20000 {
        e := entry.New()
        e.SetKey(bkey(i))
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    var (
        count  int
        cursor uint64
        err    error
    )
    for {
        cursor, err = s.Scan(cursor, 10, func(e storage.Entry) bool {
            count++
            return true
        })
        require.NoError(t, err)
        if cursor == 0 { break }
    }
    require.Equal(t, 20000, count)
}

func TestBuntStore_ScanRegexMatch(t *testing.T) {
    s := testBuntStore(t, nil)
    var key string
    for i := range 100000 {
        if i%2 == 0 { key = "even:" + strconv.Itoa(i) } else { key = "odd:" + strconv.Itoa(i) }
        e := entry.New()
        e.SetKey(key)
        e.SetTTL(int64(i))
        e.SetValue(bval(i))
        e.SetTimestamp(time.Now().UnixNano())
        hkey := xxhash.Sum64([]byte(e.Key()))
        require.NoError(t, s.Put(hkey, e))
    }
    var (
        count  int
        cursor uint64
        err    error
    )
    for {
        cursor, err = s.ScanRegexMatch(cursor, "^even:", 10, func(e storage.Entry) bool {
            count++
            return true
        })
        require.NoError(t, err)
        if cursor == 0 { break }
    }
    require.Equal(t, 50000, count)
}

func TestBuntStore_Scan_Empty(t *testing.T) {
    s := testBuntStore(t, nil)
    cursor, err := s.Scan(0, 10, func(e storage.Entry) bool { return true })
    require.NoError(t, err)
    require.Equal(t, uint64(0), cursor)
}

func TestBuntStore_GetKey_NotFound(t *testing.T) {
    s := testBuntStore(t, nil)
    _, err := s.GetKey(123)
    require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestBuntStore_Put_NotStarted(t *testing.T) {
    bs, err := New(nil)
    require.NoError(t, err)
    e := entry.New(); e.SetKey("k"); e.SetValue([]byte("v"))
    err = bs.Put(1, e)
    require.Error(t, err)
}

func TestBuntStore_Delete_NotStarted(t *testing.T) {
    bs, err := New(nil)
    require.NoError(t, err)
    err = bs.Delete(1)
    require.Error(t, err)
}

func TestBuntStore_TransferIterator_NextAfterExhaust(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New(); e.SetKey("k"); e.SetValue([]byte("v")); e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key())); require.NoError(t, s.Put(h, e))
    ti := s.TransferIterator()
    require.True(t, ti.Next())
    data, idx, err := ti.Export(); require.NoError(t, err); require.NotEmpty(t, data)
    require.NoError(t, ti.Drop(idx))
    require.False(t, ti.Next())
}

func TestBuntStore_TransferIterator_ExportBeforeNext_AndDropOutOfRange(t *testing.T) {
    s := testBuntStore(t, nil)
    ti := s.TransferIterator()
    // export before Next -> EOF
    _, _, err := ti.Export()
    require.ErrorIs(t, err, io.EOF)
    // drop out of range with keys not prepared -> no error
    require.NoError(t, ti.Drop(1<<30))
}

func TestBuntStore_ScanRegexMatch_InvalidPattern(t *testing.T) {
    s := testBuntStore(t, nil)
    _, err := s.ScanRegexMatch(0, "[", 10, func(e storage.Entry) bool { return true })
    require.Error(t, err)
}

func TestBuntStore_Stats_Check_Range_NotStarted(t *testing.T) {
    bs, err := New(nil)
    require.NoError(t, err)
    // Not started
    st := bs.Stats()
    require.Equal(t, 0, st.Length)
    require.Equal(t, 0, st.NumTables)
    require.False(t, bs.Check(1))
    // Range/RangeHKey should do nothing
    bs.Range(func(h uint64, e storage.Entry) bool { t.Fatalf("should not be called"); return true })
    bs.RangeHKey(func(h uint64) bool { t.Fatalf("should not be called"); return true })
}

func TestBuntStore_Import_CallbackError(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New(); e.SetKey("k"); e.SetValue([]byte("v")); e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key())); require.NoError(t, s.Put(h, e))
    ti := s.TransferIterator(); require.True(t, ti.Next())
    data, _, err := ti.Export(); require.NoError(t, err)
    cbErr := errors.New("cb")
    err = s.Import(data, func(u uint64, e storage.Entry) error { return cbErr })
    require.ErrorIs(t, err, cbErr)
}

func TestBuntStore_Delete_MissingMetaOrIndex(t *testing.T) {
    // Use concrete engine to manipulate internal DB
    bs, err := New(nil)
    require.NoError(t, err)
    require.NoError(t, bs.Start())
    e := entry.New(); e.SetKey("k"); e.SetValue([]byte("v")); e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key())); require.NoError(t, bs.Put(h, e))
    // Manually remove akey and ikey
    bsi := bs // same type
    _ = bsi.db.Update(func(tx *buntdb.Tx) error {
        _, _ = tx.Delete(akey(h))
        _, _ = tx.Delete(ikey("k"))
        return nil
    })
    // Now Delete should still succeed
    require.NoError(t, bs.Delete(h))
}

func TestBuntStore_TransferIterator_ConfigTypes(t *testing.T) {
    types := []any{int64(1), uint(1), uint64(1), float64(1)}
    for _, v := range types {
        cfg := DefaultConfig()
        cfg.Add("transferChunkSize", v)
        s0, err := New(cfg); require.NoError(t, err)
        s, err := s0.Fork(nil); require.NoError(t, err)
        // 3 entries -> 3 chunks when chunk size = 1
        for i := 0; i < 3; i++ {
            e := entry.New(); e.SetKey(fmt.Sprintf("tct:%d", i)); e.SetValue([]byte("v")); e.SetTimestamp(time.Now().UnixNano())
            h := xxhash.Sum64([]byte(e.Key()))
            require.NoError(t, s.Put(h, e))
        }
        ti := s.TransferIterator()
        var exported int
        for ti.Next() {
            data, idx, err := ti.Export(); require.NoError(t, err); require.NotEmpty(t, data); require.NoError(t, ti.Drop(idx)); exported++
        }
        require.Equal(t, 3, exported)
        require.Equal(t, 0, s.Stats().Length)
    }
}

func TestBuntStore_TransferIterator_ExportEOF(t *testing.T) {
    s := testBuntStore(t, nil)
    e := entry.New(); e.SetKey("k"); e.SetValue([]byte("v")); e.SetTimestamp(time.Now().UnixNano())
    h := xxhash.Sum64([]byte(e.Key())); require.NoError(t, s.Put(h, e))
    ti := s.TransferIterator()
    require.True(t, ti.Next())
    _, idx, err := ti.Export(); require.NoError(t, err)
    require.NoError(t, ti.Drop(idx))
    // now exhausted
    _, _, err = ti.Export(); require.ErrorIs(t, err, io.EOF)
}
