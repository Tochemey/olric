/*
 * Copyright 2025 Arsene Tochemey Gandote
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

package buntstore

import (
    "encoding/binary"
    "errors"
    "io"
    "log"
    "regexp"
    "strconv"
    "time"

    "github.com/tidwall/buntdb"
    msgpack "github.com/vmihailenco/msgpack/v5"

    "github.com/tochemey/olric/internal/kvstore/entry"
    "github.com/tochemey/olric/pkg/storage"
)

const defaultTransferChunkSize = 50000

// BuntStore is a storage.Engine implementation backed by BuntDB in-memory DB.
type BuntStore struct {
    db     *buntdb.DB
    config *storage.Config
    logger *log.Logger
    enableKeyIndex bool
}

// DefaultConfig returns a minimal config for BuntStore (currently empty).
func DefaultConfig() *storage.Config {
    c := storage.NewConfig(nil)
    // default export chunk size
    c.Add("transferChunkSize", defaultTransferChunkSize)
    c.Add("enableKeyIndex", true)
    return c
}

func New(c *storage.Config) (*BuntStore, error) {
    if c == nil {
        c = DefaultConfig()
    }
    return &BuntStore{config: c}, nil
}

func (b *BuntStore) SetConfig(c *storage.Config) {
    b.config = c
    if c != nil {
        if v, err := c.Get("enableKeyIndex"); err == nil {
            if bv, ok := v.(bool); ok {
                b.enableKeyIndex = bv
            }
        }
    }
}
func (b *BuntStore) SetLogger(l *log.Logger)     { b.logger = l }

// openDB is a test seam
var openDB = buntdb.Open

func (b *BuntStore) Start() error {
    if b.db != nil {
        return nil
    }
    db, err := openDB(":memory:")
    if err != nil {
        return err
    }
    b.db = db
    // derive flags
    b.enableKeyIndex = true
    if b.config != nil {
        if v, err := b.config.Get("enableKeyIndex"); err == nil {
            if bv, ok := v.(bool); ok {
                b.enableKeyIndex = bv
            }
        }
    }
    return nil
}

func (b *BuntStore) Name() string { return "buntdb" }

func (b *BuntStore) NewEntry() storage.Entry { return entry.New() }

func prefixedKey(prefix byte, u uint64) string {
    var b [22]byte
    b[0] = prefix
    b[1] = ':'
    for i := 21; i >= 2; i-- {
        b[i] = byte('0' + (u % 10))
        u /= 10
    }
    return string(b[:])
}

func vkey(hkey uint64) string { return prefixedKey('v', hkey) }
func akey(hkey uint64) string { return prefixedKey('a', hkey) }
func ikey(key string) string  { return "k:" + key }
func hkey20(u uint64) string {
    var b [20]byte
    for i := 19; i >= 0; i-- {
        b[i] = byte('0' + (u % 10))
        u /= 10
    }
    return string(b[:])
}
func putU64(b []byte, v uint64) { binary.BigEndian.PutUint64(b, v) }
func getU64(b []byte) uint64    { return binary.BigEndian.Uint64(b) }

// PutRaw stores an encoded entry blob as-is.
func (b *BuntStore) PutRaw(hkey uint64, value []byte) error {
    if b.db == nil {
        return errors.New("engine not started")
    }
    return b.db.Update(func(tx *buntdb.Tx) error {
        _, _, err := tx.Set(vkey(hkey), string(value), nil)
        return err
    })
}

// Put encodes the entry and stores it by hashed key.
func (b *BuntStore) Put(hkey uint64, e storage.Entry) error {
    if b.db == nil {
        return errors.New("engine not started")
    }
    // Keep behavior aligned with kvstore: limit key length
    if len(e.Key()) >= 256 { // table.MaxKeyLength
        return storage.ErrKeyTooLarge
    }
    // Ensure last access set
    if e.LastAccess() == 0 {
        e.SetLastAccess(time.Now().UnixNano())
    }
    buf := e.Encode()
    return b.db.Update(func(tx *buntdb.Tx) error {
        if _, _, err := tx.Set(vkey(hkey), string(buf), nil); err != nil { return err }
        if b.enableKeyIndex {
            if _, _, err := tx.Set(ikey(e.Key()), hkey20(hkey), nil); err != nil { return err }
        }
        var la [8]byte
        putU64(la[:], uint64(e.LastAccess()))
        if _, _, err := tx.Set(akey(hkey), string(la[:]), nil); err != nil { return err }
        return nil
    })
}

func (b *BuntStore) GetRaw(hkey uint64) ([]byte, error) {
    if b.db == nil {
        return nil, errors.New("engine not started")
    }
    var out []byte
    err := b.db.View(func(tx *buntdb.Tx) error {
        val, err := tx.Get(vkey(hkey))
        if err == buntdb.ErrNotFound {
            return storage.ErrKeyNotFound
        }
        if err != nil {
            return err
        }
        out = []byte(val)
        return nil
    })
    return out, err
}

func (b *BuntStore) Get(hkey uint64) (storage.Entry, error) {
    raw, err := b.GetRaw(hkey)
    if err != nil {
        return nil, err
    }
    e := entry.New()
    e.Decode(raw)

    // Read last access meta
    var lastAccess int64
    _ = b.db.View(func(tx *buntdb.Tx) error {
        if v, err := tx.Get(akey(hkey)); err == nil && len(v) == 8 {
            lastAccess = int64(getU64([]byte(v)))
        }
        return nil
    })
    if lastAccess == 0 { lastAccess = time.Now().UnixNano() }
    e.SetLastAccess(lastAccess)

    // Update last access meta best-effort
    _ = b.db.Update(func(tx *buntdb.Tx) error {
        var la [8]byte
        putU64(la[:], uint64(time.Now().UnixNano()))
        _, _, _ = tx.Set(akey(hkey), string(la[:]), nil)
        return nil
    })
    return e, nil
}

func (b *BuntStore) GetTTL(hkey uint64) (int64, error) {
    raw, err := b.GetRaw(hkey)
    if err != nil {
        return 0, err
    }
    e := entry.New()
    e.Decode(raw)
    return e.TTL(), nil
}

func (b *BuntStore) GetLastAccess(hkey uint64) (int64, error) {
    var last int64
    err := b.db.View(func(tx *buntdb.Tx) error {
        v, err := tx.Get(akey(hkey))
        if err == nil && len(v) == 8 {
            last = int64(getU64([]byte(v)))
        }
        return nil
    })
    if err != nil { return 0, err }
    if last != 0 { return last, nil }
    raw, err := b.GetRaw(hkey)
    if err != nil { return 0, err }
    e := entry.New(); e.Decode(raw)
    return e.LastAccess(), nil
}

func (b *BuntStore) GetKey(hkey uint64) (string, error) {
    raw, err := b.GetRaw(hkey)
    if err != nil {
        return "", err
    }
    e := entry.New()
    e.Decode(raw)
    return e.Key(), nil
}

func (b *BuntStore) Delete(hkey uint64) error {
    if b.db == nil {
        return errors.New("engine not started")
    }
    return b.db.Update(func(tx *buntdb.Tx) error {
        val, err := tx.Get(vkey(hkey))
        if err == buntdb.ErrNotFound { return nil }
        if err != nil { return err }
        e := entry.New(); e.Decode([]byte(val))
        if _, err := tx.Delete(vkey(hkey)); err != nil && err != buntdb.ErrNotFound { return err }
        if _, err := tx.Delete(akey(hkey)); err != nil && err != buntdb.ErrNotFound { return err }
        if b.enableKeyIndex {
            if _, err := tx.Delete(ikey(e.Key())); err != nil && err != buntdb.ErrNotFound { return err }
        }
        return nil
    })
}

func (b *BuntStore) UpdateTTL(hkey uint64, data storage.Entry) error {
    raw, err := b.GetRaw(hkey)
    if err != nil {
        return err
    }
    e := entry.New()
    e.Decode(raw)
    e.SetTTL(data.TTL())
    e.SetTimestamp(data.Timestamp())
    if la, err := b.GetLastAccess(hkey); err == nil { e.SetLastAccess(la) } else { e.SetLastAccess(time.Now().UnixNano()) }
    buf := e.Encode()
    return b.db.Update(func(tx *buntdb.Tx) error {
        if _, _, err := tx.Set(vkey(hkey), string(buf), nil); err != nil { return err }
        var la [8]byte; putU64(la[:], uint64(time.Now().UnixNano()))
        if _, _, err := tx.Set(akey(hkey), string(la[:]), nil); err != nil { return err }
        return nil
    })
}

type record struct {
    H uint64
    V []byte
}

// Transfer iterator exports the DB in chunks.
type transferIterator struct {
    s        *BuntStore
    exported bool
    keys     []string
    pos      int
    chunk    int
}

func (t *transferIterator) Next() bool {
    if t.exported {
        return false
    }
    // Prepare key list on first call
    if t.keys == nil {
        var keys []string
        _ = t.s.db.View(func(tx *buntdb.Tx) error {
            _ = tx.AscendKeys("v:*", func(k, _ string) bool {
                keys = append(keys, k)
                return true
            })
            return nil
        })
        t.keys = keys
        if t.chunk <= 0 {
            t.chunk = defaultTransferChunkSize
        }
    }
    return t.pos < len(t.keys)
}

func (t *transferIterator) Export() ([]byte, int, error) {
    if t.exported {
        return nil, 0, io.EOF
    }
    if t.keys == nil || t.pos >= len(t.keys) {
        return nil, 0, io.EOF
    }
    start := t.pos
    end := start + t.chunk
    if end > len(t.keys) {
        end = len(t.keys)
    }
    chunkKeys := t.keys[start:end]

    var recs []record
    err := t.s.db.View(func(tx *buntdb.Tx) error {
        for _, k := range chunkKeys {
            v, err := tx.Get(k)
            if err != nil {
                continue
            }
            u, _ := strconv.ParseUint(k[2:], 10, 64)
            recs = append(recs, record{H: u, V: []byte(v)})
        }
        return nil
    })
    if err != nil {
        return nil, 0, err
    }
    data, err := msgpack.Marshal(recs)
    if err != nil {
        return nil, 0, err
    }
    // Advance position and compute logical index
    chunkIndex := start / t.chunk
    t.pos = end
    if t.pos >= len(t.keys) {
        t.exported = true
    }
    return data, chunkIndex, nil
}

func (t *transferIterator) Drop(index int) error {
    // Delete keys belonging to this chunk index
    if t.keys == nil {
        return nil
    }
    start := index * t.chunk
    if start >= len(t.keys) {
        return nil
    }
    end := start + t.chunk
    if end > len(t.keys) {
        end = len(t.keys)
    }
    keys := append([]string(nil), t.keys[start:end]...)
    return t.s.db.Update(func(tx *buntdb.Tx) error {
        for _, vk := range keys {
            // parse hkey and real key
            h, _ := strconv.ParseUint(vk[2:], 10, 64)
            val, err := tx.Get(vk)
            if err == nil {
                ent := entry.New(); ent.Decode([]byte(val))
                if _, err := tx.Delete(ikey(ent.Key())); err != nil && err != buntdb.ErrNotFound { return err }
            }
            if _, err := tx.Delete(vk); err != nil && err != buntdb.ErrNotFound { return err }
            if _, err := tx.Delete(akey(h)); err != nil && err != buntdb.ErrNotFound { return err }
        }
        return nil
    })
}

func (b *BuntStore) TransferIterator() storage.TransferIterator {
    chunk := defaultTransferChunkSize
    if b.config != nil {
        if v, err := b.config.Get("transferChunkSize"); err == nil {
            switch x := v.(type) {
            case int:
                if x > 0 { chunk = x }
            case int64:
                if x > 0 { chunk = int(x) }
            case uint:
                if x > 0 { chunk = int(x) }
            case uint64:
                if x > 0 { chunk = int(x) }
            case float64:
                if x > 0 { chunk = int(x) }
            }
        }
    }
    return &transferIterator{s: b, chunk: chunk}
}

func (b *BuntStore) Import(data []byte, f func(uint64, storage.Entry) error) error {
    var recs []record
    if err := msgpack.Unmarshal(data, &recs); err != nil {
        return err
    }
    for _, r := range recs {
        e := entry.New()
        e.Decode(r.V)
        if err := f(r.H, e); err != nil {
            return err
        }
    }
    return nil
}

func (b *BuntStore) Stats() storage.Stats {
    if b.db == nil {
        return storage.Stats{}
    }
    n := 0
    _ = b.db.View(func(tx *buntdb.Tx) error {
        _ = tx.AscendKeys("v:*", func(k, v string) bool { n++; return true })
        return nil
    })
    return storage.Stats{Length: n, NumTables: 1}
}

func (b *BuntStore) Check(hkey uint64) bool {
    if b.db == nil {
        return false
    }
    err := b.db.View(func(tx *buntdb.Tx) error {
        _, err := tx.Get(vkey(hkey))
        return err
    })
    return err == nil
}

func (b *BuntStore) Range(f func(uint64, storage.Entry) bool) {
    if b.db == nil {
        return
    }
    _ = b.db.View(func(tx *buntdb.Tx) error {
        _ = tx.AscendKeys("v:*", func(k, v string) bool {
            u, _ := strconv.ParseUint(k[2:], 10, 64)
            e := entry.New()
            e.Decode([]byte(v))
            return f(u, e)
        })
        return nil
    })
}

func (b *BuntStore) RangeHKey(f func(uint64) bool) {
    if b.db == nil {
        return
    }
    _ = b.db.View(func(tx *buntdb.Tx) error {
        _ = tx.AscendKeys("v:*", func(k, _ string) bool {
            u, _ := strconv.ParseUint(k[2:], 10, 64)
            return f(u)
        })
        return nil
    })
}

func (b *BuntStore) Scan(cursor uint64, count int, f func(storage.Entry) bool) (uint64, error) {
    if b.db == nil {
        return 0, nil
    }
    var (
        idx   uint64
        taken int
        next  uint64
        stop  bool
    )
    err := b.db.View(func(tx *buntdb.Tx) error {
        return tx.AscendKeys("v:*", func(_, v string) bool {
            if stop {
                return false
            }
            if idx < cursor {
                idx++
                return true
            }
            if taken >= count {
                stop = true
                return false
            }
            e := entry.New()
            e.Decode([]byte(v))
            if !f(e) {
                stop = true
                return false
            }
            taken++
            idx++
            next = idx
            return true
        })
    })
    if err != nil {
        return 0, err
    }
    if taken == 0 {
        return 0, nil
    }
    return next, nil
}

func (b *BuntStore) ScanRegexMatch(cursor uint64, expr string, count int, f func(storage.Entry) bool) (uint64, error) {
    if b.db == nil {
        return 0, nil
    }
    r, err := regexp.Compile(expr)
    if err != nil {
        return 0, err
    }
    var (
        idx   uint64
        taken int
        next  uint64
        stop  bool
    )
    if b.enableKeyIndex {
        // Use key index to filter by regex on real keys
        err = b.db.View(func(tx *buntdb.Tx) error {
            return tx.AscendKeys("k:*", func(k, v string) bool {
                if stop { return false }
                if idx < cursor { idx++; return true }
                realKey := k[2:]
                if !r.MatchString(realKey) {
                    idx++
                    return true
                }
                if taken >= count { stop = true; return false }
                u, _ := strconv.ParseUint(v, 10, 64)
                val, err := tx.Get(vkey(u))
                if err != nil { idx++; return true }
                e := entry.New(); e.Decode([]byte(val))
                if !f(e) { stop = true; return false }
                taken++
                idx++
                next = idx
                return true
            })
        })
    } else {
        // fallback without key index
        err = b.db.View(func(tx *buntdb.Tx) error {
            return tx.AscendKeys("v:*", func(_, v string) bool {
                if stop { return false }
                if idx < cursor { idx++; return true }
                e := entry.New(); e.Decode([]byte(v))
                if !r.MatchString(e.Key()) {
                    idx++
                    return true
                }
                if taken >= count { stop = true; return false }
                if !f(e) { stop = true; return false }
                taken++
                idx++
                next = idx
                return true
            })
        })
    }
    if err != nil {
        return 0, err
    }
    if taken == 0 {
        return 0, nil
    }
    return next, nil
}

func (b *BuntStore) Compaction() (bool, error) { return true, nil }

func (b *BuntStore) Close() error {
    if b.db == nil { return nil }
    err := b.db.Close()
    b.db = nil
    return err
}

func (b *BuntStore) Destroy() error { return b.Close() }

func (b *BuntStore) Fork(c *storage.Config) (storage.Engine, error) {
    if c == nil { c = b.config.Copy() }
    child, err := New(c)
    if err != nil { return nil, err }
    if err := child.Start(); err != nil { return nil, err }
    return child, nil
}

var _ storage.Engine = (*BuntStore)(nil)
