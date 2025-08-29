package buntstore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/tochemey/olric/internal/kvstore/entry"
	"github.com/tochemey/olric/pkg/storage"
)

func testBuntStoreB(b *testing.B, c *storage.Config) storage.Engine {
	b.Helper()
	bs, err := New(c)
	if err != nil {
		b.Fatalf("new buntstore: %v", err)
	}
	if err := bs.Start(); err != nil {
		b.Fatalf("start buntstore: %v", err)
	}
	return bs
}

func makeKeys(prefix string, n int) ([]string, []uint64) {
	keys := make([]string, n)
	hkeys := make([]uint64, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%s:%d", prefix, i)
		keys[i] = k
		hkeys[i] = xxhash.Sum64([]byte(k))
	}
	return keys, hkeys
}

func BenchmarkBuntStore_Put(b *testing.B) {
	cfg := DefaultConfig() // enableKeyIndex=true by default
	s := testBuntStoreB(b, cfg)
	defer s.Close()

	keys, hkeys := makeKeys("bench:put", 4096)
	val := []byte("value-value-value-value")
	e := entry.New()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := i & (len(keys) - 1)
		e.SetKey(keys[idx])
		e.SetValue(val)
		e.SetTTL(0)
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[idx], e); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkBuntStore_Put_IndexOff(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Add("enableKeyIndex", false)
	s := testBuntStoreB(b, cfg)
	defer s.Close()

	keys, hkeys := makeKeys("bench:put", 4096)
	val := []byte("value-value-value-value")
	e := entry.New()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := i & (len(keys) - 1)
		e.SetKey(keys[idx])
		e.SetValue(val)
		e.SetTTL(0)
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[idx], e); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkBuntStore_Get(b *testing.B) {
	const preN = 100_000
	s := testBuntStoreB(b, nil)
	defer s.Close()

	keys, hkeys := makeKeys("bench:get", preN)
	val := []byte("value-value-value-value")
	for i := range preN {
		e := entry.New()
		e.SetKey(keys[i])
		e.SetValue(val)
		e.SetTTL(0)
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[i], e); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < b.N; i++ {
		h := hkeys[rnd.Intn(preN)]
		if _, err := s.Get(h); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkBuntStore_UpdateTTL(b *testing.B) {
	const preN = 50_000
	s := testBuntStoreB(b, nil)
	defer s.Close()

	keys, hkeys := makeKeys("bench:updttl", preN)
	val := []byte("v")
	for i := range preN {
		e := entry.New()
		e.SetKey(keys[i])
		e.SetValue(val)
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[i], e); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}
	e := entry.New()
	e.SetTTL(10)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := i % preN
		e.SetKey(keys[idx])
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.UpdateTTL(hkeys[idx], e); err != nil {
			b.Fatalf("update ttl: %v", err)
		}
	}
}

func BenchmarkBuntStore_Delete(b *testing.B) {
	const preN = 100_000
	s := testBuntStoreB(b, nil)
	defer s.Close()

	keys, hkeys := makeKeys("bench:del", preN)
	val := []byte("v")
	for i := range preN {
		e := entry.New()
		e.SetKey(keys[i])
		e.SetValue(val)
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[i], e); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < b.N; i++ {
		idx := rnd.Intn(preN)
		if err := s.Delete(hkeys[idx]); err != nil {
			b.Fatalf("delete: %v", err)
		}
		// reinsert to maintain steady state
		e := entry.New()
		e.SetKey(keys[idx])
		e.SetValue(val)
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[idx], e); err != nil {
			b.Fatalf("reput: %v", err)
		}
	}
}

func BenchmarkBuntStore_Scan(b *testing.B) {
	const preN = 20_000
	s := testBuntStoreB(b, nil)
	defer s.Close()

	keys, hkeys := makeKeys("bench:scan", preN)
	for i := range preN {
		e := entry.New()
		e.SetKey(keys[i])
		e.SetValue([]byte("v"))
		e.SetTimestamp(time.Now().UnixNano())
		if err := s.Put(hkeys[i], e); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var cursor uint64
	for i := 0; i < b.N; i++ {
		var err error
		cursor, err = s.Scan(cursor, 100, func(e storage.Entry) bool { return true })
		if err != nil {
			b.Fatalf("scan: %v", err)
		}
		if cursor == 0 {
			cursor = 0
		}
	}
}

func BenchmarkBuntStore_ScanRegexMatch_IndexOn(b *testing.B) {
	const preN = 100_000
	cfg := DefaultConfig()
	cfg.Add("enableKeyIndex", true)
	s := testBuntStoreB(b, cfg)
	defer s.Close()

	for i := range preN {
		k := fmt.Sprintf("%s:%d", map[bool]string{true: "even", false: "odd"}[i%2 == 0], i)
		e := entry.New()
		e.SetKey(k)
		e.SetValue([]byte("v"))
		e.SetTimestamp(time.Now().UnixNano())
		h := xxhash.Sum64([]byte(e.Key()))
		if err := s.Put(h, e); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var cursor uint64
	for i := 0; i < b.N; i++ {
		var err error
		cursor, err = s.ScanRegexMatch(cursor, "^even:", 100, func(e storage.Entry) bool { return true })
		if err != nil {
			b.Fatalf("scan regex: %v", err)
		}
		if cursor == 0 {
			cursor = 0
		}
	}
}

func BenchmarkBuntStore_ScanRegexMatch_IndexOff(b *testing.B) {
	const preN = 100_000
	cfg := DefaultConfig()
	cfg.Add("enableKeyIndex", false)
	s := testBuntStoreB(b, cfg)
	defer s.Close()

	for i := range preN {
		k := fmt.Sprintf("%s:%d", map[bool]string{true: "even", false: "odd"}[i%2 == 0], i)
		e := entry.New()
		e.SetKey(k)
		e.SetValue([]byte("v"))
		e.SetTimestamp(time.Now().UnixNano())
		h := xxhash.Sum64([]byte(e.Key()))
		if err := s.Put(h, e); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var cursor uint64
	for i := 0; i < b.N; i++ {
		var err error
		cursor, err = s.ScanRegexMatch(cursor, "^even:", 100, func(e storage.Entry) bool { return true })
		if err != nil {
			b.Fatalf("scan regex: %v", err)
		}
		if cursor == 0 {
			cursor = 0
		}
	}
}
