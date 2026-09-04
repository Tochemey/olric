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

package dmap

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/pkg/storage"
)

// isKeyIdleOnFragment is not a thread-safe function. It accesses underlying fragment for the given hkey.
func (dm *DMap) isKeyIdleOnFragment(hkey uint64, f *fragment) bool {
	if dm.config == nil {
		return false
	}

	if dm.config.maxIdleDuration.Nanoseconds() == 0 {
		return false
	}
	// Maximum time in seconds for each entry to stay idle in the map.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period
	// exceeds this limit are expired and evicted automatically.
	lastAccess, err := f.storage.GetLastAccess(hkey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false
	}
	//TODO: Handle other errors.
	ttl := (dm.config.maxIdleDuration.Nanoseconds() + lastAccess) / 1000000
	return isKeyExpired(ttl)
}

func (dm *DMap) isKeyIdle(hkey uint64) bool {
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// it's no possible to know whether the key is idle or not.
		return false
	}
	if err != nil {
		// This could be a programming error and should never be happened on production systems.
		panic(fmt.Sprintf("failed to get primary partition for: %d: %v", hkey, err))
	}
	f.Lock()
	defer f.Unlock()
	return dm.isKeyIdleOnFragment(hkey, f)
}

func (s *Service) evictKeysAtBackground() {
	defer s.wg.Done()

	num := int64(runtime.NumCPU())
	if s.config.DMaps != nil && s.config.DMaps.NumEvictionWorkers != 0 {
		num = s.config.DMaps.NumEvictionWorkers
	}
	sem := semaphore.NewWeighted(num)
	for {
		if !s.isAlive() {
			return
		}

		if err := sem.Acquire(s.ctx, 1); err != nil {
			if err != context.Canceled {
				s.log.V(3).Printf("[WARN] Failed to acquire semaphore: %v", err)
			}
			return
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer sem.Release(1)
			// Good for developing tests.
			s.evictKeys()
			select {
			case <-time.After(100 * time.Millisecond):
			case <-s.ctx.Done():
				return
			}
		}()
	}
}

func (s *Service) evictKeys() {
	partID := uint64(rand.Intn(int(s.config.PartitionCount)))
	part := s.primary.PartitionByID(partID)
	part.Map().Range(func(name, tmp interface{}) bool {
		f := tmp.(*fragment)
		s.scanFragmentForEviction(partID, name.(string), f)
		// this breaks the loop, we only scan one dmap instance per call
		return false
	})
}

// expiredKey is a key the eviction scanner sampled as expired or idle.
type expiredKey struct {
	hkey uint64
	key  string
}

// scanFragmentForEviction samples entries of the fragment and deletes the
// expired and idle ones from the cluster, the way Redis does: test up to
// twenty random keys, delete the expired ones, and start again while more
// than a quarter of the sample was expired, within a budget per call. The
// sample is taken under the fragment lock; each delete then runs under the
// key's own lock, see deleteExpiredKey, so the fragment is never locked
// across a network call and no delete slips between the replication and the
// store of a write in flight.
func (x *Service) scanFragmentForEviction(partID uint64, name string, f *fragment) {
	var maxKeyCount = 20
	var maxTotalCount = 100
	var totalCount = 0

	dm, err := x.getOrCreateDMap(strings.TrimPrefix(name, "dmap."))
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to load DMap: %s: %v", name, err)
		return
	}

	janitor := func() bool {
		if totalCount > maxTotalCount {
			// The budget is spent. Eviction will be triggered again.
			return false
		}

		candidates := make([]expiredKey, 0, maxKeyCount)
		f.Lock()
		keyCount := 0
		f.storage.RangeHKey(func(hkey uint64) bool {
			keyCount++
			if keyCount >= maxKeyCount {
				// this means 'break'.
				return false
			}

			ttl, err := f.storage.GetTTL(hkey)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to get TTL for: %d", hkey)
				return true // continue
			}

			key, err := f.storage.GetKey(hkey)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to get key for: %d", hkey)
				return true // continue
			}

			if isKeyExpired(ttl) || dm.isKeyIdleOnFragment(hkey, f) {
				candidates = append(candidates, expiredKey{hkey: hkey, key: key})
			}

			return true
		})
		f.Unlock()

		count := 0
		for _, candidate := range candidates {
			if err := dm.deleteExpiredKey(f, candidate.hkey, candidate.key); err != nil {
				// It will be tried again.
				dm.s.log.V(3).Printf("[ERROR] Failed to delete expired key: %s on DMap: %s: %v",
					candidate.key, dm.name, err)
				continue
			}

			count++
			// number of valid items removed from cache to free memory for new items.
			EvictedTotal.Increase(1)
		}

		totalCount += count
		return count >= maxKeyCount/4
	}

	defer func() {
		if totalCount > 0 {
			if x.log.V(6).Ok() {
				x.log.V(6).Printf("[DEBUG] Evicted key count is %d on PartID: %d", totalCount, partID)
			}
		}
	}()
	for {
		select {
		case <-f.ctx.Done():
			// the fragment is closed.
			return
		case <-x.ctx.Done():
			// The server has gone.
			return
		default:
		}
		// Call janitorWorker again until it returns false.
		if !janitor() {
			return
		}
	}
}

// deleteExpiredKey deletes key, sampled as expired or idle, from the cluster
// under its key lock. The expiry is checked again under the fragment lock
// first: a write of the key since the sample makes it live again, and a write
// in flight holds the key lock, so the delete cannot slip between that
// write's replication and its store and remove it from the replicas only.
func (x *DMap) deleteExpiredKey(f *fragment, hkey uint64, key string) error {
	keyLock := x.s.keyLock(hkey)
	keyLock.Lock()
	defer keyLock.Unlock()

	f.Lock()
	ttl, err := f.storage.GetTTL(hkey)
	expired := err == nil && (isKeyExpired(ttl) || x.isKeyIdleOnFragment(hkey, f))
	f.Unlock()

	if errors.Is(err, storage.ErrKeyNotFound) || (err == nil && !expired) {
		return nil
	}

	if err != nil {
		return err
	}

	if err := x.deleteRemoteCopies(x.s.ctx, hkey, key); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	if err := f.storage.Delete(hkey); err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return nil
		}

		return err
	}

	// DeleteHits is the number of deletion reqs resulting in an item being removed.
	DeleteHits.Increase(1)
	return nil
}

type lruItem struct {
	HKey       uint64
	LastAccess int64
}

// lruVictim samples entries of the fragment of e and returns the hashed key
// and the key of the least recently used one. The caller holds the fragment
// lock.
func (x *DMap) lruVictim(e *env) (uint64, string, error) {
	var idx = 1
	var items []lruItem

	// Pick random items from the distributed map and sort them by accessedAt.
	e.fragment.storage.Range(func(hkey uint64, e storage.Entry) bool {
		if idx >= x.config.lruSamples {
			return false
		}
		idx++
		i := lruItem{
			HKey:       hkey,
			LastAccess: e.LastAccess(),
		}
		items = append(items, i)
		return true
	})

	if len(items) == 0 {
		return 0, "", fmt.Errorf("nothing found to expire with LRU")
	}

	sort.Slice(items, func(i, j int) bool { return items[i].LastAccess < items[j].LastAccess })
	// Pick the first item to delete. It's the least recently used item in the sample.
	item := items[0]
	key, err := e.fragment.storage.GetKey(item.HKey)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
			GetMisses.Increase(1)
		}

		return 0, "", err
	}

	return item.HKey, key, nil
}

// evictKeyWithLRU removes the least recently used entry of the fragment of e
// from the cluster, to make room for a write. The caller holds the fragment
// lock for the whole write, see setLRUEvictionStats.
func (x *DMap) evictKeyWithLRU(e *env) error {
	hkey, key, err := x.lruVictim(e)
	if err != nil {
		return err
	}

	// Here we have a key/value pair to evict for making room for a new pair.
	if x.s.log.V(6).Ok() {
		x.s.log.V(6).Printf("[DEBUG] Evicted item on DMap: %s, key: %s with LRU", e.dmap, key)
	}

	err = x.deleteOnCluster(e.ctx, hkey, key, e.fragment)
	if err != nil {
		return err
	}

	// number of valid items removed from cache to free memory for new items.
	EvictedTotal.Increase(1)
	return nil
}
