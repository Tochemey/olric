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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/bufpool"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/resp"
	"github.com/tochemey/olric/internal/stats"
	"github.com/tochemey/olric/pkg/storage"
)

var pool = bufpool.New()

// EntriesTotal is the total number of entries(including replicas)
// stored during the life of this instance.
var EntriesTotal = stats.NewInt64Counter()

var (
	ErrKeyFound      = errors.New("key found")
	ErrWriteQuorum   = errors.New("write quorum cannot be reached")
	ErrKeyTooLarge   = errors.New("key too large")
	ErrEntryTooLarge = errors.New("entry too large for the configured table size")
)

func prepareTTL(e *env) int64 {
	var ttl int64
	switch {
	case e.putConfig.HasEX:
		ttl = (e.putConfig.EX.Nanoseconds() + time.Now().UnixNano()) / 1000000
	case e.putConfig.HasPX:
		ttl = (e.putConfig.PX.Nanoseconds() + time.Now().UnixNano()) / 1000000
	case e.putConfig.HasEXAT:
		ttl = e.putConfig.EXAT.Nanoseconds() / 1000000
	case e.putConfig.HasPXAT:
		ttl = e.putConfig.PXAT.Nanoseconds() / 1000000
	default:
		ns := e.timeout.Nanoseconds()
		if ns != 0 {
			ttl = (ns + time.Now().UnixNano()) / 1000000
		}
	}
	return ttl
}

// putOnFragment calls underlying storage engine's Put method to store the key/value pair. It's not thread-safe.
func (dm *DMap) putEntryOnFragment(e *env, nt storage.Entry) error {
	if e.putConfig.OnlyUpdateTTL {
		err := e.fragment.storage.UpdateTTL(e.hkey, nt)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				err = ErrKeyNotFound
			}
			return err
		}
		return nil
	}
	err := e.fragment.storage.Put(e.hkey, nt)
	if errors.Is(err, storage.ErrKeyTooLarge) {
		err = ErrKeyTooLarge
	}
	if errors.Is(err, storage.ErrEntryTooLarge) {
		err = ErrEntryTooLarge
	}
	if err != nil {
		return err
	}

	// total number of entries stored during the life of this instance.
	EntriesTotal.Increase(1)

	return nil
}

func (dm *DMap) prepareEntry(e *env) storage.Entry {
	nt := e.fragment.storage.NewEntry()
	nt.SetKey(e.key)
	nt.SetValue(e.value)
	nt.SetTTL(prepareTTL(e))
	nt.SetTimestamp(e.timestamp)
	return nt
}

func (dm *DMap) putOnReplicaFragment(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.BACKUP)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	err = f.storage.PutRaw(e.hkey, e.value)
	if errors.Is(err, storage.ErrKeyTooLarge) {
		err = ErrKeyTooLarge
	}
	if errors.Is(err, storage.ErrEntryTooLarge) {
		err = ErrEntryTooLarge
	}
	if err != nil {
		return err
	}

	// total number of entries stored during the life of this instance.
	EntriesTotal.Increase(1)

	return nil
}

// asyncPutOnBackup sends the encoded entry to one backup owner in the
// background, within a bounded deadline.
func (x *DMap) asyncPutOnBackup(e *env, data []byte, owner discovery.Member) {
	ctx, cancel := x.s.remoteCallContext(x.s.ctx)
	defer cancel()

	rc := x.s.client.Get(owner.String())
	cmd := protocol.NewPutEntry(e.dmap, e.key, data).Command(ctx)
	err := rc.Process(ctx, cmd)
	if err != nil {
		if x.s.log.V(3).Ok() {
			x.s.log.V(3).Printf("[ERROR] Failed to create replica in async mode: %v", err)
		}
		return
	}
	err = cmd.Err()
	if err != nil {
		if x.s.log.V(3).Ok() {
			x.s.log.V(3).Printf("[ERROR] Failed to create replica in async mode: %v", err)
		}
	}
}

// asyncPutOnCluster stores the entry locally, then replicates it to the live
// backup owners in the background.
func (x *DMap) asyncPutOnCluster(e *env, nt storage.Entry) error {
	err := x.putOnFragmentLocked(e, nt)
	if err != nil {
		return err
	}

	encodedEntry := nt.Encode()
	// Fire and forget mode.
	owners := x.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		if !x.s.isAlive() {
			return ErrServerGone
		}

		if !x.s.isMember(owner) {
			continue
		}

		x.s.spawn(func() { x.asyncPutOnBackup(e, encodedEntry, owner) })
	}

	return nil
}

// syncPutOnCluster replicates the entry to the live backup owners, waits for
// the write quorum, then stores it locally.
func (x *DMap) syncPutOnCluster(e *env, nt storage.Entry) error {
	// Quorum based replication.
	var successful int

	encodedEntry := nt.Encode()

	owners := x.s.backup.PartitionOwnersByHKey(e.hkey)
	ctx, cancel := x.s.remoteCallContext(e.ctx)
	defer cancel()

	for _, owner := range owners {
		if !x.s.isMember(owner) {
			// The owner died and the routing table has not dropped it yet;
			// dialing it can only fail. It counts as a replica that did not
			// answer, and the write quorum decides.
			x.s.log.V(3).Printf("[WARN] Skipping the replica of %s on %s: no longer a cluster member", e.dmap, owner)
			continue
		}

		rc := x.s.client.Get(owner.String())
		cmd := protocol.NewPutEntry(x.name, e.key, encodedEntry).Command(ctx)
		err := rc.Process(ctx, cmd)
		if err != nil {
			return protocol.ConvertError(err)
		}
		err = protocol.ConvertError(cmd.Err())
		if err != nil {
			if x.s.log.V(3).Ok() {
				x.s.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", owner, e.dmap, err)
			}
			continue
		}
		successful++
	}
	err := x.putOnFragmentLocked(e, nt)
	if err != nil {
		if x.s.log.V(3).Ok() {
			x.s.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", x.s.rt.This(), e.dmap, err)
		}
	} else {
		successful++
	}

	if successful >= x.s.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

// lruOverLimit reports whether the fragment of e has reached the key count or
// the memory limit of the LRU policy. MaxKeys and MaxInuse are shared by the
// partitions this member owns, each partition managing itself: with MaxKeys
// 70 and seven owned partitions every partition holds ten keys at most. The
// caller holds the fragment lock.
func (x *DMap) lruOverLimit(e *env) bool {
	st := e.fragment.storage.Stats()

	// ownedPartitionCount changes in the case of node join or leave. The
	// routing table is an eventually consistent data structure: the count is
	// checked before doing math, so a stale zero cannot panic in production.
	ownedPartitionCount := x.s.rt.OwnedPartitionCount()
	if ownedPartitionCount == 0 {
		return false
	}

	if x.config.maxKeys > 0 && st.Length > 0 && st.Length >= x.config.maxKeys/int(ownedPartitionCount) {
		return true
	}

	// WARNING: Actual allocated memory can be different.
	return x.config.maxInuse > 0 && st.Inuse > 0 && st.Inuse >= x.config.maxInuse/int(ownedPartitionCount)
}

// setLRUEvictionStats makes room for the write of e under the LRU policy with
// the fragment lock held: the victim is deleted from the cluster in place. It
// serves the single-replica write, which holds the fragment lock throughout;
// the replicated write makes room through evictForWrite instead.
func (x *DMap) setLRUEvictionStats(e *env) error {
	if !x.lruOverLimit(e) {
		return nil
	}

	return x.evictKeyWithLRU(e)
}

func (dm *DMap) checkPutConditions(e *env) error {
	// Only set the key if it does not already exist.
	if e.putConfig.HasNX {
		ttl, err := e.fragment.storage.GetTTL(e.hkey)
		if err == nil {
			if !isKeyExpired(ttl) {
				return ErrKeyFound
			}
		}
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	// Only set the key if it already exists.
	if e.putConfig.HasXX && !e.fragment.storage.Check(e.hkey) {
		ttl, err := e.fragment.storage.GetTTL(e.hkey)
		if err == nil {
			if isKeyExpired(ttl) {
				return ErrKeyNotFound
			}
		}
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// prepareWrite checks the write's conditions against the fragment, applies
// the DMap's TTL and builds the entry to store, all under the fragment lock,
// which it releases before returning, and marks the write as in flight on the
// fragment, see fragment.inFlight; the caller ends that once the entry is
// stored or the write failed. The caller holds the key lock, so nothing else
// can write the key between this check and the store that follows the
// replication. Room under the LRU policy is made by evictForWrite before.
func (x *DMap) prepareWrite(e *env) (storage.Entry, error) {
	f := e.fragment
	f.Lock()
	defer f.Unlock()

	nt, err := x.prepareWriteLocked(e, false)
	if err != nil {
		return nil, err
	}

	f.inFlight.Add(1)
	return nt, nil
}

// prepareWriteLocked is prepareWrite for a caller that holds the fragment
// lock. With evict set it also makes room under the LRU policy in place,
// which only a caller that holds the fragment lock for the whole write may
// ask for.
func (x *DMap) prepareWriteLocked(e *env, evict bool) (storage.Entry, error) {
	if err := x.checkPutConditions(e); err != nil {
		return nil, err
	}

	if x.config != nil {
		if x.config.ttlDuration.Seconds() != 0 && e.timeout.Seconds() == 0 {
			e.timeout = x.config.ttlDuration
		}

		if evict && x.config.evictionPolicy == config.LRUEviction {
			if err := x.setLRUEvictionStats(e); err != nil {
				return nil, err
			}
		}
	}

	if e.putConfig.OnlyUpdateTTL {
		// The replicas store the entry as pushed, so an update of the TTL
		// alone must carry the current value or the replicas end up with an
		// empty one. The value is copied: the entry may point into a table
		// that compaction rewrites once the lock is released.
		current, err := e.fragment.storage.Get(e.hkey)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				err = ErrKeyNotFound
			}

			return nil, err
		}

		e.value = bytes.Clone(current.Value())
	}

	return x.prepareEntry(e), nil
}

// evictForWrite makes room for the replicated write of e under the LRU
// policy before the write takes its locks: the victim is chosen under the
// fragment lock and deleted through deleteKey, under the victim's own key
// lock, so the delete cannot slip between the replication and the store of a
// write of the victim that is in flight, and no network call runs under the
// fragment lock. The room is made outside the write's own locks, so writes
// that run at the same time can exceed the limit by their number; the limit
// is a target, as the sampling behind it already made it.
func (x *DMap) evictForWrite(e *env) error {
	if x.config == nil || x.config.evictionPolicy != config.LRUEviction {
		return nil
	}

	f := e.fragment
	f.Lock()
	over := x.lruOverLimit(e)
	var key string
	var err error

	if over {
		_, key, err = x.lruVictim(e)
	}

	f.Unlock()

	if err != nil || !over {
		return err
	}

	if x.s.log.V(6).Ok() {
		x.s.log.V(6).Printf("[DEBUG] Evicted item on DMap: %s, key: %s with LRU", e.dmap, key)
	}

	if err := x.deleteKey(e.ctx, key); err != nil {
		return err
	}

	// number of valid items removed from cache to free memory for new items.
	EvictedTotal.Increase(1)
	return nil
}

// putOnFragmentLocked stores nt in the fragment of e under the fragment lock.
func (x *DMap) putOnFragmentLocked(e *env, nt storage.Entry) error {
	e.fragment.Lock()
	defer e.fragment.Unlock()

	return x.putEntryOnFragment(e, nt)
}

// putOnCluster writes the entry of e as the primary owner of its partition.
// The key lock is held for the whole write, replication included, so the
// writes of one key reach its replicas in the order they are applied here;
// the fragment lock is taken only around the storage accesses, so a replica
// that does not answer holds up the writes of this key and nothing else in
// the fragment. The lock order is key lock first, fragment lock second.
func (x *DMap) putOnCluster(e *env) error {
	part := x.getPartitionByHKey(e.hkey, partitions.PRIMARY)
	f, err := x.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	e.fragment = f

	if x.s.config.ReplicaCount <= config.MinimumReplicaCount {
		// Single replica: no network call, so one fragment lock around the
		// whole write is the cheapest correct choice.
		f.Lock()
		defer f.Unlock()

		e.timestamp = time.Now().UnixNano()
		nt, err := x.prepareWriteLocked(e, true)
		if err != nil {
			return err
		}

		return x.putEntryOnFragment(e, nt)
	}

	if err := x.evictForWrite(e); err != nil {
		return err
	}

	keyLock := x.s.keyLock(e.hkey)
	keyLock.Lock()
	defer keyLock.Unlock()

	// Stamped under the key lock, so the timestamps of one key follow the
	// order its writes are applied in: merges and read repair order by them.
	e.timestamp = time.Now().UnixNano()
	nt, err := x.prepareWrite(e)
	if err != nil {
		return err
	}

	defer f.inFlight.Add(-1)

	switch x.s.config.ReplicationMode {
	case config.AsyncReplicationMode:
		// Fire and forget mode. Calls PutBackup command in different goroutines
		// and stores the key/value pair on local storage instance.
		return x.asyncPutOnCluster(e, nt)
	case config.SyncReplicationMode:
		// Quorum based replication.
		return x.syncPutOnCluster(e, nt)
	default:
		return fmt.Errorf("invalid replication mode: %v", x.s.config.ReplicationMode)
	}
}

func (dm *DMap) writePutCommand(e *env) (*redis.StatusCmd, error) {
	cmd := protocol.NewPut(e.dmap, e.key, e.value)
	switch {
	case e.putConfig.HasEX:
		cmd.SetEX(e.putConfig.EX.Seconds())
	case e.putConfig.HasPX:
		cmd.SetPX(e.putConfig.PX.Milliseconds())
	case e.putConfig.HasEXAT:
		cmd.SetEXAT(e.putConfig.EXAT.Seconds())
	case e.putConfig.HasPXAT:
		cmd.SetPXAT(e.putConfig.PXAT.Milliseconds())
	}

	switch {
	case e.putConfig.HasNX:
		cmd.SetNX()
	case e.putConfig.HasXX:
		cmd.SetXX()
	}

	return cmd.Command(dm.s.ctx), nil
}

// put controls every write operation in Olric. It redirects the requests to its owner,
// if the key belongs to another host.
func (dm *DMap) put(e *env) error {
	e.hkey = partitions.HKey(e.dmap, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		// We are on the partition owner.
		return dm.putOnCluster(e)
	}

	// Redirect to the partition owner.
	cmd, err := dm.writePutCommand(e)
	if err != nil {
		return err
	}
	rc := dm.s.client.Get(member.String())
	err = rc.Process(e.ctx, cmd)
	if err != nil {
		return protocol.ConvertError(err)
	}
	return protocol.ConvertError(cmd.Err())
}

type PutConfig struct {
	HasEX         bool
	EX            time.Duration
	HasPX         bool
	PX            time.Duration
	HasEXAT       bool
	EXAT          time.Duration
	HasPXAT       bool
	PXAT          time.Duration
	HasNX         bool
	HasXX         bool
	OnlyUpdateTTL bool
}

// Put sets the value for the given key. It overwrites any previous value
// for that key, and it's thread-safe. The key has to be a string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) Put(ctx context.Context, key string, value any, cfg *PutConfig) error {
	valueBuf := pool.Get()
	defer pool.Put(valueBuf)

	enc := resp.New(valueBuf)
	err := enc.Encode(value)
	if err != nil {
		return err
	}

	if cfg == nil {
		cfg = &PutConfig{}
	}
	e := newEnv(ctx)
	e.putConfig = cfg
	e.dmap = dm.name
	e.key = key
	e.value = make([]byte, valueBuf.Len())
	copy(e.value[:], valueBuf.Bytes())
	return dm.put(e)
}
