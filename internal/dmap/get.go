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
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/collection"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/ptr"
	"github.com/tochemey/olric/internal/stats"
	"github.com/tochemey/olric/pkg/storage"
)

// Entry is a DMap entry with its metadata.
type Entry struct {
	Key       string
	Value     interface{}
	TTL       int64
	Timestamp int64
}

var (
	// GetMisses is the number of entries that have been requested and not found
	GetMisses = stats.NewInt64Counter()

	// GetHits is the number of entries that have been requested and found present
	GetHits = stats.NewInt64Counter()

	// EvictedTotal is the number of entries removed from cache to free memory for new entries.
	EvictedTotal = stats.NewInt64Counter()
)

// ErrReadQuorum means that read quorum cannot be reached to operate.
var ErrReadQuorum = errors.New("read quorum cannot be reached")

type version struct {
	host  *discovery.Member
	entry storage.Entry
}

func (dm *DMap) getOnFragment(e *env) (storage.Entry, error) {
	part := dm.getPartitionByHKey(e.hkey, e.kind)
	f, err := dm.loadFragment(part)
	if err != nil {
		return nil, err
	}

	f.RLock()
	defer f.RUnlock()

	entry, err := f.storage.Get(e.hkey)
	switch err {
	case storage.ErrKeyNotFound:
		err = ErrKeyNotFound
	case storage.ErrKeyTooLarge:
		err = ErrKeyTooLarge
	}
	if err != nil {
		return nil, err
	}

	if isKeyExpired(entry.TTL()) {
		return nil, ErrKeyNotFound
	}
	return entry, nil
}

// lookupOnPreviousOwner reads key on owner, a previous owner of its partition,
// under ctx, which the caller bounded for the request.
func (x *DMap) lookupOnPreviousOwner(ctx context.Context, owner *discovery.Member, key string) (*version, error) {
	cmd := protocol.NewGetEntry(x.name, key).Command(ctx)
	rc := x.s.client.Get(owner.String())
	err := rc.Process(ctx, cmd)
	if err != nil {
		return nil, protocol.ConvertError(err)
	}
	value, err := cmd.Bytes()
	if err != nil {
		return nil, protocol.ConvertError(err)
	}

	v := &version{host: owner}
	e := x.engine.NewEntry()
	e.Decode(value)
	v.entry = e
	return v, nil
}

func (dm *DMap) valueToVersion(value storage.Entry) *version {
	this := dm.s.rt.This()
	return &version{
		host:  &this,
		entry: value,
	}
}

func (dm *DMap) lookupOnThisNode(hkey uint64, key string) *version {
	// Check on localhost, the partition owner.
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	if err != nil {
		if !errors.Is(err, errFragmentNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get DMap fragment: %v", err)
		}
		return dm.valueToVersion(nil)
	}
	f.RLock()
	defer f.RUnlock()

	value, err := f.storage.Get(hkey)
	if err != nil {
		if !errors.Is(err, storage.ErrKeyNotFound) {
			// still need to use "ver". just log this error.
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on %s: %s", key, dm.name, err)
		}
		return dm.valueToVersion(nil)
	}
	// We found the key
	//
	// LRU and MaxIdleDuration eviction policies are only valid on
	// the partition owner. Normally, we shouldn't need to retrieve the keys
	// from the backup or the previous owners. When the fsck merge
	// a fragmented partition or recover keys from a backup, Olric
	// continue maintaining a reliable access log.
	return dm.valueToVersion(value)
}

// lookupOnOwners collects versions of a key/value pair on the partition owner
// by including previous partition owners. The remote reads run under the
// deadline of the request context ctx.
func (x *DMap) lookupOnOwners(ctx context.Context, hkey uint64, key string) []*version {
	owners := x.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	versions := collection.NewArrayList[*version]()
	versions.Append(x.lookupOnThisNode(hkey, key))

	// The common case has no previous owner, and the local hit must not pay
	// for a bounded context it never uses.
	live := 0
	for i := len(owners) - 2; i >= 0; i-- {
		if x.s.isMember(owners[i]) {
			live++
		}
	}

	if live == 0 {
		return versions.Items()
	}

	ctx, cancel := x.s.remoteCallContext(ctx)
	defer cancel()

	eg := new(errgroup.Group)

	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		if !x.s.isMember(owner) {
			// A departed previous owner cannot answer; its copy is gone.
			continue
		}

		eg.Go(func() error {
			version, err := x.lookupOnPreviousOwner(ctx, &owner, key)
			if err != nil {
				return fmt.Errorf("[ERROR] Failed to call get on a previous primary owner: %s: %v", owner, err)
			}
			versions.Append(version)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		if x.s.log.V(6).Ok() {
			x.s.log.V(6).Println(err.Error())
		}
	}

	return versions.Items()
}

// sortVersions orders versions newest first. The order is stable and the
// comparison strict, so copies with equal timestamps keep the order they were
// looked up in, this member's copy first: the primary's copy wins a tie, and
// a replica cannot win it with a copy that carries the same timestamp.
func (x *DMap) sortVersions(versions []*version) []*version {
	sort.SliceStable(versions,
		func(i, j int) bool {
			return versions[i].entry.Timestamp() > versions[j].entry.Timestamp()
		},
	)
	// Explicit is better than implicit.
	return versions
}

func (dm *DMap) sanitizeAndSortVersions(versions []*version) []*version {
	var sanitized []*version
	// We use versions slice for read-repair. Clear nil values first.
	for _, ver := range versions {
		if ver.entry != nil {
			sanitized = append(sanitized, ver)
		}
	}
	if len(sanitized) <= 1 {
		return sanitized
	}
	return dm.sortVersions(sanitized)
}

// lookupOnReplicas collects the versions of a key/value pair held by the
// replica owners of its partition, under the deadline of the request context
// ctx.
func (x *DMap) lookupOnReplicas(ctx context.Context, hkey uint64, key string) []*version {
	// Check backups.
	backups := x.s.backup.PartitionOwnersByHKey(hkey)
	versions := collection.NewArrayList[*version]()

	// No replica to ask, no bounded context to build.
	live := 0
	for _, replica := range backups {
		if x.s.isMember(replica) {
			live++
		}
	}

	if live == 0 {
		return versions.Items()
	}

	ctx, cancel := x.s.remoteCallContext(ctx)
	defer cancel()

	errGroup := new(errgroup.Group)
	errGroup.SetLimit(live)

	for _, replica := range backups {
		replica := replica
		if !x.s.isMember(replica) {
			// A departed replica owner cannot answer; the others serve the read.
			continue
		}

		errGroup.Go(func() error {
			cmd := protocol.NewGetEntry(x.name, key).SetReplica().Command(ctx)
			rc := x.s.client.Get(replica.String())
			err := rc.Process(ctx, cmd)
			err = protocol.ConvertError(err)
			if err != nil {
				err = fmt.Errorf("[DEBUG] Failed to call get on a replica owner: %s: %v", replica, err)
				return err
			}

			value, err := cmd.Bytes()
			err = protocol.ConvertError(err)
			if err != nil {
				err = fmt.Errorf("[DEBUG] Failed to call get on a replica owner: %s: %v", replica, err)
				return err
			}

			version := &version{host: &replica}
			e := x.engine.NewEntry()
			e.Decode(value)
			version.entry = e
			versions.Append(version)
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		if x.s.log.V(6).Ok() {
			x.s.log.V(6).Println(err.Error())
		}
	}

	return versions.Items()
}

// readRepair writes winner, the freshest version, to every holder of an older
// one. The remote writes run under the deadline of the request context ctx.
func (x *DMap) readRepair(ctx context.Context, winner *version, versions []*version) {
	// Read repair runs on every read; it must cost nothing when every copy
	// already holds the winner, which is the common case.
	stale := 0
	for _, version := range versions {
		if version.entry == nil || winner.entry.Timestamp() != version.entry.Timestamp() {
			stale++
		}
	}

	if stale == 0 {
		return
	}

	ctx, cancel := x.s.remoteCallContext(ctx)
	defer cancel()

	errGroup := new(errgroup.Group)
	errGroup.SetLimit(stale)

	for _, version := range versions {
		version := version
		if version.entry != nil && winner.entry.Timestamp() == version.entry.Timestamp() {
			continue
		}

		errGroup.Go(func() error {
			// Sync
			tmp := *version.host
			if tmp.CompareByID(x.s.rt.This()) {
				hkey := partitions.HKey(x.name, winner.entry.Key())
				part := x.getPartitionByHKey(hkey, partitions.PRIMARY)
				f, err := x.loadOrCreateFragment(part)
				if err != nil {
					return fmt.Errorf("[ERROR] Failed to get or create the fragment for: %s on %s: %v",
						winner.entry.Key(), x.name, err)
				}

				f.Lock()
				e := newEnv(context.Background())
				e.hkey = hkey
				e.fragment = f

				if err := x.putEntryOnFragment(e, winner.entry); err != nil {
					f.Unlock()
					return fmt.Errorf("[ERROR] Failed to synchronize with replica: %v", err)
				}

				f.Unlock()
				return nil
			}

			// If readRepair is enabled, this function is called by every GET request.
			cmd := protocol.NewPutEntry(x.name, winner.entry.Key(), winner.entry.Encode()).Command(ctx)
			rc := x.s.client.Get(version.host.String())

			if err := rc.Process(ctx, cmd); err != nil {
				return fmt.Errorf("[ERROR] Failed to synchronize replica %s: %v", version.host, err)
			}

			if err := cmd.Err(); err != nil {
				return fmt.Errorf("[ERROR] Failed to synchronize replica %s: %v", version.host, err)
			}

			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		x.s.log.V(3).Println(err.Error())
	}
}

// getOnCluster reads key as the primary owner of its partition: it gathers the
// versions held by the owners and the replicas, under the deadline of the
// request context ctx, and returns the freshest one after repairing the rest
// when read repair is enabled.
func (x *DMap) getOnCluster(ctx context.Context, hkey uint64, key string) (storage.Entry, error) {
	// RUnlock should not be called with defer statement here because
	// readRepair function may call putOnFragment function which needs a write
	// lock. Please don't forget calling RUnlock before returning here.
	versions := x.lookupOnOwners(ctx, hkey, key)
	if x.s.config.ReadQuorum >= config.MinimumReplicaCount {
		v := x.lookupOnReplicas(ctx, hkey, key)
		versions = append(versions, v...)
	}

	if len(versions) < x.s.config.ReadQuorum {
		return nil, ErrReadQuorum
	}

	sorted := x.sanitizeAndSortVersions(versions)
	if len(sorted) == 0 {
		// We checked everywhere, it's not here.
		return nil, ErrKeyNotFound
	}

	if len(sorted) < x.s.config.ReadQuorum {
		return nil, ErrReadQuorum
	}

	// The most up-to-date version of the values.
	winner := sorted[0]
	if isKeyExpired(winner.entry.TTL()) || x.isKeyIdle(hkey) {
		return nil, ErrKeyNotFound
	}

	if x.s.config.ReadRepair {
		// Parallel read operations may propagate different versions of
		// the same key/value pair. The rule is simple: last write wins.
		x.readRepair(ctx, winner, versions)
	}
	return winner.entry, nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contain the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (x *DMap) Get(ctx context.Context, key string) (storage.Entry, *uint64, error) {
	hkey := partitions.HKey(x.name, key)
	partition := x.s.primary.PartitionByHKey(hkey)
	member := partition.Owner()
	// We are on the partition owner
	if member.CompareByName(x.s.rt.This()) {
		entry, err := x.getOnCluster(ctx, hkey, key)
		if errors.Is(err, ErrKeyNotFound) {
			GetMisses.Increase(1)
		}
		if err != nil {
			return nil, nil, err
		}

		// number of keys that have been requested and found present
		GetHits.Increase(1)

		return entry, ptr.To(partition.ID()), nil
	}

	// Redirect to the partition owner
	cmd := protocol.NewGet(x.name, key).SetRaw().Command(x.s.ctx)
	rc := x.s.client.Get(member.String())
	err := rc.Process(ctx, cmd)
	if err != nil {
		return nil, nil, protocol.ConvertError(err)
	}

	value, err := cmd.Bytes()
	if err != nil {
		return nil, nil, protocol.ConvertError(err)
	}

	// number of keys that have been requested and found present
	GetHits.Increase(1)

	entry := x.engine.NewEntry()
	entry.Decode(value)
	return entry, ptr.To(partition.ID()), nil
}
