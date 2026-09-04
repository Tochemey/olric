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
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/stats"
)

var (
	// DeleteHits is the number of deletion requests resulting in an item being removed.
	DeleteHits = stats.NewInt64Counter()

	// DeleteMisses is the number of deletion requests for missing keys.
	DeleteMisses = stats.NewInt64Counter()
)

func (dm *DMap) deleteFromFragment(key string, kind partitions.Kind) error {
	hkey := partitions.HKey(dm.name, key)
	part := dm.getPartitionByHKey(hkey, kind)
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// key doesn't exist
		return nil
	}
	if err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	return f.storage.Delete(hkey)
}

// deleteFromPreviousOwners deletes key on the previous primary owners of its
// partition, under the deadline of the request context ctx. A previous owner
// memberlist has removed fails the delete at once, without a dial: a delete
// has no tombstone, so a copy left on a member that is only suspected would
// come back through a read, and the caller retries once the routing table
// has dropped the member.
func (x *DMap) deleteFromPreviousOwners(ctx context.Context, key string, owners []discovery.Member) error {
	if len(owners) < 2 {
		return nil
	}

	ctx, cancel := x.s.remoteCallContext(ctx)
	defer cancel()

	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		if !x.s.isMember(owner) {
			return fmt.Errorf("%w: %s", errOwnerDeparted, owner)
		}

		cmd := protocol.NewDelEntry(x.name, key).Command(ctx)
		rc := x.s.client.Get(owner.String())
		err := rc.Process(ctx, cmd)
		if err != nil {
			return protocol.ConvertError(err)
		}
		err = cmd.Err()
		if err != nil {
			return protocol.ConvertError(err)
		}
	}
	return nil
}

// deleteBackupOnCluster deletes the replicas of key, under the deadline of the
// request context ctx. A replica owner memberlist has removed fails the
// delete at once, without a dial, for the reason given on
// deleteFromPreviousOwners.
func (x *DMap) deleteBackupOnCluster(ctx context.Context, hkey uint64, key string) error {
	owners := x.s.backup.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		return nil
	}

	for _, owner := range owners {
		if !x.s.isMember(owner) {
			return fmt.Errorf("%w: %s", errOwnerDeparted, owner)
		}
	}

	ctx, cancel := x.s.remoteCallContext(ctx)
	defer cancel()

	var g errgroup.Group
	for _, owner := range owners {
		mem := owner
		g.Go(func() error {
			cmd := protocol.NewDelEntry(x.name, key).SetReplica().Command(ctx)
			rc := x.s.client.Get(mem.String())
			err := rc.Process(ctx, cmd)
			if err != nil {
				x.s.log.V(3).Printf("[ERROR] Failed to delete replica key/value on %s: %s", x.name, err)
				return protocol.ConvertError(err)
			}
			return protocol.ConvertError(cmd.Err())
		})
	}
	return g.Wait()
}

// deleteRemoteCopies deletes key on the previous owners and on the replica
// owners of its partition, under the deadline of the request context ctx. It
// takes no fragment lock: it is network work.
func (x *DMap) deleteRemoteCopies(ctx context.Context, hkey uint64, key string) error {
	owners := x.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	if err := x.deleteFromPreviousOwners(ctx, key, owners); err != nil {
		return err
	}

	if x.s.config.ReplicaCount != 0 {
		if err := x.deleteBackupOnCluster(ctx, hkey, key); err != nil {
			return err
		}
	}

	return nil
}

// deleteOnCluster deletes key everywhere, the local fragment f last. It is not
// a thread-safe function: the caller holds the fragment lock, which the
// eviction paths already do while they scan the fragment.
func (x *DMap) deleteOnCluster(ctx context.Context, hkey uint64, key string, f *fragment) error {
	if err := x.deleteRemoteCopies(ctx, hkey, key); err != nil {
		return err
	}

	if err := f.storage.Delete(hkey); err != nil {
		return err
	}

	// DeleteHits is the number of deletion reqs resulting in an item being removed.
	DeleteHits.Increase(1)

	return nil
}

// deleteKey deletes key as the primary owner of its partition. Like
// putOnCluster it holds the key lock for the whole operation and the fragment
// lock only around the storage accesses, so the remote deletes run without
// blocking the rest of the fragment.
func (x *DMap) deleteKey(ctx context.Context, key string) error {
	hkey := partitions.HKey(x.name, key)
	part := x.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := x.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	keyLock := x.s.keyLock(hkey)
	keyLock.Lock()
	defer keyLock.Unlock()

	// Check the HKey before trying to delete it.
	f.Lock()
	exists := f.storage.Check(hkey)
	f.Unlock()

	if !exists {
		// DeleteMisses is the number of deletions reqs for missing keys
		DeleteMisses.Increase(1)
		// The copies elsewhere are deleted all the same: a primary owner
		// that took a partition over after a departure holds nothing until
		// the backup copy is restored, and a delete that stopped here would
		// let the restore bring the key back.
		return x.deleteRemoteCopies(ctx, hkey, key)
	}

	if err := x.deleteRemoteCopies(ctx, hkey, key); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()

	if err := f.storage.Delete(hkey); err != nil {
		return err
	}

	// DeleteHits is the number of deletion reqs resulting in an item being removed.
	DeleteHits.Increase(1)

	return nil
}

// deleteKeys deletes keys from their owners, local and remote, and returns how
// many of them existed.
func (x *DMap) deleteKeys(ctx context.Context, keys ...string) (int, error) {
	members := make(map[discovery.Member][]string)
	for _, key := range keys {
		hkey := partitions.HKey(x.name, key)
		member := x.s.primary.PartitionByHKey(hkey).Owner()
		members[member] = append(members[member], key)
	}

	// Fan out to every owner instead of stopping at the first remote one,
	// following the errgroup pattern used by deleteBackupOnCluster.
	var count int64
	var g errgroup.Group
	for member, distributedKeys := range members {
		if member.CompareByName(x.s.rt.This()) {
			g.Go(func() error {
				for _, key := range distributedKeys {
					if err := x.deleteKey(ctx, key); err != nil {
						return err
					}
				}
				atomic.AddInt64(&count, int64(len(distributedKeys)))
				return nil
			})
			continue
		}

		g.Go(func() error {
			cmd := protocol.NewDel(x.name, distributedKeys...).Command(x.s.ctx)
			rc := x.s.client.Get(member.String())
			if err := rc.Process(ctx, cmd); err != nil {
				return protocol.ConvertError(err)
			}
			if err := cmd.Err(); err != nil {
				return protocol.ConvertError(err)
			}
			atomic.AddInt64(&count, int64(len(distributedKeys)))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return int(count), nil
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (dm *DMap) Delete(ctx context.Context, keys ...string) (int, error) {
	return dm.deleteKeys(ctx, keys...)
}
