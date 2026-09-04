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
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/pkg/storage"
)

type fragment struct {
	sync.RWMutex

	service *Service
	storage storage.Engine
	ctx     context.Context
	cancel  context.CancelFunc
	// inFlight counts the writes of this fragment that are between their
	// preparation and their local store, their replication in between,
	// and hold the fragment lock for neither. The janitor leaves a fragment
	// alone while it is not zero: wiping an empty fragment then would detach
	// the store the write is about to land in.
	inFlight atomic.Int32
}

func (f *fragment) Stats() storage.Stats {
	f.RLock()
	defer f.RUnlock()

	return f.storage.Stats()
}

func (f *fragment) Compaction() (bool, error) {
	select {
	case <-f.ctx.Done():
		// fragment is closed or destroyed
		return false, nil
	default:
	}
	return f.storage.Compaction()
}

func (f *fragment) Destroy() error {
	select {
	case <-f.ctx.Done():
		return f.storage.Destroy()
	default:
	}
	return errors.New("fragment is not closed")
}

func (f *fragment) Close() error {
	defer f.cancel()
	return f.storage.Close()
}

func (f *fragment) Name() string {
	return "DMap"
}

// transfer sends payload, one encoded storage table, to every owner in turn as
// a fragment pack of kind, publishing a migration event per owner when cluster
// events are enabled. It stops at the first owner that fails. The caller holds
// the fragment lock.
func (x *fragment) transfer(part *partitions.Partition, name string, owners []discovery.Member, kind partitions.Kind, payload []byte) error {
	fp := &fragmentPack{
		PartID:  part.ID(),
		Kind:    kind,
		Name:    strings.TrimPrefix(name, "dmap."),
		Payload: payload,
	}
	value, err := msgpack.Marshal(fp)
	if err != nil {
		return err
	}

	// An owner that left is not dialed, but the owners after it still
	// receive the table: the transfer then fails as a whole, so the balancer
	// retries it against the next routing table, and the copies that did
	// land merge again without harm.
	var departed error

	for _, owner := range owners {
		if !x.service.isMember(owner) {
			departed = fmt.Errorf("%w: %s", errOwnerDeparted, owner)
			continue
		}

		if x.service.config.EnableClusterEventsChannel {
			e := &events.FragmentMigrationEvent{
				Kind:          events.KindFragmentMigrationEvent,
				Source:        x.service.rt.This().String(),
				Target:        owner.String(),
				DataStructure: "dmap",
				PartitionID:   part.ID(),
				Identifier:    fp.Name,
				Length:        len(value),
				IsBackup:      kind == partitions.BACKUP,
				Timestamp:     time.Now().UnixNano(),
			}
			x.service.spawn(func() { x.service.publishEvent(e) })
		}

		// Bounded like every other remote call: a live owner that stops
		// answering costs one bounded attempt, not the client's retry chain.
		ctx, cancel := x.service.remoteCallContext(x.service.ctx)
		cmd := protocol.NewMoveFragment(value).Command(ctx)
		rc := x.service.client.Get(owner.String())
		err := rc.Process(ctx, cmd)
		cancel()

		if err != nil {
			return err
		}

		if err := cmd.Err(); err != nil {
			return err
		}
	}

	return departed
}

// moveTable transfers the first live table of the fragment to owners as kind
// and, when drop is set, removes it from the fragment once every owner has
// accepted it. Moves run one table per call so a large fragment is relocated
// in bounded steps; the balancer calls again while the fragment holds data.
func (x *fragment) moveTable(part *partitions.Partition, name string, owners []discovery.Member, kind partitions.Kind, drop bool) error {
	x.Lock()
	defer x.Unlock()

	i := x.storage.TransferIterator()
	if !i.Next() {
		return nil
	}

	payload, index, err := i.Export()
	if err != nil {
		return err
	}

	if err := x.transfer(part, name, owners, kind, payload); err != nil {
		return err
	}

	if drop {
		return i.Drop(index)
	}

	return nil
}

// Move transfers ownership of the fragment's first live table to owners, as
// the fragment's own kind, and drops it locally.
func (x *fragment) Move(part *partitions.Partition, name string, owners []discovery.Member) error {
	return x.moveTable(part, name, owners, part.Kind(), true)
}

// MoveWithTargetKind is Move with the receiver merging into targetKind. When
// pushing to backups (replication), keep data on primary. Only drop when
// moving ownership.
func (x *fragment) MoveWithTargetKind(part *partitions.Partition, name string, owners []discovery.Member, targetKind partitions.Kind) error {
	return x.moveTable(part, name, owners, targetKind, targetKind != partitions.BACKUP)
}

// Replicate copies every live table of the fragment to owners, to be merged
// into the partition of targetKind, and keeps the local copy. Engines whose
// transfer iterator implements storage.Replicator are copied one table at a
// time: each table is encoded under the fragment lock and sent with the lock
// released, so the reads and writes of the fragment go through between
// tables and a slow owner never holds the fragment for the whole copy. The
// receiver's merge is version-aware, so a write that lands in between is not
// overtaken by the older copy. Any other engine is copied through Export,
// which reaches only the first live table.
func (x *fragment) Replicate(part *partitions.Partition, name string, owners []discovery.Member, targetKind partitions.Kind) error {
	send := func(payload []byte) error {
		return x.transfer(part, name, owners, targetKind, payload)
	}

	x.Lock()
	i := x.storage.TransferIterator()
	r, ok := i.(storage.Replicator)
	if !ok {
		defer x.Unlock()

		if !i.Next() {
			return nil
		}

		payload, _, err := i.Export()
		if err != nil {
			return err
		}

		return send(payload)
	}

	x.Unlock()

	for index := 0; ; {
		x.Lock()
		payload, next, err := r.ExportFrom(index)
		x.Unlock()

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		if err := send(payload); err != nil {
			return err
		}

		index = next
	}
}

func (dm *DMap) newFragment() (*fragment, error) {
	c := storage.NewConfig(dm.config.engine.Config)
	engine, err := dm.engine.Fork(c)
	if err != nil {
		return nil, err
	}

	engine.SetLogger(dm.s.config.Logger)
	err = engine.Start()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &fragment{
		service: dm.s,
		storage: engine,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (dm *DMap) loadOrCreateFragment(part *partitions.Partition) (*fragment, error) {
	part.Lock()
	defer part.Unlock()

	// Critical section here. It should be protected by a lock.
	fg, ok := part.Map().Load(dm.fragmentName)
	if ok {
		// We already have the fragment.
		return fg.(*fragment), nil
	}

	f, err := dm.newFragment()
	if err != nil {
		return nil, err
	}

	part.Map().Store(dm.fragmentName, f)
	return f, nil
}

func (dm *DMap) loadFragment(part *partitions.Partition) (*fragment, error) {
	f, ok := part.Map().Load(dm.fragmentName)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

var _ partitions.Fragment = (*fragment)(nil)
