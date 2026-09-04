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
	"reflect"
	"sync"
	"time"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/cluster/routingtable"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/locker"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/service"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/pkg/flog"
	"github.com/tochemey/olric/pkg/storage"
)

var errFragmentNotFound = errors.New("fragment not found")

type storageMap struct {
	engines map[string]storage.Engine
	configs map[string]map[string]interface{}
}

type Service struct {
	sync.RWMutex // protects dmaps map

	log       *flog.Logger
	config    *config.Config
	client    *server.Client
	server    *server.Server
	rt        *routingtable.RoutingTable
	primary   *partitions.Partitions
	backup    *partitions.Partitions
	locker    *locker.Locker
	dmaps     map[string]*DMap
	storage   *storageMap
	syncState *syncstate.State
	// keys serializes the operations on one key across replication, see
	// keyLocks.
	keys   keyLocks
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// shutdownMtx guards closed against the wg.Add/wg.Wait race. Shutdown sets
	// closed under this lock before calling wg.Wait, and spawn takes the same
	// lock around wg.Add, so a background goroutine spawned from an untracked
	// caller (a request handler or the balancer) can never drive wg.Add
	// concurrently with wg.Wait.
	shutdownMtx sync.Mutex
	closed      bool
}

func registerErrors() {
	protocol.SetError("NOSUCHLOCK", ErrNoSuchLock)
	protocol.SetError("LOCKNOTACQUIRED", ErrLockNotAcquired)
	protocol.SetError("READQUORUM", ErrReadQuorum)
	protocol.SetError("WRITEQUORUM", ErrWriteQuorum)
	protocol.SetError("DMAPNOTFOUND", ErrDMapNotFound)
	protocol.SetError("KEYTOOLARGE", ErrKeyTooLarge)
	protocol.SetError("ENTRYTOOLARGE", ErrEntryTooLarge)
	protocol.SetError("KEYNOTFOUND", ErrKeyNotFound)
	protocol.SetError("KEYFOUND", ErrKeyFound)
}

func NewService(e *environment.Environment) (service.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config:  e.Get("config").(*config.Config),
		client:  e.Get("client").(*server.Client),
		server:  e.Get("server").(*server.Server),
		log:     e.Get("logger").(*flog.Logger),
		rt:      e.Get("routingtable").(*routingtable.RoutingTable),
		primary: e.Get("primary").(*partitions.Partitions),
		backup:  e.Get("backup").(*partitions.Partitions),
		locker:  e.Get("locker").(*locker.Locker),
		storage: &storageMap{
			engines: make(map[string]storage.Engine),
			configs: make(map[string]map[string]interface{}),
		},
		dmaps:  make(map[string]*DMap),
		ctx:    ctx,
		cancel: cancel,
	}
	if v := e.Get("syncstate"); v != nil {
		s.syncState = v.(*syncstate.State)
	}
	registerErrors()
	s.RegisterHandlers()
	return s, nil
}

func (s *Service) isAlive() bool {
	select {
	case <-s.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

func getType(data interface{}) string {
	t := reflect.TypeOf(data)
	if t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	}
	return t.Name()
}

// spawn runs fn on a background goroutine tracked by the service WaitGroup,
// unless shutdown has already begun. It is the only sanctioned way to start a
// tracked goroutine from a caller that is not itself counted by the WaitGroup
// (request handlers, the balancer): taking shutdownMtx around wg.Add — with
// closed set under the same lock before Shutdown's wg.Wait — guarantees wg.Add
// never races wg.Wait. It reports whether fn was started; false means the
// service is shutting down and the work was dropped, which is safe because the
// node is leaving.
func (s *Service) spawn(fn func()) bool {
	s.shutdownMtx.Lock()
	defer s.shutdownMtx.Unlock()
	if s.closed {
		return false
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
	return true
}

func (s *Service) publishEvent(e events.Event) {
	data, err := e.Encode()
	if err != nil {
		s.log.V(3).Printf("[ERROR] Failed to encode %s: %v", getType(e), err)
		return
	}
	err = s.rt.PublishClusterEvent(s.ctx, events.ClusterEventsChannel, data)
	if err != nil {
		s.log.V(3).Printf("[ERROR] Failed to publish %s to %s: %v",
			getType(e), events.ClusterEventsChannel, err)
	}
}

// Start starts the distributed map service.
func (s *Service) Start() error {
	s.wg.Add(1)
	go s.janitorWorker()

	s.wg.Add(1)
	go s.compactionWorker()

	s.wg.Add(1)
	go s.evictKeysAtBackground()

	if s.syncState != nil && s.config.EnableClusterEventsChannel {
		s.wg.Add(1)
		go s.initialSyncCompletePublisher()
	}

	return nil
}

func (s *Service) initialSyncCompletePublisher() {
	defer s.wg.Done()
	select {
	case <-s.syncState.Done():
		if !s.syncState.IsDone() {
			return
		}
		e := &events.InitialSyncCompleteEvent{
			Kind:      events.KindInitialSyncCompleteEvent,
			Source:    s.rt.This().String(),
			Timestamp: time.Now().UnixNano(),
		}
		s.spawn(func() { s.publishEvent(e) })
	case <-s.ctx.Done():
	}
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.cancel()

	// Flip closed under shutdownMtx before waiting so no further spawn can call
	// wg.Add once wg.Wait is in progress. Any spawn already past the guard has
	// completed its wg.Add under the same lock, so it is counted before Wait.
	s.shutdownMtx.Lock()
	s.closed = true
	s.shutdownMtx.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}
	return nil
}

var _ service.Service = (*Service)(nil)
