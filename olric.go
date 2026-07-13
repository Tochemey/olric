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

/*
Package olric provides a distributed cache and in-memory key/value data store.
It can be used both as an embedded Go library and as a language-independent
service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across
a cluster of computers.

Olric is designed to be a distributed cache. But it also provides Publish/Subscribe,
data replication, failure detection and simple anti-entropy services.
So it can be used as an ordinary key/value data store to scale your cloud
application.
*/
package olric

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
	"github.com/tidwall/redcon"
	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/hasher"
	"github.com/tochemey/olric/internal/checkpoint"
	"github.com/tochemey/olric/internal/cluster/balancer"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/cluster/routingtable"
	"github.com/tochemey/olric/internal/dmap"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/locker"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/pubsub"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/pkg/flog"
)

// ReleaseVersion is the current stable version of Olric
const ReleaseVersion string = "0.3.15"

var (
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")

	// ErrServerGone means that a cluster member is closed unexpectedly.
	ErrServerGone = errors.New("server is gone")

	// ErrNotJoinedYet means that the local node has not joined the cluster
	// yet, so it cannot serve requests that depend on cluster membership.
	ErrNotJoinedYet = errors.New("node has not joined the cluster yet")

	// ErrKeyNotFound means that returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyFound means that the requested key found in the cluster.
	ErrKeyFound = errors.New("key found")

	// ErrWriteQuorum means that write quorum cannot be reached to operate.
	ErrWriteQuorum = errors.New("write quorum cannot be reached")

	// ErrReadQuorum means that read quorum cannot be reached to operate.
	ErrReadQuorum = errors.New("read quorum cannot be reached")

	// ErrLockNotAcquired is returned when the requested lock could not be acquired
	ErrLockNotAcquired = errors.New("lock not acquired")

	// ErrNoSuchLock is returned when the requested lock does not exist
	ErrNoSuchLock = errors.New("no such lock")

	// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
	ErrClusterQuorum = errors.New("failed to find enough peers to create quorum")

	// ErrKeyTooLarge means that the given key is too large to process.
	// Maximum length of a key is 256 bytes.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrEntryTooLarge returned if the required space for an entry is bigger than table size.
	ErrEntryTooLarge = errors.New("entry too large for the configured table size")

	// ErrConnRefused returned if the target node refused a connection request.
	// It is good to call RefreshMetadata to update the underlying data structures.
	ErrConnRefused = errors.New("connection refused")

	// ErrMemberClosing is returned by embedded operations that need the member's
	// shared cluster client after Shutdown has begun tearing it down.
	ErrMemberClosing = errors.New("member is shutting down")
)

// Olric implements a distributed cache and in-memory key/value data store.
// It can be used both as an embedded Go library and as a language-independent
// service.
type Olric struct {
	// name is BindAddr:BindPort. It defines servers unique name in the cluster.
	name     string
	env      *environment.Environment
	config   *config.Config
	log      *flog.Logger
	hashFunc hasher.Hasher

	// Logical units to store data
	primary *partitions.Partitions
	backup  *partitions.Partitions

	// RESP server and clients.
	server *server.Server
	client *server.Client

	rt       *routingtable.RoutingTable
	balancer *balancer.Balancer

	pubsub *pubsub.Service
	dmap   *dmap.Service

	// Per-instance readiness checkpoints. A failed start of another instance
	// in the same process must not block this instance's Started callback.
	checkpoint *checkpoint.Checkpoint

	// The cluster client shared by every EmbeddedDMap on this member. Scan and
	// Pipeline need one, and it owns a routing table fetcher goroutine plus a set
	// of connection pools. The member owns it so that Shutdown reclaims it: an
	// orphaned fetcher would otherwise outlive the member and keep dialing its
	// dead address forever.
	//
	// clusterClientMtx guards clusterClient against the create/close race.
	// Shutdown sets clusterClientClosed under this lock and embeddedClusterClient
	// creates under the same lock, so a Scan racing Shutdown can never construct a
	// client behind the teardown. Checking db.ctx instead would be a check-then-act
	// race, exactly as it is for wg.Add against wg.Wait.
	clusterClientMtx    sync.Mutex
	clusterClient       *ClusterClient
	clusterClientClosed bool

	// Structures for flow control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	started func()
}

func prepareConfig(c *config.Config) (*config.Config, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	err := c.Sanitize()
	if err != nil {
		return nil, err
	}

	err = c.Validate()
	if err != nil {
		return nil, err
	}

	err = c.SetupNetworkConfig()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr,
		strconv.Itoa(c.BindPort))

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.LogLevel)),
		Writer:   c.Logger.Writer(),
	}
	c.Logger.SetOutput(filter)

	return c, nil
}

func initializeServices(db *Olric) error {
	if db.config.ReplicaCount > config.MinimumReplicaCount {
		db.env.Set("syncstate", syncstate.New())
	}
	db.rt = routingtable.New(db.env)
	db.env.Set("routingtable", db.rt)
	// The routing table prunes its own connection pool on NodeLeave, but the
	// embedded cluster client owns a second pool whose round-robin rotation
	// would otherwise keep the departed address forever: every Nth pick would
	// dial a dead member, which on a packet-dropping network burns the full
	// dial timeout instead of failing fast. The ban is sticky — a stale
	// routing table snapshot cannot resurrect the address into rotation — so
	// the join callback lifts it when a member comes back under the same
	// address.
	db.rt.AddNodeLeaveCallback(db.pruneEmbeddedClusterClientPool)
	db.rt.AddNodeJoinCallback(db.restoreEmbeddedClusterClientPool)

	db.balancer = balancer.New(db.env)
	if db.config.EnableProactiveSyncOnJoin {
		db.rt.AddCallback(db.balancer.BalanceEagerly)
	}

	// Add Services
	dt, err := pubsub.NewService(db.env)
	if err != nil {
		return err
	}
	db.pubsub = dt.(*pubsub.Service)

	dm, err := dmap.NewService(db.env)
	if err != nil {
		return err
	}
	db.dmap = dm.(*dmap.Service)

	return nil
}

// New creates a new Olric instance, otherwise returns an error.
func New(config *config.Config) (*Olric, error) {
	var err error
	config, err = prepareConfig(config)
	if err != nil {
		return nil, err
	}

	e := environment.New()
	e.Set("config", config)

	// Set the hash function. Olric distributes keys over partitions by hashing.
	partitions.SetHashFunc(config.Hasher)

	flogger := flog.New(config.Logger)
	flogger.SetLevel(config.LogVerbosity)
	if config.LogLevel == "DEBUG" {
		flogger.ShowLineNumber(1)
	}
	e.Set("logger", flogger)

	client := server.NewClient(config.Client)
	e.Set("client", client)
	e.Set("primary", partitions.New(config.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(config.PartitionCount, partitions.BACKUP))
	e.Set("locker", locker.New())
	cp := checkpoint.New()
	e.Set("checkpoint", cp)
	ctx, cancel := context.WithCancel(context.Background())
	db := &Olric{
		name:       config.MemberlistConfig.Name,
		env:        e,
		log:        flogger,
		config:     config,
		hashFunc:   config.Hasher,
		client:     client,
		primary:    e.Get("primary").(*partitions.Partitions),
		backup:     e.Get("backup").(*partitions.Partitions),
		started:    config.Started,
		checkpoint: cp,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Create a Redcon server instance
	rc := &server.Config{
		BindAddr:        config.BindAddr,
		BindPort:        config.BindPort,
		KeepAlivePeriod: config.KeepAlivePeriod,
	}

	if config.TLS != nil {
		rc.TLS = config.TLS.Server
	}

	srv := server.New(rc, flogger, cp)
	srv.SetPreConditionFunc(db.preconditionFunc)

	db.server = srv
	e.Set("server", srv)

	err = initializeServices(db)
	if err != nil {
		return nil, err
	}

	db.registerCommandHandlers()

	return db, nil
}

func (db *Olric) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	err := db.isOperable()
	if err != nil {
		protocol.WriteError(conn, err)
		return false
	}
	return true
}

func (db *Olric) registerCommandHandlers() {
	db.server.ServeMux().HandleFunc(protocol.Generic.Ping, db.pingCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Cluster.RoutingTable, db.clusterRoutingTableCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Generic.Stats, db.statsCommandHandler)
	db.server.ServeMux().HandleFunc(protocol.Cluster.Members, db.clusterMembersCommandHandler)
}

// callStartedCallback checks passed checkpoint count and calls the callback
// function.
func (db *Olric) callStartedCallback() {
	defer db.wg.Done()

	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C:
			if db.checkpoint.AllPassed() {
				if db.started != nil {
					db.started()
				}
				return
			}
		case <-db.ctx.Done():
			return
		}
	}
}

func convertClusterError(err error) error {
	switch {
	case errors.Is(err, routingtable.ErrClusterQuorum):
		return ErrClusterQuorum
	case errors.Is(err, routingtable.ErrServerGone):
		return ErrServerGone
	case errors.Is(err, routingtable.ErrOperationTimeout):
		return ErrOperationTimeout
	default:
		return err
	}
}

// WaitForInitialSync blocks until the initial replica sync is complete for this node,
// or the context is cancelled. Returns nil when sync is complete.
// Use this before marking the Pod ready in Kubernetes (e.g. in a readiness probe).
// When ReplicaCount is 1, returns immediately.
func (db *Olric) WaitForInitialSync(ctx context.Context) error {
	ss := db.env.Get("syncstate")
	if ss == nil {
		return nil
	}
	state := ss.(*syncstate.State)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-state.Done():
			if state.IsDone() {
				return nil
			}
		}
	}
}

// InitialSyncComplete returns a channel that closes when initial sync is complete.
// When ReplicaCount is 1, returns a closed channel.
func (db *Olric) InitialSyncComplete() <-chan struct{} {
	ss := db.env.Get("syncstate")
	if ss == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return ss.(*syncstate.State).Done()
}

// isOperable controls bootstrapping status and cluster quorum to prevent split-brain syndrome.
func (db *Olric) isOperable() error {
	if err := db.rt.CheckMemberCountQuorum(); err != nil {
		return convertClusterError(err)
	}
	// An Olric node has to be bootstrapped to function properly.
	return db.rt.CheckBootstrap()
}

// Start starts background servers and joins the cluster. You still must call Shutdown
// method if Start function returns an early error.
func (db *Olric) Start() error {
	db.log.V(1).Printf("[INFO] Olric %s on %s/%s %s", ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return db.server.ListenAndServe()
	})

	select {
	case <-db.server.StartedCtx.Done():
		// TCP server has been started
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Shutdown here because we could not start anything.
		return errGr.Wait()
	}

	// Balancer works periodically to balance partition data across the cluster.
	if err := db.balancer.Start(); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to run the balancer subsystem: %v", err)
		return err
	}

	// First, we need to join the cluster. Then, the routing table has been started.
	if err := db.rt.Join(); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to join the cluster: %v", err)
		return err
	}
	// Start routing table service and member discovery subsystem.
	if err := db.rt.Start(); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to run the routing table subsystem: %v", err)
		return err
	}

	// Start publish-subscribe service
	if err := db.pubsub.Start(); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to run the Publish-Subscribe service: %v", err)
		return err
	}

	// Start distributed map service
	if err := db.dmap.Start(); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to run the Distributed Map service: %v", err)
		return err
	}

	// Warn the user about his/her choice of configuration
	if db.config.ReplicationMode == config.AsyncReplicationMode && db.config.WriteQuorum > 1 {
		db.log.V(2).
			Printf("[WARN] Node is running in async replication mode. WriteQuorum (%d) is ineffective",
				db.config.WriteQuorum)
	}

	if db.started != nil {
		db.wg.Add(1)
		go db.callStartedCallback()
	}

	db.log.V(2).Printf("[INFO] Node name in the cluster: %s",
		db.name)
	if db.config.Interface != "" {
		db.log.V(2).Printf("[INFO] Node uses interface: %s",
			db.config.Interface)
	}
	db.log.V(2).Printf("[INFO] Node bindAddr: %s, bindPort: %d",
		db.config.BindAddr, db.config.BindPort)
	db.log.V(2).Printf("[INFO] Replication count is %d", db.config.ReplicaCount)

	// Wait for the TCP server.
	return errGr.Wait()
}

// embeddedClusterClient returns the cluster client shared by every EmbeddedDMap
// on this member, creating it on first use. The returned client is owned by the
// member: callers must not close it.
//
// Creation happens under clusterClientMtx, which single-flights it, so
// concurrent first-time callers cannot race two clients into existence.
func (db *Olric) embeddedClusterClient() (*ClusterClient, error) {
	db.clusterClientMtx.Lock()
	defer db.clusterClientMtx.Unlock()

	if db.clusterClientClosed {
		return nil, ErrMemberClosing
	}

	if db.clusterClient != nil {
		return db.clusterClient, nil
	}

	cc, err := NewClusterClient(
		[]string{db.rt.This().String()},
		WithHasher(db.hashFunc),
		WithConfig(db.config.Client),
		// Without these the client falls back to its own stderr logger and drops
		// the member's logging configuration on the floor.
		WithLogger(db.config.Logger),
		withLogVerbosity(db.config.LogVerbosity),
	)
	if err != nil {
		return nil, err
	}

	db.clusterClient = cc
	return cc, nil
}

// pruneEmbeddedClusterClientPool evicts a departed member's connection pool
// from the shared embedded cluster client, removing its address from the
// round-robin rotation used by Pick-based commands (routing table fetches,
// Members, Delete, Destroy). The ban is sticky: a Get through a stale routing
// table snapshot cannot resurrect the address into rotation until
// restoreEmbeddedClusterClientPool lifts it. It runs on the routing table's
// node-leave callback goroutine.
//
// Snapshot-then-act is safe against a concurrent teardown: server.Client.Ban
// on a pool that Shutdown already emptied only records the ban, and a cluster
// client created after this event seeds itself from the live member list, so
// it never learns the departed address in the first place.
func (db *Olric) pruneEmbeddedClusterClientPool(nodeName string) {
	db.clusterClientMtx.Lock()
	cc := db.clusterClient
	db.clusterClientMtx.Unlock()

	if cc == nil {
		return
	}

	if err := cc.client.Ban(nodeName); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to close the connection pool of %s on the embedded cluster client: %v", nodeName, err)
	}
}

// Both hooks run synchronously on the routing table's membership event
// goroutine, so they must stay fast: they only snapshot the client pointer
// under clusterClientMtx and touch the pool's own mutex. The one caller that
// holds clusterClientMtx longer, first-time creation in embeddedClusterClient,
// performs local-only RPCs against this member, so the wait stays bounded in
// milliseconds.

// restoreEmbeddedClusterClientPool lifts the ban pruneEmbeddedClusterClientPool
// placed on an address once a member joins (or is confirmed alive) under it.
func (db *Olric) restoreEmbeddedClusterClientPool(nodeName string) {
	db.clusterClientMtx.Lock()
	cc := db.clusterClient
	db.clusterClientMtx.Unlock()

	if cc == nil {
		return
	}

	cc.client.Unban(nodeName)
}

// closeEmbeddedClusterClient tears down the member's shared cluster client and
// closes the gate, so no later caller can create another one.
func (db *Olric) closeEmbeddedClusterClient() error {
	db.clusterClientMtx.Lock()
	cc := db.clusterClient
	db.clusterClient = nil
	db.clusterClientClosed = true
	db.clusterClientMtx.Unlock()

	if cc == nil {
		return nil
	}

	// Close outside the lock: it waits for the routing table fetcher, and holding
	// the lock across that would block a concurrent Scan on teardown.
	//
	// Close with a fresh context rather than the caller's: Shutdown's context is
	// routinely expired by the time it reaches here, and a spent context makes the
	// pool teardown bail out midway and strand connections.
	return cc.Close(context.Background())
}

// Shutdown stops background servers and leaves the cluster.
func (db *Olric) Shutdown(ctx context.Context) error {
	select {
	case <-db.ctx.Done():
		// Shutdown only once.
		return nil
	default:
	}

	db.cancel()

	var latestError error

	// Tear the embedded cluster client down first, while the RESP server it talks
	// to is still up: its fetcher is aimed at this member's own address, so
	// stopping it now means it never dials a dead socket.
	if err := db.closeEmbeddedClusterClient(); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown the embedded cluster client: %v", err)
		latestError = err
	}

	if err := db.pubsub.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown PubSub service: %v", err)
		latestError = err
	}

	if err := db.dmap.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown DMap service: %v", err)
		latestError = err
	}

	if err := db.balancer.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown balancer service: %v", err)
		latestError = err
	}

	if err := db.rt.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown routing table service: %v", err)
		latestError = err
	}

	// Shutdown Redcon server
	if err := db.server.Shutdown(ctx); err != nil {
		db.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestError = err
	}

	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
		}()
		db.wg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}

	// db.name will be shown as empty string, if the program is killed before
	// bootstrapping.
	db.log.V(2).Printf("[INFO] %s is gone", db.name)
	return latestError
}

func convertDMapError(err error) error {
	switch {
	case errors.Is(err, dmap.ErrKeyFound):
		return ErrKeyFound
	case errors.Is(err, dmap.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrDMapNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrLockNotAcquired):
		return ErrLockNotAcquired
	case errors.Is(err, dmap.ErrNoSuchLock):
		return ErrNoSuchLock
	case errors.Is(err, dmap.ErrReadQuorum):
		return ErrReadQuorum
	case errors.Is(err, dmap.ErrWriteQuorum):
		return ErrWriteQuorum
	case errors.Is(err, dmap.ErrServerGone):
		return ErrServerGone
	case errors.Is(err, dmap.ErrKeyTooLarge):
		return ErrKeyTooLarge
	case errors.Is(err, dmap.ErrEntryTooLarge):
		return ErrEntryTooLarge
	default:
		return convertClusterError(err)
	}
}
