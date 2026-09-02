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

package routingtable

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/checkpoint"
	"github.com/tochemey/olric/internal/consistent"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/service"
	"github.com/tochemey/olric/internal/syncstate"
	"github.com/tochemey/olric/pkg/flog"

	"github.com/tochemey/olric/internal/cluster/partitions"
)

// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
var ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

// tableVersion identifies the routing table installed on a member. It is
// published as an immutable snapshot so that signature and generation are
// always read together.
type tableVersion struct {
	// signature is the xxhash of the installed payload. It doubles as the
	// rebalance epoch id and is a pure function of the table content, so it
	// repeats when the table returns to an earlier state.
	signature uint64
	// generation counts the installs whose signature differed from the
	// previously installed one. Unlike the signature it never repeats: a
	// change back to an earlier signature advances it, an unchanged table
	// pushed again does not.
	generation uint64
}

type route struct {
	Owners  []discovery.Member
	Backups []discovery.Member
}

type RoutingTable struct {
	sync.RWMutex // routingMtx

	// Currently owned partition count. Approximate LRU implementation
	// uses that.
	ownedPartitionCount uint64
	// version is the installed routing table's signature and generation,
	// see tableVersion and Version.
	version atomic.Pointer[tableVersion]
	// numMembers is used to check cluster quorum.
	numMembers int32

	// These values is useful to control operation status.
	bootstrapped int32

	updateRoutingMtx sync.Mutex
	table            map[uint64]*route
	consistent       *consistent.Consistent
	this             discovery.Member
	members          *Members
	config           *config.Config
	log              *flog.Logger
	primary          *partitions.Partitions
	backup           *partitions.Partitions
	client           *server.Client
	server           *server.Server
	discovery        *discovery.Discovery
	callbacks        []func()
	callbackMtx      sync.Mutex
	// leaveCallbacks receive the name (host:port) of every member that leaves
	// the cluster; joinCallbacks receive the name of every member that joins
	// or is confirmed alive by an update. The member uses these to ban and
	// unban addresses in connection pools it owns outside this package; only
	// the pool at r.client is pruned here.
	leaveCallbacks    []func(nodeName string)
	joinCallbacks     []func(nodeName string)
	memberCallbackMtx sync.Mutex
	// eventPublisher delivers cluster events to subscribers on every member.
	// It is registered by the pubsub service; the routing table cannot
	// deliver events on its own.
	eventPublisher    ClusterEventPublisher
	eventPublisherMtx sync.RWMutex
	pushPeriod        time.Duration
	// The command handlers of the routing table service should wait for the cluster join event.
	joined chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	rebalanceMtx   sync.Mutex
	rebalanceState rebalanceState
	// earlyAcks buffers rebalance acks that arrive before their epoch becomes
	// active (epoch -> member IDs). Guarded by rebalanceMtx.
	earlyAcks map[uint64]map[uint64]struct{}

	// committedPayload holds the last routing table payload ([]byte) that was
	// successfully committed to the cluster. It backs the pull-based bootstrap
	// path and is deliberately independent of the routing mutex: a stalled
	// push fan-out must not block table pulls.
	committedPayload atomic.Value

	syncState  *syncstate.State
	checkpoint *checkpoint.Checkpoint
}

func registerErrors() {
	protocol.SetError("CLUSTERQUORUM", ErrClusterQuorum)
	protocol.SetError("CLUSTERJOIN", ErrClusterJoin)
	protocol.SetError("SERVERGONE", ErrServerGone)
	protocol.SetError("OPERATIONTIMEOUT", ErrOperationTimeout)
}

func New(e *environment.Environment) *RoutingTable {
	cp, _ := e.Get("checkpoint").(*checkpoint.Checkpoint)
	if cp == nil {
		cp = checkpoint.New()
	}
	// The routing table has to be started properly before accepting connections.
	cp.Add()
	c := e.Get("config").(*config.Config)
	log := e.Get("logger").(*flog.Logger)

	ctx, cancel := context.WithCancel(context.Background())
	cc := consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: 20, // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,
	}

	rt := &RoutingTable{
		earlyAcks:  make(map[uint64]map[uint64]struct{}),
		members:    newMembers(),
		discovery:  discovery.New(log, c),
		config:     c,
		log:        log,
		consistent: consistent.New(nil, cc),
		primary:    e.Get("primary").(*partitions.Partitions),
		backup:     e.Get("backup").(*partitions.Partitions),
		client:     e.Get("client").(*server.Client),
		server:     e.Get("server").(*server.Server),
		pushPeriod: c.RoutingTablePushInterval,
		joined:     make(chan struct{}),
		checkpoint: cp,
		ctx:        ctx,
		cancel:     cancel,
	}
	if v := e.Get("syncstate"); v != nil {
		rt.syncState = v.(*syncstate.State)
	}
	registerErrors()
	rt.RegisterHandlers()
	return rt
}

func (r *RoutingTable) Discovery() *discovery.Discovery {
	return r.discovery
}

func (r *RoutingTable) This() discovery.Member {
	return r.this
}

// IsJoined reports whether this node has completed the cluster join. Observing
// the closed joined channel establishes a happens-before edge with the write
// of r.this in Join, so This() is safe to read once IsJoined returns true.
func (r *RoutingTable) IsJoined() bool {
	select {
	case <-r.joined:
		return true
	default:
		return false
	}
}

// setNumMembers assigns the current number of members in the cluster to a variable.
func (r *RoutingTable) setNumMembers() {
	// Calling NumMembers in every request is quite expensive.
	// It's rarely updated. Just call this when the membership info changed.
	nr := int32(r.discovery.NumMembers())
	atomic.StoreInt32(&r.numMembers, nr)
}

func (r *RoutingTable) SetNumMembersEagerly(nr int32) {
	atomic.StoreInt32(&r.numMembers, nr)
}

func (r *RoutingTable) NumMembers() int32 {
	return atomic.LoadInt32(&r.numMembers)
}

func (r *RoutingTable) Members() *Members {
	return r.members
}

// setSignature records the installation of a routing table with the given
// signature and advances the generation when the signature changed. Installs
// are serialized by updateRoutingMtx.
func (r *RoutingTable) setSignature(s uint64) {
	var generation uint64
	changed := true
	if current := r.version.Load(); current != nil {
		generation = current.generation
		changed = current.signature != s
	}
	if changed {
		generation++
	}
	r.version.Store(&tableVersion{signature: s, generation: generation})
}

// Signature returns the signature of the installed routing table, zero before
// the first install.
func (r *RoutingTable) Signature() uint64 {
	signature, _ := r.Version()
	return signature
}

// Generation returns the generation of the installed routing table, zero
// before the first install. See tableVersion for how it relates to the
// signature.
func (r *RoutingTable) Generation() uint64 {
	_, generation := r.Version()
	return generation
}

// Version returns the signature and generation of the installed routing table
// as one consistent snapshot. Consumers that key work on the generation but
// report the signature, such as the balancer's rebalance ack, must read both
// here rather than through Signature and Generation separately, otherwise an
// install landing between the two reads pairs a stale signature with a fresh
// generation.
func (r *RoutingTable) Version() (signature, generation uint64) {
	current := r.version.Load()
	if current == nil {
		return 0, 0
	}
	return current.signature, current.generation
}

func (r *RoutingTable) setOwnedPartitionCount() {
	var count uint64
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		part := r.primary.PartitionByID(partID)
		if part.Owner().CompareByID(r.this) {
			count++
		}
	}
	atomic.StoreUint64(&r.ownedPartitionCount, count)
}

func (r *RoutingTable) OwnedPartitionCount() uint64 {
	return atomic.LoadUint64(&r.ownedPartitionCount)
}

func (r *RoutingTable) CheckMemberCountQuorum() error {
	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	if r.config.MemberCountQuorum > r.NumMembers() {
		return ErrClusterQuorum
	}
	return nil
}

func (r *RoutingTable) markBootstrapped() {
	// Bootstrapped by the coordinator.
	atomic.StoreInt32(&r.bootstrapped, 1)
}

func (r *RoutingTable) IsBootstrapped() bool {
	// Bootstrapped by the coordinator.
	return atomic.LoadInt32(&r.bootstrapped) == 1
}

// CheckBootstrap is called for every request and checks whether the node is bootstrapped.
// It has to be very fast for a smooth operation.
func (r *RoutingTable) CheckBootstrap() error {
	// Prevent creating expensive structures for every request,
	// Just check an integer value atomically.
	if r.IsBootstrapped() {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.config.BootstrapTimeout)
	defer cancel()
	return r.tryWithInterval(ctx, 100*time.Millisecond, func() error {
		if r.IsBootstrapped() {
			return nil
		}
		// Final error
		return ErrOperationTimeout
	})
}

func (r *RoutingTable) fillRoutingTable() {
	if r.config.ReplicaCount > int(r.NumMembers()) {
		r.log.V(1).Printf("[WARN] Desired replica count is %d and "+
			"the cluster has %d members currently",
			r.config.ReplicaCount, r.NumMembers())
	}
	table := make(map[uint64]*route)
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		rt := &route{
			Owners: r.distributePrimaryCopies(partID),
		}
		if r.config.ReplicaCount > config.MinimumReplicaCount {
			rt.Backups = r.distributeBackups(partID)
		}
		table[partID] = rt
	}
	r.table = table
}

func (r *RoutingTable) UpdateEagerly() {
	r.updateRoutingWithReason(rebalanceReasonManual, "")
}

func (r *RoutingTable) updateRoutingWithReason(reason rebalanceReason, node string) {
	// This function is called by listenMemberlistEvents and updateRoutingPeriodically
	// So this lock prevents parallel execution.
	r.Lock()
	defer r.Unlock()

	// This function is only run by the cluster coordinator.
	if !r.discovery.IsCoordinator() {
		return
	}

	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	if err := r.CheckMemberCountQuorum(); err != nil {
		r.log.V(2).Printf("[ERROR] Impossible to calculate and update routing table: %v", err)
		return
	}

	r.fillRoutingTable()
	if r.syncState != nil {
		r.syncState.Reconcile(r.partitionsPendingReceive(), r.config.InitialSyncEmptyPartitionTimeout)
	}
	previousSignature := r.Signature()
	data, signature, err := r.buildRoutingTablePayload()
	if err != nil {
		r.log.V(2).Printf("[ERROR] Failed to marshal routing table: %v", err)
		return
	}

	reports, err := r.updateRoutingTableOnCluster(data)
	if err != nil {
		r.log.V(2).Printf("[ERROR] Failed to update routing table on cluster: %v", err)
		return
	}
	r.committedPayload.Store(data)
	r.processLeftOverDataReports(reports)
	if signature != previousSignature {
		// Only members that confirmed receipt of the table gate the epoch.
		updated := make([]uint64, 0, len(reports))
		for member := range reports {
			updated = append(updated, member.ID)
		}
		r.startRebalanceEpoch(signature, reason, node, updated)
		// Proactive sync pushes primary data to newly joined backup owners.
		// It must NOT run on node-leave events: when nodes die the fragment
		// write-lock held during the network transfer would block concurrent
		// reads for the entire push duration, causing read timeouts.
		if r.config.EnableProactiveSyncOnJoin && reason == rebalanceReasonNodeJoin {
			r.wg.Add(1)
			go r.runCallbacks()
		}
	}
}

// processClusterEvent applies a membership event to the routing table's
// internal state. It returns the member callback notification the event calls
// for (nil if none); the caller fires it after this function returns, so
// callbacks never run under the members lock and always run in event order.
func (r *RoutingTable) processClusterEvent(event *discovery.ClusterEvent) (notify func()) {
	r.Members().Lock()
	defer r.Members().Unlock()

	member, _ := discovery.NewMemberFromMetadata(event.NodeMeta)

	switch event.Event {
	case memberlist.NodeJoin:
		r.Members().Add(member)
		r.consistent.Add(member)
		r.log.V(2).Printf("[INFO] Node joined: %s", member)
		// Lift any ban a leave callback placed on this address in pools owned
		// outside this package.
		notify = func() { r.notifyNodeJoinCallbacks(event.NodeName) }

		if r.config.EnableClusterEventsChannel {
			r.wg.Add(1)
			go r.publishNodeJoinEvent(&member)
		}
	case memberlist.NodeLeave:
		if _, err := r.Members().Get(member.ID); err != nil {
			r.log.V(2).Printf("[ERROR] Unknown node left: %s: %d", event.NodeName, member.ID)
			return nil
		}
		r.Members().Delete(member.ID)
		r.consistent.Remove(event.NodeName)
		// Don't try to used closed sockets again.
		r.log.V(2).Printf("[INFO] Node left: %s", event.NodeName)
		if err := r.client.Close(event.NodeName); err != nil {
			r.log.V(2).Printf("[ERROR] Failed to remove the node from pool %s: %v", event.NodeName, err)
		}
		// The member may own other connection pools (the embedded cluster
		// client's) that keep the dead address in round-robin rotation.
		notify = func() { r.notifyNodeLeaveCallbacks(event.NodeName) }

		if r.config.EnableClusterEventsChannel {
			r.wg.Add(1)
			go r.publishNodeLeftEvent(&member)
		}
	case memberlist.NodeUpdate:
		// Node's birthdate may be changed. Close the pool and re-add to the hash ring.
		// This takes linear time, but member count should be too small for a decent computer!
		r.Members().Range(func(id uint64, item discovery.Member) bool {
			if member.CompareByName(item) {
				r.Members().Delete(id)
				r.consistent.Remove(event.NodeName)
				if err := r.client.Close(event.NodeName); err != nil {
					r.log.V(2).Printf("[ERROR] Failed to remove the node from pool %s: %v", event.NodeName, err)
				}
			}
			return true
		})
		r.Members().Add(member)
		r.consistent.Add(member)
		r.log.V(2).Printf("[INFO] Node updated: %s", member)
		// An update proves the member is alive (a fast restart under the same
		// address, for instance), so it fires the join callbacks: any ban on
		// this address in pools owned outside this package must be lifted.
		notify = func() { r.notifyNodeJoinCallbacks(event.NodeName) }
	default:
		r.log.V(2).Printf("[ERROR] Unknown event received: %v", event)
		return nil
	}

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	r.setNumMembers()
	return notify
}

func (r *RoutingTable) listenClusterEvents(eventCh chan *discovery.ClusterEvent) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case e := <-eventCh:
			notify := r.processClusterEvent(e)
			if notify != nil {
				// Fired here rather than inside processClusterEvent so the
				// callbacks run without the members lock, strictly in event
				// order: a ban from a NodeLeave can never be applied after
				// the unban from a following NodeJoin for the same address.
				notify()
			}
			reason, node := rebalanceReasonFromEvent(e)
			r.updateRoutingWithReason(reason, node)
		}
	}
}

// pullRoutingTableUntilBootstrapped keeps requesting the committed routing
// table from the coordinator until this node is bootstrapped. The coordinator
// push is the fast path; this pull is the guarantee: a joiner whose push was
// lost would otherwise never bootstrap, never run its balancer and never ack a
// rebalance epoch.
func (r *RoutingTable) pullRoutingTableUntilBootstrapped() {
	defer r.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.IsBootstrapped() {
				return
			}
			if err := r.fetchRoutingTableFromCoordinator(); err != nil {
				r.log.V(3).Printf("[WARN] Failed to pull routing table from the coordinator: %v", err)
				continue
			}
			if r.IsBootstrapped() {
				r.log.V(1).Printf("[INFO] Bootstrapped by pulling the routing table from the coordinator")
				return
			}
		}
	}
}

func (r *RoutingTable) pushPeriodically() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.pushPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.updateRoutingWithReason(rebalanceReasonPeriodic, "")
		}
	}
}

func (r *RoutingTable) Join() error {
	err := r.discovery.Start()
	if err != nil {
		return err
	}

	err = r.attemptToJoin()
	if errors.Is(err, ErrClusterJoin) {
		r.log.V(1).Printf("[INFO] Forming a new Olric cluster")
		err = nil
	}
	if err != nil {
		return err
	}

	this, err := r.discovery.FindMemberByName(r.config.MemberlistConfig.Name)
	if err != nil {
		r.log.V(2).Printf("[ERROR] Failed to get this node in cluster: %v", err)
		shutdownError := r.discovery.Shutdown()
		if shutdownError != nil {
			return shutdownError
		}
		return err
	}
	r.this = this
	close(r.joined)
	return nil
}

func (r *RoutingTable) Start() error {
	select {
	case <-r.joined:
		// It's time to start the routing table service. Otherwise, this method will return an error.
	default:
		// Not yet, or the join process has failed
		return ErrNotJoinedYet
	}

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	r.setNumMembers()

	r.wg.Add(1)
	go r.listenClusterEvents(r.discovery.ClusterEvents)

	// The rejoin loop has to run before the quorum gate below, not after it.
	// A node that bootstrapped alone, because its only configured peer or the
	// service discovery result was unresolvable at Join() time, has nobody to
	// hear from: passive gossip never surfaces a peer and the gate would block
	// for its full one-hour ceiling even after that peer became reachable
	// again. The loop re-resolves and dials peers itself, and is a no-op once
	// quorum is satisfied, so one mechanism covers both the cold start and the
	// minority partition it was originally written for. Running it on its own
	// goroutine also keeps discovery.Join, which blocks while it dials, off
	// the gate, so the gate stays responsive to context cancellation.
	if r.config.MemberCountQuorum > config.MinimumMemberCountQuorum {
		r.wg.Add(1)
		go r.rejoinLoop()
	}

	// 1 Hour
	ctx, cancel := context.WithTimeout(r.ctx, time.Hour)
	defer cancel()
	err := r.tryWithInterval(ctx, time.Second, func() error {
		// Check member count quorum now. If there is no enough peers to work, wait forever.
		err := r.CheckMemberCountQuorum()
		if err != nil {
			r.log.V(2).Printf("[ERROR] Inoperable node: %v", err)
		}
		return err
	})
	if err != nil {
		return err
	}

	r.Members().Lock()
	r.Members().Add(r.this)
	r.Members().Unlock()

	r.consistent.Add(r.this)

	if r.discovery.IsCoordinator() {
		err = r.bootstrapCoordinator()
		if err != nil {
			return err
		}
	}

	r.wg.Add(1)
	go r.pullRoutingTableUntilBootstrapped()

	r.wg.Add(1)
	go r.pushPeriodically()

	if r.config.MemberlistInterface != "" {
		r.log.V(2).Printf("[INFO] Memberlist uses interface: %s", r.config.MemberlistInterface)
	}
	r.log.V(2).Printf("[INFO] Memberlist bindAddr: %s, bindPort: %d", r.config.MemberlistConfig.BindAddr, r.config.MemberlistConfig.BindPort)
	r.log.V(2).Printf("[INFO] Cluster coordinator: %s", r.discovery.GetCoordinator())
	r.checkpoint.Pass()
	return nil
}

func (r *RoutingTable) Shutdown(ctx context.Context) error {
	select {
	case <-r.ctx.Done():
		// already closed
		return nil
	default:
	}

	if err := r.discovery.Shutdown(); err != nil {
		return err
	}

	r.cancel()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
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

var _ service.Service = (*RoutingTable)(nil)
