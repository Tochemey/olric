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
	"github.com/tochemey/olric/events"
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
	// coordinator and sequence identify the push the table came from: the
	// coordinator's member id and its table sequence, see
	// RoutingTable.tableSequence. A push whose sequence differs advances the
	// generation even when the signature recurred, since it opens a new
	// epoch that this member has to ack. Both are zero for a pulled table
	// and for a push from a coordinator of the previous version.
	coordinator uint64
	sequence    uint64
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
	// tableSequence counts the distinct tables this coordinator computed, and
	// is sent with every push of the current one, so a member can tell a new
	// epoch under a recurring signature from a repeated push of the table it
	// holds. Guarded by the routing mutex.
	tableSequence uint64
	consistent    *consistent.Consistent
	this          discovery.Member
	members       *Members
	config        *config.Config
	log           *flog.Logger
	primary       *partitions.Partitions
	backup        *partitions.Partitions
	client        *server.Client
	server        *server.Server
	discovery     *discovery.Discovery
	callbacks     []func()
	callbackMtx   sync.Mutex
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
	// deliver events on its own. localEventPublisher delivers the events a
	// member emits for its own subscribers only, the node-join and node-left
	// observations; see SetLocalClusterEventPublisher for its fallback.
	eventPublisher      ClusterEventPublisher
	localEventPublisher ClusterEventPublisher
	eventPublisherMtx   sync.RWMutex
	pushPeriod          time.Duration
	// The command handlers of the routing table service should wait for the cluster join event.
	joined chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// shutdownMtx guards closed against the wg.Add/wg.Wait race: Shutdown sets
	// closed under it before waiting on wg, and spawn checks it under the same
	// lock before adding to wg.
	shutdownMtx sync.Mutex
	closed      bool

	rebalanceMtx   sync.Mutex
	rebalanceState rebalanceState
	// earlyAcks buffers rebalance acks that arrive before their epoch becomes
	// active (epoch -> member IDs). Guarded by rebalanceMtx.
	earlyAcks map[uint64]map[uint64]struct{}

	// committedPayload holds the last routing table payload ([]byte) this
	// coordinator committed, stored before its fan-out so a pull during the
	// fan-out never returns an older table than the push delivers. It backs
	// the pull path and is deliberately independent of the routing mutex: a
	// stalled push fan-out must not block table pulls.
	committedPayload atomic.Value
	// tableCoordinator is the member that pushed, or served the pull of, the
	// installed routing table. When that member leaves or restarts while this
	// member is not the coordinator, the table is pulled from the new
	// coordinator, see tableCoordinatorGone.
	tableCoordinator atomic.Pointer[discovery.Member]

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

// setVersion records the installation of a routing table with the given
// signature, pushed by coordinator as its table sequence, and advances the
// generation when the table changed. The table changed when its signature
// differs from the installed one, or when a push numbered its sequence
// differently from the installed push: the coordinator then computed a distinct
// table in between, which this member never saw, and started an epoch this
// member has to ack although the content it holds is the same. A sequence of
// zero, a pulled table or a coordinator of the previous version, decides by
// signature alone, and a pull that confirms the installed table keeps the
// sequence of the push that delivered it. Installs are serialized by
// updateRoutingMtx.
func (x *RoutingTable) setVersion(s, coordinator, sequence uint64) {
	var generation uint64
	changed := true
	if current := x.version.Load(); current != nil {
		generation = current.generation
		changed = current.signature != s
		if !changed && sequence != 0 && (current.coordinator != coordinator || current.sequence != sequence) {
			changed = true
		}

		if !changed && sequence == 0 {
			// A pull that confirms the installed table keeps the
			// identity of the push that delivered it, so the next push
			// of the same table is recognised as the same one.
			coordinator, sequence = current.coordinator, current.sequence
		}
	}

	if changed {
		generation++
	}

	x.version.Store(&tableVersion{
		signature:   s,
		generation:  generation,
		coordinator: coordinator,
		sequence:    sequence,
	})
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

// fillRoutingTable computes the routing table from the recorded owners, the
// live membership and the hash ring, in three passes: the recorded owners that
// are still members are collected for every partition, every such owner is
// then asked in one pipelined round trip which of its recorded copies still
// hold data, and the owner lists are finally pruned of empty copies and
// completed with the owners the ring designates. Membership is read once and
// each owner is dialed once, so the computation costs one round trip per
// member rather than one per owner per partition.
func (x *RoutingTable) fillRoutingTable() {
	if x.config.ReplicaCount > int(x.NumMembers()) {
		x.log.V(1).Printf("[WARN] Desired replica count is %d and "+
			"the cluster has %d members currently",
			x.config.ReplicaCount, x.NumMembers())
	}

	index := x.snapshotMembers()
	replicas := x.config.ReplicaCount > config.MinimumReplicaCount
	primaries := make([][]discovery.Member, x.config.PartitionCount)
	backups := make([][]discovery.Member, x.config.PartitionCount)
	queries := make(map[discovery.Member][]countQuery)

	for partID := range x.config.PartitionCount {
		owners := x.liveOwners(partID, x.primary.PartitionByID(partID).Owners(), index, partitions.PRIMARY)
		primaries[partID] = owners

		for _, owner := range owners {
			queries[owner] = append(queries[owner], countQuery{partID: partID})
		}

		if !replicas {
			continue
		}

		replicaOwners := x.liveOwners(partID, x.backup.PartitionByID(partID).Owners(), index, partitions.BACKUP)
		backups[partID] = replicaOwners

		for _, owner := range replicaOwners {
			queries[owner] = append(queries[owner], countQuery{partID: partID, replica: true})
		}
	}

	counts := x.probeOwners(queries)

	table := make(map[uint64]*route, x.config.PartitionCount)
	for partID := range x.config.PartitionCount {
		rt := &route{
			Owners: x.placePrimaryOwner(partID, primaries[partID], counts),
		}

		if replicas {
			rt.Backups = x.placeBackupOwners(partID, backups[partID], counts)
		}

		table[partID] = rt
	}

	x.table = table
}

func (r *RoutingTable) UpdateEagerly() {
	r.updateRoutingWithReason(rebalanceReasonManual, "")
}

// updateRoutingWithReason recomputes the routing table on the coordinator,
// commits it locally, pushes it to every member and starts the rebalance epoch
// for reason.
func (x *RoutingTable) updateRoutingWithReason(reason rebalanceReason, node string) {
	// This function is called by listenMemberlistEvents and updateRoutingPeriodically
	// So this lock prevents parallel execution.
	x.Lock()
	defer x.Unlock()

	// This function is only run by the cluster coordinator.
	if !x.discovery.IsCoordinator() {
		return
	}

	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	if err := x.CheckMemberCountQuorum(); err != nil {
		x.log.V(2).Printf("[ERROR] Impossible to calculate and update routing table: %v", err)
		return
	}

	// Snapshot the membership before computing the table, so that the list
	// reported with the epoch's completion names the members the table was
	// computed for: a member that joins while the table is being computed
	// is only reported by the next epoch, which surely reflects it.
	memberIDs, members := x.memberSnapshot()
	x.fillRoutingTable()
	if x.syncState != nil {
		scannedAt := time.Now()
		x.syncState.Reconcile(x.partitionsPendingReceive(), x.config.InitialSyncEmptyPartitionTimeout, scannedAt)
	}
	previousSignature := x.Signature()
	data, signature, err := x.buildRoutingTablePayload()
	if err != nil {
		x.log.V(2).Printf("[ERROR] Failed to marshal routing table: %v", err)
		return
	}

	if signature != previousSignature {
		x.tableSequence++
	}

	sequence := x.tableSequence

	// The payload is committed before the fan-out, so a member that pulls the
	// table while the fan-out runs never receives an older table than the
	// push in flight delivers. If the coordinator cannot install its own
	// copy the previous commit is restored, so that pulls never serve a
	// table no epoch tracks.
	previous, _ := x.committedPayload.Load().([]byte)
	x.committedPayload.Store(data)
	reports, unreachable, err := x.updateRoutingTableOnCluster(data, sequence)
	if err != nil {
		if previous != nil {
			x.committedPayload.Store(previous)
		}

		x.log.V(2).Printf("[ERROR] Failed to update routing table on cluster: %v", err)
		return
	}
	x.processLeftOverDataReports(reports)
	if signature != previousSignature {
		// Every live member the table was computed for gates the epoch; the
		// ones the fan-out could not reach receive it by retry or pull.
		x.startRebalanceEpoch(signature, reason, node, memberIDs, members)
		// Proactive sync pushes primary data to newly joined backup owners.
		// It must NOT run on node-leave events: when nodes die the fragment
		// write-lock held during the network transfer would block concurrent
		// reads for the entire push duration, causing read timeouts.
		if x.config.EnableProactiveSyncOnJoin && reason == rebalanceReasonNodeJoin {
			x.spawn(x.runCallbacks)
		}
	}

	if len(unreachable) > 0 {
		x.spawn(func() { x.retryRoutingTablePush(data, sequence, signature, unreachable) })
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
			// Captured here, not in the goroutine: the table may change
			// before the publish runs, and the event must carry the
			// generation held when the join was observed.
			generation := r.Generation()
			r.spawn(func() { r.publishNodeJoinEvent(&member, generation) })
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
			// Captured here for the same reason as on join.
			generation := r.Generation()
			r.spawn(func() { r.publishNodeLeftEvent(&member, generation) })
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

// listenClusterEvents consumes memberlist join and leave events: it updates the
// member list and the routing table, announces the change when this member is
// the coordinator, and pulls the table when the coordinator changed.
func (x *RoutingTable) listenClusterEvents(eventCh chan *discovery.ClusterEvent) {
	defer x.wg.Done()
	for {
		select {
		case <-x.ctx.Done():
			return
		case e := <-eventCh:
			notify := x.processClusterEvent(e)
			if notify != nil {
				// Fired here rather than inside processClusterEvent so the
				// callbacks run without the members lock, strictly in event
				// order: a ban from a NodeLeave can never be applied after
				// the unban from a following NodeJoin for the same address.
				notify()
				x.announceMembershipChange(e)
			}
			reason, node := rebalanceReasonFromEvent(e)
			x.updateRoutingWithReason(reason, node)

			if x.tableCoordinatorGone(e) {
				x.spawn(x.pullRoutingTableAfterCoordinatorChange)
			}
		}
	}
}

// announceMembershipChange publishes, when this member is the coordinator and
// cluster events are enabled, the membership-change-event for event: the
// authoritative announcement of a join, departure or update, carrying the
// member set after the change and the generation held when it was observed.
// It is published before the routing table is recomputed and regardless of
// whether that recomputation changes the table or passes the member-count
// quorum, so a subscriber learns of every change from one source, once.
func (x *RoutingTable) announceMembershipChange(event *discovery.ClusterEvent) {
	if !x.config.EnableClusterEventsChannel || !x.discovery.IsCoordinator() {
		return
	}

	var change string
	switch event.Event {
	case memberlist.NodeJoin:
		change = events.MembershipChangeJoin
	case memberlist.NodeLeave:
		change = events.MembershipChangeLeft
	case memberlist.NodeUpdate:
		change = events.MembershipChangeUpdate
	default:
		return
	}

	member, _ := discovery.NewMemberFromMetadata(event.NodeMeta)
	members := x.memberNames()
	generation := x.Generation()
	x.spawn(func() { x.publishMembershipChangeEvent(change, &member, members, generation) })
}

// tableCoordinatorGone reports whether event removed or restarted the member
// that pushed the installed routing table while this member is not the
// coordinator itself: the new coordinator's push may be rejected by a member
// whose membership view lags, so such a member pulls the table on its own.
func (x *RoutingTable) tableCoordinatorGone(event *discovery.ClusterEvent) bool {
	if event.Event != memberlist.NodeLeave && event.Event != memberlist.NodeUpdate {
		return false
	}

	coordinator := x.tableCoordinator.Load()
	if coordinator == nil || coordinator.Name != event.NodeName {
		return false
	}

	return !x.discovery.IsCoordinator()
}

// pullRoutingTableUntilBootstrapped keeps requesting the committed routing
// table from the coordinator until this node is bootstrapped. The coordinator
// push is the fast path; this pull is the guarantee: a joiner whose push was
// lost would otherwise never bootstrap, never run its balancer and never ack a
// rebalance epoch.
func (x *RoutingTable) pullRoutingTableUntilBootstrapped() {
	defer x.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-x.ctx.Done():
			return
		case <-ticker.C:
			if x.IsBootstrapped() {
				return
			}

			if err := x.fetchRoutingTableFromCoordinator(true); err != nil {
				x.log.V(3).Printf("[WARN] Failed to pull routing table from the coordinator: %v", err)
				continue
			}

			if x.IsBootstrapped() {
				x.log.V(1).Printf("[INFO] Bootstrapped by pulling the routing table from the coordinator")
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

	// Flip closed under shutdownMtx before waiting, so no spawn can add to wg
	// once Wait has started.
	r.shutdownMtx.Lock()
	r.closed = true
	r.shutdownMtx.Unlock()

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

// spawn runs fn on its own goroutine accounted in wg, unless the routing table
// is shutting down. closed is checked under shutdownMtx, the lock Shutdown sets
// it under before waiting on wg, so wg.Add never races wg.Wait. It reports
// whether fn was started; false means the work was dropped, which is safe
// because the node is leaving.
func (x *RoutingTable) spawn(fn func()) bool {
	x.shutdownMtx.Lock()
	defer x.shutdownMtx.Unlock()

	if x.closed {
		return false
	}

	x.wg.Go(fn)
	return true
}

var _ service.Service = (*RoutingTable)(nil)
