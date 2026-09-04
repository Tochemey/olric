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
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"

	"github.com/tochemey/olric/internal/cluster/partitions"
)

func (r *RoutingTable) lengthOfPartCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-r.joined

	lengthOfPartCmd, err := protocol.ParseLengthOfPartCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var part *partitions.Partition
	if lengthOfPartCmd.Replica {
		part = r.backup.PartitionByID(lengthOfPartCmd.PartID)
	} else {
		part = r.primary.PartitionByID(lengthOfPartCmd.PartID)
	}

	conn.WriteInt(part.Length())
}

// verifyRoutingTable checks that the member id that sent table is the
// coordinator this member knows and that table has the configured partition
// count. It returns that member, so the caller can record who the installed
// table came from.
func (x *RoutingTable) verifyRoutingTable(id uint64, table map[uint64]*route) (discovery.Member, error) {
	// Check the coordinator
	coordinator, err := x.discovery.FindMemberByID(id)
	if err != nil {
		return discovery.Member{}, err
	}

	myCoordinator := x.discovery.GetCoordinator()
	if !coordinator.CompareByID(myCoordinator) {
		return discovery.Member{}, fmt.Errorf("unrecognized cluster coordinator: %s: %s", coordinator, myCoordinator)
	}

	// Compare partition counts to catch a possible inconsistencies in configuration
	if x.config.PartitionCount != uint64(len(table)) {
		return discovery.Member{}, fmt.Errorf("invalid partition count: %d", len(table))
	}
	return coordinator, nil
}

// applyRoutingTablePayload installs a routing table received from the
// coordinator, pushed as sequence or pulled with a sequence of zero, marks this
// node as bootstrapped and triggers the balancer callbacks. It returns the
// left-over data report. When onlyBootstrap is true the payload is dropped if
// the node is already bootstrapped: the pull path is a bootstrap guarantee and
// must never regress a fresher pushed table.
//
// expected, when not nil, is the version installed when a pull was issued: the
// pulled table is applied only if no push installed another one meanwhile. A
// push whose sequence is older than the installed one, from the same
// coordinator, is not installed either; see the body.
func (x *RoutingTable) applyRoutingTablePayload(payload []byte, coordinatorID, sequence uint64, onlyBootstrap bool, expected *tableVersion) ([]byte, error) {
	x.updateRoutingMtx.Lock()
	defer x.updateRoutingMtx.Unlock()

	if onlyBootstrap && x.IsBootstrapped() {
		return nil, nil
	}

	if expected != nil && x.version.Load() != expected {
		// The pull was answered against a table that a push replaced while
		// the answer was in flight; the pushed table is the newer one.
		return nil, nil
	}

	table := make(map[uint64]*route)
	err := msgpack.Unmarshal(payload, &table)
	if err != nil {
		return nil, err
	}

	coordinator, err := x.verifyRoutingTable(coordinatorID, table)
	if err != nil {
		return nil, err
	}

	if current := x.version.Load(); sequence != 0 && current != nil && current.coordinator == coordinator.ID && sequence < current.sequence {
		// A retried push of a table older than the installed one, from the
		// same coordinator: the fan-out of the newer table landed first. The
		// answer is the current report, so the retry counts it as delivered
		// and the newer table stays.
		return x.prepareLeftOverDataReport()
	}

	// owners(atomic.value) is guarded by routingUpdateMtx against parallel writers.
	// Calculate routing signature. This is useful to control balancing tasks.
	x.setVersion(xxhash.Sum64(payload), coordinator.ID, sequence)
	x.tableCoordinator.Store(&coordinator)
	for partID, data := range table {
		// Set partition(primary copies) owners
		part := x.primary.PartitionByID(partID)
		part.SetOwners(data.Owners)

		// Set backup owners
		bpart := x.backup.PartitionByID(partID)
		bpart.SetOwners(data.Backups)
	}

	// Used by the LRU implementation.
	x.setOwnedPartitionCount()

	// Bootstrapped by the coordinator.
	x.markBootstrapped()

	// Reconcile sync state for partitions we need to receive data for.
	// Partitions no live owner holds data for are left out: nothing can
	// arrive for them, so they must not delay the rebalance ACK. Partitions
	// already pending keep their original escape deadline, so routing
	// updates arriving faster than the escape delay cannot starve the ACK.
	if x.syncState != nil {
		awaiting, scannedAt := x.partitionsAwaitingDataAt()
		x.syncState.Reconcile(awaiting, x.config.InitialSyncEmptyPartitionTimeout, scannedAt)
	}

	// Collect report
	value, err := x.prepareLeftOverDataReport()
	if err != nil {
		return nil, err
	}

	// Call balancer to distribute load evenly
	x.spawn(x.runCallbacks)
	return value, nil
}

// updateRoutingCommandHandler serves UPDATEROUTING: it checks that the sender is
// the coordinator, installs the pushed table and answers with this member's
// left-over data report.
func (x *RoutingTable) updateRoutingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-x.joined

	updateRoutingCmd, err := protocol.ParseUpdateRoutingCommand(cmd)
	if err != nil {
		x.log.V(1).Printf("[ERROR] Failed to parse routing table push from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteError(conn, err)
		return
	}

	value, err := x.applyRoutingTablePayload(updateRoutingCmd.Payload, updateRoutingCmd.CoordinatorID, updateRoutingCmd.Sequence, false, nil)
	if err != nil {
		x.log.V(1).Printf("[ERROR] Failed to apply pushed routing table from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteBulk(value)
}

// fetchRoutingCommandHandler serves the current committed routing table to a
// member that pulls it. The push in updateRoutingCommandHandler remains the
// fast path; this pull is the delivery guarantee for members the push never
// reached. It intentionally reads the committed payload from an atomic instead
// of taking the routing mutex: a stalled push fan-out must not block pulls.
func (r *RoutingTable) fetchRoutingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-r.joined

	fetchRoutingCmd, err := protocol.ParseFetchRoutingCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if !r.discovery.IsCoordinator() {
		protocol.WriteError(conn, protocol.ErrNotCoordinator)
		return
	}

	payload, ok := r.committedPayload.Load().([]byte)
	if !ok || len(payload) == 0 {
		protocol.WriteError(conn, fmt.Errorf("%w: routing table has not been committed yet", protocol.ErrInvalidArgument))
		return
	}

	r.log.V(1).Printf("[DEBUG] Routing table pulled by member: %d", fetchRoutingCmd.MemberID)
	conn.WriteBulk(payload)
}

func (r *RoutingTable) rebalanceAckCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-r.joined

	ackCmd, err := protocol.ParseRebalanceAckCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if !r.discovery.IsCoordinator() {
		protocol.WriteError(conn, protocol.ErrNotCoordinator)
		return
	}

	// A stale ack is a status, not an error: the sender re-runs its balance
	// cycle against its current table. Early and accepted acks are both OK.
	if r.handleRebalanceAck(ackCmd.Epoch, ackCmd.MemberID) == ackStale {
		conn.WriteString(protocol.StatusStaleRebalanceAck)
		return
	}
	conn.WriteString(protocol.StatusOK)
}
