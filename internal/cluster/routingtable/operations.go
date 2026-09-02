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

func (r *RoutingTable) verifyRoutingTable(id uint64, table map[uint64]*route) error {
	// Check the coordinator
	coordinator, err := r.discovery.FindMemberByID(id)
	if err != nil {
		return err
	}

	myCoordinator := r.discovery.GetCoordinator()
	if !coordinator.CompareByID(myCoordinator) {
		return fmt.Errorf("unrecognized cluster coordinator: %s: %s", coordinator, myCoordinator)
	}

	// Compare partition counts to catch a possible inconsistencies in configuration
	if r.config.PartitionCount != uint64(len(table)) {
		return fmt.Errorf("invalid partition count: %d", len(table))
	}
	return nil
}

// applyRoutingTablePayload installs a routing table received from the
// coordinator (either pushed or pulled), marks this node as bootstrapped and
// triggers the balancer callbacks. It returns the left-over data report.
// When onlyBootstrap is true the payload is dropped if the node is already
// bootstrapped: the pull path is a bootstrap guarantee and must never regress
// a fresher pushed table.
func (r *RoutingTable) applyRoutingTablePayload(payload []byte, coordinatorID uint64, onlyBootstrap bool) ([]byte, error) {
	r.updateRoutingMtx.Lock()
	defer r.updateRoutingMtx.Unlock()

	if onlyBootstrap && r.IsBootstrapped() {
		return nil, nil
	}

	table := make(map[uint64]*route)
	err := msgpack.Unmarshal(payload, &table)
	if err != nil {
		return nil, err
	}

	if err = r.verifyRoutingTable(coordinatorID, table); err != nil {
		return nil, err
	}

	// owners(atomic.value) is guarded by routingUpdateMtx against parallel writers.
	// Calculate routing signature. This is useful to control balancing tasks.
	r.setSignature(xxhash.Sum64(payload))
	for partID, data := range table {
		// Set partition(primary copies) owners
		part := r.primary.PartitionByID(partID)
		part.SetOwners(data.Owners)

		// Set backup owners
		bpart := r.backup.PartitionByID(partID)
		bpart.SetOwners(data.Backups)
	}

	// Used by the LRU implementation.
	r.setOwnedPartitionCount()

	// Bootstrapped by the coordinator.
	r.markBootstrapped()

	// Reconcile sync state for partitions we need to receive data for.
	// Partitions no live owner holds data for are left out: nothing can
	// arrive for them, so they must not delay the rebalance ACK. Partitions
	// already pending keep their original escape deadline, so routing
	// updates arriving faster than the escape delay cannot starve the ACK.
	if r.syncState != nil {
		r.syncState.Reconcile(r.partitionsAwaitingData(), r.config.InitialSyncEmptyPartitionTimeout)
	}

	// Collect report
	value, err := r.prepareLeftOverDataReport()
	if err != nil {
		return nil, err
	}

	// Call balancer to distribute load evenly
	r.spawn(r.runCallbacks)
	return value, nil
}

func (r *RoutingTable) updateRoutingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	// The command handlers of the routing table service should wait for the cluster join event.
	<-r.joined

	updateRoutingCmd, err := protocol.ParseUpdateRoutingCommand(cmd)
	if err != nil {
		r.log.V(1).Printf("[ERROR] Failed to parse routing table push from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteError(conn, err)
		return
	}

	value, err := r.applyRoutingTablePayload(updateRoutingCmd.Payload, updateRoutingCmd.CoordinatorID, false)
	if err != nil {
		r.log.V(1).Printf("[ERROR] Failed to apply pushed routing table from %s: %v", conn.RemoteAddr(), err)
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
