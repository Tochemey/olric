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

package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterEvents_NodeJoinEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := NodeJoinEvent{
		Kind:       KindNodeJoinEvent,
		Source:     "127.0.0.1:3423",
		NodeJoin:   "127.0.0.1:3576",
		NodeMeta:   "custom-meta",
		Generation: 7,
		Timestamp:  timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"node-join-event","node_join":"127.0.0.1:3576","node_meta":"custom-meta","generation":7}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_NodeLeftEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := NodeLeftEvent{
		Kind:       KindNodeLeftEvent,
		Source:     "127.0.0.1:3423",
		NodeLeft:   "127.0.0.1:3576",
		NodeMeta:   "custom-meta",
		Generation: 7,
		Timestamp:  timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"node-left-event","node_left":"127.0.0.1:3576","node_meta":"custom-meta","generation":7}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_FragmentMigrationEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := FragmentMigrationEvent{
		Kind:          KindFragmentMigrationEvent,
		Source:        "127.0.0.1:3423",
		Target:        "127.0.0.1:3576",
		Identifier:    "mydmap",
		PartitionID:   123,
		DataStructure: "dmap",
		Length:        1234,
		Timestamp:     timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"fragment-migration-event","target":"127.0.0.1:3576","data_structure":"dmap","partition_id":123,"identifier":"mydmap","is_backup":false,"length":1234}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_FragmentReceivedEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := FragmentReceivedEvent{
		Kind:          KindFragmentReceivedEvent,
		Source:        "127.0.0.1:3423",
		Identifier:    "mydmap",
		PartitionID:   123,
		DataStructure: "dmap",
		Length:        1234,
		Timestamp:     timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"fragment-received-event","data_structure":"dmap","partition_id":123,"identifier":"mydmap","is_backup":false,"length":1234}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_RebalanceStartEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := RebalanceStartEvent{
		Kind:       KindRebalanceStartEvent,
		Source:     "127.0.0.1:3423",
		Epoch:      42,
		Generation: 7,
		Reason:     "node-left",
		Node:       "127.0.0.1:3576",
		Timestamp:  timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"rebalance-start-event","epoch":42,"generation":7,"reason":"node-left","node":"127.0.0.1:3576"}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_RebalanceCompleteEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := RebalanceCompleteEvent{
		Kind:       KindRebalanceCompleteEvent,
		Source:     "127.0.0.1:3423",
		Epoch:      42,
		Generation: 7,
		Members:    []string{"127.0.0.1:3423", "127.0.0.1:3576"},
		Timestamp:  timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"rebalance-complete-event","epoch":42,"generation":7,"members":["127.0.0.1:3423","127.0.0.1:3576"]}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_InitialSyncCompleteEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := InitialSyncCompleteEvent{
		Kind:      KindInitialSyncCompleteEvent,
		Source:    "127.0.0.1:3423",
		Timestamp: timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"initial-sync-complete-event"}`
	require.Equal(t, expected, result)
}

func TestClusterEvents_MembershipChangeEvent(t *testing.T) {
	var timestamp int64 = 585199808000 // Author's birthdate!
	n := MembershipChangeEvent{
		Kind:       KindMembershipChangeEvent,
		Source:     "127.0.0.1:3423",
		Change:     MembershipChangeLeft,
		Node:       "127.0.0.1:3576",
		NodeMeta:   "custom-meta",
		Members:    []string{"127.0.0.1:3423", "127.0.0.1:4000"},
		Generation: 7,
		Timestamp:  timestamp,
	}
	result, err := n.Encode()
	require.NoError(t, err)
	expected := `{"timestamp":585199808000,"source":"127.0.0.1:3423","kind":"membership-change-event","change":"left","node":"127.0.0.1:3576","node_meta":"custom-meta","members":["127.0.0.1:3423","127.0.0.1:4000"],"generation":7}`
	require.Equal(t, expected, result)
}
