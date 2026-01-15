/*
 * Copyright 2018-2024 Burak Sezer
 * Copyright 2025 Arsene Tochemey Gandote
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
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/tochemey/olric/internal/util"
)

const (
	ClusterEventsChannel       = "cluster.events"
	KindNodeJoinEvent          = "node-join-event"
	KindNodeLeftEvent          = "node-left-event"
	KindFragmentMigrationEvent = "fragment-migration-event"
	KindFragmentReceivedEvent  = "fragment-received-event"
	KindRebalanceStartEvent    = "rebalance-start-event"
	KindRebalanceCompleteEvent = "rebalance-complete-event"
)

type Event interface {
	Encode() (string, error)
}

// encodeEvents encodes given interface to its JSON representation and preserves the order in fields slice.
func encodeEvent(data interface{}, fields []string, valueExtractor func(r reflect.Value, field string) (interface{}, error)) (string, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("{")
	r := reflect.Indirect(reflect.ValueOf(data))
	for i, field := range fields {
		sf, ok := r.Type().FieldByName(field)
		if !ok {
			return "", fmt.Errorf("field not found: %s", field)
		}

		tag := strings.Trim(string(sf.Tag), "json:")
		tag, err := strconv.Unquote(tag)
		if err != nil {
			return "", err
		}

		value, err := valueExtractor(r, field)
		if err != nil {
			return "", err
		}

		if i != 0 {
			buf.WriteString(",")
		}

		// marshal key
		key, err := json.Marshal(tag)
		if err != nil {
			return "", err
		}
		buf.Write(key)

		buf.WriteString(":")
		// marshal value
		val, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		buf.Write(val)
	}
	buf.WriteString("}")
	return util.BytesToString(buf.Bytes()), nil
}

type NodeJoinEvent struct {
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	NodeJoin  string `json:"node_join"`
	NodeMeta  string `json:"node_meta"`
	Timestamp int64  `json:"timestamp"`
}

func (n *NodeJoinEvent) Encode() (string, error) {
	fields := []string{"Timestamp", "Source", "Kind", "NodeJoin", "NodeMeta"}
	return encodeEvent(n, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "Timestamp":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "NodeJoin", "NodeMeta":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

type NodeLeftEvent struct {
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	NodeLeft  string `json:"node_left"`
	NodeMeta  string `json:"node_meta"`
	Timestamp int64  `json:"timestamp"`
}

func (n *NodeLeftEvent) Encode() (string, error) {
	fields := []string{"Timestamp", "Source", "Kind", "NodeLeft", "NodeMeta"}
	return encodeEvent(n, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "Timestamp":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "NodeLeft", "NodeMeta":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

type FragmentMigrationEvent struct {
	Kind          string `json:"kind"`
	Source        string `json:"source"`
	Target        string `json:"target"`
	Identifier    string `json:"identifier"`
	PartitionID   uint64 `json:"partition_id"`
	DataStructure string `json:"data_structure"`
	Length        int    `json:"length"`
	IsBackup      bool   `json:"is_backup"`
	Timestamp     int64  `json:"timestamp"`
}

func (f *FragmentMigrationEvent) Encode() (string, error) {
	fields := []string{
		"Timestamp",
		"Source",
		"Kind",
		"Target",
		"DataStructure",
		"PartitionID",
		"Identifier",
		"IsBackup",
		"Length",
	}
	return encodeEvent(f, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "IsBackup":
			value = r.FieldByName(field).Bool()
		case "PartitionID":
			value = r.FieldByName(field).Uint()
		case "Timestamp", "Length":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "Target", "DataStructure", "Identifier":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

type FragmentReceivedEvent struct {
	Kind          string `json:"kind"`
	Source        string `json:"source"`
	Identifier    string `json:"identifier"`
	PartitionID   uint64 `json:"partition_id"`
	DataStructure string `json:"data_structure"`
	Length        int    `json:"length"`
	IsBackup      bool   `json:"is_backup"`
	Timestamp     int64  `json:"timestamp"`
}

func (f *FragmentReceivedEvent) Encode() (string, error) {
	fields := []string{
		"Timestamp",
		"Source",
		"Kind",
		"DataStructure",
		"PartitionID",
		"Identifier",
		"IsBackup",
		"Length",
	}
	return encodeEvent(f, fields, func(r reflect.Value, field string) (interface{}, error) {
		var value interface{}
		switch field {
		case "IsBackup":
			value = r.FieldByName(field).Bool()
		case "PartitionID":
			value = r.FieldByName(field).Uint()
		case "Timestamp", "Length":
			value = r.FieldByName(field).Int()
		case "Source", "Kind", "DataStructure", "Identifier":
			value = r.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

// RebalanceStartEvent marks the beginning of a rebalance epoch in the cluster.
// It is emitted by the cluster coordinator after publishing a new routing table
// and before data movement completes.
//
// Field usage:
//   - Kind: event type identifier, always KindRebalanceStartEvent.
//   - Source: coordinator address that emitted the event.
//   - Epoch: routing table signature for this rebalance cycle. Correlate this
//     with RebalanceCompleteEvent.Epoch to know when the cycle finishes.
//   - Reason: why the rebalance started (e.g., "node-join", "node-left",
//     "node-update", "periodic", "manual").
//   - Node: the node associated with the trigger, if any (for example the
//     joined or left node). May be empty for periodic/manual updates.
//   - Timestamp: coordinator wall-clock time in nanoseconds since epoch.
//
// Application guidance: treat this as the start of data convergence for the
// routing table epoch. Do not use node-left events as completion signals;
// instead, wait for a matching RebalanceCompleteEvent with the same Epoch.
type RebalanceStartEvent struct {
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	Epoch     uint64 `json:"epoch"`
	Reason    string `json:"reason"`
	Node      string `json:"node"`
	Timestamp int64  `json:"timestamp"`
}

func (r *RebalanceStartEvent) Encode() (string, error) {
	fields := []string{"Timestamp", "Source", "Kind", "Epoch", "Reason", "Node"}
	return encodeEvent(r, fields, func(rv reflect.Value, field string) (any, error) {
		var value any
		switch field {
		case "Epoch":
			value = rv.FieldByName(field).Uint()
		case "Timestamp":
			value = rv.FieldByName(field).Int()
		case "Source", "Kind", "Reason", "Node":
			value = rv.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}

// RebalanceCompleteEvent marks the completion of a rebalance epoch.
// It is emitted by the cluster coordinator after all live members have
// acknowledged that no further fragment moves are required for the epoch.
//
// Field usage:
//   - Kind: event type identifier, always KindRebalanceCompleteEvent.
//   - Source: coordinator address that emitted the event.
//   - Epoch: routing table signature for the completed rebalance cycle. Match
//     against RebalanceStartEvent.Epoch.
//   - Timestamp: coordinator wall-clock time in nanoseconds since epoch.
//
// Application guidance: treat this as the point when rebalancing is complete
// for the given epoch. If a newer rebalance-start arrives before completion,
// the previous epoch should be considered superseded.
type RebalanceCompleteEvent struct {
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	Epoch     uint64 `json:"epoch"`
	Timestamp int64  `json:"timestamp"`
}

func (r *RebalanceCompleteEvent) Encode() (string, error) {
	fields := []string{"Timestamp", "Source", "Kind", "Epoch"}
	return encodeEvent(r, fields, func(rv reflect.Value, field string) (any, error) {
		var value any
		switch field {
		case "Epoch":
			value = rv.FieldByName(field).Uint()
		case "Timestamp":
			value = rv.FieldByName(field).Int()
		case "Source", "Kind":
			value = rv.FieldByName(field).String()
		default:
			return nil, fmt.Errorf("invalid field: %s", field)
		}
		return value, nil
	})
}
