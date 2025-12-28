/*
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

package routingtable

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/events"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
	"github.com/tochemey/olric/internal/testutil"
)

func TestRoutingTable_rebalanceCompleteEventOnAck(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c := testutil.NewConfig()
	c.EnableClusterEventsChannel = true
	rt, err := cluster.addNode(c)
	require.NoError(t, err)

	peerCfg := testutil.NewConfig()
	peerCfg.MemberlistConfig.Name = "127.0.0.1:9999"
	peer := discovery.NewMember(peerCfg)

	rt.Members().Lock()
	rt.Members().Add(peer)
	rt.Members().Unlock()

	done := make(chan events.RebalanceCompleteEvent, 1)
	rt.server.ServeMux().HandleFunc(protocol.PubSub.Publish, func(conn redcon.Conn, cmd redcon.Command) {
		publishCmd, err := protocol.ParsePublishCommand(cmd)
		require.NoError(t, err)
		require.Equal(t, events.ClusterEventsChannel, publishCmd.Channel)

		var envelope struct {
			Kind string `json:"kind"`
		}
		err = json.Unmarshal([]byte(publishCmd.Message), &envelope)
		require.NoError(t, err)
		if envelope.Kind == events.KindRebalanceCompleteEvent {
			var ev events.RebalanceCompleteEvent
			err = json.Unmarshal([]byte(publishCmd.Message), &ev)
			require.NoError(t, err)
			done <- ev
		}

		conn.WriteInt(1)
	})

	rt.startRebalanceEpoch(42, rebalanceReasonNodeLeft, peer.String())
	rt.handleRebalanceAck(42, rt.this.ID)

	select {
	case <-done:
		t.Fatalf("rebalance completion should wait for all acks")
	case <-time.After(50 * time.Millisecond):
	}

	rt.handleRebalanceAck(42, peer.ID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case ev := <-done:
		require.Equal(t, events.KindRebalanceCompleteEvent, ev.Kind)
		require.Equal(t, rt.this.String(), ev.Source)
		require.Equal(t, uint64(42), ev.Epoch)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for rebalance completion event")
	}
}

func TestRoutingTable_bootstrapStartsRebalanceEpoch(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	rt, err := cluster.addNode(testutil.NewConfig())
	require.NoError(t, err)

	signature := rt.Signature()
	if signature == 0 {
		t.Skip("unexpected zero signature")
	}

	rt.rebalanceMtx.Lock()
	epoch := rt.rebalanceState.epoch
	pendingLen := len(rt.rebalanceState.pending)
	rt.rebalanceMtx.Unlock()

	require.Equal(t, signature, epoch)
	require.NotZero(t, pendingLen)
	require.True(t, rt.handleRebalanceAck(signature, rt.this.ID))

	rt.rebalanceMtx.Lock()
	completed := rt.rebalanceState.completed
	rt.rebalanceMtx.Unlock()
	require.True(t, completed)
}
