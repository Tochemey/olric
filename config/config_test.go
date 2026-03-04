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

package config

import (
	"testing"

	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/require"
)

func TestConfig_Initialize(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		// AutoGenerate TLS certs
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		tlsInfo := &TLS{
			Client: conf.ClientTLS,
			Server: conf.ServerTLS,
		}

		c := &Config{
			TLS: tlsInfo,
		}
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
	})
	t.Run("With no TLS", func(t *testing.T) {
		c := &Config{}
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
	})
}

func TestNewMemberlistConfig_UnknownEnv(t *testing.T) {
	_, err := NewMemberlistConfig("invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown env")
}

func TestEnableProactiveSyncOnJoin_Default(t *testing.T) {
	// Proactive sync is opt-in; the default must be false so it doesn't
	// silently alter memberlist timing for users who haven't requested it.
	c := New("lan")
	require.False(t, c.EnableProactiveSyncOnJoin)
}

func TestEnableProactiveSyncOnJoin_DoesNotAlterMemberlistTiming(t *testing.T) {
	// Enabling proactive sync must only control whether primary owners push
	// data to backup owners on node-join. It must not touch memberlist probe
	// or gossip intervals — those are the user's responsibility.
	disabled := New("lan")
	require.NoError(t, disabled.Sanitize())

	enabled := New("lan")
	enabled.EnableProactiveSyncOnJoin = true
	require.NoError(t, enabled.Sanitize())

	require.Equal(t, disabled.MemberlistConfig.ProbeInterval, enabled.MemberlistConfig.ProbeInterval,
		"EnableProactiveSyncOnJoin must not change ProbeInterval")
	require.Equal(t, disabled.MemberlistConfig.ProbeTimeout, enabled.MemberlistConfig.ProbeTimeout,
		"EnableProactiveSyncOnJoin must not change ProbeTimeout")
	require.Equal(t, disabled.MemberlistConfig.GossipInterval, enabled.MemberlistConfig.GossipInterval,
		"EnableProactiveSyncOnJoin must not change GossipInterval")
	require.Equal(t, disabled.MemberlistConfig.SuspicionMult, enabled.MemberlistConfig.SuspicionMult,
		"EnableProactiveSyncOnJoin must not change SuspicionMult")
}
