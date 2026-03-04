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
	"time"

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

func TestProactiveSyncOnJoin_ApplyToMemberlist(t *testing.T) {
	c := New("lan")
	c.EnableProactiveSyncOnJoin = true
	require.NoError(t, c.Sanitize())

	require.Equal(t, 200*time.Millisecond, c.MemberlistConfig.ProbeInterval)
	require.Equal(t, 100*time.Millisecond, c.MemberlistConfig.ProbeTimeout)
	require.Equal(t, 3, c.MemberlistConfig.SuspicionMult)
	require.Equal(t, 100*time.Millisecond, c.MemberlistConfig.GossipInterval)
	require.Equal(t, 30*time.Second, c.MemberlistConfig.GossipToTheDeadTime)
}

func TestProactiveSyncOnJoin_NotAppliedWhenDisabled(t *testing.T) {
	c := New("lan")
	require.False(t, c.EnableProactiveSyncOnJoin)

	cEnabled := New("lan")
	cEnabled.EnableProactiveSyncOnJoin = true
	require.NoError(t, cEnabled.Sanitize())

	require.Equal(t, 200*time.Millisecond, cEnabled.MemberlistConfig.ProbeInterval)
	require.NotEqual(t, c.MemberlistConfig.ProbeInterval, cEnabled.MemberlistConfig.ProbeInterval,
		"disabled and enabled should produce different ProbeInterval")
}

func TestProactiveSyncOnJoin_CustomValues(t *testing.T) {
	c := New("lan")
	c.EnableProactiveSyncOnJoin = true
	c.ProactiveSyncOnJoin = &ProactiveSyncOnJoinConfig{
		ProbeInterval: 500 * time.Millisecond,
		ProbeTimeout:  250 * time.Millisecond,
	}
	require.NoError(t, c.Sanitize())

	require.Equal(t, 500*time.Millisecond, c.MemberlistConfig.ProbeInterval)
	require.Equal(t, 250*time.Millisecond, c.MemberlistConfig.ProbeTimeout)
	require.Equal(t, 3, c.MemberlistConfig.SuspicionMult) // default
}
