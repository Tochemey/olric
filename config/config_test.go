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
	"net"
	"strconv"
	"testing"

	"github.com/kapetan-io/tackle/autotls"
	"github.com/stretchr/testify/require"
)

// validSanitizedConfig returns a config that has been sanitized and validates
// successfully. Tests mutate the returned value to exercise individual error
// branches.
func validSanitizedConfig(t *testing.T) *Config {
	t.Helper()
	c := &Config{}
	require.NoError(t, c.Sanitize())
	require.NoError(t, c.Validate())
	return c
}

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

func TestRejoinInterval_Default(t *testing.T) {
	c := &Config{}
	require.NoError(t, c.Sanitize())
	require.Equal(t, DefaultRejoinInterval, c.RejoinInterval)
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

func TestConfig_Validate_Errors(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(c *Config)
		errContains string
	}{
		{
			name:        "replica count below minimum",
			mutate:      func(c *Config) { c.ReplicaCount = 0 },
			errContains: "ReplicaCount smaller than MinimumReplicaCount",
		},
		{
			name:        "read quorum non positive",
			mutate:      func(c *Config) { c.ReadQuorum = 0 },
			errContains: "ReadQuorum less than or equal to zero",
		},
		{
			name:        "read quorum greater than replica count",
			mutate:      func(c *Config) { c.ReadQuorum = 2 },
			errContains: "ReadQuorum greater than ReplicaCount",
		},
		{
			name:        "write quorum non positive",
			mutate:      func(c *Config) { c.WriteQuorum = 0 },
			errContains: "WriteQuorum less than or equal to zero",
		},
		{
			name:        "write quorum greater than replica count",
			mutate:      func(c *Config) { c.WriteQuorum = 2 },
			errContains: "WriteQuorum greater than ReplicaCount",
		},
		{
			name:        "invalid memberlist advertise addr",
			mutate:      func(c *Config) { c.MemberlistConfig.AdvertiseAddr = "not-an-ip" },
			errContains: "AdvertiseAddr has to be a valid",
		},
		{
			name:        "empty memberlist bind addr",
			mutate:      func(c *Config) { c.MemberlistConfig.BindAddr = "" },
			errContains: "BindAddr cannot be an empty string",
		},
		{
			name:        "member count quorum below minimum",
			mutate:      func(c *Config) { c.MemberCountQuorum = 0 },
			errContains: "MemberCountQuorum smaller than MinimumMemberCountQuorum",
		},
		{
			name:        "empty bind addr",
			mutate:      func(c *Config) { c.BindAddr = "" },
			errContains: "bindAddr cannot be empty",
		},
		{
			name:        "zero bind port",
			mutate:      func(c *Config) { c.BindPort = 0 },
			errContains: "bindPort cannot be empty or zero",
		},
		{
			name: "peer with itself",
			mutate: func(c *Config) {
				this := net.JoinHostPort(c.MemberlistConfig.BindAddr,
					strconv.Itoa(c.MemberlistConfig.BindPort))
				c.Peers = []string{this}
			},
			errContains: "cannot be peer with itself",
		},
		{
			name:        "invalid log level",
			mutate:      func(c *Config) { c.LogLevel = "BOGUS" },
			errContains: "invalid LogLevel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validSanitizedConfig(t)
			tt.mutate(c)
			err := c.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errContains)
		})
	}
}

func TestConfig_Validate_AllLogLevels(t *testing.T) {
	for _, lvl := range []string{LogLevelDebug, LogLevelWarn, LogLevelInfo, LogLevelError} {
		c := validSanitizedConfig(t)
		c.LogLevel = lvl
		require.NoError(t, c.Validate())
	}
}

func TestNew_AllEnvironments(t *testing.T) {
	for _, env := range []string{MemberlistEnvLocal, MemberlistEnvLAN, MemberlistEnvWAN} {
		c := New(env)
		require.NotNil(t, c)
		require.NotNil(t, c.MemberlistConfig)
		require.Equal(t, DefaultDiscoveryPort, c.MemberlistConfig.BindPort)
		require.NoError(t, c.Validate())
	}
}
