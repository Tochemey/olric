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

	"github.com/stretchr/testify/require"
)

func TestNewMemberlistConfig_AllEnvironments(t *testing.T) {
	tests := []string{
		MemberlistEnvLocal,
		MemberlistEnvLAN,
		MemberlistEnvWAN,
		"LOCAL", // case-insensitivity
		"LAN",
		"WAN",
	}
	for _, env := range tests {
		t.Run(env, func(t *testing.T) {
			m, err := NewMemberlistConfig(env)
			require.NoError(t, err)
			require.NotNil(t, m)
		})
	}
}

func TestValidateMemberlistConfig(t *testing.T) {
	t.Run("valid advertise addr", func(t *testing.T) {
		c := validSanitizedConfig(t)
		c.MemberlistConfig.AdvertiseAddr = "10.0.0.1"
		require.NoError(t, c.validateMemberlistConfig())
	})
	t.Run("invalid advertise addr", func(t *testing.T) {
		c := validSanitizedConfig(t)
		c.MemberlistConfig.AdvertiseAddr = "nope"
		require.Error(t, c.validateMemberlistConfig())
	})
	t.Run("empty bind addr", func(t *testing.T) {
		c := validSanitizedConfig(t)
		c.MemberlistConfig.BindAddr = ""
		require.Error(t, c.validateMemberlistConfig())
	})
}
