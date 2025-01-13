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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/pkg/testkit"
)

func TestConfig_Initialize(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		serverConfig, _ := testkit.GetTLSServerAndClientConfigs(t)
		c := &Config{
			TlsConfig: serverConfig,
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
