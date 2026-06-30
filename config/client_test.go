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

	"github.com/stretchr/testify/require"
)

func TestClient_RedisOptions(t *testing.T) {
	c := NewClient()
	opts := c.RedisOptions()
	require.NotNil(t, opts)
	require.Equal(t, "tcp", opts.Network)
	require.NotNil(t, opts.Dialer)
	require.Equal(t, c.MaxRetries, opts.MaxRetries)
	require.Equal(t, c.DialTimeout, opts.DialTimeout)
	require.Equal(t, c.PoolSize, opts.PoolSize)
}

func TestClient_Sanitize_DisablesAndNegatives(t *testing.T) {
	c := &Client{
		DisableRedisLogging: true,
		ReadTimeout:         -1,
		WriteTimeout:        -1,
		MaxRetries:          -1,
		MinRetryBackoff:     -1,
		MaxRetryBackoff:     -1,
	}
	require.NoError(t, c.Sanitize())
	require.Equal(t, time.Duration(0), c.ReadTimeout)
	require.Equal(t, time.Duration(0), c.WriteTimeout)
	require.Equal(t, 0, c.MaxRetries)
	require.Equal(t, time.Duration(0), c.MinRetryBackoff)
	require.Equal(t, time.Duration(0), c.MaxRetryBackoff)
	require.NoError(t, c.Validate())
}

func TestClient_Sanitize_Defaults(t *testing.T) {
	c := &Client{}
	require.NoError(t, c.Sanitize())
	require.Equal(t, DefaultDialTimeout, c.DialTimeout)
	require.Equal(t, DefaultReadTimeout, c.ReadTimeout)
	require.Equal(t, c.ReadTimeout, c.WriteTimeout)
	require.Equal(t, DefaultMaxRetries, c.MaxRetries)
	require.Equal(t, DefaultMinRetryBackoff, c.MinRetryBackoff)
	require.Equal(t, DefaultMaxRetryBackoff, c.MaxRetryBackoff)
	require.Equal(t, DefaultIdleTimeout, c.IdleTimeout)
	require.Equal(t, c.ReadTimeout+time.Second, c.PoolTimeout)
	require.NotNil(t, c.Dialer)
	require.Greater(t, c.PoolSize, 0)
}
