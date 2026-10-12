/*
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

package testutil

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetFreePort verifies that the returned port can be bound on loopback for
// both TCP and UDP, as memberlist does.
func TestGetFreePort(t *testing.T) {
	port, err := GetFreePort()
	require.NoError(t, err)

	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	require.NoError(t, err)
	require.NoError(t, u.Close())
}

// TestEnsureUDPPortFree verifies that a UDP port held by another socket is
// reported as taken, and as free once that socket is closed.
func TestEnsureUDPPortFree(t *testing.T) {
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	port := u.LocalAddr().(*net.UDPAddr).Port

	require.Error(t, ensureUDPPortFree(port))

	require.NoError(t, u.Close())
	require.NoError(t, ensureUDPPortFree(port))
}
