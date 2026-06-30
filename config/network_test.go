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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// firstInterfaceWithAddrs returns the name of a network interface that has at
// least one address. It is used to exercise the interface-based code paths in
// getBindIP without depending on a particular interface name. The loopback
// interface is always present so this should never fail on a normal host.
func firstInterfaceWithAddrs(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	require.NoError(t, err)
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		if len(addrs) > 0 {
			return iface.Name
		}
	}
	t.Skip("no network interface with addresses found")
	return ""
}

func TestConfig_SetupNetworkConfig(t *testing.T) {
	c := &Config{}
	require.NoError(t, c.Sanitize())
	require.NoError(t, c.Validate())

	require.NoError(t, c.SetupNetworkConfig())
}

func TestConfig_SetupNetworkConfig_Memberlist_AdvertiseAddr(t *testing.T) {
	c := &Config{}
	require.NoError(t, c.Sanitize())
	require.NoError(t, c.Validate())
	c.MemberlistConfig.AdvertiseAddr = "localhost"
	require.NoError(t, c.SetupNetworkConfig())
	require.NotEqual(t, "localhost", c.MemberlistConfig.AdvertiseAddr)
}

func TestGetBindIPFromNetworkInterface(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Run("returns first usable IPAddr", func(t *testing.T) {
			addrs := []net.Addr{
				&net.IPNet{IP: net.ParseIP("192.168.1.10")}, // not *net.IPAddr, skipped
				&net.IPAddr{IP: net.ParseIP("169.254.1.1")}, // link-local, skipped
				&net.IPAddr{IP: net.ParseIP("10.0.0.5")},
			}
			ip, err := getBindIPFromNetworkInterface(addrs)
			require.NoError(t, err)
			require.Equal(t, "10.0.0.5", ip)
		})
		return
	}

	t.Run("returns first usable IPNet skipping others", func(t *testing.T) {
		addrs := []net.Addr{
			&net.IPAddr{IP: net.ParseIP("10.0.0.5")},                                // not *net.IPNet, skipped
			&net.IPNet{IP: net.ParseIP("169.254.1.1"), Mask: net.CIDRMask(16, 32)},  // link-local, skipped
			&net.IPNet{IP: net.ParseIP("192.168.1.10"), Mask: net.CIDRMask(24, 32)}, // usable
		}
		ip, err := getBindIPFromNetworkInterface(addrs)
		require.NoError(t, err)
		require.Equal(t, "192.168.1.10", ip)
	})

	t.Run("error when only link-local addresses", func(t *testing.T) {
		addrs := []net.Addr{
			&net.IPNet{IP: net.ParseIP("169.254.1.1"), Mask: net.CIDRMask(16, 32)},
		}
		_, err := getBindIPFromNetworkInterface(addrs)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to find usable address")
	})

	t.Run("error on empty slice", func(t *testing.T) {
		_, err := getBindIPFromNetworkInterface(nil)
		require.Error(t, err)
	})
}

func TestAddrParts(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		ip, port, err := addrParts("127.0.0.1:3320")
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", ip)
		require.Equal(t, 3320, port)
	})
	t.Run("invalid", func(t *testing.T) {
		_, _, err := addrParts("missing-port")
		require.Error(t, err)
	})
}

func TestGetBindIP(t *testing.T) {
	t.Run("invalid bind addr", func(t *testing.T) {
		_, err := getBindIP("", "missing-port")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid BindAddr")
	})

	t.Run("no interface 0.0.0.0 picks private or public ip", func(t *testing.T) {
		ip, err := getBindIP("nonexistent-iface-xyz", "0.0.0.0:3320")
		require.NoError(t, err)
		require.NotEmpty(t, ip)
		require.NotNil(t, net.ParseIP(ip))
	})

	t.Run("no interface specific ip is returned as-is", func(t *testing.T) {
		ip, err := getBindIP("nonexistent-iface-xyz", "192.168.5.5:3320")
		require.NoError(t, err)
		require.Equal(t, "192.168.5.5", ip)
	})

	t.Run("interface with 0.0.0.0 scans addresses", func(t *testing.T) {
		ifname := firstInterfaceWithAddrs(t)
		ip, err := getBindIP(ifname, "0.0.0.0:3320")
		require.NoError(t, err)
		require.NotEmpty(t, ip)
	})

	t.Run("interface without matching specific ip errors", func(t *testing.T) {
		ifname := firstInterfaceWithAddrs(t)
		_, err := getBindIP(ifname, "203.0.113.200:3320")
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no")
	})
}

func TestConfig_SetupNetworkConfig_BindInterfaceError(t *testing.T) {
	c := validSanitizedConfig(t)
	c.Interface = firstInterfaceWithAddrs(t)
	c.BindAddr = "203.0.113.200" // unlikely to be present on any local interface
	err := c.SetupNetworkConfig()
	require.Error(t, err)
}

func TestConfig_SetupNetworkConfig_MemberlistInterfaceError(t *testing.T) {
	c := validSanitizedConfig(t)
	c.MemberlistInterface = firstInterfaceWithAddrs(t)
	c.MemberlistConfig.BindAddr = "203.0.113.200"
	err := c.SetupNetworkConfig()
	require.Error(t, err)
}
