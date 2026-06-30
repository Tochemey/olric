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

package server

import (
	"context"
	"math/rand"
	"net"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/internal/protocol"
)

func TestMux_PubSub_Command(t *testing.T) {
	s := newServer(t)

	data := make([]byte, 8)
	_, err := rand.Read(data)
	require.NoError(t, err)

	s.ServeMux().HandleFunc(protocol.PubSub.PubSubNumpat, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteInt(10)
	})

	<-s.StartedCtx.Done()

	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})

	ctx := context.Background()
	var args []interface{}
	args = append(args, "pubsub")
	args = append(args, "numpat")
	cmd := redis.NewIntCmd(ctx, args...)
	err = rdb.Process(ctx, cmd)
	require.NoError(t, err)

	num, err := cmd.Result()
	require.NoError(t, err)
	require.Equal(t, int64(10), num)
}

// noopHandler is a redcon.Handler that does nothing. It is used to exercise
// the registration logic of ServeMux without needing a network connection.
type noopHandler struct{}

func (noopHandler) ServeRESP(conn redcon.Conn, cmd redcon.Command) {}

func TestServeMux_HandleFunc(t *testing.T) {
	t.Run("registers handler", func(t *testing.T) {
		m := NewServeMux()
		m.HandleFunc("ping", noopHandler{})
		_, ok := m.handlers["ping"]
		require.True(t, ok)
	})

	t.Run("nil handler panics", func(t *testing.T) {
		m := NewServeMux()
		require.PanicsWithValue(t, "olric: nil handler", func() {
			m.HandleFunc("ping", nil)
		})
	})
}

func TestServeMux_Handle(t *testing.T) {
	t.Run("empty command panics", func(t *testing.T) {
		m := NewServeMux()
		require.PanicsWithValue(t, "olric: invalid command", func() {
			m.Handle("", noopHandler{})
		})
	})

	t.Run("nil handler panics", func(t *testing.T) {
		m := NewServeMux()
		require.PanicsWithValue(t, "olric: nil handler", func() {
			m.Handle("ping", nil)
		})
	})

	t.Run("duplicate registration panics", func(t *testing.T) {
		m := NewServeMux()
		m.Handle("ping", noopHandler{})
		require.PanicsWithValue(t, "olric: multiple registrations for ping", func() {
			m.Handle("ping", noopHandler{})
		})
	})

	t.Run("registers handler", func(t *testing.T) {
		m := NewServeMux()
		m.Handle("get", noopHandler{})
		_, ok := m.handlers["get"]
		require.True(t, ok)
	})
}
