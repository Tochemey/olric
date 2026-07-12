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
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/kapetan-io/tackle/autotls"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/pkg/flog"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func newServerWithPreConditionFunc(t *testing.T, preCond func(conn redcon.Conn, cmd redcon.Command) bool) *Server {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, fl, nil)
	s.SetPreConditionFunc(preCond)

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s
}

func newTLSServerWithPreConditionFunc(t *testing.T, preCond func(conn redcon.Conn, cmd redcon.Command) bool) (*Server, *redis.Client) {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	conf := autotls.Config{AutoTLS: true}
	require.NoError(t, autotls.Setup(&conf))

	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
		TLS:             conf.ServerTLS,
	}
	s := New(c, fl, nil)
	s.SetPreConditionFunc(preCond)

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s, redis.NewClient(&redis.Options{
		Addr:      net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort)),
		TLSConfig: conf.ClientTLS,
	})
}

func newServer(t *testing.T) *Server {
	srv := newServerWithPreConditionFunc(t, nil)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})
	return srv
}

func TestServer_RESP(t *testing.T) {
	s := newServer(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})
	respEcho(t, s, rdb)
}

func TestServer_RESP_Stats(t *testing.T) {
	s := newServer(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})
	respEcho(t, s, rdb)

	require.NotEqual(t, int64(0), CommandsTotal.Read())
	require.NotEqual(t, int64(0), ConnectionsTotal.Read())
	require.NotEqual(t, int64(0), CurrentConnections.Read())
	require.NotEqual(t, int64(0), WrittenBytesTotal.Read())
	require.NotEqual(t, int64(0), ReadBytesTotal.Read())
}

func TestTSLServer_RESP(t *testing.T) {
	srv, rdb := newTLSServerWithPreConditionFunc(t, nil)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	respEcho(t, srv, rdb)
}

func newTestLogger() *flog.Logger {
	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	return fl
}

func TestServer_HandlerHandleFunc_NilHandler_Panics(t *testing.T) {
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        0,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, newTestLogger(), nil)

	require.PanicsWithValue(t, "server: nil handler", func() {
		s.ServeMux().HandleFunc("ping", nil)
	})
}

func TestServer_Shutdown_NeverStarted(t *testing.T) {
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        0,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, newTestLogger(), nil)

	// The server was never started, so server is nil and Shutdown must
	// simply return nil.
	require.NoError(t, s.Shutdown(context.Background()))
}

func TestServer_Shutdown_AlreadyClosed(t *testing.T) {
	bindPort, err := getFreePort()
	require.NoError(t, err)

	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, newTestLogger(), nil)

	go func() {
		_ = s.ListenAndServe()
	}()

	<-s.StartedCtx.Done()

	require.NoError(t, s.Shutdown(context.Background()))
	// The second call hits the already-closed fast path.
	require.NoError(t, s.Shutdown(context.Background()))
}

func TestServer_SetPreConditionFunc_AfterStart(t *testing.T) {
	s := newServer(t)
	<-s.StartedCtx.Done()

	// After the server has started, SetPreConditionFunc must hit the
	// already-started fast path and return without overwriting the
	// precondition. The server started with a nil precondition, so it must
	// remain nil.
	s.SetPreConditionFunc(func(conn redcon.Conn, cmd redcon.Command) bool {
		return false
	})
	require.Nil(t, s.wmux.precond)
}

func TestServer_ServeMux_ReturnsWrapper(t *testing.T) {
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        0,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, newTestLogger(), nil)
	require.NotNil(t, s.ServeMux())
	require.Same(t, s.wmux, s.ServeMux())
}

func TestServer_ServeRESP_UnknownCommand(t *testing.T) {
	s := newServer(t)
	<-s.StartedCtx.Done()

	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})

	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "thiscommanddoesnotexist")
	err := rdb.Process(ctx, cmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown command")
}

func TestServer_ServeRESP_PubSub_WrongArgs(t *testing.T) {
	s := newServer(t)
	<-s.StartedCtx.Done()

	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort)),
	})

	ctx := context.Background()
	// "pubsub" with no subcommand triggers the wrong-number-of-arguments branch.
	cmd := redis.NewStringCmd(ctx, "pubsub")
	err := rdb.Process(ctx, cmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}

func TestServer_ListenAndServe_BadAddr(t *testing.T) {
	c := &Config{
		BindAddr:        "256.256.256.256",
		BindPort:        0,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, newTestLogger(), nil)
	err := s.ListenAndServe()
	require.Error(t, err)
}
