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

package pubsub

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/internal/testutil"
)

func testPubSubServer(addr string, done chan bool) {
	var ps PubSub
	go func() {
		tch := time.NewTicker(time.Millisecond * 5)
		defer tch.Stop()
		channels := []string{"achan1", "bchan2", "cchan3", "dchan4"}
		for i := 0; ; i++ {
			select {
			case <-tch.C:
			case <-done:
				for {
					var empty bool
					ps.mu.Lock()
					if len(ps.conns) == 0 {
						if ps.chans.Len() != 0 {
							panic("chans not empty")
						}
						empty = true
					}
					ps.mu.Unlock()
					if empty {
						break
					}
					time.Sleep(time.Millisecond * 10)
				}
				done <- true
				return
			}
			channel := channels[i%len(channels)]
			message := fmt.Sprintf("message %d", i)
			ps.Publish(channel, message)
		}
	}()
	panic(redcon.ListenAndServe(addr, func(conn redcon.Conn, cmd redcon.Command) {
		switch strings.ToLower(string(cmd.Args[0])) {
		default:
			conn.WriteError("ERR unknown command '" +
				string(cmd.Args[0]) + "'")
		case "publish":
			if len(cmd.Args) != 3 {
				conn.WriteError("ERR wrong number of arguments for '" +
					string(cmd.Args[0]) + "' command")
				return
			}
			count := ps.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
			conn.WriteInt(count)
		case "subscribe", "psubscribe":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for '" +
					string(cmd.Args[0]) + "' command")
				return
			}
			command := strings.ToLower(string(cmd.Args[0]))
			for i := 1; i < len(cmd.Args); i++ {
				if command == "psubscribe" {
					ps.Psubscribe(conn, string(cmd.Args[i]))
				} else {
					ps.Subscribe(conn, string(cmd.Args[i]))
				}
			}
		}
	}, nil, nil))
}

func TestPubSub(t *testing.T) {
	addr := ":12346"
	done := make(chan bool)
	go testPubSubServer(addr, done)

	final := make(chan bool)
	go func() {
		select {
		case <-time.Tick(time.Second * 30):
			panic("timeout")
		case <-final:
			return
		}
	}()

	// create 10 connections
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			var conn net.Conn
			for i := 0; i < 5; i++ {
				var err error
				conn, err = net.Dial("tcp", addr)
				if err != nil {
					time.Sleep(time.Second / 10)
					continue
				}
			}
			if conn == nil {
				require.Fail(t, "could not connect to server")
			}
			defer conn.Close()

			regs := make(map[string]int)
			var maxp int
			var maxs int
			_, err := fmt.Fprintf(conn, "subscribe achan1\r\n")
			require.NoError(t, err)

			_, err = fmt.Fprintf(conn, "subscribe bchan2 cchan3\r\n")
			require.NoError(t, err)

			_, err = fmt.Fprintf(conn, "psubscribe a*1\r\n")
			require.NoError(t, err)

			_, err = fmt.Fprintf(conn, "psubscribe b*2 c*3\r\n")
			require.NoError(t, err)

			// collect 50 messages from each channel
			rd := bufio.NewReader(conn)
			var buf []byte
			for {
				line, err := rd.ReadBytes('\n')
				if err != nil {
					require.NoError(t, err)
				}
				buf = append(buf, line...)
				n, resp := redcon.ReadNextRESP(buf)
				if n == 0 {
					continue
				}
				buf = nil
				if resp.Type != redcon.Array {
					require.Fail(t, "expected array")
				}
				var vals []redcon.RESP
				resp.ForEach(func(item redcon.RESP) bool {
					vals = append(vals, item)
					return true
				})

				name := string(vals[0].Data)
				switch name {
				case "subscribe":
					require.Len(t, vals, 3)

					ch := string(vals[1].Data)
					regs[ch] = 0
					maxs, _ = strconv.Atoi(string(vals[2].Data))
				case "psubscribe":
					require.Len(t, vals, 3)

					ch := string(vals[1].Data)
					regs[ch] = 0
					maxp, _ = strconv.Atoi(string(vals[2].Data))
				case "message":
					require.Len(t, vals, 3)

					ch := string(vals[1].Data)
					regs[ch] = regs[ch] + 1
				case "pmessage":
					require.Len(t, vals, 4)

					ch := string(vals[1].Data)
					regs[ch] = regs[ch] + 1
				}
				if len(regs) == 6 && maxp == 3 && maxs == 3 {
					ready := true
					for _, count := range regs {
						if count < 50 {
							ready = false
							break
						}
					}
					if ready {
						// all messages have been received
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	// notify sender
	done <- true
	// wait for sender
	<-done
	// stop the timeout
	final <- true
}

// startPubSubServer starts a redcon server backed by a PubSub instance on an
// ephemeral port. Once a connection issues a (P)SUBSCRIBE the connection is
// detached and driven by PubSub.bgrunner, which is what we want to exercise.
func startPubSubServer(t *testing.T) string {
	t.Helper()

	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ps := &PubSub{}
	handler := func(conn redcon.Conn, cmd redcon.Command) {
		switch strings.ToLower(string(cmd.Args[0])) {
		case "subscribe":
			for i := 1; i < len(cmd.Args); i++ {
				ps.Subscribe(conn, string(cmd.Args[i]))
			}
		case "psubscribe":
			for i := 1; i < len(cmd.Args); i++ {
				ps.Psubscribe(conn, string(cmd.Args[i]))
			}
		case "publish":
			conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
		default:
			conn.WriteError("ERR unknown command")
		}
	}

	srv := redcon.NewServer(addr, handler, nil, nil)
	signal := make(chan error)
	go func() {
		_ = srv.ListenServeAndSignal(signal)
	}()
	require.NoError(t, <-signal)
	t.Cleanup(func() {
		_ = srv.Close()
	})

	return addr
}

// TestPubSub_bgrunner exercises the various command branches handled by the
// detached-connection background runner: ping, quit, unsubscribe, punsubscribe,
// wrong-argument handling and unknown commands.
func TestPubSub_bgrunner(t *testing.T) {
	addr := startPubSubServer(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	// Drain everything the server sends so the detached connection never
	// blocks on a full write buffer.
	done := make(chan struct{})
	go func() {
		defer close(done)
		rd := bufio.NewReader(conn)
		buf := make([]byte, 4096)
		for {
			if _, err := rd.Read(buf); err != nil {
				return
			}
		}
	}()

	write := func(s string) {
		_, err := fmt.Fprint(conn, s)
		require.NoError(t, err)
		// Give bgrunner a moment to process the command in order.
		time.Sleep(20 * time.Millisecond)
	}

	// Subscribe so the connection is detached and bgrunner takes over.
	write("subscribe foo\r\n")
	// PING with no argument.
	write("ping\r\n")
	// PING with a message.
	write("ping hello\r\n")
	// PING with too many arguments -> error branch.
	write("ping a b c\r\n")
	// SUBSCRIBE with no channel -> wrong number of arguments branch.
	write("subscribe\r\n")
	// PSUBSCRIBE through the detached runner.
	write("psubscribe pat*\r\n")
	// Unsubscribe a specific channel.
	write("unsubscribe foo\r\n")
	// Unsubscribe all (no plain subscriptions remain -> nil entry branch).
	write("unsubscribe\r\n")
	// Punsubscribe all (removes the pattern subscription).
	write("punsubscribe\r\n")
	// Unknown command -> default error branch.
	write("garbage\r\n")
	// Quit closes the connection.
	write("quit\r\n")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed by the server after QUIT")
	}
}
