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

package server

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/tochemey/olric/internal/util"
)

// ServeMux is an RESP command multiplexer.
type ServeMux struct {
	handlers map[string]redcon.Handler
}

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers: make(map[string]redcon.Handler),
	}
}

// HandleFunc registers the handler function for the given command.
func (m *ServeMux) HandleFunc(command string, handler redcon.Handler) {
	if handler == nil {
		panic("olric: nil handler")
	}
	m.Handle(command, handler)
}

// Handle registers the handler for the given command.
// If a handler already exists for command, Handle panics.
func (m *ServeMux) Handle(command string, handler redcon.Handler) {
	if command == "" {
		panic("olric: invalid command")
	}
	if handler == nil {
		panic("olric: nil handler")
	}
	if _, exist := m.handlers[command]; exist {
		panic("olric: multiple registrations for " + command)
	}

	m.handlers[command] = handler
}

// ServeRESP dispatches the command to the handler.
func (m *ServeMux) ServeRESP(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(util.BytesToString(cmd.Args[0]))

	if handler, ok := m.handlers[command]; ok {
		handler.ServeRESP(conn, cmd)
		return
	}

	if command == "pubsub" {
		if len(cmd.Args) < 2 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", command))
			return
		}
		command = fmt.Sprintf("%s %s", command, util.BytesToString(cmd.Args[1]))
	}

	if handler, ok := m.handlers[command]; ok {
		handler.ServeRESP(conn, cmd)
		return
	}

	conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
}
