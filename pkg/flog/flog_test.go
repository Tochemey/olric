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

package flog

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestLogger() (*Logger, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	return New(log.New(buf, "", 0)), buf
}

func TestNew(t *testing.T) {
	l, _ := newTestLogger()
	require.NotNil(t, l)
}

func TestLogger_SetLevel(t *testing.T) {
	l, _ := newTestLogger()

	// A negative level is ignored and leaves the level untouched.
	l.SetLevel(-1)
	require.False(t, l.V(1).Ok())

	l.SetLevel(3)
	require.True(t, l.V(1).Ok())
	require.True(t, l.V(3).Ok())
	require.False(t, l.V(4).Ok())
}

func TestLogger_ShowLineNumber_NegativeIgnored(t *testing.T) {
	l, buf := newTestLogger()
	l.SetLevel(1)

	// A negative value is ignored: line numbers stay disabled.
	l.ShowLineNumber(-1)
	l.V(1).Printf("hello")
	require.Equal(t, "hello\n", buf.String())
}

func TestVerbose_Ok(t *testing.T) {
	l, _ := newTestLogger()
	l.SetLevel(2)
	require.True(t, l.V(2).Ok())
	require.False(t, l.V(3).Ok())
}

func TestVerbose_Printf_Disabled(t *testing.T) {
	l, buf := newTestLogger()
	// Level defaults to 0, so V(1) is not ok and nothing is written.
	l.V(1).Printf("should not appear")
	require.Empty(t, buf.String())
}

func TestVerbose_Printf_Enabled(t *testing.T) {
	l, buf := newTestLogger()
	l.SetLevel(5)
	l.V(1).Printf("value=%d", 7)
	require.Equal(t, "value=7\n", buf.String())
}

func TestVerbose_Printf_WithLineNumber(t *testing.T) {
	l, buf := newTestLogger()
	l.SetLevel(5)
	l.ShowLineNumber(1)
	l.V(1).Printf("value=%d", 7)

	out := buf.String()
	require.Contains(t, out, "value=7")
	// The "=> file:line" suffix is appended when line numbers are enabled.
	require.Contains(t, out, "=>")
	require.Contains(t, out, "flog_test.go:")
}

func TestVerbose_Println_Disabled(t *testing.T) {
	l, buf := newTestLogger()
	l.V(1).Println("should not appear")
	require.Empty(t, buf.String())
}

func TestVerbose_Println_Enabled(t *testing.T) {
	l, buf := newTestLogger()
	l.SetLevel(5)
	l.V(1).Println("hello", "world")
	require.Equal(t, "hello world\n", buf.String())
}

func TestVerbose_Println_WithLineNumber(t *testing.T) {
	l, buf := newTestLogger()
	l.SetLevel(5)
	l.ShowLineNumber(1)
	l.V(1).Println("hello")

	out := buf.String()
	require.True(t, strings.HasPrefix(out, "hello"))
	require.Contains(t, out, "=>")
	require.Contains(t, out, "flog_test.go:")
}
