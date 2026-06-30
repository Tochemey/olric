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

package discovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/testutil"
)

// TestDelegateCallbacks exercises the memberlist.Delegate implementation
// directly. NotifyMsg, MergeRemoteState, GetBroadcasts and LocalState are
// no-ops, but they must not panic and have to return the expected zero values.
func TestDelegateCallbacks(t *testing.T) {
	c := testutil.NewConfig()
	f := testutil.NewFlogger(c)
	d := New(f, c)

	dl, err := d.newDelegate()
	require.NoError(t, err)

	// NodeMeta returns the encoded member metadata.
	meta := dl.NodeMeta(1024)
	require.NotEmpty(t, meta)

	decoded, err := NewMemberFromMetadata(meta)
	require.NoError(t, err)
	require.Equal(t, d.member.Name, decoded.Name)
	require.True(t, d.member.CompareByID(decoded))

	// The remaining callbacks are no-ops and should not panic.
	require.NotPanics(t, func() { dl.NotifyMsg([]byte("user-data")) })
	require.NotPanics(t, func() { dl.MergeRemoteState([]byte("remote-state"), true) })
	require.NotPanics(t, func() { dl.MergeRemoteState(nil, false) })
	require.Nil(t, dl.GetBroadcasts(0, 1024))
	require.Nil(t, dl.LocalState(true))
	require.Nil(t, dl.LocalState(false))
}
