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

package pubsub

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestService_Shutdown_ContextCancelled covers the branch of Service.Shutdown
// where the provided context is cancelled before the background workers finish.
func TestService_Shutdown_ContextCancelled(t *testing.T) {
	s := &Service{}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Add to the wait group without ever calling Done so that wg.Wait blocks
	// and Shutdown has to fall back to the context.
	s.wg.Add(1)
	defer s.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Shutdown(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
