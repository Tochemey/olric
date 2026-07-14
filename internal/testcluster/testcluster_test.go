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

package testcluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/cluster/routingtable"
	"github.com/tochemey/olric/internal/environment"
	"github.com/tochemey/olric/internal/service"
)

type stubService struct{}

func (s *stubService) Start() error                   { return nil }
func (s *stubService) RegisterHandlers()              {}
func (s *stubService) Shutdown(context.Context) error { return nil }

func newStubService(_ *environment.Environment) (service.Service, error) {
	return &stubService{}, nil
}

func TestTestCluster_ClusterEventPublisher_PlainFunc(t *testing.T) {
	called := make(chan struct{}, 1)
	e := NewEnvironment(nil)
	// A plain func literal, deliberately not converted to
	// routingtable.ClusterEventPublisher: the harness must accept both.
	e.Set("cluster-event-publisher", func(_ context.Context, _, _ string) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return nil
	})

	cluster := New(newStubService)
	defer cluster.Shutdown()
	cluster.AddMember(e)

	rt := e.Get("routingtable").(*routingtable.RoutingTable)
	require.NoError(t, rt.PublishClusterEvent(context.Background(), "my-channel", "hello, world!"))

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("the injected publisher was never invoked")
	}
}

func TestTestCluster_ClusterEventPublisher_UnsupportedType(t *testing.T) {
	e := NewEnvironment(nil)
	e.Set("cluster-event-publisher", 42)

	cluster := New(newStubService)
	defer cluster.Shutdown()
	require.Panics(t, func() {
		cluster.AddMember(e)
	})
}
