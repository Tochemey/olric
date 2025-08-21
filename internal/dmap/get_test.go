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

package dmap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/cluster/routingtable"
	"github.com/tochemey/olric/internal/ptr"
	"github.com/tochemey/olric/internal/testcluster"
	"github.com/tochemey/olric/internal/testutil"
)

func TestDMap_Get_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	// Call DMap.Put on S1
	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		gr, part, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.NotNil(t, part)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}

func TestDMap_Get_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	// Call DMap.Put on S1
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)

	}

	// Call DMap.Get on S2
	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)
		expected := dm2.s.primary.PartitionByHKey(partitions.HKey(dm2.name, key)).ID()
		res, part, err := dm2.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, part)
		partValue := ptr.Deref(part, uint64(0))
		require.Exactly(t, expected, partValue)
		require.Equal(t, testutil.ToVal(i), res.Value())
	}
}

func TestDMap_Get_Lookup(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	ctx := context.Background()

	// Call DMap.Put on S1
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	s3 := cluster.AddMember(nil).(*Service)
	// Call DMap.Get on S3
	dm3, err := s3.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)
		expected := dm3.s.primary.PartitionByHKey(partitions.HKey(dm3.name, key)).ID()
		gr, part, err := dm3.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), gr.Value())
		require.NotNil(t, part)
		partValue := ptr.Deref(part, uint64(0))
		require.Exactly(t, expected, partValue)
	}
}

func TestDMap_Get_NilValue(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	// Call DMap.Put on S1
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(ctx, "foobar", nil, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	gr, part, err := dm.Get(ctx, "foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	require.NotNil(t, part)
	require.Equal(t, []byte{}, gr.Value())

	_, err = dm.Delete(ctx, "foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, _, err = dm.Get(ctx, "foobar")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_Get_NilValue_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	// Call DMap.Put on S1
	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "foobar", nil, nil)
	require.NoError(t, err)

	dm2, err := s2.NewDMap("mydmap")
	require.NoError(t, err)

	gr, part, err := dm2.Get(ctx, "foobar")
	require.NoError(t, err)
	require.Equal(t, []byte{}, gr.Value())
	require.NotNil(t, part)

	_, err = dm2.Delete(ctx, "foobar")
	require.NoError(t, err)

	_, _, err = dm2.Get(ctx, "foobar")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Put_ReadQuorum(t *testing.T) {
	cluster := testcluster.New(NewService)
	// Create DMap services with custom configuration
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	c.ReadQuorum = 2
	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, _, err = dm.Get(ctx, testutil.ToKey(1))
	if err != ErrReadQuorum {
		t.Fatalf("Expected ErrReadQuorum. Got: %v", err)
	}
}

func TestDMap_Get_ReadRepair(t *testing.T) {
	cluster := testcluster.New(NewService)
	c1 := testutil.NewConfig()
	c1.ReadRepair = true
	c1.ReplicaCount = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReadRepair = true
	c2.ReplicaCount = 2
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm1, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err = dm1.Put(ctx, testutil.ToKey(i), testutil.ToVal(i), nil)
		require.NoError(t, err)
	}

	err = s2.Shutdown(context.Background())
	require.NoError(t, err)

	rt := e2.Get("routingtable").(*routingtable.RoutingTable)
	err = rt.Shutdown(context.Background())
	require.NoError(t, err)

	c3 := testutil.NewConfig()
	c3.ReadRepair = true
	c3.ReplicaCount = 2
	e3 := testcluster.NewEnvironment(c3)
	s3 := cluster.AddMember(e3).(*Service)

	// Call DMap.Get on S2
	dm2, err := s3.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)
		expected := dm2.s.primary.PartitionByHKey(partitions.HKey(dm2.name, key)).ID()
		gr, part, err := dm2.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, part)
		partValue := ptr.Deref(part, uint64(0))
		require.Exactly(t, expected, partValue)
		require.Equal(t, testutil.ToVal(i), gr.Value())
	}
}
