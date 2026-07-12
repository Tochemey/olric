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

package olric

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/hasher"
	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/kvstore/entry"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/testutil"
	"github.com/tochemey/olric/pkg/storage"
	"github.com/tochemey/olric/stats"
)

func TestClusterClient_Ping(t *testing.T) {
	t.Run("Without TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		cluster.addMember(t)
		db := cluster.addMember(t)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		response, err := c.Ping(ctx, db.rt.This().String(), "")
		require.NoError(t, err)
		require.Equal(t, DefaultPingResponse, response)
	})
	t.Run("With TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		config := testutil.NewConfigWithTLS(t, nil, nil)
		db := cluster.addMemberWithConfig(t, config)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name}, WithConfig(config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		response, err := c.Ping(ctx, db.rt.This().String(), "")
		require.NoError(t, err)
		require.Equal(t, DefaultPingResponse, response)
	})
}

func TestClusterClient_Ping_WithMessage(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.addMember(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	message := "Olric is the best!"
	result, err := c.Ping(ctx, db.rt.This().String(), message)
	require.NoError(t, err)
	require.Equal(t, message, result)
}

func TestClusterClient_RoutingTable(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	rt, err := c.RoutingTable(ctx)
	require.NoError(t, err)

	require.Len(t, rt, int(db.config.PartitionCount))
}

func TestClusterClient_RoutingTable_Cluster(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.addMember(t) // Cluster coordinator
	<-time.After(250 * time.Millisecond)

	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	rt, err := c.RoutingTable(ctx)
	require.NoError(t, err)

	require.Len(t, rt, int(db.config.PartitionCount))
}

func TestClusterClient_Put(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)
}

func TestClusterClient_Get(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	gr, err := dm.Get(ctx, "mykey")
	require.NoError(t, err)

	res, err := gr.String()
	require.NoError(t, err)

	require.Equal(t, res, "myvalue")
}

func TestClusterClient_Delete(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	count, err := dm.Delete(ctx, "mykey")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Delete_Many_Keys(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	var keys []string
	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)
		err = dm.Put(context.Background(), key, "myvalue")
		require.NoError(t, err)
		keys = append(keys, key)
	}

	count, err := dm.Delete(context.Background(), keys...)
	require.NoError(t, err)
	require.Equal(t, 10, count)
}

func TestClusterClient_Destroy(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Destroy(ctx)
	require.NoError(t, err)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Incr(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	var errGr errgroup.Group
	for i := 0; i < 10; i++ {
		errGr.Go(func() error {
			_, err := dm.Incr(ctx, "mykey", 1)
			return err
		})
	}

	require.NoError(t, errGr.Wait())

	result, err := dm.Incr(ctx, "mykey", 1)
	require.NoError(t, err)
	require.Equal(t, 11, result)
}

func TestClusterClient_IncrByFloat(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	var errGr errgroup.Group
	for i := 0; i < 10; i++ {
		errGr.Go(func() error {
			_, err := dm.IncrByFloat(ctx, "mykey", 1.2)
			return err
		})
	}

	require.NoError(t, errGr.Wait())

	result, err := dm.IncrByFloat(ctx, "mykey", 1.2)
	require.NoError(t, err)
	require.Equal(t, 13.199999999999998, result)
}

func TestClusterClient_Decr(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", 11)
	require.NoError(t, err)

	var errGr errgroup.Group
	for i := 0; i < 10; i++ {
		errGr.Go(func() error {
			_, err := dm.Decr(ctx, "mykey", 1)
			return err
		})
	}

	require.NoError(t, errGr.Wait())

	result, err := dm.Decr(ctx, "mykey", 1)
	require.NoError(t, err)
	require.Equal(t, 0, result)
}

func TestClusterClient_GetPut(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	gr, err := dm.GetPut(ctx, "mykey", "myvalue")
	require.NoError(t, err)
	require.Nil(t, gr)

	gr, err = dm.GetPut(ctx, "mykey", "myvalue-2")
	require.NoError(t, err)

	value, err := gr.String()
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)
}

func TestClusterClient_Expire(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Expire(ctx, "mykey", time.Millisecond)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Lock_Unlock(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	lx, err := dm.Lock(ctx, "lock.foo.key", time.Second)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.NoError(t, err)
}

func TestClusterClient_Lock_Lease(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	lx, err := dm.Lock(ctx, "lock.foo.key", time.Second)
	require.NoError(t, err)

	err = lx.Lease(ctx, time.Millisecond)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	err = lx.Unlock(ctx)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestClusterClient_Lock_ErrLockNotAcquired(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	_, err = dm.Lock(ctx, "lock.foo.key", time.Second)
	require.NoError(t, err)

	_, err = dm.Lock(ctx, "lock.foo.key", time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestClusterClient_LockWithTimeout(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	lx, err := dm.LockWithTimeout(ctx, "lock.foo.key", time.Hour, time.Second)
	require.NoError(t, err)

	err = lx.Unlock(ctx)
	require.NoError(t, err)
}

func TestClusterClient_LockWithTimeout_ErrNoSuchLock(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	lx, err := dm.LockWithTimeout(ctx, "lock.foo.key", time.Millisecond, time.Second)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	err = lx.Unlock(ctx)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestClusterClient_LockWithTimeout_Then_Lease(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	lx, err := dm.LockWithTimeout(ctx, "lock.foo.key", 50*time.Millisecond, time.Second)
	require.NoError(t, err)

	// Expand its timeout value
	err = lx.Lease(ctx, time.Hour)
	require.NoError(t, err)

	<-time.After(100 * time.Millisecond)

	_, err = dm.Lock(ctx, "lock.foo.key", time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestClusterClient_LockWithTimeout_ErrLockNotAcquired(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	_, err = dm.LockWithTimeout(ctx, "lock.foo.key", time.Hour, time.Second)
	require.NoError(t, err)

	_, err = dm.Lock(ctx, "lock.foo.key", time.Millisecond)
	require.Equal(t, err, ErrLockNotAcquired)
}

func TestClusterClient_Put_Ex(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", EX(time.Second))
	require.NoError(t, err)

	<-time.After(time.Second)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Put_PX(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", PX(time.Millisecond))
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Put_EXAT(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", EXAT(time.Duration(time.Now().Add(time.Second).UnixNano())))
	require.NoError(t, err)

	<-time.After(time.Second)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Put_PXAT(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue", PXAT(time.Duration(time.Now().Add(time.Millisecond).UnixNano())))
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Put_NX(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue-2", NX())
	require.ErrorIs(t, err, ErrKeyFound)

	gr, err := dm.Get(ctx, "mykey")
	require.NoError(t, err)

	value, err := gr.String()
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)
}

func TestClusterClient_Put_XX(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue-2", XX())
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Stats(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	var empty stats.Stats
	s, err := c.Stats(ctx, db.rt.This().String())
	require.NoError(t, err)
	require.Nil(t, s.Runtime)
	require.NotEqual(t, empty, s)
}

func TestClusterClient_Stats_Cluster(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)
	db2 := cluster.addMember(t)

	<-time.After(250 * time.Millisecond)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	var empty stats.Stats
	s, err := c.Stats(ctx, db2.rt.This().String())
	require.NoError(t, err)
	require.Nil(t, s.Runtime)
	require.NotEqual(t, empty, s)
	require.Equal(t, db2.rt.This().String(), s.Member.String())
}

func TestClusterClient_Stats_CollectRuntime(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	var empty stats.Stats
	s, err := c.Stats(ctx, db.rt.This().String(), CollectRuntime())
	require.NoError(t, err)
	require.NotNil(t, s.Runtime)
	require.NotEqual(t, empty, s)
}

func TestClusterClient_Set_Options(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()

	lg := log.New(os.Stderr, "logger: ", log.Lshortfile)
	cfg := config.NewClient()
	c, err := NewClusterClient([]string{db.name}, WithConfig(cfg), WithLogger(lg))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	require.Equal(t, cfg, c.config.config)
	require.Equal(t, lg, c.config.logger)
}

func TestClusterClient_Members(t *testing.T) {
	cluster := newTestCluster(t)
	cluster.addMember(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	members, err := c.Members(ctx)
	require.NoError(t, err)
	require.Len(t, members, 2)

	coordinator := db.rt.Discovery().GetCoordinator()
	for _, member := range members {
		require.NotEqual(t, "", member.Name)
		require.NotEqual(t, 0, member.ID)
		require.NotEqual(t, 0, member.Birthdate)
		if coordinator.ID == member.ID {
			require.True(t, member.Coordinator)
		} else {
			require.False(t, member.Coordinator)
		}
	}
}

func TestClusterClient_smartPick(t *testing.T) {
	cluster := newTestCluster(t)
	db1 := cluster.addMember(t)
	db2 := cluster.addMember(t)
	db3 := cluster.addMember(t)
	db4 := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient(
		[]string{db1.name, db2.name, db3.name, db4.name},
		WithHasher(hasher.NewDefaultHasher()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	var expectedOwners map[string]struct{}
	err = testutil.TryWithInterval(20, 100*time.Millisecond, func() error {
		if err := c.fetchRoutingTable(); err != nil {
			return err
		}

		raw := c.routingTable.Load()
		if raw == nil {
			return fmt.Errorf("routing table is empty")
		}

		routingTable, ok := raw.(RoutingTable)
		if !ok {
			return fmt.Errorf("routing table is corrupt")
		}

		owners := make(map[string]struct{})
		for _, route := range routingTable {
			if len(route.PrimaryOwners) == 0 {
				continue
			}
			primary := route.PrimaryOwners[len(route.PrimaryOwners)-1]
			owners[primary] = struct{}{}
		}

		if len(owners) == 0 {
			return fmt.Errorf("routing table has no owners")
		}
		expectedOwners = owners
		return nil
	})
	require.NoError(t, err)
	expectedOwnerCount := len(expectedOwners)

	clients := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		rc, err := c.smartPick("mydmap", testutil.ToKey(i))
		require.NoError(t, err)
		clients[rc.String()] = struct{}{}
	}
	require.Len(t, clients, expectedOwnerCount)
}

func TestWithRoutingTableFetchInterval_Option(t *testing.T) {
	cfg := &clusterClientConfig{}
	opt := WithRoutingTableFetchInterval(3 * time.Second)
	opt(cfg)

	require.Equal(t, 3*time.Second, cfg.routingTableFetchInterval)
}
// getDeadAddr returns an address on the loopback interface that refuses
// connections: it binds an ephemeral port and closes the listener right away.
func getDeadAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// newFastFailClientConfig returns a sanitized client configuration without
// retries, so commands sent to dead nodes fail immediately.
func newFastFailClientConfig(t *testing.T) *config.Client {
	t.Helper()

	cc := &config.Client{
		MaxRetries:  -1,
		DialTimeout: time.Second,
	}
	require.NoError(t, cc.Sanitize())
	require.NoError(t, cc.Validate())
	return cc
}

// setDefaultHashFunc makes sure the partitions package has a hash function.
// It is normally set by NewClusterClient or the Olric node itself; tests that
// build ClusterClient values manually need to set it explicitly.
func setDefaultHashFunc(t *testing.T) {
	t.Helper()
	partitions.SetHashFunc(hasher.NewDefaultHasher())
}

// newStaticRoutingTable builds a routing table where every partition is owned
// by the given address.
func newStaticRoutingTable(partitionCount uint64, owner string) RoutingTable {
	rt := RoutingTable{}
	for partID := uint64(0); partID < partitionCount; partID++ {
		rt[partID] = Route{PrimaryOwners: []string{owner}}
	}
	return rt
}

// newDeadClusterClient returns a ClusterClient and a ClusterDMap that are wired
// to a TCP endpoint that refuses connections.
func newDeadClusterClient(t *testing.T) (*ClusterClient, *ClusterDMap, string) {
	t.Helper()
	setDefaultHashFunc(t)

	deadAddr := getDeadAddr(t)
	c := server.NewClient(newFastFailClientConfig(t))
	// Register the dead address, Pick will always return it.
	c.Get(deadAddr)

	cl := &ClusterClient{
		client:         c,
		config:         &clusterClientConfig{routingTableFetchInterval: time.Minute},
		logger:         log.New(&bytes.Buffer{}, "", 0),
		partitionCount: 7,
	}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())
	cl.routingTable.Store(newStaticRoutingTable(cl.partitionCount, deadAddr))

	dm := &ClusterDMap{
		name:          "mydmap",
		newEntry:      func() storage.Entry { return entry.New() },
		config:        &dmapConfig{},
		client:        c,
		clusterClient: cl,
	}
	return cl, dm, deadAddr
}

func TestClusterClient_clientByPartID_Errors(t *testing.T) {
	t.Run("routing table is empty", func(t *testing.T) {
		cl := &ClusterClient{}
		_, err := cl.clientByPartID(0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "routing table is empty")
	})

	t.Run("routing table is corrupt", func(t *testing.T) {
		cl := &ClusterClient{}
		cl.routingTable.Store("this-is-not-a-routing-table")
		_, err := cl.clientByPartID(0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "routing table is corrupt")
	})

	t.Run("empty primary owners", func(t *testing.T) {
		cl := &ClusterClient{client: server.NewClient(newFastFailClientConfig(t))}
		cl.routingTable.Store(RoutingTable{})
		_, err := cl.clientByPartID(0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "primary owners list for 0 is empty")
	})
}

func TestClusterDMap_smartPick_Error_Propagation(t *testing.T) {
	setDefaultHashFunc(t)

	// A ClusterClient without a routing table makes smartPick fail for all
	// DMap operations that route requests to partition owners.
	cl := &ClusterClient{partitionCount: 7}
	dm := &ClusterDMap{
		name:          "mydmap",
		newEntry:      func() storage.Entry { return entry.New() },
		config:        &dmapConfig{},
		client:        server.NewClient(newFastFailClientConfig(t)),
		clusterClient: cl,
	}

	ctx := context.Background()

	err := dm.Put(ctx, "mykey", "myvalue")
	require.Error(t, err)
	require.Contains(t, err.Error(), "routing table is empty")

	_, err = dm.Get(ctx, "mykey")
	require.Error(t, err)

	_, err = dm.Incr(ctx, "mykey", 1)
	require.Error(t, err)

	_, err = dm.Decr(ctx, "mykey", 1)
	require.Error(t, err)

	_, err = dm.GetPut(ctx, "mykey", "myvalue")
	require.Error(t, err)

	_, err = dm.IncrByFloat(ctx, "mykey", 1.2)
	require.Error(t, err)

	err = dm.Expire(ctx, "mykey", time.Second)
	require.Error(t, err)

	_, err = dm.Lock(ctx, "mykey", time.Second)
	require.Error(t, err)

	_, err = dm.LockWithTimeout(ctx, "mykey", time.Second, time.Second)
	require.Error(t, err)

	lctx := &ClusterLockContext{key: "mykey", token: "token", dm: dm}
	err = lctx.Unlock(ctx)
	require.Error(t, err)

	err = lctx.Lease(ctx, time.Second)
	require.Error(t, err)
}

func TestClusterDMap_Encode_Errors(t *testing.T) {
	setDefaultHashFunc(t)

	// smartPick succeeds (clients are created lazily, no connection is made)
	// but encoding an unsupported value type fails.
	c := server.NewClient(newFastFailClientConfig(t))
	cl := &ClusterClient{client: c, partitionCount: 7}
	cl.routingTable.Store(newStaticRoutingTable(cl.partitionCount, "127.0.0.1:0"))
	dm := &ClusterDMap{
		name:          "mydmap",
		newEntry:      func() storage.Entry { return entry.New() },
		config:        &dmapConfig{},
		client:        c,
		clusterClient: cl,
	}

	type unsupported struct{ A int }
	ctx := context.Background()

	err := dm.Put(ctx, "mykey", unsupported{A: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "can't marshal")

	_, err = dm.GetPut(ctx, "mykey", unsupported{A: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "can't marshal")
}

func TestClusterClient_DeadNode_Errors(t *testing.T) {
	cl, dm, deadAddr := newDeadClusterClient(t)
	ctx := context.Background()

	err := dm.Put(ctx, "mykey", "myvalue")
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.Delete(ctx, "mykey")
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.Incr(ctx, "mykey", 1)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.Decr(ctx, "mykey", 1)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.GetPut(ctx, "mykey", "myvalue")
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.IncrByFloat(ctx, "mykey", 1.2)
	require.ErrorIs(t, err, ErrConnRefused)

	err = dm.Expire(ctx, "mykey", time.Second)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.Lock(ctx, "mykey", time.Second)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.LockWithTimeout(ctx, "mykey", time.Second, time.Second)
	require.ErrorIs(t, err, ErrConnRefused)

	lctx := &ClusterLockContext{key: "mykey", token: "token", dm: dm}
	err = lctx.Unlock(ctx)
	require.ErrorIs(t, err, ErrConnRefused)

	err = lctx.Lease(ctx, time.Second)
	require.ErrorIs(t, err, ErrConnRefused)

	err = dm.Destroy(ctx)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = dm.Scan(ctx)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = cl.Ping(ctx, deadAddr, "hello")
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = cl.RoutingTable(ctx)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = cl.Stats(ctx, deadAddr)
	require.ErrorIs(t, err, ErrConnRefused)

	_, err = cl.Members(ctx)
	require.ErrorIs(t, err, ErrConnRefused)
}

func TestClusterClient_Pick_Errors(t *testing.T) {
	// A server.Client without registered addresses makes Pick fail.
	c := server.NewClient(newFastFailClientConfig(t))
	cl := &ClusterClient{client: c, partitionCount: 7}
	dm := &ClusterDMap{
		name:          "mydmap",
		newEntry:      func() storage.Entry { return entry.New() },
		config:        &dmapConfig{},
		client:        c,
		clusterClient: cl,
	}

	ctx := context.Background()

	_, err := dm.Delete(ctx, "mykey")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no available client")

	err = dm.Destroy(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no available client")

	_, err = cl.RoutingTable(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no available client")

	_, err = cl.Members(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no available client")

	err = cl.RefreshMetadata(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no available client")

	err = cl.fetchRoutingTable()
	require.Error(t, err)
	require.Contains(t, err.Error(), "error while loading the routing table")
}

func TestClusterDMap_makeGetResponse_Error(t *testing.T) {
	dm := &ClusterDMap{
		name:     "mydmap",
		newEntry: func() storage.Entry { return entry.New() },
	}

	cmd := redis.NewStringCmd(context.Background())
	cmd.SetErr(errors.New("something went wrong"))

	_, err := dm.makeGetResponse(cmd)
	require.Error(t, err)
}

func TestClusterClient_Close_Twice(t *testing.T) {
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)

	require.NoError(t, c.Close(ctx))
	// The second call is a no-op: the context is already canceled.
	require.NoError(t, c.Close(ctx))
}

func TestClusterClient_NewDMap_WithOptions(t *testing.T) {
	cl := &ClusterClient{client: server.NewClient(newFastFailClientConfig(t))}
	dm, err := cl.NewDMap("mydmap", StorageEntryImplementation(func() storage.Entry {
		return entry.New()
	}))
	require.NoError(t, err)
	require.Equal(t, "mydmap", dm.Name())
}

// safeBuffer is a goroutine-safe bytes.Buffer used to capture log output.
type safeBuffer struct {
	mtx sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.buf.String()
}

func TestClusterClient_fetchRoutingTablePeriodically_Error(t *testing.T) {
	buf := &safeBuffer{}
	cl := &ClusterClient{
		// No registered addresses: fetching the routing table always fails.
		client: server.NewClient(newFastFailClientConfig(t)),
		config: &clusterClientConfig{routingTableFetchInterval: time.Millisecond},
		logger: log.New(buf, "", 0),
	}
	cl.ctx, cl.cancel = context.WithCancel(context.Background())

	cl.wg.Add(1)
	go cl.fetchRoutingTablePeriodically()

	err := testutil.TryWithInterval(50, 10*time.Millisecond, func() error {
		if !strings.Contains(buf.String(), "[ERROR] Failed to fetch the latest version of the routing table") {
			return errors.New("no error logged yet")
		}
		return nil
	})
	require.NoError(t, err)

	cl.cancel()
	cl.wg.Wait()
}

func TestNewClusterClient_Errors(t *testing.T) {
	t.Run("empty addresses", func(t *testing.T) {
		_, err := NewClusterClient([]string{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "addresses cannot be empty")
	})

	t.Run("member discovery fails", func(t *testing.T) {
		deadAddr := getDeadAddr(t)
		_, err := NewClusterClient([]string{deadAddr}, WithConfig(newFastFailClientConfig(t)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "error while discovering the cluster members")
	})
}
