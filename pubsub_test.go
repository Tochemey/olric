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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kapetan-io/tackle/autotls"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/olric/internal/checkpoint"
	"github.com/tochemey/olric/internal/server"
	"github.com/tochemey/olric/internal/testutil"
)

func pubsubTestRunner(t *testing.T, ps *PubSub, kind, channel string) {
	ctx := context.Background()
	var rp *redis.PubSub
	switch kind {
	case "subscribe":
		rp = ps.Subscribe(ctx, channel)
	case "psubscribe":
		rp = ps.PSubscribe(ctx, channel)
	}

	defer func() {
		require.NoError(t, rp.Close())
	}()

	// Wait for confirmation that subscription is created before publishing anything.
	msgi, err := rp.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)

	subs := msgi.(*redis.Subscription)
	require.Equal(t, kind, subs.Kind)
	require.Equal(t, channel, subs.Channel)
	require.Equal(t, 1, subs.Count)

	// Go channel which receives messages.
	ch := rp.Channel()

	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("my-message-%d", i)
		count, err := ps.Publish(ctx, "my-channel", msg)
		require.Equal(t, int64(1), count)
		require.NoError(t, err)
		expected[msg] = struct{}{}
	}

	consumed := make(map[string]struct{})
L:
	for {
		select {
		case msg := <-ch:
			require.Equal(t, "my-channel", msg.Channel)
			consumed[msg.Payload] = struct{}{}
			if len(consumed) == 10 {
				// It would be OK
				break L
			}
		case <-time.After(5 * time.Second):
			// Enough. Break it and check the consumed items.
			break L
		}
	}

	require.Equal(t, expected, consumed)
}

func TestPubSub_Publish_Subscribe(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		config := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)

		cluster := newTestCluster(t)
		db := cluster.addMemberWithConfig(t, config)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name}, WithConfig(db.config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		pubsubTestRunner(t, ps, "subscribe", "my-channel")
	})
	t.Run("With No TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		db := cluster.addMember(t)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		pubsubTestRunner(t, ps, "subscribe", "my-channel")
	})
}

func TestPubSub_Publish_PSubscribe(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		config := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)

		cluster := newTestCluster(t)
		db := cluster.addMemberWithConfig(t, config)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name}, WithConfig(db.config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)
		pubsubTestRunner(t, ps, "psubscribe", "my-*")
	})
	t.Run("With No TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		db := cluster.addMember(t)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)
		pubsubTestRunner(t, ps, "psubscribe", "my-*")
	})
}

func TestPubSub_PubSubChannels(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		config := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)

		cluster := newTestCluster(t)
		db := cluster.addMemberWithConfig(t, config)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name}, WithConfig(db.config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		rp := ps.Subscribe(ctx, "my-channel")

		defer func() {
			require.NoError(t, rp.Close())
		}()

		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)

		channels, err := ps.PubSubChannels(ctx, "my-*")
		require.NoError(t, err)

		require.Equal(t, []string{"my-channel"}, channels)
	})
	t.Run("With No TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		db := cluster.addMember(t)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		rp := ps.Subscribe(ctx, "my-channel")

		defer func() {
			require.NoError(t, rp.Close())
		}()

		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)

		channels, err := ps.PubSubChannels(ctx, "my-*")
		require.NoError(t, err)

		require.Equal(t, []string{"my-channel"}, channels)
	})
}

func TestPubSub_PubSubNumSub(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		config := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)

		cluster := newTestCluster(t)
		db := cluster.addMemberWithConfig(t, config)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name}, WithConfig(db.config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		rp := ps.Subscribe(ctx, "my-channel")

		defer func() {
			require.NoError(t, rp.Close())
		}()

		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)

		numsub, err := ps.PubSubNumSub(ctx, "my-channel", "foobar")
		require.NoError(t, err)

		expected := map[string]int64{
			"foobar":     0,
			"my-channel": 1,
		}
		require.Equal(t, expected, numsub)
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		db := cluster.addMember(t)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		rp := ps.Subscribe(ctx, "my-channel")

		defer func() {
			require.NoError(t, rp.Close())
		}()

		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)

		numsub, err := ps.PubSubNumSub(ctx, "my-channel", "foobar")
		require.NoError(t, err)

		expected := map[string]int64{
			"foobar":     0,
			"my-channel": 1,
		}
		require.Equal(t, expected, numsub)
	})
}

func TestPubSub_PubSubNumPat(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))
		config := testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS)

		cluster := newTestCluster(t)
		db := cluster.addMemberWithConfig(t, config)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name}, WithConfig(db.config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		rp := ps.PSubscribe(ctx, "my-*")

		defer func() {
			require.NoError(t, rp.Close())
		}()

		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)

		numpat, err := ps.PubSubNumPat(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), numpat)
	})
	t.Run("With no TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		db := cluster.addMember(t)

		ctx := context.Background()
		c, err := NewClusterClient([]string{db.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps, err := c.NewPubSub(ToAddress(db.rt.This().String()))
		require.NoError(t, err)

		rp := ps.PSubscribe(ctx, "my-*")

		defer func() {
			require.NoError(t, rp.Close())
		}()

		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)

		numpat, err := ps.PubSubNumPat(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), numpat)
	})
}

func TestPubSub_EmbeddedClient_DefaultAddress(t *testing.T) {
	// Regression test for https://github.com/olric-data/olric/issues/244
	//
	// EmbeddedClient.NewPubSub without a ToAddress option used to call Pick()
	// on the node's internal connection pool. That pool is populated lazily by
	// intra-cluster traffic, so on a freshly joined non-coordinator member it
	// was often empty and NewPubSub failed with "no available client found".
	cluster := newTestCluster(t)
	db1 := cluster.addMember(t)
	db2 := cluster.addMember(t)

	e := db2.NewEmbeddedClient()
	ps, err := e.NewPubSub()
	require.NoError(t, err)

	// The default target must be the local node, not an arbitrary member.
	require.Equal(t, db2.rt.This().String(), ps.rc.Options().Addr)

	// A blank ToAddress must fall back to the local node too, instead of
	// picking a random member from the connection pool.
	for _, addr := range []string{"", " ", "\t", "\n", " \t\n "} {
		ps, err = e.NewPubSub(ToAddress(addr))
		require.NoError(t, err)
		require.Equal(t, db2.rt.This().String(), ps.rc.Options().Addr)
	}

	// An explicit non-blank ToAddress must win over the local-node default.
	ps1, err := e.NewPubSub(ToAddress(db1.rt.This().String()))
	require.NoError(t, err)
	require.Equal(t, db1.rt.This().String(), ps1.rc.Options().Addr)

	pubsubTestRunner(t, ps, "subscribe", "my-channel")
}

func TestPubSub_EmbeddedClient_NotJoined(t *testing.T) {
	// This instance is never started, so its checkpoints would otherwise leak
	// into the process-global counters and break subsequent tests.
	checkpoint.Reset()

	// Before the node joins the cluster the local member is unknown, so
	// NewPubSub without an explicit address must fail fast with a clear error
	// instead of "no available client found".
	db, err := New(testutil.NewConfig())
	require.NoError(t, err)

	_, err = db.NewEmbeddedClient().NewPubSub()
	require.ErrorIs(t, err, ErrNotJoinedYet)
}

func TestPubSub_newPubSub_AddressResolution(t *testing.T) {
	// A client with an empty connection pool; redis clients are created
	// lazily, so no server is required to test address resolution.
	client := server.NewClient(nil)

	t.Run("empty pool and no default resolver", func(t *testing.T) {
		_, err := newPubSub(client, nil)
		require.Error(t, err)
	})

	t.Run("resolver error is propagated", func(t *testing.T) {
		_, err := newPubSub(client, func() (string, error) {
			return "", ErrNotJoinedYet
		})
		require.ErrorIs(t, err, ErrNotJoinedYet)
	})

	t.Run("resolver provides the default address", func(t *testing.T) {
		ps, err := newPubSub(client, func() (string, error) {
			return "127.0.0.1:3320", nil
		})
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:3320", ps.rc.Options().Addr)
	})

	t.Run("explicit address wins over the resolver", func(t *testing.T) {
		ps, err := newPubSub(client, func() (string, error) {
			return "127.0.0.1:3320", nil
		}, ToAddress("127.0.0.1:6060"))
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:6060", ps.rc.Options().Addr)
	})

	t.Run("blank resolver result falls back to pick", func(t *testing.T) {
		// The pool has been populated by the subtests above.
		ps, err := newPubSub(client, func() (string, error) {
			return " \t\n", nil
		})
		require.NoError(t, err)
		require.Contains(t, client.Addresses(), ps.rc.Options().Addr)
	})
}

func TestPubSub_ClusterClient_NoAddress(t *testing.T) {
	// ClusterClient seeds its connection pool with all known members at
	// construction, so NewPubSub without a ToAddress must pick one of them.
	cluster := newTestCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	ps, err := c.NewPubSub()
	require.NoError(t, err)
	require.Contains(t, c.client.Addresses(), ps.rc.Options().Addr)

	pubsubTestRunner(t, ps, "subscribe", "my-channel")
}

func TestPubSub_Cluster(t *testing.T) {
	t.Run("With TLS", func(t *testing.T) {
		conf := autotls.Config{AutoTLS: true}
		require.NoError(t, autotls.Setup(&conf))

		cluster := newTestCluster(t)
		db1 := cluster.addMemberWithConfig(t, testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS))
		db2 := cluster.addMemberWithConfig(t, testutil.NewConfigWithTLS(t, conf.ServerTLS, conf.ClientTLS))

		// Create a subscriber
		ctx := context.Background()
		c, err := NewClusterClient([]string{db1.name}, WithConfig(db1.config.Client))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps1, err := c.NewPubSub(ToAddress(db1.rt.This().String()))
		require.NoError(t, err)

		rp := ps1.Subscribe(ctx, "my-channel")
		defer func() {
			require.NoError(t, rp.Close())
		}()
		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)
		receiveChan := rp.Channel()

		// Create a publisher

		e := db2.NewEmbeddedClient()
		ps2, err := e.NewPubSub(ToAddress(db2.rt.This().String()))
		require.NoError(t, err)
		expected := make(map[string]struct{})
		for i := 0; i < 10; i++ {
			msg := fmt.Sprintf("my-message-%d", i)
			count, err := ps2.Publish(ctx, "my-channel", msg)
			require.Equal(t, int64(1), count)
			require.NoError(t, err)
			expected[msg] = struct{}{}
		}

		consumed := make(map[string]struct{})
	L:
		for {
			select {
			case msg := <-receiveChan:
				require.Equal(t, "my-channel", msg.Channel)
				consumed[msg.Payload] = struct{}{}
				if len(consumed) == 10 {
					// It would be OK
					break L
				}
			case <-time.After(5 * time.Second):
				// Enough. Break it and check the consumed items.
				break L
			}
		}
	})
	t.Run("Without TLS", func(t *testing.T) {
		cluster := newTestCluster(t)
		db1 := cluster.addMember(t)
		db2 := cluster.addMember(t)

		// Create a subscriber
		ctx := context.Background()
		c, err := NewClusterClient([]string{db1.name})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, c.Close(ctx))
		}()

		ps1, err := c.NewPubSub(ToAddress(db1.rt.This().String()))
		require.NoError(t, err)

		rp := ps1.Subscribe(ctx, "my-channel")
		defer func() {
			require.NoError(t, rp.Close())
		}()
		// Wait for confirmation that subscription is created before publishing anything.
		_, err = rp.ReceiveTimeout(ctx, time.Second)
		require.NoError(t, err)
		receiveChan := rp.Channel()

		// Create a publisher

		e := db2.NewEmbeddedClient()
		ps2, err := e.NewPubSub(ToAddress(db2.rt.This().String()))
		require.NoError(t, err)
		expected := make(map[string]struct{})
		for i := 0; i < 10; i++ {
			msg := fmt.Sprintf("my-message-%d", i)
			count, err := ps2.Publish(ctx, "my-channel", msg)
			require.Equal(t, int64(1), count)
			require.NoError(t, err)
			expected[msg] = struct{}{}
		}

		consumed := make(map[string]struct{})
	L:
		for {
			select {
			case msg := <-receiveChan:
				require.Equal(t, "my-channel", msg.Channel)
				consumed[msg.Payload] = struct{}{}
				if len(consumed) == 10 {
					// It would be OK
					break L
				}
			case <-time.After(5 * time.Second):
				// Enough. Break it and check the consumed items.
				break L
			}
		}
	})
}
