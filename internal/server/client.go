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
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/tochemey/olric/config"
	"github.com/tochemey/olric/internal/roundrobin"
)


type Client struct {
	mu sync.RWMutex

	config     *config.Client
	clients    map[string]*redis.Client
	roundRobin *roundrobin.RoundRobin
	// banned holds addresses of members the cluster knows are dead. A banned
	// address never (re-)enters the round-robin rotation: Get still returns a
	// usable client for callers that explicitly target the address (from a
	// stale routing table snapshot, for instance), but Pick will not dial it.
	// The ban is lifted by Unban once the member is seen alive again.
	//
	// Invariant, maintained under mu: an address is in the rotation if and
	// only if it has an entry in clients and is not banned.
	banned map[string]struct{}
}

func NewClient(c *config.Client) *Client {
	if c == nil {
		c = config.NewClient()
		err := c.Sanitize()
		if err != nil {
			panic(fmt.Sprintf("failed to sanitize client config: %s", err))
		}
	}
	return &Client{
		config:     c,
		clients:    make(map[string]*redis.Client),
		roundRobin: roundrobin.New(nil),
		banned:     make(map[string]struct{}),
	}
}

func (c *Client) Addresses() map[string]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addresses := make(map[string]struct{})
	for address := range c.clients {
		addresses[address] = struct{}{}
	}
	return addresses
}

func (c *Client) Get(addr string) *redis.Client {
	c.mu.RLock()
	rc, ok := c.clients[addr]
	if ok {
		c.mu.RUnlock()
		return rc
	}
	c.mu.RUnlock()

	// Need the lock for writing, we modify c.clients map and the round-robin
	// implementation updates its internal state.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Need to check again, because another goroutine may have updated clients
	// between our calls to RUnlock and Lock.
	if rc, ok = c.clients[addr]; ok {
		return rc
	}

	opt := c.config.RedisOptions()
	opt.Addr = addr
	rc = redis.NewClient(opt)
	c.clients[addr] = rc
	if _, ok := c.banned[addr]; !ok {
		c.roundRobin.Add(addr)
	}
	return rc
}

func (c *Client) pickNodeRoundRobin() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addr, err := c.roundRobin.Get()
	if err == roundrobin.ErrEmptyInstance {
		return "", fmt.Errorf("no available client found")
	}
	if err != nil {
		return "", err
	}
	return addr, nil
}

func (c *Client) Pick() (*redis.Client, error) {
	addr, err := c.pickNodeRoundRobin()
	if err != nil {
		return nil, err
	}
	return c.Get(addr), nil
}

// closeLocked closes and forgets the connection pool of addr. The caller must
// hold c.mu.
func (c *Client) closeLocked(addr string) error {
	rc, ok := c.clients[addr]
	if ok {
		err := rc.Close()
		if err != nil {
			return err
		}
		c.roundRobin.Delete(addr)
		delete(c.clients, addr)
	}

	return nil
}

func (c *Client) Close(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closeLocked(addr)
}

// Ban closes the connection pool of addr like Close and additionally keeps the
// address out of the round-robin rotation until Unban: a later Get for the
// same address still returns a usable client, but Pick will not dial it. Use
// it when the cluster reports the member dead, so a caller holding a stale
// routing table cannot resurrect the address into the rotation.
func (c *Client) Ban(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.banned[addr] = struct{}{}
	return c.closeLocked(addr)
}

// Unban lifts the ban on addr. If a client for the address was created while
// the ban was in effect, the address rejoins the round-robin rotation here;
// otherwise it rejoins lazily on the next Get.
func (c *Client) Unban(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.banned[addr]; !ok {
		return
	}
	delete(c.banned, addr)
	if _, ok := c.clients[addr]; ok {
		c.roundRobin.Add(addr)
	}
}

func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, rc := range c.clients {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := rc.Close(); err != nil {
			return err
		}
		delete(c.clients, addr)
		c.roundRobin.Delete(addr)
	}

	return nil
}
