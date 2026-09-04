/*
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

package dmap

import (
	"context"
	"errors"

	"github.com/tochemey/olric/internal/discovery"
)

// errOwnerDeparted is returned by a fragment transfer whose target memberlist
// has already removed: the move fails without a dial and the balancer retries
// it against the next routing table.
var errOwnerDeparted = errors.New("owner is no longer a cluster member")

// isMember reports whether owner is a live member as memberlist sees it.
// Owners come from the routing table, which lags memberlist by one push, so a
// member that just died can still be listed as an owner; dialing it can only
// fail, and on a network that drops packets it fails slowly, so the remote
// paths skip it. The check is a map lookup under a read lock and allocates
// nothing.
func (x *Service) isMember(owner discovery.Member) bool {
	members := x.rt.Members()
	members.RLock()
	defer members.RUnlock()

	_, err := members.Get(owner.ID)
	return err == nil
}

// remoteCallContext returns the context a call to another member runs under
// on behalf of the request context ctx. When ctx carries a deadline it is
// returned as is, so nothing is allocated; otherwise the call is bounded by
// one attempt's worth of the client timeouts, so a dead peer costs a bounded
// wait instead of the client's retry chain. The caller must call cancel.
func (x *Service) remoteCallContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	c := x.config.Client
	return context.WithTimeout(ctx, c.DialTimeout+c.WriteTimeout+c.ReadTimeout)
}
