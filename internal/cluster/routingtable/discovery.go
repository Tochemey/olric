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

package routingtable

import (
	"context"
	"errors"
	"time"
)

var (
	ErrServerGone   = errors.New("server is gone")
	ErrNotJoinedYet = errors.New("not joined yet")
	ErrClusterJoin  = errors.New("cannot join the cluster")
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")
)

// bootstrapCoordinator prepares the very first routing table and bootstraps the coordinator node.
func (r *RoutingTable) bootstrapCoordinator() error {
	r.Lock()
	defer r.Unlock()

	r.fillRoutingTable()
	data, signature, err := r.buildRoutingTablePayload()
	if err != nil {
		return err
	}
	reports, err := r.updateRoutingTableOnCluster(data)
	if err != nil {
		return err
	}
	r.committedPayload.Store(data)
	updated := make([]uint64, 0, len(reports))
	for member := range reports {
		updated = append(updated, member.ID)
	}
	r.startRebalanceEpoch(signature, rebalanceReasonBootstrap, "", updated)
	// The coordinator bootstraps itself.
	r.markBootstrapped()
	r.log.V(2).Printf("[INFO] The cluster coordinator has been bootstrapped")
	return nil
}

func (r *RoutingTable) attemptToJoin() error {
	attempts := 0
	for attempts < r.config.MaxJoinAttempts {
		select {
		case <-r.ctx.Done():
			// The node is gone.
			return ErrServerGone
		default:
		}

		attempts++
		n, err := r.discovery.Join()
		if err == nil {
			r.log.V(2).Printf("[INFO] Join completed. Synced with %d initial nodes", n)
			return nil
		}

		r.log.V(2).Printf("[ERROR] Join attempt returned error: %s", err)
		if r.IsBootstrapped() {
			r.log.V(2).Printf("[INFO] Bootstrapped by the cluster coordinator")
			return nil
		}

		r.log.V(2).Printf("[INFO] Awaits for %s to join again (%d/%d)",
			r.config.JoinRetryInterval, attempts, r.config.MaxJoinAttempts)
		<-time.After(r.config.JoinRetryInterval)
	}
	return ErrClusterJoin
}

// tryRejoin actively re-resolves and contacts live peers when the member count
// quorum is not currently satisfied. It queries the configured service
// discovery backend or static peers via discovery.Join, exactly like a fresh
// Join() call would, so a peer that was unresolvable earlier is picked up as
// soon as it comes back. It is a no-op when quorum is already satisfied. The
// quorum can be short of its target either because this node never found a
// peer at startup or because it fell into a minority partition later, so the
// log message covers both rather than naming one.
func (r *RoutingTable) tryRejoin() {
	if r.CheckMemberCountQuorum() == nil {
		return
	}

	r.log.V(2).Printf("[INFO] Member count quorum is not satisfied (%d/%d members). Attempting to join the cluster.",
		r.NumMembers(), r.config.MemberCountQuorum)
	n, err := r.discovery.Join()
	if err != nil {
		r.log.V(3).Printf("[WARN] Rejoin attempt failed: %v", err)
		return
	}
	r.log.V(2).Printf("[INFO] Rejoin contacted %d node(s)", n)
}

// rejoinLoop periodically attempts to (re)join the cluster while the member
// count quorum is unsatisfied. It calls tryRejoin on each tick, which is
// silent when quorum is satisfied. Start starts this goroutine before the
// quorum gate it has to unblock, and only when MemberCountQuorum is greater
// than MinimumMemberCountQuorum. It runs until the node's context is canceled.
func (r *RoutingTable) rejoinLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.RejoinInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.tryRejoin()
		}
	}
}

func (r *RoutingTable) tryWithInterval(ctx context.Context, interval time.Duration, f func() error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var funcErr error

	funcErr = f()
	if funcErr == nil {
		// Done. No need to try with interval
		return nil
	}

loop:
	for {
		select {
		case <-ctx.Done():
			// context is done
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				break loop
			}
			if errors.Is(err, context.Canceled) {
				return ErrServerGone
			}
			return err
		case <-ticker.C:
			funcErr = f()
			if funcErr == nil {
				break loop
			}
		}
	}
	return funcErr
}
