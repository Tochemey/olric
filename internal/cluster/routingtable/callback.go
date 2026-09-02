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

func (r *RoutingTable) AddCallback(f func()) {
	r.callbackMtx.Lock()
	defer r.callbackMtx.Unlock()

	r.callbacks = append(r.callbacks, f)
}

// AddNodeLeaveCallback registers a function that is called with the name
// (host:port) of every member that leaves the cluster. Callbacks run
// synchronously on the membership event goroutine, in registration order and
// strictly in event order, and stop firing once the routing table shuts down.
// They must be fast and must not block: a slow callback stalls membership
// event processing. Register before Start; registration itself is safe at any
// time.
func (r *RoutingTable) AddNodeLeaveCallback(f func(nodeName string)) {
	r.memberCallbackMtx.Lock()
	defer r.memberCallbackMtx.Unlock()

	r.leaveCallbacks = append(r.leaveCallbacks, f)
}

// AddNodeJoinCallback registers a function that is called with the name
// (host:port) of every member that joins the cluster or is confirmed alive by
// a node-update event. It is the counterpart of AddNodeLeaveCallback: a leave
// callback that bans an address relies on this one to lift the ban when the
// member comes back. Same execution contract as AddNodeLeaveCallback.
func (r *RoutingTable) AddNodeJoinCallback(f func(nodeName string)) {
	r.memberCallbackMtx.Lock()
	defer r.memberCallbackMtx.Unlock()

	r.joinCallbacks = append(r.joinCallbacks, f)
}

// notifyNodeLeaveCallbacks fires the registered node-leave callbacks for
// nodeName, synchronously. The caller must not hold the members lock; see
// processClusterEvent, whose caller invokes this only after the lock is
// released so callbacks cannot deadlock against the routing table.
func (r *RoutingTable) notifyNodeLeaveCallbacks(nodeName string) {
	r.memberCallbackMtx.Lock()
	callbacks := make([]func(string), len(r.leaveCallbacks))
	copy(callbacks, r.leaveCallbacks)
	r.memberCallbackMtx.Unlock()

	r.fireMemberCallbacks(callbacks, nodeName)
}

// notifyNodeJoinCallbacks fires the registered node-join callbacks for
// nodeName under the same execution contract as notifyNodeLeaveCallbacks.
func (r *RoutingTable) notifyNodeJoinCallbacks(nodeName string) {
	r.memberCallbackMtx.Lock()
	callbacks := make([]func(string), len(r.joinCallbacks))
	copy(callbacks, r.joinCallbacks)
	r.memberCallbackMtx.Unlock()

	r.fireMemberCallbacks(callbacks, nodeName)
}

func (r *RoutingTable) fireMemberCallbacks(callbacks []func(string), nodeName string) {
	for _, f := range callbacks {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		f(nodeName)
	}
}

func (r *RoutingTable) runCallbacks() {
	r.callbackMtx.Lock()
	defer r.callbackMtx.Unlock()

	for _, f := range r.callbacks {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		f()
	}
}
