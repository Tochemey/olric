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
	"bytes"
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/tochemey/olric/internal/cluster/partitions"
	"github.com/tochemey/olric/internal/discovery"
	"github.com/tochemey/olric/internal/protocol"
)

const (
	// pushRetryInitialBackoff is the wait before the first attempt to push a
	// routing table again to a member the fan-out could not deliver it to,
	// and pushRetryMaxBackoff caps the doubling of that wait between further
	// attempts. Both are spread by pushRetryJitter, a fraction of the wait,
	// so retries from several members do not align. The retry ends at the
	// latest when the periodic push is due, which pushes to every member.
	pushRetryInitialBackoff = 200 * time.Millisecond
	pushRetryMaxBackoff     = 2 * time.Second
	pushRetryJitter         = 0.2

	// clusterCallConcurrency bounds how many members a table push or a key
	// count probe addresses at once. The calls are network bound, so the
	// bound is a cap on goroutines and connections, not on CPUs: at 300
	// members it is five rounds instead of the forty an eight-core
	// coordinator ran.
	clusterCallConcurrency = 64
)

type leftOverDataReport struct {
	Partitions []uint64
	Backups    []uint64
}

// countQuery asks an owner for its key count in a partition, in its backup
// copy when replica is set.
type countQuery struct {
	partID  uint64
	replica bool
}

// buildRoutingTablePayload encodes the routing table as a msgpack map whose
// entries are written in ascending partition id order, and returns the payload
// together with its signature.
//
// The signature is the xxhash of the payload and doubles as the rebalance
// epoch id, so it must be a pure function of the table content. Members derive
// their signature from the received bytes (see applyRoutingTablePayload) and
// the balancer acks that value, so the payload itself has to be canonical:
// msgpack.Marshal follows Go's randomized map iteration order and yields a
// different signature for the same table on almost every push, which
// supersedes in-flight epochs on a stable cluster. The wire format is still a
// plain msgpack map, so members running an older version keep decoding it.
func (r *RoutingTable) buildRoutingTablePayload() ([]byte, uint64, error) {
	partIDs := make([]uint64, 0, len(r.table))
	for partID := range r.table {
		partIDs = append(partIDs, partID)
	}
	slices.Sort(partIDs)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	if err := enc.EncodeMapLen(len(partIDs)); err != nil {
		return nil, 0, err
	}

	for _, partID := range partIDs {
		if err := enc.EncodeUint64(partID); err != nil {
			return nil, 0, err
		}
		if err := enc.Encode(r.table[partID]); err != nil {
			return nil, 0, err
		}
	}
	data := buf.Bytes()
	return data, xxhash.Sum64(data), nil
}

func (r *RoutingTable) prepareLeftOverDataReport() ([]byte, error) {
	res := leftOverDataReport{}
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		part := r.primary.PartitionByID(partID)
		if part.Length() != 0 {
			res.Partitions = append(res.Partitions, partID)
		}

		backup := r.backup.PartitionByID(partID)
		if backup.Length() != 0 {
			res.Backups = append(res.Backups, partID)
		}
	}
	return msgpack.Marshal(res)
}

// pendingPartition is a partition this member owns but holds no data for,
// together with the role, primary or backup, it awaits that data in. The role
// decides which owners can deliver it, see partitionsAwaitingData.
type pendingPartition struct {
	partID uint64
	kind   partitions.Kind
}

// pendingPartitions returns the partitions this member owns but holds no data
// for, each with the role it awaits data in. A partition owned in both roles
// is reported once, as primary.
func (x *RoutingTable) pendingPartitions() []pendingPartition {
	var out []pendingPartition

	for partID := uint64(0); partID < x.config.PartitionCount; partID++ {
		primaryPart := x.primary.PartitionByID(partID)
		if owners := primaryPart.Owners(); len(owners) > 0 {
			last := owners[len(owners)-1]
			if last.CompareByID(x.this) && primaryPart.Length() == 0 {
				out = append(out, pendingPartition{partID: partID, kind: partitions.PRIMARY})
				continue
			}
		}

		backupPart := x.backup.PartitionByID(partID)
		for _, m := range backupPart.Owners() {
			if m.CompareByID(x.this) && backupPart.Length() == 0 {
				out = append(out, pendingPartition{partID: partID, kind: partitions.BACKUP})
				break
			}
		}
	}

	return out
}

// partitionIDs returns the ids of pending, in order, nil when it is empty.
func partitionIDs(pending []pendingPartition) []uint64 {
	if len(pending) == 0 {
		return nil
	}

	out := make([]uint64, 0, len(pending))
	for _, p := range pending {
		out = append(out, p.partID)
	}

	return out
}

// partitionsPendingReceive returns the ids of the partitions this member owns,
// in either role, but holds no data for.
func (x *RoutingTable) partitionsPendingReceive() []uint64 {
	return partitionIDs(x.pendingPartitions())
}

// partitionsAwaitingData returns the partitions this member has to wait on
// before acking a rebalance: the owned-but-empty partitions reported by
// pendingPartitions, minus those no owner will deliver data for before the
// ack. A partition nobody delivers never receives a fragment, so waiting on
// it only delays the ack by InitialSyncEmptyPartitionTimeout.
//
// Which owners count as a source depends on the role this member awaits data
// in. A partition pending as primary is delivered only by a previous primary
// owner moving its primary fragment, so only the other primary owners are
// asked, for their primary copies. A backup copy held by another survivor
// after the primary owner died is not a source: the balancer restores it to
// the new primary owner only after ReplicaRestoreDelay, off the convergence
// path, and counting it would hold the ack for the whole escape on every
// departure. A partition pending as backup is delivered by the primary owner
// pushing its primary fragment or by a previous backup owner moving its
// backup fragment, so both kinds of owner are asked.
//
// Partitions whose escape deadline has already elapsed are kept as they are:
// the sync state no longer waits on them, so asking their owners would buy
// nothing. Every other owner is asked over the network, once, in a single
// pipelined round trip.
func (x *RoutingTable) partitionsAwaitingData() []uint64 {
	awaiting, _ := x.partitionsAwaitingDataAt()
	return awaiting
}

// partitionsAwaitingDataAt is partitionsAwaitingData together with the time
// the empty partitions were scanned, which the sync state needs: a fragment
// delivered after the scan, while the owners were being asked, must not be
// awaited, see syncstate.State.Reconcile.
func (x *RoutingTable) partitionsAwaitingDataAt() ([]uint64, time.Time) {
	scannedAt := time.Now()
	pending := x.pendingPartitions()
	if len(pending) == 0 || x.syncState == nil {
		return partitionIDs(pending), scannedAt
	}

	awaiting := make(map[uint64]struct{}, len(pending))
	queries := make(map[discovery.Member][]countQuery)

	for _, p := range pending {
		if x.syncState.Expired(p.partID) {
			awaiting[p.partID] = struct{}{}
			continue
		}

		for _, owner := range x.primary.PartitionByID(p.partID).Owners() {
			if !owner.CompareByID(x.this) {
				queries[owner] = append(queries[owner], countQuery{partID: p.partID})
			}
		}

		if p.kind == partitions.PRIMARY {
			continue
		}

		for _, owner := range x.backup.PartitionByID(p.partID).Owners() {
			if !owner.CompareByID(x.this) {
				queries[owner] = append(queries[owner], countQuery{partID: p.partID, replica: true})
			}
		}
	}

	var mtx sync.Mutex
	var g errgroup.Group
	g.SetLimit(clusterCallConcurrency)

	for owner, ownerQueries := range queries {
		g.Go(func() error {
			held := x.partitionsHeldBy(owner, ownerQueries)

			mtx.Lock()
			defer mtx.Unlock()

			for _, partID := range held {
				awaiting[partID] = struct{}{}
			}

			return nil
		})
	}

	// The probes report through awaiting; the group only bounds concurrency.
	_ = g.Wait()

	out := make([]uint64, 0, len(awaiting))
	for _, p := range pending {
		if _, ok := awaiting[p.partID]; ok {
			out = append(out, p.partID)
		}
	}

	if skipped := len(pending) - len(out); skipped > 0 {
		x.log.V(6).Printf("[DEBUG] %d of %d empty partitions have no live source and are not awaited", skipped, len(pending))
	}

	if len(out) > 0 {
		x.log.V(6).Printf("[DEBUG] Awaiting data for partitions %v", out)
	}

	return out, scannedAt
}

// keyCounts asks owner, in one pipelined round trip, for its key counts in the
// queried partitions. The count of a query that was not answered is -1, and
// the returned error is the round trip's own failure, if any.
func (x *RoutingTable) keyCounts(owner discovery.Member, queries []countQuery) ([]int64, error) {
	rc := x.client.Get(owner.String())
	pipe := rc.Pipeline()
	cmds := make([]*redis.IntCmd, len(queries))

	for i, query := range queries {
		lengthOfPart := protocol.NewLengthOfPart(query.partID)
		if query.replica {
			lengthOfPart.SetReplica()
		}

		cmds[i] = lengthOfPart.Command(x.ctx)
		_ = pipe.Process(x.ctx, cmds[i])
	}

	_, err := pipe.Exec(x.ctx)
	if err != nil {
		x.log.V(6).Printf("[DEBUG] Failed to check key counts on %s: %v", owner, err)
	}

	counts := make([]int64, len(queries))
	for i := range queries {
		count, cmdErr := cmds[i].Result()
		if cmdErr != nil {
			counts[i] = -1
			continue
		}

		counts[i] = count
	}

	return counts, err
}

// partitionsHeldBy asks owner for its key counts in the queried partitions and
// returns the ids of those it holds data for. A query that is not answered
// counts as held: memberlist removes a dead owner, and until then its data may
// still arrive, so the escape deadline keeps covering the partition.
func (x *RoutingTable) partitionsHeldBy(owner discovery.Member, queries []countQuery) []uint64 {
	counts, _ := x.keyCounts(owner, queries)

	// Left nil on purpose: on an empty cluster nothing is held, so nothing
	// needs to be allocated.
	var held []uint64

	for i, query := range queries {
		if counts[i] != 0 {
			held = append(held, query.partID)
		}
	}

	return held
}

// pushContext bounds one push of the routing table to one member by the
// client's dial, write and read timeouts together, so that a member that
// drops packets costs one bounded attempt instead of the client's retry
// chain, and a retry round cannot outlive its deadline by that chain.
func (x *RoutingTable) pushContext() (context.Context, context.CancelFunc) {
	c := x.config.Client
	return context.WithTimeout(x.ctx, c.DialTimeout+c.WriteTimeout+c.ReadTimeout)
}

// updateRoutingTableOnMember pushes data, the table this coordinator computed
// as sequence, to member within ctx and returns its left-over data report.
func (x *RoutingTable) updateRoutingTableOnMember(ctx context.Context, data []byte, sequence uint64, member discovery.Member) (*leftOverDataReport, error) {
	cmd := protocol.NewUpdateRouting(data, x.this.ID, sequence).Command(ctx)
	rc := x.client.Get(member.String())
	err := rc.Process(ctx, cmd)
	if err != nil {
		return nil, err
	}

	result, err := cmd.Bytes()
	if err != nil {
		return nil, err
	}

	report := leftOverDataReport{}
	err = msgpack.Unmarshal(result, &report)
	if err != nil {
		x.log.V(3).Printf("[ERROR] Failed to call decode ownership report from %s: %v", member, err)
		return nil, err
	}
	return &report, nil
}

// fetchRoutingTableFromCoordinator pulls the committed routing table from the
// cluster coordinator and applies it. With onlyBootstrap set it is applied
// only if this node is not bootstrapped yet, which is the delivery guarantee
// behind the push in updateRoutingTableOnCluster; without it the pulled table
// replaces the installed one, which a member does after its coordinator left.
func (x *RoutingTable) fetchRoutingTableFromCoordinator(onlyBootstrap bool) error {
	coordinator := x.discovery.GetCoordinator()
	if coordinator.ID == 0 {
		return ErrNotJoinedYet
	}

	if coordinator.CompareByID(x.this) {
		// The coordinator bootstraps itself.
		return nil
	}

	// A pull that replaces the installed table applies only if no push
	// installed another one while the answer was in flight.
	var expected *tableVersion
	if !onlyBootstrap {
		expected = x.version.Load()
	}

	cmd := protocol.NewFetchRouting(x.this.ID).Command(x.ctx)
	rc := x.client.Get(coordinator.String())
	if err := rc.Process(x.ctx, cmd); err != nil {
		return err
	}
	payload, err := cmd.Bytes()
	if err != nil {
		return err
	}

	_, err = x.applyRoutingTablePayload(payload, coordinator.ID, 0, onlyBootstrap, expected)
	return err
}

// updateRoutingTableOnCluster pushes data, an encoded routing table, to every
// member. It returns the left-over data reports of the members that installed
// it and the members it could not be delivered to, which the caller pushes to
// again, see retryRoutingTablePush. Only a failure to install the table on
// this node is an error: the coordinator must apply its own table, otherwise
// the committed epoch would reference a table it never installed.
func (x *RoutingTable) updateRoutingTableOnCluster(data []byte, sequence uint64) (map[discovery.Member]*leftOverDataReport, []discovery.Member, error) {
	var mtx sync.Mutex
	var g errgroup.Group
	var unreachable []discovery.Member
	reports := make(map[discovery.Member]*leftOverDataReport)
	sem := semaphore.NewWeighted(clusterCallConcurrency)

	x.Members().RLock()
	x.Members().Range(func(id uint64, tmp discovery.Member) bool {
		member := tmp
		g.Go(func() error {
			if err := sem.Acquire(x.ctx, 1); err != nil {
				x.log.V(3).Printf("[ERROR] Failed to acquire semaphore to update routing table on %s: %v", member, err)
				return err
			}
			defer sem.Release(1)

			ctx, cancel := x.pushContext()
			defer cancel()

			report, err := x.updateRoutingTableOnMember(ctx, data, sequence, member)
			if err != nil {
				if member.CompareByID(x.this) {
					return err
				}
				// An unreachable member must not block the commit for the
				// rest of the cluster.
				mtx.Lock()
				unreachable = append(unreachable, member)
				mtx.Unlock()
				return nil
			}

			mtx.Lock()
			defer mtx.Unlock()
			reports[member] = report
			return nil
		})
		return true
	})
	x.Members().RUnlock()

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	if len(unreachable) > 0 {
		x.log.V(2).Printf("[WARN] Routing table could not be pushed to %d member(s): %v. "+
			"Pushing again with backoff", len(unreachable), unreachable)
	}

	return reports, unreachable, nil
}

// jitter spreads d by pushRetryJitter, so that retries started at the same
// moment on several members do not stay aligned.
func jitter(d time.Duration) time.Duration {
	span := int64(float64(d) * pushRetryJitter)
	if span <= 0 {
		return d
	}

	return d - time.Duration(span/2) + time.Duration(rand.Int64N(span+1))
}

// isMember reports whether member is still in the routing table's member set.
func (x *RoutingTable) isMember(member discovery.Member) bool {
	x.Members().RLock()
	defer x.Members().RUnlock()

	_, err := x.Members().Get(member.ID)
	return err == nil
}

// retryRoutingTablePush pushes data, the routing table committed with
// signature, again to members the fan-out could not deliver it to, with
// exponential backoff and jitter. A member is dropped once the push lands or
// memberlist removes it. The retry ends when every member is dropped, when a
// table with another signature is installed, since that table's own fan-out
// and retry take over, when the periodic push is due, since it pushes to
// every member anyway, or when the routing table shuts down. A member that
// receives the table late joins the epoch through admitLatePush. A retry that
// outlives one balancer interval is logged at WARN.
func (x *RoutingTable) retryRoutingTablePush(data []byte, sequence, signature uint64, members []discovery.Member) {
	started := time.Now()
	deadline := started.Add(x.pushPeriod)
	backoff := pushRetryInitialBackoff
	remaining := members
	timer := time.NewTimer(jitter(backoff))
	defer timer.Stop()

	for attempt := 1; len(remaining) > 0; attempt++ {
		select {
		case <-x.ctx.Done():
			return
		case <-timer.C:
		}

		if x.Signature() != signature || time.Now().After(deadline) {
			return
		}

		// The members of a round are pushed to concurrently, each attempt
		// bounded by pushContext, so a round of unreachable members costs
		// one bounded attempt and not their sum; the deadline is checked
		// before every attempt is started.
		var (
			mtx    sync.Mutex
			failed []discovery.Member
			g      errgroup.Group
		)
		g.SetLimit(clusterCallConcurrency)
		stopped := false

		for _, member := range remaining {
			if x.Signature() != signature || time.Now().After(deadline) {
				stopped = true
				break
			}

			if !x.isMember(member) {
				continue
			}

			g.Go(func() error {
				ctx, cancel := x.pushContext()
				defer cancel()

				report, err := x.updateRoutingTableOnMember(ctx, data, sequence, member)
				if err != nil {
					mtx.Lock()
					failed = append(failed, member)
					mtx.Unlock()
					return nil
				}

				x.log.V(2).Printf("[INFO] Routing table pushed to %s on retry %d", member, attempt)
				x.admitLatePush(signature, member, report)
				return nil
			})
		}

		_ = g.Wait()

		if stopped {
			return
		}

		remaining = failed
		if len(remaining) > 0 && time.Since(started) > x.config.TriggerBalancerInterval {
			x.log.V(2).Printf("[WARN] Routing table still not pushed to %v after %d attempt(s) over %s",
				remaining, attempt, time.Since(started).Round(time.Millisecond))
		}

		backoff = min(2*backoff, pushRetryMaxBackoff)
		timer.Reset(jitter(backoff))
	}
}

// admitLatePush accounts for a member that installed the table committed with
// signature after the fan-out: its left-over data report is applied while that
// table is still the installed one, and it is in the pending set of the epoch
// started for that table, which gates on every live member, so the completion
// waits for its ack; a member that joined after the table was computed is
// added. An ack it sent before was recorded by handleRebalanceAck and counts
// right away.
func (x *RoutingTable) admitLatePush(signature uint64, member discovery.Member, report *leftOverDataReport) {
	x.Lock()

	if x.Signature() == signature {
		x.processLeftOverDataReports(map[discovery.Member]*leftOverDataReport{member: report})
	}

	x.Unlock()

	x.rebalanceMtx.Lock()
	defer x.rebalanceMtx.Unlock()

	if x.rebalanceState.epoch != signature || x.rebalanceState.completed {
		return
	}

	x.rebalanceState.pending[member.ID] = struct{}{}
	x.checkRebalanceCompletionLocked()
}

// pullRoutingTableAfterCoordinatorChange pulls the committed table from the
// new coordinator after the coordinator that pushed the installed table left,
// with the same backoff as retryRoutingTablePush. It is the anti-entropy half
// of table delivery: the new coordinator's push normally lands within its
// first wait, in which case the installed signature has changed and nothing
// is pulled. It ends when a pull succeeds, when a push lands meanwhile, when
// this member has become the coordinator, when the periodic push is due, or
// when the routing table shuts down.
func (x *RoutingTable) pullRoutingTableAfterCoordinatorChange() {
	signature := x.Signature()
	deadline := time.Now().Add(x.pushPeriod)
	backoff := pushRetryInitialBackoff
	timer := time.NewTimer(jitter(backoff))
	defer timer.Stop()

	for attempt := 1; ; attempt++ {
		select {
		case <-x.ctx.Done():
			return
		case <-timer.C:
		}

		if x.Signature() != signature || time.Now().After(deadline) || x.discovery.IsCoordinator() {
			return
		}

		if current := x.version.Load(); current != nil && current.coordinator == x.discovery.GetCoordinator().ID {
			// The new coordinator's push landed: the installed table is its.
			return
		}

		err := x.fetchRoutingTableFromCoordinator(false)
		if err == nil {
			x.log.V(2).Printf("[INFO] Routing table pulled from the new coordinator %s on attempt %d", x.discovery.GetCoordinator(), attempt)
			return
		}

		x.log.V(3).Printf("[WARN] Failed to pull the routing table from the new coordinator on attempt %d: %v", attempt, err)
		backoff = min(2*backoff, pushRetryMaxBackoff)
		timer.Reset(jitter(backoff))
	}
}
