# Modifications from the Original Library

This document lists everything this fork changes relative to the
[upstream repository](https://github.com/olric-data/olric). Please use the original repo for any bugs or questions that
are not specific to the changes described here.

## Table of Contents

* [Fork Scope and Housekeeping](#fork-scope-and-housekeeping)
* [New Features](#new-features)
* [Cluster Membership and Healing](#cluster-membership-and-healing)
* [Data Safety Fixes](#data-safety-fixes)
* [Client and Connection Fixes](#client-and-connection-fixes)
* [Concurrency and Shutdown Fixes](#concurrency-and-shutdown-fixes)
* [Cluster Events](#cluster-events)
* [Configuration Changes](#configuration-changes)

## Fork Scope and Housekeeping

* **Embedded mode only.** Most of the client/server code is still present, apart from the runner.
* **Removed client/server mode.**
* **Renamed the module.**
* **Upgraded Go to 1.26.0.**
* **Reworked the README** for this fork.
* **Fixed several goroutine leaks.**

## New Features

* **Meta-information can be passed to a cluster member.**
* **TLS support.**
* **`Get` responses include the partition ID.**
* **Proactive sync on join (`EnableProactiveSyncOnJoin`).** An opt-in flag (default `false`). When a node joins,
  existing primary owners push data to the new backup owners right away instead of waiting for the next balancer tick.
  The flag does nothing else, and it does not change memberlist probe or gossip timing. Tune `MemberlistConfig` directly
  if you need faster failure detection.
* **Orchestrated deployments (rolling restarts, auto-scaling).** Support for environments where nodes join and leave
  often, such as Kubernetes, Nomad, Docker Swarm and ECS. It combines proactive sync, stable node identity and an
  initial-sync readiness gate, so a new node does not serve traffic before it has received its replica data. See
  [Orchestrated Deployments](README.md#orchestrated-deployments-rolling-restarts-auto-scaling).

## Cluster Membership and Healing

* **Partition healing through a rejoin loop.** The original library calls `memberlist.Join()` once at startup, so a node
  evicted from its membership view during a long partition stays a permanent solo cluster even after the network heals.
  A background rejoin loop now re-queries service discovery and calls `discovery.Join()` whenever the live member count
  drops below quorum, and the existing LWW fragment merge and ownership reports reconcile any diverged state.
  `RejoinInterval` sets the period (default `5s`). The loop runs only when `MemberCountQuorum` is greater than `1`, so
  single-node and cache-only deployments pay nothing for it.
* **Fast failure on dead owners in the embedded scan path.** After a member crashed (`kill -9`, pod crash), an embedded
  `DMap.Scan` could stall for minutes. The iterator's routing table snapshot still named the dead address as a partition
  owner, so every scan pass dialed it. On Kubernetes a deleted pod IP drops packets, so each attempt waited out the full
  dial timeout, and the resulting error aborted the whole scan. The scan path now filters partition owners against live
  memberlist membership before dialing. It skips any owner memberlist has confirmed removed, not merely suspected, so a
  live member that is briefly flapping is never skipped mid-scan, and it reads that data from the promoted replica.
  Crash-recovery scans are bounded by failure detection plus one scan instead of the routing table repair window. See
  [SCAN on DMaps](README.md#scan-on-dmaps).
* **No sync wait on partitions nobody holds data for.** A member could not tell an owned partition that was empty
  everywhere from one whose data was still in flight, so after a membership change every owned-but-empty partition held
  back its rebalance ack for the full `InitialSyncEmptyPartitionTimeout`. When a routing table is installed, the member
  now asks each other owner of its owned-but-empty partitions for their key counts, in one pipelined round trip per
  owner, and waits only on partitions where some owner holds data or could not be answered for. Partitions whose wait
  already elapsed are not asked about again, so a stable cluster issues no extra requests. On an empty cluster a
  departure's epoch completes on the survivors' next balancer tick instead of seconds later.

The changes in the rest of this section, together with the departure-related fixes in the Data Safety, Client and
Connection, Concurrency and Shutdown, Cluster Events and Configuration sections below, converge the cluster quickly
after an abrupt member departure (the olric side of GoAkt
[#1340](https://github.com/Tochemey/goakt/issues/1340)). Their full design — the root causes, all eight fixes, and the
failure-detection and tuning guidance — is in
[Node-left convergence after an abrupt departure](NODE_LEFT_CONVERGENCE_PLAN.md).

* **Routing table pushes are retried, and members pull on a coordinator change.** A push a member rejected or could
  not receive is pushed again with exponential backoff until it lands, the member leaves, a newer table supersedes it or
  the periodic push is due. The epoch gates on every live member the table was computed for, including those the
  fan-out could not reach, which install it through the retry or their own pull and ack then; a completion therefore
  means every live member routes on the new table. The committed payload is stored before the fan-out, and a member
  whose coordinator left pulls the committed table from the new one when no push has landed.
  Before, a push rejected during the window in which a member's memberlist still listed a dead coordinator was left to
  the periodic push, one minute later, and the epoch could not complete meanwhile. Installs are monotonic: a member does
  not install a retried push older than the table it holds, nor a pulled table a push replaced meanwhile, and a pull
  that confirms the installed table keeps the identity of the push that delivered it. Retry rounds push concurrently
  with every attempt bounded by the client timeouts, and every ack buffered before an epoch starts is kept, whether or
  not the fan-out reached the member. Addresses GoAkt issue #1340.
* **A table sequence travels with every push.** Members of the previous version ignore the extra argument. A member that
  missed an intermediate table, and so holds a table whose content recurred, advances its generation and acks when the
  sequence changed. Without it a member that left and rejoined before any data moved never acked the new epoch.
* **An epoch that stays open is explained.** After three balancer intervals the coordinator logs the members whose ack
  it is waiting for, once per interval, until the epoch completes or is superseded.
* **Table computation costs one pipelined round trip per member.** The membership is read from memberlist once per
  computation and the owners are asked for their key counts in one pipelined round trip each, concurrently, instead of
  one round trip per owner per partition; pushes go to 64 members at a time instead of `NumCPU`.

## Data Safety Fixes

* **Backup-to-primary promotion on failover**, which fixes a data-loss race inherited from the original library. When a
  node died, a survivor that had been the backup owner became the primary owner of a partition while the data still sat
  in its backup fragment. The balancer then relocated that sole copy, transferring it and dropping the local one, to the
  newly assigned replica owner. If that node also died moments later, as happens on the second kill of a rolling
  restart, the partitions were destroyed even though the survivor never failed. The balancer now merges such backup
  fragments into the survivor's own primary fragment first, through the standard fragment merge path with
  last-write-wins conflict resolution, so what it later pushes to the new replica owner is a copy rather than the last
  copy. Combine this with `EnableProactiveSyncOnJoin` so the replica copy is re-established promptly after promotion.

* **Re-replication after a departure.** With `EnableProactiveSyncOnJoin` set, the survivors that hold the backup copies of
  a partition whose primary owner left the cluster re-create its primary copy on the new owner once `ReplicaRestoreDelay`
  (default one minute) has passed since the departure, counted per partition, so the partition is back at `ReplicaCount`
  copies without waiting for a write. The balancer tracks the owner list of every partition from one cycle to the
  next and schedules the restore when an owner dropped out of it; when due, the restore is dropped if that owner is
  still a member, since it drained its copy, and sent otherwise, without asking the owner, as the receiver's merge is
  version-aware. The delay keeps a rolling restart from moving data
  twice: a member back within it owns its partitions again and receives them from their previous owner. Without the
  flag the default stays the original library's lazy repair through reads.
* **A survivor's sole backup copy is never relocated.** A backup copy of a partition this member owns as primary is merged
  into its primary fragment by the promotion and, if the promotion fails, stays put; before, the balancer could move it
  to the new replica owner in the same cycle and drop it locally.
* **`Expire` keeps the value on the replicas, and ties favour the primary.** A TTL update replicated the entry with an empty
  value, which a read then served after a timestamp tie. It now carries the current value, and equal timestamps resolve
  in favour of the primary's copy.
* **The expiry scanner reaches the replicas.** It addressed the DMap by the fragment's key in the partition map, so its
  remote deletes went to a DMap that does not exist and the replicas kept every expired key. It also no longer deletes
  under the fragment lock: it samples under it and deletes each key under the key's own lock, after checking the expiry
  again.
* **Storage imports report merge failures.** An import whose merge callback failed answered as if every entry had been
  merged, so the sender recorded the copy as delivered.
* **The proactive push copies whole fragments and retries.** It copies every live table of a fragment, not only the
  first, runs on every balancer cycle once per partition per installed table as soon as the partition holds data, and
  retries a failed push on the next tick. Before, a replica owner that had not installed the table when the push
  arrived, or a partition whose primary copy was moved in later, stayed empty until the next table change.
* **No escape wait on departures.** A partition pending as primary awaits only previous primary owners, whose copy is
  moved right away; a backup copy held elsewhere is restored by the delayed step above, off the convergence path.
  Before, every departure held the node-left epoch for the whole `InitialSyncEmptyPartitionTimeout`.
* **Fragments delivered during a table install are not waited for again.** The sync state records the marks received
  between the install's scan of empty partitions and its reconcile, and the reconcile skips them.

## Client and Connection Fixes

* **Reliable `EmbeddedClient.NewPubSub`.** Without a `ToAddress` option, `NewPubSub()` used to take a connection from
  the node's internal pool, which fills lazily as a side effect of intra-cluster traffic. On a freshly joined member
  that pool was usually still empty, so the call failed intermittently with a confusing `no available client found`
  error, and even when it succeeded the PubSub client was pinned to an arbitrary cluster member and broke when that
  member left. It now targets the local node by default, which is always correct because a `PUBLISH` received by any
  member is relayed to the whole cluster. That removes the startup flakiness: once the `Started` callback fires,
  `NewPubSub()` is guaranteed to succeed. An explicit non-blank `ToAddress` still takes precedence, blank or
  whitespace-only addresses fall back to the local node instead of failing, and calling before the node has joined the
  cluster fails fast with the new `ErrNotJoinedYet` sentinel rather than racing on cluster state or returning a
  misleading error.
* **Retryable failover errors on `ClusterClient`.** When a node leaves, its connection pool is closed while an in-flight
  request may still hold the go-redis client, so the command failed with a raw `redis: client is closed` error that
  callers had no clean way to tell apart from a hard failure. That closed-pool error now surfaces as the retryable
  `ErrConnRefused` sentinel, the same treatment already given to refused and relayed dial errors, so the standard
  refresh-metadata-and-retry path recovers against the promoted owner.
* **The embedded cluster client no longer outlives member shutdown.** An embedded member's `Scan` and `Pipeline` lazily
  create an internal `ClusterClient`, a routing table fetcher goroutine plus connection pools, and nothing ever closed
  it. It was cached per `EmbeddedDMap`, and since `NewDMap` returns a fresh one per call, every `NewDMap().Pipeline()`
  leaked another client, while `Olric.Shutdown` did not know it existed. In any process that stops or restarts a member
  without exiting, such as test binaries and multi-member processes, each orphaned fetcher kept dialing the dead
  member's own address forever and logged `[ERROR] ... connection refused` every minute. The member now owns a single
  shared cluster client, created under a shutdown-gated mutex so a `Scan` racing `Shutdown` can never construct one
  behind the teardown, and `Shutdown` closes it first, while the RESP server it targets is still up. The shared client
  also inherits the member's logger and verbosity instead of writing to a default stderr logger, `Scan` no longer pays a
  members lookup plus a full routing table fetch per call, and `ClusterClient.Close` is idempotent and cancels an
  in-flight routing table fetch instead of stalling teardown for a dial timeout. A routine background refresh failure,
  expected during cluster churn and shutdown windows, is logged at `V(2)` rather than as an unconditional error.

* **Deadlines on every remote call made for a request, and no dialing of departed members.** Replica writes, replica
  and previous-owner reads, read repair, deletes on previous and replica owners and fragment transfers run under the
  request's deadline, or one attempt's worth of the client timeouts when it has none, and the go-redis client honours
  context deadlines at the socket level. Writes, reads and fragment transfers skip an owner memberlist has already removed
  instead of dialing it; deletes, which have no tombstone, fail at once on such an owner and succeed once the routing
  table has dropped it, and a delete of a key the primary owner does not hold still reaches the replicas. A peer that
  accepted connections and never answered, as a departed pod does, used to cost each request about 20 seconds.

## Concurrency and Shutdown Fixes

* **Thread-safe command registration.** The RESP command multiplexer's handler map was read by connection goroutines
  without synchronization while services registered their handlers after the server started accepting connections. A
  read-write mutex now guards registration and lookup, and it is never held while a handler runs.
* **Shutdown-safe background goroutines in the dmap service.** Cluster event publication and async backup writes called
  `wg.Add(1)` directly from request handlers and the balancer, with nothing to gate them against the `wg.Wait()` in
  `Service.Shutdown`. A fragment push or backup write arriving as a member shut down, which is common during relocation
  and backup promotion, could call `Add` concurrently with `Wait`. That is a `sync.WaitGroup` misuse the race detector
  flags, and it can panic during shutdown otherwise. These spawns now go through one guarded helper that takes a mutex
  around `wg.Add`, and `Shutdown` sets a `closed` flag under the same mutex before calling `wg.Wait()`, so `Add` can
  never run concurrently with `Wait`. Work spawned after shutdown has started is dropped, which is safe because the node
  is leaving.
* **The eviction worker treats context cancellation during graceful shutdown as expected**, so it no longer logs
  misleading warnings.
* **Shutdown-safe background goroutines in the routing table.** Cluster event publication and the balancer callbacks
  were started from request handlers and membership events with a bare `wg.Add(1)`, with nothing gating them against
  the `wg.Wait()` in `RoutingTable.Shutdown`, the same `sync.WaitGroup` misuse fixed earlier in the dmap service. A
  routing table push or a membership event arriving as a member shut down could call `Add` concurrently with `Wait`.
  Those spawns now go through one guarded helper, and `Shutdown` flips a `closed` flag under the same mutex before
  waiting, so work arriving after shutdown has begun is dropped.

* **Replication runs outside the fragment lock.** Writes and deletes hold the fragment lock only around their storage
  accesses; a striped per-key lock serializes a key's operations across replication, so a key's writes still reach its
  replicas in the primary's order while a replica that does not answer stalls that key alone. The expiry scanner and the
  LRU eviction delete under the key lock as well, so an eviction cannot remove from the replicas a write of the same key
  that is between its replication and its store, and the janitor leaves a fragment alone while such a write is in
  flight. A read of a fragment under sync-replicated write load went from 603µs to 94µs, parallel replicated writes to
  one fragment from 138µs to 49µs.
* **The balancer acks in the invocation that finished moving.** A cycle that moved data is followed by another one
  right away, up to three per invocation, instead of waiting for the next `TriggerBalancerInterval`.

## Cluster Events

* **Rebalance lifecycle events** (start and complete) are published on the cluster events channel.
* **Rebalance coordination acknowledgements** are tracked, so completion is emitted only after every member acks.
* **Rebalance coordinator mismatch errors** return the `ErrNotCoordinator` sentinel, which cuts log noise during
  coordinator transitions.
* **Deterministic routing table signature.** The signature, which doubles as the rebalance epoch id, was the hash of a
  msgpack-encoded Go map, so an unchanged table hashed differently on almost every push and every periodic push started
  a new epoch, dropping whichever node-join or node-left epoch was still in flight. The table is now encoded in
  ascending partition order, so the signature is a pure function of the table content: pushing an unchanged table
  starts no epoch and publishes no rebalance events, and an epoch started for a membership change stays active until
  every live member acks it or a genuine table change supersedes it. The wire format is unchanged, so members running
  an older version keep decoding pushed tables during a rolling upgrade.
* **Rebalance acks once per installed table.** Because the signature derives from the table content, it recurs when the
  table returns to an earlier state, for example after a member joined and left before any data moved. Keyed on the
  signature, a member that had acked it before skipped the ack and the node-left epoch never completed. The routing
  table now exposes an install generation that advances on every signature change, including a change back to an
  earlier one, and the balancer acks, and runs the proactive primary-to-backup push, once per generation.
* **Epoch identity and membership in cluster events.** `rebalance-start-event` and `rebalance-complete-event` carry
  `generation`, the coordinator's install generation of the pushed table, which unlike `epoch` never recurs on a given
  coordinator; `rebalance-complete-event` also carries `members`, the sorted addresses of the members the coordinator
  knew when it computed the converged table, so a subscriber can tell which joins and departures the table reflects;
  and `node-join-event` and `node-left-event` carry the `generation` the publisher held when it observed the change,
  so on the coordinator every epoch that reflects the change carries a higher one. Generations are comparable only
  between events of the same `source`. A completion published from the epoch start, when every ack had already
  arrived, can no longer carry an earlier timestamp than its own start event, and it is published only after the
  start has been, so subscribers never see a completion before its start.
* **Local subscribers are served first.** The cluster-wide fan-out delivered to local subscribers only when its member
  loop reached the local member and stopped at the first remote failure, so a member that was still starting, which
  rejects or holds requests until it is operable, could delay or lose an event for every subscriber. Local subscribers
  are now served before any remote member, every remote member is attempted, and the first failure is reported after
  the last attempt. The internal publish and the key count probe are served regardless of the node's operability
  precondition, as the routing table push already was: neither touches user data.
* **In-process cluster event publishing.** Cluster events (`node-join-event`, `node-left-event`, rebalance and fragment
  events) were published by each service dialing its own RESP server with a `PUBLISH` command. That cost a loopback TCP
  round trip per event and hard-wired a hidden dependency: the routing table and dmap services assumed the pubsub
  service's command handler was registered on their own server, and logged spurious `ERR unknown command 'publish'`
  errors in any partial assembly, such as a unit-test harness, where it was not. The pubsub service now registers itself
  as the routing table's cluster event publisher at construction time, and both the routing table and the dmap service
  publish through that in-process hook. The loopback round trip is gone and the cluster-wide fan-out is identical: local
  subscribers are served in process, and remote members receive the message through `PUBLISH.INTERNAL`. The user-facing
  `PUBLISH` command path is unchanged. The fan-out is shutdown-aware from both sides, so publishing aborts as soon as
  either the emitting service's context or the pubsub service's context is cancelled, and events emitted during a
  member's teardown window fail fast instead of dialing departed peers. When no publisher is registered, which is only
  possible in partial test setups because a full member always wires one before joining, events are dropped with a debug
  log instead of surfacing bogus errors.

* **One authoritative membership announcement.** The coordinator publishes `membership-change-event` for every join,
  departure and update, with the member set after the change and its generation, before the table is recomputed and
  regardless of whether the table changes or the quorum allows an update.
* **Local observations stay local (breaking).** `node-join-event` and `node-left-event` reach the publishing member's own
  subscribers only, instead of being relayed to every member, which cost `members²` deliveries per change. Consumers that
  relied on other members' copies subscribe to `membership-change-event` instead.

## Configuration Changes

* **LRU eviction config guard (breaking).** `Config.Validate()` rejects LRU setups whose per-partition budget is too
  small for the storage engine: `MaxInuse / PartitionCount` must be at least one `tableSize`, and `MaxKeys` must be at
  least `PartitionCount`. Without that check, a modest `MaxInuse` (256MiB with the default 271 partitions and 1MiB
  tables, for example) let allocated memory grow several times past the configured limit. A config that violates these
  bounds now fails at startup even if it was accepted before. Raise `MaxInuse` or `MaxKeys`, reduce `PartitionCount`, or
  lower `tableSize`.
* **`InitialSyncEmptyPartitionTimeout` bounds a narrower wait.** It is now the maximum time a member waits on an owned
  but empty partition whose other owners hold data or could not be answered for. Partitions no owner holds data for
  are not waited on at all. The default is unchanged at `15s`.
* **`ReplicaRestoreDelay` (default `1m`).** The wait, after the primary owner of a partition left the cluster, before the
  survivors re-create its primary copy from their backups on the new owner, counted per partition from the departure.
  The check runs on the balancer tick, and a negative value is rejected. It applies only with `EnableProactiveSyncOnJoin`
  set and `ReplicaCount` above 1.
