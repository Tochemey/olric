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

## Data Safety Fixes

* **Backup-to-primary promotion on failover**, which fixes a data-loss race inherited from the original library. When a
  node died, a survivor that had been the backup owner became the primary owner of a partition while the data still sat
  in its backup fragment. The balancer then relocated that sole copy, transferring it and dropping the local one, to the
  newly assigned replica owner. If that node also died moments later, as happens on the second kill of a rolling
  restart, the partitions were destroyed even though the survivor never failed. The balancer now merges such backup
  fragments into the survivor's own primary fragment first, through the standard fragment merge path with
  last-write-wins conflict resolution, so what it later pushes to the new replica owner is a copy rather than the last
  copy. Combine this with `EnableProactiveSyncOnJoin` so the replica copy is re-established promptly after promotion.

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

## Cluster Events

* **Rebalance lifecycle events** (start and complete) are published on the cluster events channel.
* **Rebalance coordination acknowledgements** are tracked, so completion is emitted only after every member acks.
* **Rebalance coordinator mismatch errors** return the `ErrNotCoordinator` sentinel, which cuts log noise during
  coordinator transitions.
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

## Configuration Changes

* **LRU eviction config guard (breaking).** `Config.Validate()` rejects LRU setups whose per-partition budget is too
  small for the storage engine: `MaxInuse / PartitionCount` must be at least one `tableSize`, and `MaxKeys` must be at
  least `PartitionCount`. Without that check, a modest `MaxInuse` (256MiB with the default 271 partitions and 1MiB
  tables, for example) let allocated memory grow several times past the configured limit. A config that violates these
  bounds now fails at startup even if it was accepted before. Raise `MaxInuse` or `MaxKeys`, reduce `PartitionCount`, or
  lower `tableSize`.
