# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Routing table pushes no longer start a new rebalance epoch when the table is unchanged. The routing table signature, which doubles as the rebalance epoch id, was the xxhash of `msgpack.Marshal(map[uint64]*route)`, and msgpack writes map entries in Go's randomized iteration order, so an unchanged table produced a different signature on almost every encoding. Because `updateRoutingWithReason` starts a new epoch whenever the signature differs from the previous one, nearly every periodic push (`RoutingTablePushInterval`) replaced the active epoch on a stable cluster: an epoch started for a node join or a node leave that was still waiting for acks was silently dropped and never completed, members that acked it afterwards were told the ack was stale, and `cluster.events` subscribers saw a `rebalance-start-event` with reason `periodic` followed by its `rebalance-complete-event` on every push. Consumers that gate on a specific epoch, such as GoAkt's `NodeJoined` and `NodeLeft` events, only fired through their fallback timeout whenever a member could not ack within one push interval. The payload is now encoded in ascending partition id order, so identical tables yield identical bytes and the same signature on the coordinator and on every member. The wire format is still a plain msgpack map, so members running an older version keep decoding it during a rolling upgrade. Pushing an unchanged table now starts no epoch and publishes no rebalance events, and an epoch started for a membership change stays active until every live member acks it or a genuine table change supersedes it. Fixes [#40](https://github.com/Tochemey/olric/issues/40).

## v0.3.18 - 2026-07-24

### Fixed

- `RoutingTable.Start()` no longer waits on member-count quorum passively. When `MemberCountQuorum` is greater than 1 and this node bootstraps alone (its only configured peer, or the `ServiceDiscovery` backend's result, was unresolvable at `Join()` time), the quorum gate relied solely on memberlist gossip surfacing a peer, and could block for the full one-hour timeout — or forever, in practice, if no other node ever dialed in first — even though the peer became resolvable moments later. The rejoin loop, which re-resolves peers and dials them every `RejoinInterval`, is the mechanism that recovers from exactly this state, but it was started *after* the gate it needed to unblock, so it could never help during a cold start. It now starts before the gate, so a node leaves the gate as soon as a peer becomes reachable through gossip, static `Peers`, or `ServiceDiscovery`, instead of only waiting to be found. The loop remains a no-op while quorum is satisfied and is still only started when `MemberCountQuorum` is greater than 1, so the default single-node case issues no extra `Join()` calls and is otherwise unchanged.
- `DMap.Delete(ctx, keys...)` no longer silently drops keys owned by remote partition owners past the first one. `deleteKeys` groups the requested keys by partition owner, then looped over that map to delete each group; the remote-owner branch returned unconditionally after handling the first remote owner (on both success and error), so any additional remote owners in the map were never processed and no error was returned. On a multi-node cluster, a multi-key delete spanning more than one remote owner beyond the first left the keys on every owner after that one untouched. The fan-out now uses `errgroup`, matching the pattern already used by `deleteBackupOnCluster`: every owner, local and remote, is processed, and the first error (if any) is returned only after all owners have been attempted; the returned count reflects the keys actually processed across every owner.

## v0.3.15 - 2026-07-13

### Fixed

- Fixed a `sync.WaitGroup` misuse in the dmap service where cluster-event publication and async backup writes called `wg.Add(1)` from untracked goroutines (request handlers, the balancer) with no gate against the concurrent `wg.Wait()` in `Service.Shutdown`. A fragment push or backup write arriving as a member shut down (common during relocation and backup promotion) could call `Add` concurrently with `Wait`, which the race detector flags and which can, outside `-race` builds, panic with a `WaitGroup` counter misuse. All such spawns now route through a single `spawn` helper that takes a mutex around `wg.Add`, and `Shutdown` sets a `closed` flag under the same mutex before `wg.Wait()`, so `Add` can never run concurrently with `Wait`. Work dropped once shutdown has begun is safe because the node is leaving. Fixes [#24](https://github.com/Tochemey/olric/issues/24).
- `ClusterClient` operations (`Get`, `Put`, and so on) no longer fail with a raw `redis: client is closed` error during a member failover. When a node leaves, its connection pool is closed while an in-flight request may still hold the go-redis client, so that command now surfaces as the retryable `ErrConnRefused` sentinel, like a refused dial, so callers refresh metadata and retry against the promoted owner instead of getting a hard error. This removes a flake in `TestIntegration_Kill_Nodes_During_Operation`.
- Embedded `DMap.Scan` no longer stalls for minutes after a member crashes (`kill -9`, pod crash). The scan path now filters partition owners against live memberlist membership before dialing them, skipping any owner that memberlist has confirmed removed (not merely suspected, so a live member that is briefly flapping is never skipped mid-scan). Previously, a scan touching a partition still owned by the dead member in the iterator's routing-table snapshot kept dialing the dead address until repair converged. On Kubernetes a deleted pod IP drops packets, so every attempt waited out the full dial timeout, and the resulting error aborted the whole scan. The dead owner's data is served by the promoted replica, so skipping it is safe and bounds crash-recovery scans by failure detection plus one scan instead of the routing-table repair window. Fixes [#22](https://github.com/Tochemey/olric/issues/22).

## v0.3.11 - 2026-07-04

### Added

- New `ErrNotJoinedYet` sentinel error, returned by operations that depend on cluster membership when the local node has not completed its cluster join yet.

### Changed

- **BREAKING:** `Config.Validate()` now rejects LRU eviction configurations whose per-partition budget is too small for the storage engine to honor, closing a memory footgun where a modest `MaxInuse` could let allocated memory grow several times past the configured limit. When `EvictionPolicy` is `LRU`, the validation enforces two invariants:
  - `MaxInuse / PartitionCount >= tableSize` — otherwise each partition's in-use cap is smaller than a single storage table, so eviction churns tables into garbage faster than compaction can reclaim them and allocated memory grows far beyond `MaxInuse`.
  - `MaxKeys >= PartitionCount` — otherwise the per-partition key budget rounds down to zero and the cache evicts on every write.

  Both checks apply to the global `DMaps` configuration and to every entry in `DMaps.Custom`. A deployment that previously started with such a configuration (while silently over-allocating memory) will now fail at startup. To resolve the error, increase `MaxInuse`/`MaxKeys`, reduce `PartitionCount`, or lower the storage engine `tableSize`.

### Fixed

- `EmbeddedClient.NewPubSub()` without a `ToAddress` option now reliably targets the local node instead of picking a connection from the node's internal pool. The pool is populated lazily as a side effect of intra-cluster traffic, so on a freshly joined member it was usually still empty and the call failed intermittently with a confusing `no available client found` error; even when it succeeded, the PubSub client was silently pinned to an arbitrary cluster member whose departure would break it. Targeting the local node is always correct because a `PUBLISH` received by any member is relayed to the whole cluster. This prevents the startup flakiness entirely: once the `Started` callback fires, `NewPubSub()` is guaranteed to succeed. An explicit non-blank `ToAddress` still takes precedence, blank or whitespace-only addresses fall back to the local node instead of failing, and calling before the node has joined the cluster fails fast with `ErrNotJoinedYet` instead of racing on cluster state or returning a misleading error. `ClusterClient.NewPubSub()` behavior is unchanged.
