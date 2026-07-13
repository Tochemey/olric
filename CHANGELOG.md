# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
