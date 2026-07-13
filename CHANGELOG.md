# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- Embedded `DMap.Scan` no longer stalls for minutes after a member dies hard (`kill -9`, pod crash). The scan path now filters partition owners against live memberlist membership before dialing them, skipping any owner memberlist has **confirmed removed** (not merely suspected, so a flapping-but-alive member is never skipped mid-scan). Previously, a scan touching a partition still owned by the dead member in the iterator's routing-table snapshot kept dialing the dead address until repair converged; on Kubernetes a deleted pod IP drops packets, so every attempt burned the full dial timeout, and the eventual error aborted the whole scan. The dead owner's data is served by the promoted replica, so skipping it is safe and bounds crash-recovery scans by failure detection plus one scan instead of the routing-table repair window. Fixes [#22](https://github.com/Tochemey/olric/issues/22).

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
