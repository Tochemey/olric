# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING:** `Config.Validate()` now rejects LRU eviction configurations whose
  per-partition budget is too small for the storage engine to honor, closing a memory
  footgun where a modest `MaxInuse` could let allocated memory grow several times past
  the configured limit. When `EvictionPolicy` is `LRU`, the validation enforces two
  invariants:
  - `MaxInuse / PartitionCount >= tableSize` — otherwise each partition's in-use cap is
    smaller than a single storage table, so eviction churns tables into garbage faster
    than compaction can reclaim them and allocated memory grows far beyond `MaxInuse`.
  - `MaxKeys >= PartitionCount` — otherwise the per-partition key budget rounds down to
    zero and the cache evicts on every write.

  Both checks apply to the global `DMaps` configuration and to every entry in
  `DMaps.Custom`. A deployment that previously started with such a configuration (while
  silently over-allocating memory) will now fail at startup. To resolve the error,
  increase `MaxInuse`/`MaxKeys`, reduce `PartitionCount`, or lower the storage engine
  `tableSize`.
