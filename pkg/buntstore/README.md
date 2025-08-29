# BuntStore (BuntDB-backed storage engine)

BuntStore is an in-memory storage engine for Olric built on top of BuntDB.
It implements `github.com/tochemey/olric/pkg/storage.Engine` and is a drop-in
alternative to the default GC-friendly `kvstore` engine.

## Why BuntStore

- Pure in-memory operation using BuntDB.
- Efficient scans and regex filtering using a lightweight key index.
- Lower write amplification on reads by separating last-access metadata.
- Chunked transfer for predictable, low-latency migrations.

## Implementation Highlights

BuntStore stores data in three namespaced key spaces inside a single BuntDB
instance:

- `v:<hkey>`: Encoded `storage.Entry` payload
- `a:<hkey>`: 8-byte big-endian last-access timestamp (nanoseconds)
- `k:<key>`: Reverse index mapping from real key to hashed key (`hkey` as a
  zero-padded decimal string)

This layout enables two key optimizations:

1) Last-Access Without Rewriting Values
   - `Get` and `Scan` no longer rewrite the full encoded value to update the
     last-access field. Instead, they update only the tiny `a:<hkey>` record.

2) Fast Regex Scans Over Keys
   - `ScanRegexMatch` iterates `k:*` (real keys) and filters with your regex,
     fetching only the matching `v:<hkey>` entries. This avoids decoding every
     value during regex filtering.

Additionally, migration is optimized:

- Chunked TransferIterator: `TransferIterator()` exports only `v:*` keys in
  fixed-size chunks (configurable), and `Drop(index)` deletes that chunk’s
  `v:<hkey>`, `a:<hkey>`, and `k:<key>` records.
- Compact transfer encoding via MsgPack for reduced CPU and payload size.

## Usage

You can select BuntStore via Olric’s configuration or use it directly.

### Select via Config (recommended)

If you don’t set `DMaps.Engine.Implementation`, Olric picks the engine based
on `DMaps.Engine.Name`. Set it to "buntdb":

```go
import (
    oconfig "github.com/tochemey/olric/config"
)

cfg := oconfig.NewConfig()
// ... your cluster/network setup ...
cfg.DMaps.Engine.Name = "buntdb" // use BuntStore instead of the default kvstore
if err := cfg.DMaps.Sanitize(); err != nil { /* handle */ }
if err := cfg.DMaps.Validate(); err != nil { /* handle */ }
```

### Instantiate Programmatically

```go
import (
    "github.com/tochemey/olric/pkg/buntstore"
)

eng, _ := buntstore.New(nil) // or buntstore.DefaultConfig()
_ = eng.Start()
// use eng as a storage.Engine
```

## Configuration

BuntStore accepts a `*storage.Config`. The following key is recognized:

- `transferChunkSize` (int): Number of entries per export chunk in the transfer
  iterator. Default: 50000.
- `enableKeyIndex` (bool): Controls the `k:<key>` reverse index which powers
  fast regex scans. Default: true. Set to false to reduce write work and
  allocations if you never use `ScanRegexMatch`.

Example:

```go
c := buntstore.DefaultConfig()
c.Add("transferChunkSize", 100000)
c.Add("enableKeyIndex", false) // optional: skip k:<key> index writes
eng, _ := buntstore.New(c)
```

## Behavior & Compatibility

- Fully implements `storage.Engine`.
- `Fork` returns a ready-to-use (started) child engine.
- `Stats` exposes `Length` and `NumTables=1` (BuntDB doesn’t expose accurate RAM usage).
- Purely in-memory: no persistence settings are toggled by BuntStore.
- Entries are encoded/decoded using the same `internal/kvstore/entry` format as
  the default engine.

## Performance Notes

- Last-access updates are best-effort and kept tiny by writing only `a:<hkey>`.
- Regex scans use the `k:` index and fetch only matching entries.
- Transfers are chunked and encoded with MsgPack to minimize spikes.
- Hot-path allocations are minimized:
  - Key construction avoids fmt and uses stack-backed builders for
    `v:<hkey>`/`a:<hkey>` and the zero-padded `hkey` string.
  - Last-access writes use a fixed-size stack buffer.
  - You can disable the key index (`enableKeyIndex=false`) to drop one write per
    Put/Delete when regex scans aren’t needed.

## Tests & Benchmarks

- Unit tests cover functionality and edge cases (errors, cursor behavior,
  import/export, update paths, etc.).
- Benchmarks include Put, Get, Scan, and ScanRegexMatch.

Run:

```sh
go test ./pkg/buntstore
# or
go test -run '^$' -bench . ./pkg/buntstore
```
