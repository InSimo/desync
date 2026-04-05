# Cache Size Limit

desync's `Cache` (read-through local + remote store) can be configured to
automatically evict old entries when the total stored data exceeds a size or
file count limit. Eviction is automatic and requires no manual maintenance.

## Configuration

| Setting | CLI flag | Config key | Env var | Default |
|---------|----------|------------|---------|---------|
| Max size (bytes) | `--cache-max-size` | `cache-max-size` | `DESYNC_CACHE_MAX_SIZE` | 0 (unlimited) |
| Max files | `--cache-max-files` | `cache-max-files` | `DESYNC_CACHE_MAX_FILES` | 0 (unlimited) |
| Partitions | `--cache-partitions` | `cache-partitions` | — | 256 |

Size accepts human-friendly suffixes: `10G`, `500M`, `1T`, `100K`.

When both limits are 0 (the default), no tracking file is created and the
cache behaves exactly as before.

## Reference implementations

The design was informed by two existing projects with cache size management:

| Aspect | ccache | fastbuild |
|--------|--------|-----------|
| **Size tracking** | Per-subdirectory stats files (16 L1 dirs), updated incrementally | No persistent tracking; full scan on manual trim |
| **Eviction policy** | Approximate LRU via file mtime | LRU via file write-time (not updated on reads) |
| **Access tracking** | Updates mtime on cache hits | None |
| **Trigger** | Automatic after each write; cleans one L2 subdirectory (amortized) | Manual only (`-cachetrim`) |
| **Concurrency** | File-based locks; non-blocking eviction lock | Atomic writes (temp+rename); no eviction locks |
| **Eviction target** | File-count-based: trim to `0.9 * total_files / 256` | Delete oldest until target byte size reached |

No existing Go library fits the requirements (disk-based, size-limited,
auto-eviction with cross-process safety). Libraries like ristretto,
golang-lru, and freecache are memory-only; diskv is disk-based but has no
eviction.

**Conclusion:** ccache's approach is the right model — automatic, amortized,
LRU-based, concurrency-safe. We adopt its core algorithm but replace
file-locked stats with lock-free mmap'd atomic counters.

## Design

### SizeLimitStore wrapper

`SizeLimitStore` wraps `LocalStore` (following the existing
`WriteDedupQueue` and `RepairableCache` wrapper patterns) and adds:

1. **mtime touch on read** — `GetChunk` calls `os.Chtimes` on the chunk
   file after a successful read, making recently-accessed chunks sort last
   during eviction.

2. **Incremental size tracking** — a persistent mmap'd file
   (`Base/.cache-sizes`) with atomic counters tracks total bytes, total
   files, and per-partition breakdowns. Updates are lock-free
   (`atomic.AddInt64`).

3. **Automatic eviction** — after each `StoreChunk`, if the total exceeds
   the configured limit, the partition with the most files is cleaned.

4. **Stale temp file cleanup** — during eviction directory scans, orphaned
   `.tmp-cacnk*` files older than 1 hour are deleted. These can only exist
   if a process crashed between creating the temp file and renaming it
   (the chunk data is fully in memory before the temp file is created, so
   slow network transfers are not a concern).

### Why mmap + atomics instead of file-locked stats?

With git-lfs-desync and git-lfs-transfer, many concurrent processes write
to the same cache. The `bytelimit` package already solves cross-process
coordination via mmap'd shared memory with atomic operations:

- **Lock-free** — `atomic.AddInt64` vs file lock + read + write + unlock
- **Single file** — one ~12 KB mmap'd file instead of many stats files
- **Zero explicit I/O** — the OS handles flushing mmap'd pages to disk

**Key difference from bytelimit**: the mmap file must be **persistent on
disk** (in the cache directory), not ephemeral. On Unix this means
`mmap(MAP_SHARED)` on a regular file. On Windows this means
`CreateFileMappingW` with a real file handle (not `INVALID_HANDLE_VALUE`).

### Partitions

ccache uses a two-level structure: 16 L1 directories (file-locked stats)
and 256 L2 directories (eviction granularity). Since we use mmap atomics
(no locks), the two-level split is unnecessary. We use a single level with
a configurable partition count (default 256, must be a power of 2).

desync's 65536 subdirectories (4-hex-char prefixes) are grouped into
partitions: `partition = prefixIndex / (65536 / P)`. With P=256, each
partition covers 256 subdirectories.

### Eviction algorithm (matching ccache)

ccache's automatic eviction uses `max_size` (bytes) only as a **trigger**.
The actual eviction target is **file-count-based**: trim the chosen
partition down to `0.9 * total_files / P`. This avoids pathological cases
where a few large files cause excessive deletion of small files, and
amortizes scan cost by removing multiple files per eviction.

The algorithm:

1. **Trigger check** (O(1)): `totalSize > maxSize` or `totalFiles > maxFiles`
2. **Eviction lock** (non-blocking CAS): skip if another process is
   already evicting. Dead holders detected via `processAlive()`.
3. **Partition selection**: pick the partition with the highest file count.
4. **Directory scan**: iterate subdirectories in the partition, skipping
   those where the bitmask bit is unset (empty directories).
5. **Sort by mtime** ascending (oldest first).
6. **Delete** until `partitionFiles <= 0.9 * totalFiles / P`.
7. **Update counters**: decrement globalSize, globalFiles, partitionSize,
   partitionFiles for each deletion.

### Subdirectory presence bitmask

To avoid `os.ReadDir` calls on empty subdirectories during eviction, the
mmap file includes a 65536-bit bitmask (8 KB). Bit `i` is set when
`StoreChunk` creates a file in subdirectory `i`, and cleared during
eviction when a directory is found empty. Stale set bits are harmless
(one extra empty readdir).

The bitmask uses `atomic.OrUint32` / `atomic.AndUint32` /
`atomic.LoadUint32` on uint32 words (Go 1.23+).

## Mmap file layout

File: `Base/.cache-sizes` (~12 KB with P=256)

```
Header (24 bytes):
  [0]   int32   eviction lock (0 = free, PID = in-progress)
  [4]   int32   partition count P
  [8]   int64   total size in bytes
  [16]  int64   total file count

Subdirectory presence bitmask (8192 bytes):
  [24]  65536 bits (2048 × uint32), bit i = subdir i may have files

Per-partition counters (P × 16 bytes):
  [8216 + p*16]      int64   partition byte size
  [8216 + p*16 + 8]  int64   partition file count

Total = 8216 + P × 16
```

The file is invisible to `ListChunks()` and `Verify()` because they filter
by chunk file extensions.

### Counter updates per operation

| Operation | globalSize | globalFiles | partitionSize | partitionFiles |
|-----------|-----------|-------------|--------------|----------------|
| StoreChunk (new file) | +newSize | +1 | +newSize | +1 |
| StoreChunk (overwrite) | +(new-old) | 0 | +(new-old) | 0 |
| RemoveChunk | -size | -1 | -size | -1 |
| Eviction delete | -size | -1 | -size | -1 |

### Cold start

When the mmap file is first created (or the partition count changes),
`initCounters()` scans existing cache content:

- `os.ReadDir(Base)` lists only existing subdirectories (empty cache = instant).
- For each valid 4-hex-char subdirectory: count `.cacnk` files and sum sizes.
- Populate partition counters, global counters, and bitmask.

This is O(N) in the number of cached files but happens only once per cache
lifetime (the file persists across process restarts).

## Files

| File | Purpose |
|------|---------|
| `sizelimit.go` | `SizeLimitStore` type: counters, eviction, mtime touch |
| `sizelimit_mmap_unix.go` | Unix persistent file-backed mmap |
| `sizelimit_mmap_windows.go` | Windows persistent file-backed mmap |
| `sizelimit_test.go` | Unit tests |
| `cmd/shared/cmdshared/config.go` | `CacheMaxSize`, `CacheMaxFiles`, `CachePartitions` config + resolvers |
| `cmd/shared/cmdshared/options.go` | `--cache-max-size`, `--cache-max-files`, `--cache-partitions` CLI flags |
| `cmd/shared/cmdshared/store.go` | `MultiStoreWithCache` wraps `LocalStore` in `SizeLimitStore` |

## Edge cases

- **Both limits 0**: no mmap file, no eviction, no mtime updates — backward compatible.
- **Cold start**: one-time scan; cost proportional to actual cache content.
- **Partition count change**: detected via stored header; triggers recreate + rescan.
- **Counter drift** (external file deletion): counters may go slightly negative; clamped during eviction.
- **Crash during eviction**: dead PID in eviction lock detected and reclaimed by next process.
- **File deleted while being read**: safe on Unix (open fd keeps inode); on Windows `os.Remove` fails for open files and is silently skipped.
- **mmap file wrong size or corrupt**: recreated and rescanned.
