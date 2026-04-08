# Cache Size Limit

desync's `Cache` (read-through local + remote store) can be configured to
automatically evict old entries when the total stored data exceeds a size or
file count limit. Eviction is automatic and requires no manual maintenance.

The eviction logic is implemented in the
[cacheevict](https://github.com/insimo/cacheevict) module, which is
reusable by other projects (e.g. git-lfs) without depending on desync.

## Configuration

| Setting | CLI flag | Config key | Env var | Default |
|---------|----------|------------|---------|---------|
| Max size (bytes) | `--cache-max-size` | `cache-max-size` | `DESYNC_CACHE_MAX_SIZE` | 0 (unlimited) |
| Max files | `--cache-max-files` | `cache-max-files` | `DESYNC_CACHE_MAX_FILES` | 0 (unlimited) |
| Partitions | `--cache-partitions` | `cache-partitions` | — | 256 |

Size accepts human-friendly suffixes: `10G`, `500M`, `1T`, `100K`.

When both limits are 0 (the default), no tracking file is created and the
cache behaves exactly as before.

## How it works

`SizeLimitStore` wraps `LocalStore` (following the existing
`WriteDedupQueue` and `RepairableCache` wrapper patterns) and delegates
eviction to a `cacheevict.Handler`:

- **`GetChunk`**: after reading, calls `handler.UseFile()` to update mtime
  for LRU tracking.
- **`StoreChunk`**: calls `handler.BeforeStore()` / `handler.AfterStore()`
  around the actual write. `AfterStore` updates counters and triggers
  eviction if limits are exceeded.
- **`RemoveChunk`**: calls `handler.BeforeRemove()` to decrement counters
  before the deletion.

desync configures the handler with callbacks for its specific layout:
- **SubdirPath**: 4-hex-char flat directories (`fmt.Sprintf("%04x", idx)`)
- **IsCachedFile**: `.cacnk` extension, excluding `.prunable`/`.protect`
- **IsTempFile**: `.tmp-cacnk` prefix (stale temp files cleaned during eviction)

See the [cacheevict README](https://github.com/insimo/cacheevict) for the eviction
algorithm, mmap file layout, and design rationale.

## Cache management commands

Three CLI commands provide manual cache management, using the
`cacheevict.Stats`, `cacheevict.Clear`, and `cacheevict.Trim` functions
which work by scanning the directory without requiring the mmap handler:

- **`desync cache-stats`** — show cache size, file count, and (if mmap
  tracking is active) hit/miss statistics. Warns if tracked counters
  diverge from actual content.
- **`desync cache-clear`** — remove all cached chunks (with confirmation).
- **`desync cache-trim`** — remove oldest chunks until within
  `--max-size`, `--max-files`, or `--max-age` limits. Falls back to
  config-file values if no flags given.

## Files

| File | Purpose |
|------|---------|
| `cache_config.go` | `DesyncCacheEvictConfig` — shared cacheevict layout config |
| `sizelimit.go` | `SizeLimitStore` wrapper using `cacheevict.Handler` |
| `sizelimit_test.go` | Integration tests |
| `cmd/desync/cachestats.go` | `desync cache-stats` command |
| `cmd/desync/cacheclear.go` | `desync cache-clear` command |
| `cmd/desync/cachetrim.go` | `desync cache-trim` command |
| `cmd/shared/cmdshared/config.go` | `CacheMaxSize`, `CacheMaxFiles`, `CachePartitions` config + resolvers |
| `cmd/shared/cmdshared/options.go` | `--cache-max-size`, `--cache-max-files`, `--cache-partitions` CLI flags |
| `cmd/shared/cmdshared/store.go` | `MultiStoreWithCache` wraps `LocalStore` in `SizeLimitStore` |

## Edge cases

- **Both limits 0**: no tracking file, no eviction — backward compatible.
- **Cold start**: one-time O(N) scan on first use; cost proportional to actual cache content.
- **Partition count change**: detected via stored header; triggers recreate + rescan.
- **Stale temp files**: `.tmp-cacnk*` files older than 1 hour are cleaned during eviction scans. These are safe to delete because chunk data is fully in memory before the temp file is created (slow network transfers don't affect temp file lifetime).
