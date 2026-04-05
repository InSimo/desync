# cacheevict

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Automatic LRU cache eviction for disk-based caches. Tracks total cache
size and file count via a persistent mmap'd file with lock-free atomic
counters, enabling safe coordination between multiple concurrent processes.

## Usage

```go
import "github.com/InSimo/cacheevict"

cfg := cacheevict.Config{
    BaseDir:    "/path/to/cache",
    MaxSize:    10 * 1024 * 1024 * 1024, // 10 GB
    MaxFiles:   0,                        // unlimited
    Partitions: 256,

    SubdirPath:   func(idx int) string { return fmt.Sprintf("%04x", idx) },
    FilePrefix:   func(relPath string) int { /* parse prefix from path */ },
    IsCachedFile: func(name string) bool { return strings.HasSuffix(name, ".dat") },
    IsTempFile:   func(name string) bool { return strings.HasPrefix(name, ".tmp-") },
}

handler, err := cacheevict.Open(cfg)
defer handler.Close()

// Store a file:
oldSize := handler.BeforeStore(path)
writeFile(path, data)
handler.AfterStore(path, oldSize)

// Read a file:
data := readFile(path)
handler.UseFile(path) // update mtime for LRU

// Remove a file:
handler.BeforeRemove(path)
os.Remove(path)
```

## Design

### Reference: ccache

The eviction algorithm is inspired by [ccache](https://ccache.dev/):

| Aspect | ccache | cacheevict |
|--------|--------|------------|
| **Size tracking** | Per-subdirectory stats files with file locks | Persistent mmap with lock-free atomic counters |
| **Eviction trigger** | `total_size > max_size` or `total_files > max_files` | Same |
| **Eviction target** | File-count-based: trim to `0.9 * total_files / P` | Same |
| **Partition selection** | Largest L2 subdir by file count | Same (largest partition by file count) |
| **Access tracking** | mtime updated on cache hits | Configurable via `UseFile` |
| **Concurrency** | File-based locks | Lock-free atomics; CAS eviction lock with stale PID detection |

### Why file-count-based eviction?

ccache's automatic eviction uses `max_size` (bytes) only as a trigger. The
actual deletion target is file-count-based: trim the chosen partition to
`0.9 * total_files / P`. This avoids pathological behavior where a few large
files cause excessive deletion of small files, and amortizes scan cost by
removing multiple files per eviction pass.

### Why mmap + atomics?

Multiple concurrent processes (e.g. parallel git-lfs transfers or desync
agents) may write to the same cache simultaneously. File-locked stats
serialize these updates. Instead, we use a persistent mmap'd file with
`sync/atomic` operations:

- **Lock-free counter updates** — `atomic.AddInt64` vs file lock + read + write + unlock
- **Single file** — one ~12 KB mmap'd file instead of many stats files
- **Zero explicit I/O** — the OS handles flushing mmap'd pages to disk

The mmap file is backed by a **regular file on disk** (not `/dev/shm` or a
page-file-backed named mapping), so counters persist across process restarts.
On Unix: `mmap(MAP_SHARED)`. On Windows: `CreateFileMappingW` with a real
file handle.

### Eviction algorithm

1. **Trigger** (O(1)): `totalSize > MaxSize` or `totalFiles > MaxFiles`.
2. **Eviction lock** (non-blocking CAS): skip if another process is already
   evicting. Dead holders detected via `kill(pid, 0)` / `OpenProcess`.
3. **Partition selection**: pick the partition with the highest file count.
4. **Directory scan**: iterate subdirectories in the partition, skipping
   those where the bitmask bit is unset (empty directories). Stale temp
   files (matching `IsTempFile`, older than 1 hour) are cleaned up during
   the scan.
5. **Sort by mtime** ascending (oldest first).
6. **Delete** until `partitionFiles <= 0.9 * totalFiles / P`.
7. **Update counters**: decrement globalSize, globalFiles, partitionSize,
   partitionFiles atomically for each deletion.

### Subdirectory presence bitmask

To avoid `os.ReadDir` on empty subdirectories during eviction, the mmap
file includes a bitmask (default: 65536 bits = 8 KB). Bit `i` is set when
a file is stored in subdirectory `i`, and cleared during eviction when a
directory is found empty. Stale set bits are harmless (one extra empty
readdir). Operations use `atomic.OrUint32` / `atomic.AndUint32` /
`atomic.LoadUint32` (Go 1.23+).

## Mmap file layout

File: `BaseDir/.cache-sizes`

```
Header (24 bytes):
  [0]   int32   eviction lock (0 = free, PID = in-progress)
  [4]   int32   partition count P
  [8]   int64   total size in bytes
  [16]  int64   total file count

Subdirectory presence bitmask (PrefixCount/8 bytes, default 8192):
  [24]  PrefixCount bits, bit i = subdir i may have files

Per-partition counters (P × 16 bytes):
  [24 + PrefixCount/8 + p*16]      int64   partition byte size
  [24 + PrefixCount/8 + p*16 + 8]  int64   partition file count

Total = 24 + PrefixCount/8 + P × 16
```

### Counter updates per operation

| Operation | globalSize | globalFiles | partitionSize | partitionFiles |
|-----------|-----------|-------------|--------------|----------------|
| AfterStore (new file) | +newSize | +1 | +newSize | +1 |
| AfterStore (overwrite) | +(new-old) | 0 | +(new-old) | 0 |
| BeforeRemove | -size | -1 | -size | -1 |
| Eviction delete | -size | -1 | -size | -1 |

### Cold start

When the tracking file is first created (or the partition count changes),
`initCounters()` walks the cache directory tree. Cost is O(N) in the number
of actual files but happens only once per cache lifetime.

## Configuration

### Config fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `BaseDir` | `string` | required | Root directory of the cache |
| `MaxSize` | `int64` | 0 (disabled) | Maximum total bytes |
| `MaxFiles` | `int64` | 0 (disabled) | Maximum total files |
| `Partitions` | `int` | 256 | Eviction partitions (power of 2) |
| `PrefixCount` | `int` | 65536 | Total subdirectory prefix slots (power of 2) |
| `SubdirPath` | `func(int) string` | required | Maps prefix index to relative directory path |
| `FilePrefix` | `func(string) int` | required | Maps relative file path to prefix index |
| `IsCachedFile` | `func(string) bool` | required | Identifies cached files by directory entry name |
| `IsTempFile` | `func(string) bool` | nil (disabled) | Identifies stale temp files for cleanup |

### Layout examples

**desync** (flat 4-hex-char, 65536 subdirs):
```go
SubdirPath:   func(idx int) string { return fmt.Sprintf("%04x", idx) }
FilePrefix:   func(rel string) int { var i int; fmt.Sscanf(rel[:4], "%04x", &i); return i }
IsCachedFile: func(name string) bool { return strings.HasSuffix(name, ".cacnk") && ... }
IsTempFile:   func(name string) bool { return strings.HasPrefix(name, ".tmp-cacnk") }
```

**git-lfs** (nested `ab/cd/`, 65536 leaf dirs):
```go
SubdirPath:   func(idx int) string { return fmt.Sprintf("%02x/%02x", idx>>8, idx&0xff) }
FilePrefix:   func(rel string) int { /* parse "ab/cd" → 0xab*256+0xcd */ }
IsCachedFile: func(name string) bool { return len(name) == 64 && isHex(name) }
IsTempFile:   nil  // temp files are in a separate directory
```

## Edge cases

- **Both limits 0**: tracking file still created, counters maintained, but no eviction.
- **Cold start**: one-time walk; cost proportional to actual cache content.
- **Partition count change**: detected via stored header; triggers recreate + rescan.
- **Counter drift** (external file deletion): counters may go slightly negative; harmless for approximate LRU.
- **Crash during eviction**: dead PID in eviction lock detected and reclaimed by next process.
- **File deleted while being read**: safe on Unix (open fd keeps inode); on Windows `os.Remove` fails for open files and is silently skipped.
- **mmap file wrong size or corrupt**: recreated and rescanned.

