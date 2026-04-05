# Changelog

## v0.1.0 — 2026-04-05

Initial preliminary release.

- Persistent mmap'd tracking file with lock-free atomic counters
- Configurable directory layout via callbacks (SubdirPath, FilePrefix, IsCachedFile, IsTempFile)
- Automatic LRU eviction: file-count-based target (90% of fair share per partition)
- Cross-process eviction lock with stale PID detection
- Subdirectory presence bitmask to skip empty directories during eviction
- Stale temporary file cleanup during eviction scans
- Hit/miss statistics (Hits, Misses, HitRatio, ResetStats)
- Versioned file format (v1.0) with 1024-byte header and reserved space for future fields
- Counter reconciliation from directory listing during eviction (self-healing drift)
- Platform support: Linux, macOS, Windows
