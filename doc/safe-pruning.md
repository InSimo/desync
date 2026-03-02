# Safe Pruning Protocol

## The Problem

`desync prune` deletes chunks not referenced by any provided index file. The normal write
order is:

1. Writer stores all chunks into the store.
2. Writer commits the index file.

If `prune` runs between steps 1 and 2, it will see the newly written chunks but not the
index that references them, and will delete those chunks. The next extraction will fail with
missing-chunk errors.

## Solution: Two-Run Safe Pruning

The safe pruning protocol requires at least **two consecutive prune runs** before any chunk
is actually deleted. This gives writers a safe window to finish committing their index.

Enable it with the `--safe-pruning` flag on `desync prune`, or by setting
`SafePruning: true` in `StoreOptions`.

## Chunk States

Each chunk can carry up to two companion marker files alongside its `.cacnk` data file:

| State              | Files present                                    | Visible to readers? |
|--------------------|--------------------------------------------------|---------------------|
| Normal             | `<id>.cacnk`                                     | Yes                 |
| Prunable           | `<id>.cacnk` + `<id>.cacnk.prunable`             | Yes                 |
| Protected          | `<id>.cacnk` + `<id>.cacnk.prunable` + `<id>.cacnk.protect` | Yes      |
| OrphanedPrunable   | `<id>.cacnk.prunable` (no `.cacnk`)              | —                   |
| OrphanedProtect    | `<id>.cacnk.protect` (no `.cacnk`)               | —                   |

Readers only look for `.cacnk` files. Marker files never affect read availability — unlike
the old quarantine protocol, the `.cacnk` file is never renamed or moved.

## Single Prune Run Algorithm

Each call to `SafePrune(ctx, ids)` executes two sequential phases using two store listings.

### Phase 1 — Orphan Cleanup

Walk the store. For each entry:

- **OrphanedPrunable** (`.prunable` without `.cacnk`): delete the `.prunable` file.
- **OrphanedProtect** (`.protect` without `.cacnk`): delete the `.protect` file.

These are leftover markers from interrupted or crashed operations.

### Phase 2 — Mark and Delete

Walk the store again. For each chunk entry:

- If the chunk **is in the keep-set** (`ids`):
  - Delete its `.prunable` marker if present (idempotent, no-op if absent).
  - Delete its `.protect` marker if present (cleanup of a writer's lingering protect).
- If the chunk **is not in the keep-set**:
  - **Normal** state: create a `.prunable` marker (first encounter — candidate for deletion
    next run).
  - **Prunable or Protected** state: perform a fresh `HasProtect` check. This may differ
    from the listing snapshot if a writer added `.protect` after the listing was taken —
    this is the key TOCTOU guard.
    - If protected: delete `.prunable` and `.protect` — a writer is actively saving this
      chunk; it returns to Normal state.
    - If not protected: delete the `.cacnk` data file, then the `.prunable` marker.

## Race Condition Analysis

Let `P` = `safe-propagation-time` — the maximum time for any write to become visible to
every reader (store propagation latency). Let `E` = the pruner's execution time between its
`HasProtect` check and the `DeleteChunk` call (a single network round-trip or syscall;
no explicit sleep in the hot path).

### Race 1 — Core TOCTOU: Pruner Misses Protect, Writer Misses Deletion

```
T1:              Writer adds .protect to C           (visible to all by T1+P)
T2 ∈ [T1, T1+P): Pruner checks HasProtect(C) → false  (protect not yet propagated)
T3 = T2+E:       Pruner deletes C                   (visible to all by T3+P)
T1+P:            Writer calls HasChunk(C)
```

At `T1+P`, the deletion is only visible if `T3+P ≤ T1+P`, i.e. `T3 ≤ T1`. But
`T3 ≥ T2 ≥ T1`, so the deletion can land in `(T1, T1+P+E)` — still invisible to the
writer's `1P` wait.

**Fix:** Writer waits `2P` from `T1`. Since `T3 < T2+E < T1+P+E`, the deletion propagates
by `T3+P < T1+2P+E`. With `E ≪ P` (delete is a single API call), `T3+P < T1+2P`, so the
writer's recheck at `T1+2P` catches any deletion. This is why `safe-propagation-time`
must bound **both** write propagation latency **and** the pruner's per-chunk execution time.
In practice (S3/GCS propagation = seconds, delete RTT = milliseconds), `2P` is vastly
conservative but necessary for formal correctness.

### Race 2 — Normal-to-Deleted: Eliminated by Design Assumption

```
T0:            Writer stores chunk C (Normal) → no protect, not in recheck list
T_m1:          Pruner cycle N:   C not in keep set → CreateMarker (.prunable added)
T_m2:          Pruner cycle N+1: C not in keep set, no protect → DeleteChunk(C)
T_commit > T0: Writer commits index with C → C is missing → DATA LOSS
```

This race requires **two full pruner cycles** to complete while the writer is still
building its index. The protocol assumes `safe-propagation-time ≪ pruner cycle time` —
propagation latency (milliseconds to seconds) is much shorter than the interval between
consecutive prune runs (minutes to hours in production). Under this assumption, a Normal
chunk cannot be deleted within the writer's `2P` window, and the race cannot occur.

This assumption must be treated as a **precondition**: set `safe-propagation-time` no
larger than half the minimum expected pruner cycle interval, not just the store's
propagation latency.

**Consequence:** Writers only need to protect and recheck chunks that **had a `.prunable`
marker at the time of reuse** — not every chunk in the index. This avoids expensive
per-chunk `HasChunk` round-trips for chunks that are safely Normal.

### Race 3 — Re-upload Timing

After re-uploading a missing chunk `C` at `T_ru` and adding `.protect`:

- `.protect` is visible to all readers by `T_ru+P`.
- A pruner that checked `HasProtect` before `T_ru+P` (stale read, no protect) could delete
  `C` at some `T_del < T_ru+2P`.

**Fix:** After re-uploading and re-protecting, wait another `2P` before committing the
index. One re-upload pass is sufficient: the second `2P` wait covers any stale-check
deletion of the freshly re-uploaded chunk.

### Race 4 — Protect Marker Cleanup by Writers

Writers must **not** remove `.protect` markers themselves. If a writer removed protect
after committing the index but before the pruner incorporated the new index into its keep
set:

```
T_commit:   Writer commits index (visible at T_commit+P)
T_commit+P: Writer removes .protect from C
T_prune_ks: Pruner reads keep set (snapshot before T_commit+P; C not in keep set)
T_check:    Pruner checks HasProtect(C) → false (writer just removed it)
            → DeleteChunk(C) → DATA LOSS
```

**Fix:** Protect markers are cleaned up **only by the pruner**:
- For chunks **in** the keep set: pruner calls `DeleteProtect` (idempotent).
- For chunks **not** in the keep set: pruner calls `HasProtect` before deleting; if found,
  `DeleteMarker + DeleteProtect` (keep chunk); if not found, `DeleteChunk + DeleteMarker`.

Protect markers on keep-set chunks linger for at most one extra pruner cycle — acceptable.

### Race 5 — Re-upload Without Re-adding Protect

When re-uploading chunk `C`, the freshly written `.cacnk` has no `.protect` marker. If a
new pruner cycle marks it Prunable before the index is committed, and a subsequent cycle
finds no protect, it will delete `C` — looping back to Race 1.

**Fix:** After re-uploading `C`, the writer also calls `CreateProtect(C)`. The second `2P`
wait then covers Race 1 for the re-uploaded chunk.

### Correctness Summary

The protocol is correct under these conditions (all satisfied by the implementation):

1. `safe-propagation-time P` bounds both write propagation latency and delete round-trip time.
2. Writer waits `2P` from when the last `.protect` was written before rechecking.
3. Only chunks that were Prunable at reuse time are rechecked (Race 2 design assumption).
4. After any re-upload + protect, writer waits another `2P` before committing.
5. Protect markers are cleaned up only by the pruner, never by writers.

## Writer Obligations

Writers using `make`, `tar -i`, `chop`, or the git-lfs agent benefit from built-in
safe-pruning support in `ChopFile` and `ChunkStream`. When a non-nil `SafePruneStore`
is passed, these functions handle the entire writer-side protocol automatically.

### `ChunkStorage` with safe pruning (automatic during chunking)

`ChopFile` and `ChunkStream` accept an optional `SafePruneStore` parameter. When non-nil,
they construct a `ChunkStorage` via `NewChunkStorageWithPruning(ws, sps)` which inlines
the safe-pruning logic directly into `StoreChunk`:

**`StoreChunk(chunk)`** — for each chunk:

1. If the chunk is **absent** from the store: store it normally (fresh chunk path).
2. If the chunk is **present and not prunable**: skip it (deduplication).
3. If the chunk is **present and prunable** (`.prunable` marker exists):
   call `CreateProtect`, capture the chunk data in memory for pre-commit rechecking,
   and update `LastProtectTime`.

Only prunable chunks are captured; memory overhead is proportional to the deduplication
hit rate, not to the total file size.

### `SafePrunePreCommit` (called internally by `ChopFile` / `ChunkStream`)

After chunking completes, `ChopFile` and `ChunkStream` call `SafePrunePreCommit`
internally before returning. This performs the recheck phase:

1. Waits until `LastProtectTime + 2×propTime` for protect markers to propagate.
2. For each captured chunk, calls `HasChunk`. If missing (pruner won the race):
   - Re-uploads the chunk via `StoreChunk`.
   - Re-adds `.protect` via `CreateProtect`.
3. If any chunks were re-uploaded, waits another `2×propTime`.

After `SafePrunePreCommit` returns, the index may be committed safely.

Use `propTime = 0` for local and SFTP stores (atomic operations, no propagation delay).
For S3 and GCS, set `safe-propagation-time` in `StoreOptions` to the store's observed
propagation latency — see [Operational Guidance](#operational-guidance).

## Operational Guidance

- **Minimum interval between runs**: Allow enough time for all concurrent write operations
  to complete between consecutive prune runs. The protocol does not enforce this — it is
  the operator's responsibility. Set `safe-propagation-time` to no more than half the
  minimum expected pruner cycle interval.
- **Enabling**: Pass `--safe-pruning` to `desync prune`. No extra flags are needed for
  `make`, `tar`, or `chop`; they handle safe pruning automatically when the store
  implements `SafePruneStore`.
- **Idempotent**: Re-running prune with `--safe-pruning` is always safe. If a run is
  interrupted, the next run cleans up any orphaned markers in Phase 1.
- **`safe-propagation-time`**: For object stores, set this to the store's write-to-read
  propagation latency (typically 1–10 seconds for S3/GCS). The writer waits `2×` this
  value, so keep it tight. Do not set it to the pruner cycle interval.

## Backend Notes

### LocalStore and SFTPStore

Both use direct filesystem or SFTP operations. All marker creates and deletes are
individual file operations. No atomic rename is required — the `.cacnk` file is never
moved. Use `propTime = 0`.

### S3Store and GCStore

Object storage does not support atomic operations. The `.cacnk`, `.prunable`, and
`.protect` objects are independent. Partial failures (e.g., `CreateProtect` succeeds but
the process crashes before committing the index) leave orphaned markers that Phase 1 of
the next prune run will clean up. No data loss occurs in any failure scenario.

Set `safe-propagation-time` in `StoreOptions` to the measured write-to-read propagation
latency of your bucket (typically a few seconds).

## State Machine

```
                   ┌───────────────────────────────────────────────────────────┐
                   │  prune: id in keep-set → DeleteMarker + DeleteProtect      │
                   │                                                             │
 ┌─────────────┐   │                                                             │
 │             │───┴──[run N, not in keep-set]──────────────────────────────►  │ .cacnk
 │   .cacnk    │                                                                │ .cacnk.prunable
 │  (Normal)   │◄──[prune: HasProtect=true → DeleteMarker+DeleteProtect]──────  │ (Prunable)
 │             │                                                                │    │
 └─────────────┘                                                                │    │ writer: HasPrunable=true
       ▲                                                                        └────┘ → CreateProtect
       │ prune: HasProtect=true                                                      │
       │ → DeleteMarker+DeleteProtect                                                ▼
       │                                                                   .cacnk + .prunable
       │                                                                   .cacnk.protect
       │                                                                   (Protected)
       │                                                                        │
       └────────────────────────────────────────────────────────────────────────┘
                                                                                │
                                              prune: HasProtect=false           │ prune: HasProtect=false
                                              → DeleteChunk + DeleteMarker      │ → DeleteChunk + DeleteMarker
                                                                                ▼
                                                                             deleted
```
