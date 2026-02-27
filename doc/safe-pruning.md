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

Each chunk progresses through up to three states:

| State        | Files on disk                        | Visible to readers? |
|--------------|--------------------------------------|---------------------|
| Normal       | `<id>.cacnk`                         | Yes                 |
| Marked       | `<id>.cacnk` + `<id>.cacnk.prunable` | Yes                 |
| Quarantined  | `<id>.cacnk.pruning`                 | No (`.cacnk` gone)  |

Readers only look for `.cacnk` files and are completely unaffected by `.prunable` markers.

## Single Prune Run Algorithm

Each call to `SafePrune(ctx, ids)` executes two sequential phases:

### Phase 1 — Cleanup

Walk the store. For each file:

- If it ends in `.pruning`: **delete it** (this chunk was quarantined in the previous run
  and still isn't referenced — final deletion).
- If it ends in `.prunable`:
  - Compute the corresponding `.cacnk` path.
  - If the `.cacnk` no longer exists (orphaned marker): **delete the `.prunable` file**.

### Phase 2 — Mark and Quarantine

Walk the store. For each `.cacnk` file:

- **Skip** temp files (prefix `.tmp-cacnk`) and non-chunk files.
- Parse the chunk ID.
- If the chunk **is in `ids`** (keep-set):
  - Remove its `.prunable` marker if present (it was rescued or re-uploaded).
  - Continue (keep the chunk).
- If the chunk **is not in `ids`**:
  - If **no `.prunable` marker exists**: create an empty `<id>.cacnk.prunable` file
    (first encounter — mark it as a candidate).
  - If a **`.prunable` marker already exists** (marked in a previous run): quarantine
    the chunk by renaming `<id>.cacnk` → `<id>.cacnk.pruning`. Then re-check whether
    the `.prunable` marker still exists (see [Race Safety](#race-safety)); if it does,
    delete the `.prunable` marker.

## Race Safety

There is a narrow TOCTOU window during phase 2 quarantine: prune evaluates the
`.prunable` marker, then executes the rename. A writer could remove `.prunable`
(via `UntagPrunable`) and commit its index in this gap, after which `RescueChunks`
would be a no-op (it sees `.cacnk` still present). Prune then quarantines the chunk
anyway, leaving it stuck in `.pruning` with no writer aware it needs rescue.

**The fix**: after renaming `.cacnk` → `.pruning`, prune immediately re-checks
whether `.prunable` still exists.

- If `.prunable` is **still present**: the rename was safe — no writer removed the
  marker during the rename. Delete `.prunable` and leave the chunk quarantined.
- If `.prunable` is **gone**: a writer removed it concurrently and may have committed
  an index referencing this chunk. Revert the rename (`.pruning` → `.cacnk`) so
  the chunk is visible again. The chunk will be re-evaluated in the next prune run.

This works because once `.cacnk` has been renamed to `.pruning`, `HasChunk` returns
false, so any new writer that checks after the rename will proceed to call
`StoreChunk` (writing a fresh `.cacnk`) rather than `UntagPrunable`. The only writer
that can remove `.prunable` after the rename is one that saw `.cacnk` exist before
the rename — and that writer will call `RescueChunks` with this chunk ID after
committing its index, restoring the reverted chunk if needed.

## Why Two Runs Are Required

- **Run 1**: Marks every unreferenced chunk with a `.prunable` companion file.
  No data is deleted yet. A writer that commits its index between run 1 and run 2
  will have `RescueChunks` clear the markers.
- **Run 2**: Quarantines chunks that are *still* unreferenced (moves `.cacnk` to
  `.pruning`). The chunk becomes invisible to readers.
- **Run 3 (Phase 1)**: Deletes the `.pruning` files from run 2.

A chunk that enters the index between run 1 and run 2 will have its `.prunable`
marker removed (by `RescueChunks` or by phase 2 of run 2) and will survive.

## Writer Obligations

Writers using `make`, `tar -i`, or `chop` must perform two operations:

### 1. `UntagPrunable` (automatic via `ChunkStorage`)

When `ChunkStorage.StoreChunk` finds a chunk already present in the store
(`HasChunk` returns true), it calls `UntagPrunable` on the underlying store
if it implements `SafePruneStore`. This removes the `.prunable` marker so a
concurrent prune run won't quarantine the chunk.

### 2. `RescueChunks` (called after index commit)

After successfully writing the index file, the commands `make`, `tar`, and `chop`
call `RescueChunks(ctx, ids)` with the set of all chunk IDs in the index. For each
chunk:

- Any `.prunable` marker is removed.
- If the `.cacnk` is missing but a `.pruning` (quarantined) file exists, it is
  renamed back to `.cacnk`.

This handles the race where prune quarantined a chunk between the writer finishing
the chunks and finishing the index.

## Operational Guidance

- **Minimum interval between runs**: Allow enough time for any concurrent `make`,
  `tar`, or `chop` operations to complete between the two prune runs. The protocol
  does not enforce this — it is the operator's responsibility.
- **Enabling**: Pass `--safe-pruning` to `desync prune`. No changes are needed for
  `make`, `tar`, or `chop`; they call `RescueChunks` unconditionally when the store
  implements `SafePruneStore`.
- **Idempotent**: Re-running prune with `--safe-pruning` is always safe. If a run is
  interrupted, the next run cleans up any partial state.

## Backend Notes

### LocalStore

Rename operations (`os.Rename`) are atomic on POSIX filesystems. Phase 1 cleanup and
phase 2 quarantine are both atomic.

### SFTPStore

Uses `PosixRename` (the POSIX rename extension) for atomic rename during quarantine.
This requires an SFTP server that supports the `posix-rename@openssh.com` extension
(OpenSSH's sftp-server does).

### S3Store and GCStore

Object storage does not support atomic rename. Quarantine is implemented as:

1. Copy `.cacnk` → `.pruning`
2. Delete `.cacnk`
3. Delete `.prunable`

If the process is interrupted between steps 1 and 2, both `.cacnk` and `.pruning`
will exist simultaneously. Phase 1 of the next run will delete the `.pruning` object,
leaving `.cacnk` intact — no data loss.

## State Machine

```
                    ┌─────────────────────────────────────────────────┐
                    │  prune: id in keep-set → remove .prunable       │
                    │  writer: HasChunk=true  → UntagPrunable         │
  ┌──────────────┐  │                                                  │
  │              │──┴──[first run, not in keep-set]──────────────────►│  .cacnk
  │  .cacnk      │                                                     │  .cacnk.prunable
  │  (normal)    │◄──[RescueChunks / id enters keep-set next run]─────│  (marked)
  │              │                                                     │     │
  └──────────────┘                                                     │     │[second run,
        ▲                                                              │     │ still not in
        │                                                              └─────┘ keep-set]
        │                                                                     │
        │ RescueChunks                                                        ▼
        │ (rename back)                                              .cacnk.pruning
        └─────────────────────────────────────────────────────────── (quarantined)
                                                                             │
                                                                             │[phase 1,
                                                                             │ next run]
                                                                             ▼
                                                                          deleted
```
