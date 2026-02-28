# Safe Index Pruning Protocol

## The Problem

`desync index-prune` deletes indexes not listed in the caller-supplied keep set.
Inside a single prune operation the sequence is:

1. Caller assembles the keep set (e.g. by querying a registry of live artifacts).
2. `PruneIndexes` calls `ListIndexes` to enumerate what is in the store.
3. Any index present in the listing but absent from the keep set is deleted.

If a writer calls `StoreIndex` between steps 1 and 2, the new index will appear
in the listing (step 2) but not in the keep set (step 1), and will be deleted in
step 3. This is a genuine data-loss race in any deployment where index writes and
index pruning can overlap.

The secondary risk is cascade loss: if the deleted index is the only reference to
certain chunks, a subsequent `desync prune` will delete those chunks too, making
the data unrecoverable.

## Why the Chunk Safe-Pruning Protocol Does Not Apply

The chunk safe-pruning protocol (`doc/safe-pruning.md`) requires writers to call
`RescueChunks` after committing an index. This is necessary because the chunk
write window spans two separate operations — `StoreChunk` then `StoreIndex` — and
a prune can slip between them.

For indexes there is no such mandatory window. `StoreIndex` is a single atomic
operation: the index is either fully written or not present at all. A rescue call
immediately after `StoreIndex` would close a window of zero operations and
therefore provides no protection. No writer changes are required.

The race for indexes is purely at the **operational level**: between when the keep
set is assembled and when `ListIndexes` runs. The two-run protocol below closes
that window without touching the write path.

## Solution: Two-Run Safe Index Pruning

Enable it with `--safe-index-pruning` on `desync index-prune`.

The protocol requires at least **two consecutive prune runs** before any index is
actually deleted. An index is deleted only when it has been absent from the keep
set in **two successive calls**. This guarantees that no index written inside the
race window of a single operation can be removed by that operation.

## How It Works

### Prunable Set

After each run, `SafePruneIndexes` writes a file named `prunable-indexes` into
the root of the index store. This file contains a JSON array of the index names
that were candidates for deletion in that run (present in the store but absent
from the keep set). It is read back at the start of the next run.

The file name `prunable-indexes` is reserved and must not be used for real
indexes. All `ListIndexes` implementations filter it from their output.

### Algorithm

Each call to `SafePruneIndexes(ctx, keep)` executes the following steps:

1. **Read** the prunable set `prev` written by the previous run (empty on the
   first run).
2. **List** all indexes currently in the store → `names`.
3. For each name in `names`:
   - If name is in `keep`: skip (keep it).
   - If name is in `prev` (was a candidate last run): add to `toDelete`.
   - Otherwise (first sighting): add to `newPrunable`.
4. **Delete** all indexes in `toDelete`.
5. **Write** `newPrunable` as the new prunable set for the next run.

### Correctness Argument

An index I can reach `toDelete` only if it was in `prev`, meaning it appeared in
the *previous* run's `ListIndexes` snapshot. It therefore existed before the
previous run. It cannot be an index that was written in the race window between
the *current* keep-set assembly (step 1 above) and the *current* `ListIndexes`
call (step 2 above) — because it already existed earlier.

Concretely: suppose a writer calls `StoreIndex("new.caibx")` between keep-set
assembly and `ListIndexes` in run N. `new.caibx` will appear in run N's listing
but will not be in `prev` (it did not exist during run N−1). It therefore goes
into `newPrunable`, not `toDelete`. It survives run N.

By run N+1 the writer's index has been live long enough that the operator's
keep-set assembly will include it (or the operator has intentionally omitted it
and genuinely wants it deleted). Either way the decision is made with full
visibility of the index.

### Interrupted Runs

If `DeleteIndexes` fails or the context is cancelled, `WritePrunableIndexSet` is
not called, so the prunable set file retains its previous contents. On the next
run:

- Indexes that were in `toDelete` but not yet deleted are still in `prev` and
  will be retried.
- Indexes in `newPrunable` were not written; they will be treated as first
  sightings again and go into `newPrunable` of the next run.

This is the safe outcome: no index is lost, and at most one extra run is needed.

## Enabling

```
desync index-prune --index-store /path/to/indexes --safe-index-pruning --yes blob.caibx
```

No changes are needed for writers (`make`, `tar`, `chop`, or any other command
that calls `StoreIndex`).

## Operational Guidance

- **Minimum interval between runs**: allow enough time for any concurrent
  `StoreIndex` calls to complete between two successive `index-prune` runs. The
  protocol itself does not enforce a delay.
- **First run**: no indexes are deleted; only the prunable set file is created.
- **Subsequent runs**: indexes that were candidates in the previous run and are
  still absent from the keep set are deleted.
- **Idempotent**: re-running with `--safe-index-pruning` is always safe. An
  interrupted run leaves no broken state; the next run resumes correctly.
- **Resetting state**: delete the `prunable-indexes` file from the index store
  root to reset to "first run" behaviour (no indexes will be deleted on the next
  run).

## Backend Notes

### Prunable Set Storage

| Backend        | Location of `prunable-indexes`                      | Write strategy        |
|----------------|-----------------------------------------------------|-----------------------|
| LocalIndexStore | `<store-root>/prunable-indexes`                    | Direct `os.Create`    |
| SFTPIndexStore  | `<store-root>/prunable-indexes`                    | Atomic `PosixRename`  |
| S3IndexStore    | `s3://<bucket>/<prefix>prunable-indexes`           | `PutObject` (replace) |
| GCIndexStore    | `gs://<bucket>/<prefix>prunable-indexes`           | `NewWriter` (replace) |

### LocalIndexStore and SFTPIndexStore

The file is a plain JSON array of index names. The SFTP backend writes via the
existing `StoreObject` helper, which uses `PosixRename` for atomicity (requires
an OpenSSH-compatible SFTP server). The local backend writes directly with
`os.Create`; a partially-written file on crash is treated as an empty set on the
next read, which is the safe fallback.

### S3IndexStore

A single `PutObject` call replaces the prunable-indexes object on each run. This
is not atomic with respect to concurrent prune operations, but simultaneous
invocations of `index-prune` are not an expected usage pattern.

### GCIndexStore

Same semantics as S3: a single object write replaces the previous state.

## State Diagram

```
                  first run: not in keep
  ┌────────────┐ ─────────────────────────────► ┌───────────────────────┐
  │            │                                 │ in prunable set       │
  │  (normal)  │ ◄─────────────────────────────  │ (listed in prunable-  │
  │            │   added to keep between runs     │  indexes file)        │
  └────────────┘                                 └───────────────────────┘
                                                           │
                                                           │ second run: still not in keep
                                                           ▼
                                                        deleted
```
