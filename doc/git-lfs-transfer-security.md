# git-lfs-transfer Security Review

This document records the security review of `cmd/git-lfs-transfer` and the
implementation tasks that follow from it.

## Threat Model

`git-lfs-transfer` is invoked by `sshd` on behalf of an authenticated SSH
user, typically via a `command=` restriction in `authorized_keys`.  The binary
reads from stdin and writes to stdout; both descriptors are the SSH channel.
stderr is also forwarded to the client.  The attacker model is a **malicious
but SSH-authenticated user** who can feed arbitrary pkt-line data to the
binary.  An admin who controls the git config of the repository is assumed
trusted.

---

## Confirmed Issues

### 1 — OID not validated as a hex string (path traversal)

**Severity:** High
**Files:** [`cmd/git-lfs-transfer/transfer.go:25`](../cmd/git-lfs-transfer/transfer.go#L25), [`cmd/git-lfs-transfer/batch.go:78`](../cmd/git-lfs-transfer/batch.go#L78), [`cmd/internal/cmdshared/oid.go:12`](../cmd/internal/cmdshared/oid.go#L12)

Every OID is checked only for minimum length (`len(oid) < 4`).  `OidIndexName`
then constructs a filesystem path by naive string concatenation:

```go
func OidIndexName(oid string) string {
    return oid[0:4] + "/" + oid + ".caibx"
}
```

A client-supplied OID such as `../../../../etc/foo` produces the path
`../..` + `/../../../../etc/foo.caibx`, which escapes the index store root
when joined by the underlying `IndexWriteStore`.

- On **download** (`get-object`, `batch`): an attacker can probe whether
  arbitrary `.caibx` files exist anywhere on the filesystem.
- On **upload** (`put-object`): `StoreIndex` can write an index file to an
  arbitrary path reachable from the index store root.

A valid Git LFS OID is a SHA-256 hex digest: exactly **64 lowercase hex
characters** (`[0-9a-f]{64}`).

---

### 2 — `<path>` argument not verified to be a git repository

**Severity:** Medium
**File:** [`cmd/git-lfs-transfer/main.go:47`](../cmd/git-lfs-transfer/main.go#L47)

`os.Args[1]` is resolved to an absolute path and used immediately to load
config and open stores.  There is no check that it is an actual git repository.
An unexpected path silently falls through to the convention-based store layout
(`<path>/desync-lfs/{chunks,index}`), potentially creating directories in
unintended locations and misleading error messages.  The user's intent when
calling `git-lfs-transfer <path> <operation>` is always a git repository.

---

### 3 — No maximum upload size

**Severity:** Medium
**File:** [`cmd/git-lfs-transfer/server.go:171`](../cmd/git-lfs-transfer/server.go#L171)

`parseSize` validates only that the `size` argument is a parseable `int64`; it
does not reject negative values or values above a reasonable ceiling.  A client
can declare an arbitrarily large size and stream that many bytes, filling the
server's disk or exhausting memory.  Negative sizes pass the check and cause
`received != size` to always be false, resulting in an empty object being
accepted and indexed.

---

### 4 — Internal error details forwarded to the SSH client via stderr

**Severity:** Medium
**File:** [`cmd/git-lfs-transfer/transfer.go`](../cmd/git-lfs-transfer/transfer.go)

Go error strings—including full filesystem paths, store URLs, and OS error
messages—are included verbatim in `WriteErrorStatus(500, ...)` responses and
also propagate to stderr, which the SSH transport forwards to the client:

```go
return s.w.WriteErrorStatus(500, fmt.Sprintf("assembling object %s: %v", oid, err))
```

This leaks the server's internal directory layout and storage configuration to
any authenticated user.

Git LFS's own server implementation addresses this by writing internal error
details to a timestamped log file and printing only the log file path to
stderr:

```
Errors logged to .git/lfs/objects/logs/20150720T141259.212878623.log
Use `git lfs logs last` to view the log.
```

---

### 5 — Uploaded objects buffered in world-accessible `/tmp`

**Severity:** Medium
**File:** [`cmd/git-lfs-transfer/main.go:127`](../cmd/git-lfs-transfer/main.go#L127), [`cmd/git-lfs-transfer/transfer.go:36,115`](../cmd/git-lfs-transfer/transfer.go#L36)

`os.TempDir()` (typically `/tmp`, mode 1777) is used to buffer entire LFS
objects before chunking.  LFS objects are often sensitive binary assets.  Any
local user can observe or read the files during the transfer window.

Beyond confidentiality, buffering the full object to disk before chunking
doubles disk I/O for large files.  Both the upload and download paths can be
made fully streaming, eliminating temp files entirely:

- **Upload:** pipe pkt-line data directly into `ChunkStream` (as `desync tar`
  does), computing the SHA-256 simultaneously via `io.MultiWriter`.
- **Download:** `indexTotalSize(idx)` gives the exact file size from the index
  metadata, before fetching any chunk data, so the `size=N` header can be sent
  immediately and chunks can be streamed sequentially to the pkt-line writer.

---

### 6 — Malformed batch entries silently discarded; sizes never validated

**Severity:** Low
**File:** [`cmd/git-lfs-transfer/batch.go:44`](../cmd/git-lfs-transfer/batch.go#L44)

Batch lines with fewer than two fields are silently skipped.  The size field is
stored as a raw string and never parsed at batch time; invalid values are
echoed back to the client verbatim.  An unparseable or negative size in the
batch response may confuse compliant clients.

---

## Implementation Tasks

Tasks are ordered by priority.  Each is self-contained.

---

### Task 1 — Validate OID format everywhere

**Priority:** High (path traversal)
**Touches:** `cmd/git-lfs-transfer/`, `cmd/internal/cmdshared/oid.go`

Add a package-level helper (e.g. in `cmd/git-lfs-transfer/validate.go` or
inline in `server.go`):

```go
var oidRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validOID(oid string) bool { return oidRe.MatchString(oid) }
```

Replace every `len(oid) < 4` guard with `!validOID(oid)` in:

- `handleGetObject` ([transfer.go:25](../cmd/git-lfs-transfer/transfer.go#L25))
- `handlePutObject` ([transfer.go:106](../cmd/git-lfs-transfer/transfer.go#L106))
- `handleVerifyObject` ([transfer.go:202](../cmd/git-lfs-transfer/transfer.go#L202))
- `batchAction` ([batch.go:78](../cmd/git-lfs-transfer/batch.go#L78))

In `handleBatch`, replace the silent `continue` with a `noop` response for
entries whose OID fails validation, so the client is informed rather than
silently ignored.

Add a table-driven unit test covering: empty string, `"../../etc/x"`, an OID
with uppercase letters, 63-char and 65-char strings, and a valid 64-char
SHA-256 hex string.

---

### Task 2 — Verify `<path>` is a git repository

**Priority:** High
**Touches:** `cmd/git-lfs-transfer/main.go`

After `filepath.Abs`, add a validation step before calling `resolveConfig`.
Use the existing `exec.Command` pattern already present in `gitConfigValue`:

```go
if err := validateGitRepo(absPath); err != nil {
    return fmt.Errorf("not a git repository %q: %w", repoPath, err)
}
```

where `validateGitRepo` runs `git -C absPath rev-parse --git-dir` and returns
an error on non-zero exit.  This implicitly rejects non-existent paths,
non-repo directories, and paths that only superficially resemble repos.

---

### Task 3 — Reject negative sizes and enforce an upload size cap

**Priority:** High
**Touches:** `cmd/git-lfs-transfer/server.go`, `cmd/git-lfs-transfer/transfer.go`

In `parseSize` ([server.go:171](../cmd/git-lfs-transfer/server.go#L171)),
after `ParseInt` succeeds, return an error if `size < 0`.

Add a constant for the maximum allowed object size (e.g. 100 GiB; make it
configurable via the desync-lfs config in a follow-up):

```go
const maxObjectSize int64 = 100 * 1024 * 1024 * 1024
```

In `handlePutObject`, check `size > maxObjectSize` before allocating any
resources and drain + return a 400 error immediately.  This prevents disk
exhaustion from a declared-but-unsent huge transfer.

Add unit tests for `parseSize` with `"-1"`, `"0"`, a valid value, and a value
exceeding the cap.

---

### Task 4 — Log internal errors to a file; send only the path to the client

**Priority:** Medium
**Touches:** `cmd/git-lfs-transfer/main.go`, `cmd/git-lfs-transfer/transfer.go`, `cmd/git-lfs-transfer/server.go`

**Design:**

When the server starts, create a timestamped log file under
`<repoPath>/desync-lfs/logs/`:

```
<repoPath>/desync-lfs/logs/20150720T141259.212878623.log
```

Use `time.Now().UTC().Format("20060102T150405.000000000")` for the filename,
matching the Git LFS convention.  Pass a `*log.Logger` (or `*os.File`) into
the `Server` struct.

In every `WriteErrorStatus(500, ...)` call in `transfer.go`, write the full
error (with OID and Go error chain) to the log file, and send a generic message
in the protocol response:

```go
// Internal 500 errors
s.logf("assembling object %s: %v", oid, err)
return s.w.WriteErrorStatus(500, "internal error")
```

400-level errors (client input mistakes such as size mismatch or invalid OID)
do not need to be sanitised—they describe the client's own data and contain no
server internals.

Only print the log file path to stderr when at least one error was actually
logged, following the git lfs pattern:

```
Errors logged to /srv/repos/myrepo.git/desync-lfs/logs/20150720T141259.212878623.log
```

The log directory (`<repoPath>/desync-lfs/logs/`) should be created alongside
the existing `desync-lfs/chunks` and `desync-lfs/index` directories.

---

### Task 5 — Stream uploads through a pipe; eliminate the upload temp file

**Priority:** Medium
**Touches:** `cmd/git-lfs-transfer/transfer.go`, `cmd/git-lfs-transfer/main.go`

**Motivation:** Buffering a full LFS object to disk before chunking doubles
disk I/O and requires world-accessible `/tmp`.  The `desync tar` command avoids
this by feeding data from a goroutine into `ChunkStream` via `io.Pipe`
([cmd/desync/tar.go:129–168](../cmd/desync/tar.go#L129)).  The same pattern
works here.

**Design for `handlePutObject`:**

1. Create an `io.Pipe()`.
2. Attach a `sha256.New()` hasher and a byte counter to the write side using
   `io.MultiWriter(pipeWriter, hasher)`.
3. In a goroutine (managed by `errgroup`): read pkt-line binary packets from
   the client and write each packet to the `MultiWriter`; on flush-pkt, close
   the pipe writer.
4. On the main goroutine: construct a `desync.Chunker` on the pipe reader and
   call `desync.ChunkStream`.
5. After `g.Wait()`, verify `received == size` and
   `hex.EncodeToString(hasher.Sum(nil)) == oid`.  Only call `StoreIndex` on
   success; otherwise return a 400 error.
   - Chunks that were already stored are orphaned but harmless—the safe-pruning
     protocol will reclaim them.
6. Remove `s.tmpDir` from the `Server` struct.  The `tmpDir` field and the
   `os.TempDir()` call in `main.go` can be removed entirely for the upload
   path.

**Design for `handleGetObject`:**

The index is fetched before any chunk data is read.  `indexTotalSize(idx)`
already computes the exact byte length of the reassembled file from the last
chunk's `Start + Size` field—no data needs to be fetched first.  This means
the `size=N` header can be sent immediately after the index lookup, and chunk
data can then be streamed directly to the pkt-line writer without any temp
file:

1. Fetch the index and call `indexTotalSize(idx)` to get the size.
2. Send the `200` status, the `size=N` header, and the delimiter packet.
3. Iterate `idx.Chunks` in order: call `s.readStore.GetChunk(c.ID)`, decompress
   with `chunk.Data()`, write the decompressed bytes as binary pkt-line
   packets.
4. Send the flush packet.

Because chunk ordering in the index is sequential by `Start` offset, no
reorder buffer is required; a simple loop suffices.  Parallel prefetching (like
`AssembleFile` does with `WriteAt`) could be added later for latency-bound
remote stores, but is out of scope here.

Note: desync has no existing `AssembleWriter`/`AssembleStream` function—
`AssembleFile` uses parallel `WriteAt` into a named file.  The sequential loop
above is self-contained in `handleGetObject` and does not require a library
change.

With both paths streaming, `s.tmpDir`, the `tmpDir` field on `Server`, and the
`os.TempDir()` call in `main.go` can all be removed entirely.

---

### Task 6 — Validate and reject malformed batch entries

**Priority:** Low
**Touches:** `cmd/git-lfs-transfer/batch.go`

Replace the silent `continue` for short lines ([batch.go:45](../cmd/git-lfs-transfer/batch.go#L45))
with an explicit `noop` response so the client is informed.

Parse the size field as `int64` at batch-ingestion time and reject entries with
non-positive or non-numeric sizes with a `noop` rather than echoing garbage
back.  OID validation is already covered by Task 1 (`batchAction` guard).

---

## Summary Table

| # | Issue | Severity | Primary file |
|---|-------|----------|-------------|
| 1 | OID path traversal — no hex validation | High | `transfer.go`, `batch.go` |
| 2 | `<path>` not verified as git repo | High | `main.go` |
| 3 | No upload size cap; negative sizes accepted | High | `server.go`, `transfer.go` |
| 4 | Internal errors leaked to SSH client | Medium | `transfer.go` |
| 5 | Upload and download buffered in world-accessible `/tmp` | Medium | `main.go`, `transfer.go` |
| 6 | Malformed batch entries silently ignored | Low | `batch.go` |
