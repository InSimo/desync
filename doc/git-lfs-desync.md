# git-lfs-desync

`git-lfs-desync` is a [Git LFS custom transfer agent](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md) that stores LFS objects using desync's content-defined chunking. Instead of uploading whole files to an LFS server, it:

1. Splits each file into variable-size chunks using a SipHash rolling hash.
2. Compresses and deduplicates chunks in a desync-compatible store.
3. Stores a `.caibx` index (keyed by LFS OID) in a separate index store.

On download, it fetches the index and reassembles the file from the chunk store. Chunks are shared across all files and commits — only genuinely new content is uploaded.

## Building

```sh
go install github.com/folbricht/desync/cmd/git-lfs-desync@latest
# or from source:
go build -o /usr/local/bin/git-lfs-desync ./cmd/git-lfs-desync
```

## Supported Backends

| Protocol                 | URL Scheme                       | Notes                                      |
| ------------------------ | -------------------------------- | ------------------------------------------ |
| **Local filesystem**     | `/path/to/dir` or `./dir`        | No credentials needed; simplest setup      |
| **S3-compatible**        | `s3+https://host/bucket/prefix/` | AWS S3, MinIO, Ceph RGW, etc.              |
| **SFTP**                 | `sftp://user@host/path/`         | Uses SSH keys or agent                     |
| **HTTP/HTTPS**           | `https://host/path/`             | Requires a write-capable HTTP store server |
| **Google Cloud Storage** | `gs://bucket/prefix/`            | Uses GCS application default credentials   |

SSH stores (`ssh://`) are read-only in desync and cannot be used with this agent.

## Flags

| Flag                                | Default                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------------------- | ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-s`, `--store`                     | config default or _(required)_              | Chunk store location. See [Supported Backends](#supported-backends) for URL schemes. May be set via the `defaults.stores` config key.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `--index-store`                     | config default, then derived from `--store` | Index store location. Falls back to `defaults.index-store` in the config, then to replacing the last path segment of `--store` with `index` (or a sibling `index` directory for local paths).                                                                                                                                                                                                                                                                                                                                                            |
| `-c`, `--cache`                     | —                                           | Local chunk store used as a download cache. On a cache miss, the chunk is fetched from `--store` and saved to the cache; subsequent downloads are served from the cache. Uploads always go directly to `--store`. Accepts the same URL schemes as `--store`.                                                                                                                                                                                                                                                                                             |
| `--cache-repair`                    | `true`                                      | If the cache returns a corrupt chunk, re-download it from `--store` and replace the cached copy.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `-n`, `--concurrency`               | `10`                                        | Number of concurrent goroutines for chunk I/O.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `-m`, `--chunk-size`                | config default, then `16:64:256`            | Min:avg:max chunk size in KB. May be set via the `defaults.chunk-size` config key.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `-e`, `--error-retry`               | `3`                                         | Number of times to retry on network error.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `-b`, `--error-retry-base-interval` | `500ms`                                     | Initial retry delay; increases linearly with each attempt.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `--client-cert`                     | —                                           | Path to client certificate for mutual TLS.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `--client-key`                      | —                                           | Path to client key for mutual TLS.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `--ca-cert`                         | —                                           | CA certificate file to trust instead of the OS trust store.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `-t`, `--trust-insecure`            | `false`                                     | Trust invalid/self-signed certificates.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `--config`                          | `$HOME/.config/desync/config.json`          | desync config file for S3 credentials and store options. Mutually exclusive with `--config-from-git`.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `--config-from-git`                 | —                                           | Read the desync config from a git object. `%(remote)` and `%(operation)` are replaced with values from the LFS init message (e.g. `%(remote)/_desync:config.json`). `%(remote)` defaults to `origin` when the remote is not available (e.g. smudge filter during `git clone`). Mutually exclusive with `--config`.                                                                                                                                                                                                                                       |
| `--digest`                          | config default, then `sha512-256`           | Hash algorithm used to identify chunks: `sha512-256` (default) or `sha256`. Must match the algorithm used when the store was originally written. May be set via the `defaults.digest` config key.                                                                                                                                                                                                                                                                                                                                                        |
| `--indexes`                         | `false`                                     | Translate LFS OIDs to desync index names and write to stdout (one per line). Reads OIDs from positional args, or from the first whitespace-delimited token of each stdin line when no args are given (blank lines are skipped). When set, no store configuration is needed and the agent exits immediately without starting the LFS transfer protocol. See [Index and Chunk Pruning](#index-and-chunk-pruning).                                                                                                                                          |
| `--max-in-flight`                   | `2147483648` (2 GB)                         | Maximum total bytes allowed in-flight across all concurrent agent processes. Limits memory usage when git-lfs spawns multiple agents (`concurrent = true`). Set to `0` to disable. Also settable via `defaults.max-in-flight` in the config or `DESYNC_MAX_INFLIGHT` env var. |
| `--max-storage-ops`                 | `0` (disabled)                              | Maximum concurrent storage operations (GetChunk, StoreChunk, GetIndex, StoreIndex) across all concurrent agent processes. Limits S3/network backend load. Set to `0` to disable. Also settable via `defaults.max-storage-ops` in the config or `DESYNC_MAX_STORAGE_OPS` env var. |
| `--cache-max-size`                  | `""` (unlimited)                            | Maximum cache size (e.g. `10G`, `500M`). When exceeded, the oldest chunks are automatically evicted. Also settable via `defaults.cache-max-size` in the config or `DESYNC_CACHE_MAX_SIZE` env var. See [doc/cache-size-limit.md](cache-size-limit.md). |
| `--cache-max-files`                 | `0` (unlimited)                             | Maximum number of cached files. When exceeded, the oldest chunks are automatically evicted. Also settable via `defaults.cache-max-files` in the config or `DESYNC_CACHE_MAX_FILES` env var. |
| `--cache-partitions`                | `256`                                       | Number of eviction partitions (power of 2). Higher values reduce per-eviction scan cost for very large caches. Also settable via `defaults.cache-partitions` in the config. |
| `--no-pipelined`                    | `false`                                     | Disable pipelined mode. When set, the agent falls back to one-object-at-a-time processing even if the client supports pipelining. See [Pipelined Mode](#pipelined-mode). |
| `--max-concurrent-uploads`          | `8`                                         | Maximum parallel upload/download operations in pipelined mode. Lightweight existence checks (`HasIndex`) run at full concurrency; only heavy work (chunking, S3 storage, assembly) is limited by this value. |
| `--safe-pruning`                    | `false`                                     | Enable the safe concurrent pruning protocol on the upload path. After each successful upload, calls `RescueChunks` on the chunk store to recover any chunks that a concurrent `desync prune --safe-pruning` may have quarantined between the `StoreChunk` and `StoreIndex` steps. Must be used together with `desync prune --safe-pruning` and `desync index-prune --safe-index-pruning`. Supported by all writable backends (local, S3, SFTP, GCS). See [Index and Chunk Pruning](#index-and-chunk-pruning) and [doc/safe-pruning.md](safe-pruning.md). |

## Config defaults

Flags that are the same for every invocation (store URL, index store URL, digest algorithm) can be set once in the desync config file under a `defaults` key, so the bare `git-lfs-desync` command works without extra `args` in the LFS agent config.

### Precedence

```
CLI flag  >  local config defaults  >  server config  >  built-in default
```

When the agent is auto-negotiated via SSH transfer negotiation (see
[Server-Provided Config](#server-provided-config)), the server provides a
config with store URLs and parameters. Local CLI flags and config file values
take priority over server-provided values, with two exceptions:

- **Chunk size**: server always wins (must match for data compatibility)
- **Safe-pruning**: if the server enables it, the client must also use it

For `--index-store` the derived-from-store fallback is applied after all config sources:

```
CLI --index-store  >  defaults.index-store  >  server index-store  >  derived from --store
```

### JSON shape

```json
{
  "defaults": {
    "digest": "sha512-256",
    "stores": ["s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"],
    "index-store": "s3+https://s3.amazonaws.com/my-bucket/lfs/index/",
    "chunk-size": "16:64:256"
  }
}
```

| Key                    | Type             | Description                                                                                                                                                                      |
| ---------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `defaults.stores`      | array of strings | Chunk store(s). The first entry is used as the single store for `git-lfs-desync`. Additional entries are ignored by this command (but used by multi-store `desync` subcommands). |
| `defaults.index-store` | string           | Index store URL or path.                                                                                                                                                         |
| `defaults.digest`      | string           | Digest algorithm (`sha512-256` or `sha256`).                                                                                                                                     |
| `defaults.chunk-size`  | string           | Min:avg:max chunk size in KB, e.g. `"16:64:256"`.                                                                                                                                |
| `defaults.max-in-flight` | integer        | Maximum total in-flight bytes across all processes. `0` to disable. Overridden by `--max-in-flight` flag or `DESYNC_MAX_INFLIGHT` env var.                                       |
| `defaults.max-storage-ops` | integer      | Maximum concurrent storage operations across all processes. `0` to disable. Overridden by `--max-storage-ops` flag or `DESYNC_MAX_STORAGE_OPS` env var.                          |

### Example: store URL in config, no per-invocation flags

```sh
cat ~/.config/desync/config.json
```

```json
{
  "s3-credentials": {
    "https://s3.amazonaws.com": {
      "access-key": "AKIAIOSFODNN7EXAMPLE",
      "secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "aws-region": "us-east-1"
    }
  },
  "defaults": {
    "stores": ["s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"]
  },
  "store-options": {
    "s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/": {
      "safe-pruning": true
    }
  }
}
```

LFS agent config (no `--store` needed):

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    concurrent = true
    concurrenttransfers = 5

[lfs]
    standalonetransferagent = desync
```

The store URL can also come from a git-resident config via `--config-from-git`, which is useful for sharing a team config without per-machine setup. Use `%(remote)` in the object name to automatically select the config for the remote being operated on.

---

## Credentials

### S3

S3 credentials are resolved in this order:

1. **Environment variables** `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION` (or the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`).
2. **desync config file** (`~/.config/desync/config.json`) — per-endpoint credentials keyed by `http://host` or `https://host`.
3. **AWS shared credentials file** — referenced from the desync config via `aws-credentials-file` and optional `aws-profile`.

Example config file with static credentials for a specific endpoint:

```json
{
  "s3-credentials": {
    "https://s3.amazonaws.com": {
      "access-key": "AKIAIOSFODNN7EXAMPLE",
      "secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "aws-region": "us-east-1"
    }
  },
  "store-options": {
    "s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/": {
      "safe-pruning": true
    }
  }
}
```

### SFTP

SFTP stores authenticate via the SSH agent or `~/.ssh` keys. No extra configuration is required beyond having a valid SSH key for the host.

### GCS

GCS stores use [application default credentials](https://cloud.google.com/docs/authentication/application-default-credentials). Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`.

## Config from Git (`--config-from-git`)

`git-lfs-desync` is invoked by Git itself during clone, fetch, and push operations, with the working directory set to the repository root. This means the desync config — containing S3 credentials and store options — normally needs to live at a fixed path on each developer's machine, making it awkward to onboard contributors or run in CI environments.

`--config-from-git <object>` solves this by reading the config JSON directly from the repository's object database via `git cat-file --textconv <object>`. The object name can be any git ref, tree path, or blob — for example, a file on a dedicated branch that never mingles with the main working tree.

### Template expansion

The `--config-from-git` value is a template: `%(remote)` is replaced with the remote name from the LFS init message, and `%(operation)` is replaced with the operation (`upload` or `download`). This lets you parameterise the config object name so the correct config is loaded automatically based on context. If the remote name is not available — for example when the smudge filter invokes the agent during `git clone` — `%(remote)` defaults to `origin`.

For example:

```ini
args = --config-from-git %(remote)/_desync:config.json
```

When git-lfs triggers an upload from `origin`, the agent loads `origin/_desync:config.json`. When pushing to `upstream`, it loads `upstream/_desync:config.json`. This is useful for repositories with multiple remotes that use different storage backends.

If `%(remote)` or `%(operation)` appear in the template but the init message does not supply a value (empty string), the placeholder is replaced with an empty string.

### Storing the config in a `_desync` branch

A clean pattern is to keep the config on an orphan branch named `_desync`. Because this branch has no parent commits, the credentials never appear in the normal commit log. The branch can be pushed to and fetched from the remote independently of the code history.

#### 1. Create the orphan branch and commit the config

```sh
# Stash any in-progress work first, as the orphan checkout clears the index
git stash

git checkout --orphan _desync
git rm -rf .    # clear the index; leaves the working tree clean for our new file

cat > config.json <<'EOF'
{
  "s3-credentials": {
    "https://s3.amazonaws.com": {
      "access-key": "AKIAIOSFODNN7EXAMPLE",
      "secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "aws-region": "us-east-1"
    }
  },
  "defaults": {
    "stores":      ["s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"],
    "index-store": "s3+https://s3.amazonaws.com/my-bucket/lfs/index/"
  },
  "store-options": {
    "s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/": {
      "safe-pruning": true
    }
  }
}
EOF

git add config.json
git commit -m "desync config"
git push origin _desync

# Return to the previous branch and restore any stashed work
git checkout -
git stash pop
```

#### 2. Reference it in the LFS agent args

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --config-from-git %(remote)/_desync:config.json \
           --cache ~/.cache/desync/chunks
    concurrent = true
    concurrenttransfers = 5

[lfs]
    standalonetransferagent = desync
```

The store and index store URLs are read from `defaults` in the committed config, so no `--store` flag is needed here.

A local cache (`--cache`) is strongly recommended. Because desync stores deduplicated chunks, many chunks are shared across different files and commits. Without a cache every download fetches each chunk from the remote store, even if it was retrieved moments ago for another file. With a cache, chunks are saved to a local directory on first download and served from there on all subsequent accesses — making repeated checkouts, branch switches, and multi-file pulls significantly faster. Create the directory before first use:

```sh
mkdir -p ~/.cache/desync/chunks
```

`git clone` fetches all remote tracking branches by default, so `origin/_desync` is available immediately after cloning without a separate `git fetch`. On machines that already have the repository checked out before `_desync` was pushed, run `git fetch origin _desync` once.

> **Security note:** Anyone with read access to the remote can read the credentials on the `_desync` branch. Use this pattern only when the remote is private and access-controlled. If finer-grained access control is needed (e.g. read access to code but not to S3 keys), store credentials in environment variables or a local file via `--config` instead.

---

## Server-Provided Config

When the server supports SSH transfer negotiation (see [git-lfs-transfer: Transfer Negotiation](git-lfs-transfer.md#transfer-negotiation)), `git-lfs-desync` can be auto-negotiated without any store URLs or credentials in the client config. The server sends the desync config in the standard JSON format via the `config` field of the LFS init message.

### Minimal client setup (auto-negotiated)

When the server advertises the desync transfer, clients only need the agent binary registered:

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --cache ~/.cache/desync/chunks
```

No `--store`, `--index-store`, `standalonetransferagent`, or credentials are needed — they come from the server automatically.

### How it works

1. Git LFS connects to the server via SSH and discovers the desync transfer is available (via the `transfers=` capability or the `git-lfs-authenticate` response).
2. Git LFS obtains the desync config from the server (via the `authenticate` command or the `transfers` JSON field).
3. Git LFS selects `git-lfs-desync` as the standalone transfer agent and passes the server config in the init message's `config` field.
4. `git-lfs-desync` parses the config and uses it for store URLs, chunk parameters, and (if advertised) credentials.

### Merge rules

Server config is merged with local config (CLI flags, config file, `--config-from-git`) via the `MergeServerConfig` function. Fields are classified into two categories:

**Server-mergeable fields** — included in the server config and merged into the client:

| Setting | Rule |
| ------- | ---- |
| Store URLs | CLI flag > local config > server *(fills gaps)* |
| Index store URL | CLI flag > local config > server *(fills gaps)* > derived from store |
| S3 credentials | Local config > server *(fills gaps; server only sends if `with-credentials`)* |
| Store options | Local config > server *(fills gaps)* |
| **Chunk size** | **Server overrides** *(must match for data compatibility)* |
| **Digest** | **Server overrides** *(must match for chunk ID consistency)* |
| **Safe-pruning** | **Server wins if enabled** *(client cannot disable it)* |

- *Fills gaps* means server values are used only when the local config doesn't have them.
- *Overrides* means the server's value always takes precedence for protocol compatibility.

**Local-only fields** — never sent by the server, never merged from server config:

- `cache`, `cache-max-size`, `cache-max-files`, `cache-partitions`
- `concurrency`, `max-in-flight`, `max-storage-ops`, `conn-pool-size`

---

## Git Configuration

The transfer agent config must be set on every machine that pushes or pulls LFS objects. The simplest place is `~/.gitconfig` (global), so it applies to all repos.

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/ \
           --safe-pruning
    concurrent = true
    concurrenttransfers = 5

[lfs]
    standalonetransferagent = desync
```

`concurrent = true` (the default) tells git-lfs to launch multiple agent processes in parallel, splitting the transfer workload between them. `concurrenttransfers` (default 3 in git-lfs) controls how many agent processes run simultaneously. Cross-process coordination — total in-flight bytes and concurrent storage operations — is handled via shared memory (see `--max-in-flight` and `--max-storage-ops`).

With `standalonetransferagent` set, git-lfs bypasses the normal LFS HTTP API entirely — no LFS server is needed.

### Pipelined mode

By default, git-lfs spawns one agent process per concurrent transfer (controlled by `concurrenttransfers`). Each process handles one LFS object at a time — git-lfs sends a transfer request and waits for the completion response before sending the next.

**Pipelined mode** allows a single agent process to handle multiple transfers concurrently. When both the client (`supportspipelined` in the init message) and the agent (not `--no-pipelined`) support it, git-lfs sends all transfer requests to a single process without waiting for completion. The agent dispatches them internally using goroutines:

- **Lightweight checks** (e.g. `HasIndex` to skip already-uploaded objects) run at full concurrency.
- **Heavy work** (chunking, S3 uploads, file assembly) is limited to `--max-concurrent-uploads` parallel operations (default 8).

Pipelined mode reduces process startup overhead and enables better sharing of connection pools and caches. It is the preferred mode when git-lfs supports it.

To disable pipelined mode (e.g. for debugging or compatibility), pass `--no-pipelined`. The agent will then process one object at a time per process, with parallelism achieved by spawning multiple agent processes as before.

### Per-repo config

To scope the agent to a single repository instead of globally:

```sh
git config lfs.customtransfer.desync.path /usr/local/bin/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/ --safe-pruning"
git config lfs.customtransfer.desync.concurrent true
git config lfs.standalonetransferagent desync
```

### Tracking file patterns

```sh
git lfs track "*.bin" "*.iso" "*.tar.gz"
git add .gitattributes
git commit -m "Track large files with LFS"
```

## Upload and Download

Uploads happen on `git push`. Downloads happen on `git checkout` / `git clone` / `git lfs pull`. Credentials must be available (via environment or config) on the machine performing each operation.

```sh
# Upload (push triggers LFS transfer)
S3_ACCESS_KEY=... S3_SECRET_KEY=... git push origin main

# Download in a fresh clone (configure agent first, then pull)
git clone <git-remote> myrepo
cd myrepo
git config lfs.customtransfer.desync.path /usr/local/bin/git-lfs-desync
git config lfs.customtransfer.desync.args "--store s3+https://... --safe-pruning"
git config lfs.customtransfer.desync.concurrent true
git config lfs.standalonetransferagent desync
S3_ACCESS_KEY=... S3_SECRET_KEY=... git lfs pull
```

## Storage Layout

Given `--store s3+https://host/bucket/lfs/chunks/`, the agent writes:

```
s3://bucket/lfs/chunks/<xx>/<64-hex-chars>.cacnk   # compressed chunks
s3://bucket/lfs/index/<lfs-oid>.caibx              # index per LFS object
```

The index store defaults to `s3+https://host/bucket/lfs/index/` (last path segment of `--store` replaced with `index`). For local paths, e.g. `--store /data/lfs/chunks`, the index defaults to `/data/lfs/index`. Override with `--index-store` if needed.

---

## Index and Chunk Pruning

Over time, LFS objects may be removed from a repository (e.g. by deleting or force-pushing branches, or by rewriting history). The corresponding `.caibx` index files and the chunks they reference remain in the stores and accumulate as stale entries. The full cleanup is a two-step process involving both `desync index-prune` and `desync prune`.

`git-lfs-desync --indexes` translates the live LFS OID set (from `git lfs ls-files --all --long`) into the corresponding index names for use with `desync index-prune`.

Depending on whether concurrent uploads or downloads may be in progress, two approaches are available.

### Immediate pruning (no concurrent operations)

Use this approach only when it is guaranteed that no upload or download is running concurrently. If that guarantee cannot be made, data loss can occur.

**Step 1 — prune stale indexes:**

```sh
git lfs ls-files --all --long \
  | git-lfs-desync --indexes \
  | desync index-prune --index-store /path/to/indexes --yes -
```

**Step 2 — prune orphaned chunks** (using the indexes that remain after Step 1 as the keep set):

```sh
desync prune -s /path/to/chunks --index-store /path/to/indexes --yes
```

### Safe pruning (concurrent operations possible)

Use this approach when uploads or downloads may be running at the same time as the pruning job. The order is reversed compared to immediate pruning: chunks are pruned first, then indexes.

**Prerequisite — enable safe pruning in the upload agent.** The `safe-pruning` flag must be set so that upload operations participate in the safe pruning protocol. It can be configured in two ways:

- **Via the desync config file** (recommended when a config file is already in use):

```json
{
  "store-options": {
    "s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/": {
      "safe-pruning": true
    }
  }
}
```

- **Via the LFS agent `args`** (when no config file is used):

```ini
args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/ --safe-pruning
```

**Step 1 — prune orphaned chunks** (using _all_ current indexes — including stale ones — as the keep set):

```sh
desync prune -s /path/to/chunks --index-store /path/to/indexes --safe-pruning --yes
```

Because stale indexes are still present, the chunks they reference are included in the keep set and are not deleted yet. Only chunks not referenced by _any_ index are removed. `--safe-pruning` additionally uses the two-run protocol to avoid deleting chunks that a concurrent upload is in the process of writing (see [doc/safe-pruning.md](safe-pruning.md)).

**Step 2 — prune stale indexes:**

```sh
git lfs ls-files --all --long \
  | git-lfs-desync --indexes \
  | desync index-prune --index-store /path/to/indexes --safe-index-pruning --yes -
```

`--safe-index-pruning` uses the two-run protocol to avoid racing with concurrent uploads (see [doc/safe-pruning-index.md](safe-pruning-index.md)). After this step, the indexes for removed LFS objects are gone, but the chunks they referenced are still present in the store. They will be collected the next time Step 1 is run.

This order ensures that a concurrent download that started before Step 2 can still retrieve all its chunks, since those chunks are only removed in a future pruning cycle.

**Concurrency constraints.** Concurrent uploads and downloads are allowed while a safe pruning operation is in progress. However, only a single pruning operation should run at any given time. Additionally, there must be a sufficient time interval between two consecutive pruning runs: any upload or download that was in progress during the first run must have fully completed before the second run starts. Running the two steps back-to-back without waiting would defeat the safety guarantees of the two-run protocol.

### Passing OIDs as arguments

OIDs can also be supplied as positional arguments instead of via stdin:

```sh
git-lfs-desync --indexes \
    abc123def4560000cafe1234dead5678 \
    0011223344556677aabbccddeeff0011
```

### Notes

- `--indexes` mode does not read any store configuration. `--store`, `--config`, and all other store-related flags are ignored.
- The index name format is `<oid[0:2]>/<oid[2:4]>/<oid[4:]>.caibx`, matching how `git-lfs-desync` stores indexes during upload and Forgejo's `Pointer.RelativePath()` convention.

---

## Download Cache

The `--cache` flag adds a local fast-path in front of the remote chunk store. On download:

1. The agent looks up each chunk in the cache first.
2. On a miss, the chunk is fetched from `--store` and written to the cache automatically.
3. Subsequent downloads of the same file (or any file sharing chunks) are served entirely from the cache without touching the remote store.

Uploads are not affected — chunks are always written directly to `--store`.

### Example: S3 store with a local cache

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/ \
           --cache /var/cache/lfs/chunks \
           --safe-pruning
    concurrent = true
```

The cache directory must exist before the agent runs:

```sh
mkdir -p /var/cache/lfs/chunks
```

### Example: SFTP store with a local cache

```sh
git config lfs.customtransfer.desync.args \
    "--store sftp://user@fileserver/lfs/chunks \
     --cache ~/.cache/lfs/chunks \
     --safe-pruning"
```

### Cache size limit

The cache can be limited by total size (`--cache-max-size`) and/or file count (`--cache-max-files`). When a limit is exceeded after a write, the oldest chunks (by last access time) are automatically evicted from the partition with the most files. No manual cleanup is needed. Multiple concurrent agent processes can safely share the same cache — size tracking uses lock-free atomic counters in a persistent shared-memory file.

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/ \
           --cache /var/cache/lfs/chunks \
           --cache-max-size 10G \
           --safe-pruning
    concurrent = true
```

Or via the config file:

```json
{
  "defaults": {
    "cache": "/var/cache/lfs/chunks",
    "cache-max-size": "10G"
  }
}
```

See [doc/cache-size-limit.md](cache-size-limit.md) for the full design.

### Cache repair

With the default `--cache-repair=true`, if a cached chunk fails its checksum the agent discards it, re-fetches the chunk from `--store`, and repopulates the cache. Set `--cache-repair=false` to disable this behaviour (the download will then fail on a corrupt chunk).

---

## Local Directory (simplest setup)

No cloud account needed. Useful for single-machine setups or testing.

```sh
mkdir -p /data/lfs/chunks /data/lfs/index
```

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store /data/lfs/chunks --index-store /data/lfs/index
    concurrent = true

[lfs]
    standalonetransferagent = desync
```

Or let `--index-store` be derived automatically:

```ini
args = --store /data/lfs/chunks
# index store defaults to /data/lfs/index
```

---

## Local Testing with MinIO (S3 path)

> **Warning:** The MinIO instance started below uses well-known default credentials (`minioadmin`/`minioadmin`) and listens on an unencrypted HTTP port. Do not expose it to the internet or any untrusted network. Keep it bound to `localhost` and tear it down when you are done testing.

The following steps reproduce the full upload/download cycle locally using [MinIO](https://min.io/) as an S3-compatible backend, testing the `s3+http://` code path without any AWS account.

### 1. Start MinIO

```sh
docker run -d \
  --name minio-lfs \
  -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
```

Create a bucket:

```sh
docker exec minio-lfs mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio-lfs mc mb local/lfs-test
```

### 2. Build the agent

```sh
go build -o /tmp/git-lfs-desync ./cmd/git-lfs-desync
```

### 3. Set up a local cache directory

```sh
mkdir -p /tmp/lfs-cache
```

### 4. Set up a test repository

```sh
mkdir /tmp/lfs-repo && cd /tmp/lfs-repo
git init
git lfs install

# Point LFS at the local agent, MinIO bucket, and local cache
git config lfs.customtransfer.desync.path /tmp/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+http://localhost:9000/lfs-test/chunks/ \
     --index-store s3+http://localhost:9000/lfs-test/index/ \
     --cache /tmp/lfs-cache \
     --safe-pruning"
git config lfs.standalonetransferagent desync
# Standalone mode requires a dummy lfs.url (no real LFS server)
git config lfs.url "https://localhost"

git lfs track "*.bin"
git add .gitattributes
git commit -m "Track .bin files with LFS"
```

### 5. Add a large file and push

```sh
# Create a test file (20 MB of random data)
dd if=/dev/urandom of=large.bin bs=1M count=20
ORIGINAL_SHA=$(sha256sum large.bin)

git add large.bin
git commit -m "Add large test file"

# Create a bare repo to push to (simulates a remote)
git init --bare /tmp/lfs-bare
git remote add origin /tmp/lfs-bare

S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin git push origin master
```

Verify chunks and index landed in MinIO:

```sh
docker exec minio-lfs mc ls --recursive local/lfs-test/index/
# → .../<oid>.caibx

docker exec minio-lfs mc ls local/lfs-test/chunks/ | wc -l
# → number of chunks (e.g. 327 for a 20 MB random file)
```

The cache is not populated on upload — only downloads fill the cache.

### 6. Clone and verify download (cache populated on first pull)

> **Note:** Use `GIT_LFS_SKIP_SMUDGE=1` during the clone. Without it, git-lfs
> triggers the smudge filter during checkout — before the agent is configured in
> the clone — and the download fails. Configure the agent first, then run
> `git lfs pull` to fetch the objects.

```sh
GIT_LFS_SKIP_SMUDGE=1 git clone /tmp/lfs-bare /tmp/lfs-clone
cd /tmp/lfs-clone

# Configure agent in the clone with the same local cache
git config lfs.customtransfer.desync.path /tmp/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+http://localhost:9000/lfs-test/chunks/ \
     --index-store s3+http://localhost:9000/lfs-test/index/ \
     --cache /tmp/lfs-cache \
     --safe-pruning"
git config lfs.standalonetransferagent desync
git config lfs.url "https://localhost"

S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin git lfs pull

# Verify integrity
sha256sum large.bin
# Must match $ORIGINAL_SHA
```

After the pull, chunks are stored in `/tmp/lfs-cache`. Verify:

```sh
ls /tmp/lfs-cache/ | wc -l
# → subdirectory entries (chunks are stored in two-character prefix directories)
```

A second `git lfs pull` (or any clone using the same `--cache` directory) will be served from the local cache without hitting MinIO.

### 7. Cleanup

```sh
docker rm -f minio-lfs
rm -rf /tmp/lfs-repo /tmp/lfs-bare /tmp/lfs-clone /tmp/lfs-cache /tmp/git-lfs-desync
```

---

## Local Testing with Garage and config-from-git

[Garage](https://garagehq.deuxfleurs.fr/) is a self-hosted S3-compatible object store. This section tests the full upload/download cycle using Garage as the backend and `--config-from-git` to supply credentials — so that `git clone` can download LFS objects during the initial checkout without any per-machine configuration.

> **Warning:** The Garage instance below serves the S3 API over unencrypted HTTP. For production use, place an HTTPS reverse proxy in front of it. Keep it bound to `localhost` and tear it down when you are done testing.

### 1. Start Garage

Generate a config file with a random RPC secret and start the container:

```sh
mkdir -p /tmp/garage-meta /tmp/garage-data

cat > /tmp/garage.toml << EOF
metadata_dir = "/tmp/garage-meta"
data_dir     = "/tmp/garage-data"
replication_factor = 1
rpc_bind_addr   = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret      = "$(openssl rand -hex 32)"

[s3_api]
s3_region    = "garage"
api_bind_addr = "[::]:3900"
root_domain  = ".s3.garage"
EOF

docker run -d \
  --name garage-lfs \
  -p 3900:3900 \
  -v /tmp/garage.toml:/etc/garage.toml \
  -v /tmp/garage-meta:/tmp/garage-meta \
  -v /tmp/garage-data:/tmp/garage-data \
  dxflrs/garage:v2.2.0
```

Apply a single-node layout, create a bucket, and create an API key:

```sh
# Apply layout (Garage assigns to the only available node automatically)
docker exec garage-lfs /garage layout assign -z dc1 -c 1G
docker exec garage-lfs /garage layout apply --version 1

# Create bucket
docker exec garage-lfs /garage bucket create lfs-test

# Create key and capture credentials
docker exec garage-lfs /garage key create lfs-key
# → outputs Key ID and Secret key — save these for the next step

# Grant the key read+write access
docker exec garage-lfs /garage bucket allow --read --write lfs-test --key lfs-key
```

### 2. Build the agent

```sh
go build -o /tmp/git-lfs-desync ./cmd/git-lfs-desync
```

### 3. Set up a local cache directory

```sh
mkdir -p /tmp/lfs-cache-garage
```

### 4. Set up a test repository

```sh
mkdir /tmp/garage-repo && cd /tmp/garage-repo
git init
git lfs install

# Configure LFS agent for pushing (direct store args with S3 credentials from env)
git config lfs.customtransfer.desync.path /tmp/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+http://localhost:3900/lfs-test/chunks/ \
     --index-store s3+http://localhost:3900/lfs-test/index/"
git config lfs.customtransfer.desync.concurrent true
git config lfs.standalonetransferagent desync
git config lfs.url "https://localhost"

git lfs track "*.bin"
git add .gitattributes
git commit -m "Track .bin files with LFS"
```

### 5. Commit the desync config on an orphan `_desync` branch

Replace `<KEY_ID>` and `<SECRET_KEY>` with the values printed by `garage key create` above.

```sh
cd /tmp/garage-repo
git checkout --orphan _desync
git rm -rf .

cat > config.json << 'EOF'
{
  "s3-credentials": {
    "http://localhost:3900": {
      "access-key": "<KEY_ID>",
      "secret-key": "<SECRET_KEY>",
      "aws-region": "garage"
    }
  },
  "defaults": {
    "stores":      ["s3+http://localhost:3900/lfs-test/chunks/"],
    "index-store": "s3+http://localhost:3900/lfs-test/index/"
  },
  "store-options": {
    "s3+http://localhost:3900/lfs-test/chunks/": {
      "safe-pruning": true
    }
  }
}
EOF

git add config.json
git commit -m "desync config"
git checkout master
```

The `aws-region` value (`"garage"`) must match the `s3_region` set in `garage.toml`.

### 6. Add a large file and push

```sh
cd /tmp/garage-repo
dd if=/dev/urandom of=large.bin bs=1M count=20
ORIGINAL_SHA=$(sha256sum large.bin)

git add large.bin
git commit -m "Add large test file"

# Create a bare repo to push to (simulates a remote)
git init --bare /tmp/garage-bare
git remote add origin /tmp/garage-bare

# Push master (LFS objects go to Garage) and the _desync config branch
S3_ACCESS_KEY=<KEY_ID> S3_SECRET_KEY=<SECRET_KEY> git push origin master
git push origin _desync
```

Verify that chunks and the index landed in Garage:

```sh
docker exec garage-lfs /garage bucket info lfs-test
# → shows object count and total size
```

### 7. Clone with LFS working during the initial checkout

Pass the LFS agent config via `git -c` so the smudge filter can download LFS objects during `git clone` — no `GIT_LFS_SKIP_SMUDGE=1` or follow-up `git lfs pull` needed.

```sh
git \
  -c lfs.customtransfer.desync.path=/tmp/git-lfs-desync \
  -c 'lfs.customtransfer.desync.args=--config-from-git %(remote)/_desync:config.json --cache /tmp/lfs-cache-garage' \
  -c lfs.standalonetransferagent=desync \
  -c lfs.url=https://localhost \
  clone /tmp/garage-bare /tmp/garage-clone

# Verify integrity
sha256sum /tmp/garage-clone/large.bin
# Must match $ORIGINAL_SHA
```

The agent reads `origin/_desync:config.json` from the cloned repository's object database to obtain S3 credentials — no credentials are needed on the command line or in any local config file.

After the clone, chunks are cached in `/tmp/lfs-cache-garage`:

```sh
ls /tmp/lfs-cache-garage/ | wc -l
# → subdirectory entries (chunks stored in two-character prefix directories)
```

A second clone (or `git lfs pull`) using the same `--cache` directory is served entirely from the local cache.

### 8. Cleanup

```sh
docker rm -f garage-lfs
# Garage writes data as root inside the container; use Docker to remove it
docker run --rm \
  -v /tmp/garage-meta:/data/meta \
  -v /tmp/garage-data:/data/chunks \
  alpine sh -c 'rm -rf /data/meta/* /data/chunks/*'
rmdir /tmp/garage-meta /tmp/garage-data
rm -rf /tmp/garage-repo /tmp/garage-bare /tmp/garage-clone \
       /tmp/lfs-cache-garage /tmp/garage.toml /tmp/git-lfs-desync
```
