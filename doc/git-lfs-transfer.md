# git-lfs-transfer

`git-lfs-transfer` is a server-side implementation of the [Git LFS SSH transfer protocol](https://github.com/git-lfs/git-lfs/blob/main/docs/proposals/ssh_adapter.md). It allows Git LFS clients to push and pull large files over SSH, with desync's content-defined chunking as the storage backend.

Unlike [`git-lfs-desync`](git-lfs-desync.md) — which is a client-side custom transfer agent — `git-lfs-transfer` runs on the server and speaks the pkt-line based SSH adapter protocol directly. This means clients use Git LFS's built-in SSH transport and do not need any custom transfer agent installed.

## How It Works

1. The Git LFS client connects to the server over SSH and invokes `git-lfs-transfer <path> <operation>`.
2. The server advertises capabilities and negotiates protocol version 1.
3. The client sends commands (`batch`, `put-object`, `get-object`, `verify-object`) using the pkt-line framing protocol.
4. On upload, the server receives the file, chunks it using desync's rolling hash, stores the chunks, and saves the index keyed by the LFS OID.
5. On download, the server looks up the index by OID, assembles the file from chunks, and streams it back.

## Building

```sh
go install github.com/folbricht/desync/cmd/git-lfs-transfer@latest
# or from source:
go build -o /usr/local/bin/git-lfs-transfer ./cmd/git-lfs-transfer
```

## Supported Backends

The same backends supported by desync are available:

| Protocol                 | URL Scheme                       | Notes                                      |
| ------------------------ | -------------------------------- | ------------------------------------------ |
| **Local filesystem**     | `/path/to/dir` or `./dir`        | No credentials needed; simplest setup      |
| **S3-compatible**        | `s3+https://host/bucket/prefix/` | AWS S3, MinIO, Ceph RGW, etc.              |
| **SFTP**                 | `sftp://user@host/path/`         | Uses SSH keys or agent                     |
| **HTTP/HTTPS**           | `https://host/path/`             | Requires a write-capable HTTP store server |
| **Google Cloud Storage** | `gs://bucket/prefix/`            | Uses GCS application default credentials   |

## Configuration

`git-lfs-transfer` is configured via a JSON file (`desync-lfs.json`), with a convention-based fallback for zero-configuration setups.

### Config File Resolution

The server resolves configuration in this order:

1. **Walk up from `<path>`** — Starting at the `<path>` argument (the repository path passed by the SSH command), walk up parent directories looking for a `desync-lfs.json` file. The first one found is used.
2. **Global fallback** — If no config file is found in the directory tree, look for `/etc/desync/desync-lfs.json`.
3. **Convention-based** — If no config file exists at all, use `<path>/desync-lfs/chunks` as the chunk store and `<path>/desync-lfs/index` as the index store.

### Config File Format

The config file uses the same JSON format as the main desync config (see the project README for the full schema), with these additional top-level keys specific to `git-lfs-transfer`:

| Key                       | Required | Default                          | Description                                                                                             |
| ------------------------- | -------- | -------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `store`                   | yes      | —                                | Chunk store location. See [Supported Backends](#supported-backends).                                    |
| `index-store`             | no       | derived from `store`             | Index store location. If omitted, derived by replacing the last path segment of `store` with `index`.   |
| `cache`                   | no       | —                                | Local chunk cache for downloads. Chunks are fetched from `store` on cache miss and saved locally.       |
| `chunk-size`              | no       | `16:64:256`                      | Min:avg:max chunk size in KB.                                                                           |
| `digest`                  | no       | `sha512-256`                     | Hash algorithm for chunk identification: `sha512-256` or `sha256`.                                      |
| `safe-pruning`            | no       | `false`                          | Enable the safe concurrent pruning protocol on uploads. See [Safe Pruning](#safe-pruning).              |
| `safe-propagation-time`   | no       | `1s`                             | Max store write propagation delay for the safe pruning protocol. Go duration format (e.g. `1s`, `30s`). |

Safe pruning options (`safe-pruning`, `safe-propagation-time`) can also be set per-store via `store-options`, and S3 credentials are configured in `s3-credentials` — both following the standard desync config format.

Example:

```json
{
  "store": "s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/",
  "index-store": "s3+https://s3.amazonaws.com/my-bucket/lfs/index/",
  "cache": "/var/cache/desync/chunks",
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

### Path Templates

Store paths in the config file support `%(path)` template expansion, which is replaced with the repository path (`<path>` argument). This is useful for per-repository storage:

```json
{
  "store": "%(path)/desync-lfs/chunks",
  "index-store": "%(path)/desync-lfs/index"
}
```

### Relative Paths

Relative local paths in the config file are resolved against the `<path>` argument. For example, with `<path>` = `/git/myrepo.git`:

```json
{
  "store": "desync-lfs/chunks"
}
```

resolves the store to `/git/myrepo.git/desync-lfs/chunks`.

URLs (paths with a scheme like `s3+https://`) and absolute paths are used as-is.

## Server Setup

### SSH Configuration

Git LFS invokes `git-lfs-transfer <path> <operation>` on the server via SSH. The binary must be in the server's `$PATH` or referenced by full path.

For a typical SSH-based Git hosting setup:

1. Install the binary on the server:

```sh
go build -o /usr/local/bin/git-lfs-transfer ./cmd/git-lfs-transfer
```

2. Ensure `git-lfs-transfer` is accessible to the SSH user (e.g. the `git` user). If using a restricted shell or `authorized_keys` with `command=`, make sure `git-lfs-transfer` is allowed.

### Convention-Based Setup (Zero Config)

The simplest setup requires no config file. Place the tool in `$PATH` and the server will automatically use `<path>/desync-lfs/chunks` and `<path>/desync-lfs/index` as the chunk and index stores. The directories are created automatically on first upload.

For example, with a repository at `/git/myrepo.git`, the first `git push` will create `/git/myrepo.git/desync-lfs/chunks/` and `/git/myrepo.git/desync-lfs/index/`.

### Per-Repository Config

Place a `desync-lfs.json` in the repository directory:

```sh
cat > /git/myrepo.git/desync-lfs.json << 'EOF'
{
  "store": "/data/lfs/chunks",
  "index-store": "/data/lfs/index"
}
EOF
```

Store directories are created automatically on first upload.

### Shared Config for Multiple Repositories

Place a `desync-lfs.json` in a common parent directory. For example, if repositories live under `/git/`:

```sh
cat > /git/desync-lfs.json << 'EOF'
{
  "store": "%(path)/desync-lfs/chunks",
  "index-store": "%(path)/desync-lfs/index"
}
EOF
```

The `%(path)` template ensures each repository gets its own storage directory.

### Global Config

Place the config at `/etc/desync/desync-lfs.json` as a fallback for all repositories:

```sh
cat > /etc/desync/desync-lfs.json << 'EOF'
{
  "store": "s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/",
  "s3-credentials": {
    "https://s3.amazonaws.com": {
      "access-key": "AKIAIOSFODNN7EXAMPLE",
      "secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "aws-region": "us-east-1"
    }
  }
}
EOF
```

## Client Setup

Clients need Git LFS 3.0+ with SSH transfer protocol support. Configure the repository to use SSH transport for LFS:

```sh
# Point LFS at the SSH remote
git config lfs.url "ssh://git@server.example.com/git/myrepo.git"
```

No custom transfer agent is needed on the client — Git LFS uses its built-in SSH transport.

## Storage Layout

The storage layout is identical to [`git-lfs-desync`](git-lfs-desync.md), making the two tools interoperable:

```
<store>/
  <xx>/<64-hex-chars>.cacnk     # compressed chunks (sharded by first 2 hex chars)

<index-store>/
  <xxxx>/<oid>.caibx             # index per LFS object (sharded by first 4 OID chars)
```

The index name for an LFS OID is `<oid[0:4]>/<oid>.caibx`. This sharding scheme matches the layout used by `git-lfs-desync`, so files uploaded by either tool can be downloaded by the other.

## Protocol Commands

`git-lfs-transfer` implements the following commands from the Git LFS SSH transfer protocol:

| Command           | Description                                                                                   |
| ----------------- | --------------------------------------------------------------------------------------------- |
| `batch`           | Checks which objects exist. Returns `download`/`upload`/`noop` action for each OID.           |
| `put-object`      | Receives a file, verifies its SHA-256 matches the OID, chunks it, and stores chunks + index.  |
| `get-object`      | Looks up the index by OID, assembles the file from chunks, and streams it back to the client. |
| `verify-object`   | Confirms an object exists and its size matches (404 if missing, 409 if size mismatch).        |
| `quit`            | Graceful shutdown.                                                                            |

## Safe Pruning

When `safe-pruning` is enabled in the config, the server participates in the safe concurrent pruning protocol during uploads. This prevents data loss when `desync prune --safe-pruning` runs concurrently with uploads.

See [safe-pruning.md](safe-pruning.md) for details on the protocol.

## Differences from git-lfs-desync

| Aspect              | `git-lfs-desync` (client)                        | `git-lfs-transfer` (server)                      |
| ------------------- | ------------------------------------------------ | ------------------------------------------------ |
| **Where it runs**   | Client machine                                   | Server (invoked via SSH)                          |
| **Protocol**        | Git LFS custom transfer agent (stdin/stdout JSON) | Git LFS SSH transfer protocol (pkt-line)         |
| **Client setup**    | Custom transfer agent in `.gitconfig`            | Only `lfs.url` pointing to SSH remote             |
| **Config**          | CLI flags + `~/.config/desync/config.json`       | `desync-lfs.json` (walk-up + global fallback)     |
| **Interoperable**   | Yes — same storage layout                        | Yes — same storage layout                         |
