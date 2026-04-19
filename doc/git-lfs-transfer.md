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

`git-lfs-transfer` is configured via a JSON file (`desync-lfs.json`). **If no config file is found at any of the locations below, desync LFS is considered disabled for that repository** and the server either delegates to another binary (see [Escape Hatch](#escape-hatch--delegation)) or rejects the session with a `403`. This makes it safe to install `git-lfs-transfer` on a server that already hosts repositories using other LFS backends — only repositories that are explicitly configured are affected.

### Config File Resolution

The server resolves configuration in this order:

1. **`desync-lfs.config.object` git config key** — If the repository at `<path>` has this key set (e.g. `_desync:config.json`), the config is read from that git object via `git cat-file --textconv`. If the object is absent, resolution continues to the next step.
2. **`desync-lfs.config.path` git config key** — If set, overrides the config filename to search for. The value may be an absolute path (used directly) or a relative filename (searched by walking up directories, same as step 3). Defaults to `desync-lfs.json`.
3. **Walk up from `<path>`** — Walk up parent directories looking for the config filename. The first match is used.
4. **Global fallback** — If no config file is found in the directory tree, look for `/etc/desync/<filename>`.

If none of those steps locate a config file, the repository is treated as if `desync-lfs.transfer=false` was set: `git-lfs-transfer` hands off to the delegate configured with `desync-lfs.transfer.exec` if one is set, otherwise it returns a `403` to the client (see [Escape Hatch](#escape-hatch--delegation)).

The git config keys are read with `git -C <path> config <key>`. If `<path>` is not a git repository or git is not available, the keys are silently ignored and resolution continues from step 3.

To set these keys for a repository:

```sh
git -C /git/myrepo.git config desync-lfs.config.object '_desync:config.json'
git -C /git/myrepo.git config desync-lfs.config.path '/etc/myorg/lfs-config.json'
```

### Config File Format

The config file uses the same JSON format as the main desync `config.json` (see the project README for the full schema). The relevant fields are:

- **`defaults.stores`** — single-element array with the chunk store location (required). See [Supported Backends](#supported-backends).
- **`defaults.index-store`** — index store location. If omitted, derived by replacing the last path segment of the store with `index`.
- **`defaults.cache`** — local chunk cache for downloads.
- **`defaults.cache-max-size`** — maximum cache size (e.g. `"10G"`). Overridable via `DESYNC_CACHE_MAX_SIZE` env var. See [doc/cache-size-limit.md](cache-size-limit.md).
- **`defaults.cache-max-files`** — maximum number of cached files. Overridable via `DESYNC_CACHE_MAX_FILES` env var.
- **`defaults.cache-partitions`** — number of eviction partitions, power of 2 (default `256`).
- **`defaults.chunk-size`** — min:avg:max chunk size in KB (default `16:64:256`).
- **`defaults.digest`** — hash algorithm: `sha512-256` (default) or `sha256`.
- **`defaults.concurrency`** — number of concurrent goroutines for chunk I/O (default `10`).
- **`defaults.max-in-flight`** — maximum total in-flight bytes across all concurrent `git-lfs-transfer` processes, e.g. `"2G"`, `"500M"`, or raw bytes as a string. `0` to disable (default). Overridable via `DESYNC_MAX_INFLIGHT` env var. When multiple SSH connections are open (`lfs.concurrenttransfers > 1`), each connection spawns a separate process; this limit coordinates them via shared memory.
- **`defaults.max-storage-ops`** — maximum concurrent storage operations (GetChunk, StoreChunk, GetIndex, StoreIndex) across all processes. `0` to disable. Overridable via `DESYNC_MAX_STORAGE_OPS` env var. Limits S3/network backend load.
- **`store-options.<url>.safe-pruning`** — enable the safe concurrent pruning protocol on uploads. See [Safe Pruning](#safe-pruning).
- **`store-options.<url>.safe-propagation-time`** — max store write propagation delay for safe pruning. Go duration format, default `1s`.
- **`s3-credentials`** — S3 credentials per endpoint.

Example:

```json
{
  "defaults": {
    "stores": ["s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"],
    "index-store": "s3+https://s3.amazonaws.com/my-bucket/lfs/index/",
    "cache": "/var/cache/desync/chunks",
    "cache-max-size": "10G"
  },
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
  "defaults": {
    "stores": ["%(path)/desync-lfs/chunks"],
    "index-store": "%(path)/desync-lfs/index"
  }
}
```

### Relative Paths

Relative local paths in the config file are resolved against the `<path>` argument. For example, with `<path>` = `/git/myrepo.git`:

```json
{
  "defaults": {
    "stores": ["desync-lfs/chunks"]
  }
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

### Per-Repository Config

Place a `desync-lfs.json` in the repository directory:

```sh
cat > /git/myrepo.git/desync-lfs.json << 'EOF'
{
  "defaults": {
    "stores": ["/data/lfs/chunks"],
    "index-store": "/data/lfs/index"
  }
}
EOF
```

Store directories are created automatically on first upload.

### Shared Config for Multiple Repositories

Place a `desync-lfs.json` in a common parent directory. For example, if repositories live under `/git/`:

```sh
cat > /git/desync-lfs.json << 'EOF'
{
  "defaults": {
    "stores": ["%(path)/desync-lfs/chunks"],
    "index-store": "%(path)/desync-lfs/index"
  }
}
EOF
```

The `%(path)` template ensures each repository gets its own storage directory.

### Global Config

Place a config at `/etc/desync/desync-lfs.json` to enable desync-LFS across every repository on the server. This is the simplest way to replicate the old "zero-config" behavior where each repo stored its chunks next to itself:

```sh
cat > /etc/desync/desync-lfs.json << 'EOF'
{
  "defaults": {
    "stores": ["%(path)/desync-lfs/chunks"],
    "index-store": "%(path)/desync-lfs/index"
  }
}
EOF
```

With this file in place, any repository served through SSH will automatically get a per-repo `desync-lfs/chunks/` and `desync-lfs/index/` directory created on first upload. Repositories that should use a different LFS backend can opt out with `git config desync-lfs.transfer false` (see [Escape Hatch](#escape-hatch--delegation)).

A global config can also point at a shared backend — for example a single S3 bucket for the whole server:

```sh
cat > /etc/desync/desync-lfs.json << 'EOF'
{
  "defaults": {
    "stores": ["s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"]
  },
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

### Auto-negotiated desync agent (optional)

If the server has `desync-lfs.advertise` enabled and the client has `git-lfs-desync` installed, the desync transfer agent can be used automatically. The client only needs the agent binary registered — no store URLs, credentials, or `standalonetransferagent` config:

```sh
# Register the agent binary (globally or per-repo)
git config lfs.customtransfer.desync.path /usr/local/bin/git-lfs-desync
```

On the first LFS operation, git-lfs connects to the server via SSH, discovers the desync transfer is available, obtains the store config, and uses `git-lfs-desync` instead of the SSH protocol. Client-local settings like `--cache` can be added via `args`:

```sh
git config lfs.customtransfer.desync.args "--cache ~/.cache/desync/chunks"
```

## Storage Layout

The storage layout is identical to [`git-lfs-desync`](git-lfs-desync.md), making the two tools interoperable:

```
<store>/
  <xx>/<64-hex-chars>.cacnk     # compressed chunks (sharded by first 2 hex chars)

<index-store>/
  <xx>/<yy>/<rest>.caibx         # index per LFS object (sharded by first 2+2 OID chars)
```

The index name for an LFS OID is `<oid[0:2]>/<oid[2:4]>/<oid[4:]>.caibx`. This sharding scheme matches the layout used by `git-lfs-desync` and Forgejo's `Pointer.RelativePath()`, so files uploaded by any tool can be downloaded by the others.

## Protocol Commands

`git-lfs-transfer` implements the following commands from the Git LFS SSH transfer protocol:

| Command           | Description                                                                                   |
| ----------------- | --------------------------------------------------------------------------------------------- |
| `batch`           | Checks which objects exist. Returns `download`/`upload`/`noop` action for each OID.           |
| `put-object`      | Receives a file, verifies its SHA-256 matches the OID, chunks it, and stores chunks + index.  |
| `get-object`      | Looks up the index by OID, assembles the file from chunks, and streams it back to the client. |
| `verify-object`   | Confirms an object exists and its size matches (404 if missing, 409 if size mismatch).        |
| `authenticate`    | Returns transfer-specific config for a named transfer (see below).                            |
| `quit`            | Graceful shutdown.                                                                            |

## Transfer Negotiation

`git-lfs-transfer` can advertise that it supports the desync custom transfer agent, allowing clients with `git-lfs-desync` installed to auto-negotiate it instead of using the SSH protocol directly. This is controlled by a per-repository git config:

```sh
git -C /git/myrepo.git config desync-lfs.advertise <mode>
```

| Mode | Config fields sent |
| ---- | ------------------ |
| `no` / `false` (default) | Nothing — desync transfer is not advertised. Clients use the SSH protocol. |
| `without-credentials` | Store URLs, index URL, chunk size, digest. No S3 credentials, no store options. Clients must have their own credentials configured locally. |
| `with-credentials` | Store URLs, index URL, chunk size, digest, plus S3 credentials and store options **filtered to only include entries matching the active store/index URLs**. Unrelated credentials are never exposed. Use short-lived tokens with restricted permissions (no delete/overwrite). |

When advertising is enabled, the server sends a `transfers=desync,ssh` capability during version negotiation. Clients that have `git-lfs-desync` registered as a custom transfer agent can then send the `authenticate` command to obtain the desync config:

```
Client:  authenticate
         transfer=desync
         <flush-pkt>

Server:  status 200
         <delim-pkt>
         {"config":{"defaults":{"stores":[...],"index-store":"...","chunk-size":"16:64:256","digest":"sha512-256"},...},"expires_in":86400}
         <flush-pkt>
```

The config JSON uses the same format as desync config files. The server constructs it via `FilterServerConfig`, which:
- Always includes chunk size and digest (required for data compatibility).
- Never includes local-only fields (cache, concurrency, limits).
- In `with-credentials` mode, S3 credentials are matched by scheme+host (e.g. only the `https://s3.amazonaws.com` entry is sent for an `s3+https://s3.amazonaws.com/bucket/` store URL), and store options are matched by URL pattern. Credentials for unrelated endpoints are never exposed.

The client merges this config with its local config via `MergeServerConfig` (see [git-lfs-desync: Merge rules](git-lfs-desync.md#merge-rules)).

Old clients that do not support transfer negotiation ignore the `transfers=` capability and continue using the SSH protocol as before.

## Escape Hatch / Delegation

In some deployments a single `git-lfs-transfer` binary serves many repositories, but a subset of those repositories should use a different LFS implementation. The escape hatch lets you opt out of desync handling on a per-repository basis and forward the session to an alternate binary.

### Triggering the Escape Hatch

Either of the following is sufficient to activate the escape hatch for a repository:

**Git config key** (per-repository, no config file required):

```sh
git -C /git/myrepo.git config desync-lfs.transfer false
```

**JSON config key** (in `desync-lfs.json`):

```json
{
  "desync-lfs": false
}
```

When the JSON trigger is used, the file is still located via the normal [config resolution order](#config-file-resolution). Setting `"desync-lfs": false` in a shared parent-directory config disables desync handling for every repository that inherits that config.

The git config key is evaluated before the JSON config is loaded, so it takes effect even when config loading would otherwise fail.

### Configuring a Delegate

When the escape hatch is triggered, `git-lfs-transfer` looks for the `desync-lfs.transfer.exec` git config key. If it is set, the specified binary is executed with the same command-line arguments (`<path> <operation>`) and with stdin/stdout/stderr inherited — handing the pkt-line session off seamlessly:

```sh
git -C /git/myrepo.git config desync-lfs.transfer.exec /usr/local/bin/git-lfs-transfer-other
```

On Unix the current process is replaced via `execve(2)` (zero overhead, no intermediate buffering). On Windows a child process is started and its exit code is forwarded.

### Rejection When No Delegate Is Configured

If the escape hatch is triggered but `desync-lfs.transfer.exec` is not set — or if executing the delegate fails — `git-lfs-transfer` performs the minimum pkt-line handshake and sends a `403` error response to the client before exiting non-zero. The git LFS client will display the error message from the server.

A `403` from the pure-SSH transfer protocol also signals to the Git LFS client that it should not use the SSH adapter for this repository. The client will then fall back to its other transport mechanisms in order: the `git-lfs-authenticate` SSH helper (if available on the server), and finally the HTTPS endpoint derived from the remote URL. This means you can use the rejection path to silently redirect a repository to a different LFS backend — for example an HTTPS-based LFS server — without any client-side reconfiguration.

## Safe Pruning

When `safe-pruning` is enabled in the config, the server participates in the safe concurrent pruning protocol during uploads. This prevents data loss when `desync prune --safe-pruning` runs concurrently with uploads.

See [safe-pruning.md](safe-pruning.md) for details on the protocol.

## Local Testing with Docker (SSH)

> **Warning:** The Docker container started below uses a throwaway SSH key with no passphrase and a default SSH server configuration that is not hardened for production use. Keep it bound to `localhost` and tear it down when you are done testing.

The following steps reproduce the full upload/download cycle using an ephemeral Docker container running OpenSSH and our `git-lfs-transfer` binary, testing the plain-SSH deployment model described in [Server Setup](#server-setup).

### 1. Build the binary

Cross-compile a static binary for the container:

```sh
CGO_ENABLED=0 GOOS=linux go build -o /tmp/git-lfs-transfer ./cmd/git-lfs-transfer
```

### 2. Generate a throwaway SSH key pair

```sh
ssh-keygen -t ed25519 -f /tmp/lfs-test-key -N "" -q
```

### 3. Start an SSH-enabled container

```sh
docker run -d \
  --name lfs-ssh-test \
  -p 2222:22 \
  alpine:latest sh -c '
    apk add --no-cache openssh git git-lfs &&
    adduser -D -s /bin/sh git &&
    passwd -u git &&
    mkdir -p /home/git/.ssh &&
    ssh-keygen -A &&
    /usr/sbin/sshd -D -e
  '
```

Copy the binary and SSH key into the container:

```sh
docker cp /tmp/git-lfs-transfer lfs-ssh-test:/usr/local/bin/git-lfs-transfer
docker exec lfs-ssh-test chmod +x /usr/local/bin/git-lfs-transfer
docker cp /tmp/lfs-test-key.pub lfs-ssh-test:/home/git/.ssh/authorized_keys
docker exec lfs-ssh-test chown -R git:git /home/git/.ssh
docker exec lfs-ssh-test chmod 700 /home/git/.ssh
docker exec lfs-ssh-test chmod 600 /home/git/.ssh/authorized_keys
```

### 4. Create a bare repository on the server

```sh
docker exec -u git lfs-ssh-test git init --bare /home/git/test-repo.git
```

Drop a per-repo `desync-lfs.json` so the server knows to handle LFS uploads for this repository. Relative paths are resolved against the repository directory, so this places the chunk and index stores under `/home/git/test-repo.git/desync-lfs/`:

```sh
docker exec -u git lfs-ssh-test sh -c 'cat > /home/git/test-repo.git/desync-lfs.json' << 'EOF'
{
  "defaults": {
    "stores": ["desync-lfs/chunks"],
    "index-store": "desync-lfs/index"
  }
}
EOF
```

### 5. Set up a test repository and push two files

The two test files share a 3 MB block of pseudo-random data, followed by 1 MB of different compressible data each. This demonstrates both deduplication (shared chunks stored once) and compression (compressible chunks shrink on disk).

```sh
mkdir /tmp/lfs-ssh-repo && cd /tmp/lfs-ssh-repo
git init
git lfs install --local
git config core.sshCommand \
    "ssh -i /tmp/lfs-test-key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

git lfs track "*.bin"
git add .gitattributes
git commit -m "Track .bin files with LFS"

# Create a 3 MB shared block of pseudo-random data (deterministic seed).
python3 -c "
import random, sys
random.seed(42)
sys.stdout.buffer.write(bytes(random.getrandbits(8) for _ in range(3*1024*1024)))
" > /tmp/shared_block

# file_a = shared 3 MB + 1 MB of zeros
cat /tmp/shared_block > file_a.bin
dd if=/dev/zero bs=1M count=1 2>/dev/null >> file_a.bin

# file_b = shared 3 MB + 1 MB of repeated text
cat /tmp/shared_block > file_b.bin
python3 -c "
import sys
line = b'The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs.\n'
sys.stdout.buffer.write((line * (1024*1024 // len(line) + 1))[:1024*1024])
" >> file_b.bin

git add file_a.bin
git commit -m "Add file_a"

git remote add origin ssh://git@localhost:2222/home/git/test-repo.git
git push origin master

git add file_b.bin
git commit -m "Add file_b"
git push origin master
```

Git LFS automatically derives the SSH endpoint from the `origin` remote URL and invokes `git-lfs-transfer` on the server via SSH. The store directories (`desync-lfs/chunks` and `desync-lfs/index` inside the bare repo, as configured in step 4) are created automatically on first upload.

### 6. Verify compression and deduplication

```sh
docker exec lfs-ssh-test find /home/git/test-repo.git/desync-lfs/chunks/ -name "*.cacnk" | wc -l
# → ~57 chunks (not ~104) — shared chunks stored only once

docker exec lfs-ssh-test du -sh /home/git/test-repo.git/desync-lfs/chunks/
# → ~3.3 MB on disk for 8 MB of raw data — compressible regions shrink
```

Without deduplication, two 4 MB files would produce roughly twice as many chunks. The shared 3 MB region produces ~47 identical chunks that are stored only once. The zeros and repeated text compress well, so the total on-disk size is well under half the raw input.

### 7. Clone and verify download

```sh
GIT_SSH_COMMAND="ssh -i /tmp/lfs-test-key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
    git clone ssh://git@localhost:2222/home/git/test-repo.git /tmp/lfs-ssh-clone

# Verify integrity — both files must match the originals
sha256sum /tmp/lfs-ssh-clone/file_a.bin /tmp/lfs-ssh-clone/file_b.bin
sha256sum /tmp/lfs-ssh-repo/file_a.bin /tmp/lfs-ssh-repo/file_b.bin
```

### 8. Cleanup

```sh
docker rm -f lfs-ssh-test
rm -rf /tmp/lfs-ssh-repo /tmp/lfs-ssh-clone /tmp/shared_block \
       /tmp/lfs-test-key /tmp/lfs-test-key.pub /tmp/git-lfs-transfer
```

---

## Differences from git-lfs-desync

| Aspect              | `git-lfs-desync` (client)                        | `git-lfs-transfer` (server)                      |
| ------------------- | ------------------------------------------------ | ------------------------------------------------ |
| **Where it runs**   | Client machine                                   | Server (invoked via SSH)                          |
| **Protocol**        | Git LFS custom transfer agent (stdin/stdout JSON) | Git LFS SSH transfer protocol (pkt-line)         |
| **Client setup**    | Custom transfer agent in `.gitconfig`            | Only `lfs.url` pointing to SSH remote             |
| **Auto-negotiation** | Receives config from server when auto-negotiated | Advertises desync transfer via `desync-lfs.advertise` |
| **Config**          | CLI flags + config file + server config          | `desync-lfs.json` required (per-repo, walk-up, or `/etc/desync/`) |
| **Interoperable**   | Yes — same storage layout                        | Yes — same storage layout                         |
