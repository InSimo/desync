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

| Protocol | URL Scheme | Notes |
|---|---|---|
| **Local filesystem** | `/path/to/dir` or `./dir` | No credentials needed; simplest setup |
| **S3-compatible** | `s3+https://host/bucket/prefix/` | AWS S3, MinIO, Ceph RGW, etc. |
| **SFTP** | `sftp://user@host/path/` | Uses SSH keys or agent |
| **HTTP/HTTPS** | `https://host/path/` | Requires a write-capable HTTP store server |
| **Google Cloud Storage** | `gs://bucket/prefix/` | Uses GCS application default credentials |

SSH stores (`ssh://`) are read-only in desync and cannot be used with this agent.

## Flags

| Flag | Default | Description |
|---|---|---|
| `-s`, `--store` | config default or *(required)* | Chunk store location. See [Supported Backends](#supported-backends) for URL schemes. May be set via the `defaults.stores` config key. |
| `--index-store` | config default, then derived from `--store` | Index store location. Falls back to `defaults.index-store` in the config, then to replacing the last path segment of `--store` with `index` (or a sibling `index` directory for local paths). |
| `-c`, `--cache` | — | Local chunk store used as a download cache. On a cache miss, the chunk is fetched from `--store` and saved to the cache; subsequent downloads are served from the cache. Uploads always go directly to `--store`. Accepts the same URL schemes as `--store`. |
| `--cache-repair` | `true` | If the cache returns a corrupt chunk, re-download it from `--store` and replace the cached copy. |
| `-n`, `--concurrency` | `10` | Number of concurrent goroutines for chunk I/O. |
| `-m`, `--chunk-size` | `16:64:256` | Min:avg:max chunk size in KB. |
| `-e`, `--error-retry` | `3` | Number of times to retry on network error. |
| `-b`, `--error-retry-base-interval` | `500ms` | Initial retry delay; increases linearly with each attempt. |
| `--client-cert` | — | Path to client certificate for mutual TLS. |
| `--client-key` | — | Path to client key for mutual TLS. |
| `--ca-cert` | — | CA certificate file to trust instead of the OS trust store. |
| `-t`, `--trust-insecure` | `false` | Trust invalid/self-signed certificates. |
| `--config` | `$HOME/.config/desync/config.json` | desync config file for S3 credentials and store options. Mutually exclusive with `--config-from-git`. |
| `--config-from-git` | — | Read the desync config from a git object (e.g. `origin/_desync:config.json`). Mutually exclusive with `--config`. |
| `--digest` | config default, then `sha512-256` | Hash algorithm used to identify chunks: `sha512-256` (default) or `sha256`. Must match the algorithm used when the store was originally written. May be set via the `defaults.digest` config key. |

## Config defaults

Flags that are the same for every invocation (store URL, index store URL, digest algorithm) can be set once in the desync config file under a `defaults` key, so the bare `git-lfs-desync` command works without extra `args` in the LFS agent config.

### Precedence

```
CLI flag  >  config defaults  >  built-in default
```

For `--index-store` the derived-from-store fallback is applied after the config default:

```
CLI --index-store  >  defaults.index-store  >  derived from --store
```

### JSON shape

```json
{
  "defaults": {
    "digest":      "sha512-256",
    "stores":      ["s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"],
    "index-store": "s3+https://s3.amazonaws.com/my-bucket/lfs/index/"
  }
}
```

| Key | Type | Description |
|---|---|---|
| `defaults.stores` | array of strings | Chunk store(s). The first entry is used as the single store for `git-lfs-desync`. Additional entries are ignored by this command (but used by multi-store `desync` subcommands). |
| `defaults.index-store` | string | Index store URL or path. |
| `defaults.digest` | string | Digest algorithm (`sha512-256` or `sha256`). |

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

The store URL can also come from a git-resident config via `--config-from-git`, which is useful for sharing a team config without per-machine setup.

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
  }
}
```

### SFTP

SFTP stores authenticate via the SSH agent or `~/.ssh` keys. No extra configuration is required beyond having a valid SSH key for the host.

### GCS

GCS stores use [application default credentials](https://cloud.google.com/docs/authentication/application-default-credentials). Run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`.

## Config from Git (`--config-from-git`)

`git-lfs-desync` is invoked by Git itself during clone, fetch, and push operations, with the working directory set to the repository root. This means the desync config — containing S3 credentials and store options — normally needs to live at a fixed path on each developer's machine, making it awkward to onboard contributors or run in CI environments.

`--config-from-git <object>` solves this by reading the config JSON directly from the repository's object database via `git cat-file --text-conv <object>`. The object name can be any git ref, tree path, or blob — for example, a file on a dedicated branch that never mingles with the main working tree.

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
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/ \
           --config-from-git origin/_desync:config.json
    concurrent = true
    concurrenttransfers = 5

[lfs]
    standalonetransferagent = desync
```

`git clone` fetches all remote tracking branches by default, so `origin/_desync` is available immediately after cloning without a separate `git fetch`. On machines that already have the repository checked out before `_desync` was pushed, run `git fetch origin _desync` once.

> **Security note:** Anyone with read access to the remote can read the credentials on the `_desync` branch. Use this pattern only when the remote is private and access-controlled. If finer-grained access control is needed (e.g. read access to code but not to S3 keys), store credentials in environment variables or a local file via `--config` instead.

---

## Git Configuration

The transfer agent config must be set on every machine that pushes or pulls LFS objects. The simplest place is `~/.gitconfig` (global), so it applies to all repos.

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/
    concurrent = true
    concurrenttransfers = 5

[lfs]
    standalonetransferagent = desync
```

`concurrent = true` allows git-lfs to pipeline multiple transfer events to the same agent process. `concurrenttransfers` (default 3 in git-lfs) caps the number of in-flight transfers; the agent reads this value from the init message and uses it to size an internal goroutine pool. For maximum throughput, set `concurrenttransfers` to roughly `--concurrency / N` where N is the average number of chunks per file.

With `standalonetransferagent` set, git-lfs bypasses the normal LFS HTTP API entirely — no LFS server is needed.

### Per-repo config

To scope the agent to a single repository instead of globally:

```sh
git config lfs.customtransfer.desync.path /usr/local/bin/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"
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
git config lfs.customtransfer.desync.args "--store s3+https://..."
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
           --cache /var/cache/lfs/chunks
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
     --cache ~/.cache/lfs/chunks"
```

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
  quay.io/minio/minio server /data
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
     --cache /tmp/lfs-cache"
git config lfs.customtransfer.desync.concurrent true
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

```sh
S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin git clone /tmp/lfs-bare /tmp/lfs-clone
cd /tmp/lfs-clone

# Configure agent in the clone with the same local cache
git config lfs.customtransfer.desync.path /tmp/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+http://localhost:9000/lfs-test/chunks/ \
     --index-store s3+http://localhost:9000/lfs-test/index/ \
     --cache /tmp/lfs-cache"
git config lfs.customtransfer.desync.concurrent true
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
