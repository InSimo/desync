# git-lfs-desync

`git-lfs-desync` is a [Git LFS custom transfer agent](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md) that stores LFS objects using desync's content-defined chunking. Instead of uploading whole files to an LFS server, it:

1. Splits each file into variable-size chunks using a SipHash rolling hash.
2. Compresses and deduplicates chunks in an S3-compatible store.
3. Stores a `.caibx` index (keyed by LFS OID) in a separate S3 prefix.

On download, it fetches the index and reassembles the file from S3 chunks. Chunks are shared across all files and commits — only genuinely new content is uploaded.

## Building

```sh
go install github.com/folbricht/desync/cmd/git-lfs-desync@latest
# or from source:
go build -o /usr/local/bin/git-lfs-desync ./cmd/git-lfs-desync
```

## Flags

| Flag | Default | Description |
|---|---|---|
| `-s`, `--store` | *(required)* | S3 URL for chunk storage, e.g. `s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/` |
| `--index-store` | derived from `--store` | S3 URL for index storage. Defaults to replacing the last path segment of `--store` with `index/`. |
| `-n`, `--concurrency` | `10` | Number of concurrent goroutines for chunk I/O. |
| `-m`, `--chunk-size` | `16:64:256` | Min:avg:max chunk size in KB. |
| `-e`, `--error-retry` | `3` | Number of times to retry on S3 errors. |
| `--config` | `$HOME/.config/desync/config.json` | desync config file for S3 credentials and store options. |

## Credentials

Credentials are resolved in this order:

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

## Git Configuration

The transfer agent config must be set on every machine that pushes or pulls LFS objects. The simplest place is `~/.gitconfig` (global), so it applies to all repos.

```ini
[lfs "customtransfer.desync"]
    path = /usr/local/bin/git-lfs-desync
    args = --store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/
    concurrent = false

[lfs]
    standalonetransferagent = desync
```

`concurrent = false` is required because git-lfs would otherwise run multiple agent processes simultaneously, each trying to write to stdout independently.

With `standalonetransferagent` set, git-lfs bypasses the normal LFS HTTP API entirely — no LFS server is needed. The S3 bucket is the only backend.

### Per-repo config

To scope the agent to a single repository instead of globally:

```sh
git config lfs.customtransfer.desync.path /usr/local/bin/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+https://s3.amazonaws.com/my-bucket/lfs/chunks/"
git config lfs.customtransfer.desync.concurrent false
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
git config lfs.customtransfer.desync.concurrent false
git config lfs.standalonetransferagent desync
S3_ACCESS_KEY=... S3_SECRET_KEY=... git lfs pull
```

## Storage Layout

Given `--store s3+https://host/bucket/lfs/chunks/`, the agent writes:

```
s3://bucket/lfs/chunks/<xx>/<64-hex-chars>.cacnk   # compressed chunks
s3://bucket/lfs/index/<lfs-oid>.caibx              # index per LFS object
```

The index store defaults to `s3+https://host/bucket/lfs/index/` (last path segment of `--store` replaced with `index`). Override with `--index-store` if needed.

---

## Local Testing with MinIO

The following steps reproduce the full upload/download cycle locally using [MinIO](https://min.io/) as an S3-compatible backend, without any AWS account.

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

### 3. Set up a test repository

```sh
mkdir /tmp/lfs-repo && cd /tmp/lfs-repo
git init
git lfs install

# Point LFS at the local agent and MinIO bucket
git config lfs.customtransfer.desync.path /tmp/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+http://localhost:9000/lfs-test/chunks/ \
     --index-store s3+http://localhost:9000/lfs-test/index/"
git config lfs.customtransfer.desync.concurrent false
git config lfs.standalonetransferagent desync
# Standalone mode requires a dummy lfs.url (no real LFS server)
git config lfs.url "https://localhost"

git lfs track "*.bin"
git add .gitattributes
git commit -m "Track .bin files with LFS"
```

### 4. Add a large file and push

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

### 5. Clone and verify download

```sh
S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin git clone /tmp/lfs-bare /tmp/lfs-clone
cd /tmp/lfs-clone

# Configure agent in the clone
git config lfs.customtransfer.desync.path /tmp/git-lfs-desync
git config lfs.customtransfer.desync.args \
    "--store s3+http://localhost:9000/lfs-test/chunks/ \
     --index-store s3+http://localhost:9000/lfs-test/index/"
git config lfs.customtransfer.desync.concurrent false
git config lfs.standalonetransferagent desync
git config lfs.url "https://localhost"

S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin git lfs pull

# Verify integrity
sha256sum large.bin
# Must match $ORIGINAL_SHA
```

### 6. Cleanup

```sh
docker rm -f minio-lfs
rm -rf /tmp/lfs-repo /tmp/lfs-bare /tmp/lfs-clone /tmp/git-lfs-desync
```
