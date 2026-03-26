package desync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
)

const originalSizeMetaKey = "Original-Size"

// S3IndexStore is a read-write index store with S3 backing
type S3IndexStore struct {
	S3StoreBase
}

// NewS3IndexStore creates an index store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3_SECRET_KEY, or via the desync config file.
func NewS3IndexStore(location *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (s S3IndexStore, e error) {
	b, err := NewS3StoreBase(location, s3Creds, region, opt, lookupType)
	if err != nil {
		return s, err
	}
	return S3IndexStore{b}, nil
}

// GetIndexReader returns a reader for an index from an S3 store. Fails if the specified index
// file does not exist.
func (s S3IndexStore) GetIndexReader(name string) (r io.ReadCloser, e error) {
	obj, err := s.client.GetObject(s.bucket, s.prefix+name, minio.GetObjectOptions{})
	if err != nil {
		return r, errors.Wrap(err, s.String())
	}
	return obj, nil
}

// GetIndex returns an Index structure from the store
func (s S3IndexStore) GetIndex(name string) (i Index, e error) {
	obj, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer obj.Close()
	return IndexFromReader(obj)
}

// HasIndex returns true if an index with the given name exists in the store.
func (s S3IndexStore) HasIndex(name string) (bool, error) {
	_, err := s.client.StatObject(s.bucket, s.prefix+name, minio.StatObjectOptions{})
	return err == nil, nil
}

// StatIndex returns metadata about the named index. The original content size
// is read from the x-amz-meta-original-size user metadata if present (set by
// StoreIndex). For indexes stored before this metadata was added, StatIndex
// falls back to GetIndex to compute the size.
func (s S3IndexStore) StatIndex(name string) (IndexInfo, error) {
	info, err := s.client.StatObject(s.bucket, s.prefix+name, minio.StatObjectOptions{})
	if err != nil {
		return IndexInfo{}, err
	}
	result := IndexInfo{
		Name:    name,
		ModTime: info.LastModified,
		Size:    -1,
	}
	// Try to read original size from user metadata (set by StoreIndex).
	if sizeStr, ok := info.UserMetadata[originalSizeMetaKey]; ok {
		if size, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			result.Size = size
		}
	}
	// Fallback: download and parse the full index.
	if result.Size < 0 {
		idx, err := s.GetIndex(name)
		if err != nil {
			return IndexInfo{}, err
		}
		result.Size = idx.TotalSize()
	}
	return result, nil
}

// StoreIndex writes the index file to the S3 store. The original content size
// is stored as x-amz-meta-original-size user metadata so that StatIndex can
// return it from a HEAD request without downloading the full index.
func (s S3IndexStore) StoreIndex(name string, idx Index) error {
	// Serialize the index to a buffer so we can pass the exact size to
	// PutObject.  Without a known size, the minio SDK uses multipart upload
	// which allocates a 128 MB buffer per part — extremely wasteful for
	// small index files (typically a few KB).
	var buf bytes.Buffer
	if _, err := idx.WriteTo(&buf); err != nil {
		return errors.Wrap(err, "serializing index")
	}

	_, err := s.client.PutObject(s.bucket, s.prefix+name, &buf, int64(buf.Len()), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		UserMetadata: map[string]string{
			originalSizeMetaKey: strconv.FormatInt(idx.TotalSize(), 10),
		},
	})
	return errors.Wrap(err, path.Base(s.Location))
}

// PruneIndexes removes all indexes from the store that are not in the keep set.
func (s S3IndexStore) PruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonPruneIndexes(ctx, keep, s)
}

// DeleteIndexes removes the named indexes from the store using a single batch request.
func (s S3IndexStore) DeleteIndexes(ctx context.Context, names []string) error {
	objectsCh := make(chan string, len(names))
	for _, name := range names {
		objectsCh <- s.prefix + name
	}
	close(objectsCh)
	var firstErr error
	for rmErr := range s.client.RemoveObjectsWithContext(ctx, s.bucket, objectsCh) {
		if firstErr == nil {
			firstErr = rmErr.Err
		}
	}
	return firstErr
}

// ListIndexes returns the names of indexes in the store, relative to the
// store root. If prefix is non-empty, only indexes under that subdirectory
// are returned.
func (s S3IndexStore) ListIndexes(ctx context.Context, prefix string) ([]string, error) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	listPrefix := s.prefix
	if prefix != "" {
		listPrefix = s.prefix + prefix + "/"
	}
	var names []string
	for object := range s.client.ListObjectsV2(s.bucket, listPrefix, true, doneCh) {
		if object.Err != nil {
			return nil, object.Err
		}
		select {
		case <-ctx.Done():
			return nil, Interrupted{}
		default:
		}
		name := strings.TrimPrefix(object.Key, s.prefix)
		if name == PrunableIndexSetFile {
			continue // reserved system file; not a real index
		}
		names = append(names, name)
	}
	return names, nil
}

// SafePruneIndexes removes indexes from the store using the two-run safe protocol.
func (s S3IndexStore) SafePruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonSafePruneIndexes(ctx, keep, s)
}

// ReadPrunableIndexSet reads the prunable set written by the previous run.
// Returns an empty set if no prior state exists.
func (s S3IndexStore) ReadPrunableIndexSet(_ context.Context) (map[string]struct{}, error) {
	obj, err := s.client.GetObject(s.bucket, s.prefix+PrunableIndexSetFile, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	var names []string
	if err := json.NewDecoder(obj).Decode(&names); err != nil {
		if err == io.EOF {
			return make(map[string]struct{}), nil // empty object
		}
		if minio.ToErrorResponse(err).StatusCode == 404 {
			return make(map[string]struct{}), nil // object does not exist
		}
		return nil, err
	}
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set, nil
}

// WritePrunableIndexSet persists the current deletion candidates for the next run.
// Removes the object when names is empty (no candidates remain).
func (s S3IndexStore) WritePrunableIndexSet(_ context.Context, names []string) error {
	if len(names) == 0 {
		return s.client.RemoveObject(s.bucket, s.prefix+PrunableIndexSetFile)
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(names); err != nil {
		return err
	}
	_, err := s.client.PutObject(
		s.bucket,
		s.prefix+PrunableIndexSetFile,
		&buf,
		int64(buf.Len()),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	return err
}
