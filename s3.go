package desync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	minio "github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"
	"github.com/pkg/errors"
)

var _ WriteStore = S3Store{}

// S3StoreBase is the base object for all chunk and index stores with S3 backing
type S3StoreBase struct {
	Location   string
	client     *minio.Client
	bucket     string
	prefix     string
	opt        StoreOptions
	converters Converters
}

// S3Store is a read-write store with S3 backing
type S3Store struct {
	S3StoreBase
}

// NewS3StoreBase initializes a base object used for chunk or index stores backed by S3.
func NewS3StoreBase(u *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (S3StoreBase, error) {
	var err error
	s := S3StoreBase{Location: u.String(), opt: opt, converters: opt.converters()}
	if !strings.HasPrefix(u.Scheme, "s3+http") {
		return s, fmt.Errorf("invalid scheme '%s', expected 's3+http' or 's3+https'", u.Scheme)
	}
	var useSSL bool
	if strings.Contains(u.Scheme, "https") {
		useSSL = true
	}

	// Pull the bucket as well as the prefix from a path-style URL
	bPath := strings.Trim(u.Path, "/")
	if bPath == "" {
		return s, fmt.Errorf("expected bucket name in path of '%s'", u.Scheme)
	}
	f := strings.Split(bPath, "/")
	s.bucket = f[0]
	s.prefix = strings.Join(f[1:], "/")

	if s.prefix != "" {
		s.prefix += "/"
	}

	s.client, err = minio.NewWithOptions(u.Host, &minio.Options{
		Creds:        s3Creds,
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookupType,
	})
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}
	return s, nil
}

func (s S3StoreBase) String() string {
	return s.Location
}

// Close the S3 base store. NOP operation but needed to implement the store interface.
func (s S3StoreBase) Close() error { return nil }

// NewS3Store creates a chunk store with S3 backing. The URL
// should be provided like this: s3+http://host:port/bucket
// Credentials are passed in via the environment variables S3_ACCESS_KEY
// and S3_SECRET_KEY, or via the desync config file.
func NewS3Store(location *url.URL, s3Creds *credentials.Credentials, region string, opt StoreOptions, lookupType minio.BucketLookupType) (s S3Store, e error) {
	b, err := NewS3StoreBase(location, s3Creds, region, opt, lookupType)
	if err != nil {
		return s, err
	}
	return S3Store{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s S3Store) GetChunk(id ChunkID) (*Chunk, error) {
	name := s.nameFromID(id)
	var attempt int
retry:
	attempt++
	obj, err := s.client.GetObject(s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		if attempt <= s.opt.ErrorRetry {
			goto retry
		}
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := io.ReadAll(obj)
	if err != nil {
		if attempt <= s.opt.ErrorRetry {
			goto retry
		}
		if e, ok := err.(minio.ErrorResponse); ok {
			switch e.Code {
			case "NoSuchBucket":
				err = fmt.Errorf("bucket '%s' does not exist", s.bucket)
			case "NoSuchKey":
				err = ChunkMissing{ID: id}
			default: // Without ListBucket perms in AWS, we get Permission Denied for a missing chunk, not 404
				err = errors.Wrap(err, fmt.Sprintf("chunk %s could not be retrieved from s3 store", id))
			}
		}
		return nil, err
	}
	return NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
}

// StoreChunk adds a new chunk to the store
func (s S3Store) StoreChunk(chunk *Chunk) error {
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}
	var attempt int
retry:
	attempt++
	_, err = s.client.PutObject(s.bucket, name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		if attempt < s.opt.ErrorRetry {
			goto retry
		}
	}
	return errors.Wrap(err, s.String())
}

// HasChunk returns true if the chunk is in the store
func (s S3Store) HasChunk(id ChunkID) (bool, error) {
	name := s.nameFromID(id)
	_, err := s.client.StatObject(s.bucket, name, minio.StatObjectOptions{})
	return err == nil, nil
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s S3Store) RemoveChunk(id ChunkID) error {
	name := s.nameFromID(id)
	return s.client.RemoveObject(s.bucket, name)
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s S3Store) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.client.ListObjectsV2(s.bucket, s.prefix, true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}

		id, err := s.idFromName(object.Key)
		if err != nil {
			continue
		}

		// Drop the chunk if it's not on the list
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s S3Store) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}

func (s S3Store) pruningNamesFromID(id ChunkID) (prunable, pruning string) {
	name := s.nameFromID(id)
	return name + PrunableExt, name + PruningExt
}

// UntagPrunable removes the .prunable companion object for a chunk, if it exists.
func (s S3Store) UntagPrunable(id ChunkID) error {
	prunable, _ := s.pruningNamesFromID(id)
	err := s.client.RemoveObject(s.bucket, prunable)
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.Code == "NoSuchKey" {
			return nil
		}
	}
	return err
}

// ListChunks returns every chunk-related entry in the store together with its
// pruning status. It is called by commonSafePrune and commonRescueChunks.
func (s S3Store) ListChunks(ctx context.Context) ([]ChunkEntry, error) {
	var entries []ChunkEntry
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.client.ListObjectsV2(s.bucket, s.prefix, true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		select {
		case <-ctx.Done():
			return nil, Interrupted{}
		default:
		}
		key := object.Key
		if strings.HasSuffix(key, PruningExt) {
			cacnkName := strings.TrimSuffix(key, PruningExt)
			id, err := s.idFromName(cacnkName)
			if err != nil {
				continue
			}
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPruning})
		} else if strings.HasSuffix(key, PrunableExt) {
			cacnkName := strings.TrimSuffix(key, PrunableExt)
			id, err := s.idFromName(cacnkName)
			if err != nil {
				continue
			}
			_, serr := s.client.StatObject(s.bucket, cacnkName, minio.StatObjectOptions{})
			if serr != nil {
				entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedPrunable})
			} else {
				entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPrunable})
			}
		} else {
			id, err := s.idFromName(key)
			if err != nil {
				continue
			}
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusNormal})
		}
	}
	return entries, nil
}

// MarkerExists reports whether the .prunable companion for id is present.
func (s S3Store) MarkerExists(id ChunkID) (bool, error) {
	prunable, _ := s.pruningNamesFromID(id)
	_, err := s.client.StatObject(s.bucket, prunable, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if e, ok := err.(minio.ErrorResponse); ok && e.Code == "NoSuchKey" {
		return false, nil
	}
	return false, err
}

// DeletePruning removes the .pruning object for id. No-op if absent.
func (s S3Store) DeletePruning(id ChunkID) error {
	_, pruning := s.pruningNamesFromID(id)
	err := s.client.RemoveObject(s.bucket, pruning)
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.Code == "NoSuchKey" {
			return nil
		}
		return err
	}
	return nil
}

// DeleteMarker removes the .prunable companion for id. No-op if absent.
func (s S3Store) DeleteMarker(id ChunkID) error {
	prunable, _ := s.pruningNamesFromID(id)
	err := s.client.RemoveObject(s.bucket, prunable)
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.Code == "NoSuchKey" {
			return nil
		}
		return err
	}
	return nil
}

// CreateMarker creates an empty .prunable companion object for id.
func (s S3Store) CreateMarker(id ChunkID) error {
	prunable, _ := s.pruningNamesFromID(id)
	_, err := s.client.PutObject(s.bucket, prunable, bytes.NewReader(nil), 0, minio.PutObjectOptions{})
	return err
}

// Quarantine copies .cacnk to .pruning and removes .cacnk, making the chunk
// invisible to readers.
func (s S3Store) Quarantine(id ChunkID) error {
	cacnk := s.nameFromID(id)
	_, pruning := s.pruningNamesFromID(id)
	src := minio.NewSourceInfo(s.bucket, cacnk, nil)
	dst, err := minio.NewDestinationInfo(s.bucket, pruning, nil, nil)
	if err != nil {
		return err
	}
	if err := s.client.CopyObject(dst, src); err != nil {
		return err
	}
	return s.client.RemoveObject(s.bucket, cacnk)
}

// Restore copies .pruning back to .cacnk and removes .pruning. No-op if
// .pruning is absent.
func (s S3Store) Restore(id ChunkID) error {
	cacnk := s.nameFromID(id)
	_, pruning := s.pruningNamesFromID(id)
	src := minio.NewSourceInfo(s.bucket, pruning, nil)
	dst, err := minio.NewDestinationInfo(s.bucket, cacnk, nil, nil)
	if err != nil {
		return err
	}
	if err := s.client.CopyObject(dst, src); err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.Code == "NoSuchKey" {
			return nil
		}
		return err
	}
	return s.client.RemoveObject(s.bucket, pruning)
}

// SafePrune implements the two-run safe pruning protocol for an S3Store.
func (s S3Store) SafePrune(ctx context.Context, ids map[ChunkID]struct{}) error {
	return commonSafePrune(ctx, ids, s)
}

// RescueChunks rescues any chunks in ids that were quarantined by SafePrune.
func (s S3Store) RescueChunks(ctx context.Context, ids map[ChunkID]struct{}) error {
	return commonRescueChunks(ctx, ids, s)
}

func (s S3Store) idFromName(name string) (ChunkID, error) {
	var n string
	if s.opt.Uncompressed {
		if !strings.HasSuffix(name, UncompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), UncompressedChunkExt)
	} else {
		if !strings.HasSuffix(name, CompressedChunkExt) {
			return ChunkID{}, fmt.Errorf("object %s is not a chunk", name)
		}
		n = strings.TrimSuffix(strings.TrimPrefix(name, s.prefix), CompressedChunkExt)
	}
	fragments := strings.Split(n, "/")
	if len(fragments) != 2 {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	idx := fragments[0]
	sid := fragments[1]
	if !strings.HasPrefix(sid, idx) {
		return ChunkID{}, fmt.Errorf("incorrect chunk name for object %s", name)
	}
	return ChunkIDFromString(sid)
}
