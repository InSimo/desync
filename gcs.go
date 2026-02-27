package desync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

var _ WriteStore = GCStore{}

// GCStoreBase is the base object for all chunk and index stores with Google
// Storage backing
type GCStoreBase struct {
	Location   string
	client     *storage.BucketHandle
	bucket     string
	prefix     string
	opt        StoreOptions
	converters Converters
}

// GCStore is a read-write store with Google Storage backing
type GCStore struct {
	GCStoreBase
}

// normalizeGCPrefix converts path to a regular format,
// where there never is a leading slash,
// and every folder name always is followed by a slash
// so example outputs will be:
//
//	<blank string>
//	folder1/
//	folder1/folder2/folder3/
func normalizeGCPrefix(path string) string {
	prefix := strings.Trim(path, "/")

	if prefix != "" {
		prefix += "/"
	}

	return prefix
}

// NewGCStoreBase initializes a base object used for chunk or index stores
// backed by Google Storage.
func NewGCStoreBase(u *url.URL, opt StoreOptions) (GCStoreBase, error) {
	var err error
	ctx := context.TODO()
	s := GCStoreBase{Location: u.String(), opt: opt, converters: opt.converters()}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	// Pull the bucket as well as the prefix from a path-style URL
	s.bucket = u.Host
	s.prefix = normalizeGCPrefix(u.Path)

	client, err := storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, s.String())
	}

	s.client = client.Bucket(s.bucket)
	return s, nil
}

func (s GCStoreBase) String() string {
	return s.Location
}

// Close the GCS base store. NOP operation but needed to implement the store interface.
func (s GCStoreBase) Close() error { return nil }

// NewGCStore creates a chunk store with Google Storage backing. The URL
// should be provided like this: gs://bucketname/prefix
// Credentials are passed in via the environment variables. TODO
func NewGCStore(location *url.URL, opt StoreOptions) (s GCStore, e error) {
	b, err := NewGCStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return GCStore{b}, nil
}

// GetChunk reads and returns one chunk from the store
func (s GCStore) GetChunk(id ChunkID) (*Chunk, error) {
	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	rc, err := s.client.Object(name).NewReader(ctx)

	if err == storage.ErrObjectNotExist {
		log.Warning("Unable to create reader for object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, ChunkMissing{ID: id}
	} else if err != nil {
		log.WithError(err).Error("Unable to retrieve object from GCS bucket")
		return nil, errors.Wrap(err, s.String())
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)

	if err == storage.ErrObjectNotExist {
		log.Warning("Unable to read from object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, ChunkMissing{ID: id}
	} else if err != nil {
		log.WithError(err).Error("Unable to retrieve object from GCS bucket")
		return nil, errors.Wrap(err, fmt.Sprintf("chunk %s could not be retrieved from GCS bucket", id))
	}

	log.Debug("Retrieved chunk from GCS bucket")

	return NewChunkFromStorage(id, b, s.converters, s.opt.SkipVerify)
}

// StoreChunk adds a new chunk to the store
func (s GCStore) StoreChunk(chunk *Chunk) error {

	ctx := context.TODO()
	contentType := "application/zstd"
	name := s.nameFromID(chunk.ID())

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	b, err := chunk.Storage(s.converters)
	if err != nil {
		log.WithError(err).Error("Cannot retrieve chunk data")
		return err
	}

	r := bytes.NewReader(b)
	w := s.client.Object(name).NewWriter(ctx)
	w.ContentType = contentType
	_, err = io.Copy(w, r)

	if err != nil {
		log.WithError(err).Error("Error when copying data from local filesystem to object in GCS bucket")
		return errors.Wrap(err, s.String())
	}

	err = w.Close()
	if err != nil {
		log.WithError(err).Error("Error when finalizing copying of data from local filesystem to object in GCS bucket")
		return errors.Wrap(err, s.String())
	}

	log.Debug("Uploaded chunk to GCS bucket")
	return nil
}

// HasChunk returns true if the chunk is in the store
func (s GCStore) HasChunk(id ChunkID) (bool, error) {

	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	_, err := s.client.Object(name).Attrs(ctx)

	if err == storage.ErrObjectNotExist {
		log.WithField("exists", false).Debug("Chunk does not exist in GCS bucket")
		return false, nil
	} else if err != nil {
		log.WithError(err).Error("Unable to query attributes for object in GCS bucket")
		return false, err
	} else {
		log.WithField("exists", true).Debug("Chunk exists in GCS bucket")
		return true, nil
	}
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s GCStore) RemoveChunk(id ChunkID) error {
	ctx := context.TODO()
	name := s.nameFromID(id)

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   name,
		})
	)

	err := s.client.Object(name).Delete(ctx)

	if err != nil {
		log.WithError(err).Error("Unable to delete object in GCS bucket")
		return err
	} else {
		log.Debug("Removed chunk from GCS bucket")
		return nil
	}
}

// Prune removes any chunks from the store that are not contained in a list (map)
func (s GCStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	query := &storage.Query{Prefix: s.prefix}
	it := s.client.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		id, err := s.idFromName(attrs.Name)
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

func (s GCStore) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.prefix + sID[0:4] + "/" + sID
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}

func (s GCStore) pruningNamesFromID(id ChunkID) (prunable, pruning string) {
	name := s.nameFromID(id)
	return name + PrunableExt, name + PruningExt
}

// UntagPrunable removes the .prunable companion object for a chunk, if it exists.
func (s GCStore) UntagPrunable(id ChunkID) error {
	ctx := context.TODO()
	prunable, _ := s.pruningNamesFromID(id)
	err := s.client.Object(prunable).Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	return err
}

// RescueChunks rescues any chunks in ids that were quarantined by SafePrune.
func (s GCStore) RescueChunks(ctx context.Context, ids map[ChunkID]struct{}) error {
	for id := range ids {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		cacnk := s.nameFromID(id)
		prunable, pruning := s.pruningNamesFromID(id)
		// Remove prunable marker.
		if err := s.client.Object(prunable).Delete(ctx); err != nil && err != storage.ErrObjectNotExist {
			return err
		}
		// Check if .cacnk exists.
		_, err := s.client.Object(cacnk).Attrs(ctx)
		if err == nil {
			// .cacnk exists; remove stale quarantined copy.
			if rerr := s.client.Object(pruning).Delete(ctx); rerr != nil && rerr != storage.ErrObjectNotExist {
				return rerr
			}
		} else if err == storage.ErrObjectNotExist {
			// .cacnk missing; try to rescue from quarantine.
			copier := s.client.Object(cacnk).CopierFrom(s.client.Object(pruning))
			if _, cerr := copier.Run(ctx); cerr != nil {
				if cerr == storage.ErrObjectNotExist {
					continue
				}
				return cerr
			}
			if rerr := s.client.Object(pruning).Delete(ctx); rerr != nil && rerr != storage.ErrObjectNotExist {
				return rerr
			}
		} else {
			return err
		}
	}
	return nil
}

// SafePrune implements the two-run safe pruning protocol for a GCStore.
func (s GCStore) SafePrune(ctx context.Context, ids map[ChunkID]struct{}) error {
	// Phase 1: cleanup.
	query := &storage.Query{Prefix: s.prefix}
	it := s.client.Objects(ctx, query)
	for {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		name := attrs.Name
		if strings.HasSuffix(name, PruningExt) {
			if derr := s.client.Object(name).Delete(ctx); derr != nil && derr != storage.ErrObjectNotExist {
				return derr
			}
			continue
		}
		if strings.HasSuffix(name, PrunableExt) {
			cacnk := strings.TrimSuffix(name, PrunableExt)
			_, serr := s.client.Object(cacnk).Attrs(ctx)
			if serr == storage.ErrObjectNotExist {
				// orphaned marker
				if derr := s.client.Object(name).Delete(ctx); derr != nil && derr != storage.ErrObjectNotExist {
					return derr
				}
			}
		}
	}

	// Phase 2: mark + quarantine.
	it2 := s.client.Objects(ctx, query)
	for {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		attrs, err := it2.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		name := attrs.Name
		if strings.HasSuffix(name, PrunableExt) || strings.HasSuffix(name, PruningExt) {
			continue
		}
		id, err := s.idFromName(name)
		if err != nil {
			continue
		}
		prunable, pruning := s.pruningNamesFromID(id)
		if _, ok := ids[id]; ok {
			// In keep-set: remove prunable marker.
			if rerr := s.client.Object(prunable).Delete(ctx); rerr != nil && rerr != storage.ErrObjectNotExist {
				return rerr
			}
			continue
		}
		// Not in keep-set.
		_, serr := s.client.Object(prunable).Attrs(ctx)
		if serr == storage.ErrObjectNotExist {
			// First encounter: create empty marker.
			w := s.client.Object(prunable).NewWriter(ctx)
			if werr := w.Close(); werr != nil {
				return werr
			}
		} else if serr == nil {
			// Already marked: quarantine.
			copier := s.client.Object(pruning).CopierFrom(s.client.Object(name))
			if _, cerr := copier.Run(ctx); cerr != nil {
				return cerr
			}
			if rerr := s.client.Object(name).Delete(ctx); rerr != nil {
				return rerr
			}
			if rerr := s.client.Object(prunable).Delete(ctx); rerr != nil && rerr != storage.ErrObjectNotExist {
				return rerr
			}
		} else {
			return serr
		}
	}
	return nil
}

func (s GCStore) idFromName(name string) (ChunkID, error) {
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
