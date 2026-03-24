package desync

import (
	"context"
	"encoding/json"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

// GCIndexStore is a read-write index store with Google Storage backing
type GCIndexStore struct {
	GCStoreBase
}

// NewGCIndexStore creates an index store with Google Storage backing. The URL
// should be provided like this: gc://bucket/prefix
func NewGCIndexStore(location *url.URL, opt StoreOptions) (s GCIndexStore, e error) {
	b, err := NewGCStoreBase(location, opt)
	if err != nil {
		return s, err
	}
	return GCIndexStore{b}, nil
}

// GetIndexReader returns a reader for an index from an Google Storage store. Fails if the specified index
// file does not exist.
func (s GCIndexStore) GetIndexReader(name string) (r io.ReadCloser, err error) {
	ctx := context.TODO()

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   s.prefix + name,
		})
	)

	obj, err := s.client.Object(s.prefix + name).NewReader(ctx)

	if err == storage.ErrObjectNotExist {
		log.Warning("Unable to create reader for object in GCS bucket; the object may not exist, or the bucket may not exist, or you may not have permission to access it")
		return nil, errors.Wrap(err, s.String())
	} else if err != nil {
		log.WithError(err).Error("Error when creating index reader from GCS bucket")
		return nil, errors.Wrap(err, s.String())
	}

	log.Debug("Created index reader from GCS bucket")
	return obj, nil
}

// GetIndex returns an Index structure from the store
func (s GCIndexStore) GetIndex(name string) (i Index, e error) {
	obj, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer obj.Close()
	return IndexFromReader(obj)
}

// HasIndex returns true if an index with the given name exists in the store.
func (s GCIndexStore) HasIndex(name string) (bool, error) {
	ctx := context.TODO()
	_, err := s.client.Object(s.prefix + name).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	return err == nil, err
}

// StatIndex returns metadata about the named index. The original content size
// is read from GCS object metadata if present. Falls back to GetIndex for
// indexes stored before this metadata was added.
func (s GCIndexStore) StatIndex(name string) (IndexInfo, error) {
	ctx := context.TODO()
	attrs, err := s.client.Object(s.prefix + name).Attrs(ctx)
	if err != nil {
		return IndexInfo{}, err
	}
	result := IndexInfo{
		Name:    name,
		ModTime: attrs.Updated,
		Size:    -1,
	}
	if sizeStr, ok := attrs.Metadata[originalSizeMetaKey]; ok {
		if size, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			result.Size = size
		}
	}
	if result.Size < 0 {
		idx, err := s.GetIndex(name)
		if err != nil {
			return IndexInfo{}, err
		}
		result.Size = idx.TotalSize()
	}
	return result, nil
}

// StoreIndex writes the index file to the Google Storage store
func (s GCIndexStore) StoreIndex(name string, idx Index) error {
	ctx := context.TODO()

	var (
		log = Log.WithFields(logrus.Fields{
			"bucket": s.bucket,
			"name":   s.prefix + name,
		})
	)

	w := s.client.Object(s.prefix + name).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.Metadata = map[string]string{
		originalSizeMetaKey: strconv.FormatInt(idx.TotalSize(), 10),
	}

	_, err := idx.WriteTo(w)

	if err != nil {
		log.WithError(err).Error("Error when copying data from local filesystem to object in GCS bucket")
		w.Close()
		return errors.Wrap(err, path.Base(s.Location))
	}

	err = w.Close()

	if err != nil {
		log.WithError(err).Error("Error when finalizing copying of data from local filesystem to object in GCS bucket")
		return errors.Wrap(err, path.Base(s.Location))
	}

	log.Debug("Index written to GCS bucket")
	return nil
}

// PruneIndexes removes all indexes from the store that are not in the keep set.
func (s GCIndexStore) PruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonPruneIndexes(ctx, keep, s)
}

// DeleteIndexes removes the named indexes from the store.
func (s GCIndexStore) DeleteIndexes(ctx context.Context, names []string) error {
	for _, name := range names {
		if err := s.client.Object(s.prefix + name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}

// ListIndexes returns the names of indexes in the store, relative to the
// store root. If prefix is non-empty, only indexes under that subdirectory
// are returned.
func (s GCIndexStore) ListIndexes(ctx context.Context, prefix string) ([]string, error) {
	listPrefix := s.prefix
	if prefix != "" {
		listPrefix = s.prefix + prefix + "/"
	}
	query := &storage.Query{Prefix: listPrefix}
	it := s.client.Objects(ctx, query)
	var names []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(attrs.Name, s.prefix)
		if name == PrunableIndexSetFile {
			continue // reserved system file; not a real index
		}
		names = append(names, name)
	}
	return names, nil
}

// SafePruneIndexes removes indexes from the store using the two-run safe protocol.
func (s GCIndexStore) SafePruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonSafePruneIndexes(ctx, keep, s)
}

// ReadPrunableIndexSet reads the prunable set written by the previous run.
// Returns an empty set if no prior state exists.
func (s GCIndexStore) ReadPrunableIndexSet(ctx context.Context) (map[string]struct{}, error) {
	r, err := s.client.Object(s.prefix + PrunableIndexSetFile).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return make(map[string]struct{}), nil
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var names []string
	if err := json.NewDecoder(r).Decode(&names); err != nil {
		if err == io.EOF {
			return make(map[string]struct{}), nil // empty object
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
func (s GCIndexStore) WritePrunableIndexSet(ctx context.Context, names []string) error {
	if len(names) == 0 {
		err := s.client.Object(s.prefix + PrunableIndexSetFile).Delete(ctx)
		if err == storage.ErrObjectNotExist {
			return nil
		}
		return err
	}
	w := s.client.Object(s.prefix + PrunableIndexSetFile).NewWriter(ctx)
	w.ContentType = "application/json"
	if err := json.NewEncoder(w).Encode(names); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}
