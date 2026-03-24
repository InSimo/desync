package desync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// LocalIndexStore is used to read/write index files on local disk
type LocalIndexStore struct {
	Path string
}

// NewLocalIndexStore creates an instance of a local index store, it only checks presence
// of the store
func NewLocalIndexStore(path string) (LocalIndexStore, error) {
	info, err := os.Stat(path)
	if err != nil {
		return LocalIndexStore{}, err
	}
	if !info.IsDir() {
		return LocalIndexStore{}, fmt.Errorf("%s is not a directory", path)
	}
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return LocalIndexStore{Path: path}, nil
}

// GetIndexReader returns a reader of an index file in the store or an error if
// the specified index file does not exist.
func (s LocalIndexStore) GetIndexReader(name string) (rdr io.ReadCloser, e error) {
	return os.Open(s.Path + name)
}

// GetIndex returns an Index structure from the store
func (s LocalIndexStore) GetIndex(name string) (i Index, e error) {
	f, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer f.Close()
	idx, err := IndexFromReader(f)
	if os.IsNotExist(err) {
		err = errors.Errorf("Index file does not exist: %v", err)
	}
	return idx, err
}

// StoreIndex stores an index in the index store with the given name.
func (s LocalIndexStore) StoreIndex(name string, idx Index) error {
	path := s.Path + name
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	i, err := os.Create(path)
	if err != nil {
		return err
	}
	defer i.Close()
	_, err = idx.WriteTo(i)
	return err
}

// HasIndex returns true if an index with the given name exists in the store.
func (s LocalIndexStore) HasIndex(name string) (bool, error) {
	_, err := os.Stat(s.Path + name)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// StatIndex returns metadata about the named index. For local stores the
// modification time comes from the filesystem and the original content size
// is computed by parsing the index (fast for local I/O).
func (s LocalIndexStore) StatIndex(name string) (IndexInfo, error) {
	fi, err := os.Stat(s.Path + name)
	if err != nil {
		return IndexInfo{}, err
	}
	idx, err := s.GetIndex(name)
	if err != nil {
		return IndexInfo{}, err
	}
	return IndexInfo{
		Name:    name,
		Size:    idx.TotalSize(),
		ModTime: fi.ModTime(),
	}, nil
}

func (s LocalIndexStore) String() string {
	return s.Path
}

// Close the index store. NOP operation, needed to implement IndexStore interface
func (s LocalIndexStore) Close() error { return nil }

// PruneIndexes removes all indexes from the store that are not in the keep set.
func (s LocalIndexStore) PruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonPruneIndexes(ctx, keep, s)
}

// DeleteIndexes removes the named indexes from the store.
func (s LocalIndexStore) DeleteIndexes(ctx context.Context, names []string) error {
	for _, name := range names {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err := os.Remove(s.Path + name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// ListIndexes returns the relative paths of index files in the store,
// always relative to the store root. If prefix is non-empty, only indexes
// under that subdirectory are returned.
func (s LocalIndexStore) ListIndexes(ctx context.Context, prefix string) ([]string, error) {
	walkRoot := s.Path
	if prefix != "" {
		walkRoot = filepath.Join(s.Path, filepath.FromSlash(prefix))
	}
	var names []string
	err := filepath.WalkDir(walkRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.Path, p)
		if err != nil {
			return err
		}
		if rel == PrunableIndexSetFile {
			return nil // reserved system file; not a real index
		}
		names = append(names, rel)
		return nil
	})
	return names, err
}

// SafePruneIndexes removes indexes from the store using the two-run safe protocol.
func (s LocalIndexStore) SafePruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonSafePruneIndexes(ctx, keep, s)
}

// ReadPrunableIndexSet reads the prunable set written by the previous run.
// Returns an empty set if no prior state exists.
func (s LocalIndexStore) ReadPrunableIndexSet(_ context.Context) (map[string]struct{}, error) {
	f, err := os.Open(s.Path + PrunableIndexSetFile)
	if os.IsNotExist(err) {
		return make(map[string]struct{}), nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var names []string
	if err := json.NewDecoder(f).Decode(&names); err != nil {
		if err == io.EOF {
			return make(map[string]struct{}), nil // empty file from interrupted write
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
// Removes the file when names is empty (no candidates remain).
func (s LocalIndexStore) WritePrunableIndexSet(_ context.Context, names []string) error {
	p := s.Path + PrunableIndexSetFile
	if len(names) == 0 {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(names)
}
