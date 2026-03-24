package desync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
)

// SFTPIndexStore is an index store backed by SFTP over SSH
type SFTPIndexStore struct {
	*SFTPStoreBase
}

// NewSFTPIndexStore initializes and index store backed by SFTP over SSH.
func NewSFTPIndexStore(location *url.URL, opt StoreOptions) (*SFTPIndexStore, error) {
	b, err := newSFTPStoreBase(location, opt)
	if err != nil {
		return nil, err
	}
	return &SFTPIndexStore{b}, nil
}

// GetIndexReader returns a reader of an index from an SFTP store. Fails if the specified
// index file does not exist.
func (s *SFTPIndexStore) GetIndexReader(name string) (r io.ReadCloser, e error) {
	f, err := s.client.Open(s.pathFromName(name))
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.Errorf("Index file does not exist: %v", err)
		}
		return r, err
	}
	return f, nil
}

// GetIndex reads an index from an SFTP store, returns an error if the specified index file does not exist.
func (s *SFTPIndexStore) GetIndex(name string) (i Index, e error) {
	f, err := s.GetIndexReader(name)
	if err != nil {
		return i, err
	}
	defer f.Close()
	return IndexFromReader(f)
}

// StoreIndex adds a new index to the store
func (s *SFTPIndexStore) StoreIndex(name string, idx Index) error {
	r, w := io.Pipe()

	go func() {
		defer w.Close()
		idx.WriteTo(w)
	}()
	return s.StoreObject(s.pathFromName(name), r)
}

// HasIndex returns true if an index with the given name exists in the store.
func (s *SFTPIndexStore) HasIndex(name string) (bool, error) {
	_, err := s.client.Stat(s.pathFromName(name))
	return err == nil, nil
}

// StatIndex returns metadata about the named index. For SFTP stores the
// modification time comes from the remote stat and the original content
// size is computed by parsing the index.
func (s *SFTPIndexStore) StatIndex(name string) (IndexInfo, error) {
	fi, err := s.client.Stat(s.pathFromName(name))
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

func (s *SFTPIndexStore) pathFromName(name string) string {
	return path.Join(s.path, name)
}

// PruneIndexes removes all indexes from the store that are not in the keep set.
func (s *SFTPIndexStore) PruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonPruneIndexes(ctx, keep, s)
}

// DeleteIndexes removes the named indexes from the store.
func (s *SFTPIndexStore) DeleteIndexes(ctx context.Context, names []string) error {
	for _, name := range names {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err := s.client.Remove(s.pathFromName(name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// ListIndexes returns the names of indexes in the store, relative to the
// store root. If prefix is non-empty, only indexes under that subdirectory
// are returned.
func (s *SFTPIndexStore) ListIndexes(ctx context.Context, prefix string) ([]string, error) {
	walkRoot := strings.TrimSuffix(s.path, "/")
	if prefix != "" {
		walkRoot = walkRoot + "/" + prefix
	}
	walker := s.client.Walk(walkRoot)
	var names []string
	for walker.Step() {
		if err := walker.Err(); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, Interrupted{}
		default:
		}
		if walker.Stat().IsDir() {
			continue
		}
		rel := strings.TrimPrefix(walker.Path(), s.path)
		if rel == PrunableIndexSetFile {
			continue // reserved system file; not a real index
		}
		names = append(names, rel)
	}
	return names, nil
}

// SafePruneIndexes removes indexes from the store using the two-run safe protocol.
func (s *SFTPIndexStore) SafePruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonSafePruneIndexes(ctx, keep, s)
}

// ReadPrunableIndexSet reads the prunable set written by the previous run.
// Returns an empty set if no prior state exists.
func (s *SFTPIndexStore) ReadPrunableIndexSet(_ context.Context) (map[string]struct{}, error) {
	f, err := s.client.Open(s.pathFromName(PrunableIndexSetFile))
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
// Removes the file when names is empty (no candidates remain). Uses an atomic
// PosixRename via StoreObject to avoid leaving a partially-written file.
func (s *SFTPIndexStore) WritePrunableIndexSet(_ context.Context, names []string) error {
	if len(names) == 0 {
		err := s.client.Remove(s.pathFromName(PrunableIndexSetFile))
		if err != nil && os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(names); err != nil {
		return err
	}
	return s.StoreObject(s.pathFromName(PrunableIndexSetFile), &buf)
}
