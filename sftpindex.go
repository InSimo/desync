package desync

import (
	"context"
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

func (s *SFTPIndexStore) pathFromName(name string) string {
	return path.Join(s.path, name)
}

// PruneIndexes removes all indexes from the store that are not in the keep set.
func (s *SFTPIndexStore) PruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	names, err := s.ListIndexes(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, ok := keep[name]; ok {
			continue
		}
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

// ListIndexes returns the names of all indexes in the store, relative to the store root.
func (s *SFTPIndexStore) ListIndexes(ctx context.Context) ([]string, error) {
	walker := s.client.Walk(strings.TrimSuffix(s.path, "/"))
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
		names = append(names, rel)
	}
	return names, nil
}
