package desync

import (
	"github.com/insimo/cacheevict"
)

var _ WriteStore = &SizeLimitStore{}

// SizeLimitStore wraps a LocalStore and adds automatic LRU cache eviction
// to keep total stored bytes within a configurable limit. It delegates all
// eviction logic to a cacheevict.Handler.
type SizeLimitStore struct {
	ls      LocalStore
	handler *cacheevict.Handler
}

// NewSizeLimitStore creates a SizeLimitStore wrapping ls. maxSize is the
// maximum cache size in bytes (0 = no byte limit). maxFiles is the maximum
// number of cached files (0 = no file limit). partitions must be a power
// of 2 (or 0 for the default of 256).
func NewSizeLimitStore(ls LocalStore, maxSize int64, maxFiles int64, partitions int) (*SizeLimitStore, error) {
	cfg := DesyncCacheEvictConfig(ls.Base, ls.Opt.Uncompressed)
	cfg.MaxSize = maxSize
	cfg.MaxFiles = maxFiles
	cfg.Partitions = partitions

	handler, err := cacheevict.Open(cfg)
	if err != nil {
		return nil, err
	}

	return &SizeLimitStore{ls: ls, handler: handler}, nil
}

func (s *SizeLimitStore) GetChunk(id ChunkID) (*Chunk, error) {
	chunk, err := s.ls.GetChunk(id)
	if err != nil {
		return chunk, err
	}
	_, p := s.ls.nameFromID(id)
	s.handler.UseFile(p)
	return chunk, nil
}

func (s *SizeLimitStore) HasChunk(id ChunkID) (bool, error) {
	return s.ls.HasChunk(id)
}

func (s *SizeLimitStore) StoreChunk(chunk *Chunk) error {
	_, p := s.ls.nameFromID(chunk.ID())
	oldSize := s.handler.BeforeStore(p)
	if err := s.ls.StoreChunk(chunk); err != nil {
		return err
	}
	s.handler.AfterStore(p, oldSize)
	return nil
}

func (s *SizeLimitStore) RemoveChunk(id ChunkID) error {
	_, p := s.ls.nameFromID(id)
	s.handler.BeforeRemove(p)
	return s.ls.RemoveChunk(id)
}

func (s *SizeLimitStore) String() string {
	return s.ls.String()
}

func (s *SizeLimitStore) Close() error {
	if s.handler != nil {
		return s.handler.Close()
	}
	return nil
}

// --- Delegated methods ---

func (s *SizeLimitStore) GetChunkSize(id ChunkID) (int64, error) {
	return s.ls.GetChunkSize(id)
}

// TotalSize returns the current tracked total cache size in bytes.
func (s *SizeLimitStore) TotalSize() int64 {
	return s.handler.TotalSize()
}

// TotalFiles returns the current tracked total file count.
func (s *SizeLimitStore) TotalFiles() int64 {
	return s.handler.TotalFiles()
}
