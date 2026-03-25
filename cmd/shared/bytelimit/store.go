package bytelimit

import (
	"context"

	"github.com/folbricht/desync"
)

// GatedStore wraps a desync.Store, gating GetChunk (data transfer) but
// not HasChunk (metadata query).
type GatedStore struct {
	desync.Store
	Gate *Gate
}

func (s *GatedStore) GetChunk(id desync.ChunkID, dst ...*desync.Chunk) (*desync.Chunk, error) {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return nil, err
	}
	defer s.Gate.ReleaseOp()
	return s.Store.GetChunk(id, dst...)
}

// GatedWriteStore wraps a desync.WriteStore, gating GetChunk and
// StoreChunk (data transfers) but not HasChunk (metadata query).
type GatedWriteStore struct {
	desync.WriteStore
	Gate *Gate
}

func (s *GatedWriteStore) GetChunk(id desync.ChunkID, dst ...*desync.Chunk) (*desync.Chunk, error) {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return nil, err
	}
	defer s.Gate.ReleaseOp()
	return s.WriteStore.GetChunk(id, dst...)
}

func (s *GatedWriteStore) StoreChunk(c *desync.Chunk) error {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return err
	}
	defer s.Gate.ReleaseOp()
	return s.WriteStore.StoreChunk(c)
}

// GatedIndexWriteStore wraps a desync.IndexWriteStore, gating GetIndex
// and StoreIndex (data transfers) but not HasIndex or GetIndexReader
// (metadata query / called within GetIndex).
type GatedIndexWriteStore struct {
	desync.IndexWriteStore
	Gate *Gate
}

func (s *GatedIndexWriteStore) GetIndex(name string) (desync.Index, error) {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return desync.Index{}, err
	}
	defer s.Gate.ReleaseOp()
	return s.IndexWriteStore.GetIndex(name)
}

func (s *GatedIndexWriteStore) StoreIndex(name string, idx desync.Index) error {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return err
	}
	defer s.Gate.ReleaseOp()
	return s.IndexWriteStore.StoreIndex(name, idx)
}
