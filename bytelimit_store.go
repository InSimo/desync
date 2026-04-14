package desync

import "context"

// GatedStore wraps a Store, gating GetChunk (data transfer) but
// not HasChunk (metadata query).
type GatedStore struct {
	Store
	Gate *Gate
}

func (s *GatedStore) GetChunk(id ChunkID) (*Chunk, error) {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return nil, err
	}
	defer s.Gate.ReleaseOp()
	return s.Store.GetChunk(id)
}

// GatedWriteStore wraps a WriteStore, gating GetChunk and
// StoreChunk (data transfers) but not HasChunk (metadata query).
type GatedWriteStore struct {
	WriteStore
	Gate *Gate
}

func (s *GatedWriteStore) GetChunk(id ChunkID) (*Chunk, error) {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return nil, err
	}
	defer s.Gate.ReleaseOp()
	return s.WriteStore.GetChunk(id)
}

func (s *GatedWriteStore) StoreChunk(c *Chunk) error {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return err
	}
	defer s.Gate.ReleaseOp()
	return s.WriteStore.StoreChunk(c)
}

// GatedIndexWriteStore wraps a IndexWriteStore, gating GetIndex
// and StoreIndex (data transfers) but not HasIndex or GetIndexReader
// (metadata query / called within GetIndex).
type GatedIndexWriteStore struct {
	IndexWriteStore
	Gate *Gate
}

func (s *GatedIndexWriteStore) GetIndex(name string) (Index, error) {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return Index{}, err
	}
	defer s.Gate.ReleaseOp()
	return s.IndexWriteStore.GetIndex(name)
}

func (s *GatedIndexWriteStore) StoreIndex(name string, idx Index) error {
	if err := s.Gate.AcquireOp(context.TODO()); err != nil {
		return err
	}
	defer s.Gate.ReleaseOp()
	return s.IndexWriteStore.StoreIndex(name, idx)
}
