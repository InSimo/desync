package desync

import (
	"sync"
	"time"
)

// ChunkStorage stores chunks in a writable store. It can be safely used by multiple goroutines and
// contains an internal cache of what chunks have been stored previously.
//
// When constructed with NewChunkStorageWithPruning, it also handles the
// safe-pruning writer protocol: prunable chunks that are reused get a
// .protect marker and are captured in memory for SafePrunePreCommit.
type ChunkStorage struct {
	sync.Mutex
	ws              WriteStore
	processed       map[ChunkID]struct{}
	sps             SafePruneStore     // nil when safe pruning disabled
	captured        map[ChunkID]*Chunk // prunable chunks captured for rechecking
	lastProtectTime time.Time
}

// NewChunkStorage initializes a ChunkStorage object.
func NewChunkStorage(ws WriteStore) *ChunkStorage {
	return &ChunkStorage{
		ws:        ws,
		processed: make(map[ChunkID]struct{}),
	}
}

// NewChunkStorageWithPruning initializes a ChunkStorage that participates in
// the safe-pruning protocol. Prunable chunks that are reused get a .protect
// marker and are captured for SafePrunePreCommit.
func NewChunkStorageWithPruning(ws WriteStore, sps SafePruneStore) *ChunkStorage {
	return &ChunkStorage{
		ws:        ws,
		processed: make(map[ChunkID]struct{}),
		sps:       sps,
		captured:  make(map[ChunkID]*Chunk),
	}
}

// Mark a chunk in the in-memory cache as having been processed and returns true
// if it was already marked, and is therefore presumably already stored.
func (s *ChunkStorage) markProcessed(id ChunkID) bool {
	s.Lock()
	defer s.Unlock()
	_, ok := s.processed[id]
	s.processed[id] = struct{}{}
	return ok
}

// Unmark a chunk in the in-memory cache. This is used if a chunk is first
// marked as processed, but then actually fails to be stored. Unmarking the
// makes it eligible to be re-tried again in case of errors.
func (s *ChunkStorage) unmarkProcessed(id ChunkID) {
	s.Lock()
	defer s.Unlock()
	delete(s.processed, id)
}

// StoreChunk stores a single chunk in a synchronous manner.
// The returned captured flag indicates whether the chunk was retained
// for safe-pruning verification (callers that pool chunks must not
// reclaim a captured chunk until after SafePrunePreCommit).
func (s *ChunkStorage) StoreChunk(chunk *Chunk) (captured bool, err error) {

	// Mark this chunk as done so no other goroutine will attempt to store it
	// at the same time. If this is the first time this chunk is marked, it'll
	// return false and we need to continue processing/storing the chunk below.
	if s.markProcessed(chunk.ID()) {
		return false, nil
	}

	id := chunk.ID()

	present, err := s.ws.HasChunk(id)
	if err != nil {
		s.unmarkProcessed(id)
		return false, err
	}
	if present {
		// Chunk already in store. When safe pruning is active, check whether
		// it carries a .prunable marker. If so, add a .protect marker and
		// capture the chunk data for SafePrunePreCommit rechecking.
		if s.sps != nil {
			prunable, err := s.sps.HasPrunable(id)
			if err != nil {
				s.unmarkProcessed(id)
				return false, err
			}
			if prunable {
				if err := s.sps.CreateProtect(id); err != nil {
					s.unmarkProcessed(id)
					return false, err
				}
				s.Lock()
				s.captured[id] = chunk
				s.lastProtectTime = time.Now()
				s.Unlock()
				return true, nil
			}
		}
		return false, nil
	}

	// Chunk absent: store it.
	// The chunk was marked as "processed" above. If there's a problem to actually
	// store it, we need to unmark it again.
	defer func() {
		if err != nil {
			s.unmarkProcessed(id)
		}
	}()

	return false, s.ws.StoreChunk(chunk)
}

// Chunks returns a snapshot of the prunable chunks captured so far.
// Every chunk in the returned map has already had CreateProtect called.
// Returns nil if safe pruning is not enabled.
func (s *ChunkStorage) Chunks() map[ChunkID]*Chunk {
	s.Lock()
	defer s.Unlock()
	if s.captured == nil {
		return nil
	}
	out := make(map[ChunkID]*Chunk, len(s.captured))
	for id, ch := range s.captured {
		out[id] = ch
	}
	return out
}

// LastProtectTime returns the time of the most recent CreateProtect call,
// or the zero time if no chunks were protected.
func (s *ChunkStorage) LastProtectTime() time.Time {
	s.Lock()
	defer s.Unlock()
	return s.lastProtectTime
}
