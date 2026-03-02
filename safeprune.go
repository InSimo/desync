package desync

import (
	"context"
	"sync"
	"time"
)

// commonSafePrune implements the protect-marker safe pruning algorithm using
// the primitive operations exposed by SafePruneStore. It is called by each
// backend's SafePrune method.
//
// Phase 1 cleans up orphaned markers left by previous runs. Phase 2 marks
// new deletion candidates and deletes chunks that were already marked in a
// prior run and have no .protect companion set by a concurrent writer.
func commonSafePrune(ctx context.Context, ids map[ChunkID]struct{}, s SafePruneStore) error {
	list, err := s.ListChunks(ctx)
	if err != nil {
		return err
	}

	// Phase 1: clean up orphaned markers from prior runs.
	for _, e := range list {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		switch e.Status {
		case ChunkStatusOrphanedPrunable:
			if err := s.DeleteMarker(e.ID); err != nil {
				return err
			}
		case ChunkStatusOrphanedProtect:
			if err := s.DeleteProtect(e.ID); err != nil {
				return err
			}
		}
	}

	// Phase 2: mark new candidates; check-and-delete already-marked ones.
	// A second ListChunks call reflects the post-Phase-1 state.
	list2, err := s.ListChunks(ctx)
	if err != nil {
		return err
	}
	for _, e := range list2 {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if e.Status == ChunkStatusOrphanedPrunable || e.Status == ChunkStatusOrphanedProtect {
			continue // cleaned up in Phase 1; skip if still appearing
		}
		if _, keep := ids[e.ID]; keep {
			// Chunk is referenced: remove any pruning markers and linger protect.
			if err := s.DeleteMarker(e.ID); err != nil {
				return err
			}
			if err := s.DeleteProtect(e.ID); err != nil {
				return err
			}
			continue
		}
		switch e.Status {
		case ChunkStatusNormal:
			if err := s.CreateMarker(e.ID); err != nil {
				return err
			}
		case ChunkStatusPrunable, ChunkStatusProtected:
			// Fresh HasProtect check — may differ from the listing if a writer
			// added .protect after ListChunks ran. This is the TOCTOU guard.
			protected, err := s.HasProtect(e.ID)
			if err != nil {
				return err
			}
			if protected {
				// Writer is saving this chunk: remove the prunable marker and
				// the protect marker; chunk survives in Normal state.
				if err := s.DeleteMarker(e.ID); err != nil {
					return err
				}
				if err := s.DeleteProtect(e.ID); err != nil {
					return err
				}
			} else {
				// No protect: safe to delete.
				if err := s.DeleteChunk(e.ID); err != nil {
					return err
				}
				if err := s.DeleteMarker(e.ID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// CapturingWriteStore wraps a WriteStore and intercepts the ReuseChunk /
// StoreChunk two-step protocol. When ReuseChunk detects a prunable chunk, it
// sets a protect marker and records the chunk ID in protectPending. The
// subsequent StoreChunk call (driven by ChunkStorage) captures the chunk data
// in memory without re-storing it. Fresh chunks and non-prunable reused chunks
// pass through unmodified.
//
// After chunking, pass [CapturingWriteStore.Chunks] and
// [CapturingWriteStore.LastProtectTime] to [SafePrunePreCommit].
type CapturingWriteStore struct {
	WriteStore
	sps             SafePruneStore
	mu              sync.Mutex
	chunks          map[ChunkID]*Chunk
	lastProtectTime time.Time
	protectPending  map[ChunkID]struct{} // set by ReuseChunk, consumed by StoreChunk
}

// NewCapturingWriteStore wraps ws and, for each chunk reused through it,
// calls HasPrunable on sps. Prunable chunks are captured and protected in-line.
func NewCapturingWriteStore(ws WriteStore, sps SafePruneStore) *CapturingWriteStore {
	return &CapturingWriteStore{
		WriteStore:     ws,
		sps:            sps,
		chunks:         make(map[ChunkID]*Chunk),
		protectPending: make(map[ChunkID]struct{}),
	}
}

// ReuseChunk checks whether the chunk is already present in the underlying
// store. If absent → ReuseAbsent. If present and not prunable → ReuseOK. If
// present and prunable → CreateProtect, mark protectPending, return
// ReuseProtectRequired so that ChunkStorage forwards the chunk to StoreChunk
// for capture.
func (c *CapturingWriteStore) ReuseChunk(id ChunkID) (ReuseStatus, error) {
	present, err := c.WriteStore.HasChunk(id)
	if err != nil {
		return 0, err
	}
	if !present {
		return ReuseAbsent, nil
	}
	prunable, err := c.sps.HasPrunable(id)
	if err != nil {
		return 0, err
	}
	if !prunable {
		return ReuseOK, nil
	}
	if err := c.sps.CreateProtect(id); err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.protectPending[id] = struct{}{}
	c.mu.Unlock()
	return ReuseProtectRequired, nil
}

// StoreChunk checks whether the chunk was flagged by ReuseChunk as needing
// capture (protectPending). If so, it captures the chunk data in memory
// without re-storing it. Otherwise it forwards to the underlying WriteStore.
func (c *CapturingWriteStore) StoreChunk(chunk *Chunk) error {
	id := chunk.ID()
	c.mu.Lock()
	_, pending := c.protectPending[id]
	if pending {
		delete(c.protectPending, id)
		c.chunks[id] = chunk
		c.lastProtectTime = time.Now()
	}
	c.mu.Unlock()
	if pending {
		return nil
	}
	return c.WriteStore.StoreChunk(chunk)
}

// Chunks returns a snapshot of the prunable chunks captured so far.
// Every chunk in the returned map has already had CreateProtect called.
func (c *CapturingWriteStore) Chunks() map[ChunkID]*Chunk {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[ChunkID]*Chunk, len(c.chunks))
	for id, ch := range c.chunks {
		out[id] = ch
	}
	return out
}

// LastProtectTime returns the time of the most recent CreateProtect call,
// or the zero time if no chunks were protected.
func (c *CapturingWriteStore) LastProtectTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastProtectTime
}

// SafePrunePreCommit is the recheck phase of the writer-side protect-marker
// protocol. Call it BEFORE committing an index, passing the result of
// [CapturingWriteStore.Chunks] and [CapturingWriteStore.LastProtectTime].
//
// Every chunk in chunks has already had CreateProtect called by
// CapturingWriteStore.StoreChunk. SafePrunePreCommit waits 2×propTime from
// lastProtect for the protect markers to propagate (use 0 for local/SFTP
// stores), then verifies each chunk is still present. Any chunk deleted by a
// concurrent pruner during the race window is re-uploaded and re-protected,
// followed by a second 2×propTime wait.
func SafePrunePreCommit(ctx context.Context, chunks map[ChunkID]*Chunk, lastProtect time.Time, s SafePruneStore, propTime time.Duration) error {
	if len(chunks) == 0 {
		return nil
	}

	// Wait 2×propTime from when the last protect was written.
	if propTime > 0 {
		if remaining := time.Until(lastProtect.Add(2 * propTime)); remaining > 0 {
			select {
			case <-ctx.Done():
				return Interrupted{}
			case <-time.After(remaining):
			}
		}
	}

	var lastReuploadTime time.Time
	for id, chunk := range chunks {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		ok, err := s.HasChunk(id)
		if err != nil {
			return err
		}
		if !ok {
			// Chunk was deleted by a concurrent pruner in the race window.
			// Re-upload it and re-add protect so the next pruner cycle keeps it.
			if err := s.StoreChunk(chunk); err != nil {
				return err
			}
			if err := s.CreateProtect(id); err != nil {
				return err
			}
			lastReuploadTime = time.Now()
		}
	}

	// After re-uploads, wait another 2×propTime for the fresh chunk and its
	// protect marker to propagate before the caller commits the index.
	if !lastReuploadTime.IsZero() && propTime > 0 {
		if remaining := time.Until(lastReuploadTime.Add(2 * propTime)); remaining > 0 {
			select {
			case <-ctx.Done():
				return Interrupted{}
			case <-time.After(remaining):
			}
		}
	}
	return nil
}
