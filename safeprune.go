package desync

import (
	"context"
	"time"
)

// commonSafePrune implements the protect-marker safe pruning algorithm using
// the primitive operations exposed by SafePruneStore. It is called by each
// backend's SafePrune method.
//
// Phase 1 cleans up orphaned markers left by previous runs. Phase 2 marks
// new deletion candidates and deletes chunks that were already marked in a
// prior run and have no .protect companion set by a concurrent writer.
// If finalizeOnly is true, Normal chunks are not marked — only already-prunable
// chunks are acted on.
func commonSafePrune(ctx context.Context, ids map[ChunkID]int64, s SafePruneStore, finalizeOnly bool, dryRun bool) ([]ChunkID, PruneStats, error) {
	list, err := s.ListChunks(ctx)
	if err != nil {
		return nil, PruneStats{}, err
	}

	// Phase 1: clean up orphaned markers from prior runs.
	// Skipped in dry-run mode because no mutations are performed.
	if !dryRun {
		for _, e := range list {
			select {
			case <-ctx.Done():
				return nil, PruneStats{}, Interrupted{}
			default:
			}
			switch e.Status {
			case ChunkStatusOrphanedPrunable:
				if err := s.DeletePrunable(e.ID); err != nil {
					return nil, PruneStats{}, err
				}
			case ChunkStatusOrphanedProtect:
				if err := s.DeleteProtect(e.ID); err != nil {
					return nil, PruneStats{}, err
				}
			}
		}
	}

	// Phase 2: mark new candidates; check-and-delete already-marked ones.
	// A second ListChunks call reflects the post-Phase-1 state (identical to
	// list in dry-run mode since Phase 1 was skipped).
	list2, err := s.ListChunks(ctx)
	if err != nil {
		return nil, PruneStats{}, err
	}
	seen := make(map[ChunkID]struct{}, len(list2))
	var stats PruneStats
	for _, e := range list2 {
		select {
		case <-ctx.Done():
			return nil, PruneStats{}, Interrupted{}
		default:
		}
		if e.Status == ChunkStatusOrphanedPrunable || e.Status == ChunkStatusOrphanedProtect {
			continue // cleaned up in Phase 1; skip if still appearing
		}
		seen[e.ID] = struct{}{}
		if uncompSize, keep := ids[e.ID]; keep {
			stats.Kept.Count++
			stats.Kept.StoredSize += e.StoredSize
			stats.Kept.DeduplicatedSize += uncompSize
			// Chunk is referenced: remove any pruning markers and linger protect.
			if !dryRun {
				if err := s.DeletePrunable(e.ID); err != nil {
					return nil, PruneStats{}, err
				}
				if err := s.DeleteProtect(e.ID); err != nil {
					return nil, PruneStats{}, err
				}
			}
			continue
		}
		switch e.Status {
		case ChunkStatusNormal:
			if !finalizeOnly {
				stats.Prunable.Count++
				stats.Prunable.StoredSize += e.StoredSize
				if !dryRun {
					if err := s.CreatePrunable(e.ID); err != nil {
						return nil, PruneStats{}, err
					}
				}
			}
		case ChunkStatusPrunable, ChunkStatusProtected:
			// Fresh HasProtect check — may differ from the listing if a writer
			// added .protect after ListChunks ran. This is the TOCTOU guard.
			// Called even in dry-run mode because it is read-only.
			protected, err := s.HasProtect(e.ID)
			if err != nil {
				return nil, PruneStats{}, err
			}
			if protected {
				stats.Protected.Count++
				stats.Protected.StoredSize += e.StoredSize
				// Writer is saving this chunk: remove the prunable marker and
				// the protect marker; chunk survives in Normal state.
				if !dryRun {
					if err := s.DeletePrunable(e.ID); err != nil {
						return nil, PruneStats{}, err
					}
					if err := s.DeleteProtect(e.ID); err != nil {
						return nil, PruneStats{}, err
					}
				}
			} else {
				stats.Deletable.Count++
				stats.Deletable.StoredSize += e.StoredSize
				// No protect: safe to delete.
				if !dryRun {
					if err := s.DeleteChunk(e.ID); err != nil {
						return nil, PruneStats{}, err
					}
					if err := s.DeletePrunable(e.ID); err != nil {
						return nil, PruneStats{}, err
					}
				}
			}
		}
	}
	var missing []ChunkID
	for id := range ids {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing, stats, nil
}

// SafePrunePreCommit is the recheck phase of the writer-side protect-marker
// protocol. Call it BEFORE committing an index, passing the result of
// [ChunkStorage.Chunks] and [ChunkStorage.LastProtectTime].
//
// Every chunk in chunks has already had CreateProtect called by
// ChunkStorage.StoreChunk. SafePrunePreCommit waits 2×propTime from
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
