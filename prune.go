package desync

import "context"

// ChunkSizeStats bundles a chunk count with the aggregate stored and
// deduplicated sizes for a subset of chunks.
type ChunkSizeStats struct {
	Count           int
	StoredSize      int64 // sum of on-disk/object sizes (from ListChunks)
	DeduplicatedSize int64 // sum of uncompressed sizes from the ids map; 0 when unavailable
}

// PruneStats holds counters reported by Prune and SafePrune. In dry-run mode
// the counters reflect what would happen; in normal mode they reflect what did.
// Orphaned markers (.prunable or .protect without a matching chunk) are not
// counted because they carry no chunk data.
type PruneStats struct {
	// Deletable counts chunks deleted (or that would be deleted).
	// In safe-pruning mode this counts only already-prunable chunks with no
	// protect marker; in normal mode it counts all unreferenced chunks.
	// DeduplicatedSize is always 0 (unreferenced chunks are not in ids).
	Deletable ChunkSizeStats

	// Prunable counts Normal chunks marked as prunable (or that would be
	// marked) during the first pass of safe pruning. Always zero in normal
	// (non-safe) pruning mode.
	// DeduplicatedSize is always 0 (unreferenced chunks are not in ids).
	Prunable ChunkSizeStats

	// Protected counts chunks kept because a concurrent writer set a protect
	// marker. Always zero in normal pruning mode.
	// DeduplicatedSize is always 0 (unreferenced chunks are not in ids).
	Protected ChunkSizeStats

	// Kept counts chunks retained because they are referenced by the ids set.
	// Both StoredSize and DeduplicatedSize are populated.
	Kept ChunkSizeStats
}

// commonPrune implements normal (non-safe) pruning via ListChunks + DeleteChunk.
// It is called by each backend's Prune method. It returns the IDs that were
// present in ids but absent from the store (missing chunks), pruning statistics,
// and any error.
func commonPrune(ctx context.Context, ids map[ChunkID]int64, s PruneStore, dryRun bool) ([]ChunkID, PruneStats, error) {
	list, err := s.ListChunks(ctx)
	if err != nil {
		return nil, PruneStats{}, err
	}
	seen := make(map[ChunkID]struct{}, len(list))
	var stats PruneStats
	for _, e := range list {
		select {
		case <-ctx.Done():
			return nil, PruneStats{}, Interrupted{}
		default:
		}
		// Orphaned markers have no chunk data; nothing to delete.
		if e.Status == ChunkStatusOrphanedPrunable || e.Status == ChunkStatusOrphanedProtect {
			continue
		}
		seen[e.ID] = struct{}{}
		if uncompSize, keep := ids[e.ID]; !keep {
			stats.Deletable.Count++
			stats.Deletable.StoredSize += e.StoredSize
			if !dryRun {
				if err := s.DeleteChunk(e.ID); err != nil {
					return nil, PruneStats{}, err
				}
			}
		} else {
			stats.Kept.Count++
			stats.Kept.StoredSize += e.StoredSize
			stats.Kept.DeduplicatedSize += uncompSize
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
