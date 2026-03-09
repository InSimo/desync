package desync

import "context"

// PruneStats holds counters reported by Prune and SafePrune. In dry-run mode
// the counters reflect what would happen; in normal mode they reflect what did.
// Orphaned markers (.prunable or .protect without a matching chunk) are not
// counted because they carry no chunk data.
type PruneStats struct {
	// Deletable is the number of chunks deleted, or that would be deleted.
	// In safe-pruning mode this counts only already-prunable chunks with no
	// protect marker; in normal mode it counts all unreferenced chunks.
	Deletable int

	// Prunable is the number of Normal chunks marked as prunable (or that would
	// be marked) during the first pass of safe pruning. Always zero in normal
	// (non-safe) pruning mode.
	Prunable int

	// Protected is the number of chunks that were kept because a concurrent
	// writer had set a protect marker. Always zero in normal pruning mode.
	Protected int

	// Kept is the number of chunks retained because they are referenced by
	// the provided index set.
	Kept int
}

// commonPrune implements normal (non-safe) pruning via ListChunks + DeleteChunk.
// It is called by each backend's Prune method. It returns the IDs that were
// present in ids but absent from the store (missing chunks), pruning statistics,
// and any error.
func commonPrune(ctx context.Context, ids map[ChunkID]struct{}, s PruneStore, dryRun bool) ([]ChunkID, PruneStats, error) {
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
		if _, keep := ids[e.ID]; !keep {
			stats.Deletable++
			if !dryRun {
				if err := s.DeleteChunk(e.ID); err != nil {
					return nil, PruneStats{}, err
				}
			}
		} else {
			stats.Kept++
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
