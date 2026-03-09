package desync

import "context"

// commonPrune implements normal (non-safe) pruning via ListChunks + DeleteChunk.
// It is called by each backend's Prune method. It returns the IDs that were
// present in ids but absent from the store (missing chunks).
func commonPrune(ctx context.Context, ids map[ChunkID]struct{}, s PruneStore) ([]ChunkID, error) {
	list, err := s.ListChunks(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[ChunkID]struct{}, len(list))
	for _, e := range list {
		select {
		case <-ctx.Done():
			return nil, Interrupted{}
		default:
		}
		// Orphaned markers have no chunk data; nothing to delete.
		if e.Status == ChunkStatusOrphanedPrunable || e.Status == ChunkStatusOrphanedProtect {
			continue
		}
		seen[e.ID] = struct{}{}
		if _, keep := ids[e.ID]; !keep {
			if err := s.DeleteChunk(e.ID); err != nil {
				return nil, err
			}
		}
	}
	var missing []ChunkID
	for id := range ids {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing, nil
}
