package desync

import "context"

// commonPrune implements normal (non-safe) pruning via ListChunks + DeleteChunk.
// It is called by each backend's Prune method.
func commonPrune(ctx context.Context, ids map[ChunkID]struct{}, s SafePruneStore) error {
	list, err := s.ListChunks(ctx)
	if err != nil {
		return err
	}
	for _, e := range list {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		// Orphaned markers have no chunk data; nothing to delete.
		if e.Status == ChunkStatusOrphanedPrunable || e.Status == ChunkStatusOrphanedProtect {
			continue
		}
		if _, keep := ids[e.ID]; !keep {
			if err := s.DeleteChunk(e.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
