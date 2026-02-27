package desync

import "context"

// commonSafePrune implements the two-phase safe pruning algorithm using the
// primitive operations exposed by SafePruneStore. It is called by each
// backend's SafePrune method.
//
// Phase 1 removes any leftover .pruning files from a prior run and cleans up
// orphaned .prunable markers. Phase 2 marks new candidates for deletion and
// quarantines chunks that were already marked in a previous run.
func commonSafePrune(ctx context.Context, ids map[ChunkID]struct{}, s SafePruneStore) error {
	list, err := s.ListChunks(ctx)
	if err != nil {
		return err
	}

	// Phase 1: delete quarantined chunks from a prior run; delete orphaned markers.
	for _, e := range list {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		switch e.Status {
		case ChunkStatusPruning:
			if err := s.DeletePruning(e.ID); err != nil {
				return err
			}
		case ChunkStatusOrphanedPrunable:
			if err := s.DeleteMarker(e.ID); err != nil {
				return err
			}
		}
	}

	// Phase 2: mark new candidates; quarantine already-marked ones.
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
		if e.Status == ChunkStatusPruning || e.Status == ChunkStatusOrphanedPrunable {
			continue // cleaned up in Phase 1; skip if still appearing
		}
		if _, keep := ids[e.ID]; keep {
			if err := s.DeleteMarker(e.ID); err != nil {
				return err
			}
			continue
		}
		switch e.Status {
		case ChunkStatusNormal:
			if err := s.CreateMarker(e.ID); err != nil {
				return err
			}
		case ChunkStatusPrunable:
			// Quarantine implementations may call a test hook internally between
			// the rename and return to simulate race windows.
			if err := s.Quarantine(e.ID); err != nil {
				return err
			}
			// Post-quarantine TOCTOU re-check: if .prunable is gone, a writer
			// removed it concurrently and may have committed an index referencing
			// this chunk. Revert the quarantine so the chunk is not left invisible.
			still, err := s.MarkerExists(e.ID)
			if err != nil {
				return err
			}
			if !still {
				_ = s.Restore(e.ID) // best-effort revert; error intentionally ignored
				continue
			}
			if err := s.DeleteMarker(e.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// commonRescueChunks implements the RescueChunks algorithm using the primitive
// operations exposed by SafePruneStore. For each chunk in ids it removes any
// .prunable marker, then either deletes a stale .pruning file (if the chunk
// data is live) or restores the chunk from quarantine (if the data is in .pruning).
func commonRescueChunks(ctx context.Context, ids map[ChunkID]struct{}, s SafePruneStore) error {
	for id := range ids {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err := s.DeleteMarker(id); err != nil {
			return err
		}
		hasChunk, err := s.HasChunk(id)
		if err != nil {
			return err
		}
		if hasChunk {
			if err := s.DeletePruning(id); err != nil {
				return err
			}
		} else {
			if err := s.Restore(id); err != nil {
				return err
			}
		}
	}
	return nil
}
