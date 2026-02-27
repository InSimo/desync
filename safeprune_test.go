package desync

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLocalStoreSafePruneFullCycle verifies the three-run state machine:
//
//	run 1: unreferenced chunk gains a .prunable marker (still readable)
//	run 2: marked chunk is quarantined (.cacnk → .pruning, invisible to readers)
//	run 3: quarantined .pruning file is deleted
func TestLocalStoreSafePruneFullCycle(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	keep := NewChunk([]byte("keep this chunk"))
	prune := NewChunk([]byte("prune this chunk"))
	require.NoError(t, s.StoreChunk(keep))
	require.NoError(t, s.StoreChunk(prune))

	keepID := keep.ID()
	pruneID := prune.ID()
	keepSet := map[ChunkID]struct{}{keepID: {}}

	_, keepPath := s.nameFromID(keepID)
	_, prunePath := s.nameFromID(pruneID)
	prunePrunable, prunePruning := s.pruningPathsFromID(pruneID)
	keepPrunable, _ := s.pruningPathsFromID(keepID)

	// --- Run 1: mark ---
	require.NoError(t, s.SafePrune(ctx, keepSet))

	_, err = os.Stat(keepPath)
	require.NoError(t, err, "keep chunk must survive run 1")
	_, err = os.Stat(keepPrunable)
	require.True(t, os.IsNotExist(err), "keep chunk must not gain a .prunable marker")

	_, err = os.Stat(prunePath)
	require.NoError(t, err, "prune chunk .cacnk must still exist after run 1")
	_, err = os.Stat(prunePrunable)
	require.NoError(t, err, "prune chunk must have a .prunable marker after run 1")
	_, err = os.Stat(prunePruning)
	require.True(t, os.IsNotExist(err), "prune chunk must not be quarantined after run 1")

	// Marked chunk is still readable.
	_, err = s.GetChunk(pruneID)
	require.NoError(t, err, "marked chunk must still be readable")

	// --- Run 2: quarantine ---
	require.NoError(t, s.SafePrune(ctx, keepSet))

	_, err = os.Stat(prunePath)
	require.True(t, os.IsNotExist(err), "prune chunk .cacnk must be gone after run 2")
	_, err = os.Stat(prunePruning)
	require.NoError(t, err, "prune chunk must be quarantined (.pruning) after run 2")
	_, err = os.Stat(prunePrunable)
	require.True(t, os.IsNotExist(err), "prune chunk .prunable must be gone after run 2")

	// Quarantined chunk is invisible to readers.
	_, err = s.GetChunk(pruneID)
	require.Error(t, err)
	_, ok := err.(ChunkMissing)
	require.True(t, ok, "quarantined chunk must appear missing to readers")

	// Keep chunk still intact.
	_, err = s.GetChunk(keepID)
	require.NoError(t, err, "keep chunk must survive run 2")

	// --- Run 3: delete ---
	require.NoError(t, s.SafePrune(ctx, keepSet))

	_, err = os.Stat(prunePruning)
	require.True(t, os.IsNotExist(err), "quarantined .pruning must be deleted in run 3")
	_, err = os.Stat(prunePath)
	require.True(t, os.IsNotExist(err), "chunk data must be fully gone after run 3")

	// Keep chunk still intact.
	_, err = s.GetChunk(keepID)
	require.NoError(t, err, "keep chunk must survive run 3")
}

// TestLocalStoreSafePruneKeepSetRemovesMarker verifies that a chunk entering the
// keep-set between run 1 and run 2 has its .prunable marker removed by run 2's
// phase 2, preventing it from being quarantined.
func TestLocalStoreSafePruneKeepSetRemovesMarker(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("reprieved chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	prunable, pruning := s.pruningPathsFromID(id)
	_, cacnk := s.nameFromID(id)

	// Run 1 with empty keep-set: chunk gets marked.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	_, err = os.Stat(prunable)
	require.NoError(t, err, "chunk must be marked after run 1")

	// Run 2 with the chunk now in the keep-set: marker is removed, no quarantine.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{id: {}}))

	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be cleared when chunk enters keep-set")
	_, err = os.Stat(pruning)
	require.True(t, os.IsNotExist(err), "chunk must not be quarantined when it enters keep-set")
	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must survive when chunk enters keep-set")
}

// TestLocalStoreUntagPrunable verifies that UntagPrunable removes a .prunable
// marker and resets the "seen" counter so the next SafePrune run re-marks rather
// than quarantines.
func TestLocalStoreUntagPrunable(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("untag me"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	prunable, pruning := s.pruningPathsFromID(id)

	// UntagPrunable on a chunk with no marker is a no-op.
	require.NoError(t, s.UntagPrunable(id))

	// Run 1 creates the .prunable marker.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	_, err = os.Stat(prunable)
	require.NoError(t, err, ".prunable must exist after run 1")

	// UntagPrunable removes it.
	require.NoError(t, s.UntagPrunable(id))
	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be gone after UntagPrunable")

	// Run 2 (counter reset): chunk is re-marked, not quarantined.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	_, err = os.Stat(prunable)
	require.NoError(t, err, "chunk must be re-marked after counter reset")
	_, err = os.Stat(pruning)
	require.True(t, os.IsNotExist(err), "chunk must not be quarantined when counter was reset")
}

// TestLocalStoreRescueChunksQuarantined verifies that RescueChunks renames a
// quarantined .pruning file back to .cacnk.
func TestLocalStoreRescueChunksQuarantined(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("rescue me from quarantine"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	_, cacnk := s.nameFromID(id)
	_, pruning := s.pruningPathsFromID(id)

	// Two runs with empty keep-set to quarantine the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))

	_, err = os.Stat(cacnk)
	require.True(t, os.IsNotExist(err), ".cacnk must be gone (quarantined)")
	_, err = os.Stat(pruning)
	require.NoError(t, err, ".pruning must exist")

	// RescueChunks restores the chunk.
	require.NoError(t, s.RescueChunks(ctx, map[ChunkID]struct{}{id: {}}))

	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must be restored by RescueChunks")
	_, err = os.Stat(pruning)
	require.True(t, os.IsNotExist(err), ".pruning must be gone after rescue")

	_, err = s.GetChunk(id)
	require.NoError(t, err, "rescued chunk must be readable")
}

// TestLocalStoreRescueChunksPrunable verifies that RescueChunks removes a
// .prunable marker from a chunk that is still present at .cacnk.
func TestLocalStoreRescueChunksPrunable(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("clear my marker"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	_, cacnk := s.nameFromID(id)
	prunable, _ := s.pruningPathsFromID(id)

	// One run to mark the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	_, err = os.Stat(prunable)
	require.NoError(t, err, ".prunable must exist")

	// RescueChunks clears the marker and preserves .cacnk.
	require.NoError(t, s.RescueChunks(ctx, map[ChunkID]struct{}{id: {}}))

	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be cleared by RescueChunks")
	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must still exist")
}

// TestLocalStoreSafePruneUncompressed runs the full three-run cycle in
// uncompressed (no-extension) mode to ensure the filename logic is correct.
func TestLocalStoreSafePruneUncompressed(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{Uncompressed: true})
	require.NoError(t, err)

	chunk := NewChunk([]byte("uncompressed prune test"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	_, cacnk := s.nameFromID(id)
	prunable, pruning := s.pruningPathsFromID(id)

	empty := map[ChunkID]struct{}{}

	require.NoError(t, s.SafePrune(ctx, empty))
	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must exist after run 1 (uncompressed)")
	_, err = os.Stat(prunable)
	require.NoError(t, err, ".prunable must exist after run 1 (uncompressed)")

	require.NoError(t, s.SafePrune(ctx, empty))
	_, err = os.Stat(cacnk)
	require.True(t, os.IsNotExist(err), ".cacnk must be quarantined after run 2 (uncompressed)")
	_, err = os.Stat(pruning)
	require.NoError(t, err, ".pruning must exist after run 2 (uncompressed)")

	require.NoError(t, s.SafePrune(ctx, empty))
	_, err = os.Stat(pruning)
	require.True(t, os.IsNotExist(err), ".pruning must be deleted after run 3 (uncompressed)")
}

// TestLocalStoreSafePruneRaceRevert injects a concurrent UntagPrunable call via
// testPostQuarantine to simulate the TOCTOU race: SafePrune renames .cacnk →
// .pruning, then (before the re-check) a writer removes .prunable. SafePrune
// must detect that .prunable is gone and revert .pruning → .cacnk.
func TestLocalStoreSafePruneRaceRevert(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("race revert chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	_, cacnk := s.nameFromID(id)
	prunable, pruning := s.pruningPathsFromID(id)

	// Run 1: mark the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	_, err = os.Stat(prunable)
	require.NoError(t, err, ".prunable must exist after run 1")

	// Run 2 with injected race: after SafePrune renames .cacnk → .pruning the
	// callback removes .prunable, simulating a writer calling UntagPrunable and
	// committing its index in the gap. SafePrune must revert the quarantine.
	s.testPostQuarantine = func(chunkID ChunkID) { s.UntagPrunable(chunkID) }
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))

	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must be restored after race revert")
	_, err = os.Stat(pruning)
	require.True(t, os.IsNotExist(err), ".pruning must be gone after race revert")
	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be gone (writer removed it)")
}
