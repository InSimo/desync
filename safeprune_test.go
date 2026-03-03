package desync

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

// propagationDelays configures random propagation latency injected into store
// operations to simulate distributed-system eventual consistency. A nil value
// disables all delays.
type propagationDelays struct {
	afterRead    time.Duration // max random sleep after read ops
	afterAdd     time.Duration // max random sleep after add/protect ops
	beforeWrite  time.Duration // max random sleep before write/add ops
	beforeDelete time.Duration // max random sleep before delete ops
}

// sleep sleeps for a uniformly random duration in [0, max). It is a no-op if d
// is nil or max is zero.
func (d *propagationDelays) sleep(max time.Duration) {
	if d == nil || max <= 0 {
		return
	}
	time.Sleep(time.Duration(rand.Int63n(int64(max))))
}

func (d *propagationDelays) sleepAfterRead()    { if d != nil { d.sleep(d.afterRead) } }
func (d *propagationDelays) sleepAfterAdd()     { if d != nil { d.sleep(d.afterAdd) } }
func (d *propagationDelays) sleepBeforeWrite()  { if d != nil { d.sleep(d.beforeWrite) } }
func (d *propagationDelays) sleepBeforeDelete() { if d != nil { d.sleep(d.beforeDelete) } }

// ---------------------------------------------------------------------------
// mockStore — in-memory SafePruneStore for algorithm verification
// ---------------------------------------------------------------------------

// mockStore is an in-memory implementation of SafePruneStore that is used to
// verify commonSafePrune and SafePrunePreCommit without touching the filesystem
// or any network backend.
type mockStore struct {
	mu       sync.Mutex
	chunks   map[ChunkID]*Chunk   // live chunks
	markers  map[ChunkID]struct{} // .prunable markers
	protects map[ChunkID]struct{} // .protect markers

	// postHasProtectHook, if non-nil, is called inside HasProtect() after the
	// lock is released and the result captured. Used by tests to inject
	// concurrent operations into the TOCTOU race window.
	postHasProtectHook func(id ChunkID)
	delays             *propagationDelays // optional propagation delay simulation
}

func newMockStore() *mockStore {
	return &mockStore{
		chunks:   make(map[ChunkID]*Chunk),
		markers:  make(map[ChunkID]struct{}),
		protects: make(map[ChunkID]struct{}),
	}
}

// Store interface

func (s *mockStore) GetChunk(id ChunkID) (*Chunk, error) {
	s.mu.Lock()
	c, ok := s.chunks[id]
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	if !ok {
		return nil, ChunkMissing{id}
	}
	return c, nil
}

func (s *mockStore) HasChunk(id ChunkID) (bool, error) {
	s.mu.Lock()
	_, ok := s.chunks[id]
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	return ok, nil
}

func (s *mockStore) StoreChunk(chunk *Chunk) error {
	s.delays.sleepBeforeWrite()
	s.mu.Lock()
	s.chunks[chunk.ID()] = chunk
	s.mu.Unlock()
	s.delays.sleepAfterAdd()
	return nil
}

// GetRandomChunk returns a random live chunk (Normal or Prunable), or nil if
// none exists. Both Normal and Prunable chunks are eligible for reuse: the
// writer calls HasPrunable and adds a .protect marker if needed.
func (s *mockStore) GetRandomChunk() *Chunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	var candidates []*Chunk
	for _, c := range s.chunks {
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[rand.Intn(len(candidates))]
}

func (s *mockStore) Close() error   { return nil }
func (s *mockStore) String() string { return "mockStore" }

// PruneStore

func (s *mockStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.chunks {
		if _, keep := ids[id]; !keep {
			delete(s.chunks, id)
		}
	}
	return nil
}

// SafePruneStore — high-level operation (delegates to commonSafePrune)

func (s *mockStore) SafePrune(ctx context.Context, ids map[ChunkID]struct{}, finalizeOnly bool) error {
	return commonSafePrune(ctx, ids, s, finalizeOnly)
}

// SafePruneStore — primitive operations

func (s *mockStore) ListChunks(_ context.Context) ([]ChunkEntry, error) {
	s.mu.Lock()
	type presence struct{ hasChunk, hasPrunable, hasProtect bool }
	byID := make(map[ChunkID]*presence)
	for id := range s.chunks {
		if _, ok := byID[id]; !ok {
			byID[id] = &presence{}
		}
		byID[id].hasChunk = true
	}
	for id := range s.markers {
		if _, ok := byID[id]; !ok {
			byID[id] = &presence{}
		}
		byID[id].hasPrunable = true
	}
	for id := range s.protects {
		if _, ok := byID[id]; !ok {
			byID[id] = &presence{}
		}
		byID[id].hasProtect = true
	}
	var entries []ChunkEntry
	for id, p := range byID {
		switch {
		case p.hasChunk && p.hasPrunable && p.hasProtect:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusProtected})
		case p.hasChunk && p.hasPrunable:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPrunable})
		case p.hasChunk:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusNormal})
		case p.hasPrunable:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedPrunable})
		case p.hasProtect:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedProtect})
		}
	}
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	return entries, nil
}

func (s *mockStore) HasPrunable(id ChunkID) (bool, error) {
	s.mu.Lock()
	_, ok := s.markers[id]
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	return ok, nil
}

func (s *mockStore) DeletePrunable(id ChunkID) error {
	s.delays.sleepBeforeDelete()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.markers, id)
	return nil
}

func (s *mockStore) CreatePrunable(id ChunkID) error {
	s.delays.sleepBeforeWrite()
	s.mu.Lock()
	s.markers[id] = struct{}{}
	s.mu.Unlock()
	s.delays.sleepAfterAdd()
	return nil
}

func (s *mockStore) CreateProtect(id ChunkID) error {
	s.delays.sleepBeforeWrite()
	s.mu.Lock()
	s.protects[id] = struct{}{}
	s.mu.Unlock()
	s.delays.sleepAfterAdd()
	return nil
}

func (s *mockStore) DeleteProtect(id ChunkID) error {
	s.delays.sleepBeforeDelete()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.protects, id)
	return nil
}

func (s *mockStore) HasProtect(id ChunkID) (bool, error) {
	s.mu.Lock()
	_, ok := s.protects[id]
	hook := s.postHasProtectHook
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	if hook != nil {
		hook(id)
	}
	return ok, nil
}

func (s *mockStore) DeleteChunk(id ChunkID) error {
	s.delays.sleepBeforeDelete()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.chunks, id)
	return nil
}

// Helper assertions

func (s *mockStore) assertLive(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chunks[id]
	require.True(t, ok, "expected chunk %s to be live", id)
}

func (s *mockStore) assertGone(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chunks[id]
	require.False(t, ok, "expected chunk %s to be gone", id)
}

func (s *mockStore) assertMarked(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.markers[id]
	require.True(t, ok, "expected chunk %s to have a .prunable marker", id)
}

func (s *mockStore) assertUnmarked(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.markers[id]
	require.False(t, ok, "expected chunk %s to have no .prunable marker", id)
}

func (s *mockStore) assertProtected(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.protects[id]
	require.True(t, ok, "expected chunk %s to have a .protect marker", id)
}

func (s *mockStore) assertUnprotected(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.protects[id]
	require.False(t, ok, "expected chunk %s to have no .protect marker", id)
}

// ---------------------------------------------------------------------------
// LocalStore tests — verify the on-disk protect-marker protocol
// ---------------------------------------------------------------------------

// TestLocalStoreSafePruneFullCycle verifies the two-run state machine:
//
//	run 1: unreferenced chunk gains a .prunable marker (still readable)
//	run 2: marked chunk with no .protect is deleted
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
	prunePrunable := s.markerPathFromID(pruneID)
	keepPrunable := s.markerPathFromID(keepID)

	// --- Run 1: mark ---
	require.NoError(t, s.SafePrune(ctx, keepSet, false))

	_, err = os.Stat(keepPath)
	require.NoError(t, err, "keep chunk must survive run 1")
	_, err = os.Stat(keepPrunable)
	require.True(t, os.IsNotExist(err), "keep chunk must not gain a .prunable marker")

	_, err = os.Stat(prunePath)
	require.NoError(t, err, "prune chunk .cacnk must still exist after run 1")
	_, err = os.Stat(prunePrunable)
	require.NoError(t, err, "prune chunk must have a .prunable marker after run 1")

	// Marked chunk is still readable.
	_, err = s.GetChunk(pruneID)
	require.NoError(t, err, "marked chunk must still be readable")

	// --- Run 2: delete (no protect) ---
	require.NoError(t, s.SafePrune(ctx, keepSet, false))

	_, err = os.Stat(prunePath)
	require.True(t, os.IsNotExist(err), "prune chunk .cacnk must be gone after run 2")
	_, err = os.Stat(prunePrunable)
	require.True(t, os.IsNotExist(err), "prune chunk .prunable must be gone after run 2")

	// Deleted chunk is invisible to readers.
	_, err = s.GetChunk(pruneID)
	require.Error(t, err)
	_, ok := err.(ChunkMissing)
	require.True(t, ok, "deleted chunk must appear missing to readers")

	// Keep chunk still intact.
	_, err = s.GetChunk(keepID)
	require.NoError(t, err, "keep chunk must survive run 2")
}

// TestLocalStoreSafePruneKeepSetRemovesMarker verifies that a chunk entering
// the keep-set between run 1 and run 2 has its .prunable marker removed by
// run 2's Phase 2, preventing deletion.
func TestLocalStoreSafePruneKeepSetRemovesMarker(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("reprieved chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	prunable := s.markerPathFromID(id)
	_, cacnk := s.nameFromID(id)

	// Run 1 with empty keep-set: chunk gets marked.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))
	_, err = os.Stat(prunable)
	require.NoError(t, err, "chunk must be marked after run 1")

	// Run 2 with the chunk now in the keep-set: marker removed, no deletion.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{id: {}}, false))

	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be cleared when chunk enters keep-set")
	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must survive when chunk enters keep-set")
}

// TestLocalStoreSafePruneProtectKeepsChunk verifies that a .protect companion
// written by a writer prevents the pruner from deleting a marked chunk.
func TestLocalStoreSafePruneProtectKeepsChunk(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{})
	require.NoError(t, err)

	chunk := NewChunk([]byte("protect me"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	_, cacnk := s.nameFromID(id)
	prunable := s.markerPathFromID(id)
	protect := s.protectPathFromID(id)

	// Run 1: marks the chunk as prunable.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))
	_, err = os.Stat(prunable)
	require.NoError(t, err, ".prunable must exist after run 1")

	// Writer adds a protect marker (pre-commit protection).
	require.NoError(t, s.CreateProtect(id))
	_, err = os.Stat(protect)
	require.NoError(t, err, ".protect must exist after CreateProtect")

	// Run 2: HasProtect=true → keep chunk, clean markers.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))

	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must survive run 2 when protect is present")
	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be cleaned up by run 2")
	_, err = os.Stat(protect)
	require.True(t, os.IsNotExist(err), ".protect must be cleaned up by run 2")

	// Chunk is still readable.
	_, err = s.GetChunk(id)
	require.NoError(t, err, "protected chunk must still be readable after run 2")
}

// TestLocalStoreSafePruneUncompressed runs the two-run cycle in
// uncompressed mode to ensure the filename logic is correct.
func TestLocalStoreSafePruneUncompressed(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocalStore(t.TempDir(), StoreOptions{Uncompressed: true})
	require.NoError(t, err)

	chunk := NewChunk([]byte("uncompressed prune test"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	_, cacnk := s.nameFromID(id)
	prunable := s.markerPathFromID(id)

	empty := map[ChunkID]struct{}{}

	require.NoError(t, s.SafePrune(ctx, empty, false))
	_, err = os.Stat(cacnk)
	require.NoError(t, err, ".cacnk must exist after run 1 (uncompressed)")
	_, err = os.Stat(prunable)
	require.NoError(t, err, ".prunable must exist after run 1 (uncompressed)")

	require.NoError(t, s.SafePrune(ctx, empty, false))
	_, err = os.Stat(cacnk)
	require.True(t, os.IsNotExist(err), ".cacnk must be deleted after run 2 (uncompressed)")
	_, err = os.Stat(prunable)
	require.True(t, os.IsNotExist(err), ".prunable must be gone after run 2 (uncompressed)")
}

// ---------------------------------------------------------------------------
// Mock-store tests — verify commonSafePrune / SafePrunePreCommit state machine
// ---------------------------------------------------------------------------

// TestMockSafePruneFullCycle verifies the two-run state machine using the
// in-memory mock: mark → delete.
func TestMockSafePruneFullCycle(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	keep := NewChunk([]byte("mock keep chunk"))
	prune := NewChunk([]byte("mock prune chunk"))
	require.NoError(t, s.StoreChunk(keep))
	require.NoError(t, s.StoreChunk(prune))

	keepID := keep.ID()
	pruneID := prune.ID()
	keepSet := map[ChunkID]struct{}{keepID: {}}

	// Run 1: unreferenced chunk gains a marker.
	require.NoError(t, s.SafePrune(ctx, keepSet, false))
	s.assertLive(t, keepID)
	s.assertUnmarked(t, keepID)
	s.assertLive(t, pruneID)
	s.assertMarked(t, pruneID)

	// Marked chunk is still readable.
	_, err := s.GetChunk(pruneID)
	require.NoError(t, err, "marked chunk must still be readable")

	// Run 2: marked chunk with no protect is deleted.
	require.NoError(t, s.SafePrune(ctx, keepSet, false))
	s.assertLive(t, keepID)
	s.assertGone(t, pruneID)
	s.assertUnmarked(t, pruneID)

	// Deleted chunk is invisible to readers.
	_, err = s.GetChunk(pruneID)
	require.Error(t, err)
	_, isMissing := err.(ChunkMissing)
	require.True(t, isMissing, "deleted chunk must appear missing")
}

// TestMockSafePruneKeepSetRemovesMarker verifies that a chunk entering the
// keep-set between run 1 and run 2 has its marker removed and is not deleted.
func TestMockSafePruneKeepSetRemovesMarker(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("reprieved mock chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// Run 1 with empty keep-set: chunk gets marked.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))
	s.assertMarked(t, id)

	// Run 2 with the chunk now in the keep-set: marker removed, no deletion.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{id: {}}, false))
	s.assertLive(t, id)
	s.assertUnmarked(t, id)
}

// TestMockSafePruneProtectKeepsChunk verifies that a .protect companion
// written by a writer prevents deletion of a marked chunk.
func TestMockSafePruneProtectKeepsChunk(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("mock protect me"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// Run 1: marks the chunk as prunable.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))
	s.assertLive(t, id)
	s.assertMarked(t, id)

	// Writer adds a protect marker.
	require.NoError(t, s.CreateProtect(id))
	s.assertProtected(t, id)

	// Run 2: HasProtect=true → keep chunk, clean markers.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))
	s.assertLive(t, id)
	s.assertUnmarked(t, id)
	s.assertUnprotected(t, id)

	// Chunk is still readable.
	_, err := s.GetChunk(id)
	require.NoError(t, err, "protected chunk must survive run 2")
}

// TestMockSafePruneOrphanedMarkersCleanup verifies that Phase 1 deletes both
// ChunkStatusOrphanedPrunable and ChunkStatusOrphanedProtect entries.
func TestMockSafePruneOrphanedMarkersCleanup(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	// Inject an orphaned .prunable marker (no live chunk).
	orphanPrunableID := NewChunk([]byte("orphaned prunable marker")).ID()
	s.mu.Lock()
	s.markers[orphanPrunableID] = struct{}{}
	s.mu.Unlock()

	// Inject an orphaned .protect marker (no live chunk, no .prunable).
	orphanProtectID := NewChunk([]byte("orphaned protect marker")).ID()
	s.mu.Lock()
	s.protects[orphanProtectID] = struct{}{}
	s.mu.Unlock()

	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}, false))

	s.assertGone(t, orphanPrunableID)
	s.assertUnmarked(t, orphanPrunableID)
	s.assertGone(t, orphanProtectID)
	s.assertUnprotected(t, orphanProtectID)
}

// TestMockSafePruneProtectRaceNoSee tests the core TOCTOU scenario (Race 1):
// a writer adds a .protect marker, but a stale-reading pruner instance that
// checked HasProtect=false before the marker propagated proceeds to delete the
// chunk. The writer's HasChunk recheck must detect the deletion and re-upload.
func TestMockSafePruneProtectRaceNoSee(t *testing.T) {
	s := newMockStore()

	// Chunk C with .prunable marker (pruner marked it for potential deletion).
	chunk := NewChunk([]byte("race noSee chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()
	s.mu.Lock()
	s.markers[id] = struct{}{}
	s.mu.Unlock()

	// Step 1: writer detects HasPrunable=true and adds protect.
	marked, err := s.HasPrunable(id)
	require.NoError(t, err)
	require.True(t, marked)
	require.NoError(t, s.CreateProtect(id))

	// Step 2: simulate Race 1 — a stale-reading pruner (which computed
	// HasProtect=false before the protect propagated) now deletes the chunk.
	// We model this as a direct deletion without going through SafePrune,
	// since the real race involves a pruner replica with a stale view.
	s.mu.Lock()
	delete(s.chunks, id)  // stale pruner deletes chunk data
	delete(s.markers, id) // stale pruner deletes .prunable
	delete(s.protects, id)
	s.mu.Unlock()

	// Step 3: writer's recheck after 2P wait detects the deletion.
	present, err := s.HasChunk(id)
	require.NoError(t, err)
	require.False(t, present, "chunk must be missing after stale-pruner deletion")

	// Step 4: writer re-uploads the chunk and re-adds protect.
	require.NoError(t, s.StoreChunk(chunk))
	require.NoError(t, s.CreateProtect(id))

	// Step 5: after another 2P wait, the chunk is present and protected.
	present, err = s.HasChunk(id)
	require.NoError(t, err)
	require.True(t, present, "re-uploaded chunk must be present")
	s.assertProtected(t, id)
}

// TestMockSafePrunePreCommitMissingChunk verifies that SafePrunePreCommit
// re-uploads a chunk that was deleted by a pruner during the write window,
// rather than returning an error.
func TestMockSafePrunePreCommitMissingChunk(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("missing after prune"))
	id := chunk.ID()

	// Simulate what ChunkStorage.StoreChunk would do for a prunable chunk:
	// the chunk data was captured and CreateProtect was called, but then the
	// pruner won the race and deleted the chunk before SafePrunePreCommit ran.
	// lastProtect records when CreateProtect was called.
	lastProtect := time.Now()
	s.mu.Lock()
	s.protects[id] = struct{}{} // CreateProtect already called by ChunkStorage
	s.mu.Unlock()
	// (chunk data is absent — pruner deleted it)

	// SafePrunePreCommit: wait 2P (0 here), HasChunk → false →
	// re-upload + CreateProtect (no error).
	err := SafePrunePreCommit(ctx, map[ChunkID]*Chunk{id: chunk}, lastProtect, s, 0)
	require.NoError(t, err, "SafePrunePreCommit must re-upload the missing chunk, not error")

	// The chunk must now be present and protected.
	s.assertLive(t, id)
	s.assertProtected(t, id)
}

// TestChunkStorageWithPruning verifies that ChunkStorage with safe pruning
// correctly handles prunable, fresh, and normal chunks: prunable chunks get
// .protect markers and are captured; fresh chunks are stored without capture;
// normal (non-prunable) existing chunks are skipped without capture.
func TestChunkStorageWithPruning(t *testing.T) {
	s := newMockStore()

	// Set up a prunable chunk: present in the store with a .prunable marker.
	prunable := NewChunk([]byte("prunable reused chunk"))
	prunableID := prunable.ID()
	s.mu.Lock()
	s.chunks[prunableID] = prunable
	s.markers[prunableID] = struct{}{}
	s.mu.Unlock()

	cs := NewChunkStorageWithPruning(s, s)

	// Prunable chunk: StoreChunk detects it is present+prunable → .protect set, captured.
	err := cs.StoreChunk(prunable)
	require.NoError(t, err)
	s.assertProtected(t, prunableID)

	captured := cs.Chunks()
	_, found := captured[prunableID]
	require.True(t, found, "prunable reused chunk must be captured")
	require.False(t, cs.LastProtectTime().IsZero(), "LastProtectTime must be set after protecting a chunk")

	// Fresh chunk: absent from the store → stored, not captured.
	fresh := NewChunk([]byte("fresh chunk not in store"))
	freshID := fresh.ID()

	err = cs.StoreChunk(fresh)
	require.NoError(t, err)
	s.assertLive(t, freshID)

	captured = cs.Chunks()
	_, found = captured[freshID]
	require.False(t, found, "fresh chunk must not be captured")

	// Normal (non-prunable) existing chunk: present, no .prunable → skipped, not captured.
	normal := NewChunk([]byte("normal reused chunk"))
	normalID := normal.ID()
	s.mu.Lock()
	s.chunks[normalID] = normal
	s.mu.Unlock()

	err = cs.StoreChunk(normal)
	require.NoError(t, err)

	captured = cs.Chunks()
	_, found = captured[normalID]
	require.False(t, found, "normal chunk must not be captured")
	s.assertUnprotected(t, normalID)

	// Without pruning: existing chunk is not captured (sps is nil).
	csNoPrune := NewChunkStorage(s)
	existing := NewChunk([]byte("existing no-prune chunk"))
	existingID := existing.ID()
	s.mu.Lock()
	s.chunks[existingID] = existing
	s.markers[existingID] = struct{}{} // prunable, but sps is nil
	s.mu.Unlock()

	err = csNoPrune.StoreChunk(existing)
	require.NoError(t, err)
	require.Nil(t, csNoPrune.Chunks(), "Chunks() must return nil when safe pruning is disabled")
}

// TestMockSafePruneContextCancellationPhase1 verifies that a cancelled context
// causes commonSafePrune to return Interrupted{} during Phase 1 processing.
func TestMockSafePruneContextCancellationPhase1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := newMockStore()

	// Inject an orphaned .prunable marker so Phase 1 has an entry to process.
	id := NewChunk([]byte("phase1 cancel chunk")).ID()
	s.mu.Lock()
	s.markers[id] = struct{}{} // OrphanedPrunable (no chunk data)
	s.mu.Unlock()

	cancel() // cancel before SafePrune
	err := s.SafePrune(ctx, map[ChunkID]struct{}{}, false)
	_, ok := err.(Interrupted)
	require.True(t, ok, "expected Interrupted from Phase 1, got: %v", err)
}

// TestMockSafePruneContextCancellationPhase2 verifies that a cancelled context
// causes commonSafePrune to return Interrupted{} during Phase 2 processing.
func TestMockSafePruneContextCancellationPhase2(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := newMockStore()

	// A normal chunk gives Phase 2 something to iterate over.
	chunk := NewChunk([]byte("phase2 cancel chunk"))
	require.NoError(t, s.StoreChunk(chunk))

	cancel() // cancel before SafePrune; Phase 1 or Phase 2 will observe the cancellation
	err := s.SafePrune(ctx, map[ChunkID]struct{}{}, false)
	_, ok := err.(Interrupted)
	require.True(t, ok, "expected Interrupted, got: %v", err)
}

// TestMockSafePruneMultipleChunkStates exercises all five ChunkStatus values
// simultaneously and verifies each chunk ends up in the correct state after a
// single SafePrune call.
func TestMockSafePruneMultipleChunkStates(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	// Normal → will become Prunable after this run.
	normal := NewChunk([]byte("mock normal chunk"))
	require.NoError(t, s.StoreChunk(normal))

	// Prunable (no protect) → will be deleted.
	prunable := NewChunk([]byte("mock prunable chunk"))
	require.NoError(t, s.StoreChunk(prunable))
	s.mu.Lock()
	s.markers[prunable.ID()] = struct{}{}
	s.mu.Unlock()

	// Protected (chunk + .prunable + .protect) → will be kept (HasProtect=true).
	protected := NewChunk([]byte("mock protected chunk"))
	require.NoError(t, s.StoreChunk(protected))
	s.mu.Lock()
	s.markers[protected.ID()] = struct{}{}
	s.protects[protected.ID()] = struct{}{}
	s.mu.Unlock()

	// OrphanedPrunable → marker deleted in Phase 1.
	orphanedPrunableID := NewChunk([]byte("mock orphaned prunable")).ID()
	s.mu.Lock()
	s.markers[orphanedPrunableID] = struct{}{}
	s.mu.Unlock()

	// OrphanedProtect → protect deleted in Phase 1.
	orphanedProtectID := NewChunk([]byte("mock orphaned protect")).ID()
	s.mu.Lock()
	s.protects[orphanedProtectID] = struct{}{}
	s.mu.Unlock()

	keepSet := map[ChunkID]struct{}{} // keep nothing
	require.NoError(t, s.SafePrune(ctx, keepSet, false))

	// Normal → Prunable (marker added, still live).
	s.assertLive(t, normal.ID())
	s.assertMarked(t, normal.ID())

	// Prunable → deleted (no protect).
	s.assertGone(t, prunable.ID())
	s.assertUnmarked(t, prunable.ID())

	// Protected → kept (HasProtect=true → DeletePrunable+DeleteProtect, chunk survives).
	s.assertLive(t, protected.ID())
	s.assertUnmarked(t, protected.ID())
	s.assertUnprotected(t, protected.ID())

	// OrphanedPrunable → marker gone.
	s.assertGone(t, orphanedPrunableID)
	s.assertUnmarked(t, orphanedPrunableID)

	// OrphanedProtect → protect gone.
	s.assertGone(t, orphanedProtectID)
	s.assertUnprotected(t, orphanedProtectID)
}

// TestMockSafePruneFinalizeOnly verifies finalizeOnly=true behaviour:
// - A Normal chunk is left unmarked (not turned into Prunable).
// - A Prunable chunk with no protect is deleted (finalized).
// - A Prunable chunk with a protect is kept and its markers cleaned.
func TestMockSafePruneFinalizeOnly(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	// normalChunk: Normal state — should remain untouched by finalizeOnly pass.
	normal := NewChunk([]byte("finalize only normal chunk"))
	require.NoError(t, s.StoreChunk(normal))

	// prunableChunk: already marked — finalizeOnly pass should delete it.
	prunable := NewChunk([]byte("finalize only prunable chunk"))
	require.NoError(t, s.StoreChunk(prunable))
	require.NoError(t, s.CreatePrunable(prunable.ID()))
	s.assertMarked(t, prunable.ID())

	// protectedChunk: marked + protected — finalizeOnly pass should keep it.
	protected := NewChunk([]byte("finalize only protected chunk"))
	require.NoError(t, s.StoreChunk(protected))
	require.NoError(t, s.CreatePrunable(protected.ID()))
	require.NoError(t, s.CreateProtect(protected.ID()))
	s.assertMarked(t, protected.ID())
	s.assertProtected(t, protected.ID())

	// Run a finalizeOnly pass with an empty keep set (keep nothing new).
	empty := map[ChunkID]struct{}{}
	require.NoError(t, s.SafePrune(ctx, empty, true))

	// Normal chunk must remain live and unmarked — finalizeOnly does not mark new candidates.
	s.assertLive(t, normal.ID())
	s.assertUnmarked(t, normal.ID())

	// Prunable chunk must be deleted — it was already marked, no protect.
	s.assertGone(t, prunable.ID())
	s.assertUnmarked(t, prunable.ID())

	// Protected chunk must survive — HasProtect=true → keep, clean markers.
	s.assertLive(t, protected.ID())
	s.assertUnmarked(t, protected.ID())
	s.assertUnprotected(t, protected.ID())
}

// ---------------------------------------------------------------------------
// mockIndexStore — in-memory index store for stress testing
// ---------------------------------------------------------------------------

// hasChecker is the minimal interface required by mockIndexStore.CheckInvariant.
type hasChecker interface {
	HasChunk(ChunkID) (bool, error)
}

// mockChunker implements the ChunkerInterface for the stress test. It yields
// stressChunksPerIdx chunks per "file": odd slots reuse an existing chunk from
// the store (deduplication path); even slots produce fresh random data. After
// all chunks are emitted, Next returns an empty slice to signal EOF.
type mockChunker struct {
	store     *mockStore
	remaining int
	slot      int
	offset    uint64
}

func newMockChopper(store *mockStore, numChunks int) *mockChunker {
	return &mockChunker{store: store, remaining: numChunks}
}

func (m *mockChunker) Next() (uint64, []byte, error) {
	if m.remaining <= 0 {
		return m.offset, nil, nil // EOF
	}
	m.remaining--
	slot := m.slot
	m.slot++

	var data []byte
	if slot%2 == 1 {
		if reused := m.store.GetRandomChunk(); reused != nil {
			if d, err := reused.Data(); err == nil {
				// Copy to avoid sharing the backing array with the store's chunk.
				data = make([]byte, len(d))
				copy(data, d)
			}
		}
	}
	if data == nil {
		data = make([]byte, 32)
		cryptorand.Read(data)
	}

	start := m.offset
	m.offset += uint64(len(data))
	return start, data, nil
}

func (m *mockChunker) Min() uint64 { return 32 }
func (m *mockChunker) Avg() uint64 { return 32 }
func (m *mockChunker) Max() uint64 { return 32 }

// mockIndexStore is a thread-safe, in-memory index store used by the
// safe-prune stress harness. It tracks live indexes and supports computing
// the pruner's keep set and checking the invariant that all referenced chunks
// are present in the chunk store.
type mockIndexStore struct {
	mu      sync.RWMutex
	indexes map[string]Index // index name → Index
}

func newMockIndexStore() *mockIndexStore {
	return &mockIndexStore{
		indexes: make(map[string]Index),
	}
}

func (s *mockIndexStore) StoreIndex(name string, idx Index) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexes[name] = idx
}

func (s *mockIndexStore) DeleteIndex(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.indexes, name)
}

// AllChunkIDs returns the union of all chunk IDs referenced by all live indexes.
// Used by the pruner to compute its keep set.
func (s *mockIndexStore) AllChunkIDs() map[ChunkID]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make(map[ChunkID]struct{})
	for _, idx := range s.indexes {
		for _, c := range idx.Chunks {
			ids[c.ID] = struct{}{}
		}
	}
	return ids
}

// CheckInvariant verifies that every chunk referenced by every live index is
// present in cs.
func (s *mockIndexStore) CheckInvariant(cs hasChecker) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for name, idx := range s.indexes {
		for _, c := range idx.Chunks {
			ok, err := cs.HasChunk(c.ID)
			if err != nil {
				return fmt.Errorf("index %q chunk %s: HasChunk error: %w", name, c.ID, err)
			}
			if !ok {
				return fmt.Errorf("index %q chunk %s: missing from chunk store", name, c.ID)
			}
		}
	}
	return nil
}

// RandomIndex atomically captures a randomly selected live index. Returns
// ok=false if the store is currently empty.
func (s *mockIndexStore) RandomIndex() (name string, idx Index, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.indexes) == 0 {
		return "", Index{}, false
	}
	names := make([]string, 0, len(s.indexes))
	for n := range s.indexes {
		names = append(names, n)
	}
	name = names[rand.Intn(len(names))]
	return name, s.indexes[name], true
}

// HasIndex reports whether a named index is currently live.
func (s *mockIndexStore) HasIndex(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.indexes[name]
	return ok
}

// runSafePruneStress is the shared stress harness. It runs stressNumWriters
// writer goroutines and one pruner goroutine concurrently for stressDuration,
// then performs a final end-state invariant check.
//
// store is the underlying chunk/safe-prune store.
// prune is called by the pruner goroutine with the live keep set.
// sps, if non-nil, enables the safe-pruning writer protocol via
// NewChunkStorageWithPruning + SafePrunePreCommit; pass nil for unsafe tests.
// propagationTime is the maximum store write propagation latency; writers wait
// 2×propagationTime between adding protect markers and rechecking chunks.
// Use 0 for stores with immediate visibility (local/mock).
// prunerSleep is the delay between consecutive prune calls. For safe-prune
// tests this should be at least twice the expected writer iteration time to
// satisfy the design assumption (Race 2): the writer completes faster than
// two full pruner cycles for Normal (fresh) chunks.
// delay, if non-nil, injects random propagation delays around store operations.
//
// Returns nil if the invariant held throughout, or the first violation found.
func runSafePruneStress(
	t *testing.T,
	store *mockStore,
	prune func(context.Context, map[ChunkID]struct{}) error,
	sps SafePruneStore,
	propagationTime time.Duration,
	prunerSleep time.Duration,
	delay *propagationDelays,
) error {
	t.Helper()
	const (
		stressNumWriters   = 6
		stressChunksPerIdx = 3 // chunks per index, exercises multi-chunk keep sets
		stressDuration     = 3 * time.Second
		stressNumReaders   = 3
	)

	sharedIndexStore := newMockIndexStore()

	ctx, cancel := context.WithTimeout(context.Background(), stressDuration)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	// Pruner goroutine: repeatedly calls prune with the current keep set.
	g.Go(func() error {
		for {
			select {
			case <-gCtx.Done():
				return nil
			default:
			}
			keepSet := sharedIndexStore.AllChunkIDs()
			if delay != nil {
				delay.sleep(delay.afterRead)
			}
			if err := prune(gCtx, keepSet); err != nil {
				if gCtx.Err() != nil {
					return nil
				}
				return err
			}
			if prunerSleep > 0 {
				time.Sleep(prunerSleep)
			}
		}
	})

	// Writer goroutines: each continuously creates new unique chunks and indexes.
	for i := range stressNumWriters {
		workerID := i
		g.Go(func() error {
			for iteration := 0; ; iteration++ {
				select {
				case <-gCtx.Done():
					return nil
				default:
				}

				// Use the production ChunkStream pipeline: mockChunker feeds
				// chunks (mix of fresh + reused) into ChunkStream, which
				// internally handles ChunkStorage + SafePrunePreCommit.
				chopper := newMockChopper(store, stressChunksPerIdx)
				idx, err := ChunkStream(gCtx, chopper, store, 1, sps, propagationTime)
				if err != nil {
					if gCtx.Err() != nil {
						return nil
					}
					return fmt.Errorf("worker %d iter %d: ChunkStream: %w", workerID, iteration, err)
				}

				// Commit the index.
				name := fmt.Sprintf("w%d-i%d", workerID, iteration)
				if delay != nil {
					delay.sleep(delay.beforeWrite)
				}
				sharedIndexStore.StoreIndex(name, idx)
				if delay != nil {
					delay.sleep(delay.afterAdd)
				}

				// Inline invariant check — verify only this worker's own chunks
				// to avoid observing other workers' transient windows.
				for _, ic := range idx.Chunks {
					ok, err := store.HasChunk(ic.ID)
					if err != nil {
						return fmt.Errorf("worker %d iter %d: HasChunk: %w", workerID, iteration, err)
					}
					if !ok {
						return fmt.Errorf("worker %d iter %d: own chunk %s missing after commit",
							workerID, iteration, ic.ID)
					}
				}

				// Expire the previous iteration's index.
				if iteration > 0 {
					if delay != nil {
						delay.sleep(delay.beforeDelete)
					}
					sharedIndexStore.DeleteIndex(fmt.Sprintf("w%d-i%d", workerID, iteration-1))
				}
			}
		})
	}

	// Reader goroutines: simulate concurrent clients fetching an index and then
	// downloading each of its chunks.
	for range stressNumReaders {
		g.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return nil
				default:
				}

				name, idx, ok := sharedIndexStore.RandomIndex()
				if !ok {
					continue // no indexes yet; retry immediately
				}

				if delay != nil {
					delay.sleep(delay.afterRead)
				}

				for _, c := range idx.Chunks {
					if delay != nil {
						delay.sleep(delay.afterRead)
					}
					if gCtx.Err() != nil {
						return nil
					}

					present, err := store.HasChunk(c.ID)
					if err != nil {
						return fmt.Errorf("reader %q: HasChunk %s: %w", name, c.ID, err)
					}

					if !present {
						// A chunk can be transiently absent if a stale-reading pruner
						// deleted it while the writer's re-upload is in flight. Wait for
						// the re-upload + protect propagation before concluding the chunk
						// is truly missing.
						wait := time.Millisecond
						if delay != nil && delay.beforeWrite+delay.afterAdd > wait {
							wait = delay.beforeWrite + delay.afterAdd
						}
						time.Sleep(wait)
						present, err = store.HasChunk(c.ID)
						if err != nil {
							return fmt.Errorf("reader %q: HasChunk retry %s: %w", name, c.ID, err)
						}
						if gCtx.Err() != nil {
							return nil
						}

						// Only a protocol violation if the index is still live.
						if !present && sharedIndexStore.HasIndex(name) {
							return fmt.Errorf("reader: index %q chunk %s missing while index is live",
								name, c.ID)
						}
					}
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	// End-state check: after all goroutines have exited, no live index may
	// reference a missing chunk.
	return sharedIndexStore.CheckInvariant(store)
}

// TestMockSafePruneStressWithProtect verifies that the protect-marker
// safe-prune protocol upholds the invariant under concurrent load.
//
// Run with -race to surface concurrent access bugs:
//
//	go test -race -run TestMockSafePruneStressWithProtect -v -count=1 .
func TestMockSafePruneStressWithProtect(t *testing.T) {
	cs := newMockStore()
	// propagationTime=50ms: SafePrunePreCommit waits 100ms before rechecking
	// captured chunks, ensuring any in-flight pruner delete (Race 1 TOCTOU
	// between HasProtect and DeleteChunk) completes before the recheck.
	// prunerSleep=500ms: two pruner cycles take >1s, well beyond a writer
	// iteration (~100ms with the propagation wait), satisfying the Race 2 design
	// assumption that the writer completes faster than two pruner cycles.
	safePrune := func(ctx context.Context, ids map[ChunkID]struct{}) error {
		return cs.SafePrune(ctx, ids, false)
	}
	require.NoError(t, runSafePruneStress(t, cs, safePrune, cs, 50*time.Millisecond, 500*time.Millisecond, nil))
}

// TestMockUnsafePruneStress confirms that the stress harness detects invariant
// violations when an unsafe prune implementation is used. The unsafe Prune
// method immediately deletes any chunk not in the keep set — so a chunk can
// be permanently deleted between StoreChunk and the index commit, violating
// the invariant. The test asserts that an error IS returned, proving the
// harness can distinguish safe from unsafe implementations.
func TestMockUnsafePruneStress(t *testing.T) {
	cs := newMockStore()
	err := runSafePruneStress(t, cs, cs.Prune, nil, 0, time.Millisecond, nil)
	require.Error(t, err, "unsafe Prune must trigger an invariant violation")
}

// TestMockSafePruneStressWithProtectWithDelay verifies that the safe-prune
// protocol upholds the invariant even when store operations have random
// propagation delays simulating distributed-system eventual consistency.
//
// Run with -race to surface concurrent access bugs:
//
//	go test -race -run TestMockSafePruneStressWithProtectWithDelay -v -count=1 .
func TestMockSafePruneStressWithProtectWithDelay(t *testing.T) {
	delay := &propagationDelays{
		afterRead:    2 * time.Millisecond,
		afterAdd:     2 * time.Millisecond,
		beforeWrite:  1 * time.Millisecond,
		beforeDelete: 1 * time.Millisecond,
	}
	cs := newMockStore()
	cs.delays = delay
	// prunerSleep=500ms: with store operation delays plus -race overhead, a
	// writer iteration can take well over 100ms. Two pruner cycles must exceed
	// this to satisfy the Race 2 design assumption.
	safePrune := func(ctx context.Context, ids map[ChunkID]struct{}) error {
		return cs.SafePrune(ctx, ids, false)
	}
	require.NoError(t, runSafePruneStress(t, cs, safePrune, cs, 50*time.Millisecond, 500*time.Millisecond, delay))
}

// TestMockUnsafePruneStressWithDelay confirms that propagation delays make
// invariant violations more likely with an unsafe prune implementation.
func TestMockUnsafePruneStressWithDelay(t *testing.T) {
	delay := &propagationDelays{
		afterRead:    2 * time.Millisecond,
		afterAdd:     2 * time.Millisecond,
		beforeWrite:  1 * time.Millisecond,
		beforeDelete: 1 * time.Millisecond,
	}
	cs := newMockStore()
	cs.delays = delay
	err := runSafePruneStress(t, cs, cs.Prune, nil, 0, time.Millisecond, delay)
	require.Error(t, err, "unsafe Prune must trigger an invariant violation even with propagation delays")
}
