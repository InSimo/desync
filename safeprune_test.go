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
	afterAdd     time.Duration // max random sleep after add/restore ops
	beforeWrite  time.Duration // max random sleep before write/add ops
	beforeDelete time.Duration // max random sleep before delete/quarantine ops
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

// chunkPool is a bounded, thread-safe pool of chunks shared across writer
// goroutines. It enables reuse of chunks across index iterations.
type chunkPool struct {
	mu     sync.Mutex
	chunks []*Chunk
	cap    int
}

func newChunkPool(cap int) *chunkPool {
	return &chunkPool{cap: cap, chunks: make([]*Chunk, 0, cap)}
}

// add inserts c into the pool. When the pool is full, a random slot is
// replaced so the pool stays fresh and older chunks cycle out over time.
func (p *chunkPool) add(c *Chunk) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.chunks) < p.cap {
		p.chunks = append(p.chunks, c)
	} else {
		p.chunks[rand.Intn(p.cap)] = c
	}
}

// pick returns a random chunk from the pool, or nil if the pool is empty.
func (p *chunkPool) pick() *Chunk {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.chunks) == 0 {
		return nil
	}
	return p.chunks[rand.Intn(len(p.chunks))]
}

// ---------------------------------------------------------------------------
// mockStore — in-memory SafePruneStore for algorithm verification
// ---------------------------------------------------------------------------

// mockStore is an in-memory implementation of SafePruneStore that is used to
// verify commonSafePrune and commonRescueChunks without touching the filesystem
// or any network backend.
type mockStore struct {
	mu         sync.Mutex
	chunks     map[ChunkID]*Chunk   // live chunks (Normal or Prunable state)
	quarantine map[ChunkID]*Chunk   // quarantined chunks (Pruning state)
	markers    map[ChunkID]struct{} // .prunable markers

	// postQuarantineHook, if non-nil, is called inside Quarantine() after the
	// chunk has been moved to quarantine and before returning. It is used by
	// tests to inject concurrent operations into the TOCTOU race window.
	postQuarantineHook func(id ChunkID)
	delays             *propagationDelays // optional propagation delay simulation
}

func newMockStore() *mockStore {
	return &mockStore{
		chunks:     make(map[ChunkID]*Chunk),
		quarantine: make(map[ChunkID]*Chunk),
		markers:    make(map[ChunkID]struct{}),
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

func (s *mockStore) Close() error  { return nil }
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

// SafePruneStore — high-level operations (delegate to common functions)

func (s *mockStore) SafePrune(ctx context.Context, ids map[ChunkID]struct{}) error {
	return commonSafePrune(ctx, ids, s)
}

func (s *mockStore) RescueChunks(ctx context.Context, ids map[ChunkID]struct{}) error {
	return commonRescueChunks(ctx, ids, s)
}

func (s *mockStore) UntagPrunable(id ChunkID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.markers, id)
	return nil
}

// SafePruneStore — primitive operations

func (s *mockStore) ListChunks(_ context.Context) ([]ChunkEntry, error) {
	s.mu.Lock()
	var entries []ChunkEntry
	for id, chunk := range s.chunks {
		_ = chunk
		if _, marked := s.markers[id]; marked {
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPrunable})
		} else {
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusNormal})
		}
	}
	for id := range s.quarantine {
		entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPruning})
	}
	for id := range s.markers {
		_, inChunks := s.chunks[id]
		_, inQuarantine := s.quarantine[id]
		if !inChunks && !inQuarantine {
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedPrunable})
		}
	}
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	return entries, nil
}

func (s *mockStore) MarkerExists(id ChunkID) (bool, error) {
	s.mu.Lock()
	_, ok := s.markers[id]
	s.mu.Unlock()
	s.delays.sleepAfterRead()
	return ok, nil
}

func (s *mockStore) DeletePruning(id ChunkID) error {
	s.delays.sleepBeforeDelete()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.quarantine, id)
	return nil
}

func (s *mockStore) DeleteMarker(id ChunkID) error {
	s.delays.sleepBeforeDelete()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.markers, id)
	return nil
}

func (s *mockStore) CreateMarker(id ChunkID) error {
	s.delays.sleepBeforeWrite()
	s.mu.Lock()
	s.markers[id] = struct{}{}
	s.mu.Unlock()
	s.delays.sleepAfterAdd()
	return nil
}

func (s *mockStore) Quarantine(id ChunkID) error {
	s.delays.sleepBeforeDelete()
	s.mu.Lock()
	chunk, ok := s.chunks[id]
	if ok {
		s.quarantine[id] = chunk
		delete(s.chunks, id)
	}
	hook := s.postQuarantineHook
	s.mu.Unlock()
	// Call hook with lock released so the hook can acquire the lock itself.
	if hook != nil {
		hook(id)
	}
	return nil
}

func (s *mockStore) Restore(id ChunkID) error {
	s.delays.sleepBeforeWrite()
	s.mu.Lock()
	chunk, ok := s.quarantine[id]
	if ok {
		s.chunks[id] = chunk
		delete(s.quarantine, id)
	}
	s.mu.Unlock()
	s.delays.sleepAfterAdd()
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

func (s *mockStore) assertQuarantined(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.quarantine[id]
	require.True(t, ok, "expected chunk %s to be quarantined", id)
}

func (s *mockStore) assertGone(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, inChunks := s.chunks[id]
	_, inQuarantine := s.quarantine[id]
	require.False(t, inChunks || inQuarantine, "expected chunk %s to be gone", id)
}

func (s *mockStore) assertMarked(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.markers[id]
	require.True(t, ok, "expected chunk %s to have a prunable marker", id)
}

func (s *mockStore) assertUnmarked(t *testing.T, id ChunkID) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.markers[id]
	require.False(t, ok, "expected chunk %s to have no prunable marker", id)
}

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

// ---------------------------------------------------------------------------
// Mock-store tests — verify commonSafePrune / commonRescueChunks state machine
// ---------------------------------------------------------------------------

// TestMockSafePruneFullCycle verifies the three-run state machine using the
// in-memory mock: mark → quarantine → delete.
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
	require.NoError(t, s.SafePrune(ctx, keepSet))
	s.assertLive(t, keepID)
	s.assertUnmarked(t, keepID)
	s.assertLive(t, pruneID)
	s.assertMarked(t, pruneID)

	// Marked chunk is still readable.
	_, err := s.GetChunk(pruneID)
	require.NoError(t, err, "marked chunk must still be readable")

	// Run 2: marked chunk is quarantined.
	require.NoError(t, s.SafePrune(ctx, keepSet))
	s.assertLive(t, keepID)
	s.assertQuarantined(t, pruneID)
	s.assertUnmarked(t, pruneID)

	// Quarantined chunk is invisible to readers.
	_, err = s.GetChunk(pruneID)
	require.Error(t, err)
	_, isMissing := err.(ChunkMissing)
	require.True(t, isMissing, "quarantined chunk must appear missing")

	// Run 3: quarantined chunk is deleted.
	require.NoError(t, s.SafePrune(ctx, keepSet))
	s.assertLive(t, keepID)
	s.assertGone(t, pruneID)
}

// TestMockSafePruneKeepSetRemovesMarker verifies that a chunk entering the
// keep-set between run 1 and run 2 has its marker removed and is not quarantined.
func TestMockSafePruneKeepSetRemovesMarker(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("reprieved mock chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// Run 1 with empty keep-set: chunk gets marked.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertMarked(t, id)

	// Run 2 with the chunk now in the keep-set: marker removed, no quarantine.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{id: {}}))
	s.assertLive(t, id)
	s.assertUnmarked(t, id)
}

// TestMockSafePruneUntagPrunable verifies that UntagPrunable resets the marker
// so the next SafePrune run re-marks instead of quarantining the chunk.
func TestMockSafePruneUntagPrunable(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("mock untag me"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// UntagPrunable on a chunk with no marker is a no-op.
	require.NoError(t, s.UntagPrunable(id))
	s.assertUnmarked(t, id)

	// Run 1 creates the marker.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertMarked(t, id)

	// UntagPrunable removes it.
	require.NoError(t, s.UntagPrunable(id))
	s.assertUnmarked(t, id)

	// Run 2: chunk is re-marked, not quarantined.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertLive(t, id)
	s.assertMarked(t, id)
}

// TestMockSafePruneRescueChunksQuarantined verifies that RescueChunks restores
// a quarantined chunk back to the live state.
func TestMockSafePruneRescueChunksQuarantined(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("mock rescue from quarantine"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// Two runs to quarantine the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertQuarantined(t, id)

	// RescueChunks restores the chunk.
	require.NoError(t, s.RescueChunks(ctx, map[ChunkID]struct{}{id: {}}))
	s.assertLive(t, id)
	s.assertUnmarked(t, id)

	_, err := s.GetChunk(id)
	require.NoError(t, err, "rescued chunk must be readable")
}

// TestMockSafePruneRescueChunksPrunable verifies that RescueChunks removes the
// .prunable marker from a chunk that is still live (not yet quarantined).
func TestMockSafePruneRescueChunksPrunable(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("mock clear my marker"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// One run to mark the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertLive(t, id)
	s.assertMarked(t, id)

	// RescueChunks clears the marker, chunk stays live.
	require.NoError(t, s.RescueChunks(ctx, map[ChunkID]struct{}{id: {}}))
	s.assertLive(t, id)
	s.assertUnmarked(t, id)
}

// TestMockSafePruneOrphanedMarkerCleanup verifies that Phase 1 deletes
// ChunkStatusOrphanedPrunable entries (markers with no corresponding chunk).
func TestMockSafePruneOrphanedMarkerCleanup(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	// Directly inject an orphaned marker (no live chunk, no quarantine entry).
	id := NewChunk([]byte("orphaned mock marker")).ID()
	s.mu.Lock()
	s.markers[id] = struct{}{}
	s.mu.Unlock()

	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))

	s.assertGone(t, id)
	s.assertUnmarked(t, id)
}

// TestMockSafePruneRaceRevert injects a concurrent UntagPrunable call via
// postQuarantineHook to simulate the TOCTOU race: SafePrune quarantines a
// chunk, then (before the re-check) a writer removes the marker. SafePrune
// must detect that the marker is gone and revert the quarantine.
func TestMockSafePruneRaceRevert(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("mock race revert chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// Run 1: mark the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertMarked(t, id)

	// Run 2: after Quarantine moves the chunk to quarantine, the hook removes
	// the marker (simulating a writer calling UntagPrunable in the race window).
	// SafePrune must revert the quarantine.
	s.postQuarantineHook = func(cid ChunkID) {
		_ = s.UntagPrunable(cid)
	}
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))

	s.assertLive(t, id)
	s.assertUnmarked(t, id)
}

// TestMockSafePruneRaceConcurrentWriteRescue simulates a writer calling
// RescueChunks in the Quarantine hook window. The writer rescues the chunk;
// SafePrune's subsequent TOCTOU revert is a no-op since the chunk is already
// back in the live map.
func TestMockSafePruneRaceConcurrentWriteRescue(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	chunk := NewChunk([]byte("mock concurrent rescue chunk"))
	require.NoError(t, s.StoreChunk(chunk))
	id := chunk.ID()

	// Run 1: mark the chunk.
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))
	s.assertMarked(t, id)

	// Run 2: hook calls RescueChunks synchronously (lock is released by
	// Quarantine before the hook runs, so no deadlock).
	s.postQuarantineHook = func(cid ChunkID) {
		_ = s.RescueChunks(ctx, map[ChunkID]struct{}{cid: {}})
	}
	require.NoError(t, s.SafePrune(ctx, map[ChunkID]struct{}{}))

	// The writer won: chunk is live and unmarked.
	s.assertLive(t, id)
	s.assertUnmarked(t, id)
}

// TestMockSafePruneContextCancellationPhase1 verifies that a cancelled context
// causes commonSafePrune to return Interrupted{} during Phase 1 processing.
func TestMockSafePruneContextCancellationPhase1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := newMockStore()

	// Inject a chunk directly into quarantine so Phase 1 has an entry to process.
	chunk := NewChunk([]byte("phase1 cancel chunk"))
	id := chunk.ID()
	s.mu.Lock()
	s.quarantine[id] = chunk
	s.mu.Unlock()

	cancel() // cancel before SafePrune
	err := s.SafePrune(ctx, map[ChunkID]struct{}{})
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

	cancel() // cancel before SafePrune; Phase 1 list is empty, Phase 2 hits the chunk
	err := s.SafePrune(ctx, map[ChunkID]struct{}{})
	_, ok := err.(Interrupted)
	require.True(t, ok, "expected Interrupted from Phase 2, got: %v", err)
}

// ---------------------------------------------------------------------------
// mockIndexStore — in-memory index store for stress testing
// ---------------------------------------------------------------------------

// stressChunkStore is the minimal chunk storage interface required by
// runSafePruneStress and mockIndexStore.CheckInvariant.
type stressChunkStore interface {
	StoreChunk(*Chunk) error
	HasChunk(ChunkID) (bool, error)
}

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
// present (and not quarantined) in cs. It holds the read lock for the entire
// call so the index snapshot is consistent — concurrent StoreIndex/DeleteIndex
// calls block only for the duration of the check.
func (s *mockIndexStore) CheckInvariant(cs stressChunkStore) error {
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

// TestMockSafePruneMultipleChunkStates exercises all four ChunkStatus values
// simultaneously and verifies each chunk ends up in the correct state after a
// single SafePrune call.
func TestMockSafePruneMultipleChunkStates(t *testing.T) {
	ctx := context.Background()
	s := newMockStore()

	// Normal chunk → will become Prunable after this run.
	normal := NewChunk([]byte("mock normal chunk"))
	require.NoError(t, s.StoreChunk(normal))

	// Prunable chunk → will be quarantined after this run.
	prunable := NewChunk([]byte("mock prunable chunk"))
	require.NoError(t, s.StoreChunk(prunable))
	s.mu.Lock()
	s.markers[prunable.ID()] = struct{}{}
	s.mu.Unlock()

	// Pruning chunk → will be deleted in Phase 1.
	pruning := NewChunk([]byte("mock pruning chunk"))
	s.mu.Lock()
	s.quarantine[pruning.ID()] = pruning
	s.mu.Unlock()

	// OrphanedPrunable → marker deleted in Phase 1, no chunk data.
	orphanedChunk := NewChunk([]byte("mock orphaned marker"))
	orphanedID := orphanedChunk.ID()
	s.mu.Lock()
	s.markers[orphanedID] = struct{}{}
	s.mu.Unlock()

	keepSet := map[ChunkID]struct{}{} // keep nothing

	require.NoError(t, s.SafePrune(ctx, keepSet))

	// Normal → Prunable (marked, still live).
	s.assertLive(t, normal.ID())
	s.assertMarked(t, normal.ID())

	// Prunable → Pruning (quarantined, marker removed).
	s.assertQuarantined(t, prunable.ID())
	s.assertUnmarked(t, prunable.ID())

	// Pruning → gone (deleted in Phase 1).
	s.assertGone(t, pruning.ID())

	// OrphanedPrunable → marker gone, no data.
	s.assertGone(t, orphanedID)
	s.assertUnmarked(t, orphanedID)
}

// noopRescue is a RescueChunks substitute that does nothing. It is used by
// the unsafe stress test where there is no quarantine state to rescue from.
func noopRescue(_ context.Context, _ map[ChunkID]struct{}) error { return nil }

// runSafePruneStress is the shared stress harness. It runs stressNumWriters
// writer goroutines and one pruner goroutine concurrently for stressDuration,
// then performs a final end-state invariant check.
//
// prune is called by the pruner goroutine with the live keep set.
// rescue is called by each writer after committing its index.
// prunerSleep is the delay inserted between consecutive prune calls. Both
// safe and unsafe tests use time.Millisecond so that the conditions are
// identical and any invariant violation reflects the pruning strategy, not
// pruning frequency.
// delay, if non-nil, injects random propagation delays around index-store
// operations to simulate distributed-system eventual consistency.
//
// Returns nil if the invariant held throughout, or the first violation found.
func runSafePruneStress(
	t *testing.T,
	cs stressChunkStore,
	prune func(context.Context, map[ChunkID]struct{}) error,
	rescue func(context.Context, map[ChunkID]struct{}) error,
	prunerSleep time.Duration,
	delay *propagationDelays,
) error {
	t.Helper()
	const (
		stressNumWriters   = 6
		stressChunksPerIdx = 3  // chunks per index, exercises multi-chunk keep sets
		stressDuration     = 3 * time.Second
		stressChunkPoolCap = 30 // shared pool cap; ~5 chunks per writer
		stressNumReaders   = 3
	)

	sharedIndexStore := newMockIndexStore()
	pool := newChunkPool(stressChunkPoolCap)

	ctx, cancel := context.WithTimeout(context.Background(), stressDuration)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	// Pruner goroutine: repeatedly calls prune with the current keep set.
	// prunerSleep between iterations gives writers time to complete StoreIndex
	// + rescue before the pruner can advance a chunk through all three phases
	// (mark → quarantine → delete) in rapid succession.
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

				// 1. Build stressChunksPerIdx chunks, alternating between fresh
				// chunks (even slots) and pool chunks (odd slots). Pool reuse
				// exercises the scenario where a chunk that has been through one
				// or more prune cycles is re-referenced by a new index.
				chunkIDs := make(map[ChunkID]struct{}, stressChunksPerIdx)
				idxChunks := make([]IndexChunk, 0, stressChunksPerIdx)
				offset := uint64(0)
				for j := 0; j < stressChunksPerIdx; j++ {
					var chunk *Chunk

					// Odd slots: try to reuse a chunk that may have been through prune cycles.
					if j%2 == 1 {
						if reused := pool.pick(); reused != nil {
							chunk = reused
							// Re-store: ensures the chunk is in the live map even if it was
							// quarantined between its last reference and now.
							if err := cs.StoreChunk(chunk); err != nil {
								return fmt.Errorf("worker %d iter %d: StoreChunk (reuse): %w",
									workerID, iteration, err)
							}
						}
					}

					// Even slots (or fallback when pool is empty): generate a fresh chunk.
					if chunk == nil {
						data := make([]byte, 32)
						if _, err := cryptorand.Read(data); err != nil {
							return fmt.Errorf("worker %d iter %d: rand.Read: %w", workerID, iteration, err)
						}
						chunk = NewChunk(data)
						if err := cs.StoreChunk(chunk); err != nil {
							return fmt.Errorf("worker %d iter %d: StoreChunk: %w", workerID, iteration, err)
						}
						pool.add(chunk)
					}

					chunkIDs[chunk.ID()] = struct{}{}
					idxChunks = append(idxChunks, IndexChunk{ID: chunk.ID(), Start: offset, Size: 32})
					offset += 32
				}

				// 2. Store index first (chunks enter the pruner's keep set on
				// the next AllChunkIDs call), then rescue any chunk that was
				// transiently quarantined between StoreChunk and StoreIndex.
				name := fmt.Sprintf("w%d-i%d", workerID, iteration)
				if delay != nil {
					delay.sleep(delay.beforeWrite)
				}
				sharedIndexStore.StoreIndex(name, Index{Chunks: idxChunks})
				if delay != nil {
					delay.sleep(delay.afterAdd)
				}
				if err := rescue(gCtx, chunkIDs); err != nil {
					if gCtx.Err() != nil {
						return nil
					}
					return fmt.Errorf("worker %d iter %d: rescue: %w", workerID, iteration, err)
				}

				// 3. Inline invariant check — verify only this worker's own chunks
				// to avoid observing other workers' transient StoreIndex→RescueChunks
				// windows. Readers provide continuous global coverage.
				for _, ic := range idxChunks {
					ok, err := cs.HasChunk(ic.ID)
					if err != nil {
						return fmt.Errorf("worker %d iter %d: HasChunk: %w", workerID, iteration, err)
					}
					if !ok {
						return fmt.Errorf("worker %d iter %d: own chunk %s missing after rescue",
							workerID, iteration, ic.ID)
					}
				}

				// 4. Expire the previous iteration's index so its chunks become
				// pruning candidates on the next prune run.
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
	// downloading each of its chunks, with propagation delays between operations.
	for range stressNumReaders {
		g.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return nil
				default:
				}

				// Atomically snapshot a random live index.
				name, idx, ok := sharedIndexStore.RandomIndex()
				if !ok {
					continue // no indexes yet; retry immediately
				}

				// Simulate propagation delay for the index fetch.
				if delay != nil {
					delay.sleep(delay.afterRead)
				}

				// Fetch each chunk referenced by the index.
				for _, c := range idx.Chunks {
					if delay != nil {
						delay.sleep(delay.afterRead)
					}
					if gCtx.Err() != nil {
						return nil
					}

					present, err := cs.HasChunk(c.ID)
					if err != nil {
						return fmt.Errorf("reader %q: HasChunk %s: %w", name, c.ID, err)
					}

					if !present {
						// A chunk can be transiently absent in the window between a
						// writer's StoreIndex and its RescueChunks completing. Wait
						// for that operation to propagate before concluding the chunk
						// is truly missing. The wait is bounded by the maximum
						// propagation time of a Restore call (beforeWrite + afterAdd).
						// A minimum of 1ms is always applied so the writer goroutine
						// has a chance to run RescueChunks even when delay is nil.
						wait := time.Millisecond
						if delay != nil && delay.beforeWrite+delay.afterAdd > wait {
							wait = delay.beforeWrite + delay.afterAdd
						}
						time.Sleep(wait)
						present, err = cs.HasChunk(c.ID)
						if err != nil {
							return fmt.Errorf("reader %q: HasChunk retry %s: %w", name, c.ID, err)
						}
						if gCtx.Err() != nil {
							return nil
						}

						// Only a protocol violation if the index is still live: a
						// missing chunk belonging to a deleted index is legitimate
						// (it has been pruned after the index was removed).
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
	// reference a missing or quarantined chunk.
	return sharedIndexStore.CheckInvariant(cs)
}

// TestMockSafePruneStressParallel verifies that the safe-prune protocol
// (SafePrune + RescueChunks) upholds the invariant under concurrent load.
//
// Run with -race to surface concurrent access bugs:
//
//	go test -race -run TestMockSafePruneStressParallel -v -count=1 .
func TestMockSafePruneStressParallel(t *testing.T) {
	cs := newMockStore()
	require.NoError(t, runSafePruneStress(t, cs, cs.SafePrune, cs.RescueChunks, time.Millisecond, nil))
}

// TestMockUnsafePruneStressParallel confirms that the stress harness detects
// invariant violations when an unsafe prune implementation is used. The unsafe
// Prune method (mirroring S3Store.Prune) immediately deletes any chunk not in
// the keep set, with no quarantine window and no rescue mechanism — so a chunk
// can be permanently deleted between StoreChunk and StoreIndex, violating the
// invariant. The test asserts that an error IS returned, proving the harness
// can distinguish safe from unsafe implementations.
func TestMockUnsafePruneStressParallel(t *testing.T) {
	cs := newMockStore()
	err := runSafePruneStress(t, cs, cs.Prune, noopRescue, time.Millisecond, nil)
	require.Error(t, err, "unsafe Prune must trigger an invariant violation")
}

// TestMockSafePruneStressWithPropagationDelay verifies that the safe-prune
// protocol (SafePrune + RescueChunks) upholds the invariant even when store
// operations have random propagation delays simulating distributed-system
// eventual consistency.
//
// Run with -race to surface concurrent access bugs:
//
//	go test -race -run TestMockSafePruneStressWithPropagationDelay -v -count=1 .
func TestMockSafePruneStressWithPropagationDelay(t *testing.T) {
	delay := &propagationDelays{
		afterRead:    2 * time.Millisecond,
		afterAdd:     2 * time.Millisecond,
		beforeWrite:  1 * time.Millisecond,
		beforeDelete: 1 * time.Millisecond,
	}
	cs := newMockStore()
	cs.delays = delay
	require.NoError(t, runSafePruneStress(t, cs, cs.SafePrune, cs.RescueChunks, time.Millisecond, delay))
}

// TestMockUnsafePruneStressWithPropagationDelay confirms that propagation
// delays make invariant violations more likely with an unsafe prune
// implementation, not less. The harness must still detect a violation.
func TestMockUnsafePruneStressWithPropagationDelay(t *testing.T) {
	delay := &propagationDelays{
		afterRead:    2 * time.Millisecond,
		afterAdd:     2 * time.Millisecond,
		beforeWrite:  1 * time.Millisecond,
		beforeDelete: 1 * time.Millisecond,
	}
	cs := newMockStore()
	cs.delays = delay
	err := runSafePruneStress(t, cs, cs.Prune, noopRescue, time.Millisecond, delay)
	require.Error(t, err, "unsafe Prune must trigger an invariant violation even with propagation delays")
}
