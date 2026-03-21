package desync

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// mockIndexPruneStore — in-memory SafePruneIndexStore for algorithm tests
// ---------------------------------------------------------------------------

// mockIndexPruneStore is a thread-safe, in-memory implementation of
// SafePruneIndexStore used to verify commonSafePruneIndexes without touching
// the filesystem or any network backend.
type mockIndexPruneStore struct {
	mu       sync.Mutex
	indexes  map[string]struct{} // live index names
	prunable map[string]struct{} // persisted prunable set
}

var _ SafePruneIndexStore = (*mockIndexPruneStore)(nil)

func newMockIndexPruneStore() *mockIndexPruneStore {
	return &mockIndexPruneStore{
		indexes:  make(map[string]struct{}),
		prunable: make(map[string]struct{}),
	}
}

// IndexStore — GetIndexReader and GetIndex are not exercised by the algorithm
// tests but are required to satisfy the interface.

func (s *mockIndexPruneStore) GetIndexReader(_ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}
func (s *mockIndexPruneStore) GetIndex(_ string) (Index, error) {
	return Index{}, fmt.Errorf("not implemented")
}
func (s *mockIndexPruneStore) HasIndex(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.indexes[name]
	return ok, nil
}
func (s *mockIndexPruneStore) Close() error   { return nil }
func (s *mockIndexPruneStore) String() string { return "mockIndexPruneStore" }

// ListableIndexStore

func (s *mockIndexPruneStore) ListIndexes(_ context.Context, _ string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.indexes))
	for name := range s.indexes {
		names = append(names, name)
	}
	return names, nil
}

// IndexPruneStore

func (s *mockIndexPruneStore) PruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonPruneIndexes(ctx, keep, s)
}

func (s *mockIndexPruneStore) DeleteIndexes(_ context.Context, names []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		delete(s.indexes, name)
	}
	return nil
}

// SafePruneIndexStore

func (s *mockIndexPruneStore) SafePruneIndexes(ctx context.Context, keep map[string]struct{}) error {
	return commonSafePruneIndexes(ctx, keep, s)
}

func (s *mockIndexPruneStore) ReadPrunableIndexSet(_ context.Context) (map[string]struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := make(map[string]struct{}, len(s.prunable))
	for name := range s.prunable {
		set[name] = struct{}{}
	}
	return set, nil
}

func (s *mockIndexPruneStore) WritePrunableIndexSet(_ context.Context, names []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prunable = make(map[string]struct{}, len(names))
	for _, name := range names {
		s.prunable[name] = struct{}{}
	}
	return nil
}

// storeIndex adds name to the live index set.
func (s *mockIndexPruneStore) storeIndex(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexes[name] = struct{}{}
}

func (s *mockIndexPruneStore) assertPresent(t *testing.T, name string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.indexes[name]
	require.True(t, ok, "expected index %q to be present in the store", name)
}

func (s *mockIndexPruneStore) assertAbsent(t *testing.T, name string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.indexes[name]
	require.False(t, ok, "expected index %q to be absent from the store", name)
}

func (s *mockIndexPruneStore) assertPrunable(t *testing.T, name string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.prunable[name]
	require.True(t, ok, "expected index %q to be in the prunable set", name)
}

func (s *mockIndexPruneStore) assertNotPrunable(t *testing.T, name string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.prunable[name]
	require.False(t, ok, "expected index %q to not be in the prunable set", name)
}

// ---------------------------------------------------------------------------
// Tests for commonSafePruneIndexes
// ---------------------------------------------------------------------------

// TestMockSafePruneIndexesFullCycle verifies the two-run deletion state machine:
// run 1 records candidates in the prunable set without deleting anything; run 2
// deletes the candidates that are still absent from the keep set.
func TestMockSafePruneIndexesFullCycle(t *testing.T) {
	ctx := context.Background()
	s := newMockIndexPruneStore()
	s.storeIndex("keep.caibx")
	s.storeIndex("prune-a.caibx")
	s.storeIndex("prune-b.caibx")

	keep := map[string]struct{}{"keep.caibx": {}}

	// Run 1: candidates are recorded in the prunable set but not deleted.
	require.NoError(t, s.SafePruneIndexes(ctx, keep))
	s.assertPresent(t, "keep.caibx")
	s.assertPresent(t, "prune-a.caibx")
	s.assertPresent(t, "prune-b.caibx")
	s.assertNotPrunable(t, "keep.caibx")
	s.assertPrunable(t, "prune-a.caibx")
	s.assertPrunable(t, "prune-b.caibx")

	// Run 2: candidates from run 1 that are still absent from keep are deleted.
	require.NoError(t, s.SafePruneIndexes(ctx, keep))
	s.assertPresent(t, "keep.caibx")
	s.assertAbsent(t, "prune-a.caibx")
	s.assertAbsent(t, "prune-b.caibx")
	s.assertNotPrunable(t, "prune-a.caibx")
	s.assertNotPrunable(t, "prune-b.caibx")
}

// TestMockSafePruneIndexesNewIndexProtected is the core race-protection test.
// It simulates an index written after the keep set was assembled but before
// ListIndexes runs: that index must survive the run that first observes it.
// Only an index that was already a candidate in the previous run can be deleted,
// so a brand-new index is always protected for at least one full run.
func TestMockSafePruneIndexesNewIndexProtected(t *testing.T) {
	ctx := context.Background()
	s := newMockIndexPruneStore()
	s.storeIndex("old.caibx")

	empty := map[string]struct{}{}

	// Run 1: old.caibx becomes a candidate.
	require.NoError(t, s.SafePruneIndexes(ctx, empty))
	s.assertPresent(t, "old.caibx")
	s.assertPrunable(t, "old.caibx")

	// A new index is written after run 1 — this simulates the race window
	// where StoreIndex is called after the keep set for run 2 was assembled.
	s.storeIndex("new.caibx")

	// Run 2: old.caibx is deleted (confirmed candidate from run 1); new.caibx
	// is a first sighting and goes into the prunable set but is NOT deleted.
	require.NoError(t, s.SafePruneIndexes(ctx, empty))
	s.assertAbsent(t, "old.caibx")
	s.assertPresent(t, "new.caibx")
	s.assertPrunable(t, "new.caibx")
}

// TestMockSafePruneIndexesKeepSetRemovesFromPrunable verifies that an index
// added to the keep set between run 1 and run 2 survives run 2 and is removed
// from the prunable set.
func TestMockSafePruneIndexesKeepSetRemovesFromPrunable(t *testing.T) {
	ctx := context.Background()
	s := newMockIndexPruneStore()
	s.storeIndex("reprieved.caibx")

	// Run 1 with empty keep set: index becomes a candidate.
	require.NoError(t, s.SafePruneIndexes(ctx, map[string]struct{}{}))
	s.assertPresent(t, "reprieved.caibx")
	s.assertPrunable(t, "reprieved.caibx")

	// Run 2 with the index now in the keep set: must survive, prunable entry cleared.
	require.NoError(t, s.SafePruneIndexes(ctx, map[string]struct{}{"reprieved.caibx": {}}))
	s.assertPresent(t, "reprieved.caibx")
	s.assertNotPrunable(t, "reprieved.caibx")
}

// TestMockSafePruneIndexesAllKept verifies that when every index is in the keep
// set the prunable set stays empty and nothing is ever deleted.
func TestMockSafePruneIndexesAllKept(t *testing.T) {
	ctx := context.Background()
	s := newMockIndexPruneStore()
	s.storeIndex("a.caibx")
	s.storeIndex("b.caibx")

	keep := map[string]struct{}{"a.caibx": {}, "b.caibx": {}}

	require.NoError(t, s.SafePruneIndexes(ctx, keep))
	s.assertPresent(t, "a.caibx")
	s.assertPresent(t, "b.caibx")
	s.assertNotPrunable(t, "a.caibx")
	s.assertNotPrunable(t, "b.caibx")

	require.NoError(t, s.SafePruneIndexes(ctx, keep))
	s.assertPresent(t, "a.caibx")
	s.assertPresent(t, "b.caibx")
}

// TestMockSafePruneIndexesEmptyStore verifies that multiple runs against an
// empty store are no-ops.
func TestMockSafePruneIndexesEmptyStore(t *testing.T) {
	ctx := context.Background()
	s := newMockIndexPruneStore()
	require.NoError(t, s.SafePruneIndexes(ctx, map[string]struct{}{}))
	require.NoError(t, s.SafePruneIndexes(ctx, map[string]struct{}{}))
}

// TestMockSafePruneIndexesContextCancellation verifies that a cancelled context
// causes commonSafePruneIndexes to return Interrupted{}.
func TestMockSafePruneIndexesContextCancellation(t *testing.T) {
	s := newMockIndexPruneStore()
	s.storeIndex("a.caibx")

	// Prime the prunable set so the second run has an entry to iterate over.
	require.NoError(t, s.SafePruneIndexes(context.Background(), map[string]struct{}{}))
	s.assertPrunable(t, "a.caibx")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.SafePruneIndexes(ctx, map[string]struct{}{})
	_, ok := err.(Interrupted)
	require.True(t, ok, "expected Interrupted when context is cancelled, got: %v", err)
}

// TestMockSafePruneIndexesGoneBetweenRuns verifies that an index removed from
// the store by external means between run 1 and run 2 is handled gracefully:
// it is absent from ListIndexes and is therefore simply dropped from the
// prunable set without any attempt to delete it again.
func TestMockSafePruneIndexesGoneBetweenRuns(t *testing.T) {
	ctx := context.Background()
	s := newMockIndexPruneStore()
	s.storeIndex("a.caibx")
	s.storeIndex("b.caibx")

	empty := map[string]struct{}{}

	// Run 1: both become candidates.
	require.NoError(t, s.SafePruneIndexes(ctx, empty))
	s.assertPrunable(t, "a.caibx")
	s.assertPrunable(t, "b.caibx")

	// Externally remove b.caibx (e.g. deleted by another process or a prior
	// manual operation) before run 2.
	s.mu.Lock()
	delete(s.indexes, "b.caibx")
	s.mu.Unlock()

	// Run 2: a.caibx is deleted normally; b.caibx is absent from the listing
	// and requires no action — no error, no spurious DeleteIndexes call.
	require.NoError(t, s.SafePruneIndexes(ctx, empty))
	s.assertAbsent(t, "a.caibx")
	s.assertAbsent(t, "b.caibx")
	s.assertNotPrunable(t, "a.caibx")
	s.assertNotPrunable(t, "b.caibx")
}

// TestMockSafePruneIndexesVsUnsafeRace directly contrasts the safe and unsafe
// pruning strategies to demonstrate the race protection. An index present in
// the store but absent from the keep set (because it was written after the keep
// set was assembled) is deleted immediately by PruneIndexes but deferred by
// SafePruneIndexes.
func TestMockSafePruneIndexesVsUnsafeRace(t *testing.T) {
	ctx := context.Background()
	// new.caibx was written after the keep set was assembled, so the caller
	// does not know about it yet.
	keep := map[string]struct{}{}

	// Unsafe: PruneIndexes deletes new.caibx in the same operation.
	unsafe := newMockIndexPruneStore()
	unsafe.storeIndex("new.caibx")
	require.NoError(t, unsafe.PruneIndexes(ctx, keep))
	unsafe.assertAbsent(t, "new.caibx")

	// Safe: SafePruneIndexes records new.caibx as a candidate but does not
	// delete it until the next run.
	safe := newMockIndexPruneStore()
	safe.storeIndex("new.caibx")
	require.NoError(t, safe.SafePruneIndexes(ctx, keep))
	safe.assertPresent(t, "new.caibx")
	safe.assertPrunable(t, "new.caibx")
}
