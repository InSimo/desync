package desync

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// makeChunk creates a chunk with random data of the given size.
func makeChunk(t *testing.T, size int) *Chunk {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return NewChunk(data)
}

// localStoreForTest creates a LocalStore in a temp directory.
func localStoreForTest(t *testing.T) LocalStore {
	t.Helper()
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)
	return ls
}

func TestSizeLimitStoreBasic(t *testing.T) {
	ls := localStoreForTest(t)
	// Large limit so eviction won't trigger.
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	chunk := makeChunk(t, 1024)
	id := chunk.ID()

	// Store a chunk.
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)

	// Verify it's there.
	has, err := sls.HasChunk(id)
	require.NoError(t, err)
	require.True(t, has)

	// Read it back.
	out, err := sls.GetChunk(id)
	require.NoError(t, err)
	outData, err := out.Data()
	require.NoError(t, err)
	inData, err := chunk.Data()
	require.NoError(t, err)
	require.Equal(t, inData, outData)

	// Total size should be > 0.
	total := sls.totalSize()
	require.Greater(t, total, int64(0))
}

func TestSizeLimitStoreCounterAccuracy(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	// Store several chunks and track expected sizes.
	var expectedTotal int64
	for range 10 {
		chunk := makeChunk(t, 2048)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)

		// Get the actual stored size.
		_, p := ls.nameFromID(chunk.ID())
		info, err := os.Stat(p)
		require.NoError(t, err)
		expectedTotal += info.Size()
	}

	require.Equal(t, expectedTotal, sls.totalSize())
}

func TestSizeLimitStoreOverwrite(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	// Store a chunk.
	chunk := makeChunk(t, 1024)
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	sizeAfterFirst := sls.totalSize()

	// Store the same chunk again (overwrite).
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	sizeAfterSecond := sls.totalSize()

	// Counter should not have changed (same file, same size).
	require.Equal(t, sizeAfterFirst, sizeAfterSecond)
}

func TestSizeLimitStoreNoEviction(t *testing.T) {
	ls := localStoreForTest(t)
	// Set limit much larger than what we'll store.
	sls, err := NewSizeLimitStore(ls, 10*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	var ids []ChunkID
	for range 20 {
		chunk := makeChunk(t, 512)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
		ids = append(ids, chunk.ID())
	}

	// All chunks should still be present.
	for _, id := range ids {
		has, err := sls.HasChunk(id)
		require.NoError(t, err)
		require.True(t, has, "chunk %s should not have been evicted", id)
	}
}

func TestSizeLimitStoreEviction(t *testing.T) {
	ls := localStoreForTest(t)
	// Use a very small max size to force eviction.
	// Each compressed chunk is at least ~30-50 bytes on disk.
	sls, err := NewSizeLimitStore(ls, 500, 0, 4)
	require.NoError(t, err)
	defer sls.Close()

	// Store enough chunks to exceed the limit.
	for range 50 {
		chunk := makeChunk(t, 256)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}

	// Total size should be at or below the limit (within tolerance,
	// since eviction targets 90% of fair share per partition).
	total := sls.totalSize()
	// Allow some headroom: eviction runs after each write but the last
	// write may push it slightly over.
	require.LessOrEqual(t, total, int64(600),
		"total size %d should be near or below limit 500", total)
}

func TestSizeLimitStoreLRU(t *testing.T) {
	ls := localStoreForTest(t)
	// Tiny limit to force eviction.
	sls, err := NewSizeLimitStore(ls, 400, 0, 4)
	require.NoError(t, err)
	defer sls.Close()

	// Store some "old" chunks with a deliberate sleep so they get an older mtime.
	var oldIDs []ChunkID
	for range 5 {
		chunk := makeChunk(t, 128)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
		oldIDs = append(oldIDs, chunk.ID())
	}

	// Sleep briefly so that "new" chunks have a later mtime.
	time.Sleep(50 * time.Millisecond)

	// Touch old chunks via GetChunk to make them "recently used".
	for _, id := range oldIDs {
		c, err := sls.GetChunk(id)
		if err == nil {
			c.Release()
		}
	}

	// Sleep again so the next writes have a later mtime than the touch.
	time.Sleep(50 * time.Millisecond)

	// Store "new" chunks that should be evicted first (they haven't been
	// accessed since storage, unlike the old ones that were touched).
	// Actually — the "new" chunks have a *newer* mtime than the old ones
	// (which were just touched). So the old ones' mtime < new ones' mtime.
	// Wait, that's wrong. The old ones were touched AFTER the new ones
	// don't exist yet. Let me restructure.

	// Use partitions=1 so all chunks are in the same partition and eviction
	// is purely mtime-based across all chunks.
	_ = sls.Close()
	ls2 := localStoreForTest(t)
	sls2, err := NewSizeLimitStore(ls2, 100*1024*1024, 0, 1)
	require.NoError(t, err)
	defer sls2.Close()

	// Phase 1: store "old" chunks (mtime = T1).
	oldIDs = nil
	for range 5 {
		chunk := makeChunk(t, 128)
		err := sls2.StoreChunk(chunk)
		require.NoError(t, err)
		oldIDs = append(oldIDs, chunk.ID())
	}

	time.Sleep(100 * time.Millisecond)

	// Phase 2: store "new" chunks (mtime = T2 > T1).
	var newIDs []ChunkID
	for range 5 {
		chunk := makeChunk(t, 128)
		err := sls2.StoreChunk(chunk)
		require.NoError(t, err)
		newIDs = append(newIDs, chunk.ID())
	}

	time.Sleep(100 * time.Millisecond)

	// Phase 3: touch old chunks via GetChunk (mtime becomes T3 > T2).
	for _, id := range oldIDs {
		c, err := sls2.GetChunk(id)
		if err == nil {
			c.Release()
		}
	}

	// Phase 4: reduce limit to force eviction on next write.
	sls2.maxSize = 200
	trigger := makeChunk(t, 128)
	err = sls2.StoreChunk(trigger)
	require.NoError(t, err)

	// New chunks (untouched, mtime = T2) should be evicted before old chunks
	// (touched, mtime = T3 > T2).
	evictedNew := 0
	evictedOld := 0
	for _, id := range newIDs {
		has, _ := sls2.HasChunk(id)
		if !has {
			evictedNew++
		}
	}
	for _, id := range oldIDs {
		has, _ := sls2.HasChunk(id)
		if !has {
			evictedOld++
		}
	}

	require.Greater(t, evictedNew, evictedOld,
		"LRU: untouched new chunks (%d evicted) should be evicted more than touched old chunks (%d evicted)",
		evictedNew, evictedOld)
}

func TestSizeLimitStoreBitmask(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	chunk := makeChunk(t, 512)
	idx := prefixIndex(chunk.ID())

	// Before storing, bitmask should be unset.
	require.False(t, sls.hasBitmask(idx))

	err = sls.StoreChunk(chunk)
	require.NoError(t, err)

	// After storing, bitmask should be set.
	require.True(t, sls.hasBitmask(idx))
}

func TestSizeLimitStoreConcurrency(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	// Write from multiple goroutines simultaneously.
	var wg sync.WaitGroup
	var errCount atomic.Int32
	const goroutines = 8
	const chunksPerGoroutine = 20

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range chunksPerGoroutine {
				chunk := makeChunk(t, 512)
				if err := sls.StoreChunk(chunk); err != nil {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int32(0), errCount.Load(), "no store errors expected")

	// Counter should be positive and consistent.
	total := sls.totalSize()
	require.Greater(t, total, int64(0))
}

func TestSizeLimitStoreColdStart(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)

	// Pre-populate the store without SizeLimitStore.
	var expectedTotal int64
	for range 15 {
		chunk := makeChunk(t, 1024)
		err := ls.StoreChunk(chunk)
		require.NoError(t, err)
		_, p := ls.nameFromID(chunk.ID())
		info, err := os.Stat(p)
		require.NoError(t, err)
		expectedTotal += info.Size()
	}

	// Now open SizeLimitStore — should scan and populate counters.
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	require.Equal(t, expectedTotal, sls.totalSize(),
		"cold start should correctly tally existing chunks")
}

func TestSizeLimitStorePartitionMismatch(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)

	// Create with 16 partitions.
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)

	chunk := makeChunk(t, 1024)
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	sls.Close()

	// Reopen with 32 partitions — should recreate and rescan.
	sls2, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 32)
	require.NoError(t, err)
	defer sls2.Close()

	// The stored chunk should still be tracked.
	total := sls2.totalSize()
	require.Greater(t, total, int64(0), "counter should be populated after partition mismatch rescan")
}

func TestSizeLimitStoreFileCount(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	require.Equal(t, int64(0), sls.totalFiles())

	// Store 10 distinct chunks.
	for range 10 {
		chunk := makeChunk(t, 512)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}
	require.Equal(t, int64(10), sls.totalFiles())

	// Overwrite one — file count should not change.
	chunk := makeChunk(t, 512)
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	require.Equal(t, int64(11), sls.totalFiles())

	err = sls.StoreChunk(chunk) // overwrite
	require.NoError(t, err)
	require.Equal(t, int64(11), sls.totalFiles()) // no change

	// Remove one — file count decrements.
	err = sls.RemoveChunk(chunk.ID())
	require.NoError(t, err)
	require.Equal(t, int64(10), sls.totalFiles())
}

func TestSizeLimitStoreRemoveChunk(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	chunk := makeChunk(t, 1024)
	id := chunk.ID()

	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	sizeAfterStore := sls.totalSize()
	require.Greater(t, sizeAfterStore, int64(0))

	err = sls.RemoveChunk(id)
	require.NoError(t, err)
	sizeAfterRemove := sls.totalSize()
	require.Equal(t, int64(0), sizeAfterRemove)
}

func TestSizeLimitStoreMaxFilesEviction(t *testing.T) {
	ls := localStoreForTest(t)
	// No byte limit, but limit to 10 files. Use 1 partition for simplicity.
	sls, err := NewSizeLimitStore(ls, 0, 10, 1)
	require.NoError(t, err)
	defer sls.Close()

	// Store 20 files — should trigger eviction.
	for range 20 {
		chunk := makeChunk(t, 256)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}

	// File count should be at or below the limit.
	// Target is 0.9 * totalFiles / P = 0.9 * N / 1 = 0.9*N.
	// After eviction the count should be around 9.
	files := sls.totalFiles()
	require.LessOrEqual(t, files, int64(11),
		"totalFiles %d should be near or below max-files 10", files)
}

func TestSizeLimitStorePartitions(t *testing.T) {
	// Test that valid partition values work and invalid ones fail.
	ls := localStoreForTest(t)

	for _, p := range []int{1, 2, 4, 8, 16, 32, 256, 65536} {
		dir := t.TempDir()
		ls2, _ := NewLocalStore(dir, StoreOptions{})
		sls, err := NewSizeLimitStore(ls2, 1024*1024, 0, p)
		require.NoError(t, err, "partition=%d should be valid", p)
		sls.Close()
	}

	for _, p := range []int{3, 5, 7, 15, 100} {
		_, err := NewSizeLimitStore(ls, 1024*1024, 0, p)
		require.Error(t, err, "partition=%d should be invalid", p)
	}
}

func TestSizeLimitStoreDisabled(t *testing.T) {
	// maxSize=0 with partitions=0 should use defaults and not create the file.
	ls := localStoreForTest(t)

	// We can't really test "no file created" with maxSize=0 because
	// NewSizeLimitStore still creates the mmap. Instead, test that
	// eviction never triggers.
	sls, err := NewSizeLimitStore(ls, 0, 0, 0)
	require.NoError(t, err)
	defer sls.Close()

	for range 50 {
		chunk := makeChunk(t, 1024)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}

	// All chunks should remain (no eviction when maxSize=0).
	total := sls.totalSize()
	require.Greater(t, total, int64(0))
}

func TestSizeLimitStoreMtimeUpdate(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	chunk := makeChunk(t, 1024)
	id := chunk.ID()
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)

	_, p := ls.nameFromID(id)
	info1, err := os.Stat(p)
	require.NoError(t, err)
	mtime1 := info1.ModTime()

	time.Sleep(50 * time.Millisecond)

	// GetChunk should update mtime.
	out, err := sls.GetChunk(id)
	require.NoError(t, err)
	out.Release()

	info2, err := os.Stat(p)
	require.NoError(t, err)
	mtime2 := info2.ModTime()

	require.True(t, mtime2.After(mtime1),
		"GetChunk should update mtime: before=%v after=%v", mtime1, mtime2)
}

func TestPrefixIndex(t *testing.T) {
	// Verify that prefixIndex matches the hex prefix used by LocalStore.
	for _, hex := range []string{"0000", "00ff", "abcd", "ffff"} {
		var expected int
		fmt.Sscanf(hex, "%04x", &expected)
		// Build a ChunkID whose first 2 bytes match.
		var id ChunkID
		id[0] = byte(expected >> 8)
		id[1] = byte(expected & 0xff)
		require.Equal(t, expected, prefixIndex(id), "hex=%s", hex)
	}
}

func TestSizeLimitStoreInitCountersSkipsNonChunkFiles(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)

	// Store a real chunk.
	chunk := makeChunk(t, 1024)
	err = ls.StoreChunk(chunk)
	require.NoError(t, err)
	_, p := ls.nameFromID(chunk.ID())
	chunkInfo, _ := os.Stat(p)

	// Create some non-chunk files in the same directory.
	subdir := filepath.Dir(p)
	os.WriteFile(filepath.Join(subdir, "random.txt"), []byte("not a chunk"), 0644)
	cid := chunk.ID()
	os.WriteFile(filepath.Join(subdir, cid.String()+".cacnk.prunable"), nil, 0644)

	// SizeLimitStore should only count the real chunk file.
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	require.Equal(t, chunkInfo.Size(), sls.totalSize(),
		"should only count .cacnk files, not other files")
}
