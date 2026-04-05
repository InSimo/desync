package desync

import (
	"crypto/rand"
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
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	chunk := makeChunk(t, 1024)
	id := chunk.ID()

	err = sls.StoreChunk(chunk)
	require.NoError(t, err)

	has, err := sls.HasChunk(id)
	require.NoError(t, err)
	require.True(t, has)

	out, err := sls.GetChunk(id)
	require.NoError(t, err)
	outData, err := out.Data()
	require.NoError(t, err)
	inData, err := chunk.Data()
	require.NoError(t, err)
	require.Equal(t, inData, outData)

	require.Greater(t, sls.TotalSize(), int64(0))
	require.Equal(t, int64(1), sls.TotalFiles())
}

func TestSizeLimitStoreCounterAccuracy(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	var expectedTotal int64
	for range 10 {
		chunk := makeChunk(t, 2048)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
		_, p := ls.nameFromID(chunk.ID())
		info, err := os.Stat(p)
		require.NoError(t, err)
		expectedTotal += info.Size()
	}

	require.Equal(t, expectedTotal, sls.TotalSize())
	require.Equal(t, int64(10), sls.TotalFiles())
}

func TestSizeLimitStoreOverwrite(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	chunk := makeChunk(t, 1024)
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	sizeAfterFirst := sls.TotalSize()

	err = sls.StoreChunk(chunk)
	require.NoError(t, err)

	require.Equal(t, sizeAfterFirst, sls.TotalSize())
	require.Equal(t, int64(1), sls.TotalFiles()) // overwrite doesn't add
}

func TestSizeLimitStoreNoEviction(t *testing.T) {
	ls := localStoreForTest(t)
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

	for _, id := range ids {
		has, err := sls.HasChunk(id)
		require.NoError(t, err)
		require.True(t, has)
	}
}

func TestSizeLimitStoreEviction(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 500, 0, 4)
	require.NoError(t, err)
	defer sls.Close()

	for range 50 {
		chunk := makeChunk(t, 256)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}

	require.LessOrEqual(t, sls.TotalSize(), int64(600))
}

func TestSizeLimitStoreMaxFilesEviction(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 0, 10, 1)
	require.NoError(t, err)
	defer sls.Close()

	for range 20 {
		chunk := makeChunk(t, 256)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}

	require.LessOrEqual(t, sls.TotalFiles(), int64(11))
}

func TestSizeLimitStoreLRU(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 1)
	require.NoError(t, err)
	defer sls.Close()

	// Phase 1: store "old" chunks.
	var oldIDs []ChunkID
	for range 5 {
		chunk := makeChunk(t, 128)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
		oldIDs = append(oldIDs, chunk.ID())
	}

	time.Sleep(100 * time.Millisecond)

	// Phase 2: store "new" chunks (newer mtime).
	var newIDs []ChunkID
	for range 5 {
		chunk := makeChunk(t, 128)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
		newIDs = append(newIDs, chunk.ID())
	}

	time.Sleep(100 * time.Millisecond)

	// Phase 3: touch old chunks via GetChunk (mtime becomes newest).
	for _, id := range oldIDs {
		c, err := sls.GetChunk(id)
		if err == nil {
			c.Release()
		}
	}

	// Phase 4: reduce limit to force eviction.
	// Access internal handler to change limit (test-only).
	sls.handler.SetMaxSize(200)
	trigger := makeChunk(t, 128)
	err = sls.StoreChunk(trigger)
	require.NoError(t, err)

	evictedNew := 0
	evictedOld := 0
	for _, id := range newIDs {
		has, _ := sls.HasChunk(id)
		if !has {
			evictedNew++
		}
	}
	for _, id := range oldIDs {
		has, _ := sls.HasChunk(id)
		if !has {
			evictedOld++
		}
	}

	require.Greater(t, evictedNew, evictedOld)
}

func TestSizeLimitStoreConcurrency(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	var wg sync.WaitGroup
	var errCount atomic.Int32

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				chunk := makeChunk(t, 512)
				if err := sls.StoreChunk(chunk); err != nil {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int32(0), errCount.Load())
	require.Greater(t, sls.TotalSize(), int64(0))
}

func TestSizeLimitStoreColdStart(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)

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

	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	require.Equal(t, expectedTotal, sls.TotalSize())
	require.Equal(t, int64(15), sls.TotalFiles())
}

func TestSizeLimitStorePartitionMismatch(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)

	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	chunk := makeChunk(t, 1024)
	err = sls.StoreChunk(chunk)
	require.NoError(t, err)
	sls.Close()

	sls2, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 32)
	require.NoError(t, err)
	defer sls2.Close()
	require.Greater(t, sls2.TotalSize(), int64(0))
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
	require.Greater(t, sls.TotalSize(), int64(0))

	err = sls.RemoveChunk(id)
	require.NoError(t, err)
	require.Equal(t, int64(0), sls.TotalSize())
	require.Equal(t, int64(0), sls.TotalFiles())
}

func TestSizeLimitStorePartitions(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8, 16, 32, 256, 65536} {
		dir := t.TempDir()
		ls, _ := NewLocalStore(dir, StoreOptions{})
		sls, err := NewSizeLimitStore(ls, 1024*1024, 0, p)
		require.NoError(t, err, "partition=%d should be valid", p)
		sls.Close()
	}

	ls := localStoreForTest(t)
	for _, p := range []int{3, 5, 7, 15, 100} {
		_, err := NewSizeLimitStore(ls, 1024*1024, 0, p)
		require.Error(t, err, "partition=%d should be invalid", p)
	}
}

func TestSizeLimitStoreDisabled(t *testing.T) {
	ls := localStoreForTest(t)
	sls, err := NewSizeLimitStore(ls, 0, 0, 0)
	require.NoError(t, err)
	defer sls.Close()

	for range 50 {
		chunk := makeChunk(t, 1024)
		err := sls.StoreChunk(chunk)
		require.NoError(t, err)
	}

	require.Equal(t, int64(50), sls.TotalFiles())
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
	info1, _ := os.Stat(p)
	mtime1 := info1.ModTime()

	time.Sleep(50 * time.Millisecond)

	out, err := sls.GetChunk(id)
	require.NoError(t, err)
	out.Release()

	info2, _ := os.Stat(p)
	mtime2 := info2.ModTime()
	require.True(t, mtime2.After(mtime1))
}

func TestSizeLimitStoreInitCountersSkipsNonChunkFiles(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLocalStore(dir, StoreOptions{})
	require.NoError(t, err)

	chunk := makeChunk(t, 1024)
	err = ls.StoreChunk(chunk)
	require.NoError(t, err)
	_, p := ls.nameFromID(chunk.ID())
	chunkInfo, _ := os.Stat(p)

	subdir := filepath.Dir(p)
	os.WriteFile(filepath.Join(subdir, "random.txt"), []byte("not a chunk"), 0644)
	cid := chunk.ID()
	os.WriteFile(filepath.Join(subdir, cid.String()+".cacnk.prunable"), nil, 0644)

	sls, err := NewSizeLimitStore(ls, 100*1024*1024, 0, 16)
	require.NoError(t, err)
	defer sls.Close()

	require.Equal(t, chunkInfo.Size(), sls.TotalSize())
	require.Equal(t, int64(1), sls.TotalFiles())
}
