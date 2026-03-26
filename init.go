package desync

import "sync"

var (
	globalPool     *ChunkPool
	globalPoolOnce sync.Once
	maxChunkSize   uint64
)

// Init configures desync for the given max chunk size: sets up the zstd
// compression window and creates the global chunk pool.  Must be called
// before any chunking or compression operations.  When Init is not called,
// chunk pooling is disabled and all allocations go through the GC.
func Init(maxChunkBytes uint64) {
	maxChunkSize = maxChunkBytes
	InitCompression(maxChunkBytes)
	globalPoolOnce.Do(func() {
		globalPool = NewChunkPool(int(maxChunkBytes))
	})
}

// GetChunkPool returns the global chunk pool, or nil if Init was not called.
func GetChunkPool() *ChunkPool {
	return globalPool
}

// getPooledChunk returns a reset chunk from the global pool, or nil if
// the pool is not initialized (Init was not called).
func getPooledChunk() *Chunk {
	if globalPool == nil {
		return nil
	}
	return globalPool.Get()
}
