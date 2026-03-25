package desync

import "sync"

var (
	globalPool     *ChunkPool
	globalPoolOnce sync.Once
	maxChunkSize   uint64
)

// Init configures desync for the given max chunk size: sets up the zstd
// compression window and prepares the global chunk pool.  Must be called
// before any chunking or compression operations.
func Init(maxChunkBytes uint64) {
	maxChunkSize = maxChunkBytes
	InitCompression(maxChunkBytes)
}

// GetChunkPool returns the global chunk pool, creating it on first call.
// If Init was not called, a default max chunk size of 256 KB is used.
func GetChunkPool() *ChunkPool {
	globalPoolOnce.Do(func() {
		size := maxChunkSize
		if size == 0 {
			size = 256 * 1024
		}
		globalPool = NewChunkPool(int(size))
	})
	return globalPool
}
