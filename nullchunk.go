package desync

import "sync"

// NullChunk is used in places where it's common to see requests for chunks
// containing only 0-bytes. When a chunked file has large areas of 0-bytes,
// the chunking algorithm does not produce split boundaries, which results
// in many chunks of 0-bytes of size MAX (max chunk size). The NullChunk can be
// used to make requesting this kind of chunk more efficient by serving it
// from memory, rather than request it from disk or network and decompress
// it repeatedly.
type NullChunk struct {
	Data []byte
	ID   ChunkID
}

var nullChunkCache sync.Map // size → *NullChunk

// NewNullChunk returns an initialized chunk consisting of 0-bytes of 'size'
// which must match the max size used in the index to be effective.
// Results are cached by size to avoid repeated allocation of large zero buffers.
func NewNullChunk(size uint64) *NullChunk {
	if v, ok := nullChunkCache.Load(size); ok {
		return v.(*NullChunk)
	}
	b := make([]byte, int(size))
	nc := &NullChunk{
		Data: b,
		ID:   Digest.Sum(b),
	}
	nullChunkCache.Store(size, nc)
	return nc
}
