package desync

import (
	"errors"
	"sync"
)

// Chunk holds chunk data plain, storage format, or both. If a chunk is created
// from storage data, such as read from a compressed chunk store, and later the
// application requires the plain data, it'll be converted on demand by applying
// the given storage converters in reverse order. The converters can only be used
// to read the plain data, not to convert back to storage format.
type Chunk struct {
	data         []byte     // Plain data if available
	storage      []byte     // Storage format (compressed, encrypted, etc)
	converters   Converters // Modifiers to convert from storage format to plain
	id           ChunkID
	idCalculated bool
	dataBuf      []byte // Backing buffer for data (pooled chunks only)
	storageBuf   []byte // Backing buffer for storage (pooled chunks only)
}

// Reset clears a chunk's fields for reuse, preserving backing buffers.
func (c *Chunk) Reset() {
	c.data = nil
	c.storage = nil
	c.converters = nil
	c.id = ChunkID{}
	c.idCalculated = false
}

// ChunkPool is a pool of Chunk objects with pre-allocated backing buffers.
// Using a pool eliminates per-chunk heap allocations in hot paths like
// ChopFile, where chunks are created, compressed, stored, and discarded
// in rapid succession.
type ChunkPool struct {
	pool sync.Pool
}

// NewChunkPool creates a pool of chunks with backing buffers of maxSize bytes
// for both plain and compressed data.
func NewChunkPool(maxSize int) *ChunkPool {
	return &ChunkPool{
		pool: sync.Pool{
			New: func() any {
				return &Chunk{
					dataBuf:    make([]byte, maxSize),
					storageBuf: make([]byte, maxSize),
				}
			},
		},
	}
}

// Get returns a reset chunk from the pool.
func (p *ChunkPool) Get() *Chunk {
	c := p.pool.Get().(*Chunk)
	c.Reset()
	return c
}

// Put returns a chunk to the pool.  Non-pooled chunks (dataBuf == nil) are
// silently ignored.
func (p *ChunkPool) Put(c *Chunk) {
	if c == nil || c.dataBuf == nil {
		return
	}
	c.Reset()
	p.pool.Put(c)
}

// dstChunk returns the destination chunk from a variadic GetChunk argument,
// or nil if none was provided.
func dstChunk(dst []*Chunk) *Chunk {
	if len(dst) > 0 {
		return dst[0]
	}
	return nil
}

// chunkFromStorage populates a chunk with compressed storage data.  When dst
// is a pooled chunk, b is assigned directly to c.storage (no copy) so the
// chunk stays pooled and the caller's pool.Put works correctly.  When dst is
// nil, a new Chunk is allocated via NewChunkFromStorage.
//
// For backends that can read directly into storageBuf (LocalStore, S3Store,
// GCStore), use the inline read-into-buffer pattern instead of this helper.
func chunkFromStorage(id ChunkID, b []byte, conv Converters, skipVerify bool, dst []*Chunk) (*Chunk, error) {
	if c := dstChunk(dst); c != nil {
		c.storage = b
		c.converters = conv
		c.id = id
		c.idCalculated = false
		if !skipVerify {
			if sum := c.ID(); sum != id {
				return nil, ChunkInvalid{ID: id, Sum: sum}
			}
		} else {
			c.idCalculated = true
		}
		return c, nil
	}
	return NewChunkFromStorage(id, b, conv, skipVerify)
}

// growStorageBuf ensures storageBuf can hold at least size bytes,
// reallocating to nextPow2(size) if needed.  Also grows dataBuf to
// the same capacity so decompressed data fits.
func (c *Chunk) growStorageBuf(size int) {
	newCap := int(nextPow2(uint64(size)))
	if cap(c.storageBuf) < newCap {
		c.storageBuf = make([]byte, newCap)
	}
	if cap(c.dataBuf) < newCap {
		c.dataBuf = make([]byte, newCap)
	}
}

// NewChunk creates a new chunk from plain data. The data is trusted and the ID is
// calculated on demand.
func NewChunk(b []byte) *Chunk {
	return &Chunk{data: b}
}

// NewChunkWithID creates a new chunk from either compressed or uncompressed data
// (or both if available). It also expects an ID and validates that it matches
// the uncompressed data unless skipVerify is true. If called with just compressed
// data, it'll decompress it for the ID validation.
func NewChunkWithID(id ChunkID, b []byte, skipVerify bool) (*Chunk, error) {
	c := &Chunk{id: id, data: b}
	if skipVerify {
		c.idCalculated = true // Pretend this was calculated. No need to re-calc later
		return c, nil
	}
	sum := c.ID()
	if sum != id {
		return nil, ChunkInvalid{ID: id, Sum: sum}
	}
	return c, nil
}

// NewChunkFromStorage builds a new chunk from data that is not in plain format.
// It uses raw storage format from its source and the modifiers are used to convert
// into plain data as needed.
func NewChunkFromStorage(id ChunkID, b []byte, modifiers Converters, skipVerify bool) (*Chunk, error) {
	c := &Chunk{id: id, storage: b, converters: modifiers}
	if skipVerify {
		c.idCalculated = true // Pretend this was calculated. No need to re-calc later
		return c, nil
	}
	sum := c.ID()
	if sum != id {
		return nil, ChunkInvalid{ID: id, Sum: sum}
	}
	return c, nil
}

// Data returns the chunk data in uncompressed form. If the chunk was created
// with compressed data only, it'll be decompressed, stored and returned. The
// caller must not modify the data in the returned slice.
func (c *Chunk) Data() ([]byte, error) {
	if len(c.data) > 0 {
		return c.data, nil
	}
	if len(c.storage) > 0 {
		if c.dataBuf != nil && c.converters.hasCompression() {
			decompressed, err := Decompress(c.dataBuf[:0], c.storage)
			if err != nil {
				return nil, err
			}
			c.data = decompressed
			return c.data, nil
		}
		var err error
		c.data, err = c.converters.fromStorage(c.storage)
		return c.data, err
	}
	return nil, errors.New("no data in chunk")
}

// ID returns the checksum/ID of the uncompressed chunk data. The ID is stored
// after the first call and doesn't need to be re-calculated. Note that calculating
// the ID may mean decompressing the data first.
func (c *Chunk) ID() ChunkID {
	if c.idCalculated {
		return c.id
	}
	b, err := c.Data()
	if err != nil {
		return ChunkID{}
	}
	c.id = Digest.Sum(b)
	c.idCalculated = true
	return c.id
}

// Storage returns the chunk data in compressed form. If the chunk was created
// with compressed data and same modifiers, this data will be returned as is.
// When the chunk has a backing storageBuf (pooled), compression writes
// directly into it, avoiding a heap allocation.
// The caller must not modify the data in the returned slice.
func (c *Chunk) Storage(modifiers Converters) ([]byte, error) {
	if len(c.storage) > 0 && modifiers.equal(c.converters) {
		return c.storage, nil
	}
	b, err := c.Data()
	if err != nil {
		return nil, err
	}
	if c.storageBuf != nil && modifiers.hasCompression() {
		compressed, err := Compress(c.storageBuf[:0], b)
		if err != nil {
			return nil, err
		}
		c.storage = compressed
		c.converters = modifiers
		return c.storage, nil
	}
	return modifiers.toStorage(b)
}
