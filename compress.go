//go:build !datadog
// +build !datadog

package desync

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Create a reader/writer that caches compressors.
var (
	encoder, _ = zstd.NewWriter(nil)
	decoder, _ = zstd.NewReader(nil)
)

// compressBufPool recycles destination buffers for Compress to reduce GC
// pressure.  Each pooled buffer grows to the maximum compressed output
// size seen by that goroutine and stays that size.
var compressBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0)
		return &b
	},
}

// decompressBufPool recycles destination buffers for Decompress when
// the caller does not supply one (dst == nil).
var decompressBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0)
		return &b
	},
}

// Compress a block using the only (currently) supported algorithm.
func Compress(src []byte) ([]byte, error) {
	bufp := compressBufPool.Get().(*[]byte)
	compressed := encoder.EncodeAll(src, (*bufp)[:0])

	// Return a right-sized copy so the caller owns its own memory.
	// The pool buffer (which may be oversized) goes back for reuse.
	result := make([]byte, len(compressed))
	copy(result, compressed)

	*bufp = compressed[:0] // keep the backing array for next use
	compressBufPool.Put(bufp)
	return result, nil
}

// Decompress a block using the only supported algorithm. If you already have
// a buffer it can be passed into dst and will be used. If dst=nil, a pooled
// buffer is used internally and the result is copied to a right-sized slice.
func Decompress(dst, src []byte) ([]byte, error) {
	if dst != nil {
		return decoder.DecodeAll(src, dst)
	}

	bufp := decompressBufPool.Get().(*[]byte)
	decompressed, err := decoder.DecodeAll(src, (*bufp)[:0])
	if err != nil {
		*bufp = decompressed[:0]
		decompressBufPool.Put(bufp)
		return nil, err
	}

	result := make([]byte, len(decompressed))
	copy(result, decompressed)

	*bufp = decompressed[:0]
	decompressBufPool.Put(bufp)
	return result, nil
}
