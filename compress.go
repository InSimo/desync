//go:build !datadog
// +build !datadog

package desync

import (
	"math/bits"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Global encoder/decoder — initialized exactly once, lazily.
// Call InitCompression before any Compress/Decompress to set a window
// size matched to the max chunk size.  If not called, defaults apply.
var (
	encoder      *zstd.Encoder
	decoder      *zstd.Decoder
	compressOnce sync.Once
	// encOpts is set by InitCompression before the once fires.
	encOpts []zstd.EOption
)

func initCompression() {
	encoder, _ = zstd.NewWriter(nil, encOpts...)
	decoder, _ = zstd.NewReader(nil)
}

func getEncoder() *zstd.Encoder {
	compressOnce.Do(initCompression)
	return encoder
}

func getDecoder() *zstd.Decoder {
	compressOnce.Do(initCompression)
	return decoder
}

// InitCompression configures the global zstd encoder with a window size
// matched to maxChunkBytes.  Since no chunk can exceed maxChunkBytes, a
// larger window only wastes memory.  The window is rounded up to the
// next power of two (zstd requirement) and capped at the library default
// of 8 MiB so we never *increase* memory usage.
//
// Must be called before any Compress/Decompress calls.  Subsequent calls
// are no-ops (the first call wins).
func InitCompression(maxChunkBytes uint64) {
	const defaultWindow = 8 << 20 // zstd default (not exported by the library)

	win := nextPow2(maxChunkBytes)
	win = max(win, zstd.MinWindowSize)
	win = min(win, defaultWindow)
	encOpts = []zstd.EOption{zstd.WithWindowSize(int(win))}
	// The actual initialization happens on the first getEncoder/getDecoder call.
}

// nextPow2 returns the smallest power of two >= v.
func nextPow2(v uint64) uint64 {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len64(v-1)
}

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
	compressed := getEncoder().EncodeAll(src, (*bufp)[:0])

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
	dec := getDecoder()
	if dst != nil {
		return dec.DecodeAll(src, dst)
	}

	bufp := decompressBufPool.Get().(*[]byte)
	decompressed, err := dec.DecodeAll(src, (*bufp)[:0])
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
