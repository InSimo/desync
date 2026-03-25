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

// Compress appends zstd-compressed data to dst and returns the extended slice.
// Pass nil to let the encoder allocate.
func Compress(dst, src []byte) ([]byte, error) {
	return getEncoder().EncodeAll(src, dst), nil
}

// Decompress appends zstd-decompressed data to dst and returns the extended slice.
// Pass nil to let the decoder allocate.
func Decompress(dst, src []byte) ([]byte, error) {
	return getDecoder().DecodeAll(src, dst)
}
