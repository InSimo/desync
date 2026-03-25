//go:build datadog
// +build datadog

package desync

import (
	"github.com/DataDog/zstd"
)

// InitCompression is a no-op for the DataDog zstd backend.
func InitCompression(maxChunkBytes uint64) {}

// Compress appends zstd-compressed data to dst and returns the extended slice.
func Compress(dst, src []byte) ([]byte, error) {
	return zstd.CompressLevel(dst, src, 3)
}

// Decompress a block using the only supported algorithm. If you already have
// a buffer it can be passed into out and will be used. If out=nil, a buffer
// will be allocated.
func Decompress(out, in []byte) ([]byte, error) {
	return zstd.Decompress(out, in)
}
