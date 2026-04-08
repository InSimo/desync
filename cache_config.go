package desync

import (
	"fmt"
	"strings"

	"github.com/insimo/cacheevict"
)

// DesyncCacheEvictConfig returns a cacheevict.Config configured for desync's
// flat 4-hex-char subdirectory layout. This is used by SizeLimitStore and
// the cache management commands (cache-stats, cache-clear, cache-trim).
func DesyncCacheEvictConfig(baseDir string, uncompressed bool) cacheevict.Config {
	chunkExt := CompressedChunkExt
	if uncompressed {
		chunkExt = UncompressedChunkExt
	}

	return cacheevict.Config{
		BaseDir: baseDir,
		SubdirPath: func(idx int) string {
			return fmt.Sprintf("%04x", idx)
		},
		FilePrefix: func(relPath string) int {
			parts := strings.SplitN(relPath, "/", 2)
			if len(parts) == 0 || len(parts[0]) != 4 {
				return 0
			}
			var idx int
			fmt.Sscanf(parts[0], "%04x", &idx)
			return idx
		},
		IsCachedFile: func(name string) bool {
			if chunkExt == "" {
				return !strings.HasSuffix(name, PrunableExt) &&
					!strings.HasSuffix(name, ProtectExt) &&
					!strings.HasPrefix(name, tmpChunkPrefix)
			}
			return strings.HasSuffix(name, chunkExt) &&
				!strings.HasSuffix(name, PrunableExt) &&
				!strings.HasSuffix(name, ProtectExt)
		},
		IsTempFile: func(name string) bool {
			return strings.HasPrefix(name, tmpChunkPrefix)
		},
	}
}
