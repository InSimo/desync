package desync

import (
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

var _ WriteStore = &SizeLimitStore{}

const (
	// cacheSizesFile is the name of the persistent mmap'd file stored in the
	// cache base directory.
	cacheSizesFile = ".cache-sizes"

	// Layout offsets within the mmap'd region.
	//
	//   Header (24 bytes):
	//     [0]  int32  eviction lock (0 = free, PID = in-progress)
	//     [4]  int32  partition count P
	//     [8]  int64  total size in bytes (global)
	//     [16] int64  total file count (global)
	//
	//   Bitmask (8192 bytes):
	//     [24] 65536 bits — one per subdirectory prefix
	//
	//   Per-partition (P × 16 bytes):
	//     [8216 + p*16]     int64  partition size (bytes)
	//     [8216 + p*16 + 8] int64  partition file count
	slHeaderSize      = 24
	slBitmaskOffset   = slHeaderSize                    // 24
	slBitmaskSize     = 65536 / 8                       // 8192
	slCounterOffset   = slBitmaskOffset + slBitmaskSize // 8216
	slPartitionStride = 16                              // 2 × int64 per partition

	// DefaultCachePartitions is the default number of eviction partitions,
	// matching ccache's effective 256 L2-directory granularity.
	DefaultCachePartitions = 256
)

// SizeLimitStore wraps a LocalStore and adds automatic LRU cache eviction
// to keep total stored bytes within a configurable limit. It uses a
// persistent mmap'd file with atomic counters for lock-free cross-process
// size tracking, inspired by the bytelimit package.
//
// Eviction follows ccache's algorithm: max_size (bytes) is used as a trigger
// condition; the actual eviction target is file-count-based. The partition
// with the most files is selected, and the oldest files (by mtime) are
// deleted until the partition's file count drops to 0.9 * totalFiles / P.
type SizeLimitStore struct {
	ls         LocalStore
	maxSize    int64  // maximum cache size in bytes; 0 = disabled
	maxFiles   int64  // maximum number of cached files; 0 = disabled
	partitions int    // number of partitions (power of 2)
	data       []byte // mmap'd persistent region
	pid        int32
}

// NewSizeLimitStore creates a SizeLimitStore wrapping ls. maxSize is the
// maximum cache size in bytes (0 = no byte limit). maxFiles is the maximum
// number of cached files (0 = no file limit). partitions must be a power
// of 2 (or 0 for the default of 256). The mmap'd tracking file is created
// in ls.Base.
func NewSizeLimitStore(ls LocalStore, maxSize int64, maxFiles int64, partitions int) (*SizeLimitStore, error) {
	if partitions <= 0 {
		partitions = DefaultCachePartitions
	}
	if partitions < 1 || partitions > 65536 || bits.OnesCount(uint(partitions)) != 1 {
		return nil, fmt.Errorf("cache-partitions must be a power of 2 between 1 and 65536, got %d", partitions)
	}

	fileSize := slCounterOffset + partitions*slPartitionStride
	path := filepath.Join(ls.Base, cacheSizesFile)

	data, isNew, err := openCacheSizesFile(path, fileSize)
	if err != nil {
		return nil, fmt.Errorf("open cache sizes: %w", err)
	}

	s := &SizeLimitStore{
		ls:         ls,
		maxSize:    maxSize,
		maxFiles:   maxFiles,
		partitions: partitions,
		data:       data,
		pid:        int32(os.Getpid()),
	}

	if isNew {
		atomic.StoreInt32(s.storedPartitionCount(), int32(partitions))
		if err := s.initCounters(); err != nil {
			closeCacheSizesFile(data)
			return nil, fmt.Errorf("init cache counters: %w", err)
		}
	} else {
		// Validate that the stored partition count matches.
		stored := atomic.LoadInt32(s.storedPartitionCount())
		if int(stored) != partitions {
			// Partition count changed — recreate and rescan.
			closeCacheSizesFile(data)
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("remove stale cache sizes: %w", err)
			}
			data, _, err = openCacheSizesFile(path, fileSize)
			if err != nil {
				return nil, fmt.Errorf("recreate cache sizes: %w", err)
			}
			s.data = data
			atomic.StoreInt32(s.storedPartitionCount(), int32(partitions))
			if err := s.initCounters(); err != nil {
				closeCacheSizesFile(data)
				return nil, fmt.Errorf("init cache counters after recreate: %w", err)
			}
		}
	}

	return s, nil
}

// --- mmap layout accessors ---

func (s *SizeLimitStore) evictionLock() *int32 {
	return (*int32)(unsafe.Pointer(&s.data[0]))
}

func (s *SizeLimitStore) storedPartitionCount() *int32 {
	return (*int32)(unsafe.Pointer(&s.data[4]))
}

func (s *SizeLimitStore) globalSize() *int64 {
	return (*int64)(unsafe.Pointer(&s.data[8]))
}

func (s *SizeLimitStore) globalFiles() *int64 {
	return (*int64)(unsafe.Pointer(&s.data[16]))
}

func (s *SizeLimitStore) partitionSize(p int) *int64 {
	return (*int64)(unsafe.Pointer(&s.data[slCounterOffset+p*slPartitionStride]))
}

func (s *SizeLimitStore) partitionFiles(p int) *int64 {
	return (*int64)(unsafe.Pointer(&s.data[slCounterOffset+p*slPartitionStride+8]))
}

func (s *SizeLimitStore) bitmaskWord(prefixIdx int) *uint32 {
	wordIdx := prefixIdx / 32
	return (*uint32)(unsafe.Pointer(&s.data[slBitmaskOffset+wordIdx*4]))
}

func (s *SizeLimitStore) setBitmask(prefixIdx int) {
	atomic.OrUint32(s.bitmaskWord(prefixIdx), 1<<uint(prefixIdx%32))
}

func (s *SizeLimitStore) hasBitmask(prefixIdx int) bool {
	return atomic.LoadUint32(s.bitmaskWord(prefixIdx))&(1<<uint(prefixIdx%32)) != 0
}

func (s *SizeLimitStore) clearBitmask(prefixIdx int) {
	atomic.AndUint32(s.bitmaskWord(prefixIdx), ^(1<<uint(prefixIdx%32)))
}

// --- partition helpers ---

// prefixIndex returns the 0–65535 index for a ChunkID, derived from the
// first two bytes of the hash (matching LocalStore's 4-hex-char prefix).
func prefixIndex(id ChunkID) int {
	return int(id[0])<<8 | int(id[1])
}

// partitionOf returns which partition a chunk belongs to.
func (s *SizeLimitStore) partitionOf(id ChunkID) int {
	return prefixIndex(id) / (65536 / s.partitions)
}

// totalSize returns the global total cache size in bytes. O(1).
func (s *SizeLimitStore) totalSize() int64 {
	return atomic.LoadInt64(s.globalSize())
}

// totalFiles returns the global total file count. O(1).
func (s *SizeLimitStore) totalFiles() int64 {
	return atomic.LoadInt64(s.globalFiles())
}

// --- Store interface ---

func (s *SizeLimitStore) GetChunk(id ChunkID) (*Chunk, error) {
	chunk, err := s.ls.GetChunk(id)
	if err != nil {
		return chunk, err
	}
	// Touch mtime for LRU tracking (best-effort).
	_, p := s.ls.nameFromID(id)
	now := time.Now()
	_ = os.Chtimes(p, now, now)
	return chunk, nil
}

func (s *SizeLimitStore) HasChunk(id ChunkID) (bool, error) {
	return s.ls.HasChunk(id)
}

func (s *SizeLimitStore) StoreChunk(chunk *Chunk) error {
	id := chunk.ID()
	_, p := s.ls.nameFromID(id)

	// Get old size (0 if absent) for correct delta accounting.
	var oldSize int64
	var isNew bool
	if info, err := os.Stat(p); err == nil {
		oldSize = info.Size()
	} else {
		isNew = true
	}

	if err := s.ls.StoreChunk(chunk); err != nil {
		return err
	}

	// Get new stored size.
	var newSize int64
	if info, err := os.Stat(p); err == nil {
		newSize = info.Size()
	}

	sizeDelta := newSize - oldSize
	var filesDelta int64
	if isNew {
		filesDelta = 1
	}

	partition := s.partitionOf(id)
	if sizeDelta != 0 {
		atomic.AddInt64(s.globalSize(), sizeDelta)
		atomic.AddInt64(s.partitionSize(partition), sizeDelta)
	}
	if filesDelta != 0 {
		atomic.AddInt64(s.globalFiles(), filesDelta)
		atomic.AddInt64(s.partitionFiles(partition), filesDelta)
	}

	s.setBitmask(prefixIndex(id))

	s.maybeEvict()
	return nil
}

func (s *SizeLimitStore) RemoveChunk(id ChunkID) error {
	_, p := s.ls.nameFromID(id)

	var size int64
	if info, err := os.Stat(p); err == nil {
		size = info.Size()
	}

	if err := s.ls.RemoveChunk(id); err != nil {
		return err
	}

	if size > 0 {
		partition := s.partitionOf(id)
		atomic.AddInt64(s.globalSize(), -size)
		atomic.AddInt64(s.globalFiles(), -1)
		atomic.AddInt64(s.partitionSize(partition), -size)
		atomic.AddInt64(s.partitionFiles(partition), -1)
	}
	return nil
}

func (s *SizeLimitStore) String() string {
	return s.ls.String()
}

func (s *SizeLimitStore) Close() error {
	if s.data != nil {
		_ = syncCacheSizesFile(s.data)
		err := closeCacheSizesFile(s.data)
		s.data = nil
		return err
	}
	return nil
}

// --- Delegated methods ---

func (s *SizeLimitStore) GetChunkSize(id ChunkID) (int64, error) {
	return s.ls.GetChunkSize(id)
}

// --- Eviction ---

// maybeEvict checks if the cache exceeds the size limit and, if so, evicts
// the oldest files from the partition with the most files.
func (s *SizeLimitStore) needsEviction() bool {
	if s.maxSize > 0 && s.totalSize() > s.maxSize {
		return true
	}
	if s.maxFiles > 0 && s.totalFiles() > s.maxFiles {
		return true
	}
	return false
}

func (s *SizeLimitStore) maybeEvict() {
	if s.maxSize <= 0 && s.maxFiles <= 0 {
		return
	}
	if !s.needsEviction() {
		return
	}

	// Try to acquire the eviction lock (non-blocking CAS).
	if !atomic.CompareAndSwapInt32(s.evictionLock(), 0, s.pid) {
		// Another process holds the lock — check if it's still alive.
		holder := atomic.LoadInt32(s.evictionLock())
		if holder != 0 && processAlive(holder) {
			return // another live process is evicting
		}
		// Stale lock — try to reclaim.
		if !atomic.CompareAndSwapInt32(s.evictionLock(), holder, s.pid) {
			return // someone else reclaimed it
		}
	}
	defer atomic.StoreInt32(s.evictionLock(), 0)

	// Find the partition with the most files (matching ccache's approach).
	maxPartition := 0
	maxFiles := atomic.LoadInt64(s.partitionFiles(0))
	for i := 1; i < s.partitions; i++ {
		f := atomic.LoadInt64(s.partitionFiles(i))
		if f > maxFiles {
			maxFiles = f
			maxPartition = i
		}
	}
	if maxFiles <= 0 {
		return
	}

	s.evictPartition(maxPartition)
}

type cachedFileInfo struct {
	path      string
	size      int64
	mtime     time.Time
	prefixIdx int
}

// evictPartition scans all subdirectories in the given partition, sorts files
// by mtime (oldest first), and deletes until the partition's file count drops
// to 0.9 * totalFiles / P (matching ccache's file-count-based eviction).
func (s *SizeLimitStore) evictPartition(partition int) {
	subdirsPerPartition := 65536 / s.partitions
	startIdx := partition * subdirsPerPartition
	endIdx := startIdx + subdirsPerPartition

	chunkExt := CompressedChunkExt
	if s.ls.Opt.Uncompressed {
		chunkExt = UncompressedChunkExt
	}

	// Collect all chunk files in this partition.
	var files []cachedFileInfo
	for idx := startIdx; idx < endIdx; idx++ {
		if !s.hasBitmask(idx) {
			continue
		}
		prefix := fmt.Sprintf("%04x", idx)
		dir := filepath.Join(s.ls.Base, prefix)
		entries, err := os.ReadDir(dir)
		if err != nil {
			// Directory doesn't exist or can't be read — clear bitmask.
			s.clearBitmask(idx)
			continue
		}
		hasFiles := false
		now := time.Now()
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()

			// Clean up stale temporary files (orphaned by crashed writes).
			if strings.HasPrefix(name, tmpChunkPrefix) {
				if info, err := e.Info(); err == nil && now.Sub(info.ModTime()) > time.Hour {
					_ = os.Remove(filepath.Join(dir, name))
				}
				continue
			}

			// Only consider chunk files (skip .prunable, .protect, etc.)
			if chunkExt == "" {
				// Uncompressed: no extension, but skip files with known marker extensions.
				if strings.HasSuffix(name, PrunableExt) || strings.HasSuffix(name, ProtectExt) {
					continue
				}
			} else {
				if !strings.HasSuffix(name, chunkExt) || strings.HasSuffix(name, PrunableExt) || strings.HasSuffix(name, ProtectExt) {
					continue
				}
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			hasFiles = true
			files = append(files, cachedFileInfo{
				path:      filepath.Join(dir, name),
				size:      info.Size(),
				mtime:     info.ModTime(),
				prefixIdx: idx,
			})
		}
		if !hasFiles {
			s.clearBitmask(idx)
		}
	}

	if len(files) == 0 {
		return
	}

	// Sort by mtime ascending (oldest first).
	sort.Slice(files, func(i, j int) bool {
		return files[i].mtime.Before(files[j].mtime)
	})

	// Eviction target: trim partition file count to 90% of fair share
	// (matching ccache: 0.9 * total_files / P).
	targetFiles := atomic.LoadInt64(s.globalFiles()) * 9 / 10 / int64(s.partitions)
	currentFiles := atomic.LoadInt64(s.partitionFiles(partition))

	for _, f := range files {
		if currentFiles <= targetFiles {
			break
		}
		if err := os.Remove(f.path); err != nil {
			continue // file may be in use (Windows) or already deleted
		}
		atomic.AddInt64(s.globalSize(), -f.size)
		atomic.AddInt64(s.globalFiles(), -1)
		atomic.AddInt64(s.partitionSize(partition), -f.size)
		atomic.AddInt64(s.partitionFiles(partition), -1)
		currentFiles--
	}
}

// --- Initialization ---

// initCounters scans existing cache content and populates the mmap'd
// counters and bitmask. Called only when the tracking file is first created
// (or recreated after a partition count change).
func (s *SizeLimitStore) initCounters() error {
	// List only existing subdirectories (empty cache = fast return).
	topEntries, err := os.ReadDir(s.ls.Base)
	if err != nil {
		return err
	}

	chunkExt := CompressedChunkExt
	if s.ls.Opt.Uncompressed {
		chunkExt = UncompressedChunkExt
	}

	for _, te := range topEntries {
		if !te.IsDir() {
			continue
		}
		name := te.Name()
		if len(name) != 4 {
			continue
		}
		// Parse the 4-hex-char prefix.
		var idx int
		if _, err := fmt.Sscanf(name, "%04x", &idx); err != nil {
			continue
		}
		if idx < 0 || idx >= 65536 {
			continue
		}

		dir := filepath.Join(s.ls.Base, name)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		var dirSize int64
		var dirFiles int64
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			eName := e.Name()
			if chunkExt == "" {
				if strings.HasSuffix(eName, PrunableExt) || strings.HasSuffix(eName, ProtectExt) {
					continue
				}
			} else {
				if !strings.HasSuffix(eName, chunkExt) {
					continue
				}
				if strings.HasSuffix(eName, PrunableExt) || strings.HasSuffix(eName, ProtectExt) {
					continue
				}
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			dirSize += info.Size()
			dirFiles++
		}

		if dirFiles > 0 {
			s.setBitmask(idx)
			partition := idx / (65536 / s.partitions)
			atomic.AddInt64(s.globalSize(), dirSize)
			atomic.AddInt64(s.globalFiles(), dirFiles)
			atomic.AddInt64(s.partitionSize(partition), dirSize)
			atomic.AddInt64(s.partitionFiles(partition), dirFiles)
		}
	}
	return nil
}
