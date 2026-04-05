// Package cacheevict provides automatic LRU cache eviction for disk-based
// caches. It tracks total cache size and file count via a persistent mmap'd
// file with lock-free atomic counters, enabling safe coordination between
// multiple concurrent processes.
//
// The eviction algorithm uses max_size/max_files as trigger conditions,
// while the actual eviction target is file-count-based (trim the largest
// partition to 90% of its fair share). This avoids pathological behavior
// with mixed file sizes and amortizes scan cost.
//
// See the README for the full design rationale and mmap file layout.
package cacheevict

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

const (
	// trackingFileName is the name of the persistent mmap'd file.
	trackingFileName = ".cache-sizes"

	// File format version. Major version change = incompatible layout.
	majorVersion = 0
	minorVersion = 1

	// Default values.
	DefaultPartitions  = 256
	DefaultPrefixCount = 65536

	// Mmap layout offsets.
	//
	//   Header (1024 bytes, padded for future fields):
	//     [0]    int32   major version
	//     [4]    int32   minor version
	//     [8]    int32   eviction lock (PID)
	//     [12]   int32   prefix count
	//     [16]   int32   partition count
	//     [20]   int32   (reserved)
	//     [24]   int64   total size in bytes
	//     [32]   int64   total file count
	//     [40]   int64   hit count
	//     [48]   int64   miss count
	//     [56..1023]     reserved (zero)
	//
	//   Bitmask (PrefixCount/8 bytes):
	//     [1024]  PrefixCount bits
	//
	//   Per-partition (P × 16 bytes):
	//     [1024 + PrefixCount/8 + p*16]      int64  partition size
	//     [1024 + PrefixCount/8 + p*16 + 8]  int64  partition files
	offMajorVersion  = 0
	offMinorVersion  = 4
	offEvictionLock  = 8
	offPrefixCount   = 12
	offPartitionCount = 16
	// offReserved32  = 20
	offTotalSize     = 24
	offTotalFiles    = 32
	offHits          = 40
	offMisses        = 48
	headerSize       = 1024

	bitmaskOffset   = headerSize // 1024
	partitionStride = 16         // 2 × int64 per partition

	// staleTempAge is the minimum age for temp files to be cleaned up.
	staleTempAge = time.Hour
)

// Config configures the eviction handler.
type Config struct {
	// BaseDir is the root directory of the cache.
	BaseDir string

	// MaxSize is the maximum total size in bytes. 0 = no byte limit.
	MaxSize int64

	// MaxFiles is the maximum number of cached files. 0 = no file limit.
	MaxFiles int64

	// Partitions is the number of eviction partitions (power of 2).
	// Default: 256.
	Partitions int

	// PrefixCount is the total number of subdirectory prefix slots (power
	// of 2). Default: 65536. Determines bitmask size and maximum
	// granularity.
	PrefixCount int

	// SubdirPath returns the relative path from BaseDir for a given prefix
	// index (0 to PrefixCount-1). Used by the eviction scanner.
	SubdirPath func(prefixIdx int) string

	// FilePrefix returns the prefix index (0 to PrefixCount-1) for a file
	// given its path relative to BaseDir.
	FilePrefix func(relPath string) int

	// IsCachedFile returns true if a directory entry name represents a
	// cached file eligible for eviction and size tracking.
	IsCachedFile func(name string) bool

	// IsTempFile returns true if a directory entry name is a stale
	// temporary file eligible for cleanup. During eviction, temp files
	// older than 1 hour are deleted. nil disables temp cleanup.
	IsTempFile func(name string) bool
}

// Handler tracks cache size and file count via persistent shared memory
// and performs automatic LRU eviction when limits are exceeded.
// Safe for concurrent use by multiple goroutines and processes.
type Handler struct {
	cfg         Config
	data        []byte // mmap'd persistent region
	pid         int32
	prefixCount int
	partitions  int
	bitmaskSize int // bytes
	counterOff  int // offset to partition counters
}

// Open creates or opens the eviction handler. The persistent tracking
// file is stored as BaseDir/.cache-sizes.
func Open(cfg Config) (*Handler, error) {
	if cfg.Partitions <= 0 {
		cfg.Partitions = DefaultPartitions
	}
	if cfg.PrefixCount <= 0 {
		cfg.PrefixCount = DefaultPrefixCount
	}
	if cfg.PrefixCount < 1 || bits.OnesCount(uint(cfg.PrefixCount)) != 1 {
		return nil, fmt.Errorf("cacheevict: PrefixCount must be a power of 2, got %d", cfg.PrefixCount)
	}
	if cfg.Partitions < 1 || cfg.Partitions > cfg.PrefixCount || bits.OnesCount(uint(cfg.Partitions)) != 1 {
		return nil, fmt.Errorf("cacheevict: Partitions must be a power of 2 between 1 and PrefixCount (%d), got %d", cfg.PrefixCount, cfg.Partitions)
	}
	if cfg.SubdirPath == nil || cfg.FilePrefix == nil || cfg.IsCachedFile == nil {
		return nil, fmt.Errorf("cacheevict: SubdirPath, FilePrefix, and IsCachedFile must be set")
	}

	bmSize := cfg.PrefixCount / 8
	counterOff := bitmaskOffset + bmSize
	fileSize := counterOff + cfg.Partitions*partitionStride
	path := filepath.Join(cfg.BaseDir, trackingFileName)

	data, isNew, err := openCacheSizesFile(path, fileSize)
	if err != nil {
		return nil, fmt.Errorf("cacheevict: open tracking file: %w", err)
	}

	h := &Handler{
		cfg:         cfg,
		data:        data,
		pid:         int32(os.Getpid()),
		prefixCount: cfg.PrefixCount,
		partitions:  cfg.Partitions,
		bitmaskSize: bmSize,
		counterOff:  counterOff,
	}

	if isNew {
		// Zero the header region (may contain stale data from a
		// previous file with a different layout/size).
		for i := range headerSize {
			h.data[i] = 0
		}
		h.writeHeader()
		if err := h.initCounters(); err != nil {
			closeCacheSizesFile(data)
			return nil, fmt.Errorf("cacheevict: init counters: %w", err)
		}
	} else {
		// Validate version.
		storedMajor := atomic.LoadInt32(h.ptrInt32(offMajorVersion))
		if storedMajor != majorVersion {
			closeCacheSizesFile(data)
			return nil, fmt.Errorf("cacheevict: incompatible tracking file version %d (expected %d)", storedMajor, majorVersion)
		}

		// Check if prefix count or partition count changed → recreate.
		storedPrefix := atomic.LoadInt32(h.ptrInt32(offPrefixCount))
		storedPart := atomic.LoadInt32(h.ptrInt32(offPartitionCount))
		if int(storedPrefix) != cfg.PrefixCount || int(storedPart) != cfg.Partitions {
			closeCacheSizesFile(data)
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("cacheevict: remove stale tracking file: %w", err)
			}
			data, _, err = openCacheSizesFile(path, fileSize)
			if err != nil {
				return nil, fmt.Errorf("cacheevict: recreate tracking file: %w", err)
			}
			h.data = data
			h.writeHeader()
			if err := h.initCounters(); err != nil {
				closeCacheSizesFile(data)
				return nil, fmt.Errorf("cacheevict: init counters after recreate: %w", err)
			}
		}
	}

	return h, nil
}

// writeHeader stores version, prefix count, and partition count.
func (h *Handler) writeHeader() {
	atomic.StoreInt32(h.ptrInt32(offMajorVersion), majorVersion)
	atomic.StoreInt32(h.ptrInt32(offMinorVersion), minorVersion)
	atomic.StoreInt32(h.ptrInt32(offPrefixCount), int32(h.prefixCount))
	atomic.StoreInt32(h.ptrInt32(offPartitionCount), int32(h.partitions))
}

// Close flushes counters to disk and releases the mmap'd region.
func (h *Handler) Close() error {
	if h.data != nil {
		_ = syncCacheSizesFile(h.data)
		err := closeCacheSizesFile(h.data)
		h.data = nil
		return err
	}
	return nil
}

// --- mmap layout accessors ---

func (h *Handler) ptrInt32(off int) *int32 {
	return (*int32)(unsafe.Pointer(&h.data[off]))
}

func (h *Handler) ptrInt64(off int) *int64 {
	return (*int64)(unsafe.Pointer(&h.data[off]))
}

func (h *Handler) evictionLock() *int32  { return h.ptrInt32(offEvictionLock) }
func (h *Handler) globalSize() *int64    { return h.ptrInt64(offTotalSize) }
func (h *Handler) globalFiles() *int64   { return h.ptrInt64(offTotalFiles) }
func (h *Handler) hitCounter() *int64    { return h.ptrInt64(offHits) }
func (h *Handler) missCounter() *int64   { return h.ptrInt64(offMisses) }

func (h *Handler) partitionSize(p int) *int64 {
	return (*int64)(unsafe.Pointer(&h.data[h.counterOff+p*partitionStride]))
}

func (h *Handler) partitionFiles(p int) *int64 {
	return (*int64)(unsafe.Pointer(&h.data[h.counterOff+p*partitionStride+8]))
}

func (h *Handler) bitmaskWord(prefixIdx int) *uint32 {
	wordIdx := prefixIdx / 32
	return (*uint32)(unsafe.Pointer(&h.data[bitmaskOffset+wordIdx*4]))
}

func (h *Handler) setBitmask(prefixIdx int) {
	atomic.OrUint32(h.bitmaskWord(prefixIdx), 1<<uint(prefixIdx%32))
}

func (h *Handler) hasBitmask(prefixIdx int) bool {
	return atomic.LoadUint32(h.bitmaskWord(prefixIdx))&(1<<uint(prefixIdx%32)) != 0
}

func (h *Handler) clearBitmask(prefixIdx int) {
	atomic.AndUint32(h.bitmaskWord(prefixIdx), ^(1<<uint(prefixIdx%32)))
}

// --- partition helpers ---

func (h *Handler) partitionOf(prefixIdx int) int {
	return prefixIdx / (h.prefixCount / h.partitions)
}

func (h *Handler) prefixFromPath(path string) int {
	rel, err := filepath.Rel(h.cfg.BaseDir, path)
	if err != nil {
		return 0
	}
	return h.cfg.FilePrefix(filepath.ToSlash(rel))
}

// --- Public API ---

// SetMaxSize updates the maximum cache size limit.
func (h *Handler) SetMaxSize(maxSize int64) {
	h.cfg.MaxSize = maxSize
}

// SetMaxFiles updates the maximum file count limit.
func (h *Handler) SetMaxFiles(maxFiles int64) {
	h.cfg.MaxFiles = maxFiles
}

// TotalSize returns the current tracked total cache size in bytes.
func (h *Handler) TotalSize() int64 {
	return atomic.LoadInt64(h.globalSize())
}

// TotalFiles returns the current tracked total file count.
func (h *Handler) TotalFiles() int64 {
	return atomic.LoadInt64(h.globalFiles())
}

// Hits returns the number of cache hits (UseFile calls) since last reset.
func (h *Handler) Hits() int64 {
	return atomic.LoadInt64(h.hitCounter())
}

// Misses returns the number of cache misses (new files stored via
// AfterStore) since last reset.
func (h *Handler) Misses() int64 {
	return atomic.LoadInt64(h.missCounter())
}

// HitRatio returns hits / (hits + misses), or 0 if no requests have been
// recorded.
func (h *Handler) HitRatio() float64 {
	hits := h.Hits()
	misses := h.Misses()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// ResetStats zeroes the hit and miss counters.
func (h *Handler) ResetStats() {
	atomic.StoreInt64(h.hitCounter(), 0)
	atomic.StoreInt64(h.missCounter(), 0)
}

// BeforeStore stats the file at path and returns its current size (0 if
// absent). Call this before writing the file, then pass the result to
// AfterStore.
func (h *Handler) BeforeStore(path string) int64 {
	if info, err := os.Stat(path); err == nil {
		return info.Size()
	}
	return 0
}

// AfterStore stats the file at path to get the new size, computes the
// delta against oldSize (from BeforeStore), atomically updates all
// counters, sets the bitmask bit, and triggers eviction if over limit.
// If oldSize is 0 (new file), increments the miss counter.
func (h *Handler) AfterStore(path string, oldSize int64) {
	var newSize int64
	if info, err := os.Stat(path); err == nil {
		newSize = info.Size()
	}

	prefixIdx := h.prefixFromPath(path)
	partition := h.partitionOf(prefixIdx)

	sizeDelta := newSize - oldSize
	isNew := oldSize == 0

	if sizeDelta != 0 {
		atomic.AddInt64(h.globalSize(), sizeDelta)
		atomic.AddInt64(h.partitionSize(partition), sizeDelta)
	}
	if isNew {
		atomic.AddInt64(h.globalFiles(), 1)
		atomic.AddInt64(h.partitionFiles(partition), 1)
		atomic.AddInt64(h.missCounter(), 1)
	}

	h.setBitmask(prefixIdx)
	h.maybeEvict()
}

// BeforeRemove stats the file at path and immediately decrements all
// counters. Call this before deleting the file. No AfterRemove is
// needed — if the remove fails, the counter is slightly off but this
// is acceptable for approximate LRU.
func (h *Handler) BeforeRemove(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return // file doesn't exist, nothing to track
	}

	prefixIdx := h.prefixFromPath(path)
	partition := h.partitionOf(prefixIdx)

	atomic.AddInt64(h.globalSize(), -info.Size())
	atomic.AddInt64(h.globalFiles(), -1)
	atomic.AddInt64(h.partitionSize(partition), -info.Size())
	atomic.AddInt64(h.partitionFiles(partition), -1)
}

// UseFile marks a cached file as recently used for LRU tracking.
// Currently updates the file's mtime to now. Best-effort, errors ignored.
// Increments the hit counter.
func (h *Handler) UseFile(path string) {
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	atomic.AddInt64(h.hitCounter(), 1)
}

// --- Eviction ---

func (h *Handler) needsEviction() bool {
	if h.cfg.MaxSize > 0 && h.TotalSize() > h.cfg.MaxSize {
		return true
	}
	if h.cfg.MaxFiles > 0 && h.TotalFiles() > h.cfg.MaxFiles {
		return true
	}
	return false
}

func (h *Handler) maybeEvict() {
	if h.cfg.MaxSize <= 0 && h.cfg.MaxFiles <= 0 {
		return
	}
	if !h.needsEviction() {
		return
	}

	// Non-blocking CAS on eviction lock.
	if !atomic.CompareAndSwapInt32(h.evictionLock(), 0, h.pid) {
		holder := atomic.LoadInt32(h.evictionLock())
		if holder == h.pid {
			return // another goroutine in this process is already evicting
		}
		if holder != 0 && processAlive(holder) {
			return // another live process is evicting
		}
		// Stale lock from a dead process — try to reclaim.
		if !atomic.CompareAndSwapInt32(h.evictionLock(), holder, h.pid) {
			return
		}
	}
	defer atomic.StoreInt32(h.evictionLock(), 0)

	// Pick partition with the most files.
	maxPartition := 0
	maxFiles := atomic.LoadInt64(h.partitionFiles(0))
	for i := 1; i < h.partitions; i++ {
		f := atomic.LoadInt64(h.partitionFiles(i))
		if f > maxFiles {
			maxFiles = f
			maxPartition = i
		}
	}
	if maxFiles <= 0 {
		return
	}

	h.evictPartition(maxPartition)
}

type fileEntry struct {
	path      string
	size      int64
	mtime     time.Time
	prefixIdx int
}

func (h *Handler) evictPartition(partition int) {
	prefixesPerPartition := h.prefixCount / h.partitions
	startIdx := partition * prefixesPerPartition
	endIdx := startIdx + prefixesPerPartition

	var files []fileEntry
	for idx := startIdx; idx < endIdx; idx++ {
		if !h.hasBitmask(idx) {
			continue
		}
		subdir := filepath.Join(h.cfg.BaseDir, h.cfg.SubdirPath(idx))
		entries, err := os.ReadDir(subdir)
		if err != nil {
			h.clearBitmask(idx)
			continue
		}
		hasFiles := false
		now := time.Now()
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()

			// Clean up stale temp files.
			if h.cfg.IsTempFile != nil && h.cfg.IsTempFile(name) {
				if info, err := e.Info(); err == nil && now.Sub(info.ModTime()) > staleTempAge {
					_ = os.Remove(filepath.Join(subdir, name))
				}
				continue
			}

			if !h.cfg.IsCachedFile(name) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			hasFiles = true
			files = append(files, fileEntry{
				path:      filepath.Join(subdir, name),
				size:      info.Size(),
				mtime:     info.ModTime(),
				prefixIdx: idx,
			})
		}
		if !hasFiles {
			h.clearBitmask(idx)
		}
	}

	// The actual file listing gives us the true partition state, which may
	// differ from the counters if files were deleted externally. We
	// reconcile the counters from the listing rather than decrementing
	// per-file. This self-heals counter drift.
	var actualSize int64
	var actualFiles int64
	for _, f := range files {
		actualSize += f.size
		actualFiles++
	}

	if actualFiles == 0 {
		// Reconcile: partition is empty but counters may be stale.
		oldSize := atomic.SwapInt64(h.partitionSize(partition), 0)
		oldFiles := atomic.SwapInt64(h.partitionFiles(partition), 0)
		atomic.AddInt64(h.globalSize(), -oldSize)
		atomic.AddInt64(h.globalFiles(), -oldFiles)
		return
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].mtime.Before(files[j].mtime)
	})

	// Eviction target: trim partition to 90% of fair share by file count.
	targetFiles := atomic.LoadInt64(h.globalFiles()) * 9 / 10 / int64(h.partitions)

	remainingSize := actualSize
	remainingFiles := actualFiles
	for _, f := range files {
		if remainingFiles <= targetFiles {
			break
		}
		if err := os.Remove(f.path); err != nil {
			continue // file may be in use (Windows) or already deleted
		}
		remainingSize -= f.size
		remainingFiles--
	}

	// Reconcile partition counters with actual state after eviction.
	// Update globals by the delta between old counter and new actual value.
	oldPartSize := atomic.SwapInt64(h.partitionSize(partition), remainingSize)
	oldPartFiles := atomic.SwapInt64(h.partitionFiles(partition), remainingFiles)
	atomic.AddInt64(h.globalSize(), remainingSize-oldPartSize)
	atomic.AddInt64(h.globalFiles(), remainingFiles-oldPartFiles)
}

// --- Initialization ---

// initCounters walks the cache directory tree and populates all counters
// and the bitmask. Called only when the tracking file is first created.
// Cost is O(N) in the number of actual files, not O(PrefixCount).
func (h *Handler) initCounters() error {
	return filepath.WalkDir(h.cfg.BaseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			// Skip hidden directories (like .cache-sizes parent).
			if d.Name() != filepath.Base(h.cfg.BaseDir) && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !h.cfg.IsCachedFile(d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}

		prefixIdx := h.prefixFromPath(path)
		partition := h.partitionOf(prefixIdx)

		h.setBitmask(prefixIdx)
		atomic.AddInt64(h.globalSize(), info.Size())
		atomic.AddInt64(h.globalFiles(), 1)
		atomic.AddInt64(h.partitionSize(partition), info.Size())
		atomic.AddInt64(h.partitionFiles(partition), 1)
		return nil
	})
}
