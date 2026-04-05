// Package cacheevict provides automatic LRU cache eviction for disk-based
// caches. It tracks total cache size and file count via a persistent mmap'd
// file with lock-free atomic counters, enabling safe coordination between
// multiple concurrent processes.
//
// The eviction algorithm is inspired by ccache: max_size/max_files are used
// as trigger conditions, while the actual eviction target is file-count-based
// (trim the largest partition to 0.9 * totalFiles / P). This avoids
// pathological behavior with mixed file sizes and amortizes scan cost.
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

	// Default values.
	DefaultPartitions  = 256
	DefaultPrefixCount = 65536

	// Mmap layout offsets.
	headerSize      = 24
	bitmaskOffset   = headerSize                   // 24
	partitionStride = 16 // 2 × int64 per partition

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
	counterOff := headerSize + bmSize
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
		atomic.StoreInt32(h.storedPartitionCount(), int32(cfg.Partitions))
		if err := h.initCounters(); err != nil {
			closeCacheSizesFile(data)
			return nil, fmt.Errorf("cacheevict: init counters: %w", err)
		}
	} else {
		stored := atomic.LoadInt32(h.storedPartitionCount())
		if int(stored) != cfg.Partitions {
			closeCacheSizesFile(data)
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("cacheevict: remove stale tracking file: %w", err)
			}
			data, _, err = openCacheSizesFile(path, fileSize)
			if err != nil {
				return nil, fmt.Errorf("cacheevict: recreate tracking file: %w", err)
			}
			h.data = data
			atomic.StoreInt32(h.storedPartitionCount(), int32(cfg.Partitions))
			if err := h.initCounters(); err != nil {
				closeCacheSizesFile(data)
				return nil, fmt.Errorf("cacheevict: init counters after recreate: %w", err)
			}
		}
	}

	return h, nil
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

func (h *Handler) evictionLock() *int32 {
	return (*int32)(unsafe.Pointer(&h.data[0]))
}

func (h *Handler) storedPartitionCount() *int32 {
	return (*int32)(unsafe.Pointer(&h.data[4]))
}

func (h *Handler) globalSize() *int64 {
	return (*int64)(unsafe.Pointer(&h.data[8]))
}

func (h *Handler) globalFiles() *int64 {
	return (*int64)(unsafe.Pointer(&h.data[16]))
}

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

// TotalSize returns the current tracked total cache size in bytes.
func (h *Handler) TotalSize() int64 {
	return atomic.LoadInt64(h.globalSize())
}

// TotalFiles returns the current tracked total file count.
func (h *Handler) TotalFiles() int64 {
	return atomic.LoadInt64(h.globalFiles())
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
func (h *Handler) UseFile(path string) {
	now := time.Now()
	_ = os.Chtimes(path, now, now)
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
		if holder != 0 && processAlive(holder) {
			return
		}
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

	if len(files) == 0 {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].mtime.Before(files[j].mtime)
	})

	// Eviction target: 0.9 * totalFiles / P (matching ccache).
	targetFiles := atomic.LoadInt64(h.globalFiles()) * 9 / 10 / int64(h.partitions)
	currentFiles := atomic.LoadInt64(h.partitionFiles(partition))

	for _, f := range files {
		if currentFiles <= targetFiles {
			break
		}
		if err := os.Remove(f.path); err != nil {
			continue
		}
		atomic.AddInt64(h.globalSize(), -f.size)
		atomic.AddInt64(h.globalFiles(), -1)
		atomic.AddInt64(h.partitionSize(partition), -f.size)
		atomic.AddInt64(h.partitionFiles(partition), -1)
		currentFiles--
	}
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
