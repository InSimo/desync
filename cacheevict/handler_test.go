package cacheevict

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// desyncConfig returns a Config mimicking desync's flat 4-hex-char layout.
func desyncConfig(dir string, maxSize, maxFiles int64, partitions int) Config {
	return Config{
		BaseDir:    dir,
		MaxSize:    maxSize,
		MaxFiles:   maxFiles,
		Partitions: partitions,
		SubdirPath: func(idx int) string { return fmt.Sprintf("%04x", idx) },
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
			return strings.HasSuffix(name, ".cacnk") &&
				!strings.HasSuffix(name, ".prunable") &&
				!strings.HasSuffix(name, ".protect")
		},
		IsTempFile: func(name string) bool {
			return strings.HasPrefix(name, ".tmp-cacnk")
		},
	}
}

// lfsConfig returns a Config mimicking git-lfs's nested ab/cd/ layout.
func lfsConfig(dir string, maxSize, maxFiles int64, partitions int) Config {
	return Config{
		BaseDir:    dir,
		MaxSize:    maxSize,
		MaxFiles:   maxFiles,
		Partitions: partitions,
		SubdirPath: func(idx int) string {
			return fmt.Sprintf("%02x/%02x", idx>>8, idx&0xff)
		},
		FilePrefix: func(relPath string) int {
			parts := strings.Split(relPath, "/")
			if len(parts) < 2 {
				return 0
			}
			var a, b int
			fmt.Sscanf(parts[0], "%02x", &a)
			fmt.Sscanf(parts[1], "%02x", &b)
			return a*256 + b
		},
		IsCachedFile: func(name string) bool {
			return len(name) == 64 && isHex(name)
		},
	}
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// storeDesyncChunk writes a fake chunk file in desync layout, using the
// handler's BeforeStore/AfterStore flow.
func storeDesyncChunk(t *testing.T, h *Handler, dir string, size int) string {
	t.Helper()
	data := make([]byte, 32)
	rand.Read(data)
	id := hex.EncodeToString(data)
	prefix := id[:4]
	subdir := filepath.Join(dir, prefix)
	os.MkdirAll(subdir, 0755)
	path := filepath.Join(subdir, id+".cacnk")

	oldSize := h.BeforeStore(path)
	content := make([]byte, size)
	rand.Read(content)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	h.AfterStore(path, oldSize)
	return path
}

// storeDesyncChunkRaw writes a fake chunk without handler tracking (for cold start tests).
func storeDesyncChunkRaw(t *testing.T, dir string, size int) string {
	t.Helper()
	data := make([]byte, 32)
	rand.Read(data)
	id := hex.EncodeToString(data)
	prefix := id[:4]
	subdir := filepath.Join(dir, prefix)
	os.MkdirAll(subdir, 0755)
	path := filepath.Join(subdir, id+".cacnk")
	content := make([]byte, size)
	rand.Read(content)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// storeLFSObject writes a fake LFS object using the handler's flow.
func storeLFSObject(t *testing.T, h *Handler, dir string, size int) string {
	t.Helper()
	data := make([]byte, 32)
	rand.Read(data)
	oid := hex.EncodeToString(data)
	subdir := filepath.Join(dir, oid[:2], oid[2:4])
	os.MkdirAll(subdir, 0755)
	path := filepath.Join(subdir, oid)

	oldSize := h.BeforeStore(path)
	content := make([]byte, size)
	rand.Read(content)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	h.AfterStore(path, oldSize)
	return path
}

// storeLFSObjectRaw writes a fake LFS object without handler tracking (for cold start tests).
func storeLFSObjectRaw(t *testing.T, dir string, size int) string {
	t.Helper()
	data := make([]byte, 32)
	rand.Read(data)
	oid := hex.EncodeToString(data)
	subdir := filepath.Join(dir, oid[:2], oid[2:4])
	os.MkdirAll(subdir, 0755)
	path := filepath.Join(subdir, oid)
	content := make([]byte, size)
	rand.Read(content)
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertEqual(t *testing.T, got, want int64, msg string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %d, want %d", msg, got, want)
	}
}

func assertGreater(t *testing.T, got, threshold int64, msg string) {
	t.Helper()
	if got <= threshold {
		t.Errorf("%s: got %d, want > %d", msg, got, threshold)
	}
}

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBeforeAfterStore(t *testing.T) {
	dir := t.TempDir()
	cfg := desyncConfig(dir, 100*1024*1024, 0, 16)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Store a new file via the helper (which calls BeforeStore/AfterStore).
	path := storeDesyncChunk(t, h, dir, 512)
	assertEqual(t, h.TotalFiles(), 1, "new file should add 1")
	assertEqual(t, h.TotalSize(), 512, "size should be 512")

	// Overwrite the same file — should not change file count.
	oldSize := h.BeforeStore(path)
	os.WriteFile(path, make([]byte, 512), 0644)
	h.AfterStore(path, oldSize)
	assertEqual(t, h.TotalFiles(), 1, "overwrite should not add files")
}

func TestBeforeRemove(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	filePath := storeDesyncChunk(t, h, dir, 256)
	assertEqual(t, h.TotalFiles(), 1, "after store")
	assertGreater(t, h.TotalSize(), 0, "after store")

	// Remove it.
	h.BeforeRemove(filePath)
	os.Remove(filePath)
	assertEqual(t, h.TotalFiles(), 0, "after remove")
	assertEqual(t, h.TotalSize(), 0, "after remove")
}

func TestUseFile(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	path := storeDesyncChunk(t, h, dir, 512)
	info1, _ := os.Stat(path)
	mtime1 := info1.ModTime()

	time.Sleep(50 * time.Millisecond)
	h.UseFile(path)

	info2, _ := os.Stat(path)
	mtime2 := info2.ModTime()
	if !mtime2.After(mtime1) {
		t.Errorf("UseFile should update mtime: before=%v after=%v", mtime1, mtime2)
	}
}

func TestEvictionBySize(t *testing.T) {
	dir := t.TempDir()
	cfg := desyncConfig(dir, 500, 0, 4)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for range 50 {
		storeDesyncChunk(t, h, dir, 256)
	}

	// Should have evicted down near the limit.
	if h.TotalSize() > 600 {
		t.Errorf("totalSize %d should be near or below limit 500", h.TotalSize())
	}
}

func TestEvictionByFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := desyncConfig(dir, 0, 10, 1)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for range 20 {
		storeDesyncChunk(t, h, dir, 64)
	}

	if h.TotalFiles() > 11 {
		t.Errorf("totalFiles %d should be near or below max-files 10", h.TotalFiles())
	}
}

func TestEvictionLRU(t *testing.T) {
	dir := t.TempDir()
	cfg := desyncConfig(dir, 100*1024*1024, 0, 1)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Store "old" files.
	var oldPaths []string
	for range 5 {
		path := storeDesyncChunk(t, h, dir, 128)
		oldPaths = append(oldPaths, path)
	}

	time.Sleep(100 * time.Millisecond)

	// Store "new" files (newer mtime).
	var newPaths []string
	for range 5 {
		path := storeDesyncChunk(t, h, dir, 128)
		newPaths = append(newPaths, path)
	}

	time.Sleep(100 * time.Millisecond)

	// Touch old files (mtime becomes newest).
	for _, p := range oldPaths {
		h.UseFile(p)
	}

	// Force eviction by lowering limit.
	h.cfg.MaxSize = 200
	storeDesyncChunk(t, h, dir, 128)

	evictedNew := 0
	evictedOld := 0
	for _, p := range newPaths {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			evictedNew++
		}
	}
	for _, p := range oldPaths {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			evictedOld++
		}
	}

	if evictedNew <= evictedOld {
		t.Errorf("LRU: untouched new (%d evicted) should be evicted more than touched old (%d evicted)",
			evictedNew, evictedOld)
	}
}

func TestColdStart(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate without handler.
	var totalSize int64
	for range 10 {
		path := storeDesyncChunkRaw(t, dir, 512)
		info, _ := os.Stat(path)
		totalSize += info.Size()
	}

	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	assertEqual(t, h.TotalSize(), totalSize, "cold start size")
	assertEqual(t, h.TotalFiles(), 10, "cold start files")
}

func TestLFSLayout(t *testing.T) {
	dir := t.TempDir()
	cfg := lfsConfig(dir, 100*1024*1024, 0, 16)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Store LFS-style objects.
	for range 10 {
		storeLFSObject(t, h, dir, 1024)
	}

	assertEqual(t, h.TotalFiles(), 10, "lfs file count")
	assertGreater(t, h.TotalSize(), 0, "lfs total size")
}

func TestLFSColdStart(t *testing.T) {
	dir := t.TempDir()

	var totalSize int64
	for range 5 {
		path := storeLFSObjectRaw(t, dir, 256)
		info, _ := os.Stat(path)
		totalSize += info.Size()
	}

	cfg := lfsConfig(dir, 100*1024*1024, 0, 16)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	assertEqual(t, h.TotalSize(), totalSize, "lfs cold start size")
	assertEqual(t, h.TotalFiles(), 5, "lfs cold start files")
}

func TestConcurrency(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	var wg sync.WaitGroup
	var errCount atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				storeDesyncChunk(t, h, dir, 256)
				if h.TotalSize() < 0 {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if errCount.Load() != 0 {
		t.Errorf("negative size detected %d times", errCount.Load())
	}
	assertGreater(t, h.TotalSize(), 0, "concurrent writes")
	assertGreater(t, h.TotalFiles(), 0, "concurrent writes files")
}

func TestPartitionMismatch(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	storeDesyncChunk(t, h, dir, 512)
	h.Close()

	// Reopen with different partitions — should rescan.
	h2, err := Open(desyncConfig(dir, 100*1024*1024, 0, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	assertGreater(t, h2.TotalSize(), 0, "rescan after partition change")
}

func TestStaleTempCleanup(t *testing.T) {
	dir := t.TempDir()
	// Start with no limit so nothing is evicted during setup.
	cfg := desyncConfig(dir, 0, 0, 1)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Store chunks (no eviction yet).
	var subdirWithChunks string
	for range 50 {
		path := storeDesyncChunk(t, h, dir, 256)
		if subdirWithChunks == "" {
			subdirWithChunks = filepath.Dir(path)
		}
	}

	// Plant a stale temp file in a subdir that has chunks.
	tmpPath := filepath.Join(subdirWithChunks, ".tmp-cacnk123456")
	os.WriteFile(tmpPath, []byte("stale"), 0644)
	old := time.Now().Add(-2 * time.Hour)
	os.Chtimes(tmpPath, old, old)

	// Now enable a small limit and trigger eviction.
	h.cfg.MaxSize = 500
	storeDesyncChunk(t, h, dir, 256)

	// Stale temp file should have been cleaned up during eviction.
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("stale temp file should have been deleted")
	}
}

func TestDisabled(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for range 50 {
		storeDesyncChunk(t, h, dir, 1024)
	}

	// No eviction should have happened.
	assertEqual(t, h.TotalFiles(), 50, "disabled: all files remain")
}

func TestInvalidPartitions(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []int{3, 5, 7, 15, 100} {
		cfg := desyncConfig(dir, 1024, 0, p)
		_, err := Open(cfg)
		if err == nil {
			t.Errorf("partitions=%d should be invalid", p)
		}
	}
}
