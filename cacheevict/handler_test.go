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
	assertEqual(t, h.Hits(), 0, "initial hits")
	assertEqual(t, h.Misses(), 0, "initial misses")
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBeforeAfterStore(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Store a new file (miss).
	path := storeDesyncChunk(t, h, dir, 512)
	assertEqual(t, h.TotalFiles(), 1, "new file should add 1")
	assertEqual(t, h.TotalSize(), 512, "size should be 512")
	assertEqual(t, h.Misses(), 1, "new file = 1 miss")
	assertEqual(t, h.Hits(), 0, "no hits yet")

	// Overwrite the same file — not a miss (oldSize != 0).
	oldSize := h.BeforeStore(path)
	os.WriteFile(path, make([]byte, 512), 0644)
	h.AfterStore(path, oldSize)
	assertEqual(t, h.TotalFiles(), 1, "overwrite should not add files")
	assertEqual(t, h.Misses(), 1, "overwrite should not add miss")
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
	assertEqual(t, h.Misses(), 1, "store = miss")

	h.BeforeRemove(filePath)
	os.Remove(filePath)
	assertEqual(t, h.TotalFiles(), 0, "after remove")
	assertEqual(t, h.TotalSize(), 0, "after remove")
	assertEqual(t, h.Misses(), 1, "remove should not change misses")
}

func TestUseFile(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	path := storeDesyncChunk(t, h, dir, 512)
	assertEqual(t, h.Hits(), 0, "no hits before UseFile")

	info1, _ := os.Stat(path)
	mtime1 := info1.ModTime()

	time.Sleep(50 * time.Millisecond)
	h.UseFile(path)

	info2, _ := os.Stat(path)
	mtime2 := info2.ModTime()
	if !mtime2.After(mtime1) {
		t.Errorf("UseFile should update mtime: before=%v after=%v", mtime1, mtime2)
	}
	assertEqual(t, h.Hits(), 1, "UseFile should increment hits")

	h.UseFile(path)
	h.UseFile(path)
	assertEqual(t, h.Hits(), 3, "multiple UseFile calls")
}

func TestEvictionBySize(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 500, 0, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for range 50 {
		storeDesyncChunk(t, h, dir, 256)
	}

	if h.TotalSize() > 600 {
		t.Errorf("totalSize %d should be near or below limit 500", h.TotalSize())
	}
	assertEqual(t, h.Misses(), 50, "50 stores = 50 misses")
}

func TestEvictionByFiles(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 0, 10, 1))
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
	assertEqual(t, h.Misses(), 20, "20 stores = 20 misses")
}

func TestEvictionLRU(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	var oldPaths []string
	for range 5 {
		path := storeDesyncChunk(t, h, dir, 128)
		oldPaths = append(oldPaths, path)
	}

	time.Sleep(100 * time.Millisecond)

	var newPaths []string
	for range 5 {
		path := storeDesyncChunk(t, h, dir, 128)
		newPaths = append(newPaths, path)
	}

	time.Sleep(100 * time.Millisecond)

	for _, p := range oldPaths {
		h.UseFile(p)
	}

	assertEqual(t, h.Misses(), 10, "10 stores = 10 misses")
	assertEqual(t, h.Hits(), 5, "5 UseFile calls = 5 hits")

	h.cfg.MaxSize = 200
	storeDesyncChunk(t, h, dir, 128)
	assertEqual(t, h.Misses(), 11, "11th store")

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
	assertEqual(t, h.Hits(), 0, "cold start hits")
	assertEqual(t, h.Misses(), 0, "cold start misses")
}

func TestLFSLayout(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(lfsConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for range 10 {
		storeLFSObject(t, h, dir, 1024)
	}

	assertEqual(t, h.TotalFiles(), 10, "lfs file count")
	assertGreater(t, h.TotalSize(), 0, "lfs total size")
	assertEqual(t, h.Misses(), 10, "lfs misses")
}

func TestLFSColdStart(t *testing.T) {
	dir := t.TempDir()

	var totalSize int64
	for range 5 {
		path := storeLFSObjectRaw(t, dir, 256)
		info, _ := os.Stat(path)
		totalSize += info.Size()
	}

	h, err := Open(lfsConfig(dir, 100*1024*1024, 0, 16))
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
	assertEqual(t, h.Misses(), 160, "160 stores = 160 misses")
}

func TestPartitionMismatch(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	storeDesyncChunk(t, h, dir, 512)
	h.Close()

	h2, err := Open(desyncConfig(dir, 100*1024*1024, 0, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	assertGreater(t, h2.TotalSize(), 0, "rescan after partition change")
	// Stats reset on recreate.
	assertEqual(t, h2.Hits(), 0, "hits reset after recreate")
	assertEqual(t, h2.Misses(), 0, "misses reset after recreate")
}

func TestStaleTempCleanup(t *testing.T) {
	dir := t.TempDir()
	cfg := desyncConfig(dir, 0, 0, 1)
	h, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	var subdirWithChunks string
	for range 50 {
		path := storeDesyncChunk(t, h, dir, 256)
		if subdirWithChunks == "" {
			subdirWithChunks = filepath.Dir(path)
		}
	}

	tmpPath := filepath.Join(subdirWithChunks, ".tmp-cacnk123456")
	os.WriteFile(tmpPath, []byte("stale"), 0644)
	old := time.Now().Add(-2 * time.Hour)
	os.Chtimes(tmpPath, old, old)

	h.cfg.MaxSize = 500
	storeDesyncChunk(t, h, dir, 256)

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

	assertEqual(t, h.TotalFiles(), 50, "disabled: all files remain")
	assertEqual(t, h.Misses(), 50, "50 stores = 50 misses even when disabled")
}

func TestInvalidPartitions(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []int{3, 5, 7, 15, 100} {
		_, err := Open(desyncConfig(dir, 1024, 0, p))
		if err == nil {
			t.Errorf("partitions=%d should be invalid", p)
		}
	}
}

func TestHitRatio(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// No requests → ratio 0.
	if r := h.HitRatio(); r != 0 {
		t.Errorf("empty hit ratio: got %f, want 0", r)
	}

	// 1 miss (store).
	storeDesyncChunk(t, h, dir, 256)
	if r := h.HitRatio(); r != 0 {
		t.Errorf("after 1 miss: got %f, want 0", r)
	}

	// 1 hit (use).
	path := storeDesyncChunk(t, h, dir, 256)
	h.UseFile(path)
	// 2 misses, 1 hit → ratio = 1/3 ≈ 0.333
	r := h.HitRatio()
	if r < 0.33 || r > 0.34 {
		t.Errorf("after 2 misses + 1 hit: got %f, want ~0.333", r)
	}
}

func TestResetStats(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	path := storeDesyncChunk(t, h, dir, 256)
	h.UseFile(path)
	assertEqual(t, h.Misses(), 1, "before reset misses")
	assertEqual(t, h.Hits(), 1, "before reset hits")

	h.ResetStats()
	assertEqual(t, h.Misses(), 0, "after reset misses")
	assertEqual(t, h.Hits(), 0, "after reset hits")
	if r := h.HitRatio(); r != 0 {
		t.Errorf("after reset: ratio %f, want 0", r)
	}

	// Counters still work after reset.
	storeDesyncChunk(t, h, dir, 256)
	h.UseFile(path)
	assertEqual(t, h.Misses(), 1, "post-reset misses")
	assertEqual(t, h.Hits(), 1, "post-reset hits")
}

func TestVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	h.Close()

	// Corrupt the major version in the tracking file.
	path := filepath.Join(dir, trackingFileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	// Write major version 99 at offset 0.
	buf := []byte{99, 0, 0, 0}
	f.WriteAt(buf, 0)
	f.Close()

	// Open should fail with version error.
	_, err = Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err == nil {
		t.Fatal("expected version mismatch error")
	}
	if !strings.Contains(err.Error(), "incompatible") {
		t.Errorf("error should mention 'incompatible': %v", err)
	}
}

func TestStatsPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	h, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}

	path := storeDesyncChunk(t, h, dir, 256)
	h.UseFile(path)
	h.UseFile(path)
	assertEqual(t, h.Misses(), 1, "before close misses")
	assertEqual(t, h.Hits(), 2, "before close hits")
	h.Close()

	// Reopen — stats should persist.
	h2, err := Open(desyncConfig(dir, 100*1024*1024, 0, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	assertEqual(t, h2.Misses(), 1, "after reopen misses")
	assertEqual(t, h2.Hits(), 2, "after reopen hits")
	assertEqual(t, h2.TotalFiles(), 1, "after reopen files")
}
