package cmdshared

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"
)

// ProfileDir returns the heap profile directory from the DESYNC_HEAP_PROFILE_DIR
// environment variable, or "" if profiling is not enabled.
func ProfileDir() string {
	return os.Getenv("DESYNC_HEAP_PROFILE_DIR")
}

// InitProfiling sets up GC tracing and stderr redirection when
// DESYNC_HEAP_PROFILE_DIR is set. Call this early in main().
// Returns a cleanup function to close the stderr log file.
func InitProfiling() func() {
	dir := ProfileDir()
	if dir == "" {
		return func() {}
	}
	os.MkdirAll(dir, 0755)

	// Enable GC tracing via GODEBUG (if not already set).
	if os.Getenv("GODEBUG") == "" {
		os.Setenv("GODEBUG", "gctrace=1")
	}

	// Redirect stderr to a per-process log file so gctrace output is captured.
	pid := os.Getpid()
	logPath := filepath.Join(dir, fmt.Sprintf("gctrace_%d.log", pid))
	f, err := os.Create(logPath)
	if err != nil {
		return func() {}
	}
	// Duplicate stderr to the file.
	origStderr := os.Stderr
	os.Stderr = f
	return func() {
		os.Stderr = origStderr
		f.Close()
	}
}

var profileCount atomic.Int64

// WriteHeapProfile writes memory stats to the profiling directory.
// Every call writes a lightweight MemStats line (~200 bytes).
// Every 100th call also writes a full pprof heap profile for detailed analysis.
// No-op if DESYNC_HEAP_PROFILE_DIR is not set.
func WriteHeapProfile(label string) {
	dir := ProfileDir()
	if dir == "" {
		return
	}
	pid := os.Getpid()
	ts := time.Now().UnixMilli()

	// Always write lightweight MemStats.
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	statsPath := filepath.Join(dir, fmt.Sprintf("memstats_%s_%d_%d.txt", label, pid, ts))
	os.WriteFile(statsPath, []byte(fmt.Sprintf(
		"HeapInuse=%dMB HeapIdle=%dMB HeapSys=%dMB StackSys=%dMB Sys=%dMB NumGC=%d\n",
		m.HeapInuse>>20, m.HeapIdle>>20, m.HeapSys>>20, m.StackInuse>>20, m.Sys>>20, m.NumGC,
	)), 0644)

	// Write full pprof profile every 100 calls (expensive due to gzip).
	if profileCount.Add(1)%100 == 1 {
		profPath := filepath.Join(dir, fmt.Sprintf("heap_%s_%d_%d.prof", label, pid, ts))
		f, err := os.Create(profPath)
		if err != nil {
			return
		}
		defer f.Close()
		pprof.WriteHeapProfile(f)
	}
}
