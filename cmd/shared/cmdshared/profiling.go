package cmdshared

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
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

// WriteHeapProfile writes a heap profile to the profiling directory.
// No-op if DESYNC_HEAP_PROFILE_DIR is not set.
func WriteHeapProfile(label string) {
	dir := ProfileDir()
	if dir == "" {
		return
	}
	pid := os.Getpid()
	name := fmt.Sprintf("heap_%s_%d_%d.prof", label, pid, time.Now().UnixMilli())
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	pprof.WriteHeapProfile(f)
}
