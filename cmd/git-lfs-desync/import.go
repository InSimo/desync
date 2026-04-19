package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
)

// runImport walks dir recursively and uploads any LFS objects (files whose
// basename is a 64-char lowercase hex SHA-256) that are not already present
// in the index store. Each file's SHA-256 is verified against its filename
// before chunking. Uploads run with up to `concurrency` workers in flight.
//
// Returns a non-nil error if any file was invalid (OID mismatch, read error,
// or upload failure). Walk errors fail the whole operation immediately.
func runImport(ctx context.Context, dir string, client *desync.Client, concurrency int) error {
	if concurrency <= 0 {
		concurrency = 8
	}
	stats := &importStats{out: os.Stderr}
	stats.render()

	// Producer/worker layout: a single goroutine walks the tree and pushes
	// paths onto a bounded channel. Worker goroutines pull paths and call
	// importOne. The channel buffer equals concurrency so the walker keeps
	// a small queue ahead of the workers.
	jobs := make(chan string, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				if ctx.Err() != nil {
					return
				}
				stats.importOne(path, client)
			}
		}()
	}

	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		if !isValidOID(d.Name()) {
			return nil
		}
		stats.discovered.Add(1)
		stats.render()
		select {
		case jobs <- path:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	close(jobs)
	wg.Wait()

	// Terminate the live progress line with a newline so the shell prompt
	// returns on its own line.
	stats.finish()

	if walkErr != nil {
		return fmt.Errorf("walking %s: %w", dir, walkErr)
	}
	if stats.invalid.Load() > 0 {
		return fmt.Errorf("%d file(s) failed to import", stats.invalid.Load())
	}
	return nil
}

// importStats tracks per-file outcomes and renders a live progress line.
type importStats struct {
	discovered atomic.Int64
	skipped    atomic.Int64
	uploaded   atomic.Int64
	invalid    atomic.Int64

	mu  sync.Mutex // serialises writes to out
	out io.Writer
}

// importOne processes a single file: skip if already present, verify SHA-256,
// then upload via PutObjectFromFile. Updates counters and re-renders progress.
func (s *importStats) importOne(path string, client *desync.Client) {
	oid := filepath.Base(path)
	name := cmdshared.OidIndexName(oid)

	if exists, err := client.IndexStore.HasIndex(name); err == nil && exists {
		s.skipped.Add(1)
		s.render()
		return
	}

	if err := verifyFileSHA256(path, oid); err != nil {
		s.invalid.Add(1)
		s.logf("invalid %s: %v", oid, err)
		return
	}

	if err := client.PutObjectFromFile(name, path, nil); err != nil {
		s.invalid.Add(1)
		s.logf("upload failed %s: %v", oid, err)
		return
	}
	s.uploaded.Add(1)
	s.render()
}

// render overwrites the current progress line in place.
func (s *importStats) render() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.out, "\r\033[Kdiscovered=%d skipped=%d uploaded=%d invalid=%d",
		s.discovered.Load(), s.skipped.Load(), s.uploaded.Load(), s.invalid.Load())
}

// logf prints a full-line message above the progress indicator, then
// re-renders the progress line so it remains visible.
func (s *importStats) logf(format string, args ...any) {
	s.mu.Lock()
	fmt.Fprint(s.out, "\r\033[K")
	fmt.Fprintf(s.out, format, args...)
	fmt.Fprintln(s.out)
	fmt.Fprintf(s.out, "\r\033[Kdiscovered=%d skipped=%d uploaded=%d invalid=%d",
		s.discovered.Load(), s.skipped.Load(), s.uploaded.Load(), s.invalid.Load())
	s.mu.Unlock()
}

// finish terminates the progress line with a newline.
func (s *importStats) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintln(s.out)
}

// isValidOID returns true if name is exactly 64 lowercase hex characters.
func isValidOID(name string) bool {
	if len(name) != 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// verifyFileSHA256 streams path through SHA-256 and returns an error if
// the digest does not match oid.
func verifyFileSHA256(path, oid string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != oid {
		return fmt.Errorf("SHA-256 mismatch: expected %s, got %s", oid, got)
	}
	return nil
}
