package desync

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestObjectReadSeekRead reproduces the pattern used by Go's http.ServeContent
// (and Forgejo's ServeContentByReadSeeker): read a few bytes, Seek(0, SeekStart),
// Seek(0, SeekEnd) to learn the size, Seek(0, SeekStart), then read all bytes.
// A single-chunk object must deliver its full content through that sequence.
func TestObjectReadSeekRead(t *testing.T) {
	InitWorkerPool(4, 4)

	dir := t.TempDir()
	chunkDir := filepath.Join(dir, "chunks")
	idxDir := filepath.Join(dir, "indexes")
	for _, d := range []string{chunkDir, idxDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	store, err := NewLocalStore(chunkDir, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	idxStore, err := NewLocalIndexStore(idxDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idxStore.Close()

	client := NewClient(store, store, idxStore, ClientOptions{
		N:        4,
		MinChunk: ChunkSizeMinDefault,
		AvgChunk: ChunkSizeAvgDefault,
		MaxChunk: ChunkSizeMaxDefault,
	})

	// Small blob — well under ChunkSizeMinDefault so it lands in a single chunk.
	want := make([]byte, 18821)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObject("test.caibx", bytes.NewReader(want), int64(len(want))); err != nil {
		t.Fatal(err)
	}

	obj, err := client.GetObject("test.caibx")
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()

	// Step 1: read the first 512 bytes (MIME sniff).
	head := make([]byte, 512)
	n, err := io.ReadFull(obj, head)
	if err != nil {
		t.Fatalf("initial ReadFull: n=%d err=%v", n, err)
	}
	if !bytes.Equal(head, want[:512]) {
		t.Fatalf("initial read differs from source")
	}

	// Step 2: Seek(0, SeekStart) — simulates the MIME reset.
	if _, err := obj.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek(0, Start) after read: %v", err)
	}

	// Step 3: Seek(0, SeekEnd) — http.ServeContent asks for size.
	size, err := obj.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek(0, End): %v", err)
	}
	if size != int64(len(want)) {
		t.Fatalf("Seek(0, End) returned %d, want %d", size, len(want))
	}

	// Step 4: Seek back to start, then read the whole thing.
	if _, err := obj.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek(0, Start) before full read: %v", err)
	}
	got, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d bytes after seek, want %d", len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read content differs from source")
	}
}

