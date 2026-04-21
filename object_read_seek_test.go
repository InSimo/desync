package desync

import (
	"bufio"
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

	client, _, want := newObjectTestFixture(t, 18821)

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

// TestObjectReadSeekMatrix exercises Read/Seek on a multi-chunk object,
// covering sequential reads, every Seek whence, chunk boundary positions,
// interleaved seek+partial-read sequences, error paths, and edge cases.
// The subtests only use the io.ReadSeekCloser surface — no reference to
// Object internals — so they pass unchanged across refactors of the
// position state model.
func TestObjectReadSeekMatrix(t *testing.T) {
	InitWorkerPool(4, 4)

	// 1 MiB of random data — guaranteed to chunk into at least 4 slots
	// at avg=64KiB, min=16KiB, max=256KiB.
	const size = 1 << 20
	client, name, want := newObjectTestFixture(t, size)

	// Boundary positions derived from the actual index.
	idx, err := client.IndexStore.GetIndex(name)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Chunks) < 4 {
		t.Fatalf("need at least 4 chunks for matrix; got %d", len(idx.Chunks))
	}
	boundary := int64(idx.Chunks[1].Start)                     // first internal boundary
	lastBoundary := int64(idx.Chunks[len(idx.Chunks)-1].Start) // start of last chunk
	midFirst := int64(idx.Chunks[0].Size) / 2                  // mid-first-chunk
	midLast := lastBoundary + int64(idx.Chunks[len(idx.Chunks)-1].Size)/2

	newObj := func(t *testing.T) *Object {
		t.Helper()
		obj, err := client.GetObject(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = obj.Close() })
		return obj
	}

	t.Run("sequential ReadAll matches source", func(t *testing.T) {
		got, err := io.ReadAll(newObj(t))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadAll got %d bytes, want %d", len(got), len(want))
		}
	})

	t.Run("tiny buffered reads cross chunk boundaries", func(t *testing.T) {
		// bufio with a buffer much smaller than a chunk forces many small
		// Read() calls, some of which will span chunk boundaries.
		br := bufio.NewReaderSize(newObj(t), 17)
		got, err := io.ReadAll(br)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("tiny-buffered read differs")
		}
	})

	t.Run("http.ServeContent pattern on multi-chunk", func(t *testing.T) {
		obj := newObj(t)
		head := make([]byte, 512)
		if _, err := io.ReadFull(obj, head); err != nil {
			t.Fatal(err)
		}
		if _, err := obj.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		n, err := obj.Seek(0, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(want)) {
			t.Fatalf("SeekEnd returned %d, want %d", n, len(want))
		}
		if _, err := obj.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(obj)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("post-seek read differs")
		}
	})

	seekStartCases := []struct {
		name string
		n    int64
	}{
		{"0", 0},
		{"1", 1},
		{"midFirstChunk", midFirst},
		{"chunkBoundary", boundary},
		{"chunkBoundary+1", boundary + 1},
		{"chunkBoundary-1", boundary - 1},
		{"midLastChunk", midLast},
		{"size-1", int64(len(want) - 1)},
	}
	for _, tc := range seekStartCases {
		t.Run("SeekStart_"+tc.name, func(t *testing.T) {
			obj := newObj(t)
			pos, err := obj.Seek(tc.n, io.SeekStart)
			if err != nil {
				t.Fatalf("Seek: %v", err)
			}
			if pos != tc.n {
				t.Fatalf("Seek returned %d, want %d", pos, tc.n)
			}
			got, err := io.ReadAll(obj)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(got, want[tc.n:]) {
				t.Fatalf("read %d bytes starting at %d, content differs", len(got), tc.n)
			}
		})
	}

	t.Run("SeekStart to size yields EOF", func(t *testing.T) {
		obj := newObj(t)
		if _, err := obj.Seek(int64(len(want)), io.SeekStart); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 16)
		n, err := obj.Read(buf)
		if n != 0 || err != io.EOF {
			t.Fatalf("Read at EOF: n=%d err=%v, want 0/EOF", n, err)
		}
	})

	t.Run("SeekCurrent forward and backward", func(t *testing.T) {
		obj := newObj(t)
		// Read 1 chunk worth.
		first := make([]byte, idx.Chunks[0].Size)
		if _, err := io.ReadFull(obj, first); err != nil {
			t.Fatal(err)
		}
		// Forward seek by one chunk.
		pos, err := obj.Seek(int64(idx.Chunks[1].Size), io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		expected := int64(idx.Chunks[0].Size + idx.Chunks[1].Size)
		if pos != expected {
			t.Fatalf("after +Current: %d, want %d", pos, expected)
		}
		// Backward seek to start.
		pos, err = obj.Seek(-expected, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if pos != 0 {
			t.Fatalf("after -Current to 0: %d", pos)
		}
		got, err := io.ReadAll(obj)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("full read after forward+backward seek differs")
		}
	})

	t.Run("SeekEnd reads last byte", func(t *testing.T) {
		obj := newObj(t)
		pos, err := obj.Seek(-1, io.SeekEnd)
		if err != nil {
			t.Fatal(err)
		}
		if pos != int64(len(want)-1) {
			t.Fatalf("Seek(-1, End): %d, want %d", pos, len(want)-1)
		}
		got, err := io.ReadAll(obj)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != want[len(want)-1] {
			t.Fatalf("last byte: got %v, want %v", got, want[len(want)-1:])
		}
	})

	t.Run("SeekEnd 0 yields EOF", func(t *testing.T) {
		obj := newObj(t)
		if _, err := obj.Seek(0, io.SeekEnd); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 16)
		n, err := obj.Read(buf)
		if n != 0 || err != io.EOF {
			t.Fatalf("Read at EOF: n=%d err=%v", n, err)
		}
	})

	t.Run("interleaved seek and partial read", func(t *testing.T) {
		obj := newObj(t)
		type step struct {
			off int64
			n   int
		}
		// Hand-picked steps touching several chunks, forward and backward.
		steps := []step{
			{0, 100},
			{boundary - 50, 100},
			{int64(len(want)) - 200, 200},
			{midFirst, 500},
			{boundary, 1000},
			{lastBoundary + 10, 50},
		}
		for i, s := range steps {
			if _, err := obj.Seek(s.off, io.SeekStart); err != nil {
				t.Fatalf("step %d seek %d: %v", i, s.off, err)
			}
			got := make([]byte, s.n)
			nr, err := io.ReadFull(obj, got)
			if err != nil {
				t.Fatalf("step %d read %d: n=%d err=%v", i, s.n, nr, err)
			}
			if !bytes.Equal(got, want[s.off:s.off+int64(s.n)]) {
				t.Fatalf("step %d: content differs at offset %d len %d", i, s.off, s.n)
			}
		}
	})

	t.Run("negative SeekStart errors", func(t *testing.T) {
		obj := newObj(t)
		if _, err := obj.Seek(-1, io.SeekStart); err == nil {
			t.Fatal("expected error for negative Seek, got nil")
		}
	})

	t.Run("invalid whence errors", func(t *testing.T) {
		obj := newObj(t)
		if _, err := obj.Seek(0, 999); err == nil {
			t.Fatal("expected error for invalid whence, got nil")
		}
	})

	t.Run("read after Close", func(t *testing.T) {
		obj, err := client.GetObject(name)
		if err != nil {
			t.Fatal(err)
		}
		_ = obj.Close()
		buf := make([]byte, 16)
		if _, err := obj.Read(buf); err != io.ErrClosedPipe {
			t.Fatalf("Read after Close: err=%v, want io.ErrClosedPipe", err)
		}
	})
}

// TestObjectZeroByte covers an empty object: Read should return (0, io.EOF)
// and Seek should accept 0 without blocking.
func TestObjectZeroByte(t *testing.T) {
	InitWorkerPool(4, 4)
	client, name, _ := newObjectTestFixture(t, 0)

	obj, err := client.GetObject(name)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()

	buf := make([]byte, 16)
	n, err := obj.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("Read on empty object: n=%d err=%v", n, err)
	}

	pos, err := obj.Seek(0, io.SeekStart)
	if err != nil || pos != 0 {
		t.Fatalf("Seek(0, Start) on empty: pos=%d err=%v", pos, err)
	}
	pos, err = obj.Seek(0, io.SeekEnd)
	if err != nil || pos != 0 {
		t.Fatalf("Seek(0, End) on empty: pos=%d err=%v", pos, err)
	}
}

// TestObjectSingleByte — the smallest non-empty object: one byte, then EOF.
func TestObjectSingleByte(t *testing.T) {
	InitWorkerPool(4, 4)
	client, name, want := newObjectTestFixture(t, 1)

	obj, err := client.GetObject(name)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()

	buf := make([]byte, 16)
	n, err := obj.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 1 || buf[0] != want[0] {
		t.Fatalf("first Read: n=%d buf[0]=%d want=%d", n, buf[0], want[0])
	}
	// Next Read is definitively EOF.
	n, err = obj.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("second Read: n=%d err=%v", n, err)
	}
}

// newObjectTestFixture writes sizeBytes of deterministic pseudo-random data
// through a Client into a temporary LocalStore + LocalIndexStore, stores it
// under the name "test.caibx", and returns the client, the name, and the
// source bytes. t.Cleanup handles teardown.
func newObjectTestFixture(t *testing.T, sizeBytes int) (*Client, string, []byte) {
	t.Helper()

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
	t.Cleanup(func() { _ = store.Close() })
	idxStore, err := NewLocalIndexStore(idxDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idxStore.Close() })

	client := NewClient(store, store, idxStore, ClientOptions{
		N:        4,
		MinChunk: ChunkSizeMinDefault,
		AvgChunk: ChunkSizeAvgDefault,
		MaxChunk: ChunkSizeMaxDefault,
	})

	data := make([]byte, sizeBytes)
	if sizeBytes > 0 {
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
	}
	name := "test.caibx"
	if _, err := client.PutObject(name, bytes.NewReader(data), int64(sizeBytes)); err != nil {
		t.Fatal(err)
	}
	return client, name, data
}

