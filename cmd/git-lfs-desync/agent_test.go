package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/folbricht/desync"
)

func TestDeriveIndexURL(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		// S3 URLs — replace last path segment with "index/".
		{
			"s3+https://s3.amazonaws.com/my-bucket/chunks/",
			"s3+https://s3.amazonaws.com/my-bucket/index/",
		},
		{
			"s3+http://localhost:9000/bucket/lfs/chunks/",
			"s3+http://localhost:9000/bucket/lfs/index/",
		},
		{
			"s3+https://host/bucket/chunks",
			"s3+https://host/bucket/index/",
		},
		// Local filesystem paths — sibling "index" directory.
		{
			"/path/to/chunks",
			"/path/to/index",
		},
		{
			"/path/to/chunks/",
			"/path/to/index",
		},
		{
			"chunks",
			"index",
		},
	}
	for _, c := range cases {
		got, err := deriveIndexURL(c.input)
		if err != nil {
			t.Errorf("deriveIndexURL(%q): unexpected error: %v", c.input, err)
			continue
		}
		if got != c.want {
			t.Errorf("deriveIndexURL(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestParseChunkSizeParam(t *testing.T) {
	min, avg, max, err := parseChunkSizeParam("16:64:256")
	if err != nil {
		t.Fatal(err)
	}
	if min != 16*1024 || avg != 64*1024 || max != 256*1024 {
		t.Errorf("unexpected sizes: min=%d avg=%d max=%d", min, avg, max)
	}

	_, _, _, err = parseChunkSizeParam("bad")
	if err == nil {
		t.Error("expected error for invalid chunk size param")
	}
}

func TestAgentInit(t *testing.T) {
	var buf bytes.Buffer
	a := &Agent{enc: json.NewEncoder(&buf)}

	raw, _ := json.Marshal(initRequest{Event: "init", Operation: "upload"})
	a.handleInit(raw)

	var got map[string]interface{}
	if err := json.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decoding init response: %v", err)
	}
	// Response should be an empty JSON object
	if len(got) != 0 {
		t.Errorf("expected empty object, got %v", got)
	}
}

func TestAgentUploadDownload(t *testing.T) {
	// Create temporary directories for chunk store and index store.
	chunkDir := t.TempDir()
	indexDir := t.TempDir()
	tmpDir := t.TempDir()

	chunkStore, err := desync.NewLocalStore(chunkDir, desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer chunkStore.Close()

	indexStore, err := desync.NewLocalIndexStore(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	defer indexStore.Close()

	// Create a test file with known content.
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	content := bytes.Repeat([]byte("hello desync git-lfs "), 10000)
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	a := &Agent{
		writeStore:      chunkStore,
		readStore:       chunkStore,
		indexWriteStore: indexStore,
		n:               4,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}

	oid := "abc123def456"

	// Test upload.
	uploadMsg, _ := json.Marshal(transferRequest{
		Event: "upload",
		OID:   oid,
		Size:  int64(len(content)),
		Path:  srcFile,
	})
	a.handleUpload(context.Background(), uploadMsg)

	// Parse the complete event from the buffer.
	dec := json.NewDecoder(&buf)
	var events []json.RawMessage
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		events = append(events, raw)
	}

	// Last event must be a "complete" with no error.
	if len(events) == 0 {
		t.Fatal("no events received from upload")
	}
	var last completeEvent
	if err := json.Unmarshal(events[len(events)-1], &last); err != nil {
		t.Fatal(err)
	}
	if last.Event != "complete" {
		t.Errorf("expected complete event, got %q", last.Event)
	}
	if last.Error != nil {
		t.Errorf("upload error: %v", last.Error.Message)
	}

	// Test download.
	buf.Reset()
	downloadMsg, _ := json.Marshal(transferRequest{
		Event: "download",
		OID:   oid,
		Size:  int64(len(content)),
	})
	a.handleDownload(context.Background(), downloadMsg)

	dec = json.NewDecoder(&buf)
	events = nil
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		events = append(events, raw)
	}

	if len(events) == 0 {
		t.Fatal("no events received from download")
	}
	var dlComplete completeEvent
	if err := json.Unmarshal(events[len(events)-1], &dlComplete); err != nil {
		t.Fatal(err)
	}
	if dlComplete.Event != "complete" {
		t.Errorf("expected complete event, got %q", dlComplete.Event)
	}
	if dlComplete.Error != nil {
		t.Errorf("download error: %v", dlComplete.Error.Message)
	}
	if dlComplete.Path == "" {
		t.Error("expected non-empty path in download complete event")
	}

	// Verify downloaded content matches source.
	got, err := os.ReadFile(dlComplete.Path)
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("downloaded content does not match source")
	}
}

// TestAgentUploadDownloadViaLocalURL exercises the full upload/download cycle
// using chunkStoreFromURL and indexStoreFromURL with local directory paths,
// verifying that the store factory functions work end-to-end.
func TestAgentUploadDownloadViaLocalURL(t *testing.T) {
	chunkDir := t.TempDir()
	indexDir := t.TempDir()
	tmpDir := t.TempDir()

	opt := desync.StoreOptions{}

	chunkStore, err := chunkStoreFromURL(chunkDir, opt)
	if err != nil {
		t.Fatalf("chunkStoreFromURL(%q): %v", chunkDir, err)
	}
	defer chunkStore.Close()

	indexStore, err := indexStoreFromURL(indexDir, opt)
	if err != nil {
		t.Fatalf("indexStoreFromURL(%q): %v", indexDir, err)
	}
	defer indexStore.Close()

	srcFile := filepath.Join(t.TempDir(), "source.bin")
	content := bytes.Repeat([]byte("hello desync git-lfs local "), 5000)
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	a := &Agent{
		writeStore:      chunkStore,
		readStore:       chunkStore,
		indexWriteStore: indexStore,
		n:               4,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}

	oid := "localurl-test-oid-abc123"

	// Upload.
	uploadMsg, _ := json.Marshal(transferRequest{
		Event: "upload",
		OID:   oid,
		Size:  int64(len(content)),
		Path:  srcFile,
	})
	a.handleUpload(context.Background(), uploadMsg)

	var uploadComplete completeEvent
	dec := json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &uploadComplete)
	}
	if uploadComplete.Event != "complete" {
		t.Fatalf("expected complete event, got %q", uploadComplete.Event)
	}
	if uploadComplete.Error != nil {
		t.Fatalf("upload error: %v", uploadComplete.Error.Message)
	}

	// Download.
	buf.Reset()
	downloadMsg, _ := json.Marshal(transferRequest{
		Event: "download",
		OID:   oid,
		Size:  int64(len(content)),
	})
	a.handleDownload(context.Background(), downloadMsg)

	var dlComplete completeEvent
	dec = json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &dlComplete)
	}
	if dlComplete.Event != "complete" {
		t.Fatalf("expected complete event, got %q", dlComplete.Event)
	}
	if dlComplete.Error != nil {
		t.Fatalf("download error: %v", dlComplete.Error.Message)
	}
	if dlComplete.Path == "" {
		t.Fatal("expected non-empty path in download complete event")
	}

	got, err := os.ReadFile(dlComplete.Path)
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("downloaded content does not match source")
	}
}

// TestAgentWithCache verifies that when a cache store is configured, chunks
// fetched from the remote store during a download are stored in the cache,
// and a subsequent download is served entirely from the cache (the remote
// store can be absent for the second download).
func TestAgentWithCache(t *testing.T) {
	remoteDir := t.TempDir()
	cacheDir := t.TempDir()
	indexDir := t.TempDir()
	tmpDir := t.TempDir()

	remoteStore, err := desync.NewLocalStore(remoteDir, desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer remoteStore.Close()

	cacheStore, err := desync.NewLocalStore(cacheDir, desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cacheStore.Close()

	indexStore, err := desync.NewLocalIndexStore(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	defer indexStore.Close()

	// Upload: write chunks to the remote store (no cache on upload path).
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	content := bytes.Repeat([]byte("cache-test-content "), 5000)
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	oid := "cache-test-oid-xyz"
	uploadAgent := &Agent{
		writeStore:      remoteStore,
		readStore:       remoteStore,
		indexWriteStore: indexStore,
		n:               4,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}
	uploadMsg, _ := json.Marshal(transferRequest{Event: "upload", OID: oid, Size: int64(len(content)), Path: srcFile})
	uploadAgent.handleUpload(context.Background(), uploadMsg)
	var uploadDone completeEvent
	dec := json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &uploadDone)
	}
	if uploadDone.Error != nil {
		t.Fatalf("upload error: %v", uploadDone.Error.Message)
	}

	// First download: readStore is Cache{remote, cache}. Chunks are fetched
	// from the remote store and automatically saved to cacheDir.
	buf.Reset()
	downloadAgent := &Agent{
		writeStore:      remoteStore,
		readStore:       desync.NewCache(remoteStore, cacheStore),
		indexWriteStore: indexStore,
		n:               4,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}
	dlMsg, _ := json.Marshal(transferRequest{Event: "download", OID: oid, Size: int64(len(content))})
	downloadAgent.handleDownload(context.Background(), dlMsg)
	var dl1Complete completeEvent
	dec = json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &dl1Complete)
	}
	if dl1Complete.Error != nil {
		t.Fatalf("first download error: %v", dl1Complete.Error.Message)
	}
	got, err := os.ReadFile(dl1Complete.Path)
	if err != nil {
		t.Fatalf("reading first download: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("first download content mismatch")
	}

	// Verify the cache was populated: cacheDir should now contain chunk files.
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Error("cache directory is empty after first download; cache was not populated")
	}

	// Second download: readStore uses only the cache (no remote). All chunks
	// must be served from the cache without touching the remote store.
	buf.Reset()
	cacheOnlyAgent := &Agent{
		writeStore:      cacheStore,
		readStore:       cacheStore,
		indexWriteStore: indexStore,
		n:               4,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}
	cacheOnlyAgent.handleDownload(context.Background(), dlMsg)
	var dl2Complete completeEvent
	dec = json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &dl2Complete)
	}
	if dl2Complete.Error != nil {
		t.Fatalf("cache-only download error: %v", dl2Complete.Error.Message)
	}
	got, err = os.ReadFile(dl2Complete.Path)
	if err != nil {
		t.Fatalf("reading cache-only download: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("cache-only download content mismatch")
	}
}

func TestAgentConcurrentTransfers(t *testing.T) {
	chunkDir := t.TempDir()
	indexDir := t.TempDir()
	tmpDir := t.TempDir()

	chunkStore, err := desync.NewLocalStore(chunkDir, desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer chunkStore.Close()

	indexStore, err := desync.NewLocalIndexStore(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	defer indexStore.Close()

	var buf bytes.Buffer
	a := &Agent{
		writeStore:      chunkStore,
		readStore:       chunkStore,
		indexWriteStore: indexStore,
		n:               2,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}

	// Build input: init (concurrenttransfers=3) + 3 uploads + terminate.
	var input bytes.Buffer
	enc := json.NewEncoder(&input)

	enc.Encode(initRequest{Event: "init", Operation: "upload", Concurrent: true, ConcurrentTransfers: 3})

	oids := []string{"oid-concurrent-1", "oid-concurrent-2", "oid-concurrent-3"}
	content := bytes.Repeat([]byte("concurrent-test "), 5000)
	for _, oid := range oids {
		f := filepath.Join(t.TempDir(), "src-"+oid)
		if err := os.WriteFile(f, content, 0644); err != nil {
			t.Fatal(err)
		}
		enc.Encode(transferRequest{Event: "upload", OID: oid, Size: int64(len(content)), Path: f})
	}
	enc.Encode(map[string]string{"event": "terminate"})

	if err := a.run(context.Background(), &input); err != nil {
		t.Fatalf("run error: %v", err)
	}

	// Collect all complete events and verify each OID succeeded.
	dec := json.NewDecoder(&buf)
	completes := map[string]completeEvent{}
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		var evt completeEvent
		json.Unmarshal(raw, &evt)
		if evt.Event == "complete" {
			completes[evt.OID] = evt
		}
	}
	for _, oid := range oids {
		evt, ok := completes[oid]
		if !ok {
			t.Errorf("no complete event for OID %s", oid)
			continue
		}
		if evt.Error != nil {
			t.Errorf("OID %s error: %v", oid, evt.Error.Message)
		}
	}
}

type trackingIndexWriteStore struct {
	desync.IndexWriteStore
	storeCalls atomic.Int64
}

func (s *trackingIndexWriteStore) StoreIndex(name string, idx desync.Index) error {
	s.storeCalls.Add(1)
	return s.IndexWriteStore.StoreIndex(name, idx)
}

func TestAgentUploadSkipsRedundantUpload(t *testing.T) {
	chunkDir := t.TempDir()
	indexDir := t.TempDir()
	tmpDir := t.TempDir()

	chunkStore, err := desync.NewLocalStore(chunkDir, desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer chunkStore.Close()

	rawIndex, err := desync.NewLocalIndexStore(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	defer rawIndex.Close()
	trackerIndex := &trackingIndexWriteStore{IndexWriteStore: rawIndex}

	content := bytes.Repeat([]byte("redundant-upload-test "), 5000)
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	newAgent := func() *Agent {
		var buf bytes.Buffer
		return &Agent{
			writeStore:      chunkStore,
			readStore:       chunkStore,
			indexWriteStore: trackerIndex,
			n:               4,
			minChunk:        4 * 1024,
			avgChunk:        16 * 1024,
			maxChunk:        64 * 1024,
			tmpDir:          tmpDir,
			enc:             json.NewEncoder(&buf),
		}
	}

	uploadMsg, _ := json.Marshal(transferRequest{
		Event: "upload", OID: "dup-upload-oid",
		Size: int64(len(content)), Path: srcFile,
	})

	// First upload: must succeed and call StoreIndex.
	a1 := newAgent()
	a1.handleUpload(context.Background(), uploadMsg)
	if trackerIndex.storeCalls.Load() == 0 {
		t.Fatal("first upload did not call StoreIndex")
	}

	// Reset counter, then upload the same OID again.
	trackerIndex.storeCalls.Store(0)
	a2 := newAgent()
	a2.handleUpload(context.Background(), uploadMsg)

	// Without the HasIndex fix this assertion FAILS (second upload calls StoreIndex again).
	if n := trackerIndex.storeCalls.Load(); n != 0 {
		t.Errorf("second upload called StoreIndex %d times; expected 0 (OID already indexed)", n)
	}
}

func TestOIDIndexName(t *testing.T) {
	cases := []struct {
		oid  string
		want string
	}{
		{"abc123def456", "abc1/abc123def456.caibx"},
		{"0000111122223333", "0000/0000111122223333.caibx"},
		{"deadbeefcafe1234", "dead/deadbeefcafe1234.caibx"},
	}
	for _, c := range cases {
		got := oidIndexName(c.oid)
		if got != c.want {
			t.Errorf("oidIndexName(%q) = %q, want %q", c.oid, got, c.want)
		}
	}
}

func TestLocalIndexStoreSharding(t *testing.T) {
	chunkDir := t.TempDir()
	indexDir := t.TempDir()
	tmpDir := t.TempDir()

	chunkStore, err := desync.NewLocalStore(chunkDir, desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer chunkStore.Close()

	indexStore, err := desync.NewLocalIndexStore(indexDir)
	if err != nil {
		t.Fatal(err)
	}
	defer indexStore.Close()

	srcFile := filepath.Join(t.TempDir(), "source.bin")
	content := bytes.Repeat([]byte("sharding-test-content "), 5000)
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	oid := "abcd1234efgh5678"

	var buf bytes.Buffer
	a := &Agent{
		writeStore:      chunkStore,
		readStore:       chunkStore,
		indexWriteStore: indexStore,
		n:               4,
		minChunk:        4 * 1024,
		avgChunk:        16 * 1024,
		maxChunk:        64 * 1024,
		tmpDir:          tmpDir,
		enc:             json.NewEncoder(&buf),
	}

	uploadMsg, _ := json.Marshal(transferRequest{
		Event: "upload",
		OID:   oid,
		Size:  int64(len(content)),
		Path:  srcFile,
	})
	a.handleUpload(context.Background(), uploadMsg)

	var uploadDone completeEvent
	dec := json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &uploadDone)
	}
	if uploadDone.Error != nil {
		t.Fatalf("upload error: %v", uploadDone.Error.Message)
	}

	// Assert the index is stored at the sharded path, not flat.
	shardedPath := filepath.Join(indexDir, oid[0:4], oid+".caibx")
	if _, err := os.Stat(shardedPath); err != nil {
		t.Errorf("index not found at sharded path %q: %v", shardedPath, err)
	}
	flatPath := filepath.Join(indexDir, oid+".caibx")
	if _, err := os.Stat(flatPath); err == nil {
		t.Errorf("index unexpectedly found at flat path %q (should be sharded)", flatPath)
	}

	// Assert download reconstructs the file correctly from the sharded path.
	buf.Reset()
	downloadMsg, _ := json.Marshal(transferRequest{
		Event: "download",
		OID:   oid,
		Size:  int64(len(content)),
	})
	a.handleDownload(context.Background(), downloadMsg)

	var dlComplete completeEvent
	dec = json.NewDecoder(&buf)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		json.Unmarshal(raw, &dlComplete)
	}
	if dlComplete.Error != nil {
		t.Fatalf("download error: %v", dlComplete.Error.Message)
	}

	got, err := os.ReadFile(dlComplete.Path)
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("downloaded content does not match source")
	}
}

func TestAgentTerminate(t *testing.T) {
	var input bytes.Buffer
	enc := json.NewEncoder(&input)
	enc.Encode(map[string]string{"event": "terminate"})

	var buf bytes.Buffer
	a := &Agent{enc: json.NewEncoder(&buf)}

	if err := a.run(context.Background(), &input); err != nil {
		t.Errorf("run returned error: %v", err)
	}
}

func TestRunIndexesFromArgs(t *testing.T) {
	var buf bytes.Buffer
	args := []string{"abc123def456", "dead0000cafe1234"}
	if err := runIndexes(args, nil, &buf); err != nil {
		t.Fatalf("runIndexes: %v", err)
	}
	got := buf.String()
	want := "abc1/abc123def456.caibx\ndead/dead0000cafe1234.caibx\n"
	if got != want {
		t.Errorf("runIndexes output = %q, want %q", got, want)
	}
}

func TestRunIndexesFromStdin(t *testing.T) {
	input := strings.NewReader(
		"abc123def4560000 * big-file.bin\n" +
			"deadbeef00001234 - other.bin\n" +
			"\n" + // blank line — should be skipped
			"cafe00001111abcd * third.bin\n",
	)
	var buf bytes.Buffer
	if err := runIndexes(nil, input, &buf); err != nil {
		t.Fatalf("runIndexes: %v", err)
	}
	got := buf.String()
	want := "abc1/abc123def4560000.caibx\ndead/deadbeef00001234.caibx\ncafe/cafe00001111abcd.caibx\n"
	if got != want {
		t.Errorf("runIndexes output = %q, want %q", got, want)
	}
}
