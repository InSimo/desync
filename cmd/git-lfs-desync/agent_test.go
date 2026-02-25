package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/folbricht/desync"
)

func TestDeriveIndexURL(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
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
