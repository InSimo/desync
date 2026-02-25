package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/folbricht/desync"
)

func TestChunkStoreFromLocalPath(t *testing.T) {
	dir := t.TempDir()
	store, err := chunkStoreFromURL(dir, desync.StoreOptions{})
	if err != nil {
		t.Fatalf("chunkStoreFromURL(%q): %v", dir, err)
	}
	defer store.Close()

	data := bytes.Repeat([]byte("test-chunk-data-local"), 100)
	chunk := desync.NewChunk(data)
	id := chunk.ID()

	if err := store.StoreChunk(chunk); err != nil {
		t.Fatalf("StoreChunk: %v", err)
	}
	got, err := store.GetChunk(id)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	gotData, err := got.Data()
	if err != nil {
		t.Fatalf("Data: %v", err)
	}
	if !bytes.Equal(gotData, data) {
		t.Error("retrieved chunk data does not match original")
	}
}

func TestIndexStoreFromLocalPath(t *testing.T) {
	dir := t.TempDir()
	store, err := indexStoreFromURL(dir, desync.StoreOptions{})
	if err != nil {
		t.Fatalf("indexStoreFromURL(%q): %v", dir, err)
	}
	defer store.Close()

	// Build a minimal but valid index (feature flags must match the library's digest algorithm).
	idx := desync.Index{
		Index: desync.FormatIndex{
			FeatureFlags: desync.CaFormatSHA512256 | desync.CaFormatExcludeNoDump,
		},
	}
	const name = "test.caibx"
	if err := store.StoreIndex(name, idx); err != nil {
		t.Fatalf("StoreIndex: %v", err)
	}
	got, err := store.GetIndex(name)
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if len(got.Chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(got.Chunks))
	}
}

func TestChunkStoreSSHSchemeError(t *testing.T) {
	_, err := chunkStoreFromURL("ssh://host/path", desync.StoreOptions{})
	if err == nil {
		t.Fatal("expected error for ssh:// chunk store, got nil")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("expected 'read-only' in error, got: %v", err)
	}
}

func TestIndexStoreSSHSchemeError(t *testing.T) {
	_, err := indexStoreFromURL("ssh://host/path", desync.StoreOptions{})
	if err == nil {
		t.Fatal("expected error for ssh:// index store, got nil")
	}
}

func TestChunkStoreLocalPathNotExist(t *testing.T) {
	_, err := chunkStoreFromURL("/nonexistent/path/does/not/exist", desync.StoreOptions{})
	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
}

func TestIndexStoreLocalPathNotExist(t *testing.T) {
	_, err := indexStoreFromURL("/nonexistent/path/does/not/exist", desync.StoreOptions{})
	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
}
