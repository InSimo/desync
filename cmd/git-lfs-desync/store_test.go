package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/desyncconfig"
)

func TestChunkStoreFromLocalPath(t *testing.T) {
	dir := t.TempDir()
	store, err := desyncconfig.WritableStore(dir, desyncconfig.Config{}, desyncconfig.CmdStoreOptions{})
	if err != nil {
		t.Fatalf("WritableStore(%q): %v", dir, err)
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
	store, err := desyncconfig.WritableIndexStore(dir, desyncconfig.Config{}, desyncconfig.CmdStoreOptions{})
	if err != nil {
		t.Fatalf("WritableIndexStore(%q): %v", dir, err)
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
	_, err := desyncconfig.WritableStore("ssh://host/path", desyncconfig.Config{}, desyncconfig.CmdStoreOptions{})
	if err == nil {
		t.Fatal("expected error for ssh:// chunk store, got nil")
	}
	if !strings.Contains(err.Error(), "writing") {
		t.Errorf("expected 'writing' in error, got: %v", err)
	}
}

func TestIndexStoreSSHSchemeError(t *testing.T) {
	_, err := desyncconfig.WritableIndexStore("ssh://host/path", desyncconfig.Config{}, desyncconfig.CmdStoreOptions{})
	if err == nil {
		t.Fatal("expected error for ssh:// index store, got nil")
	}
}

func TestChunkStoreLocalPathNotExist(t *testing.T) {
	_, err := desyncconfig.WritableStore("/nonexistent/path/does/not/exist", desyncconfig.Config{}, desyncconfig.CmdStoreOptions{})
	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
}

func TestIndexStoreLocalPathNotExist(t *testing.T) {
	_, err := desyncconfig.WritableIndexStore("/nonexistent/path/does/not/exist", desyncconfig.Config{}, desyncconfig.CmdStoreOptions{})
	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
}
