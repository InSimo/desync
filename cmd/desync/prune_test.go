package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPruneCommandIndexStore(t *testing.T) {
	// Create a blank store and populate it with chunks from blob1
	store := t.TempDir()
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// Create a temp index store dir and copy blob2.caibx into it
	indexStoreDir := t.TempDir()
	src, err := os.ReadFile("testdata/blob2.caibx")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(indexStoreDir, "blob2.caibx"), src, 0644)
	require.NoError(t, err)

	// Prune using --index-store
	pruneCmd := newPruneCommand(context.Background())
	pruneCmd.SetArgs([]string{"-s", store, "--index-store", indexStoreDir, "--yes"})
	_, err = pruneCmd.ExecuteC()
	require.NoError(t, err)
}

func TestPruneCommand(t *testing.T) {
	// Create a blank store
	store := t.TempDir()

	// Run a "chop" command to populate the store
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// Now prune the store. Using a different index that doesn't have the exact same chunks
	pruneCmd := newPruneCommand(context.Background())
	pruneCmd.SetArgs([]string{"-s", store, "testdata/blob2.caibx", "--yes"})
	_, err = pruneCmd.ExecuteC()
	require.NoError(t, err)
}
