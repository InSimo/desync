package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

// countFilesWithSuffix counts files under root whose name ends with suffix.
func countFilesWithSuffix(t *testing.T, root, suffix string) int {
	t.Helper()
	var n int
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, suffix) {
			n++
		}
		return nil
	})
	require.NoError(t, err)
	return n
}

// TestSafePruneCommand verifies the two-run safe pruning protocol end-to-end
// using the CLI. blob1 chunks exclusive to blob1 progress through:
//
//	run 1 → marked with .prunable (chunk still readable)
//	run 2 → chunk deleted outright (no .pruning quarantine step)
func TestSafePruneCommand(t *testing.T) {
	store := t.TempDir()

	// Populate the store with blob1 chunks.
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)

	// Run 1: marks unreferenced chunks (blob1-only) with .prunable companions.
	pruneCmd1 := newPruneCommand(context.Background())
	pruneCmd1.SetArgs([]string{"--safe-pruning", "-s", store, "testdata/blob2.caibx", "--yes"})
	_, err = pruneCmd1.ExecuteC()
	require.NoError(t, err)

	prunableAfterRun1 := countFilesWithSuffix(t, store, ".prunable")
	pruningAfterRun1 := countFilesWithSuffix(t, store, ".pruning")
	require.Greater(t, prunableAfterRun1, 0, "run 1 must create .prunable markers")
	require.Equal(t, 0, pruningAfterRun1, "run 1 must not create any .pruning files")

	// Run 2: deletes chunks that are still marked and have no .protect companion.
	pruneCmd2 := newPruneCommand(context.Background())
	pruneCmd2.SetArgs([]string{"--safe-pruning", "-s", store, "testdata/blob2.caibx", "--yes"})
	_, err = pruneCmd2.ExecuteC()
	require.NoError(t, err)

	prunableAfterRun2 := countFilesWithSuffix(t, store, ".prunable")
	pruningAfterRun2 := countFilesWithSuffix(t, store, ".pruning")
	require.Equal(t, 0, prunableAfterRun2, "run 2 must clear all .prunable markers")
	require.Equal(t, 0, pruningAfterRun2, "run 2 must not create any .pruning files")
}
