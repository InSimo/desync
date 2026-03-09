package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// blob1 store, pruned to keep only blob2 chunks:
//
//	131 chunks total (blob1 unique)
//	124 shared with blob2 → kept
//	  7 blob1-only        → deletable
const (
	pruneKeptCount      = 124
	pruneKeptStored     = 974447
	pruneKeptDedup      = 1034083
	pruneDeletableCount = 7
	pruneDeletableStored = 76027
	pruneNumIndexes     = 1
	pruneTotalIndexSize = 2097152

	// blob2_without_data.caibx references 131 unique chunks, 7 of which are
	// absent from the blob1 store.
	pruneMissingCount = 7
)

// chopBlob1 creates a fresh local store and populates it with all chunks from blob1.
func chopBlob1(t *testing.T) string {
	t.Helper()
	store := t.TempDir()
	chopCmd := newChopCommand(context.Background())
	chopCmd.SetArgs([]string{"-s", store, "testdata/blob1.caibx", "testdata/blob1"})
	chopCmd.SetOutput(io.Discard)
	_, err := chopCmd.ExecuteC()
	require.NoError(t, err)
	return store
}

// runPruneJSON runs the prune command with --format=json and returns the
// decoded output.  extra args are appended after the fixed store / format
// flags.
func runPruneJSON(t *testing.T, store string, extraArgs ...string) (pruneStatsJSON, error) {
	t.Helper()
	buf := new(bytes.Buffer)
	prev := stdout
	stdout = buf
	t.Cleanup(func() { stdout = prev })

	args := append([]string{"-s", store, "--format", "json"}, extraArgs...)
	pruneCmd := newPruneCommand(context.Background())
	pruneCmd.SetArgs(args)
	pruneCmd.SetOutput(io.Discard)
	_, cmdErr := pruneCmd.ExecuteC()

	var out pruneStatsJSON
	if buf.Len() > 0 {
		require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	}
	return out, cmdErr
}

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

func TestPruneCommandJSONDryRun(t *testing.T) {
	store := chopBlob1(t)

	out, err := runPruneJSON(t, store, "--dry-run", "testdata/blob2.caibx")
	require.NoError(t, err)

	require.True(t, out.DryRun)
	require.False(t, out.SafePruning)

	require.Equal(t, pruneDeletableCount, out.Deletable.Count)
	require.Equal(t, int64(pruneDeletableStored), out.Deletable.StoredSize)
	require.Equal(t, int64(0), out.Deletable.DeduplicatedSize)

	require.Equal(t, pruneKeptCount, out.Kept.Count)
	require.Equal(t, int64(pruneKeptStored), out.Kept.StoredSize)
	require.Equal(t, int64(pruneKeptDedup), out.Kept.DeduplicatedSize)

	require.Equal(t, pruneNumIndexes, out.NumIndexes)
	require.Equal(t, int64(pruneTotalIndexSize), out.TotalIndexSize)

	// Dry-run must not delete anything.
	require.Equal(t, 131, countFilesWithSuffix(t, store, ".cacnk"))
}

func TestPruneCommandJSONRealRun(t *testing.T) {
	store := chopBlob1(t)

	out, err := runPruneJSON(t, store, "--yes", "testdata/blob2.caibx")
	require.NoError(t, err)

	require.False(t, out.DryRun)
	require.False(t, out.SafePruning)

	require.Equal(t, pruneDeletableCount, out.Deletable.Count)
	require.Equal(t, int64(pruneDeletableStored), out.Deletable.StoredSize)

	require.Equal(t, pruneKeptCount, out.Kept.Count)
	require.Equal(t, int64(pruneKeptStored), out.Kept.StoredSize)
	require.Equal(t, int64(pruneKeptDedup), out.Kept.DeduplicatedSize)

	require.Equal(t, pruneNumIndexes, out.NumIndexes)
	require.Equal(t, int64(pruneTotalIndexSize), out.TotalIndexSize)

	// Deletable chunks must be gone.
	require.Equal(t, pruneKeptCount, countFilesWithSuffix(t, store, ".cacnk"))
}

func TestPruneCommandJSONReportMissingDryRun(t *testing.T) {
	store := chopBlob1(t)

	// blob2_without_data.caibx references 7 chunks that are not in the store.
	out, err := runPruneJSON(t, store, "--dry-run", "--report-missing", "testdata/blob2_without_data.caibx")
	require.Error(t, err, "prune must exit non-zero when missing chunks are detected")

	require.NotNil(t, out.Missing)
	require.Equal(t, pruneMissingCount, out.Missing.Total)
	require.Len(t, out.Missing.Indexes, 1)

	idx := out.Missing.Indexes[0]
	require.Equal(t, "testdata/blob2_without_data.caibx", idx.Name)
	require.Equal(t, 131, idx.TotalChunks)
	require.Len(t, idx.MissingChunks, pruneMissingCount)
}

func TestPruneCommandJSONReportMissingRealRun(t *testing.T) {
	store := chopBlob1(t)

	out, err := runPruneJSON(t, store, "--yes", "--report-missing", "testdata/blob2_without_data.caibx")
	require.Error(t, err, "prune must exit non-zero when missing chunks are detected")

	require.NotNil(t, out.Missing)
	require.Equal(t, pruneMissingCount, out.Missing.Total)
	require.Len(t, out.Missing.Indexes, 1)

	// The 7 deletable (blob1-only) chunks must have been removed.
	require.Equal(t, pruneKeptCount, countFilesWithSuffix(t, store, ".cacnk"))
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

func TestSafePruneCommandJSONDryRun(t *testing.T) {
	store := chopBlob1(t)

	// With an unmarked store, dry-run reports what run 1 would mark.
	out, err := runPruneJSON(t, store, "--safe-pruning", "--dry-run", "testdata/blob2.caibx")
	require.NoError(t, err)

	require.True(t, out.DryRun)
	require.True(t, out.SafePruning)

	// Nothing is deletable yet (no .prunable markers exist).
	require.Equal(t, 0, out.Deletable.Count)

	require.NotNil(t, out.Prunable)
	require.Equal(t, pruneDeletableCount, out.Prunable.Count)
	require.Equal(t, int64(pruneDeletableStored), out.Prunable.StoredSize)

	require.Equal(t, pruneKeptCount, out.Kept.Count)
	require.Equal(t, int64(pruneKeptStored), out.Kept.StoredSize)
	require.Equal(t, int64(pruneKeptDedup), out.Kept.DeduplicatedSize)

	require.Equal(t, pruneNumIndexes, out.NumIndexes)
	require.Equal(t, int64(pruneTotalIndexSize), out.TotalIndexSize)

	// Dry-run must not create any markers.
	require.Equal(t, 0, countFilesWithSuffix(t, store, ".prunable"))
}

func TestSafePruneCommandJSONRun1(t *testing.T) {
	store := chopBlob1(t)

	out, err := runPruneJSON(t, store, "--safe-pruning", "--yes", "testdata/blob2.caibx")
	require.NoError(t, err)

	require.False(t, out.DryRun)
	require.True(t, out.SafePruning)

	// Run 1 marks, does not delete.
	require.Equal(t, 0, out.Deletable.Count)
	require.NotNil(t, out.Prunable)
	require.Equal(t, pruneDeletableCount, out.Prunable.Count)
	require.Equal(t, int64(pruneDeletableStored), out.Prunable.StoredSize)

	require.Equal(t, pruneKeptCount, out.Kept.Count)
	require.Equal(t, int64(pruneKeptStored), out.Kept.StoredSize)
	require.Equal(t, int64(pruneKeptDedup), out.Kept.DeduplicatedSize)

	// All chunks still present after run 1.
	require.Equal(t, 131, countFilesWithSuffix(t, store, ".cacnk"))
	require.Equal(t, pruneDeletableCount, countFilesWithSuffix(t, store, ".prunable"))
}

func TestSafePruneCommandJSONRun2(t *testing.T) {
	store := chopBlob1(t)

	// Run 1: mark.
	_, err := runPruneJSON(t, store, "--safe-pruning", "--yes", "testdata/blob2.caibx")
	require.NoError(t, err)

	// Run 2: delete.
	out, err := runPruneJSON(t, store, "--safe-pruning", "--yes", "testdata/blob2.caibx")
	require.NoError(t, err)

	require.Equal(t, pruneDeletableCount, out.Deletable.Count)
	require.Equal(t, int64(pruneDeletableStored), out.Deletable.StoredSize)

	// No Normal chunks to mark in run 2 (all were already marked in run 1).
	require.Nil(t, out.Prunable)

	require.Equal(t, pruneKeptCount, out.Kept.Count)
	require.Equal(t, int64(pruneKeptStored), out.Kept.StoredSize)

	// Deletable chunks removed; no markers remain.
	require.Equal(t, pruneKeptCount, countFilesWithSuffix(t, store, ".cacnk"))
	require.Equal(t, 0, countFilesWithSuffix(t, store, ".prunable"))
}
