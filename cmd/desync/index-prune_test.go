package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexPruneCommand(t *testing.T) {
	// Create a temp index store and copy both test indexes into it
	indexStoreDir := t.TempDir()

	for _, name := range []string{"blob1.caibx", "blob2.caibx"} {
		src, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(indexStoreDir, name), src, 0644)
		require.NoError(t, err)
	}

	// Prune: keep only blob2.caibx
	cmd := newIndexPruneCommand(context.Background())
	cmd.SetArgs([]string{"--index-store", indexStoreDir, "blob2.caibx", "--yes"})
	_, err := cmd.ExecuteC()
	require.NoError(t, err)

	// blob1.caibx should be gone
	_, err = os.Stat(filepath.Join(indexStoreDir, "blob1.caibx"))
	require.True(t, os.IsNotExist(err))

	// blob2.caibx should still be present
	_, err = os.Stat(filepath.Join(indexStoreDir, "blob2.caibx"))
	require.NoError(t, err)
}
