package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/pktline"
	"github.com/stretchr/testify/require"
)

// testServer creates a Server backed by local stores in temporary directories.
func testServer(t *testing.T, operation string) *Server {
	t.Helper()
	chunkDir := t.TempDir()
	indexDir := t.TempDir()

	chunkStore, err := desync.NewLocalStore(chunkDir, desync.StoreOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { chunkStore.Close() })

	indexStore, err := desync.NewLocalIndexStore(indexDir)
	require.NoError(t, err)
	t.Cleanup(func() { indexStore.Close() })

	return &Server{
		operation:  operation,
		writeStore: chunkStore,
		readStore:  chunkStore,
		indexStore: indexStore,
		n:          2,
		minChunk:   16 * 1024,
		avgChunk:   64 * 1024,
		maxChunk:   256 * 1024,
		tmpDir:     t.TempDir(),
	}
}

// runSession writes the client-side pkt-line messages, runs the server, and
// returns the server's output.
func runSession(t *testing.T, srv *Server, clientInput func(w *pktline.Writer)) []byte {
	t.Helper()
	var clientBuf, serverBuf bytes.Buffer
	clientInput(pktline.NewWriter(&clientBuf))
	err := srv.Run(context.Background(), &clientBuf, &serverBuf)
	require.NoError(t, err)
	return serverBuf.Bytes()
}

func TestVersionNegotiation(t *testing.T) {
	srv := testServer(t, "download")

	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		// quit
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))

	// Capability advertisement.
	cap, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "version=1", cap)
	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)

	// Version accepted.
	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)
	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)

	// Quit response.
	status, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)
	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)
}

func TestBatchDownloadExistingObject(t *testing.T) {
	srv := testServer(t, "download")

	// Upload a test object first.
	oid, size := uploadTestObject(t, srv, []byte("hello world, this is a test file for LFS transfer"))

	// Now test batch download.
	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		// batch
		w.WritePacketText("batch")
		w.WriteDelim()
		w.WritePacketText(fmt.Sprintf("%s %d", oid, size))
		w.WriteFlush()
		// quit
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	// Batch response.
	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)

	// hash-algo argument.
	algo, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "hash-algo=sha256", algo)

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrDelim)

	// OID response line.
	line, err := r.ReadPacketText()
	require.NoError(t, err)
	parts := strings.Fields(line)
	require.Len(t, parts, 3)
	require.Equal(t, oid, parts[0])
	require.Equal(t, fmt.Sprintf("%d", size), parts[1])
	require.Equal(t, "download", parts[2])

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)
}

func TestBatchDownloadMissingObject(t *testing.T) {
	srv := testServer(t, "download")

	fakeOID := strings.Repeat("ab", 32)
	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText("batch")
		w.WriteDelim()
		w.WritePacketText(fmt.Sprintf("%s 1234", fakeOID))
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)
	r.ReadPacket() // hash-algo
	r.ReadPacket() // delim

	line, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Contains(t, line, "noop")
}

func TestBatchUploadNewObject(t *testing.T) {
	srv := testServer(t, "upload")

	fakeOID := strings.Repeat("cd", 32)
	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText("batch")
		w.WriteDelim()
		w.WritePacketText(fmt.Sprintf("%s 1234", fakeOID))
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)
	r.ReadPacket() // hash-algo
	r.ReadPacket() // delim

	line, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Contains(t, line, "upload")
}

func TestPutAndGetObject(t *testing.T) {
	srv := testServer(t, "upload")

	data := generateTestData(100 * 1024) // 100KB
	oid := sha256Hex(data)
	size := len(data)

	// Upload via put-object.
	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		// put-object
		w.WritePacketText(fmt.Sprintf("put-object %s", oid))
		w.WritePacketText(fmt.Sprintf("size=%d", size))
		w.WriteDelim()
		// Send data in chunks.
		for off := 0; off < len(data); {
			end := off + 60000
			if end > len(data) {
				end = len(data)
			}
			w.WriteBinaryPacket(data[off:end])
			off = end
		}
		w.WriteFlush()
		// quit
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	// put-object response.
	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)
	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)

	// Now download via get-object.
	srv.operation = "download"
	out = runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText(fmt.Sprintf("get-object %s", oid))
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r = pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	// get-object response.
	status, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)

	sizeArg, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("size=%d", size), sizeArg)

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrDelim)

	// Read binary data.
	var received bytes.Buffer
	for {
		pkt, readErr := r.ReadRawPacket()
		if errors.Is(readErr, pktline.ErrFlush) {
			break
		}
		require.NoError(t, readErr)
		received.Write(pkt)
	}

	require.Equal(t, size, received.Len())
	require.Equal(t, oid, sha256Hex(received.Bytes()))
}

func TestVerifyObject(t *testing.T) {
	srv := testServer(t, "upload")

	data := []byte("verify me please, this needs to be long enough to chunk properly or at least to test")
	oid, size := uploadTestObject(t, srv, data)

	// verify-object with correct size.
	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText(fmt.Sprintf("verify-object %s", oid))
		w.WritePacketText(fmt.Sprintf("size=%d", size))
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", status)
}

func TestVerifyObjectSizeMismatch(t *testing.T) {
	srv := testServer(t, "upload")

	data := []byte("verify mismatch test data that should be somewhat long")
	oid, _ := uploadTestObject(t, srv, data)

	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText(fmt.Sprintf("verify-object %s", oid))
		w.WritePacketText("size=999999")
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 409", status)
}

func TestVerifyObjectNotFound(t *testing.T) {
	srv := testServer(t, "upload")

	fakeOID := strings.Repeat("ee", 32)
	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText(fmt.Sprintf("verify-object %s", fakeOID))
		w.WritePacketText("size=100")
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 404", status)
}

func TestResolveConfigWalk(t *testing.T) {
	// Create a directory structure with a config file in a parent.
	root := t.TempDir()
	repoDir := filepath.Join(root, "repos", "myrepo.git")
	require.NoError(t, os.MkdirAll(repoDir, 0755))

	configContent := `{
		"defaults": {
			"stores": ["desync-lfs/chunks"],
			"index-store": "desync-lfs/index"
		}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(root, configFileName), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)

	// Relative paths should be resolved against repoDir.
	require.Equal(t, filepath.Join(repoDir, "desync-lfs", "chunks"), cfg.ResolveStore(""))
	require.Equal(t, filepath.Join(repoDir, "desync-lfs", "index"), cfg.ResolveIndexStore(""))
}

func TestResolveConfigPerRepo(t *testing.T) {
	repoDir := t.TempDir()

	configContent := `{
		"defaults": {
			"stores": ["/absolute/chunks"],
			"index-store": "/absolute/index"
		}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, configFileName), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)

	require.Equal(t, "/absolute/chunks", cfg.ResolveStore(""))
	require.Equal(t, "/absolute/index", cfg.ResolveIndexStore(""))
}

func TestResolveConfigConventionFallback(t *testing.T) {
	repoDir := t.TempDir()
	// No config file anywhere — should fall back to convention.

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)

	require.Equal(t, filepath.Join(repoDir, "desync-lfs", "chunks"), cfg.ResolveStore(""))
	require.Equal(t, filepath.Join(repoDir, "desync-lfs", "index"), cfg.ResolveIndexStore(""))
}

func TestResolveConfigDeriveIndex(t *testing.T) {
	repoDir := t.TempDir()

	configContent := `{"defaults": {"stores": ["desync-lfs/chunks"]}}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, configFileName), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)

	require.Equal(t, filepath.Join(repoDir, "desync-lfs", "chunks"), cfg.ResolveStore(""))
	// Index derived from store.
	require.Equal(t, filepath.Join(repoDir, "desync-lfs", "index"), cfg.ResolveIndexStore(""))
}

func TestResolveConfigPathTemplate(t *testing.T) {
	repoDir := t.TempDir()

	configContent := `{
		"defaults": {
			"stores": ["s3+https://bucket/%(path)/chunks/"],
			"index-store": "s3+https://bucket/%(path)/index/"
		}
	}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, configFileName), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)

	require.Equal(t, fmt.Sprintf("s3+https://bucket/%s/chunks/", repoDir), cfg.ResolveStore(""))
	require.Equal(t, fmt.Sprintf("s3+https://bucket/%s/index/", repoDir), cfg.ResolveIndexStore(""))
}

func TestUnknownCommand(t *testing.T) {
	srv := testServer(t, "download")

	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText("unknown-cmd foo")
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	status, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 400", status)
}

// --- helpers ---

// uploadTestObject uploads data through the server and returns OID and size.
func uploadTestObject(t *testing.T, srv *Server, data []byte) (string, int64) {
	t.Helper()
	oid := sha256Hex(data)
	size := int64(len(data))

	// Create the index and chunks directly using desync APIs.
	tmpFile := filepath.Join(t.TempDir(), "upload.tmp")
	require.NoError(t, os.WriteFile(tmpFile, data, 0644))

	idx, _, err := desync.IndexFromFile(context.Background(), tmpFile, 2,
		srv.minChunk, srv.avgChunk, srv.maxChunk, desync.NullProgressBar{})
	require.NoError(t, err)

	err = desync.ChopFile(context.Background(), tmpFile, idx.Chunks, srv.writeStore, 2, desync.NullProgressBar{}, nil, 0)
	require.NoError(t, err)

	indexName := oidIndexName(oid)
	require.NoError(t, srv.indexStore.StoreIndex(indexName, idx))

	return oid, size
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func generateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

// skipCapAndVersion reads and discards the capability advertisement and
// version response from the server output.
func skipCapAndVersion(t *testing.T, r *pktline.Reader) {
	t.Helper()
	// Capability: "version=1" + flush.
	_, err := r.ReadPacket()
	require.NoError(t, err)
	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)
	// Version accepted: "status 200" + flush.
	_, err = r.ReadPacket()
	require.NoError(t, err)
	_, err = r.ReadPacket()
	require.ErrorIs(t, err, pktline.ErrFlush)
}
