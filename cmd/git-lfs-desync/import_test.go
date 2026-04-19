package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
)

func TestIsValidOID(t *testing.T) {
	good := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if !isValidOID(good) {
		t.Errorf("expected %q to be valid", good)
	}
	for _, bad := range []string{
		"",
		"short",
		"ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789", // uppercase
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678",  // 63 chars
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567890", // 65 chars
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678g", // non-hex
	} {
		if isValidOID(bad) {
			t.Errorf("expected %q to be invalid", bad)
		}
	}
}

func TestVerifyFileSHA256(t *testing.T) {
	dir := t.TempDir()
	data := []byte("hello world")
	oid := hex.EncodeToString(sha256Sum(data))
	good := filepath.Join(dir, oid)
	if err := os.WriteFile(good, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyFileSHA256(good, oid); err != nil {
		t.Errorf("unexpected mismatch for good file: %v", err)
	}

	bad := filepath.Join(dir, "corrupt")
	if err := os.WriteFile(bad, []byte("different"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyFileSHA256(bad, oid); err == nil {
		t.Error("expected mismatch error for corrupt file")
	}
}

// TestRunImport is an end-to-end test of the import flow.
// It lays out three valid LFS objects, one decoy file (not a valid OID),
// and one OID-named file whose contents don't match. It then checks:
//   1. First run uploads the three valid files, flags the mismatch as invalid,
//      and silently ignores the decoy.
//   2. Second run skips everything already uploaded (idempotency).
//   3. One of the uploaded objects round-trips byte-for-byte via GetObjectToFile.
func TestRunImport(t *testing.T) {
	desync.Init(256 * 1024)

	lfsDir := t.TempDir()

	// Three valid LFS objects. Sizes picked to exercise the small-file path
	// (single-chunk) and the large-file path (multi-chunk) of PutObjectFromFile.
	payloads := [][]byte{
		bytes.Repeat([]byte("A"), 100),             // small: single chunk
		bytes.Repeat([]byte("B"), 64*1024),         // medium
		bytes.Repeat([]byte("C"), 2*1024*1024+123), // large: multi-chunk
	}
	validOIDs := make([]string, 0, len(payloads))
	for _, data := range payloads {
		oid := hex.EncodeToString(sha256Sum(data))
		writeLFSObject(t, lfsDir, oid, data)
		validOIDs = append(validOIDs, oid)
	}

	// Decoy: filename is not a valid OID. Must be silently ignored
	// (not counted in discovered).
	if err := os.WriteFile(filepath.Join(lfsDir, "README"), []byte("ignored"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Corrupt: filename is a valid OID shape, but contents hash to something
	// else. Must be counted as invalid.
	corruptOID := hex.EncodeToString(sha256Sum([]byte("real content")))
	writeLFSObject(t, lfsDir, corruptOID, []byte("tampered content"))

	client := newTestClient(t)
	defer client.Close()

	// First import: 3 uploaded, 1 invalid, decoy ignored.
	if err := runImport(context.Background(), lfsDir, client, 2); err == nil {
		t.Error("expected error due to invalid file")
	}
	for _, oid := range validOIDs {
		ok, err := client.IndexStore.HasIndex(cmdshared.OidIndexName(oid))
		if err != nil || !ok {
			t.Errorf("OID %s not found in index store after import (err=%v)", oid, err)
		}
	}
	ok, _ := client.IndexStore.HasIndex(cmdshared.OidIndexName(corruptOID))
	if ok {
		t.Errorf("corrupt OID %s must not be in the index store", corruptOID)
	}

	// Second import: idempotent. Valid files skipped; corrupt still flagged.
	if err := runImport(context.Background(), lfsDir, client, 2); err == nil {
		t.Error("expected error due to invalid file on re-run")
	}

	// Round-trip: reassemble one object and compare to the original payload.
	roundTripFile := filepath.Join(t.TempDir(), "assembled")
	if err := client.GetObjectToFile(cmdshared.OidIndexName(validOIDs[2]), roundTripFile, nil); err != nil {
		t.Fatalf("GetObjectToFile: %v", err)
	}
	got, err := os.ReadFile(roundTripFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payloads[2]) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d", len(got), len(payloads[2]))
	}
}

func sha256Sum(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

// writeLFSObject creates <root>/<oid[0:2]>/<oid[2:4]>/<oid> with data.
func writeLFSObject(t *testing.T, root, oid string, data []byte) {
	t.Helper()
	dir := filepath.Join(root, oid[0:2], oid[2:4])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, oid), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func newTestClient(t *testing.T) *desync.Client {
	t.Helper()
	chunkStore, err := desync.NewLocalStore(t.TempDir(), desync.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	indexStore, err := desync.NewLocalIndexStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return desync.NewClient(chunkStore, chunkStore, indexStore, desync.ClientOptions{
		MinChunk: 16 * 1024,
		AvgChunk: 64 * 1024,
		MaxChunk: 256 * 1024,
		N:        4,
	})
}
