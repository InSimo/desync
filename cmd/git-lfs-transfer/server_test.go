package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

// --- git config key tests ---

// initGitRepo runs "git init" in dir with a dummy identity so commits work.
func initGitRepo(t *testing.T, dir string) {
	t.Helper()
	cmds := [][]string{
		{"git", "init", dir},
		{"git", "-C", dir, "config", "user.email", "test@test.com"},
		{"git", "-C", dir, "config", "user.name", "Test"},
	}
	for _, args := range cmds {
		require.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}
}

// gitSetConfig sets a git config key in dir.
func gitSetConfig(t *testing.T, dir, key, value string) {
	t.Helper()
	require.NoError(t, exec.Command("git", "-C", dir, "config", key, value).Run())
}

func TestResolveConfigGitConfigPath_Relative(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)
	gitSetConfig(t, repoDir, "desync-lfs.config.path", "myconfig.json")

	configContent := `{"defaults": {"stores": ["/custom/chunks"], "index-store": "/custom/index"}}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "myconfig.json"), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	require.Equal(t, "/custom/chunks", cfg.ResolveStore(""))
	require.Equal(t, "/custom/index", cfg.ResolveIndexStore(""))
}

func TestResolveConfigGitConfigPath_Absolute(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	cfgFile := filepath.Join(t.TempDir(), "absolute.json")
	configContent := `{"defaults": {"stores": ["/abs/chunks"], "index-store": "/abs/index"}}`
	require.NoError(t, os.WriteFile(cfgFile, []byte(configContent), 0644))
	gitSetConfig(t, repoDir, "desync-lfs.config.path", cfgFile)

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	require.Equal(t, "/abs/chunks", cfg.ResolveStore(""))
	require.Equal(t, "/abs/index", cfg.ResolveIndexStore(""))
}

func TestResolveConfigGitConfigPath_WalkUp(t *testing.T) {
	root := t.TempDir()
	repoDir := filepath.Join(root, "repos", "myrepo.git")
	require.NoError(t, os.MkdirAll(repoDir, 0755))
	initGitRepo(t, repoDir)
	gitSetConfig(t, repoDir, "desync-lfs.config.path", "custom.json")

	// Place the file in a parent directory, not repoDir itself.
	configContent := `{"defaults": {"stores": ["/walked/chunks"], "index-store": "/walked/index"}}`
	require.NoError(t, os.WriteFile(filepath.Join(root, "custom.json"), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	require.Equal(t, "/walked/chunks", cfg.ResolveStore(""))
}

func TestResolveConfigGitConfigObject_Found(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	configContent := `{"defaults": {"stores": ["/git/obj/chunks"], "index-store": "/git/obj/index"}}`
	cfgPath := filepath.Join(repoDir, "desync-lfs.json")
	require.NoError(t, os.WriteFile(cfgPath, []byte(configContent), 0644))
	require.NoError(t, exec.Command("git", "-C", repoDir, "add", "desync-lfs.json").Run())
	require.NoError(t, exec.Command("git", "-C", repoDir, "commit", "-m", "add config").Run())

	gitSetConfig(t, repoDir, "desync-lfs.config.object", "HEAD:desync-lfs.json")

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	require.Equal(t, "/git/obj/chunks", cfg.ResolveStore(""))
	require.Equal(t, "/git/obj/index", cfg.ResolveIndexStore(""))
}

func TestResolveConfigGitConfigObject_NotFound_FallsBackToPath(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)
	// Point at an object that doesn't exist.
	gitSetConfig(t, repoDir, "desync-lfs.config.object", "HEAD:nonexistent.json")

	// Provide a regular config file to fall back to.
	configContent := `{"defaults": {"stores": ["/fallback/chunks"], "index-store": "/fallback/index"}}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, configFileName), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	require.Equal(t, "/fallback/chunks", cfg.ResolveStore(""))
}

func TestResolveConfigGitConfigObject_TakesPrecedenceOverPath(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)

	// Commit the object config.
	objContent := `{"defaults": {"stores": ["/from/object/chunks"], "index-store": "/from/object/index"}}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "desync-lfs.json"), []byte(objContent), 0644))
	require.NoError(t, exec.Command("git", "-C", repoDir, "add", "desync-lfs.json").Run())
	require.NoError(t, exec.Command("git", "-C", repoDir, "commit", "-m", "add config").Run())

	// Also write a different file on disk that the path key points to.
	pathContent := `{"defaults": {"stores": ["/from/path/chunks"], "index-store": "/from/path/index"}}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "other.json"), []byte(pathContent), 0644))

	gitSetConfig(t, repoDir, "desync-lfs.config.object", "HEAD:desync-lfs.json")
	gitSetConfig(t, repoDir, "desync-lfs.config.path", "other.json")

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	// Object must win.
	require.Equal(t, "/from/object/chunks", cfg.ResolveStore(""))
}

func TestResolveConfigNoGitRepo(t *testing.T) {
	// Plain temp dir — not a git repo. gitConfigValue should return "" silently.
	repoDir := t.TempDir()
	configContent := `{"defaults": {"stores": ["/plain/chunks"], "index-store": "/plain/index"}}`
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, configFileName), []byte(configContent), 0644))

	cfg, err := resolveConfig(repoDir)
	require.NoError(t, err)
	require.Equal(t, "/plain/chunks", cfg.ResolveStore(""))
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

// --- size validation tests ---

func TestParseSize(t *testing.T) {
	cases := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"1073741824", 1 << 30, false},
		{"-1", 0, true},
		{"-999", 0, true},
		{"abc", 0, true},
		{"", 0, true},
	}
	for _, tc := range cases {
		args := map[string]string{"size": tc.input}
		got, err := parseSize(args)
		if tc.wantErr {
			require.Error(t, err, "parseSize(%q) should error", tc.input)
		} else {
			require.NoError(t, err, "parseSize(%q) should not error", tc.input)
			require.Equal(t, tc.want, got)
		}
	}
	// Missing key.
	_, err := parseSize(map[string]string{})
	require.Error(t, err)
}

func TestPutObjectNegativeSize(t *testing.T) {
	srv := testServer(t, "upload")
	oid := strings.Repeat("aa", 32)

	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText("put-object " + oid)
		w.WritePacketText("size=-1")
		w.WriteDelim()
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

func TestPutObjectTooLarge(t *testing.T) {
	srv := testServer(t, "upload")
	oid := strings.Repeat("bb", 32)
	tooBig := maxObjectSize + 1

	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText("put-object " + oid)
		w.WritePacketText(fmt.Sprintf("size=%d", tooBig))
		w.WriteDelim()
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

// --- git repo validation tests ---

func TestValidateGitRepo_Valid(t *testing.T) {
	repoDir := t.TempDir()
	initGitRepo(t, repoDir)
	require.NoError(t, validateGitRepo(repoDir))
}

func TestValidateGitRepo_NotARepo(t *testing.T) {
	dir := t.TempDir()
	err := validateGitRepo(dir)
	require.Error(t, err)
}

func TestValidateGitRepo_NonExistent(t *testing.T) {
	err := validateGitRepo("/nonexistent/path/that/cannot/exist")
	require.Error(t, err)
}

// --- OID validation tests ---

func TestValidOID(t *testing.T) {
	cases := []struct {
		oid   string
		valid bool
	}{
		// Valid: exactly 64 lowercase hex chars.
		{strings.Repeat("a", 64), true},
		{strings.Repeat("0", 64), true},
		{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", true},
		// Wrong length.
		{"", false},
		{strings.Repeat("a", 63), false},
		{strings.Repeat("a", 65), false},
		// Uppercase not allowed.
		{strings.Repeat("A", 64), false},
		{"E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855", false},
		// Path traversal attempts.
		{"../../etc/passwd" + strings.Repeat("a", 48), false},
		{strings.Repeat(".", 64), false},
		{strings.Repeat("/", 64), false},
		// Non-hex chars.
		{strings.Repeat("g", 64), false},
		{strings.Repeat("z", 64), false},
	}
	for _, tc := range cases {
		if got := validOID(tc.oid); got != tc.valid {
			t.Errorf("validOID(%q) = %v, want %v", tc.oid, got, tc.valid)
		}
	}
}

func TestGetObjectInvalidOID(t *testing.T) {
	srv := testServer(t, "download")

	for _, badOID := range []string{"", "short", "../../etc/passwd" + strings.Repeat("a", 48), strings.Repeat("G", 64)} {
		out := runSession(t, srv, func(w *pktline.Writer) {
			w.WritePacketText("version 1")
			w.WriteFlush()
			w.WritePacketText("get-object " + badOID)
			w.WriteFlush()
			w.WritePacketText("quit")
			w.WriteFlush()
		})
		r := pktline.NewReader(bytes.NewReader(out))
		skipCapAndVersion(t, r)
		status, err := r.ReadPacketText()
		require.NoError(t, err)
		require.Equal(t, "status 404", status, "expected 404 for invalid OID %q", badOID)
	}
}

func TestPutObjectInvalidOID(t *testing.T) {
	srv := testServer(t, "upload")

	data := []byte("some data")
	for _, badOID := range []string{"", "short", "../../etc/shadow" + strings.Repeat("a", 48), strings.Repeat("G", 64)} {
		out := runSession(t, srv, func(w *pktline.Writer) {
			w.WritePacketText("version 1")
			w.WriteFlush()
			w.WritePacketText("put-object " + badOID)
			w.WritePacketText(fmt.Sprintf("size=%d", len(data)))
			w.WriteDelim()
			w.WriteBinaryPacket(data)
			w.WriteFlush()
			w.WritePacketText("quit")
			w.WriteFlush()
		})
		r := pktline.NewReader(bytes.NewReader(out))
		skipCapAndVersion(t, r)
		status, err := r.ReadPacketText()
		require.NoError(t, err)
		require.Equal(t, "status 400", status, "expected 400 for invalid OID %q", badOID)
	}
}

func TestVerifyObjectInvalidOID(t *testing.T) {
	srv := testServer(t, "upload")

	for _, badOID := range []string{"", "short", strings.Repeat("G", 64)} {
		out := runSession(t, srv, func(w *pktline.Writer) {
			w.WritePacketText("version 1")
			w.WriteFlush()
			w.WritePacketText("verify-object " + badOID)
			w.WritePacketText("size=10")
			w.WriteFlush()
			w.WritePacketText("quit")
			w.WriteFlush()
		})
		r := pktline.NewReader(bytes.NewReader(out))
		skipCapAndVersion(t, r)
		status, err := r.ReadPacketText()
		require.NoError(t, err)
		require.Equal(t, "status 404", status, "expected 404 for invalid OID %q", badOID)
	}
}

func TestBatchInvalidOIDReturnsError(t *testing.T) {
	srv := testServer(t, "upload")

	validOIDStr := strings.Repeat("ab", 32)
	badOIDStr := "../../etc/passwd" + strings.Repeat("a", 48) // 64 chars but contains non-hex

	out := runSession(t, srv, func(w *pktline.Writer) {
		w.WritePacketText("version 1")
		w.WriteFlush()
		w.WritePacketText("batch")
		w.WriteDelim()
		w.WritePacketText(fmt.Sprintf("%s 100", badOIDStr))
		w.WritePacketText(fmt.Sprintf("%s 100", validOIDStr))
		w.WriteFlush()
		w.WritePacketText("quit")
		w.WriteFlush()
	})

	r := pktline.NewReader(bytes.NewReader(out))
	skipCapAndVersion(t, r)

	// Whole batch must fail with 400 when any OID is invalid.
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
