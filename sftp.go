package desync

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"path"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

var _ WriteStore = &SFTPStore{}

// SFTPStoreBase is the base object for SFTP chunk and index stores.
type SFTPStoreBase struct {
	location *url.URL
	path     string
	client   *sftp.Client
	cancel   context.CancelFunc
	opt      StoreOptions
}

// SFTPStore is a chunk store that uses SFTP over SSH.
type SFTPStore struct {
	pool        chan *SFTPStoreBase
	location    *url.URL
	n           int
	converters  Converters
	safePruning bool
}

// Creates a base sftp client
func newSFTPStoreBase(location *url.URL, opt StoreOptions) (*SFTPStoreBase, error) {
	sshCmd := os.Getenv("CASYNC_SSH_PATH")
	if sshCmd == "" {
		sshCmd = "ssh"
	}
	host := location.Host
	path := location.Path
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	// If a username was given in the URL, prefix the host
	if location.User != nil {
		host = location.User.Username() + "@" + location.Host
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := exec.CommandContext(ctx, sshCmd, host, "-s", "sftp")
	c.Stderr = os.Stderr
	r, err := c.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	w, err := c.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err = c.Start(); err != nil {
		cancel()
		return nil, err
	}
	client, err := sftp.NewClientPipe(r, w)
	if err != nil {
		cancel()
		return nil, err
	}
	// The stat has really two jobs. Confirm that the path actually exists on the
	// server, and also make sure the handshake has happened successfully. SSH
	// may fail if multiple instances access the SSH agent concurrently.
	if _, err = client.Stat(path); err != nil {
		cancel()
		return nil, errors.Wrapf(err, "failed to stat '%s'", path)
	}
	return &SFTPStoreBase{location, path, client, cancel, opt}, nil
}

// StoreObject adds a new object to a writable index or chunk store.
func (s *SFTPStoreBase) StoreObject(name string, r io.Reader) error {
	// Write to a tempfile on the remote server. This is not 100% guaranteed to not
	// conflict between goroutines, there's no tempfile() function for remote servers.
	// Use a large enough random number instead to build a tempfile
	tmpfile := name + strconv.Itoa(rand.Int())
	d := path.Dir(name)
	var errCount int
retry:
	f, err := s.client.Create(tmpfile)
	if err != nil {
		// It's possible the parent dir doesn't yet exist. Create it while ignoring
		// errors since that could be racy and fail if another goroutine does the
		// same.
		if errCount < 1 {
			s.client.Mkdir(d)
			errCount++
			goto retry
		}
		return errors.Wrap(err, "sftp:create "+tmpfile)
	}

	if _, err := io.Copy(f, r); err != nil {
		s.client.Remove(tmpfile)
		return errors.Wrap(err, "sftp:copying chunk data to "+tmpfile)
	}
	if err = f.Close(); err != nil {
		return errors.Wrap(err, "sftp:closing "+tmpfile)
	}
	return errors.Wrap(s.client.PosixRename(tmpfile, name), "sftp:renaming "+tmpfile+" to "+name)
}

// Close terminates all client connections
func (s *SFTPStoreBase) Close() error {
	if s.cancel != nil {
		defer s.cancel()
	}
	return s.client.Close()
}

func (s *SFTPStoreBase) String() string {
	return s.location.String()
}

// Returns the path for a chunk
func (s *SFTPStoreBase) nameFromID(id ChunkID) string {
	sID := id.String()
	name := s.path + sID[0:4] + "/" + sID
	if s.opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return name
}

// NewSFTPStore initializes a chunk store using SFTP over SSH.
func NewSFTPStore(location *url.URL, opt StoreOptions) (*SFTPStore, error) {
	s := &SFTPStore{make(chan *SFTPStoreBase, opt.N), location, opt.N, opt.converters(), opt.SafePruning}
	for i := 0; i < opt.N; i++ {
		c, err := newSFTPStoreBase(location, opt)
		if err != nil {
			return nil, err
		}
		s.pool <- c
	}
	return s, nil
}

// GetChunk returns a chunk from an SFTP store, returns ChunkMissing if the file does not exist
func (s *SFTPStore) GetChunk(id ChunkID) (*Chunk, error) {
	c := <-s.pool
	defer func() { s.pool <- c }()
	name := c.nameFromID(id)
	f, err := c.client.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = ChunkMissing{id}
		}
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read from %s", name)
	}
	return NewChunkFromStorage(id, b, s.converters, c.opt.SkipVerify)
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s *SFTPStore) RemoveChunk(id ChunkID) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	name := c.nameFromID(id)
	if _, err := c.client.Stat(name); err != nil {
		return ChunkMissing{id}
	}
	return c.client.Remove(name)
}

// StoreChunk adds a new chunk to the store
func (s *SFTPStore) StoreChunk(chunk *Chunk) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	name := c.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}

	return c.StoreObject(name, bytes.NewReader(b))
}

// HasChunk returns true if the chunk is in the store
func (s *SFTPStore) HasChunk(id ChunkID) (bool, error) {
	c := <-s.pool
	defer func() { s.pool <- c }()
	name := c.nameFromID(id)
	_, err := c.client.Stat(name)
	return err == nil, nil
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s *SFTPStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	walker := c.client.Walk(c.path)

	for walker.Step() {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err := walker.Err(); err != nil {
			return err
		}
		info := walker.Stat()
		if info.IsDir() { // Skip dirs
			continue
		}
		path := walker.Path()
		if !strings.HasSuffix(path, CompressedChunkExt) { // Skip files without chunk extension
			continue
		}
		// Skip compressed chunks if this is running in uncompressed mode and vice-versa
		var sID string
		if c.opt.Uncompressed {
			if !strings.HasSuffix(path, UncompressedChunkExt) {
				return nil
			}
			sID = strings.TrimSuffix(filepath.Base(path), UncompressedChunkExt)
		} else {
			if !strings.HasSuffix(path, CompressedChunkExt) {
				return nil
			}
			sID = strings.TrimSuffix(filepath.Base(path), CompressedChunkExt)
		}
		// Convert the name into a checksum, if that fails we're probably not looking
		// at a chunk file and should skip it.
		id, err := ChunkIDFromString(sID)
		if err != nil {
			continue
		}
		// See if the chunk we're looking at is in the list we want to keep, if not
		// remove it.
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SFTPStoreBase) markerPathFromID(id ChunkID) string {
	return s.nameFromID(id) + PrunableExt
}

func (s *SFTPStoreBase) protectPathFromID(id ChunkID) string {
	return s.nameFromID(id) + ProtectExt
}

// ListChunks returns every chunk-related entry in the store together with its
// pruning status. It is called by commonSafePrune.
func (s *SFTPStore) ListChunks(ctx context.Context) ([]ChunkEntry, error) {
	c := <-s.pool
	defer func() { s.pool <- c }()

	type presence struct{ hasChunk, hasPrunable, hasProtect bool }
	byID := make(map[ChunkID]*presence)

	chunkExt := CompressedChunkExt
	if c.opt.Uncompressed {
		chunkExt = UncompressedChunkExt
	}

	walker := c.client.Walk(c.path)
	for walker.Step() {
		select {
		case <-ctx.Done():
			return nil, Interrupted{}
		default:
		}
		if err := walker.Err(); err != nil {
			return nil, err
		}
		if walker.Stat().IsDir() {
			continue
		}
		base := filepath.Base(walker.Path())
		var id ChunkID
		var parseErr error
		if strings.HasSuffix(base, ProtectExt) {
			nameWithout := strings.TrimSuffix(base, ProtectExt)
			idStr := strings.TrimSuffix(nameWithout, chunkExt)
			id, parseErr = ChunkIDFromString(idStr)
			if parseErr != nil {
				continue
			}
			if _, ok := byID[id]; !ok {
				byID[id] = &presence{}
			}
			byID[id].hasProtect = true
		} else if strings.HasSuffix(base, PrunableExt) {
			nameWithout := strings.TrimSuffix(base, PrunableExt)
			idStr := strings.TrimSuffix(nameWithout, chunkExt)
			id, parseErr = ChunkIDFromString(idStr)
			if parseErr != nil {
				continue
			}
			if _, ok := byID[id]; !ok {
				byID[id] = &presence{}
			}
			byID[id].hasPrunable = true
		} else {
			var idStr string
			if c.opt.Uncompressed {
				idStr = base
			} else {
				if !strings.HasSuffix(base, CompressedChunkExt) {
					continue
				}
				idStr = strings.TrimSuffix(base, CompressedChunkExt)
			}
			id, parseErr = ChunkIDFromString(idStr)
			if parseErr != nil {
				continue
			}
			if _, ok := byID[id]; !ok {
				byID[id] = &presence{}
			}
			byID[id].hasChunk = true
		}
	}

	var entries []ChunkEntry
	for id, p := range byID {
		switch {
		case p.hasChunk && p.hasPrunable && p.hasProtect:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusProtected})
		case p.hasChunk && p.hasPrunable:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPrunable})
		case p.hasChunk:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusNormal})
		case p.hasPrunable:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedPrunable})
		case p.hasProtect:
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedProtect})
		}
	}
	return entries, nil
}

// HasPrunable reports whether the .prunable companion for id is present.
func (s *SFTPStore) HasPrunable(id ChunkID) (bool, error) {
	c := <-s.pool
	defer func() { s.pool <- c }()
	_, err := c.client.Stat(c.markerPathFromID(id))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// DeleteMarker removes the .prunable companion for id. No-op if absent.
func (s *SFTPStore) DeleteMarker(id ChunkID) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	err := c.client.Remove(c.markerPathFromID(id))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

// CreateMarker creates an empty .prunable companion file for id.
func (s *SFTPStore) CreateMarker(id ChunkID) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	f, err := c.client.Create(c.markerPathFromID(id))
	if err != nil {
		return err
	}
	return f.Close()
}

// CreateProtect creates an empty .protect companion file for id.
func (s *SFTPStore) CreateProtect(id ChunkID) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	f, err := c.client.Create(c.protectPathFromID(id))
	if err != nil {
		return err
	}
	return f.Close()
}

// DeleteProtect removes the .protect companion for id. No-op if absent.
func (s *SFTPStore) DeleteProtect(id ChunkID) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	err := c.client.Remove(c.protectPathFromID(id))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

// HasProtect reports whether the .protect companion for id is present.
func (s *SFTPStore) HasProtect(id ChunkID) (bool, error) {
	c := <-s.pool
	defer func() { s.pool <- c }()
	_, err := c.client.Stat(c.protectPathFromID(id))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// DeleteChunk removes the chunk data file for id. No-op if absent.
func (s *SFTPStore) DeleteChunk(id ChunkID) error {
	c := <-s.pool
	defer func() { s.pool <- c }()
	err := c.client.Remove(c.nameFromID(id))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

// SafePruningEnabled reports whether the store was opened with safe pruning enabled.
func (s *SFTPStore) SafePruningEnabled() bool { return s.safePruning }

// SafePrune implements the protect-marker safe pruning protocol for an SFTPStore.
func (s *SFTPStore) SafePrune(ctx context.Context, ids map[ChunkID]struct{}, finalizeOnly bool) error {
	return commonSafePrune(ctx, ids, s, finalizeOnly)
}

// Close terminates all client connections
func (s *SFTPStore) Close() error {
	var err error
	for i := 0; i < s.n; i++ {
		c := <-s.pool
		err = c.Close()
	}
	return err
}

func (s *SFTPStore) String() string {
	return s.location.String()
}
