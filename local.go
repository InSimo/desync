package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/folbricht/tempfile"
)

var _ WriteStore = LocalStore{}

const (
	tmpChunkPrefix = ".tmp-cacnk"
)

// LocalStore casync store
type LocalStore struct {
	Base string

	// When accessing chunks, should mtime be updated? Useful when this is
	// a cache. Old chunks can be identified and removed from the store that way
	UpdateTimes bool

	Opt StoreOptions

	converters Converters
}

// NewLocalStore creates an instance of a local castore, it only checks presence
// of the store
func NewLocalStore(dir string, opt StoreOptions) (LocalStore, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return LocalStore{}, err
	}
	if !info.IsDir() {
		return LocalStore{}, fmt.Errorf("%s is not a directory", dir)
	}
	return LocalStore{Base: dir, Opt: opt, converters: opt.converters()}, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s LocalStore) GetChunk(id ChunkID) (*Chunk, error) {
	_, p := s.nameFromID(id)
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, ChunkMissing{id}
	}
	return NewChunkFromStorage(id, b, s.converters, s.Opt.SkipVerify)
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s LocalStore) RemoveChunk(id ChunkID) error {
	_, p := s.nameFromID(id)
	if _, err := os.Stat(p); err != nil {
		return ChunkMissing{id}
	}
	return os.Remove(p)
}

// StoreChunk adds a new chunk to the store
func (s LocalStore) StoreChunk(chunk *Chunk) error {
	d, p := s.nameFromID(chunk.ID())
	b, err := chunk.Storage(s.converters)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d, 0755); err != nil {
		return err
	}
	tmp, err := tempfile.NewMode(d, tmpChunkPrefix, 0644)
	if err != nil {
		return err
	}
	if _, err = tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmp.Name()) // clean up
		return err
	}
	tmp.Close() // Windows can't rename open files, close explicitly
	return os.Rename(tmp.Name(), p)
}

// Verify all chunks in the store. If repair is set true, bad chunks are deleted.
// n determines the number of concurrent operations. w is used to write any messages
// intended for the user, typically os.Stderr.
func (s LocalStore) Verify(ctx context.Context, n int, repair bool, w io.Writer) error {
	var wg sync.WaitGroup
	ids := make(chan ChunkID)

	// Start the workers
	for range n {
		wg.Add(1)
		go func() {
			for id := range ids {
				_, err := s.GetChunk(id)
				switch err.(type) {
				case ChunkInvalid: // bad chunk, report and delete (if repair=true)
					msg := err.Error()
					if repair {
						if err = s.RemoveChunk(id); err != nil {
							msg = msg + ":" + err.Error()
						} else {
							msg = msg + ": removed"
						}
					}
					fmt.Fprintln(w, msg)
				case nil:
				default: // unexpected, print the error and carry on
					fmt.Fprintln(w, err)
				}
			}
			wg.Done()
		}()
	}

	// Go through all chunks underneath Base, filtering out other files, then feed
	// the IDs to the workers
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil { // failed to walk? => fail
			return err
		}
		if info.IsDir() { // Skip dirs
			return nil
		}
		// Skip compressed chunks if this is running in uncompressed mode and vice-versa
		var sID string
		if s.Opt.Uncompressed {
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
			return nil
		}
		// Feed the workers
		ids <- id
		return nil
	})
	close(ids)
	wg.Wait()
	return err
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s LocalStore) Prune(ctx context.Context, ids map[ChunkID]struct{}) error {
	// Go through all chunks underneath Base, filtering out other directories and files
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil { // failed to walk? => fail
			return err
		}
		if info.IsDir() { // Skip dirs
			return nil
		}

		// If the chunk is only partially downloaded remove it
		if strings.HasPrefix(filepath.Base(path), tmpChunkPrefix) {
			_ = os.Remove(path)
			return nil
		}

		// Skip compressed chunks if this is running in uncompressed mode and vice-versa
		var sID string
		if s.Opt.Uncompressed {
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
			return nil
		}
		// See if the chunk we're looking at is in the list we want to keep, if not
		// remove it.
		if _, ok := ids[id]; !ok {
			if err = s.RemoveChunk(id); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// HasChunk returns true if the chunk is in the store
func (s LocalStore) HasChunk(id ChunkID) (bool, error) {
	_, p := s.nameFromID(id)
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s LocalStore) String() string {
	return s.Base
}

// Close the store. NOP operation, needed to implement Store interface.
func (s LocalStore) Close() error { return nil }

// GetChunkSize returns the bytes size of the raw, possibly compressed chunk in this store.
func (s LocalStore) GetChunkSize(id ChunkID) (int64, error) {
	_, p := s.nameFromID(id)
	i, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	return i.Size(), nil
}

func (s LocalStore) nameFromID(id ChunkID) (dir, name string) {
	sID := id.String()
	dir = filepath.Join(s.Base, sID[0:4])
	name = filepath.Join(dir, sID)
	if s.Opt.Uncompressed {
		name += UncompressedChunkExt
	} else {
		name += CompressedChunkExt
	}
	return
}

func (s LocalStore) pruningPathsFromID(id ChunkID) (prunable, pruning string) {
	_, p := s.nameFromID(id)
	return p + PrunableExt, p + PruningExt
}

// UntagPrunable removes the .prunable companion file for a chunk, if it exists.
func (s LocalStore) UntagPrunable(id ChunkID) error {
	prunable, _ := s.pruningPathsFromID(id)
	err := os.Remove(prunable)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// RescueChunks rescues any chunks in ids that were quarantined by SafePrune.
// For each chunk: removes any .prunable marker; if the .cacnk is missing but
// a .pruning file exists, renames it back.
func (s LocalStore) RescueChunks(ctx context.Context, ids map[ChunkID]struct{}) error {
	for id := range ids {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		_, cacnk := s.nameFromID(id)
		prunable, pruning := s.pruningPathsFromID(id)
		// Always remove any prunable marker.
		if err := os.Remove(prunable); err != nil && !os.IsNotExist(err) {
			return err
		}
		if _, err := os.Stat(cacnk); err == nil {
			// .cacnk exists; remove stale quarantined copy if any.
			if err2 := os.Remove(pruning); err2 != nil && !os.IsNotExist(err2) {
				return err2
			}
		} else if os.IsNotExist(err) {
			// .cacnk missing; rescue from quarantine if available.
			if _, serr := os.Stat(pruning); serr == nil {
				if err2 := os.Rename(pruning, cacnk); err2 != nil {
					return err2
				}
			}
		} else {
			return err
		}
	}
	return nil
}

// SafePrune implements the two-run safe pruning protocol for a LocalStore.
// Phase 1: delete previously quarantined (.pruning) chunks and orphaned
// .prunable markers. Phase 2: for chunks not in ids, create a .prunable
// marker on first encounter, or quarantine (rename .cacnk → .pruning) if
// the marker already exists; for chunks in ids, remove any .prunable marker.
func (s LocalStore) SafePrune(ctx context.Context, ids map[ChunkID]struct{}) error {
	// Phase 1: cleanup — remove quarantined chunks and orphaned prunable markers.
	if err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, PruningExt) {
			return os.Remove(path)
		}
		if strings.HasSuffix(path, PrunableExt) {
			// Remove orphaned marker if corresponding .cacnk is gone.
			cacnk := strings.TrimSuffix(path, PrunableExt)
			if _, serr := os.Stat(cacnk); os.IsNotExist(serr) {
				return os.Remove(path)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Phase 2: mark new candidates and quarantine previously marked chunks.
	return filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return Interrupted{}
		default:
		}
		if err != nil {
			// A file deleted during quarantine (e.g. .prunable removed after
			// renaming .cacnk → .pruning) may still appear in the walk's
			// pre-read directory listing. Treat "not exist" as "skip".
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Skip temp files and non-chunk files (.prunable, .pruning, etc.).
		if strings.HasPrefix(filepath.Base(path), tmpChunkPrefix) {
			_ = os.Remove(path)
			return nil
		}
		var sID string
		if s.Opt.Uncompressed {
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
		id, err := ChunkIDFromString(sID)
		if err != nil {
			return nil
		}
		prunable, pruning := s.pruningPathsFromID(id)
		if _, ok := ids[id]; ok {
			// Chunk is in keep-set: remove any prunable marker.
			if rerr := os.Remove(prunable); rerr != nil && !os.IsNotExist(rerr) {
				return rerr
			}
			return nil
		}
		// Chunk is not in keep-set.
		if _, serr := os.Stat(prunable); os.IsNotExist(serr) {
			// First encounter: create empty marker.
			return os.WriteFile(prunable, nil, 0644)
		}
		// Already marked: quarantine by renaming .cacnk → .pruning, then remove marker.
		if rerr := os.Rename(path, pruning); rerr != nil {
			return rerr
		}
		return os.Remove(prunable)
	})
}
