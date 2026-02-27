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

	// testPostQuarantine, if non-nil, is called by SafePrune immediately after
	// renaming a chunk to .pruning and before the post-quarantine re-check.
	// It exists solely to allow tests to inject a concurrent UntagPrunable call.
	testPostQuarantine func(id ChunkID)
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

// ListChunks returns every chunk-related entry in the store together with its
// pruning status. It is called by commonSafePrune and commonRescueChunks.
func (s LocalStore) ListChunks(ctx context.Context) ([]ChunkEntry, error) {
	var entries []ChunkEntry
	chunkExt := CompressedChunkExt
	if s.Opt.Uncompressed {
		chunkExt = UncompressedChunkExt
	}
	err := filepath.Walk(s.Base, func(path string, info os.FileInfo, err error) error {
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
		base := filepath.Base(path)
		if strings.HasSuffix(base, PruningExt) {
			nameWithoutPruning := strings.TrimSuffix(base, PruningExt)
			idStr := strings.TrimSuffix(nameWithoutPruning, chunkExt)
			id, err := ChunkIDFromString(idStr)
			if err != nil {
				return nil
			}
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPruning})
		} else if strings.HasSuffix(base, PrunableExt) {
			nameWithoutPrunable := strings.TrimSuffix(base, PrunableExt)
			idStr := strings.TrimSuffix(nameWithoutPrunable, chunkExt)
			id, err := ChunkIDFromString(idStr)
			if err != nil {
				return nil
			}
			cacnk := strings.TrimSuffix(path, PrunableExt)
			if _, serr := os.Stat(cacnk); os.IsNotExist(serr) {
				entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusOrphanedPrunable})
			} else if serr == nil {
				entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusPrunable})
			}
		} else {
			var idStr string
			if s.Opt.Uncompressed {
				idStr = base
			} else {
				if !strings.HasSuffix(base, CompressedChunkExt) {
					return nil
				}
				idStr = strings.TrimSuffix(base, CompressedChunkExt)
			}
			id, err := ChunkIDFromString(idStr)
			if err != nil {
				return nil
			}
			entries = append(entries, ChunkEntry{ID: id, Status: ChunkStatusNormal})
		}
		return nil
	})
	return entries, err
}

// MarkerExists reports whether the .prunable companion for id is present.
func (s LocalStore) MarkerExists(id ChunkID) (bool, error) {
	prunable, _ := s.pruningPathsFromID(id)
	_, err := os.Stat(prunable)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// DeletePruning removes the .pruning file for id. No-op if absent.
func (s LocalStore) DeletePruning(id ChunkID) error {
	_, pruning := s.pruningPathsFromID(id)
	err := os.Remove(pruning)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// DeleteMarker removes the .prunable companion for id. No-op if absent.
func (s LocalStore) DeleteMarker(id ChunkID) error {
	prunable, _ := s.pruningPathsFromID(id)
	err := os.Remove(prunable)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// CreateMarker creates an empty .prunable companion for id.
func (s LocalStore) CreateMarker(id ChunkID) error {
	prunable, _ := s.pruningPathsFromID(id)
	return os.WriteFile(prunable, nil, 0644)
}

// Quarantine renames .cacnk to .pruning, making the chunk invisible to
// readers. The testPostQuarantine hook (if set) is called after the rename
// and before returning to allow tests to inject concurrent operations.
func (s LocalStore) Quarantine(id ChunkID) error {
	_, cacnk := s.nameFromID(id)
	_, pruning := s.pruningPathsFromID(id)
	if err := os.Rename(cacnk, pruning); err != nil {
		return err
	}
	if s.testPostQuarantine != nil {
		s.testPostQuarantine(id)
	}
	return nil
}

// Restore renames .pruning back to .cacnk. No-op if .pruning is absent.
func (s LocalStore) Restore(id ChunkID) error {
	_, cacnk := s.nameFromID(id)
	_, pruning := s.pruningPathsFromID(id)
	if _, err := os.Stat(pruning); os.IsNotExist(err) {
		return nil
	}
	return os.Rename(pruning, cacnk)
}

// SafePrune implements the two-run safe pruning protocol for a LocalStore.
func (s LocalStore) SafePrune(ctx context.Context, ids map[ChunkID]struct{}) error {
	return commonSafePrune(ctx, ids, s)
}

// RescueChunks rescues any chunks in ids that were quarantined by SafePrune.
// For each chunk: removes any .prunable marker; if the .cacnk is missing but
// a .pruning file exists, renames it back.
func (s LocalStore) RescueChunks(ctx context.Context, ids map[ChunkID]struct{}) error {
	return commonRescueChunks(ctx, ids, s)
}
