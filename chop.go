package desync

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

// ChopFile split a file according to a list of chunks obtained from an Index
// and stores them in the provided store. When sps is non-nil, ChopFile
// participates in the safe-pruning protocol: prunable chunks get .protect
// markers and SafePrunePreCommit is called before returning.
// Chunks are taken from the global pool and returned after StoreChunk
// completes (or after SafePrunePreCommit for captured chunks), eliminating
// per-chunk heap allocations.
func ChopFile(ctx context.Context, name string, chunks []IndexChunk, ws WriteStore, n int, pb ProgressBar, sps SafePruneStore, propTime time.Duration) error {
	pool := GetChunkPool()
	in := make(chan IndexChunk)
	g, gCtx := errgroup.WithContext(ctx)

	// Setup and start the progressbar if any
	pb.SetTotal(len(chunks))
	pb.Start()
	defer pb.Finish()

	var s *ChunkStorage
	if sps != nil {
		s = NewChunkStorageWithPruning(ws, sps)
	} else {
		s = NewChunkStorage(ws)
	}

	// Start the workers, each having its own filehandle to read concurrently
	for range n {
		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("unable to open file %s, %s", name, err)
		}
		defer f.Close()

		g.Go(func() error {
			for c := range in {
				// Update progress bar if any
				pb.Increment()

				var (
					chunk *Chunk
					err   error
				)
				if pool != nil {
					chunk = pool.Get()
					err = readChunkInto(f, c, chunk)
				} else {
					chunk, err = readChunkFromFile(f, c)
				}
				if err != nil {
					if pool != nil {
						pool.Put(chunk)
					}
					return err
				}

				captured, err := s.StoreChunk(chunk)
				if err != nil {
					if pool != nil {
						pool.Put(chunk)
					}
					return err
				}
				if pool != nil && !captured {
					pool.Put(chunk)
				}
			}
			return nil
		})
	}

	// Feed the workers, stop if there are any errors
loop:
	for _, c := range chunks {
		select {
		case <-gCtx.Done():
			break loop
		case in <- c:
		}
	}

	close(in)

	if err := g.Wait(); err != nil {
		return err
	}
	if sps != nil {
		err := SafePrunePreCommit(ctx, s.Chunks(), s.LastProtectTime(), sps, propTime)
		// Return captured chunks to the pool now that SafePrunePreCommit is done.
		if pool != nil {
			for _, chunk := range s.Chunks() {
				pool.Put(chunk)
			}
		}
		return err
	}
	return nil
}

// readChunkFromFile reads chunk data from file and returns a new Chunk.
func readChunkFromFile(f *os.File, c IndexChunk) (*Chunk, error) {
	b := make([]byte, c.Size)
	if _, err := f.Seek(int64(c.Start), io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, err
	}
	return NewChunkWithID(c.ID, b, false)
}

// readChunkInto reads chunk data into a pooled chunk's backing buffer.
func readChunkInto(f *os.File, c IndexChunk, chunk *Chunk) error {
	chunk.data = chunk.dataBuf[:c.Size]
	if _, err := f.Seek(int64(c.Start), io.SeekStart); err != nil {
		return err
	}
	if _, err := io.ReadFull(f, chunk.data); err != nil {
		return err
	}
	chunk.id = c.ID
	chunk.idCalculated = false
	// Verify the chunk ID matches the data.
	if sum := chunk.ID(); sum != c.ID {
		return ChunkInvalid{ID: c.ID, Sum: sum}
	}
	return nil
}
