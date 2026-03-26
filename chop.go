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
func ChopFile(ctx context.Context, name string, chunks []IndexChunk, ws WriteStore, n int, pb ProgressBar, sps SafePruneStore, propTime time.Duration) error {
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
				pb.Increment()

				chunk, err := readChunkFromFile(f, c)
				if err != nil {
					return err
				}

				captured, err := s.StoreChunk(chunk)
				if err != nil {
					chunk.Release()
					return err
				}
				if !captured {
					chunk.Release()
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
		for _, chunk := range s.Chunks() {
			chunk.Release()
		}
		return err
	}
	return nil
}

// readChunkFromFile reads chunk data from file and returns a chunk.
// When the global pool is available, reads into a pooled chunk's dataBuf.
// Otherwise allocates a new chunk.
func readChunkFromFile(f *os.File, c IndexChunk) (*Chunk, error) {
	if chunk := getPooledChunk(); chunk != nil {
		if int(c.Size) > cap(chunk.dataBuf) {
			chunk.growStorageBuf(int(c.Size))
		}
		chunk.data = chunk.dataBuf[:c.Size]
		if _, err := f.Seek(int64(c.Start), io.SeekStart); err != nil {
			chunk.Release()
			return nil, err
		}
		if _, err := io.ReadFull(f, chunk.data); err != nil {
			chunk.Release()
			return nil, err
		}
		chunk.id = c.ID
		chunk.idCalculated = false
		if sum := chunk.ID(); sum != c.ID {
			chunk.Release()
			return nil, ChunkInvalid{ID: c.ID, Sum: sum}
		}
		return chunk, nil
	}

	b := make([]byte, c.Size)
	if _, err := f.Seek(int64(c.Start), io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, err
	}
	return NewChunkWithID(c.ID, b, false)
}
