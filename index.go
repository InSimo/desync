package desync

import (
	"bufio"
	"context"
	"crypto"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"

	"io"
)

// Index represents the content of an index file
type Index struct {
	Index  FormatIndex
	Chunks []IndexChunk
}

// IndexChunk is a table entry in an index file containing the chunk ID (SHA256)
// Similar to an FormatTableItem but with Start and Size instead of just offset to
// make it easier to use throughout the application.
type IndexChunk struct {
	ID    ChunkID
	Start uint64
	Size  uint64
}

// IndexFromReader parses a caibx structure (from a reader) and returns a populated Caibx
// object
func IndexFromReader(r io.Reader) (c Index, err error) {
	d := NewFormatDecoder(bufio.NewReader(r))
	var ok bool
	// Read the index
	e, err := d.Next()
	if err != nil {
		return c, errors.Wrap(err, "reading index")
	}

	c.Index, ok = e.(FormatIndex)
	if !ok {
		return c, errors.New("input is not an index file")
	}

	// Ensure the algorithm the library uses matches that of the index file
	switch Digest.Algorithm() {
	case crypto.SHA512_256:
		if c.Index.FeatureFlags&CaFormatSHA512256 == 0 {
			return c, errors.New("index file uses SHA256")
		}
	case crypto.SHA256:
		if c.Index.FeatureFlags&CaFormatSHA512256 != 0 {
			return c, errors.New("index file uses SHA512-256")
		}
	}

	// Read the table
	e, err = d.Next()
	if err != nil {
		return c, errors.Wrap(err, "reading chunk table")
	}
	table, ok := e.(FormatTable)
	if !ok {
		return c, errors.New("index table not found in input")
	}

	// Convert the chunk table into a different format for easier use
	c.Chunks = make([]IndexChunk, len(table.Items))
	var lastOffset uint64
	for i, r := range table.Items {
		c.Chunks[i].ID = r.Chunk
		c.Chunks[i].Start = lastOffset
		c.Chunks[i].Size = r.Offset - lastOffset
		lastOffset = r.Offset
		// Check the max size of the chunk only. The min apparently doesn't apply
		// to the last chunk.
		if c.Chunks[i].Size > c.Index.ChunkSizeMax {
			return c, fmt.Errorf("chunk size %d is larger than maximum %d", c.Chunks[i].Size, c.Index.ChunkSizeMax)
		}
	}
	return
}

// TotalSize returns the total uncompressed content size represented by the index.
func (i *Index) TotalSize() int64 {
	if len(i.Chunks) == 0 {
		return 0
	}
	last := i.Chunks[len(i.Chunks)-1]
	return int64(last.Start + last.Size)
}

// WriteTo writes the index and chunk table into a stream
func (i *Index) WriteTo(w io.Writer) (int64, error) {
	index := FormatIndex{
		FormatHeader: FormatHeader{Size: 48, Type: CaFormatIndex},
		FeatureFlags: i.Index.FeatureFlags,
		ChunkSizeMin: i.Index.ChunkSizeMin,
		ChunkSizeAvg: i.Index.ChunkSizeAvg,
		ChunkSizeMax: i.Index.ChunkSizeMax,
	}

	bw := bufio.NewWriter(w)
	d := NewFormatEncoder(bw)
	n, err := d.Encode(index)
	if err != nil {
		return n, err
	}

	// Convert the chunk list back into the format used in index files (with offset
	// instead of start+size)
	var offset uint64
	fChunks := make([]FormatTableItem, len(i.Chunks))
	for p, c := range i.Chunks {
		offset += c.Size
		fChunks[p] = FormatTableItem{Chunk: c.ID, Offset: offset}
	}
	table := FormatTable{
		FormatHeader: FormatHeader{Size: math.MaxUint64, Type: CaFormatTable},
		Items:        fChunks,
	}
	n1, err := d.Encode(table)

	if err := bw.Flush(); err != nil {
		return n + n1, err
	}
	return n + n1, err
}

// Length returns the total (uncompressed) size of the indexed stream
func (i *Index) Length() int64 {
	if len(i.Chunks) < 1 {
		return 0
	}
	lastChunk := i.Chunks[len(i.Chunks)-1]
	return int64(lastChunk.Start + lastChunk.Size)
}

// ChunkStream splits up a blob into chunks using the provided chunker (single stream),
// populates a store with the chunks and returns an index. Hashing and compression
// is performed in n goroutines while the hashing algorithm is performed serially.
// When sps is non-nil, ChunkStream participates in the safe-pruning protocol:
// prunable chunks get .protect markers and SafePrunePreCommit is called before returning.
// ChunkerInterface is the interface used by ChunkStream to obtain successive
// chunks of data from an input stream. Chunker implements this interface.
type ChunkerInterface interface {
	Next() (uint64, []byte, error)
	Min() uint64
	Avg() uint64
	Max() uint64
}

func ChunkStream(ctx context.Context, c ChunkerInterface, ws WriteStore, n int, sps SafePruneStore, propTime time.Duration) (Index, error) {
	// Use global worker pool if initialized; otherwise fall back to
	// legacy per-call goroutines.
	if pool := getWorkerPool(); pool != nil {
		return chunkStreamPooled(ctx, c, ws, pool, sps, propTime)
	}
	return chunkStreamLegacy(ctx, c, ws, n, sps, propTime)
}

// chunkStreamPooled uses the global WorkerPool.  Chunk hashing is done by
// CPU-bound process workers; storage (compress + I/O) by I/O-bound storage
// workers.  All concurrent ChunkStream callers share the same pools.
func chunkStreamPooled(ctx context.Context, c ChunkerInterface, ws WriteStore, pool *WorkerPool, sps SafePruneStore, propTime time.Duration) (Index, error) {
	var s *ChunkStorage
	if sps != nil {
		s = NewChunkStorageWithPruning(ws, sps)
	} else {
		s = NewChunkStorage(ws)
	}

	var (
		mu      sync.Mutex
		results = make(map[int]IndexChunk)
		wg      sync.WaitGroup
		errPtr  atomic.Pointer[error]
	)

	recordResult := func(num int, r IndexChunk) {
		mu.Lock()
		results[num] = r
		mu.Unlock()
	}

	var num int
	for {
		start, b, err := c.Next()
		if err != nil {
			return Index{}, err
		}
		if len(b) == 0 {
			break
		}

		chunk := copyChunkData(b)
		wg.Add(1)

		select {
		case <-ctx.Done():
			chunk.Release()
			wg.Done()
			goto done
		case pool.processQ <- processTask{
			chunk:    chunk,
			start:    start,
			num:      num,
			store:    s,
			record:   recordResult,
			wg:       &wg,
			firstErr: &errPtr,
			storageQ: pool.storageQ,
		}:
		}
		num++
	}
done:
	wg.Wait()

	if ep := errPtr.Load(); ep != nil {
		return Index{}, *ep
	}

	return buildIndex(c, results, s, sps, ctx, propTime)
}

// chunkStreamLegacy is the original goroutine-per-call implementation.
func chunkStreamLegacy(ctx context.Context, c ChunkerInterface, ws WriteStore, n int, sps SafePruneStore, propTime time.Duration) (Index, error) {
	type chunkJob struct {
		num   int
		start uint64
		chunk *Chunk
	}
	var (
		mu      sync.Mutex
		in      = make(chan chunkJob)
		results = make(map[int]IndexChunk)
	)

	g, gCtx := errgroup.WithContext(ctx)
	var s *ChunkStorage
	if sps != nil {
		s = NewChunkStorageWithPruning(ws, sps)
	} else {
		s = NewChunkStorage(ws)
	}

	recordResult := func(num int, r IndexChunk) {
		mu.Lock()
		defer mu.Unlock()
		results[num] = r
	}

	for range n {
		g.Go(func() error {
			for c := range in {
				idxChunk := IndexChunk{Start: c.start, Size: uint64(len(c.chunk.data)), ID: c.chunk.ID()}
				recordResult(c.num, idxChunk)

				captured, err := s.StoreChunk(c.chunk)
				if err != nil {
					if !captured {
						c.chunk.Release()
					}
					return err
				}
				if !captured {
					c.chunk.Release()
				}
			}
			return nil
		})
	}

	var num int
loop:
	for {
		start, b, err := c.Next()
		if err != nil {
			return Index{}, err
		}
		if len(b) == 0 {
			break
		}

		chunk := copyChunkData(b)

		select {
		case <-gCtx.Done():
			break loop
		case in <- chunkJob{num: num, start: start, chunk: chunk}:
		}
		num++
	}
	close(in)

	if err := g.Wait(); err != nil {
		return Index{}, err
	}

	return buildIndex(c, results, s, sps, ctx, propTime)
}

// copyChunkData copies chunk bytes from the chunker's reusable buffer into
// a pooled or freshly allocated Chunk.
func copyChunkData(b []byte) *Chunk {
	chunk := getPooledChunk()
	if chunk != nil {
		if len(b) > cap(chunk.dataBuf) {
			chunk.growStorageBuf(len(b))
		}
		chunk.data = chunk.dataBuf[:len(b)]
		copy(chunk.data, b)
	} else {
		clone := make([]byte, len(b))
		copy(clone, b)
		chunk = NewChunk(clone)
	}
	return chunk
}

// buildIndex assembles the final Index from collected results and handles
// safe-pruning pre-commit if needed.
func buildIndex(c ChunkerInterface, results map[int]IndexChunk, s *ChunkStorage, sps SafePruneStore, ctx context.Context, propTime time.Duration) (Index, error) {
	chunks := make([]IndexChunk, len(results))
	for i := 0; i < len(results); i++ {
		chunks[i] = results[i]
	}

	index := Index{
		Index: FormatIndex{
			FeatureFlags: CaFormatExcludeNoDump | CaFormatSHA512256,
			ChunkSizeMin: c.Min(),
			ChunkSizeAvg: c.Avg(),
			ChunkSizeMax: c.Max(),
		},
		Chunks: chunks,
	}

	if sps != nil {
		err := SafePrunePreCommit(ctx, s.Chunks(), s.LastProtectTime(), sps, propTime)
		for _, chunk := range s.Chunks() {
			chunk.Release()
		}
		if err != nil {
			return Index{}, err
		}
	}
	return index, nil
}
