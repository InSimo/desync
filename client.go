package desync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// ProgressFunc is called periodically with the number of bytes processed so far.
type ProgressFunc func(bytesProcessed int64)

// ByteReservation represents an in-flight byte reservation that must be
// released when the operation completes.
type ByteReservation interface {
	Release()
}

// TransferGate controls admission for transfer operations. It provides
// per-transfer byte limiting and per-storage-operation concurrency
// limiting. Implementations must be safe for concurrent use.
//
// The bytelimit.Gate type in cmd/shared/bytelimit satisfies this interface.
type TransferGate interface {
	// Acquire reserves size bytes for a transfer operation. It blocks
	// until the reservation can be made or ctx is cancelled.
	Acquire(ctx context.Context, size int64) (ByteReservation, error)

	// AcquireOp reserves a storage operation slot. Blocks if at the
	// configured maximum. Returns nil immediately if ops limiting is
	// disabled.
	AcquireOp(ctx context.Context) error

	// ReleaseOp releases a storage operation slot.
	ReleaseOp()

	// MaxOps returns the configured maximum concurrent storage operations.
	// Returns 0 if ops limiting is disabled.
	MaxOps() int32
}

// ClientOptions configures a Client.
type ClientOptions struct {
	// MinChunk, AvgChunk, MaxChunk are chunk size boundaries in bytes.
	MinChunk, AvgChunk, MaxChunk uint64
	// N is the max number of in-flight chunks for prefetch (default 10).
	N int
	// Gate, if non-nil, provides per-transfer byte admission control and
	// per-storage-operation concurrency limiting. PutObjectFromFile and
	// GetObjectToFile will call Gate.Acquire before starting and release
	// after completion. Store operations will be wrapped with AcquireOp/ReleaseOp.
	Gate TransferGate
}

// ObjectInfo holds metadata for a stored object.
type ObjectInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// Client provides a high-level API for desync chunk-based storage,
// mirroring the subset of minio.Client methods used by Forgejo.
//
// Callers are responsible for creating the underlying stores (using
// cmdshared helpers) and initializing global state (Init, InitWorkerPool)
// before constructing a Client.
type Client struct {
	ReadStore     Store
	WriteStore    WriteStore
	IndexStore    IndexWriteStore
	RawIndexStore IndexWriteStore // unwrapped, for metadata ops (list, delete)
	MinChunk      uint64
	AvgChunk      uint64
	MaxChunk      uint64
	N             int          // max in-flight chunks for prefetch
	Gate          TransferGate // optional admission control; nil = disabled
}

// NewClient creates a Client from pre-opened stores.
//
// Example initialization (using cmdshared helpers):
//
//	cfg, _, _ := cmdshared.LoadConfig(configFile)
//	desync.Init(maxChunk)
//	desync.InitWorkerPool(n, storageOps)
//	readStore, _ := cmdshared.MultiStoreWithCache(cfg, cmdOpt, cacheURL, storeURLs...)
//	writeStore, _ := cmdshared.WritableStore(storeURLs[0], cfg, cmdOpt)
//	indexStore, _ := cmdshared.WritableIndexStore(indexStoreURL, cfg, cmdOpt)
//	client := desync.NewClient(readStore, writeStore, indexStore, opts)
func NewClient(readStore Store, writeStore WriteStore, indexStore IndexWriteStore, opts ClientOptions) *Client {
	n := opts.N
	if n <= 0 {
		n = 10
	}
	c := &Client{
		ReadStore:     readStore,
		WriteStore:    writeStore,
		IndexStore:    indexStore,
		RawIndexStore: indexStore,
		MinChunk:      opts.MinChunk,
		AvgChunk:      opts.AvgChunk,
		MaxChunk:      opts.MaxChunk,
		N:             n,
		Gate:          opts.Gate,
	}

	// Wrap stores with per-operation gating if ops limiting is active.
	if opts.Gate != nil && opts.Gate.MaxOps() > 0 {
		c.WriteStore = &gatedWriteStore{WriteStore: writeStore, gate: opts.Gate}
		c.ReadStore = &gatedStore{Store: readStore, gate: opts.Gate}
		c.IndexStore = &gatedIndexWriteStore{IndexWriteStore: indexStore, gate: opts.Gate}
	}

	return c
}

// Close closes all underlying stores. Safe to call multiple times.
func (c *Client) Close() error {
	var errs []error
	if c.ReadStore != nil {
		errs = append(errs, c.ReadStore.Close())
		c.ReadStore = nil
	}
	if c.WriteStore != nil {
		errs = append(errs, c.WriteStore.Close())
		c.WriteStore = nil
	}
	if c.IndexStore != nil {
		errs = append(errs, c.IndexStore.Close())
		c.IndexStore = nil
	}
	c.RawIndexStore = nil
	return errors.Join(errs...)
}

// GetObject loads a desync index and returns a readable Object with
// prefetching. The returned Object implements io.Reader, io.Seeker,
// and io.Closer.
func (c *Client) GetObject(name string) (*Object, error) {
	idx, err := c.IndexStore.GetIndex(name)
	if err != nil {
		return nil, err
	}

	return &Object{
		idx:   idx,
		store: c.ReadStore,
		n:     c.N,
		size:  idx.TotalSize(),
	}, nil
}

// PutObject chunks the data from r and stores it under the given name.
// Returns the total uncompressed size written.
func (c *Client) PutObject(name string, r io.Reader, size int64) (int64, error) {
	chunker, err := NewChunker(r, c.MinChunk, c.AvgChunk, c.MaxChunk)
	if err != nil {
		return 0, fmt.Errorf("desync: failed to create chunker: %w", err)
	}

	idx, err := ChunkStream(context.Background(), &chunker, c.WriteStore, c.N, nil, 0)
	chunker.Release()
	if err != nil {
		return 0, fmt.Errorf("desync: failed to chunk stream for %s: %w", name, err)
	}

	if err := c.IndexStore.StoreIndex(name, idx); err != nil {
		return 0, fmt.Errorf("desync: failed to store index %s: %w", name, err)
	}

	return idx.TotalSize(), nil
}

// StatObject returns metadata for the named object.
// For S3 backends with original-size metadata, this is a single HEAD request.
func (c *Client) StatObject(name string) (ObjectInfo, error) {
	info, err := c.IndexStore.StatIndex(name)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Name:    name,
		Size:    info.Size,
		ModTime: info.ModTime,
	}, nil
}

// HasObject checks if the named object exists using a cheap HEAD request.
func (c *Client) HasObject(name string) (bool, error) {
	return c.IndexStore.HasIndex(name)
}

// RemoveObject deletes the index for the named object.
// Chunks are not removed — they are reclaimed by safe-pruning.
func (c *Client) RemoveObject(name string) error {
	pruner, ok := c.RawIndexStore.(IndexPruneStore)
	if !ok {
		return fmt.Errorf("desync: index store does not support deletion")
	}
	return pruner.DeleteIndexes(context.Background(), []string{name})
}

// ListObjects returns the names of all indexes matching the given prefix.
func (c *Client) ListObjects(prefix string) ([]string, error) {
	listable, ok := c.RawIndexStore.(ListableIndexStore)
	if !ok {
		return nil, fmt.Errorf("desync: index store does not support listing")
	}
	return listable.ListIndexes(context.Background(), prefix)
}

// PutObjectFromFile chunks a file and stores the chunks + index.
//
// For large files (> MaxChunk * N), uses IndexFromFile for parallel
// content-defined chunking followed by ChopFile for storage.
// For small files, uses single-pass ChunkStream which avoids the
// overhead of parallel chunkers and double file traversal.
//
// The optional progress function is called periodically with bytes processed.
func (c *Client) PutObjectFromFile(name, path string, progress ProgressFunc) error {
	size, err := GetFileSize(path)
	if err != nil {
		return fmt.Errorf("desync: stat %s: %w", path, err)
	}

	// Acquire per-transfer byte reservation if gating is configured.
	if c.Gate != nil {
		slot, err := c.Gate.Acquire(context.Background(), int64(size))
		if err != nil {
			return fmt.Errorf("desync: in-flight limit: %w", err)
		}
		defer slot.Release()
	}

	// Small files: single-pass ChunkStream (lower overhead).
	if size <= c.MaxChunk*uint64(c.N) {
		return c.putObjectSmall(name, path, progress)
	}

	// Large files: parallel IndexFromFile + ChopFile (better throughput).
	var pb ProgressBar = NullProgressBar{}
	if progress != nil {
		pb = &progressBarFunc{progress: progress}
	}

	idx, _, err := IndexFromFile(context.Background(), path, c.N, c.MinChunk, c.AvgChunk, c.MaxChunk, pb)
	if err != nil {
		return fmt.Errorf("desync: failed to index %s: %w", path, err)
	}

	if err := ChopFile(context.Background(), path, idx.Chunks, c.WriteStore, c.N, NullProgressBar{}, nil, 0); err != nil {
		return fmt.Errorf("desync: failed to store chunks for %s: %w", name, err)
	}

	if err := c.IndexStore.StoreIndex(name, idx); err != nil {
		return fmt.Errorf("desync: failed to store index %s: %w", name, err)
	}
	return nil
}

// putObjectSmall uses single-pass ChunkStream for small files.
func (c *Client) putObjectSmall(name, path string, progress ProgressFunc) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("desync: open %s: %w", path, err)
	}
	defer f.Close()

	chunker, err := NewChunker(f, c.MinChunk, c.AvgChunk, c.MaxChunk)
	if err != nil {
		return fmt.Errorf("desync: failed to create chunker: %w", err)
	}

	var ci ChunkerInterface = &chunker
	if progress != nil {
		ci = &progressChunkerWrapper{c: &chunker, progress: progress}
	}

	idx, err := ChunkStream(context.Background(), ci, c.WriteStore, c.N, nil, 0)
	chunker.Release()
	if err != nil {
		return fmt.Errorf("desync: failed to chunk stream for %s: %w", name, err)
	}

	if err := c.IndexStore.StoreIndex(name, idx); err != nil {
		return fmt.Errorf("desync: failed to store index %s: %w", name, err)
	}
	return nil
}

// progressBarFunc adapts a ProgressFunc to the ProgressBar interface.
type progressBarFunc struct {
	progress ProgressFunc
}

func (p *progressBarFunc) SetTotal(total int)          {}
func (p *progressBarFunc) Start()                      {}
func (p *progressBarFunc) Set(current int)             { p.progress(int64(current)) }
func (p *progressBarFunc) Add(n int) int               { return 0 }
func (p *progressBarFunc) Increment() int              { return 0 }
func (p *progressBarFunc) Finish()                     {}
func (p *progressBarFunc) Write(b []byte) (int, error) { return len(b), nil }

// GetObjectToFile fetches an object and writes it to a file.
// Chunks are fetched in parallel via the shared worker pool and written
// out of order using WriteAt, matching AssembleFile's concurrency.
// The optional progress function is called after each chunk is written.
func (c *Client) GetObjectToFile(name, path string, progress ProgressFunc) error {
	idx, err := c.IndexStore.GetIndex(name)
	if err != nil {
		return err
	}

	// Acquire per-transfer byte reservation if gating is configured.
	if c.Gate != nil {
		slot, err := c.Gate.Acquire(context.Background(), idx.TotalSize())
		if err != nil {
			return fmt.Errorf("desync: in-flight limit: %w", err)
		}
		defer slot.Release()
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("desync: create %s: %w", path, err)
	}
	if err := f.Truncate(idx.TotalSize()); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("desync: truncate %s: %w", path, err)
	}

	pool := GetWorkerPool()
	if pool == nil {
		// No worker pool: fall back to sequential Object read.
		obj := &Object{idx: idx, store: c.ReadStore, n: c.N, size: idx.TotalSize(), Progress: progress}
		if _, err := io.Copy(f, obj); err != nil {
			f.Close()
			obj.Close()
			os.Remove(path)
			return fmt.Errorf("desync: writing %s: %w", path, err)
		}
		obj.Close()
		return f.Close()
	}

	nChunks := len(idx.Chunks)
	window := c.N
	if window <= 0 {
		window = 10
	}

	// Two-phase pipeline: fetch+decompress → file write.
	fetchCh := make(chan FetchResult, window)
	writeCh := make(chan WriteResult, window)

	fetchPending := 0
	nextSubmit := 0
	writePending := 0

	// Fill initial prefetch window.
	for fetchPending < window && nextSubmit < nChunks {
		pool.SubmitFetch(idx.Chunks[nextSubmit].ID, c.ReadStore, nextSubmit, fetchCh)
		nextSubmit++
		fetchPending++
	}

	var written int64
	var firstErr error

	// Drain both channels until all chunks are fetched and written.
	for fetchPending > 0 || writePending > 0 {
		select {
		case fr := <-fetchCh:
			fetchPending--

			// Submit more fetches to keep the window full.
			if nextSubmit < nChunks {
				pool.SubmitFetch(idx.Chunks[nextSubmit].ID, c.ReadStore, nextSubmit, fetchCh)
				nextSubmit++
				fetchPending++
			}

			if fr.Err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("desync: chunk fetch error: %w", fr.Err)
				}
				continue
			}

			// Submit write to file I/O workers.
			pool.SubmitFileWrite(f, fr.Data, int64(idx.Chunks[fr.Idx].Start), fr.Chunk, fr.Idx, writeCh)
			writePending++

		case wr := <-writeCh:
			writePending--
			if wr.Err != nil && firstErr == nil {
				firstErr = fmt.Errorf("desync: write chunk: %w", wr.Err)
			}
			written += int64(wr.Written)
			if progress != nil {
				progress(written)
			}
		}
	}

	if firstErr != nil {
		f.Close()
		os.Remove(path)
		return firstErr
	}
	return f.Close()
}

// progressChunkerWrapper wraps a Chunker to report progress after each chunk.
type progressChunkerWrapper struct {
	c        ChunkerInterface
	progress ProgressFunc
	total    int64
}

func (w *progressChunkerWrapper) Next() (uint64, []byte, error) {
	start, b, err := w.c.Next()
	if err == nil && len(b) > 0 {
		w.total += int64(len(b))
		w.progress(w.total)
	}
	return start, b, err
}

func (w *progressChunkerWrapper) Min() uint64 { return w.c.Min() }
func (w *progressChunkerWrapper) Avg() uint64 { return w.c.Avg() }
func (w *progressChunkerWrapper) Max() uint64 { return w.c.Max() }

// --- Gated store wrappers (per-storage-operation concurrency control) ---

type gatedStore struct {
	Store
	gate TransferGate
}

func (s *gatedStore) GetChunk(id ChunkID) (*Chunk, error) {
	if err := s.gate.AcquireOp(context.Background()); err != nil {
		return nil, err
	}
	defer s.gate.ReleaseOp()
	return s.Store.GetChunk(id)
}

type gatedWriteStore struct {
	WriteStore
	gate TransferGate
}

func (s *gatedWriteStore) GetChunk(id ChunkID) (*Chunk, error) {
	if err := s.gate.AcquireOp(context.Background()); err != nil {
		return nil, err
	}
	defer s.gate.ReleaseOp()
	return s.WriteStore.GetChunk(id)
}

func (s *gatedWriteStore) StoreChunk(c *Chunk) error {
	if err := s.gate.AcquireOp(context.Background()); err != nil {
		return err
	}
	defer s.gate.ReleaseOp()
	return s.WriteStore.StoreChunk(c)
}

type gatedIndexWriteStore struct {
	IndexWriteStore
	gate TransferGate
}

func (s *gatedIndexWriteStore) GetIndex(name string) (Index, error) {
	if err := s.gate.AcquireOp(context.Background()); err != nil {
		return Index{}, err
	}
	defer s.gate.ReleaseOp()
	return s.IndexWriteStore.GetIndex(name)
}

func (s *gatedIndexWriteStore) StoreIndex(name string, idx Index) error {
	if err := s.gate.AcquireOp(context.Background()); err != nil {
		return err
	}
	defer s.gate.ReleaseOp()
	return s.IndexWriteStore.StoreIndex(name, idx)
}
