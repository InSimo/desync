package desync

import (
	"context"
	"fmt"
	"io"
	"time"
)

// ClientOptions configures a Client.
type ClientOptions struct {
	// MinChunk, AvgChunk, MaxChunk are chunk size boundaries in bytes.
	MinChunk, AvgChunk, MaxChunk uint64
	// N is the max number of in-flight chunks for prefetch (default 10).
	N int
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
	N             int // max in-flight chunks for prefetch
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
	return &Client{
		ReadStore:     readStore,
		WriteStore:    writeStore,
		IndexStore:    indexStore,
		RawIndexStore: indexStore,
		MinChunk:      opts.MinChunk,
		AvgChunk:      opts.AvgChunk,
		MaxChunk:      opts.MaxChunk,
		N:             n,
	}
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
