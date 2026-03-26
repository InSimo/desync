package desync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const DefaultErrorRetry = 3
const DefaultErrorRetryBaseInterval = 500 * time.Millisecond
const DefaultSafePropagationTime = 1 * time.Second

// Store is a generic interface implemented by read-only stores, like SSH or
// HTTP remote stores currently.
type Store interface {
	// GetChunk reads and returns a chunk from the store.  When the
	// global chunk pool is initialized (via Init), implementations
	// return pooled chunks with pre-allocated buffers.  Callers must
	// call chunk.Release() when done to return it to the pool.
	GetChunk(id ChunkID) (*Chunk, error)
	HasChunk(id ChunkID) (bool, error)
	io.Closer
	fmt.Stringer
}

// WriteStore is implemented by stores supporting both read and write operations
// such as a local store or an S3 store.
type WriteStore interface {
	Store
	StoreChunk(c *Chunk) error
}

// PruneStore is a store that supports read, write and pruning of chunks
type PruneStore interface {
	WriteStore
	Prune(ctx context.Context, ids map[ChunkID]int64, dryRun bool) ([]ChunkID, PruneStats, error)

	// ListChunks returns every chunk-related entry in the store together with
	// its pruning status.
	ListChunks(ctx context.Context) ([]ChunkEntry, error)

	// DeleteChunk removes the chunk data file for id. No-op if absent.
	DeleteChunk(id ChunkID) error
}

// ChunkStatus describes the pruning state of a chunk in the store.
type ChunkStatus int

const (
	ChunkStatusNormal           ChunkStatus = iota // .cacnk only
	ChunkStatusPrunable                            // .cacnk + .prunable (no protect)
	ChunkStatusProtected                           // .cacnk + .prunable + .protect
	ChunkStatusOrphanedPrunable                    // .prunable without .cacnk
	ChunkStatusOrphanedProtect                     // .protect without .cacnk or .prunable
)

// ChunkEntry pairs a chunk ID with its current pruning state and on-disk size.
type ChunkEntry struct {
	ID         ChunkID
	Status     ChunkStatus
	StoredSize int64 // on-disk/object size of the chunk data file; 0 for orphaned markers
}

// SafePruneStore extends PruneStore with the protect-marker safe pruning
// protocol. Stores that implement this interface can be pruned concurrently
// with ongoing chunk write operations. See doc/safe-pruning.md for the full
// protocol description.
type SafePruneStore interface {
	PruneStore

	// SafePrune runs one iteration of the safe pruning algorithm. The first
	// call marks unreferenced chunks with a .prunable companion file; the
	// second call deletes chunks that are still unreferenced and unprotected.
	// If finalizeOnly is true, Normal chunks are left unmarked — only
	// already-prunable chunks are deleted (or restored if protected).
	SafePrune(ctx context.Context, ids map[ChunkID]int64, finalizeOnly bool, dryRun bool) ([]ChunkID, PruneStats, error)

	// HasPrunable reports whether the .prunable companion for id is present.
	// Called by writers to detect chunks that need protect markers before the
	// index is committed.
	HasPrunable(id ChunkID) (bool, error)

	// DeletePrunable removes the .prunable companion for id. No-op if absent.
	DeletePrunable(id ChunkID) error

	// CreatePrunable creates an empty .prunable companion for id.
	CreatePrunable(id ChunkID) error

	// CreateProtect creates an empty .protect companion for id. Writers call
	// this when reusing a prunable chunk to prevent the pruner from deleting it.
	CreateProtect(id ChunkID) error

	// DeleteProtect removes the .protect companion for id. No-op if absent.
	// Called by the pruner to clean up protect markers.
	DeleteProtect(id ChunkID) error

	// HasProtect reports whether the .protect companion for id is present.
	// Called by the pruner as the TOCTOU guard before deleting a prunable chunk.
	HasProtect(id ChunkID) (bool, error)
}

// IndexInfo holds metadata about an index without requiring the full index
// to be downloaded and parsed. Size is the original (uncompressed) content
// size, not the size of the index file itself.
type IndexInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// IndexStore is implemented by stores that hold indexes.
type IndexStore interface {
	GetIndexReader(name string) (io.ReadCloser, error)
	GetIndex(name string) (Index, error)
	HasIndex(name string) (bool, error)
	// StatIndex returns metadata about the named index. For backends that
	// support user metadata (S3, GCS), the original content size is read
	// from a HEAD/stat request without downloading the full index. For
	// other backends, StatIndex falls back to GetIndex internally.
	StatIndex(name string) (IndexInfo, error)
	io.Closer
	fmt.Stringer
}

// IndexWriteStore is used by stores that support reading and writing of indexes.
type IndexWriteStore interface {
	IndexStore
	StoreIndex(name string, idx Index) error
}

// ListableIndexStore is implemented by index stores that support listing
// stored indexes. The prefix parameter restricts results to a subdirectory
// (e.g. "ab/cd"); pass "" to list all indexes. Returned names are always
// relative to the store root (i.e. full index names), regardless of prefix.
type ListableIndexStore interface {
	ListIndexes(ctx context.Context, prefix string) ([]string, error)
}

// IndexPruneStore is implemented by index stores that support deleting indexes.
type IndexPruneStore interface {
	IndexStore
	ListableIndexStore
	PruneIndexes(ctx context.Context, keep map[string]struct{}) error
	DeleteIndexes(ctx context.Context, names []string) error
}

// SafePruneIndexStore extends IndexPruneStore with the two-run safe pruning
// protocol for indexes. Unlike the chunk safe-pruning protocol, no writer
// cooperation is required: StoreIndex is atomic, so there is no window between
// writing an index and it being fully committed. The race this protocol guards
// against is at the operational level — between the moment the caller assembles
// the keep set and the moment ListIndexes runs inside a single prune operation.
// See doc/safe-pruning-index.md for the full protocol description.
type SafePruneIndexStore interface {
	IndexPruneStore

	// SafePruneIndexes runs one iteration of the safe index pruning algorithm.
	// An index is only deleted when it has been absent from the keep set in two
	// consecutive calls, ensuring that an index written after the keep set was
	// assembled cannot be deleted in the same operation.
	SafePruneIndexes(ctx context.Context, keep map[string]struct{}) error

	// ReadPrunableIndexSet returns the set of index names recorded as deletion
	// candidates by the previous SafePruneIndexes call. Returns an empty set if
	// no prior state exists (first run).
	ReadPrunableIndexSet(ctx context.Context) (map[string]struct{}, error)

	// WritePrunableIndexSet persists the current set of deletion candidates so
	// that the next SafePruneIndexes call can consult it. An empty slice removes
	// any previously written state.
	WritePrunableIndexSet(ctx context.Context, names []string) error
}

// StoreOptions provide additional common settings used in chunk stores, such as compression
// error retry or timeouts. Not all options available are applicable to all types of stores.
type StoreOptions struct {
	// Concurrency used in the store. Depending on store type, it's used for
	// the number of goroutines, processes, or connection pool size.
	N int `json:"n,omitempty"`

	// Cert file name for HTTP SSL connections that require mutual SSL.
	ClientCert string `json:"client-cert,omitempty"`
	// Key file name for HTTP SSL connections that require mutual SSL.
	ClientKey string `json:"client-key,omitempty"`

	// CA certificates to trust in TLS connections. If not set, the systems CA store is used.
	CACert string `json:"ca-cert,omitempty"`

	// Trust any certificate presented by the remote chunk store.
	TrustInsecure bool `json:"trust-insecure,omitempty"`

	// Authorization header value for HTTP stores
	HTTPAuth string `json:"http-auth,omitempty"`

	// Cookie header value for HTTP stores
	HTTPCookie string `json:"http-cookie,omitempty"`

	// Timeout for waiting for objects to be retrieved. Infinite if negative. Default: 1 minute
	Timeout time.Duration `json:"timeout,omitempty"`

	// Number of times object retrieval should be attempted on error. Useful when dealing
	// with unreliable connections.
	ErrorRetry int `json:"error-retry,omitempty"`

	// Number of nanoseconds to wait before first retry attempt.
	// Retry attempt number N for the same request will wait N times this interval.
	ErrorRetryBaseInterval time.Duration `json:"error-retry-base-interval,omitempty"`

	// If SkipVerify is true, this store will not verify the data it reads and serves up. This is
	// helpful when a store is merely a proxy and the data will pass through additional stores
	// before being used. Verifying the checksum of a chunk requires it be uncompressed, so if
	// a compressed chunkstore is being proxied, all chunks would have to be decompressed first.
	// This setting avoids the extra overhead. While this could be used in other cases, it's not
	// recommended as a damaged chunk might be processed further leading to unpredictable results.
	SkipVerify bool `json:"skip-verify,omitempty"`

	// Store and read chunks uncompressed, without chunk file extension
	Uncompressed bool `json:"uncompressed"`

	// SafePruning enables the protect-marker safe pruning protocol, which
	// allows the prune command to run concurrently with chunk write operations
	// without risking deletion of newly written chunks. See doc/safe-pruning.md.
	SafePruning bool `json:"safe-pruning,omitempty"`

	// SafePropagationTime is the maximum time for a write to become visible to
	// all readers in an eventually-consistent store (e.g. S3, GCS). Writers
	// wait 2×SafePropagationTime after adding .protect markers before committing
	// an index. Set to 0 (the default) for immediately-consistent stores such
	// as local filesystems or SFTP. Must be set to at most 1/2 of the minimum
	// expected pruner cycle time for the protocol to be correct.
	SafePropagationTime time.Duration `json:"safe-propagation-time,omitempty"`
}

// NewStoreOptionsWithDefaults creates a new StoreOptions struct with the default values set
func NewStoreOptionsWithDefaults() (o StoreOptions) {
	o.ErrorRetry = DefaultErrorRetry
	o.ErrorRetryBaseInterval = DefaultErrorRetryBaseInterval
	o.SafePropagationTime = DefaultSafePropagationTime
	return o
}

func (o *StoreOptions) UnmarshalJSON(data []byte) error {
	// Set all the default values before loading the JSON store options
	o.ErrorRetry = DefaultErrorRetry
	o.ErrorRetryBaseInterval = DefaultErrorRetryBaseInterval
	o.SafePropagationTime = DefaultSafePropagationTime
	type Alias StoreOptions
	return json.Unmarshal(data, (*Alias)(o))
}

// Returns data converters that convert between plain and storage-format. Each layer
// represents a modification such as compression or encryption and is applied in order
// depending on the direction of data. If data is written to storage, the layer's toStorage
// method is called in the order they are returned. If data is read, the fromStorage
// method is called in reverse order.
func (o *StoreOptions) converters() []converter {
	var m []converter
	if !o.Uncompressed {
		m = append(m, Compressor{})
	}
	return m
}
