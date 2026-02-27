package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/folbricht/desync"
)

// LFS custom transfer protocol message types.

type initRequest struct {
	Event               string `json:"event"`
	Operation           string `json:"operation"`
	Concurrent          bool   `json:"concurrent"`
	ConcurrentTransfers int    `json:"concurrenttransfers"`
}

type transferRequest struct {
	Event  string      `json:"event"`
	OID    string      `json:"oid"`
	Size   int64       `json:"size"`
	Path   string      `json:"path,omitempty"`
	Action *lfsAction  `json:"action,omitempty"`
}

type lfsAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header"`
}

type progressEvent struct {
	Event          string `json:"event"`
	OID            string `json:"oid"`
	BytesSoFar     int64  `json:"bytesSoFar"`
	BytesSinceLast int64  `json:"bytesSinceLast"`
}

type completeEvent struct {
	Event string    `json:"event"`
	OID   string    `json:"oid"`
	Path  string    `json:"path,omitempty"`
	Error *lfsError `json:"error,omitempty"`
}

type lfsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// oidIndexName converts a Git LFS OID to the index file name used in the
// desync index store. The first 4 characters of the OID are used as a
// sharding prefix directory to avoid flat-directory hot spots:
//
//	"abc123def456..." → "abc1/abc123def456....caibx"
func oidIndexName(oid string) string {
	return oid[0:4] + "/" + oid + ".caibx"
}

// Agent implements the Git LFS custom transfer agent protocol.
type Agent struct {
	writeStore      desync.WriteStore
	readStore       desync.Store // used for downloads; may wrap writeStore with a cache
	indexWriteStore desync.IndexWriteStore
	n               int
	minChunk        uint64
	avgChunk        uint64
	maxChunk        uint64
	tmpDir          string
	enc             *json.Encoder
	mu              sync.Mutex
}

// Run reads LFS protocol messages from stdin and dispatches handlers.
// Output is written to os.Stdout.
func (a *Agent) Run(ctx context.Context) error {
	a.enc = json.NewEncoder(os.Stdout)
	return a.run(ctx, os.Stdin)
}

func (a *Agent) run(ctx context.Context, r io.Reader) error {
	dec := json.NewDecoder(r)

	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return nil // EOF with no messages
	}
	var base struct {
		Event string `json:"event"`
	}
	json.Unmarshal(raw, &base)

	switch base.Event {
	case "init":
		var req initRequest
		json.Unmarshal(raw, &req)
		a.handleInit(raw)
		n := req.ConcurrentTransfers
		if n <= 0 {
			n = 1
		}
		return a.runLoop(ctx, dec, n)
	case "terminate":
		return nil
	default:
		// Unknown first event — treat as serial.
		return a.runLoop(ctx, dec, 1)
	}
}

func (a *Agent) runLoop(ctx context.Context, dec *json.Decoder, concurrentTransfers int) error {
	sem := make(chan struct{}, concurrentTransfers)
	var wg sync.WaitGroup

	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			// EOF — git-lfs closed stdin without "terminate".
			wg.Wait()
			return nil
		}
		var base struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			return fmt.Errorf("parsing event: %w", err)
		}

		switch base.Event {
		case "upload", "download":
			wg.Add(1)
			sem <- struct{}{} // backpressure: blocks if at capacity
			ev := base.Event
			go func(msg json.RawMessage) {
				defer wg.Done()
				defer func() { <-sem }()
				if ev == "upload" {
					a.handleUpload(ctx, msg)
				} else {
					a.handleDownload(ctx, msg)
				}
			}(raw)
		case "terminate":
			wg.Wait()
			return nil
		default:
			// ignore unknown events
		}
	}
}

func (a *Agent) handleInit(raw json.RawMessage) {
	// Reply with empty object to signal readiness.
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enc.Encode(struct{}{})
}

func (a *Agent) handleUpload(ctx context.Context, raw json.RawMessage) {
	var req transferRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		a.sendComplete(req.OID, "", fmt.Errorf("parsing upload request: %w", err))
		return
	}

	// Skip re-upload if this OID is already present in the index store.
	if exists, err := a.indexWriteStore.HasIndex(oidIndexName(req.OID)); err == nil && exists {
		a.sendComplete(req.OID, "", nil)
		return
	}

	// Chunk the file and build an index.
	pb := &lfsProgressBar{agent: a, oid: req.OID, totalBytes: req.Size}
	idx, _, err := desync.IndexFromFile(ctx, req.Path, a.n, a.minChunk, a.avgChunk, a.maxChunk, pb)
	if err != nil {
		a.sendComplete(req.OID, "", err)
		return
	}

	// Wrap the chunk store to count bytes stored.
	cs := &countingWriteStore{WriteStore: a.writeStore}

	// Start progress reporting goroutine.
	progressCtx, stopProgress := context.WithCancel(ctx)
	go a.progressLoop(progressCtx, req.OID, &cs.bytes, req.Size)

	// Store chunks in S3.
	if err := desync.ChopFile(ctx, req.Path, idx.Chunks, cs, a.n, desync.NullProgressBar{}); err != nil {
		stopProgress()
		a.sendComplete(req.OID, "", err)
		return
	}
	stopProgress()

	// Store the index in the S3 index store.
	if err := a.indexWriteStore.StoreIndex(oidIndexName(req.OID), idx); err != nil {
		a.sendComplete(req.OID, "", err)
		return
	}

	a.sendComplete(req.OID, "", nil)
}

func (a *Agent) handleDownload(ctx context.Context, raw json.RawMessage) {
	var req transferRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		a.sendComplete(req.OID, "", fmt.Errorf("parsing download request: %w", err))
		return
	}

	// Fetch the index from the S3 index store.
	idx, err := a.indexWriteStore.GetIndex(oidIndexName(req.OID))
	if err != nil {
		a.sendComplete(req.OID, "", fmt.Errorf("fetching index for %s: %w", req.OID, err))
		return
	}

	// Write to a temporary file; git-lfs moves it to the object store on success.
	tmpFile := filepath.Join(a.tmpDir, "git-lfs-desync-"+req.OID)

	// Wrap the store to count bytes retrieved.
	cs := newCountingReadStore(a.readStore, idx)

	// Start progress reporting goroutine.
	progressCtx, stopProgress := context.WithCancel(ctx)
	go a.progressLoop(progressCtx, req.OID, &cs.bytes, req.Size)

	_, err = desync.AssembleFile(ctx, tmpFile, idx, cs, nil, desync.AssembleOptions{N: a.n})
	stopProgress()
	if err != nil {
		os.Remove(tmpFile)
		a.sendComplete(req.OID, "", err)
		return
	}

	a.sendComplete(req.OID, tmpFile, nil)
}

// progressLoop sends LFS progress events every 500ms until ctx is cancelled.
func (a *Agent) progressLoop(ctx context.Context, oid string, counter *atomic.Int64, total int64) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := counter.Load()
			if cur != last {
				a.sendProgress(oid, cur, cur-last)
				last = cur
			}
		}
	}
}

func (a *Agent) sendProgress(oid string, bytesSoFar, bytesSinceLast int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enc.Encode(progressEvent{
		Event:          "progress",
		OID:            oid,
		BytesSoFar:     bytesSoFar,
		BytesSinceLast: bytesSinceLast,
	})
}

func (a *Agent) sendComplete(oid, path string, err error) {
	evt := completeEvent{Event: "complete", OID: oid, Path: path}
	if err != nil {
		evt.Error = &lfsError{Code: 2, Message: err.Error()}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enc.Encode(evt)
}

// countingWriteStore wraps a WriteStore and counts bytes of chunks stored.
type countingWriteStore struct {
	desync.WriteStore
	bytes atomic.Int64
}

func (s *countingWriteStore) StoreChunk(chunk *desync.Chunk) error {
	err := s.WriteStore.StoreChunk(chunk)
	if err == nil {
		if data, e := chunk.Data(); e == nil {
			s.bytes.Add(int64(len(data)))
		}
	}
	return err
}

// countingReadStore wraps a Store and counts bytes retrieved, using the index
// chunk sizes so progress tracking works without decompressing chunks.
type countingReadStore struct {
	desync.Store
	chunkSizes map[desync.ChunkID]uint64
	bytes      atomic.Int64
}

func newCountingReadStore(s desync.Store, idx desync.Index) *countingReadStore {
	m := make(map[desync.ChunkID]uint64, len(idx.Chunks))
	for _, c := range idx.Chunks {
		m[c.ID] = c.Size
	}
	return &countingReadStore{Store: s, chunkSizes: m}
}

func (s *countingReadStore) GetChunk(id desync.ChunkID) (*desync.Chunk, error) {
	chunk, err := s.Store.GetChunk(id)
	if err == nil {
		s.bytes.Add(int64(s.chunkSizes[id]))
	}
	return chunk, err
}

// lfsProgressBar is a desync.ProgressBar used during IndexFromFile (chunking phase).
// It sends LFS progress events based on the fraction of chunks processed.
type lfsProgressBar struct {
	agent      *Agent
	oid        string
	totalBytes int64
	total      atomic.Int64 // total number of chunks (set by SetTotal)
	current    atomic.Int64 // chunks processed so far
	lastBytes  atomic.Int64 // last bytesSoFar sent
}

func (p *lfsProgressBar) SetTotal(total int) {
	p.total.Store(int64(total))
}

func (p *lfsProgressBar) Start() {}

func (p *lfsProgressBar) Finish() {}

func (p *lfsProgressBar) Increment() int {
	return p.Add(1)
}

func (p *lfsProgressBar) Add(n int) int {
	cur := p.current.Add(int64(n))
	total := p.total.Load()
	if total <= 0 || p.totalBytes <= 0 {
		return int(cur)
	}
	bytesSoFar := int64(float64(p.totalBytes) * float64(cur) / float64(total))
	last := p.lastBytes.Load()
	if bytesSoFar != last {
		p.lastBytes.Store(bytesSoFar)
		p.agent.sendProgress(p.oid, bytesSoFar, bytesSoFar-last)
	}
	return int(cur)
}

func (p *lfsProgressBar) Set(current int) {
	cur := p.current.Load()
	if int64(current) > cur {
		p.Add(int(int64(current) - cur))
	}
}

func (p *lfsProgressBar) Write(b []byte) (n int, err error) {
	return len(b), nil
}
