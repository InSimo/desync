package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/bytelimit"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
)

// LFS custom transfer protocol message types.

type initRequest struct {
	Event               string           `json:"event"`
	Operation           string           `json:"operation"`
	Remote              string           `json:"remote"`
	Concurrent          bool             `json:"concurrent"`
	ConcurrentTransfers int              `json:"concurrenttransfers"`
	SupportsPipelined   bool             `json:"supportspipelined"`
	Config              *json.RawMessage `json:"config,omitempty"`
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

type initResponse struct {
	Pipelined bool      `json:"pipelined,omitempty"`
	Error     *lfsError `json:"error,omitempty"`
}

// probeIndexName and probeChunkID are dummy values used during init to verify
// store connectivity. HasIndex/HasChunk returning (false, nil) is success;
// only a non-nil error indicates a store problem.
const probeIndexName = "0000/0000000000000000000000000000000000000000000000000000000000000000.caibx"

var probeChunkID = desync.ChunkID{} // zero value

// oidIndexName is a package-local alias for cmdshared.OidIndexName.
var oidIndexName = cmdshared.OidIndexName

// Agent implements the Git LFS custom transfer agent protocol.
type Agent struct {
	writeStore      desync.WriteStore
	readStore       desync.Store // used for downloads; may wrap writeStore with a cache
	indexWriteStore desync.IndexWriteStore
	n               int
	minChunk        uint64
	avgChunk        uint64
	maxChunk        uint64
	tmpDir              string
	safePruning         bool
	safePropagationTime time.Duration
	gate             *bytelimit.Gate // cross-process in-flight byte limit; may be nil
	pipelinedEnabled bool            // respond with pipelined=true in init (default true)
	enc              *json.Encoder
	mu               sync.Mutex
	// setup is called once from handleInit with remote and operation from the
	// LFS init message. It expands %(remote)/%(operation) in cfgFromGit, loads
	// config, and initializes writeStore/readStore/indexWriteStore on the Agent.
	// serverConfig, when non-nil, is the server-provided config from the init
	// message; it supplies defaults for store URLs and credentials when the
	// agent was auto-negotiated rather than manually configured.
	// Nil setup means stores are already initialized (used in tests).
	setup func(remote, operation string, serverConfig *cmdshared.Config) error
}

func (a *Agent) Close() {
	if a.writeStore != nil {
		a.writeStore.Close()
	}
	if a.readStore != nil {
		a.readStore.Close()
	}
	if a.indexWriteStore != nil {
		a.indexWriteStore.Close()
	}
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
		if a.pipelinedEnabled && req.SupportsPipelined {
			return a.pipelinedRunLoop(ctx, dec, n)
		}
		return a.runLoop(ctx, dec, n)
	case "terminate":
		return nil
	default:
		// Unknown first event — treat as serial.
		return a.runLoop(ctx, dec, 1)
	}
}

// runLoop processes transfer events from git-lfs one at a time.
//
// The Git LFS custom transfer protocol is always serial per process:
// git-lfs sends one transfer request and waits for the completion
// response before sending the next.  With concurrent=true, git-lfs
// achieves parallelism by spawning N agent processes, each handling
// one object at a time.  Cross-process coordination (memory limits)
// is handled by the bytelimit gate, not by in-process concurrency.
func (a *Agent) runLoop(ctx context.Context, dec *json.Decoder, concurrentTransfers int) error {
	_ = concurrentTransfers // informational only; not used for in-process concurrency

	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			// EOF — git-lfs closed stdin without "terminate".
			return nil
		}
		var base struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			return fmt.Errorf("parsing event: %w", err)
		}

		switch base.Event {
		case "upload":
			a.handleUpload(ctx, raw)
		case "download":
			a.handleDownload(ctx, raw)
		case "terminate":
			return nil
		default:
			// ignore unknown events
		}
	}
}

// pipelinedRunLoop processes transfer events concurrently within a single
// process. git-lfs sends multiple requests without waiting for completion;
// responses are dispatched by OID.
func (a *Agent) pipelinedRunLoop(ctx context.Context, dec *json.Decoder, concurrentTransfers int) error {
	sem := make(chan struct{}, concurrentTransfers)
	var wg sync.WaitGroup

	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break // EOF
		}
		var base struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			break
		}

		switch base.Event {
		case "upload":
			sem <- struct{}{}
			wg.Add(1)
			go func(r json.RawMessage) {
				defer wg.Done()
				defer func() { <-sem }()
				a.handleUpload(ctx, r)
			}(raw)
		case "download":
			sem <- struct{}{}
			wg.Add(1)
			go func(r json.RawMessage) {
				defer wg.Done()
				defer func() { <-sem }()
				a.handleDownload(ctx, r)
			}(raw)
		case "terminate":
			wg.Wait()
			return nil
		}
	}
	wg.Wait()
	return nil
}

func (a *Agent) handleInit(raw json.RawMessage) {
	var req initRequest
	json.Unmarshal(raw, &req) // best-effort; fields have safe zero values

	// Parse server-provided config if present.
	var serverConfig *cmdshared.Config
	if req.Config != nil {
		serverConfig = &cmdshared.Config{}
		if err := json.Unmarshal(*req.Config, serverConfig); err != nil {
			serverConfig = nil // ignore malformed config
		}
	}

	var errMsg string

	// Initialize stores using remote and operation from the init message.
	if a.setup != nil {
		if err := a.setup(req.Remote, req.Operation, serverConfig); err != nil {
			errMsg = "initialization failed: " + err.Error()
		}
	}

	// Probe the index store (only if setup succeeded).
	if errMsg == "" {
		if _, err := a.indexWriteStore.HasIndex(probeIndexName); err != nil {
			errMsg = "index store not available: " + err.Error()
		}
	}

	// Probe the chunk store (writeStore for uploads, readStore for downloads).
	if errMsg == "" {
		var chunkErr error
		switch req.Operation {
		case "upload":
			_, chunkErr = a.writeStore.HasChunk(probeChunkID)
		default: // "download" and anything else
			_, chunkErr = a.readStore.HasChunk(probeChunkID)
		}
		if chunkErr != nil {
			errMsg = "chunk store not available: " + chunkErr.Error()
		}
	}

	resp := initResponse{}
	if errMsg != "" {
		resp.Error = &lfsError{Code: 2, Message: errMsg}
	} else if a.pipelinedEnabled && req.SupportsPipelined {
		resp.Pipelined = true
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.enc.Encode(resp)
}

func (a *Agent) handleUpload(ctx context.Context, raw json.RawMessage) {
	var req transferRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		a.sendComplete(req.OID, "", fmt.Errorf("parsing upload request: %w", err))
		return
	}

	ctx, task := trace.NewTask(ctx, "upload")
	defer task.End()
	trace.Logf(ctx, "oid", "%.12s size=%d", req.OID, req.Size)

	// Skip re-upload if this OID is already present in the index store.
	if exists, err := a.indexWriteStore.HasIndex(oidIndexName(req.OID)); err == nil && exists {
		a.sendComplete(req.OID, "", nil)
		return
	}

	// Acquire in-flight byte slot (blocks if over the cross-process limit).
	if a.gate != nil {
		region := trace.StartRegion(ctx, "acquire-gate")
		slot, err := a.gate.Acquire(ctx, req.Size)
		region.End()
		if err != nil {
			a.sendComplete(req.OID, "", fmt.Errorf("in-flight limit: %w", err))
			return
		}
		defer slot.Release()
	}

	// Chunk the file and build an index.
	var idx desync.Index
	trace.WithRegion(ctx, "index-from-file", func() {
		pb := &lfsProgressBar{agent: a, oid: req.OID, totalBytes: req.Size}
		var err2 error
		idx, _, err2 = desync.IndexFromFile(ctx, req.Path, a.n, a.minChunk, a.avgChunk, a.maxChunk, pb)
		if err2 != nil {
			a.sendComplete(req.OID, "", err2)
		}
	})
	if idx.Chunks == nil {
		return // error already sent
	}
	cmdshared.WriteHeapProfile("upload_after_index")

	// Wrap the chunk store to count bytes stored.
	cs := &countingWriteStore{WriteStore: a.writeStore}

	var sps desync.SafePruneStore
	if a.safePruning {
		if ps, ok := a.writeStore.(desync.SafePruneStore); ok {
			sps = ps
		}
	}

	// Start progress reporting goroutine.
	progressCtx, stopProgress := context.WithCancel(ctx)
	go a.progressLoop(progressCtx, req.OID, &cs.bytes, req.Size)

	// Store chunks in the remote store.
	var chopErr error
	trace.WithRegion(ctx, "chop-file", func() {
		chopErr = desync.ChopFile(ctx, req.Path, idx.Chunks, cs, a.n, desync.NullProgressBar{}, sps, a.safePropagationTime)
	})
	if chopErr != nil {
		stopProgress()
		a.sendComplete(req.OID, "", chopErr)
		return
	}
	cmdshared.WriteHeapProfile("upload_after_chop")
	stopProgress()

	// Store the index in the S3 index store.
	var storeIdxErr error
	trace.WithRegion(ctx, "store-index", func() {
		storeIdxErr = a.indexWriteStore.StoreIndex(oidIndexName(req.OID), idx)
	})
	if storeIdxErr != nil {
		a.sendComplete(req.OID, "", storeIdxErr)
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

	ctx, task := trace.NewTask(ctx, "download")
	defer task.End()
	trace.Logf(ctx, "oid", "%.12s size=%d", req.OID, req.Size)

	// Acquire in-flight byte slot (blocks if over the cross-process limit).
	if a.gate != nil {
		region := trace.StartRegion(ctx, "acquire-gate")
		slot, err := a.gate.Acquire(ctx, req.Size)
		region.End()
		if err != nil {
			a.sendComplete(req.OID, "", fmt.Errorf("in-flight limit: %w", err))
			return
		}
		defer slot.Release()
	}

	// Fetch the index from the S3 index store.
	var idx desync.Index
	var fetchErr error
	trace.WithRegion(ctx, "fetch-index", func() {
		idx, fetchErr = a.indexWriteStore.GetIndex(oidIndexName(req.OID))
	})
	if fetchErr != nil {
		a.sendComplete(req.OID, "", fmt.Errorf("fetching index for %s: %w", req.OID, fetchErr))
		return
	}

	// Write to a temporary file; git-lfs moves it to the object store on success.
	tmpFile := filepath.Join(a.tmpDir, "git-lfs-desync-"+req.OID)

	// Wrap the store to count bytes retrieved.
	cs := newCountingReadStore(a.readStore, idx)

	// Start progress reporting goroutine.
	progressCtx, stopProgress := context.WithCancel(ctx)
	go a.progressLoop(progressCtx, req.OID, &cs.bytes, req.Size)

	var assembleErr error
	trace.WithRegion(ctx, "assemble-file", func() {
		_, assembleErr = desync.AssembleFile(ctx, tmpFile, idx, cs, nil, desync.AssembleOptions{N: a.n})
	})
	stopProgress()
	cmdshared.WriteHeapProfile("download_after_assemble")
	if assembleErr != nil {
		os.Remove(tmpFile)
		a.sendComplete(req.OID, "", assembleErr)
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
