package desync

import (
	"sync"
	"sync/atomic"
)

// WorkerPool provides fixed-size pools of goroutines for:
//   - chunk processing (CPU-bound: hashing, compression, decompression)
//   - storage operations (I/O-bound: S3/filesystem PUT/GET)
//   - file I/O (WriteAt for downloads)
//   - chunking (CPU-bound: rolling hash, runs pChunker.start to completion)
//
// A single global pool is shared across all concurrent callers,
// preventing goroutine explosion when multiple LFS sessions run in parallel.
type WorkerPool struct {
	processQ    chan processTask
	storageQ    chan storageTask
	fileWriteQ  chan fileWriteTask
	chunkerQ    chan chunkerTask
}

// processTask is submitted to a ChunkProcessWorker.
type processTask struct {
	// Upload: compute chunk ID (hash), record result, enqueue storageTask.
	// Fields used: chunk, start, num, store, record, wg, firstErr, storageQ.
	chunk    *Chunk
	start    uint64
	num      int
	store    *ChunkStorage
	record   func(int, IndexChunk)
	wg       *sync.WaitGroup
	firstErr *atomic.Pointer[error]
	storageQ chan<- storageTask

	// Download: decompress chunk, send result to caller.
	// Fields used: getChunk, getData, getIdx, resultCh.
	getChunk *Chunk              // compressed chunk from storage
	getData  bool                // true = download (decompress) task
	getIdx   int                 // caller-provided index
	resultCh chan<- FetchResult  // where to send decompressed result
}

// storageTask is submitted to a StorageWorker.
type storageTask struct {
	// Upload: store chunk (compress + I/O), then wg.Done().
	chunk    *Chunk
	store    *ChunkStorage
	wg       *sync.WaitGroup
	firstErr *atomic.Pointer[error]

	// Download: fetch chunk from store, then enqueue decompress task.
	getID    ChunkID             // chunk to fetch
	getStore Store               // store to fetch from
	getData  bool                // true = download (fetch) task
	getIdx   int                 // caller-provided index, passed through to FetchResult
	processQ chan<- processTask  // where to enqueue decompress followup
	resultCh chan<- FetchResult  // passed through to the decompress task
}

// chunkerTask is a long-running task: run a pChunker's start() to completion.
// Submitted to the chunker pool by PutObjectFromFile.
type chunkerTask struct {
	fn  func() // runs pChunker.start(ctx) — blocks until sync or EOF
}

// FetchResult carries a decompressed chunk from the download pipeline.
type FetchResult struct {
	Idx   int    // caller-provided index, passed through unchanged
	Chunk *Chunk
	Data  []byte
	Err   error
}

// fileWriteTask is submitted to a file I/O worker for WriteAt operations.
type fileWriteTask struct {
	f        WriteAtCloser       // file to write to
	data     []byte              // chunk data to write
	offset   int64               // file offset
	chunk    *Chunk              // released after write
	idx      int                 // caller-provided index, passed through
	resultCh chan<- WriteResult  // where to send completion
}

// WriteAtCloser is an interface for concurrent file writes.
type WriteAtCloser interface {
	WriteAt(b []byte, off int64) (int, error)
}

// WriteResult carries the result of a file write operation.
type WriteResult struct {
	Idx     int   // caller-provided index, passed through
	Written int   // bytes written
	Err     error
}

// GetWorkerPool returns the global pool, or nil if not initialized.
// Exported for use by Forgejo's DesyncStorage.
func GetWorkerPool() *WorkerPool {
	return getWorkerPool()
}

// SubmitFetch enqueues a chunk fetch+decompress task. The result is sent
// to resultCh when both the storage fetch and decompression are complete.
// idx is passed through unchanged in FetchResult.Idx.
func (p *WorkerPool) SubmitFetch(id ChunkID, store Store, idx int, resultCh chan<- FetchResult) {
	p.storageQ <- storageTask{
		getID:    id,
		getStore: store,
		getData:  true,
		getIdx:   idx,
		processQ: p.processQ,
		resultCh: resultCh,
	}
}

// SubmitFileWrite enqueues a file write task. The file write worker
// writes data at the specified offset and sends the result to resultCh.
func (p *WorkerPool) SubmitFileWrite(f WriteAtCloser, data []byte, offset int64, chunk *Chunk, idx int, resultCh chan<- WriteResult) {
	p.fileWriteQ <- fileWriteTask{
		f:        f,
		data:     data,
		offset:   offset,
		chunk:    chunk,
		idx:      idx,
		resultCh: resultCh,
	}
}

var (
	globalWorkerPool     *WorkerPool
	globalWorkerPoolOnce sync.Once
	globalWorkerPoolMu   sync.Mutex
	poolConfig     struct {
		processWorkers int
		storageWorkers int
	}
)

// InitWorkerPool configures the global worker pool sizes.  Must be called
// before the first ChunkStream invocation.  If not called, ChunkStream
// falls back to the legacy per-call goroutine model.
//
// processWorkers: number of CPU-bound workers (hashing, compression, decompression).
// storageWorkers: number of I/O-bound workers (S3 PUT/GET, filesystem).
func InitWorkerPool(processWorkers, storageWorkers int) {
	globalWorkerPoolMu.Lock()
	defer globalWorkerPoolMu.Unlock()
	poolConfig.processWorkers = processWorkers
	poolConfig.storageWorkers = storageWorkers
}

// getWorkerPool returns the global pool, initializing it on first call.
// Returns nil if InitWorkerPool was never called (legacy mode).
func getWorkerPool() *WorkerPool {
	globalWorkerPoolMu.Lock()
	pw := poolConfig.processWorkers
	sw := poolConfig.storageWorkers
	globalWorkerPoolMu.Unlock()

	if pw == 0 && sw == 0 {
		return nil // legacy mode
	}

	globalWorkerPoolOnce.Do(func() {
		if pw <= 0 {
			pw = 10
		}
		if sw <= 0 {
			sw = 20
		}
		// File write workers default to same count as storage workers.
		// Chunker workers = 2×process workers so two large files can be
		// chunked in parallel (each needing up to pw chunker tasks).
		fw := sw
		cw := pw * 2
		globalWorkerPool = &WorkerPool{
			processQ:   make(chan processTask, pw*2),
			storageQ:   make(chan storageTask, sw*2),
			fileWriteQ: make(chan fileWriteTask, fw*2),
			chunkerQ:   make(chan chunkerTask, cw*2),
		}
		for range pw {
			go globalWorkerPool.processWorker()
		}
		for range sw {
			go globalWorkerPool.storageWorker()
		}
		for range fw {
			go globalWorkerPool.fileWriteWorker()
		}
		for range cw {
			go globalWorkerPool.chunkerWorker()
		}
	})
	return globalWorkerPool
}

// setFirstErr records the first error encountered during a stream.
func setFirstErr(p *atomic.Pointer[error], err error) {
	p.CompareAndSwap(nil, &err)
}

func (p *WorkerPool) processWorker() {
	for task := range p.processQ {
		if task.getData {
			// Download: decompress chunk and send result.
			data, err := task.getChunk.Data()
			task.resultCh <- FetchResult{
				Idx:   task.getIdx,
				Chunk: task.getChunk,
				Data:  data,
				Err:   err,
			}
			continue
		}

		// Upload: compute chunk ID (SHA hash), record, enqueue store.
		id := task.chunk.ID()
		idxChunk := IndexChunk{
			Start: task.start,
			Size:  uint64(len(task.chunk.data)),
			ID:    id,
		}
		task.record(task.num, idxChunk)

		task.storageQ <- storageTask{
			chunk:    task.chunk,
			store:    task.store,
			wg:       task.wg,
			firstErr: task.firstErr,
		}
	}
}

func (p *WorkerPool) storageWorker() {
	for task := range p.storageQ {
		if task.getData {
			// Download: fetch chunk from store, enqueue decompress.
			chunk, err := task.getStore.GetChunk(task.getID)
			if err != nil {
				task.resultCh <- FetchResult{Idx: task.getIdx, Err: err}
				continue
			}
			task.processQ <- processTask{
				getChunk: chunk,
				getData:  true,
				getIdx:   task.getIdx,
				resultCh: task.resultCh,
			}
			continue
		}

		// Upload: store chunk (compress + I/O).
		captured, err := task.store.StoreChunk(task.chunk)
		if err != nil {
			setFirstErr(task.firstErr, err)
		}
		if !captured {
			task.chunk.Release()
		}
		task.wg.Done()
	}
}

// SubmitChunker enqueues a long-running chunker task. The task function
// runs a pChunker.start() to completion (rolling hash until sync or EOF).
func (p *WorkerPool) SubmitChunker(fn func()) {
	p.chunkerQ <- chunkerTask{fn: fn}
}

func (p *WorkerPool) chunkerWorker() {
	for task := range p.chunkerQ {
		task.fn()
	}
}

func (p *WorkerPool) fileWriteWorker() {
	for task := range p.fileWriteQ {
		n, err := task.f.WriteAt(task.data, task.offset)
		if task.chunk != nil {
			task.chunk.Release()
		}
		task.resultCh <- WriteResult{Idx: task.idx, Written: n, Err: err}
	}
}
