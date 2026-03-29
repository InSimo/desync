package desync

import (
	"sync"
	"sync/atomic"
)

// WorkerPool provides fixed-size pools of goroutines for chunk processing
// (CPU-bound: hashing) and storage operations (I/O-bound: S3/filesystem).
// A single global pool is shared across all concurrent ChunkStream callers,
// preventing goroutine explosion when multiple LFS sessions run in parallel.
type WorkerPool struct {
	processQ chan processTask
	storageQ chan storageTask
}

// processTask is submitted to a ChunkProcessWorker.
// The worker computes the chunk ID (SHA hash), records the result,
// then enqueues a storageTask for the I/O stage.
type processTask struct {
	chunk    *Chunk
	start    uint64
	num      int
	store    *ChunkStorage
	record   func(int, IndexChunk) // callback to record IndexChunk result
	wg       *sync.WaitGroup       // Done() called after storage completes
	firstErr *atomic.Pointer[error] // first error from any task in this stream
	storageQ chan<- storageTask     // where to enqueue the followup store task
}

// storageTask is submitted to a StorageWorker.
// The worker stores the chunk (compression + I/O) and releases it.
type storageTask struct {
	chunk    *Chunk
	store    *ChunkStorage
	wg       *sync.WaitGroup
	firstErr *atomic.Pointer[error]
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
// processWorkers: number of CPU-bound workers (hashing).
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
		globalWorkerPool = &WorkerPool{
			processQ: make(chan processTask, pw*2),
			storageQ: make(chan storageTask, sw*2),
		}
		for range pw {
			go globalWorkerPool.processWorker()
		}
		for range sw {
			go globalWorkerPool.storageWorker()
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
		// CPU work: compute chunk ID (SHA hash).
		id := task.chunk.ID()
		idxChunk := IndexChunk{
			Start: task.start,
			Size:  uint64(len(task.chunk.data)),
			ID:    id,
		}
		task.record(task.num, idxChunk)

		// Enqueue storage task for the I/O stage.
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
