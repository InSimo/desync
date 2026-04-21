package desync

import (
	"fmt"
	"io"
)

// Object provides sequential and random-access reads over a desync-stored
// file, reassembling it from chunks on the fly. It uses a sliding-window
// prefetch: at most N chunks are in-flight at any time. Results arrive on
// a single channel and are parked in a pre-allocated array indexed by
// absolute chunk position.
//
// Position invariant (single source of truth):
//
//	The next byte to serve is idx.Chunks[chunkIdx].Start + chunkOffset.
//	For 0 <= i < chunkIdx, chunks[i] is released (zero slot).
//	If chunkIdx <  len(idx.Chunks): 0 <= chunkOffset < Chunks[chunkIdx].Size.
//	If chunkIdx == len(idx.Chunks): we are at EOF and chunkOffset == 0.
//
// Every state-mutating method must preserve this invariant.
//
// Object implements io.Reader, io.Seeker, and io.Closer.
type Object struct {
	idx      Index
	store    Store
	n        int // max in-flight chunks
	size     int64
	Progress ProgressFunc // optional, called when a chunk is released

	// Prefetch pipeline. chunks[i] holds the state for chunk i of the index.
	chunks []objectChunkSlot

	// Scheduling
	nextRequestIdx int // next chunk to submit to workers
	inFlight       int // chunks requested but not yet arrived

	// I/O
	resultCh chan FetchResult // single channel for all results

	// Position (see invariant above).
	chunkIdx    int
	chunkOffset int64

	closed bool
}

type objectChunkSlot struct {
	chunk     *Chunk
	data      []byte
	err       error
	requested bool
	valid     bool
}

// Stat returns metadata for this object.
func (o *Object) Stat() (ObjectInfo, error) {
	return ObjectInfo{Size: o.size}, nil
}

// currentOffset returns the global byte position in [0, size].
func (o *Object) currentOffset() int64 {
	if o.chunkIdx >= len(o.idx.Chunks) {
		return o.size
	}
	return int64(o.idx.Chunks[o.chunkIdx].Start) + o.chunkOffset
}

// splitOffset converts a global byte offset to (chunkIdx, chunkOffset)
// preserving the position invariant. Called only from Seek.
func (o *Object) splitOffset(global int64) (int, int64) {
	if global >= o.size {
		return len(o.idx.Chunks), 0
	}
	// Linear scan — runs once per Seek, not once per Read.
	for i, c := range o.idx.Chunks {
		if int64(c.Start+c.Size) > global {
			return i, global - int64(c.Start)
		}
	}
	return len(o.idx.Chunks), 0
}

// Read reads up to len(p) bytes from the object, automatically fetching
// and buffering chunks as needed.
func (o *Object) Read(p []byte) (int, error) {
	if o.closed {
		return 0, io.ErrClosedPipe
	}
	if o.chunks == nil {
		o.startPrefetch()
	}

	total := 0
	pool := GetWorkerPool()
	for len(p) > 0 && o.chunkIdx < len(o.idx.Chunks) {
		slot := &o.chunks[o.chunkIdx]
		for !slot.valid {
			o.receiveOne(pool)
		}
		if slot.err != nil {
			if total > 0 {
				return total, nil
			}
			return 0, fmt.Errorf("desync: chunk fetch error: %w", slot.err)
		}
		n := copy(p, slot.data[o.chunkOffset:])
		p = p[n:]
		total += n
		o.chunkOffset += int64(n)
		if int(o.chunkOffset) == len(slot.data) {
			o.releaseSlot(o.chunkIdx)
			o.chunkIdx++
			o.chunkOffset = 0
			o.submitMore(pool)
		}
	}
	if total == 0 {
		return 0, io.EOF
	}
	return total, nil
}

// Seek sets the read position. Chunks already in the prefetch window are
// reused; released chunks will be re-requested on demand.
func (o *Object) Seek(offset int64, whence int) (int64, error) {
	var newGlobal int64
	switch whence {
	case io.SeekStart:
		newGlobal = offset
	case io.SeekCurrent:
		newGlobal = o.currentOffset() + offset
	case io.SeekEnd:
		newGlobal = o.size + offset
	default:
		return 0, fmt.Errorf("desync: invalid seek whence %d", whence)
	}
	if newGlobal < 0 {
		return 0, fmt.Errorf("desync: negative seek position")
	}
	o.chunkIdx, o.chunkOffset = o.splitOffset(newGlobal)

	if o.chunks != nil {
		// Pull the request pointer back if the new position is behind
		// already-issued requests. submitMore skips slots still valid or
		// in flight, so this is cheap.
		if o.chunkIdx < o.nextRequestIdx {
			o.nextRequestIdx = o.chunkIdx
		}
		o.submitMore(GetWorkerPool())
	}
	return o.currentOffset(), nil
}

// Close releases all resources. Any in-flight chunk fetches are drained.
func (o *Object) Close() error {
	if !o.closed {
		o.closed = true
		o.drainPending()
	}
	return nil
}

// Size returns the total uncompressed size of the object.
func (o *Object) Size() int64 {
	return o.size
}

// startPrefetch allocates the slot array and channel and kicks off the
// first window of requests starting from the current chunkIdx.
func (o *Object) startPrefetch() {
	o.chunks = make([]objectChunkSlot, len(o.idx.Chunks))
	o.resultCh = make(chan FetchResult, o.n)
	o.nextRequestIdx = o.chunkIdx
	o.submitMore(GetWorkerPool())
}

// submitMore fills the prefetch window up to o.n in-flight requests.
func (o *Object) submitMore(pool *WorkerPool) {
	for o.inFlight < o.n && o.nextRequestIdx < len(o.idx.Chunks) {
		i := o.nextRequestIdx
		o.nextRequestIdx++
		if o.chunks[i].requested || o.chunks[i].valid {
			continue
		}
		o.chunks[i].requested = true
		o.inFlight++
		pool.SubmitFetch(o.idx.Chunks[i].ID, o.store, i, o.resultCh)
	}
}

// receiveOne reads one result from the channel and parks it.
func (o *Object) receiveOne(pool *WorkerPool) {
	fr := <-o.resultCh
	o.chunks[fr.Idx] = objectChunkSlot{
		chunk:     fr.Chunk,
		data:      fr.Data,
		err:       fr.Err,
		requested: true,
		valid:     true,
	}
	o.inFlight--
	o.submitMore(pool)
}

// releaseSlot releases any memory held by chunk i and zeroes the slot.
// The caller is responsible for advancing past it.
func (o *Object) releaseSlot(i int) {
	if o.chunks[i].chunk != nil {
		o.chunks[i].chunk.Release()
	}
	o.chunks[i] = objectChunkSlot{}
	if o.Progress != nil {
		o.Progress(o.currentOffset())
	}
}

// drainPending consumes all in-flight results and releases all chunks.
func (o *Object) drainPending() {
	if o.resultCh == nil {
		return
	}
	for o.inFlight > 0 {
		fr := <-o.resultCh
		o.inFlight--
		if fr.Chunk != nil {
			fr.Chunk.Release()
		}
	}
	// Release any arrived-but-unconsumed chunks.
	for i := range o.chunks {
		if o.chunks[i].valid && o.chunks[i].chunk != nil {
			o.chunks[i].chunk.Release()
		}
		o.chunks[i] = objectChunkSlot{}
	}
}
