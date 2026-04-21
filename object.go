package desync

import (
	"fmt"
	"io"
)

// Object provides sequential and random-access reads over a desync-stored
// file, reassembling it from chunks on the fly. It uses a sliding-window
// prefetch: at most N chunks are in-flight at any time. Results arrive on
// a single channel and are parked in a pre-allocated array indexed by
// absolute chunk position. New requests are submitted as results arrive,
// so the window slides forward without deadlocking the shared pool.
//
// Object implements io.Reader, io.Seeker, and io.Closer.
type Object struct {
	idx      Index
	store    Store
	n        int // max in-flight chunks
	size     int64
	Progress ProgressFunc // optional, called after each chunk is consumed

	// Per-chunk state — allocated once on first Read, indexed by absolute chunk position.
	chunks []objectChunkSlot

	// Flow control
	chunkIdx       int // current chunk being read (absolute index)
	nextRequestIdx int // next chunk to submit to workers
	inFlight       int // chunks requested but not yet arrived

	// I/O
	resultCh chan FetchResult // single channel for all results
	buf      []byte           // unconsumed bytes from chunks[chunkIdx]
	offset   int64
	closed   bool
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

// Read reads up to len(p) bytes from the object, automatically fetching
// and buffering chunks as needed. Supports the read-seek-read pattern used
// by http.ServeContent: after a Seek, o.buf is cleared even though the
// current chunk may still be valid in the slot — Read re-populates buf
// from the current chunk rather than treating buf-empty as "advance".
func (o *Object) Read(p []byte) (int, error) {
	if o.closed {
		return 0, io.ErrClosedPipe
	}
	if o.chunks == nil {
		startChunk := o.chunkForOffset(o.offset)
		if startChunk >= len(o.idx.Chunks) {
			return 0, io.EOF
		}
		o.startPrefetch(startChunk)
	}

	total := 0
	for len(p) > 0 {
		if len(o.buf) == 0 {
			if o.chunkIdx >= len(o.idx.Chunks) {
				if total > 0 {
					return total, nil
				}
				return 0, io.EOF
			}
			// Wait for the current chunk to arrive (no-op if already valid
			// from a prior Read or a Seek within the prefetch window).
			pool := GetWorkerPool()
			for !o.chunks[o.chunkIdx].valid {
				o.receiveOne(pool)
			}
			if o.chunks[o.chunkIdx].err != nil {
				if total > 0 {
					return total, nil
				}
				return 0, fmt.Errorf("desync: chunk fetch error: %w", o.chunks[o.chunkIdx].err)
			}
			chunkStart := int64(o.idx.Chunks[o.chunkIdx].Start)
			chunkEnd := chunkStart + int64(len(o.chunks[o.chunkIdx].data))
			if o.offset >= chunkEnd {
				// Offset landed at or past the end of this chunk (sequential
				// read exhausted it, or a Seek arrived right at the boundary).
				if err := o.advanceToNextChunk(); err != nil {
					if total > 0 {
						return total, nil
					}
					return 0, err
				}
				continue
			}
			o.buf = o.chunks[o.chunkIdx].data
			if skip := int(o.offset - chunkStart); skip > 0 {
				o.buf = o.buf[skip:]
			}
		}
		n := copy(p, o.buf)
		o.buf = o.buf[n:]
		p = p[n:]
		total += n
		o.offset += int64(n)
	}
	return total, nil
}

// Seek sets the read position. It preserves in-flight prefetch work
// when possible (short seeks forward reuse already-requested chunks).
func (o *Object) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = o.offset + offset
	case io.SeekEnd:
		newOffset = o.size + offset
	default:
		return 0, fmt.Errorf("desync: invalid seek whence %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("desync: negative seek position")
	}
	o.offset = newOffset
	o.buf = nil
	// Don't drain — prior work may still be useful after a short seek.
	// startPrefetch will skip already-requested/valid chunks.
	if o.chunks != nil {
		o.startPrefetch(o.chunkForOffset(newOffset))
	}
	return o.offset, nil
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

// startPrefetch sets the read position to fromChunk and fills the
// prefetch window. Prior in-flight work is preserved.
func (o *Object) startPrefetch(fromChunk int) {
	pool := GetWorkerPool()

	// Allocate state on first call.
	if o.chunks == nil {
		o.chunks = make([]objectChunkSlot, len(o.idx.Chunks))
		o.resultCh = make(chan FetchResult, o.n)
	}

	o.chunkIdx = fromChunk
	o.nextRequestIdx = fromChunk
	o.buf = nil
	o.submitMore(pool)
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

// advanceToNextChunk releases the current chunk, moves to the next,
// and waits for it to arrive.
func (o *Object) advanceToNextChunk() error {
	pool := GetWorkerPool()

	// Release and clear the current chunk slot before moving forward.
	if o.chunks[o.chunkIdx].chunk != nil {
		o.chunks[o.chunkIdx].chunk.Release()
	}
	o.chunks[o.chunkIdx] = objectChunkSlot{}
	if o.Progress != nil {
		o.Progress(o.offset)
	}
	o.chunkIdx++

	if o.chunkIdx >= len(o.idx.Chunks) {
		return io.EOF
	}

	// Wait until the new current chunk arrives — park out-of-order results.
	for !o.chunks[o.chunkIdx].valid {
		o.receiveOne(pool)
	}

	slot := &o.chunks[o.chunkIdx]
	if slot.err != nil {
		return fmt.Errorf("desync: chunk fetch error: %w", slot.err)
	}
	o.buf = slot.data
	return nil
}

// chunkForOffset returns the index of the chunk containing the given byte offset.
func (o *Object) chunkForOffset(offset int64) int {
	for i, c := range o.idx.Chunks {
		if int64(c.Start+c.Size) > offset {
			return i
		}
	}
	return len(o.idx.Chunks)
}
