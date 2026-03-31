package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
	"github.com/git-lfs/pktline"
)

// handleGetObject retrieves an LFS object by OID: looks up its desync index,
// and streams the reassembled content directly from the chunk store without
// buffering to a temporary file.  The exact object size is computed from the
// index before any chunks are fetched, so the size header is sent first.
func (s *Server) handleGetObject(ctx context.Context, oid string) error {
	args, _, err := s.readArgs()
	if err != nil {
		return err
	}
	_ = args // id, token — not used

	if !validOID(oid) {
		return writeErrorStatus(s.pl, 404, fmt.Sprintf("invalid OID %q", oid))
	}

	indexName := oidIndexName(oid)
	idx, err := s.indexStore.GetIndex(indexName)
	if err != nil {
		return writeErrorStatus(s.pl, 404, fmt.Sprintf("object %s not found", oid))
	}

	size := indexTotalSize(idx)

	// Acquire in-flight byte slot (blocks if over the cross-process limit).
	if s.gate != nil {
		slot, err := s.gate.Acquire(ctx, int64(size))
		if err != nil {
			return writeErrorStatus(s.pl, 503, fmt.Sprintf("in-flight limit: %v", err))
		}
		defer slot.Release()
	}

	// Send success response with size before fetching any chunk data.
	if err := writeStatus(s.pl, 200); err != nil {
		return err
	}
	if err := s.pl.WritePacketText(fmt.Sprintf("size=%d", size)); err != nil {
		return err
	}
	if err := s.pl.WriteDelim(); err != nil {
		return err
	}

	// Prefetch and decompress chunks in parallel (up to s.n concurrent
	// workers), then stream them to the client in index order.  Each chunk
	// gets its own result channel so the consumer reads results in the
	// original order while workers run ahead.  Memory is bounded to ~s.n
	// decompressed chunks in flight.
	//
	// Any error after the 200 response has been sent cannot be reported as a
	// protocol error; we close the connection by returning it to the caller.
	type prefetched struct {
		chunk *desync.Chunk
		data  []byte
		err   error
	}
	pending := make([]chan prefetched, len(idx.Chunks))
	sem := make(chan struct{}, s.n)
	for i, c := range idx.Chunks {
		ch := make(chan prefetched, 1)
		pending[i] = ch
		id := c.ID
		sem <- struct{}{} // acquire slot (blocks if s.n workers busy)
		go func() {
			defer func() { <-sem }()
			chunk, err := s.readStore.GetChunk(id)
			if err != nil {
				ch <- prefetched{err: err}
				return
			}
			data, err := chunk.Data()
			ch <- prefetched{chunk: chunk, data: data, err: err}
		}()
	}
	for i, ch := range pending {
		pc := <-ch
		if pc.err != nil {
			s.logf("get-object %s: chunk %d: %v", oid, i, pc.err)
			return fmt.Errorf("get-object %s: chunk error: %w", oid, pc.err)
		}
		data := pc.data
		for len(data) > 0 {
			n := pktline.MaxPacketLength
			if n > len(data) {
				n = len(data)
			}
			if err := s.pl.WritePacket(data[:n]); err != nil {
				pc.chunk.Release()
				return err
			}
			data = data[n:]
		}
		pc.chunk.Release()
	}

	cmdshared.WriteHeapProfile("get_after_stream")
	return s.pl.WriteFlush()
}

// handlePutObject receives an LFS object from the client, chunks it using
// desync, stores the chunks, and stores the index keyed by OID.
//
// Data is streamed through an io.Pipe into ChunkStream without buffering to a
// temporary file.  The SHA-256 of the received bytes is computed in parallel
// via io.MultiWriter and verified against the OID before the index is stored.
func (s *Server) handlePutObject(ctx context.Context, oid string) error {
	args, hasDelim, err := s.readArgs()
	if err != nil {
		return err
	}

	size, err := parseSize(args)
	if err != nil {
		return writeErrorStatus(s.pl, 400, err.Error())
	}

	if !validOID(oid) {
		if hasDelim {
			s.drainBinaryData()
		}
		return writeErrorStatus(s.pl, 400, fmt.Sprintf("invalid OID %q", oid))
	}

	if size > maxObjectSize {
		if hasDelim {
			s.drainBinaryData()
		}
		return writeErrorStatus(s.pl, 400, fmt.Sprintf("object too large: %d bytes exceeds limit of %d", size, maxObjectSize))
	}

	// Acquire in-flight byte slot (blocks if over the cross-process limit).
	if s.gate != nil {
		slot, err := s.gate.Acquire(ctx, size)
		if err != nil {
			if hasDelim {
				s.drainBinaryData()
			}
			return writeErrorStatus(s.pl, 503, fmt.Sprintf("in-flight limit: %v", err))
		}
		defer slot.Release()
	}

	// The goroutine reads pkt-line binary packets from stdin and writes them
	// to both the pipe (for chunking) and the hasher (for OID verification).
	// If the pipe write fails (ChunkStream stopped reading), it drains stdin
	// so the command loop can continue cleanly.
	hasher := sha256.New()
	var received int64

	pr, pw := io.Pipe()
	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer pw.Close()
		if !hasDelim {
			return nil
		}
		for {
			data, length, readErr := s.pl.ReadPacketWithLength()
			if readErr != nil {
				return readErr
			}
			if length == 0 { // flush-pkt
				return nil
			}
			if _, writeErr := pw.Write(data); writeErr != nil {
				// ChunkStream stopped reading; drain stdin before returning.
				s.drainBinaryData()
				return nil
			}
			hasher.Write(data)
			received += int64(len(data))
		}
	})

	var sps desync.SafePruneStore
	if s.safePruning {
		if sp, ok := s.writeStore.(desync.SafePruneStore); ok {
			sps = sp
		}
	}

	chunker, chunkerErr := desync.NewChunker(pr, s.minChunk, s.avgChunk, s.maxChunk)
	var idx desync.Index
	var chunkStreamErr error
	if chunkerErr == nil {
		idx, chunkStreamErr = desync.ChunkStream(ctx, &chunker, s.writeStore, s.n, sps, s.safePropTime)
	}
	chunker.Release()

	// Close the read end of the pipe so the goroutine unblocks if it is
	// waiting on a pw.Write() that ChunkStream is no longer consuming.
	pr.CloseWithError(io.ErrClosedPipe)
	if goroutineErr := g.Wait(); goroutineErr != nil {
		return fmt.Errorf("reading put-object data: %w", goroutineErr)
	}

	if chunkerErr != nil {
		s.logf("put-object %s: creating chunker: %v", oid, chunkerErr)
		return writeErrorStatus(s.pl, 500, "internal error")
	}
	cmdshared.WriteHeapProfile("put_after_chunkstream")
	if chunkStreamErr != nil {
		s.logf("put-object %s: storing chunks: %v", oid, chunkStreamErr)
		return writeErrorStatus(s.pl, 500, "internal error")
	}

	// Verify declared size and content hash before committing the index.
	// Any chunks already stored without a matching index are orphaned and
	// will be reclaimed by the safe-pruning protocol.
	if received != size {
		return writeErrorStatus(s.pl, 400, fmt.Sprintf("size mismatch: expected %d, received %d", size, received))
	}
	if actualOID := hex.EncodeToString(hasher.Sum(nil)); actualOID != oid {
		return writeErrorStatus(s.pl, 400, fmt.Sprintf("OID mismatch: expected %s, got %s", oid, actualOID))
	}

	indexName := oidIndexName(oid)
	if err := s.indexStore.StoreIndex(indexName, idx); err != nil {
		s.logf("put-object %s: storing index: %v", oid, err)
		return writeErrorStatus(s.pl, 500, "internal error")
	}

	if err := writeStatus(s.pl, 200); err != nil {
		return err
	}
	return s.pl.WriteFlush()
}

// handleVerifyObject confirms an object exists, its size matches, and all
// chunks referenced by its index are present in the store.  The chunk check
// uses HasChunk (a cheap HEAD/stat operation) and runs up to s.n checks in
// parallel to keep latency low even for objects with many chunks.
func (s *Server) handleVerifyObject(ctx context.Context, oid string) error {
	args, _, err := s.readArgs()
	if err != nil {
		return err
	}

	size, err := parseSize(args)
	if err != nil {
		return writeErrorStatus(s.pl, 400, err.Error())
	}

	if !validOID(oid) {
		return writeErrorStatus(s.pl, 404, fmt.Sprintf("invalid OID %q", oid))
	}

	indexName := oidIndexName(oid)
	idx, err := s.indexStore.GetIndex(indexName)
	if err != nil {
		return writeErrorStatus(s.pl, 404, fmt.Sprintf("object %s not found", oid))
	}

	actualSize := indexTotalSize(idx)
	if actualSize != size {
		return writeErrorStatus(s.pl, 409, fmt.Sprintf("size mismatch for %s: expected %d, got %d",
			oid, size, actualSize))
	}

	// Verify that every chunk referenced by the index exists in the store.
	// Deduplicate first — the same chunk ID can appear many times in an index.
	seen := make(map[desync.ChunkID]struct{}, len(idx.Chunks))
	var unique []desync.ChunkID
	for _, c := range idx.Chunks {
		if _, ok := seen[c.ID]; !ok {
			seen[c.ID] = struct{}{}
			unique = append(unique, c.ID)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, s.n)
	for _, id := range unique {
		id := id
		g.Go(func() error {
			select {
			case sem <- struct{}{}:
			case <-gCtx.Done():
				return gCtx.Err()
			}
			defer func() { <-sem }()
			has, err := s.readStore.HasChunk(id)
			if err != nil {
				return fmt.Errorf("checking chunk %s: %w", id, err)
			}
			if !has {
				return fmt.Errorf("missing chunk %s", id)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		s.logf("verify-object %s: %v", oid, err)
		return writeErrorStatus(s.pl, 404, fmt.Sprintf("object %s incomplete: %v", oid, err))
	}

	if err := writeStatus(s.pl, 200); err != nil {
		return err
	}
	return s.pl.WriteFlush()
}

// drainBinaryData reads and discards binary pkt-line packets until flush.
func (s *Server) drainBinaryData() {
	for {
		_, length, err := s.pl.ReadPacketWithLength()
		if err != nil || length == 0 { // stop on error or flush-pkt
			return
		}
	}
}
