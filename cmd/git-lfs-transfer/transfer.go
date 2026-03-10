package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/pktline"
)

// handleGetObject retrieves an LFS object by OID: looks up its desync index,
// assembles the file from chunks, and streams the result back to the client.
func (s *Server) handleGetObject(ctx context.Context, oid string) error {
	args, _, err := s.readArgs()
	if err != nil {
		return err
	}
	_ = args // id, token — not used

	if !validOID(oid) {
		return s.w.WriteErrorStatus(404, fmt.Sprintf("invalid OID %q", oid))
	}

	indexName := oidIndexName(oid)
	idx, err := s.indexStore.GetIndex(indexName)
	if err != nil {
		return s.w.WriteErrorStatus(404, fmt.Sprintf("object %s not found", oid))
	}

	// Assemble the file to a temporary location.
	tmpFile, err := os.CreateTemp(s.tmpDir, "git-lfs-transfer-get-*")
	if err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("creating temp file: %v", err))
	}
	tmpName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpName)

	opts := desync.AssembleOptions{N: s.n}
	if _, err := desync.AssembleFile(ctx, tmpName, idx, s.readStore, nil, opts); err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("assembling object %s: %v", oid, err))
	}

	// Open and stat the assembled file.
	f, err := os.Open(tmpName)
	if err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("opening assembled file: %v", err))
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("stat assembled file: %v", err))
	}
	size := fi.Size()

	// Send success response with size, then stream the data.
	if err := s.w.WriteStatus(200); err != nil {
		return err
	}
	if err := s.w.WritePacketText(fmt.Sprintf("size=%d", size)); err != nil {
		return err
	}
	if err := s.w.WriteDelim(); err != nil {
		return err
	}

	// Stream file content as pkt-line binary packets.
	buf := make([]byte, pktline.MaxPayload)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := s.w.WriteBinaryPacket(buf[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	return s.w.WriteFlush()
}

// handlePutObject receives an LFS object from the client, chunks it using
// desync, stores the chunks, and stores the index keyed by OID.
func (s *Server) handlePutObject(ctx context.Context, oid string) error {
	args, hasDelim, err := s.readArgs()
	if err != nil {
		return err
	}

	size, err := parseSize(args)
	if err != nil {
		return s.w.WriteErrorStatus(400, err.Error())
	}

	if !validOID(oid) {
		// Drain any data.
		if hasDelim {
			s.drainBinaryData()
		}
		return s.w.WriteErrorStatus(400, fmt.Sprintf("invalid OID %q", oid))
	}

	// Receive the binary data into a temp file.
	tmpFile, err := os.CreateTemp(s.tmpDir, "git-lfs-transfer-put-*")
	if err != nil {
		if hasDelim {
			s.drainBinaryData()
		}
		return s.w.WriteErrorStatus(500, fmt.Sprintf("creating temp file: %v", err))
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	hasher := sha256.New()
	var received int64

	if hasDelim {
		for {
			data, readErr := s.r.ReadRawPacket()
			if errors.Is(readErr, pktline.ErrFlush) {
				break
			}
			if readErr != nil {
				tmpFile.Close()
				return fmt.Errorf("reading put-object data: %w", readErr)
			}
			n, writeErr := tmpFile.Write(data)
			if writeErr != nil {
				tmpFile.Close()
				return fmt.Errorf("writing temp file: %w", writeErr)
			}
			hasher.Write(data[:n])
			received += int64(n)
		}
	}
	tmpFile.Close()

	// Verify size.
	if received != size {
		return s.w.WriteErrorStatus(400, fmt.Sprintf("size mismatch: expected %d, received %d", size, received))
	}

	// Verify OID (SHA-256 of content).
	actualOID := hex.EncodeToString(hasher.Sum(nil))
	if actualOID != oid {
		return s.w.WriteErrorStatus(400, fmt.Sprintf("OID mismatch: expected %s, got %s", oid, actualOID))
	}

	// Chunk the file and build the index.
	idx, _, err := desync.IndexFromFile(ctx, tmpName, s.n, s.minChunk, s.avgChunk, s.maxChunk, desync.NullProgressBar{})
	if err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("chunking object %s: %v", oid, err))
	}

	// Store the chunks.
	var sps desync.SafePruneStore
	if s.safePruning {
		if sp, ok := s.writeStore.(desync.SafePruneStore); ok {
			sps = sp
		}
	}
	if err := desync.ChopFile(ctx, tmpName, idx.Chunks, s.writeStore, s.n, desync.NullProgressBar{}, sps, s.safePropTime); err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("storing chunks for %s: %v", oid, err))
	}

	// Store the index.
	indexName := oidIndexName(oid)
	if err := s.indexStore.StoreIndex(indexName, idx); err != nil {
		return s.w.WriteErrorStatus(500, fmt.Sprintf("storing index for %s: %v", oid, err))
	}

	// Success.
	if err := s.w.WriteStatus(200); err != nil {
		return err
	}
	return s.w.WriteFlush()
}

// handleVerifyObject confirms an object exists and its size matches.
func (s *Server) handleVerifyObject(_ context.Context, oid string) error {
	args, _, err := s.readArgs()
	if err != nil {
		return err
	}

	size, err := parseSize(args)
	if err != nil {
		return s.w.WriteErrorStatus(400, err.Error())
	}

	if !validOID(oid) {
		return s.w.WriteErrorStatus(404, fmt.Sprintf("invalid OID %q", oid))
	}

	indexName := oidIndexName(oid)
	idx, err := s.indexStore.GetIndex(indexName)
	if err != nil {
		return s.w.WriteErrorStatus(404, fmt.Sprintf("object %s not found", oid))
	}

	actualSize := indexTotalSize(idx)
	if actualSize != size {
		return s.w.WriteErrorStatus(409, fmt.Sprintf("size mismatch for %s: expected %d, got %d",
			oid, size, actualSize))
	}

	if err := s.w.WriteStatus(200); err != nil {
		return err
	}
	return s.w.WriteFlush()
}

// drainBinaryData reads and discards binary pkt-line packets until flush.
func (s *Server) drainBinaryData() {
	for {
		_, err := s.r.ReadPacket()
		if err != nil {
			return
		}
	}
}
