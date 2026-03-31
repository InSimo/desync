package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"
)

// handleBatch implements the "batch" command. It reads OID+size lines from the
// client and responds with actions for each object.
//
// For download: objects with an existing index get action "download"; missing
// objects get "noop".
//
// For upload: objects without an existing index get action "upload"; already-
// indexed objects get "noop".
func (s *Server) handleBatch(ctx context.Context, _ string) error {
	// Read arguments until delim-pkt, then OID lines until flush-pkt.
	args, hasDelim, err := s.readArgs()
	if err != nil {
		return err
	}
	_ = args // hash-algo, transfer, refname — noted but not used yet

	type oidEntry struct {
		oid  string
		size int64
	}
	var entries []oidEntry

	var invalidOID string
	var malformedEntry string // first malformed size or short line
	if hasDelim {
		for {
			data, length, err := s.pl.ReadPacketWithLength()
			if err != nil {
				return fmt.Errorf("reading batch OID lines: %w", err)
			}
			if length == 0 { // flush-pkt
				break
			}
			line := strings.TrimSuffix(string(data), "\n")
			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue // skip blank lines silently
			}
			if len(parts) < 2 {
				if malformedEntry == "" {
					malformedEntry = line
				}
				continue
			}
			if !validOID(parts[0]) && invalidOID == "" {
				invalidOID = parts[0]
			}
			size, parseErr := strconv.ParseInt(parts[1], 10, 64)
			if parseErr != nil || size < 0 {
				if malformedEntry == "" {
					malformedEntry = parts[1]
				}
				continue
			}
			entries = append(entries, oidEntry{oid: parts[0], size: size})
		}
	}

	if invalidOID != "" {
		return writeErrorStatus(s.pl, 400, fmt.Sprintf("invalid OID %q", invalidOID))
	}
	if malformedEntry != "" {
		return writeErrorStatus(s.pl, 400, fmt.Sprintf("malformed batch entry %q", malformedEntry))
	}

	// Write response header.
	if err := writeStatus(s.pl, 200); err != nil {
		return err
	}
	if err := s.pl.WritePacketText("hash-algo=sha256"); err != nil {
		return err
	}
	if err := s.pl.WriteDelim(); err != nil {
		return err
	}

	// Check each OID in parallel (up to s.n concurrent workers) and collect
	// actions in entry order.  Results are stored by index — no mutex needed
	// since each goroutine writes to a unique position.
	actions := make([]string, len(entries))
	if len(entries) > 0 {
		g, gCtx := errgroup.WithContext(ctx)
		sem := make(chan struct{}, s.n)
		for i, e := range entries {
			i, e := i, e
			g.Go(func() error {
				select {
				case sem <- struct{}{}:
				case <-gCtx.Done():
					return gCtx.Err()
				}
				defer func() { <-sem }()
				actions[i] = s.batchAction(e.oid)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}

	// Write responses in original entry order.
	for i, e := range entries {
		line := fmt.Sprintf("%s %d %s", e.oid, e.size, actions[i])
		if err := s.pl.WritePacketText(line); err != nil {
			return err
		}
	}

	return s.pl.WriteFlush()
}

// batchAction determines the action for a single OID based on the operation
// and whether the object's index already exists.
func (s *Server) batchAction(oid string) string {
	indexName := oidIndexName(oid)
	exists, _ := s.indexStore.HasIndex(indexName)

	switch s.operation {
	case "download":
		if exists {
			return "download"
		}
		return "noop"
	case "upload":
		if exists {
			return "noop"
		}
		return "upload"
	default:
		return "noop"
	}
}
