package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/folbricht/desync/cmd/internal/pktline"
)

// handleBatch implements the "batch" command. It reads OID+size lines from the
// client and responds with actions for each object.
//
// For download: objects with an existing index get action "download"; missing
// objects get "noop".
//
// For upload: objects without an existing index get action "upload"; already-
// indexed objects get "noop".
func (s *Server) handleBatch(_ context.Context, _ string) error {
	// Read arguments until delim-pkt, then OID lines until flush-pkt.
	args, hasDelim, err := s.readArgs()
	if err != nil {
		return err
	}
	_ = args // hash-algo, transfer, refname — noted but not used yet

	type oidEntry struct {
		oid  string
		size string
	}
	var entries []oidEntry

	var invalidOID string
	if hasDelim {
		for {
			data, err := s.r.ReadPacket()
			if errors.Is(err, pktline.ErrFlush) {
				break
			}
			if err != nil {
				return fmt.Errorf("reading batch OID lines: %w", err)
			}
			line := string(data)
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			if !validOID(parts[0]) && invalidOID == "" {
				invalidOID = parts[0]
			}
			entries = append(entries, oidEntry{oid: parts[0], size: parts[1]})
		}
	}

	if invalidOID != "" {
		return s.w.WriteErrorStatus(400, fmt.Sprintf("invalid OID %q", invalidOID))
	}

	// Write response header.
	if err := s.w.WriteStatus(200); err != nil {
		return err
	}
	if err := s.w.WritePacketText("hash-algo=sha256"); err != nil {
		return err
	}
	if err := s.w.WriteDelim(); err != nil {
		return err
	}

	// Check each OID and respond with the appropriate action.
	for _, e := range entries {
		action := s.batchAction(e.oid)
		line := fmt.Sprintf("%s %s %s", e.oid, e.size, action)
		if err := s.w.WritePacketText(line); err != nil {
			return err
		}
	}

	return s.w.WriteFlush()
}

// batchAction determines the action for a single OID based on the operation
// and whether the object's index already exists.
func (s *Server) batchAction(oid string) string {
	indexName := oidIndexName(oid)
	_, err := s.indexStore.GetIndex(indexName)
	exists := err == nil

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
