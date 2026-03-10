package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/internal/cmdshared"
	"github.com/folbricht/desync/cmd/internal/pktline"
)

// Server implements the Git LFS SSH transfer protocol (git-lfs-transfer).
type Server struct {
	operation string // "upload" or "download"

	writeStore     desync.WriteStore
	readStore      desync.Store
	indexStore     desync.IndexWriteStore
	n              int
	minChunk       uint64
	avgChunk       uint64
	maxChunk       uint64
	safePruning    bool
	safePropTime   time.Duration

	r *pktline.Reader
	w *pktline.Writer

	tmpDir string
}

func (s *Server) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	s.r = pktline.NewReader(stdin)
	s.w = pktline.NewWriter(stdout)

	if err := s.advertiseCapabilities(); err != nil {
		return fmt.Errorf("capability advertisement: %w", err)
	}
	if err := s.negotiateVersion(); err != nil {
		return fmt.Errorf("version negotiation: %w", err)
	}
	return s.commandLoop(ctx)
}

func (s *Server) advertiseCapabilities() error {
	if err := s.w.WritePacketText("version=1"); err != nil {
		return err
	}
	return s.w.WriteFlush()
}

func (s *Server) negotiateVersion() error {
	line, err := s.r.ReadPacketText()
	if err != nil {
		return fmt.Errorf("reading version request: %w", err)
	}

	// Expect "version <N>"
	if !strings.HasPrefix(line, "version ") {
		if err := s.w.WriteErrorStatus(400, "expected version request"); err != nil {
			return err
		}
		return fmt.Errorf("unexpected line: %q", line)
	}

	version := strings.TrimPrefix(line, "version ")
	// Consume flush-pkt after version line.
	if _, err := s.r.ReadPacket(); !errors.Is(err, pktline.ErrFlush) {
		return fmt.Errorf("expected flush after version request, got: %v", err)
	}

	if version != "1" {
		return s.w.WriteErrorStatus(400, fmt.Sprintf("unsupported version %q", version))
	}

	// Accepted.
	if err := s.w.WriteStatus(200); err != nil {
		return err
	}
	return s.w.WriteFlush()
}

func (s *Server) commandLoop(ctx context.Context) error {
	for {
		line, err := s.r.ReadPacketText()
		if err != nil {
			if errors.Is(err, pktline.ErrFlush) {
				continue
			}
			return fmt.Errorf("reading command: %w", err)
		}

		cmd, arg, _ := strings.Cut(line, " ")
		switch cmd {
		case "quit":
			// Consume flush-pkt.
			s.r.ReadPacket()
			s.w.WriteStatus(200)
			s.w.WriteFlush()
			return nil

		case "batch":
			if err := s.handleBatch(ctx, arg); err != nil {
				return fmt.Errorf("batch: %w", err)
			}

		case "get-object":
			if err := s.handleGetObject(ctx, arg); err != nil {
				return fmt.Errorf("get-object: %w", err)
			}

		case "put-object":
			if err := s.handlePutObject(ctx, arg); err != nil {
				return fmt.Errorf("put-object: %w", err)
			}

		case "verify-object":
			if err := s.handleVerifyObject(ctx, arg); err != nil {
				return fmt.Errorf("verify-object: %w", err)
			}

		default:
			// Unknown command: read until flush, send error.
			s.drainUntilFlush()
			if err := s.w.WriteErrorStatus(400, fmt.Sprintf("unknown command %q", cmd)); err != nil {
				return err
			}
		}
	}
}

// drainUntilFlush reads and discards packets until a flush-pkt.
func (s *Server) drainUntilFlush() {
	for {
		_, err := s.r.ReadPacket()
		if err != nil {
			return
		}
	}
}

// readArgs reads argument lines until a flush-pkt or delim-pkt.
// Returns the arguments as a map and whether a delim-pkt was encountered
// (meaning binary data follows).
func (s *Server) readArgs() (map[string]string, bool, error) {
	args := make(map[string]string)
	for {
		data, err := s.r.ReadPacket()
		if errors.Is(err, pktline.ErrFlush) {
			return args, false, nil
		}
		if errors.Is(err, pktline.ErrDelim) {
			return args, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		line := string(data)
		if k, v, ok := strings.Cut(line, "="); ok {
			args[k] = v
		}
	}
}

// maxObjectSize is the maximum accepted LFS object size (100 GiB).  Uploads
// that declare a larger size are rejected before any data is received.
const maxObjectSize int64 = 100 * 1024 * 1024 * 1024

// parseSize extracts and validates the "size" argument.  It returns an error
// for missing, non-numeric, or negative values; callers enforce upper bounds.
func parseSize(args map[string]string) (int64, error) {
	sizeStr, ok := args["size"]
	if !ok {
		return 0, fmt.Errorf("missing required 'size' argument")
	}
	n, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", sizeStr, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("size must not be negative")
	}
	return n, nil
}

// oidIndexName returns the sharded index name for a Git LFS OID.
var oidIndexName = cmdshared.OidIndexName

// validOID reports whether oid is a valid Git LFS SHA-256 object identifier:
// exactly 64 lowercase hexadecimal characters.  This is checked before using
// the OID as a filesystem path component to prevent path traversal attacks.
func validOID(oid string) bool {
	if len(oid) != 64 {
		return false
	}
	for _, c := range oid {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// indexTotalSize returns the total file size represented by an index.
func indexTotalSize(idx desync.Index) int64 {
	if len(idx.Chunks) == 0 {
		return 0
	}
	last := idx.Chunks[len(idx.Chunks)-1]
	return int64(last.Start + last.Size)
}
