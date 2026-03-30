package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/folbricht/desync"
	"github.com/folbricht/desync/cmd/shared/bytelimit"
	"github.com/folbricht/desync/cmd/shared/cmdshared"
	"github.com/folbricht/desync/cmd/shared/pktline"
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
	gate           *bytelimit.Gate // cross-process in-flight byte limit; may be nil

	pl *pktline.Pktline

	// logDir is the directory for per-session error log files.  Empty
	// disables logging (e.g. in tests).  The log file and directory are
	// created lazily on the first error so that error-free sessions leave
	// no files behind.
	logDir          string
	logFile         *os.File // opened lazily by logf
	logPath         string   // set when logFile is opened
	hasLoggedErrors bool
}

// logf writes an internal error message to a per-session log file, creating
// the file (and its directory) on first use.  It is a no-op when logDir is
// empty, and silently swallows OS errors to avoid masking the original error.
func (s *Server) logf(format string, args ...any) {
	if s.logFile == nil {
		if s.logDir == "" {
			return
		}
		if err := os.MkdirAll(s.logDir, 0755); err != nil {
			return
		}
		logName := time.Now().UTC().Format("20060102T150405.000000000") + ".log"
		s.logPath = filepath.Join(s.logDir, logName)
		f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err != nil {
			return
		}
		s.logFile = f
	}
	fmt.Fprintf(s.logFile, format+"\n", args...)
	s.hasLoggedErrors = true
}

func (s *Server) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	s.pl = pktline.New(stdin, stdout)

	// Optional execution tracing: write to LFS_TRANSFER_TRACE_DIR/<pid>.trace.
	if traceDir := os.Getenv("LFS_TRANSFER_TRACE_DIR"); traceDir != "" {
		os.MkdirAll(traceDir, 0o755)
		path := fmt.Sprintf("%s/%d.trace", traceDir, os.Getpid())
		if f, err := os.Create(path); err == nil {
			trace.Start(f)
			defer func() {
				trace.Stop()
				f.Close()
			}()
		}
	}

	if err := s.advertiseCapabilities(); err != nil {
		return fmt.Errorf("capability advertisement: %w", err)
	}
	if err := s.negotiateVersion(); err != nil {
		return fmt.Errorf("version negotiation: %w", err)
	}
	return s.commandLoop(ctx)
}

func (s *Server) advertiseCapabilities() error {
	if err := s.pl.WritePacketText("version=1"); err != nil {
		return err
	}
	return s.pl.WriteFlush()
}

func (s *Server) negotiateVersion() error {
	line, err := s.pl.ReadPacketText()
	if err != nil {
		return fmt.Errorf("reading version request: %w", err)
	}

	// Accept both "version <N>" (space) and "version=<N>" (equals).
	var version string
	if strings.HasPrefix(line, "version ") {
		version = strings.TrimPrefix(line, "version ")
	} else if strings.HasPrefix(line, "version=") {
		version = strings.TrimPrefix(line, "version=")
	} else {
		if err := s.pl.WriteErrorStatus(400, "expected version request"); err != nil {
			return err
		}
		return fmt.Errorf("unexpected line: %q", line)
	}
	// Consume flush-pkt after version line.
	if _, length, err := s.pl.ReadPacketWithLength(); err != nil {
		return fmt.Errorf("expected flush after version request: %v", err)
	} else if length != 0 {
		return fmt.Errorf("expected flush after version request, got length %d", length)
	}

	if version != "1" {
		return s.pl.WriteErrorStatus(400, fmt.Sprintf("unsupported version %q", version))
	}

	// Accepted.
	if err := s.pl.WriteStatus(200); err != nil {
		return err
	}
	return s.pl.WriteFlush()
}

func (s *Server) commandLoop(ctx context.Context) error {
	for {
		line, length, err := s.pl.ReadPacketTextWithLength()
		if err != nil {
			return fmt.Errorf("reading command: %w", err)
		}
		if length <= 1 { // flush-pkt (0) or delim-pkt (1)
			continue
		}

		cmd, arg, _ := strings.Cut(line, " ")
		switch cmd {
		case "quit":
			// Consume flush-pkt.
			s.pl.ReadPacket()
			s.pl.WriteStatus(200)
			s.pl.WriteFlush()
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
			if err := s.pl.WriteErrorStatus(400, fmt.Sprintf("unknown command %q", cmd)); err != nil {
				return err
			}
		}
	}
}

// drainUntilFlush reads and discards packets until a flush-pkt.
func (s *Server) drainUntilFlush() {
	for {
		_, length, err := s.pl.ReadPacketWithLength()
		if err != nil || length == 0 { // stop on error or flush-pkt
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
		data, length, err := s.pl.ReadPacketWithLength()
		if err != nil {
			return nil, false, err
		}
		if length == 0 { // flush-pkt
			return args, false, nil
		}
		if length == 1 { // delim-pkt
			return args, true, nil
		}
		line := strings.TrimSuffix(string(data), "\n")
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
