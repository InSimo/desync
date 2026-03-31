// Package pktline wraps github.com/git-lfs/pktline with performance
// overrides and helpers for the Git LFS SSH transfer protocol.
//
// See https://git-scm.com/docs/protocol-common#_pkt_line_format
package pktline

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	lfs_pktline "github.com/git-lfs/pktline"
)

// MaxPayload is the maximum data bytes in a single pkt-line packet.
const MaxPayload = lfs_pktline.MaxPacketLength

// bufSize matches the max pkt-line packet size to minimize syscalls.
const bufSize = 64 * 1024

// Pktline wraps the upstream git-lfs/pktline implementation with:
//   - Optimized ReadPacketWithLength (avoids per-call allocations)
//   - WriteDelim that does NOT auto-flush (avoids mid-message syscalls)
//   - WriteStatus/WriteErrorStatus helpers for the LFS SSH transfer protocol
//
// Go embedding is not inheritance: methods on the embedded Pktline call
// their own ReadPacketWithLength, not ours.  So we must also override
// ReadPacket, ReadPacketText, and ReadPacketTextWithLength to ensure
// they use our optimized read path.
type Pktline struct {
	*lfs_pktline.Pktline
	br *bufio.Reader
	bw *bufio.Writer
}

// New creates a Pktline that reads from r and writes to w.
func New(r io.Reader, w io.Writer) *Pktline {
	br := bufio.NewReaderSize(r, bufSize)
	bw := bufio.NewWriterSize(w, bufSize)
	return &Pktline{
		Pktline: lfs_pktline.NewPktline(br, bw),
		br:      br,
		bw:      bw,
	}
}

// --- Read overrides (use io.ReadFull instead of ioutil.ReadAll+LimitReader) ---

// ReadPacketWithLength reads a single pkt-line packet.
// Returns (data, length, err) where length 0=flush, 1=delim.
func (p *Pktline) ReadPacketWithLength() ([]byte, int, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(p.br, hdr[:]); err != nil {
		return nil, 0, err
	}
	switch string(hdr[:]) {
	case "0000":
		return nil, 0, nil
	case "0001":
		return nil, 1, nil
	}
	pktLen, err := strconv.ParseInt(string(hdr[:]), 16, 0)
	if err != nil {
		return nil, 0, err
	}
	length := int(pktLen)
	if length < 4 {
		return nil, length, fmt.Errorf("invalid pkt-line length %d", length)
	}
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(p.br, payload); err != nil {
		return nil, length, fmt.Errorf("reading pkt-line payload: %w", err)
	}
	return payload, length, nil
}

// ReadPacket reads raw bytes (no LF stripping). Delegates to ReadPacketWithLength.
func (p *Pktline) ReadPacket() ([]byte, error) {
	data, _, err := p.ReadPacketWithLength()
	return data, err
}

// ReadPacketText reads a text packet, stripping trailing LF.
func (p *Pktline) ReadPacketText() (string, error) {
	data, _, err := p.ReadPacketWithLength()
	return strings.TrimSuffix(string(data), "\n"), err
}

// ReadPacketTextWithLength reads a text packet with length for flush/delim detection.
func (p *Pktline) ReadPacketTextWithLength() (string, int, error) {
	data, length, err := p.ReadPacketWithLength()
	return strings.TrimSuffix(string(data), "\n"), length, err
}

// --- Write override ---
//
// WriteFlush is inherited from the upstream (writes "0000" + flushes bufio).
//
// WriteDelim is overridden to NOT flush.  Delim appears mid-message
// (between header and body), so data stays buffered until WriteFlush.
func (p *Pktline) WriteDelim() error {
	_, err := p.bw.WriteString("0001")
	return err
}



