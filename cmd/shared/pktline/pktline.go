// Package pktline implements Git's pkt-line framing protocol.
//
// Each packet is prefixed by a 4-byte hex length that includes the prefix
// itself. Two special packets are defined: flush-pkt ("0000") signals the end
// of a message, and delim-pkt ("0001") separates header from data within a
// message.
//
// See https://git-scm.com/docs/protocol-common#_pkt_line_format
package pktline

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// MaxPayload is the maximum data bytes in a single pkt-line (65516).
// The total packet size is 65520 (4-byte header + payload).
const MaxPayload = 65516

// Sentinel errors returned by Reader.
var (
	ErrFlush   = errors.New("flush-pkt")
	ErrDelim   = errors.New("delim-pkt")
	ErrInvalid = errors.New("invalid pkt-line header")
)

// Reader reads pkt-line formatted data from an underlying io.Reader.
type Reader struct {
	r   *bufio.Reader
	buf []byte
}

// NewReader returns a Reader that reads pkt-line packets from r.
// The buffer is sized to hold a full pkt-line packet (64KB) to
// minimize read syscalls.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:   bufio.NewReaderSize(r, 64*1024),
		buf: make([]byte, 4),
	}
}

// ReadPacket reads a single pkt-line packet. It returns the payload bytes
// (without the 4-byte length prefix or trailing LF).
// For binary data that may end in \n, use ReadRawPacket instead.
//
// Special packets return a nil slice and a sentinel error:
//   - flush-pkt ("0000"): returns (nil, ErrFlush)
//   - delim-pkt ("0001"): returns (nil, ErrDelim)
func (r *Reader) ReadPacket() ([]byte, error) {
	data, err := r.ReadRawPacket()
	if err != nil {
		return data, err
	}
	// Strip trailing LF from text packets.
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	return data, nil
}

// ReadRawPacket reads a single pkt-line packet without stripping trailing LF.
// Use this for reading binary data.
//
// Special packets return a nil slice and a sentinel error:
//   - flush-pkt ("0000"): returns (nil, ErrFlush)
//   - delim-pkt ("0001"): returns (nil, ErrDelim)
func (r *Reader) ReadRawPacket() ([]byte, error) {
	if _, err := io.ReadFull(r.r, r.buf); err != nil {
		return nil, fmt.Errorf("reading pkt-line header: %w", err)
	}

	switch string(r.buf) {
	case "0000":
		return nil, ErrFlush
	case "0001":
		return nil, ErrDelim
	}

	var length uint16
	if _, err := hex.Decode(r.buf[:2], r.buf); err != nil {
		return nil, ErrInvalid
	}
	length = binary.BigEndian.Uint16(r.buf[:2])

	if length < 4 {
		return nil, fmt.Errorf("%w: length %d < 4", ErrInvalid, length)
	}

	payloadLen := int(length) - 4
	data := make([]byte, payloadLen)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return nil, fmt.Errorf("reading pkt-line payload: %w", err)
	}

	return data, nil
}

// ReadPacketText reads a single text pkt-line and returns it as a string.
func (r *Reader) ReadPacketText() (string, error) {
	data, err := r.ReadPacket()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadUntilFlush reads all packets until a flush-pkt and returns them.
// The flush-pkt itself is consumed but not included in the result.
// A delim-pkt is returned as a nil entry in the slice.
func (r *Reader) ReadUntilFlush() ([][]byte, error) {
	var packets [][]byte
	for {
		data, err := r.ReadPacket()
		if errors.Is(err, ErrFlush) {
			return packets, nil
		}
		if errors.Is(err, ErrDelim) {
			packets = append(packets, nil) // nil marks a delim
			continue
		}
		if err != nil {
			return packets, err
		}
		packets = append(packets, data)
	}
}

// Writer writes pkt-line formatted data to an underlying io.Writer.
type Writer struct {
	w io.Writer
}

// NewWriter returns a Writer that writes pkt-line packets to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WritePacket writes a single pkt-line packet containing data.
// A trailing LF is appended automatically. The data must not exceed MaxPayload-1
// bytes (to leave room for the LF).
func (w *Writer) WritePacket(data []byte) error {
	// length = 4 (header) + len(data) + 1 (LF)
	length := 4 + len(data) + 1
	if length > 4+MaxPayload {
		return fmt.Errorf("pkt-line payload too large: %d > %d", len(data), MaxPayload-1)
	}
	header := fmt.Sprintf("%04x", length)
	if _, err := io.WriteString(w.w, header); err != nil {
		return err
	}
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	_, err := w.w.Write([]byte{'\n'})
	return err
}

// WritePacketText writes a text pkt-line.
func (w *Writer) WritePacketText(s string) error {
	return w.WritePacket([]byte(s))
}

// WriteBinaryPacket writes a pkt-line containing binary data (no trailing LF).
func (w *Writer) WriteBinaryPacket(data []byte) error {
	length := 4 + len(data)
	if length > 4+MaxPayload {
		return fmt.Errorf("pkt-line payload too large: %d > %d", len(data), MaxPayload)
	}
	header := fmt.Sprintf("%04x", length)
	if _, err := io.WriteString(w.w, header); err != nil {
		return err
	}
	_, err := w.w.Write(data)
	return err
}

// WriteFlush writes a flush-pkt ("0000").
func (w *Writer) WriteFlush() error {
	_, err := io.WriteString(w.w, "0000")
	return err
}

// WriteDelim writes a delim-pkt ("0001").
func (w *Writer) WriteDelim() error {
	_, err := io.WriteString(w.w, "0001")
	return err
}

// WriteStatus writes a status line: "status <code>\n".
func (w *Writer) WriteStatus(code int) error {
	return w.WritePacketText(fmt.Sprintf("status %d", code))
}

// WriteErrorStatus writes a status error response: status line + delim + error message + flush.
func (w *Writer) WriteErrorStatus(code int, msg string) error {
	if err := w.WriteStatus(code); err != nil {
		return err
	}
	if err := w.WriteDelim(); err != nil {
		return err
	}
	for _, line := range strings.Split(msg, "\n") {
		if err := w.WritePacketText(line); err != nil {
			return err
		}
	}
	return w.WriteFlush()
}
