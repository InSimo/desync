package pktline

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReadRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	require.NoError(t, w.WritePacketText("version 1"))
	require.NoError(t, w.WritePacketText("key=value"))
	require.NoError(t, w.WriteDelim())
	require.NoError(t, w.WritePacketText("data line"))
	require.NoError(t, w.WriteFlush())

	r := NewReader(&buf)

	s, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "version 1", s)

	s, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "key=value", s)

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, ErrDelim)

	s, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "data line", s)

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, ErrFlush)
}

func TestReadUntilFlush(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	require.NoError(t, w.WritePacketText("line1"))
	require.NoError(t, w.WritePacketText("line2"))
	require.NoError(t, w.WriteFlush())

	r := NewReader(&buf)
	packets, err := r.ReadUntilFlush()
	require.NoError(t, err)
	require.Len(t, packets, 2)
	require.Equal(t, "line1", string(packets[0]))
	require.Equal(t, "line2", string(packets[1]))
}

func TestReadUntilFlushWithDelim(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	require.NoError(t, w.WritePacketText("header"))
	require.NoError(t, w.WriteDelim())
	require.NoError(t, w.WritePacketText("body"))
	require.NoError(t, w.WriteFlush())

	r := NewReader(&buf)
	packets, err := r.ReadUntilFlush()
	require.NoError(t, err)
	require.Len(t, packets, 3)
	require.Equal(t, "header", string(packets[0]))
	require.Nil(t, packets[1]) // delim marker
	require.Equal(t, "body", string(packets[2]))
}

func TestBinaryPacket(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02, 0xff, 0xfe}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	require.NoError(t, w.WriteBinaryPacket(data))
	require.NoError(t, w.WriteFlush())

	r := NewReader(&buf)
	// Binary packets don't have trailing LF, so the reader
	// won't strip anything.
	got, err := r.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestWriteStatus(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	require.NoError(t, w.WriteStatus(200))
	require.NoError(t, w.WriteFlush())

	r := NewReader(&buf)
	s, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", s)
}

func TestWriteErrorStatus(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	require.NoError(t, w.WriteErrorStatus(404, "not found"))

	r := NewReader(&buf)
	s, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 404", s)

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, ErrDelim)

	s, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "not found", s)

	_, err = r.ReadPacket()
	require.ErrorIs(t, err, ErrFlush)
}

func TestEmptyStream(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	_, err := r.ReadPacket()
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.Unwrap(err)) || err != nil)
}

func TestPayloadTooLarge(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	bigData := make([]byte, MaxPayload+1)
	require.Error(t, w.WritePacket(bigData))
}
