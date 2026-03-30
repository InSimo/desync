package pktline

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReadRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	pl := New(&buf, &buf)

	require.NoError(t, pl.WritePacketText("version 1"))
	require.NoError(t, pl.WritePacketText("key=value"))
	require.NoError(t, pl.WriteDelim())
	require.NoError(t, pl.WritePacketText("data line"))
	require.NoError(t, pl.WriteFlush())

	// Re-create reader on the written data.
	r := New(bytes.NewReader(buf.Bytes()), io.Discard)

	s, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "version 1", s)

	s, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "key=value", s)

	_, length, err := r.ReadPacketWithLength()
	require.NoError(t, err)
	require.Equal(t, 1, length)

	s, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "data line", s)

	_, length, err = r.ReadPacketWithLength()
	require.NoError(t, err)
	require.Equal(t, 0, length)
}

func TestBinaryPacket(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02, 0xff, 0xfe}
	var buf bytes.Buffer
	pl := New(&buf, &buf)
	require.NoError(t, pl.WritePacket(data))
	require.NoError(t, pl.WriteFlush())

	r := New(bytes.NewReader(buf.Bytes()), io.Discard)
	got, err := r.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestWriteStatus(t *testing.T) {
	var buf bytes.Buffer
	pl := New(&buf, &buf)
	require.NoError(t, pl.WriteStatus(200))
	require.NoError(t, pl.WriteFlush())

	r := New(bytes.NewReader(buf.Bytes()), io.Discard)
	s, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 200", s)
}

func TestWriteErrorStatus(t *testing.T) {
	var buf bytes.Buffer
	pl := New(&buf, &buf)
	require.NoError(t, pl.WriteErrorStatus(404, "not found"))

	r := New(bytes.NewReader(buf.Bytes()), io.Discard)
	s, err := r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "status 404", s)

	_, length, err := r.ReadPacketWithLength()
	require.NoError(t, err)
	require.Equal(t, 1, length)

	s, err = r.ReadPacketText()
	require.NoError(t, err)
	require.Equal(t, "not found", s)

	_, length, err = r.ReadPacketWithLength()
	require.NoError(t, err)
	require.Equal(t, 0, length)
}

func TestPayloadTooLarge(t *testing.T) {
	pl := New(bytes.NewReader(nil), &bytes.Buffer{})
	bigData := make([]byte, MaxPayload+1)
	require.Error(t, pl.WritePacket(bigData))
}
