package main

import (
	"fmt"
	"strings"

	"github.com/git-lfs/pktline"
)

// writeStatus writes a "status <code>\n" pkt-line text packet.
func writeStatus(pl *pktline.Pktline, code int) error {
	return pl.WritePacketText(fmt.Sprintf("status %d", code))
}

// writeErrorStatus writes a complete error response:
// status line + delim + error message lines + flush.
func writeErrorStatus(pl *pktline.Pktline, code int, msg string) error {
	if err := writeStatus(pl, code); err != nil {
		return err
	}
	if err := pl.WriteDelim(); err != nil {
		return err
	}
	for _, line := range strings.Split(msg, "\n") {
		if err := pl.WritePacketText(line); err != nil {
			return err
		}
	}
	return pl.WriteFlush()
}
