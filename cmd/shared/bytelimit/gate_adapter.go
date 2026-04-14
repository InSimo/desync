package bytelimit

import (
	"context"

	"github.com/folbricht/desync"
)

// Verify that *Gate satisfies the structural requirements at compile time.
// The actual interface conformance is via GateAdapter below, because
// Gate.Acquire returns *Reservation (concrete) while TransferGate requires
// ByteReservation (interface).

// GateAdapter wraps a *Gate to satisfy the desync.TransferGate interface.
type GateAdapter struct {
	*Gate
}

func (a *GateAdapter) Acquire(ctx context.Context, size int64) (desync.ByteReservation, error) {
	return a.Gate.Acquire(ctx, size)
}

// AsTransferGate returns the Gate as a desync.TransferGate, or nil if the
// Gate has no limits configured (both byte limit and ops limit are zero).
func (g *Gate) AsTransferGate() desync.TransferGate {
	if g.limit == 0 && g.maxOps == 0 {
		return nil
	}
	return &GateAdapter{g}
}
