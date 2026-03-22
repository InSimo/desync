// Package bytelimit provides cross-process in-flight byte tracking via
// shared memory.  Multiple independent processes (e.g. git-lfs-transfer
// instances spawned by concurrent SSH connections, or git-lfs-desync agent
// processes spawned by git-lfs with concurrent=true) use a shared memory
// region to coordinate so the total bytes being processed doesn't exceed
// a configurable limit.
//
// The shared memory region contains 64 fixed-size slots.  Each process
// claims one slot at OpenGate time (by writing its PID) and atomically
// updates the byte counter for each Acquire/Release.  Before starting
// work on an object, the process sums all active slots to check whether
// the limit would be exceeded.  If so, it waits and retries.  A single
// object that exceeds the limit is still allowed to proceed once no
// other bytes are in flight (the "lone wolf" rule).
//
// Crash safety: if a process dies without releasing its slot, the stale
// PID is detected by other processes via kill(pid, 0) and the slot is
// reclaimed.
package bytelimit

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// MaxSlots is the maximum number of concurrent processes tracked.
	MaxSlots = 64

	// slotSize is the byte size of each slot: int32 PID + 4 bytes padding + int64 byte count.
	slotSize = 16

	// shmSize is the total size of the shared memory region.
	shmSize = MaxSlots * slotSize

	// retryInterval is how long Acquire sleeps before rechecking when over the limit.
	retryInterval = 100 * time.Millisecond
)

// Gate controls admission based on total in-flight bytes across all
// processes sharing the same memory region.  One Gate per process;
// it owns a single slot in shared memory.
type Gate struct {
	data  []byte // mmap'd shared memory
	limit int64
	pid   int32
	slot  int // index of this process's slot (-1 if disabled)
}

// Reservation represents an in-flight byte reservation.
// Call Release when the object has been fully processed.
type Reservation struct {
	gate *Gate
	size int64
}

// slot layout helpers.  Each slot is 16 bytes:
//
//	offset 0:  int32  PID  (0 = unused)
//	offset 4:  int32  (padding)
//	offset 8:  int64  byte count (cumulative across concurrent operations)
func (g *Gate) slotPID(i int) *int32 {
	return (*int32)(unsafe.Pointer(&g.data[i*slotSize]))
}

func (g *Gate) slotBytes(i int) *int64 {
	return (*int64)(unsafe.Pointer(&g.data[i*slotSize+8]))
}

// OpenGate opens (or creates) the shared memory region, claims a slot
// for this process, and returns a Gate.
//
// limit is the maximum total in-flight bytes allowed across all processes.
// A limit of 0 disables admission control (Acquire always succeeds immediately).
func OpenGate(limit int64) (*Gate, error) {
	if limit < 0 {
		return nil, fmt.Errorf("bytelimit: negative limit %d", limit)
	}

	g := &Gate{
		limit: limit,
		pid:   int32(os.Getpid()),
		slot:  -1,
	}

	if limit == 0 {
		return g, nil
	}

	data, err := openSharedMem()
	if err != nil {
		return nil, fmt.Errorf("bytelimit: open shared memory: %w", err)
	}
	g.data = data

	// Claim a slot: find one already owned by our PID (e.g. after exec)
	// or an empty one.
	for i := range MaxSlots {
		p := atomic.LoadInt32(g.slotPID(i))
		if p == g.pid {
			g.slot = i
			// Reset byte counter in case of stale state from a previous run
			// with the same PID.
			atomic.StoreInt64(g.slotBytes(i), 0)
			return g, nil
		}
	}
	for i := range MaxSlots {
		if atomic.CompareAndSwapInt32(g.slotPID(i), 0, g.pid) {
			atomic.StoreInt64(g.slotBytes(i), 0)
			g.slot = i
			return g, nil
		}
	}

	// All slots occupied — try reaping stale PIDs once.
	g.reapStale()
	for i := range MaxSlots {
		if atomic.CompareAndSwapInt32(g.slotPID(i), 0, g.pid) {
			atomic.StoreInt64(g.slotBytes(i), 0)
			g.slot = i
			return g, nil
		}
	}

	closeSharedMem(data)
	return nil, fmt.Errorf("bytelimit: all %d slots occupied", MaxSlots)
}

// Close releases this process's slot and unmaps the shared memory.
func (g *Gate) Close() error {
	if g.slot >= 0 {
		atomic.StoreInt64(g.slotBytes(g.slot), 0)
		atomic.StoreInt32(g.slotPID(g.slot), 0)
		g.slot = -1
	}
	if g.data != nil {
		return closeSharedMem(g.data)
	}
	return nil
}

// Acquire blocks until the process is admitted to start processing an
// object of the given size.  It atomically adds size to this process's
// slot counter.  The returned Reservation must be released when the
// object has been fully processed.
//
// If ctx is cancelled, Acquire returns ctx.Err() without modifying the
// counter.
//
// Corner case ("lone wolf" rule): if the total in-flight bytes would
// exceed the limit, but no bytes are currently in flight (across all
// processes and all operations), the call succeeds — a single object
// must be processable even when it exceeds the limit.
func (g *Gate) Acquire(ctx context.Context, size int64) (*Reservation, error) {
	if g.limit == 0 {
		return &Reservation{gate: g, size: size}, nil
	}

	reaped := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Read current state before modifying anything.
		total := g.totalInFlight()

		if total+size <= g.limit {
			// Under the limit — commit the addition and admit.
			atomic.AddInt64(g.slotBytes(g.slot), size)
			return &Reservation{gate: g, size: size}, nil
		}

		// Over the limit.  Check lone-wolf: if there are zero in-flight
		// bytes across all processes (including our own), we may proceed —
		// a single object must be processable even when it exceeds the
		// limit, but only once all other work is done.
		if total == 0 {
			atomic.AddInt64(g.slotBytes(g.slot), size)
			return &Reservation{gate: g, size: size}, nil
		}

		// Over the limit with other processes active — wait.

		// Reap stale PIDs once per wait cycle.
		if !reaped {
			g.reapStale()
			reaped = true
			continue // retry immediately after reaping
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
			reaped = false
		}
	}
}

// totalInFlight sums the byte counters of all active slots.
func (g *Gate) totalInFlight() int64 {
	var total int64
	for i := range MaxSlots {
		p := atomic.LoadInt32(g.slotPID(i))
		if p == 0 {
			continue
		}
		total += atomic.LoadInt64(g.slotBytes(i))
	}
	return total
}

// reapStale detects slots held by dead processes and clears them.
func (g *Gate) reapStale() {
	for i := range MaxSlots {
		p := atomic.LoadInt32(g.slotPID(i))
		if p == 0 || p == g.pid {
			continue
		}
		if !processAlive(p) {
			if atomic.CompareAndSwapInt32(g.slotPID(i), p, 0) {
				atomic.StoreInt64(g.slotBytes(i), 0)
			}
		}
	}
}

// Release subtracts this reservation's byte count from the process's slot.
// It is safe to call Release multiple times.
func (r *Reservation) Release() {
	if r == nil || r.gate == nil || r.size == 0 {
		return
	}
	if r.gate.slot >= 0 {
		atomic.AddInt64(r.gate.slotBytes(r.gate.slot), -r.size)
	}
	r.size = 0
}
