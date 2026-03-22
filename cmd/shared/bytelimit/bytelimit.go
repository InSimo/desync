// Package bytelimit provides cross-process in-flight byte tracking via
// shared memory.  Multiple independent processes (e.g. git-lfs-transfer
// instances spawned by concurrent SSH connections, or git-lfs-desync agent
// processes spawned by git-lfs with concurrent=true) use a shared memory
// region to coordinate so the total bytes being processed doesn't exceed
// a configurable limit.
//
// The shared memory region contains 64 fixed-size slots.  Each process
// claims a slot by writing its PID and the byte count of the object it
// is about to process.  Before starting work, it sums all active slots
// to check whether the limit would be exceeded.  If so, it waits and
// retries.  A single object that exceeds the limit is still allowed to
// proceed once no other objects are in flight (the "lone wolf" rule).
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
// processes sharing the same memory region.
type Gate struct {
	data  []byte // mmap'd shared memory
	limit int64
}

// Slot represents a claimed slot in the shared memory region.
// Call Release when the object has been fully processed.
type Slot struct {
	gate *Gate
	idx  int
}

// slot layout helpers.  Each slot is 16 bytes:
//
//	offset 0:  int32  PID  (0 = unused)
//	offset 4:  int32  (padding)
//	offset 8:  int64  byte count
func (g *Gate) slotPID(i int) *int32 {
	return (*int32)(unsafe.Pointer(&g.data[i*slotSize]))
}

func (g *Gate) slotBytes(i int) *int64 {
	return (*int64)(unsafe.Pointer(&g.data[i*slotSize+8]))
}

// OpenGate opens (or creates) the shared memory region and returns a Gate.
// limit is the maximum total in-flight bytes allowed across all processes.
// A limit of 0 disables admission control (Acquire always succeeds immediately).
func OpenGate(limit int64) (*Gate, error) {
	if limit < 0 {
		return nil, fmt.Errorf("bytelimit: negative limit %d", limit)
	}

	data, err := openSharedMem()
	if err != nil {
		return nil, fmt.Errorf("bytelimit: open shared memory: %w", err)
	}

	return &Gate{data: data, limit: limit}, nil
}

// Close unmaps the shared memory.  It does NOT remove the shared memory
// file — other processes may still be using it.
func (g *Gate) Close() error {
	return closeSharedMem(g.data)
}

// Acquire blocks until the process is admitted.  It claims a slot and
// records the given byte count.  The returned Slot must be released when
// the object has been fully processed.
//
// If ctx is cancelled, Acquire returns ctx.Err() without claiming a slot.
//
// Corner case ("lone wolf" rule): if size exceeds the limit but no other
// process is active, the call succeeds — a single object larger than the
// limit must still be processable.
func (g *Gate) Acquire(ctx context.Context, size int64) (*Slot, error) {
	if g.limit == 0 {
		// Admission control disabled — always succeed.
		return &Slot{gate: g, idx: -1}, nil
	}

	pid := int32(os.Getpid())

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Clean up stale slots from crashed processes.
		g.reapStale(pid)

		// Sum in-flight bytes and count active slots (excluding our own PID).
		var total int64
		var othersActive int
		for i := range MaxSlots {
			p := atomic.LoadInt32(g.slotPID(i))
			if p == 0 {
				continue
			}
			b := atomic.LoadInt64(g.slotBytes(i))
			if p == pid {
				// Our own slot from a previous iteration (shouldn't happen
				// normally, but be safe).
				total += b
			} else {
				total += b
				othersActive++
			}
		}

		// Check admission.
		if total+size <= g.limit || othersActive == 0 {
			// Admitted — claim a slot.
			slot, err := g.claimSlot(pid, size)
			if err != nil {
				// All slots full — wait and retry.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(retryInterval):
					continue
				}
			}
			return slot, nil
		}

		// Over the limit — wait and retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}

// claimSlot finds an empty slot and atomically writes the PID and byte count.
func (g *Gate) claimSlot(pid int32, size int64) (*Slot, error) {
	for i := range MaxSlots {
		// Try to claim an empty slot (PID == 0).
		if atomic.CompareAndSwapInt32(g.slotPID(i), 0, pid) {
			atomic.StoreInt64(g.slotBytes(i), size)
			return &Slot{gate: g, idx: i}, nil
		}
	}
	return nil, fmt.Errorf("bytelimit: all %d slots occupied", MaxSlots)
}

// reapStale detects slots held by dead processes and clears them.
func (g *Gate) reapStale(myPID int32) {
	for i := range MaxSlots {
		p := atomic.LoadInt32(g.slotPID(i))
		if p == 0 || p == myPID {
			continue
		}
		if !processAlive(p) {
			// Process is dead — reclaim the slot.  Use CAS to avoid
			// racing with another reaper or with the dead process's
			// successor (which may have already claimed the slot).
			if atomic.CompareAndSwapInt32(g.slotPID(i), p, 0) {
				atomic.StoreInt64(g.slotBytes(i), 0)
			}
		}
	}
}

// Release clears this slot, allowing other processes to use the capacity.
// It is safe to call Release multiple times.
func (s *Slot) Release() {
	if s == nil || s.gate == nil || s.idx < 0 {
		return
	}
	atomic.StoreInt64(s.gate.slotBytes(s.idx), 0)
	atomic.StoreInt32(s.gate.slotPID(s.idx), 0)
	s.gate = nil
}
