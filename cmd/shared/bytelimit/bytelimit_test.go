package bytelimit

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenAndClose(t *testing.T) {
	g, err := OpenGate(1 << 30) // 1 GB
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRelease(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	ctx := context.Background()
	r, err := g.Acquire(ctx, 100*1024*1024) // 100 MB
	if err != nil {
		t.Fatal(err)
	}

	// Slot should show our bytes.
	b := atomic.LoadInt64(g.slotBytes(g.slot))
	if b != 100*1024*1024 {
		t.Errorf("slot bytes = %d, want %d", b, 100*1024*1024)
	}

	r.Release()

	// After release, bytes should be 0.
	b = atomic.LoadInt64(g.slotBytes(g.slot))
	if b != 0 {
		t.Errorf("after release: slot bytes = %d, want 0", b)
	}
}

func TestReleaseIdempotent(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	r, err := g.Acquire(context.Background(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	r.Release()
	r.Release() // should not panic or double-subtract
}

func TestDisabledGate(t *testing.T) {
	g, err := OpenGate(0)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	r, err := g.Acquire(context.Background(), 1<<40) // 1 TB — should still succeed
	if err != nil {
		t.Fatal(err)
	}
	r.Release()
}

func TestLoneWolf(t *testing.T) {
	// A single object larger than the limit should proceed when alone.
	g, err := OpenGate(1024) // 1 KB limit
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := g.Acquire(ctx, 1<<20) // 1 MB — exceeds 1 KB limit, lone wolf
	if err != nil {
		t.Fatalf("lone wolf should succeed, got: %v", err)
	}
	r.Release()
}

func TestCumulativeInProcess(t *testing.T) {
	// Multiple Acquire calls within the same process should accumulate.
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	ctx := context.Background()
	r1, err := g.Acquire(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := g.Acquire(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}

	b := atomic.LoadInt64(g.slotBytes(g.slot))
	if b != 300 {
		t.Errorf("cumulative bytes = %d, want 300", b)
	}

	r1.Release()
	b = atomic.LoadInt64(g.slotBytes(g.slot))
	if b != 200 {
		t.Errorf("after r1 release: bytes = %d, want 200", b)
	}

	r2.Release()
	b = atomic.LoadInt64(g.slotBytes(g.slot))
	if b != 0 {
		t.Errorf("after r2 release: bytes = %d, want 0", b)
	}
}

func TestSlotClaimedAtInit(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Slot should be claimed with our PID.
	if g.slot < 0 {
		t.Fatal("slot not claimed")
	}
	pid := atomic.LoadInt32(g.slotPID(g.slot))
	if pid != int32(os.Getpid()) {
		t.Errorf("slot PID = %d, want %d", pid, os.Getpid())
	}
}

func TestReapStale(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Manually write a stale slot with a PID that doesn't exist.
	stalePID := int32(1 << 30)
	staleSlot := (g.slot + 1) % MaxSlots // pick a different slot
	atomic.StoreInt32(g.slotPID(staleSlot), stalePID)
	atomic.StoreInt64(g.slotBytes(staleSlot), 999)

	g.reapStale()

	pid := atomic.LoadInt32(g.slotPID(staleSlot))
	bytes := atomic.LoadInt64(g.slotBytes(staleSlot))
	if pid != 0 || bytes != 0 {
		t.Errorf("stale slot not reaped: PID=%d bytes=%d", pid, bytes)
	}
}

func TestOverLimitBlocksCrossProcess(t *testing.T) {
	g, err := OpenGate(1024) // 1 KB limit
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Simulate another process by writing a fake slot with PID 1 (always alive).
	fakeSlot := (g.slot + 1) % MaxSlots
	atomic.StoreInt32(g.slotPID(fakeSlot), 1)
	atomic.StoreInt64(g.slotBytes(fakeSlot), 1024)

	// Try to acquire — should block (over limit, another process active).
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err = g.Acquire(ctx, 512)
	if err == nil {
		t.Fatal("expected timeout, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got: %v", err)
	}

	// Clear the fake slot — should succeed now.
	atomic.StoreInt32(g.slotPID(fakeSlot), 0)
	atomic.StoreInt64(g.slotBytes(fakeSlot), 0)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	r, err := g.Acquire(ctx2, 512)
	if err != nil {
		t.Fatalf("should succeed after clearing fake slot: %v", err)
	}
	r.Release()
}

func TestContextCancellation(t *testing.T) {
	g, err := OpenGate(100)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Simulate another process over the limit.
	fakeSlot := (g.slot + 1) % MaxSlots
	atomic.StoreInt32(g.slotPID(fakeSlot), 1)
	atomic.StoreInt64(g.slotBytes(fakeSlot), 100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = g.Acquire(ctx, 50)
	if err != context.Canceled {
		t.Fatalf("expected Canceled, got: %v", err)
	}

	// Clean up.
	atomic.StoreInt32(g.slotPID(fakeSlot), 0)
	atomic.StoreInt64(g.slotBytes(fakeSlot), 0)
}
