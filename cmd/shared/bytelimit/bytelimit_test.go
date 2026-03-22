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
	slot, err := g.Acquire(ctx, 100*1024*1024) // 100 MB
	if err != nil {
		t.Fatal(err)
	}
	slot.Release()
}

func TestReleaseIdempotent(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	slot, err := g.Acquire(context.Background(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	slot.Release()
	slot.Release() // should not panic
}

func TestDisabledGate(t *testing.T) {
	g, err := OpenGate(0)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	slot, err := g.Acquire(context.Background(), 1<<40) // 1 TB — should still succeed
	if err != nil {
		t.Fatal(err)
	}
	slot.Release()
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

	slot, err := g.Acquire(ctx, 1<<20) // 1 MB — exceeds 1 KB limit but lone wolf
	if err != nil {
		t.Fatalf("lone wolf should succeed, got: %v", err)
	}
	slot.Release()
}

func TestSlotWrittenCorrectly(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	slot, err := g.Acquire(context.Background(), 42*1024*1024) // 42 MB
	if err != nil {
		t.Fatal(err)
	}

	// Verify the slot has our PID and the right byte count.
	pid := atomic.LoadInt32(g.slotPID(slot.idx))
	bytes := atomic.LoadInt64(g.slotBytes(slot.idx))

	if pid != int32(os.Getpid()) {
		t.Errorf("slot PID = %d, want %d", pid, os.Getpid())
	}
	if bytes != 42*1024*1024 {
		t.Errorf("slot bytes = %d, want %d", bytes, 42*1024*1024)
	}

	slot.Release()

	// Verify cleared.
	pid = atomic.LoadInt32(g.slotPID(slot.idx))
	bytes = atomic.LoadInt64(g.slotBytes(slot.idx))
	if pid != 0 || bytes != 0 {
		t.Errorf("after release: PID=%d bytes=%d, want 0/0", pid, bytes)
	}
}

func TestReapStale(t *testing.T) {
	g, err := OpenGate(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Manually write a stale slot with a PID that doesn't exist.
	// Use PID 2^30 which is almost certainly not a real process.
	stalePID := int32(1 << 30)
	slotIdx := 0
	atomic.StoreInt32(g.slotPID(slotIdx), stalePID)
	atomic.StoreInt64(g.slotBytes(slotIdx), 999)

	// Reap should clear it.
	g.reapStale(int32(os.Getpid()))

	pid := atomic.LoadInt32(g.slotPID(slotIdx))
	bytes := atomic.LoadInt64(g.slotBytes(slotIdx))
	if pid != 0 || bytes != 0 {
		t.Errorf("stale slot not reaped: PID=%d bytes=%d", pid, bytes)
	}
}

func TestOverLimitBlocksCrossProcess(t *testing.T) {
	// Simulate another process by manually writing a slot with a fake PID
	// that is "alive" (use PID 1 — init/systemd, always alive on Linux).
	g, err := OpenGate(1024) // 1 KB limit
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Write a fake slot as if another process is using 1024 bytes.
	fakePID := int32(1) // PID 1 is always alive
	atomic.StoreInt32(g.slotPID(0), fakePID)
	atomic.StoreInt64(g.slotBytes(0), 1024)

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

	// Clear the fake slot — now we should succeed.
	atomic.StoreInt32(g.slotPID(0), 0)
	atomic.StoreInt64(g.slotBytes(0), 0)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	slot, err := g.Acquire(ctx2, 512)
	if err != nil {
		t.Fatalf("should succeed after clearing fake slot: %v", err)
	}
	slot.Release()
}

func TestContextCancellation(t *testing.T) {
	g, err := OpenGate(100)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Write a fake slot to push over the limit.
	atomic.StoreInt32(g.slotPID(0), 1)
	atomic.StoreInt64(g.slotBytes(0), 100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = g.Acquire(ctx, 50)
	if err != context.Canceled {
		t.Fatalf("expected Canceled, got: %v", err)
	}

	// Clean up fake slot.
	atomic.StoreInt32(g.slotPID(0), 0)
	atomic.StoreInt64(g.slotBytes(0), 0)
}
