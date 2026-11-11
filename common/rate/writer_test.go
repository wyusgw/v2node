package rate

import (
	"testing"
	"time"
)

// measure pushes total bytes through b in chunk-sized Waits and returns the
// achieved rate in bytes/s.
func measure(b *DynamicBucket, total, chunk int64) float64 {
	start := time.Now()
	for sent := int64(0); sent < total; sent += chunk {
		b.Wait(chunk)
	}
	return float64(total) / time.Since(start).Seconds()
}

func TestBucketRate(t *testing.T) {
	for _, rate := range []int64{125000, 1250000, 12500000} { // 1, 10, 100 Mbps
		b := NewDynamicBucket(rate)
		// one second's worth, after the initial (one-tick) burst
		got := measure(b, rate, 8192)
		if got < float64(rate)*0.95 || got > float64(rate)*1.05 {
			t.Errorf("rate %d: achieved %.0f B/s", rate, got)
		}
	}
}

func TestBucketBurstBoundedToOneTick(t *testing.T) {
	const rate = 12500000 // 100 Mbps, 250 KB per 20 ms tick
	b := NewDynamicBucket(rate)
	if c := b.Get().Capacity(); c != rate/ticksPerSecond {
		t.Fatalf("capacity %d, want %d", c, rate/ticksPerSecond)
	}
}

func TestUnlimited(t *testing.T) {
	b := NewDynamicBucket(0)
	if b.Get() != nil {
		t.Fatal("rate 0 should be unlimited")
	}
	start := time.Now()
	b.Wait(1 << 40)
	if time.Since(start) > 10*time.Millisecond {
		t.Error("unlimited bucket blocked")
	}
}

func TestUpdateLive(t *testing.T) {
	b := NewDynamicBucket(125000)
	b.Update(1250000)
	if got := measure(b, 1250000, 8192); got < 1250000*0.95 {
		t.Errorf("after raising the limit achieved %.0f B/s", got)
	}
	b.Update(0)
	if b.Get() != nil {
		t.Error("update to 0 did not make the bucket unlimited")
	}
}

func TestDuplexIndependent(t *testing.T) {
	d := NewDuplexBucket(125000, 1250000)
	done := make(chan float64)
	go func() { done <- measure(d.Up, 125000, 4096) }()
	down := measure(d.Down, 1250000, 8192)
	up := <-done
	if up < 125000*0.9 || up > 125000*1.1 || down < 1250000*0.9 || down > 1250000*1.1 {
		t.Errorf("up %.0f B/s down %.0f B/s, want ~125000 / ~1250000", up, down)
	}
}
