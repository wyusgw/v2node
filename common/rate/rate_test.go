package rate

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport/pipe"
)

// pump writes total bytes into w in buf.Size chunks from a goroutine and
// returns how long it took until r had delivered all of them.
func pump(t *testing.T, w buf.Writer, r buf.Reader, total int) time.Duration {
	t.Helper()
	errc := make(chan error, 1)
	start := time.Now()
	go func() {
		for sent := 0; sent < total; {
			b := buf.New()
			n := min(int(buf.Size), total-sent)
			b.Extend(int32(n))
			if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				errc <- err
				return
			}
			sent += n
		}
		errc <- nil
	}()
	for got := 0; got < total; {
		mb, err := r.ReadMultiBuffer()
		if err != nil {
			t.Errorf("read: %v", err)
			return time.Since(start)
		}
		got += int(mb.Len())
		buf.ReleaseMulti(mb)
	}
	elapsed := time.Since(start)
	if err := <-errc; err != nil {
		t.Errorf("write: %v", err)
	}
	return elapsed
}

// assertRate checks total bytes moved in elapsed match rate bytes/s. The
// bucket starts full with one fill slice, so that burst is free.
func assertRate(t *testing.T, name string, total int, rate int64, elapsed time.Duration) {
	t.Helper()
	burst := max(rate/ticksPerSecond, 1)
	want := time.Duration(float64(int64(total)-burst) / float64(rate) * float64(time.Second))
	got := float64(total) / elapsed.Seconds()
	t.Logf("%s: %d bytes in %v = %.0f B/s (limit %d B/s)", name, total, elapsed.Round(time.Millisecond), got, rate)
	if elapsed < want*85/100 || elapsed > want*120/100 {
		t.Errorf("%s: took %v, want about %v (limit %d B/s, measured %.0f B/s)", name, elapsed, want, rate, got)
	}
}

func TestWriterLimitsThroughput(t *testing.T) {
	for _, rate := range []int64{512 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024} {
		r, w := pipe.New()
		lw := NewRateLimitWriter(w, NewDynamicBucket(rate))
		total := int(rate) // ~1s
		assertRate(t, "writer", total, rate, pump(t, lw, r, total))
	}
}

func TestReaderLimitsThroughput(t *testing.T) {
	for _, rate := range []int64{512 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024} {
		r, w := pipe.New()
		lr := NewRateLimitReader(&buf.TimeoutWrapperReader{Reader: r}, NewDynamicBucket(rate))
		total := int(rate)
		assertRate(t, "reader", total, rate, pump(t, w, lr, total))
	}
}

func TestUnlimitedDoesNotWait(t *testing.T) {
	r, w := pipe.New()
	lw := NewRateLimitWriter(w, NewDynamicBucket(0))
	if d := pump(t, lw, r, 64*1024*1024); d > 500*time.Millisecond {
		t.Errorf("unlimited writer took %v for 64MiB", d)
	}
}

func TestUpdateAppliesToLiveWriter(t *testing.T) {
	r, w := pipe.New()
	b := NewDynamicBucket(512 * 1024)
	lw := NewRateLimitWriter(w, b)
	assertRate(t, "before update", 512*1024, 512*1024, pump(t, lw, r, 512*1024))

	b.Update(4 * 1024 * 1024)
	assertRate(t, "after raise", 4*1024*1024, 4*1024*1024, pump(t, lw, r, 4*1024*1024))

	b.Update(0)
	if d := pump(t, lw, r, 32*1024*1024); d > 500*time.Millisecond {
		t.Errorf("after lifting limit: 32MiB took %v", d)
	}
}

func TestDuplexDirectionsAreIndependent(t *testing.T) {
	const up, down = 1024 * 1024, 4 * 1024 * 1024
	d := NewDuplexBucket(up, down)
	upR, upW := pipe.New()
	downR, downW := pipe.New()
	upLW := NewRateLimitWriter(upW, d.Up)
	downLW := NewRateLimitWriter(downW, d.Down)

	// Run both directions at once: each must get its own full rate.
	type res struct{ d time.Duration }
	upc, downc := make(chan res, 1), make(chan res, 1)
	go func() { upc <- res{pump(t, upLW, upR, up)} }()
	go func() { downc <- res{pump(t, downLW, downR, down)} }()
	assertRate(t, "up", up, up, (<-upc).d)
	assertRate(t, "down", down, down, (<-downc).d)
}
