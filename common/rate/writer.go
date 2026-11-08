package rate

import (
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type Writer struct {
	writer  buf.Writer
	limiter *DynamicBucket
}

type DynamicBucket struct {
	v atomic.Value // *ratelimit.Bucket
}

// fillInterval is how often the bucket is refilled. Refilling a whole second's quota at once
// lets a connection burst uncapped and then stall; small slices keep the speed near the limit.
const fillInterval = 20 * time.Millisecond
const ticksPerSecond = int64(time.Second / fillInterval)

func newBucket(rate int64) *ratelimit.Bucket {
	if rate <= 0 {
		return nil // unlimited
	}
	quantum := rate / ticksPerSecond
	if quantum < 1 {
		quantum = 1
	}
	return ratelimit.NewBucketWithQuantum(fillInterval, quantum, quantum)
}

func NewDynamicBucket(rate int64) *DynamicBucket {
	d := &DynamicBucket{}
	d.v.Store(newBucket(rate))
	return d
}

func (d *DynamicBucket) Get() *ratelimit.Bucket {
	return d.v.Load().(*ratelimit.Bucket)
}

func (d *DynamicBucket) Update(rate int64) {
	d.v.Store(newBucket(rate))
}

// DuplexBucket holds separate upload and download buckets for one user, so
// each direction gets the full configured rate instead of sharing it.
type DuplexBucket struct {
	Up   *DynamicBucket
	Down *DynamicBucket
}

// NewDuplexBucket takes the upload and download rates in bytes/s; a rate of
// 0 leaves that direction unlimited.
func NewDuplexBucket(up, down int64) *DuplexBucket {
	return &DuplexBucket{
		Up:   NewDynamicBucket(up),
		Down: NewDynamicBucket(down),
	}
}

func (d *DuplexBucket) Update(up, down int64) {
	d.Up.Update(up)
	d.Down.Update(down)
}

// Wait blocks until n bytes are allowed through, or returns at once when the
// bucket is currently unlimited. It lets a DynamicBucket pace raw connection
// reads outside the buf.Reader/Writer pipeline.
func (d *DynamicBucket) Wait(n int64) {
	if b := d.Get(); b != nil {
		b.Wait(n)
	}
}

func NewRateLimitWriter(writer buf.Writer, limiter *DynamicBucket) buf.Writer {
	return &Writer{
		writer:  writer,
		limiter: limiter,
	}
}

func (w *Writer) Close() error {
	return common.Close(w.writer)
}

func (w *Writer) Interrupt() {
	common.Interrupt(w.writer)
}

// WriteMultiBuffer passes mb on in parts of at most one refill each: waiting
// for all of it at once makes a low limit deliver tens of KB in one burst.
func (w *Writer) WriteMultiBuffer(mb buf.MultiBuffer) error {
	limiter := w.limiter.Get()
	if limiter == nil {
		return w.writer.WriteMultiBuffer(mb)
	}
	for len(mb) > 0 {
		var part buf.MultiBuffer
		var size int64
		part, mb, size = splitRefill(mb, limiter.Capacity())
		limiter.Wait(size)
		if err := w.writer.WriteMultiBuffer(part); err != nil {
			buf.ReleaseMulti(mb)
			return err
		}
	}
	return nil
}

// splitRefill cuts mb after the buffers that fit in limit bytes, keeping at
// least one buffer in part.
func splitRefill(mb buf.MultiBuffer, limit int64) (part, rest buf.MultiBuffer, size int64) {
	n := 0
	for n < len(mb) && (n == 0 || size+int64(mb[n].Len()) <= limit) {
		size += int64(mb[n].Len())
		n++
	}
	return mb[:n:n], mb[n:], size
}
