package rate

import (
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// Reader throttles data read from the client (upload) for inbounds that hand
// the dispatcher a ready-made link, where there's no upload writer to wrap.
// Waiting after each read applies backpressure, so the client is slowed to
// the configured rate.
type Reader struct {
	reader  buf.TimeoutReader
	limiter *DynamicBucket
	// read from the client but not passed on yet, see take
	pending buf.MultiBuffer
}

func NewRateLimitReader(reader buf.TimeoutReader, limiter *DynamicBucket) *Reader {
	return &Reader{
		reader:  reader,
		limiter: limiter,
	}
}

// take passes on mb in parts of at most one refill and keeps the rest for the
// next read, so a low limit doesn't let a whole read through in one burst.
func (r *Reader) take(mb buf.MultiBuffer, err error) (buf.MultiBuffer, error) {
	limiter := r.limiter.Get()
	if limiter == nil {
		return mb, err
	}
	if err != nil {
		// Nothing reads after an error, so pass the rest on now.
		if n := int64(mb.Len()); n > 0 {
			limiter.Wait(n)
		}
		return mb, err
	}
	part, rest, size := splitRefill(mb, limiter.Capacity())
	if len(rest) > 0 {
		r.pending = rest
	}
	if size > 0 {
		limiter.Wait(size)
	}
	return part, nil
}

func (r *Reader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if mb := r.pending; len(mb) > 0 {
		r.pending = nil
		return r.take(mb, nil)
	}
	return r.take(r.reader.ReadMultiBuffer())
}

func (r *Reader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	if mb := r.pending; len(mb) > 0 {
		r.pending = nil
		return r.take(mb, nil)
	}
	return r.take(r.reader.ReadMultiBufferTimeout(timeout))
}

func (r *Reader) Interrupt() {
	common.Interrupt(r.reader)
}
