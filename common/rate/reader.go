package rate

import (
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type Reader struct {
	reader  buf.TimeoutReader
	limiter *DynamicBucket
	pending buf.MultiBuffer
}

func NewRateLimitReader(reader buf.TimeoutReader, limiter *DynamicBucket) *Reader {
	return &Reader{
		reader:  reader,
		limiter: limiter,
	}
}

func (r *Reader) take(mb buf.MultiBuffer, err error) (buf.MultiBuffer, error) {
	limiter := r.limiter.Get()
	if limiter == nil {
		return mb, err
	}
	if err != nil {
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
