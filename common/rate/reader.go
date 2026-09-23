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
}

func NewRateLimitReader(reader buf.TimeoutReader, limiter *DynamicBucket) *Reader {
	return &Reader{
		reader:  reader,
		limiter: limiter,
	}
}

func (r *Reader) wait(mb buf.MultiBuffer) {
	if n := int64(mb.Len()); n > 0 {
		if limiter := r.limiter.Get(); limiter != nil {
			limiter.Wait(n)
		}
	}
}

func (r *Reader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	r.wait(mb)
	return mb, err
}

func (r *Reader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBufferTimeout(timeout)
	r.wait(mb)
	return mb, err
}

func (r *Reader) Interrupt() {
	common.Interrupt(r.reader)
}
