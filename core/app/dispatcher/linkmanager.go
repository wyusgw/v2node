package dispatcher

import (
	"io"
	sync "sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
	// killed is set by CloseAll. Closing/interrupting the underlying writer
	// and reader only works when they're pipes (Dispatch); links handed in
	// ready-made through DispatchLink wrap the raw client connection, where
	// Close/Interrupt can be no-ops. Failing the next read/write instead
	// makes the protocol handler tear the connection down itself.
	killed atomic.Bool
	// downlink is the download pipe of a Dispatch link. Killing only the
	// upload side leaves data from the target still flowing to the client,
	// so CloseAll interrupts this one as well.
	downlink []interface{}
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.killed.Load() {
		buf.ReleaseMulti(mb)
		return io.ErrClosedPipe
	}
	return w.writer.WriteMultiBuffer(mb)
}

// ManagedReader fails reads once its link has been killed by CloseAll.
type ManagedReader struct {
	reader buf.TimeoutReader
	writer *ManagedWriter
}

func NewManagedReader(reader buf.Reader, writer *ManagedWriter) *ManagedReader {
	return &ManagedReader{
		reader: &buf.TimeoutWrapperReader{Reader: reader},
		writer: writer,
	}
}

func (r *ManagedReader) check(mb buf.MultiBuffer, err error) (buf.MultiBuffer, error) {
	if r.writer.killed.Load() {
		buf.ReleaseMulti(mb)
		return nil, io.ErrClosedPipe
	}
	return mb, err
}

func (r *ManagedReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	return r.check(r.reader.ReadMultiBuffer())
}

func (r *ManagedReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	return r.check(r.reader.ReadMultiBufferTimeout(timeout))
}

func (r *ManagedReader) Interrupt() {
	common.Interrupt(r.reader)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	return common.Close(w.writer)
}

type LinkManager struct {
	links  map[*ManagedWriter]buf.Reader
	mu     sync.RWMutex
	closed bool
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.links[writer] = reader
	}
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		delete(m.links, writer)
	}
}

func (m *LinkManager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true

	links := m.links
	m.links = make(map[*ManagedWriter]buf.Reader)
	m.mu.Unlock()

	for w, r := range links {
		w.killed.Store(true)
		for _, p := range w.downlink {
			common.Interrupt(p)
			common.Close(p)
		}
		common.Close(w.writer)
		common.Interrupt(r)
	}
}
