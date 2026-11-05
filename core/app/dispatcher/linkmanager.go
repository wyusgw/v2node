package dispatcher

import (
	"io"
	sync "sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
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
	// ip is the client address the link was opened from, so CloseIP can
	// drop a single device without touching the user's other links.
	ip string
	// conn is the client connection, nil when the inbound has none (e.g. UDP).
	// Zero-copy splice runs on the raw sockets and never touches the pipes, so
	// only closing the connection ends it. Mux streams share it, which is fine:
	// they belong to the same user and device.
	conn net.Conn
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

// Interrupt forwards a real interrupt to the underlying pipe. Without it,
// common.Interrupt falls back to Close, which lets a failed connection's
// buffered data be drained instead of released right away.
func (w *ManagedWriter) Interrupt() {
	w.manager.RemoveWriter(w)
	common.Interrupt(w.writer)
}

type LinkManager struct {
	links  map[*ManagedWriter]buf.Reader
	mu     sync.RWMutex
	closed bool
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	closed := m.closed
	if !closed {
		m.links[writer] = reader
	}
	m.mu.Unlock()
	// CloseAll ran after this link passed the limiter check, nothing else would stop it
	if closed {
		killLink(writer, reader)
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
		killLink(w, r)
	}
}

// CloseIP drops only the links opened from ip. Unlike CloseAll the manager
// stays open, so the user's other devices keep connecting through it.
func (m *LinkManager) CloseIP(ip string) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	links := make(map[*ManagedWriter]buf.Reader)
	for w, r := range m.links {
		if w.ip == ip {
			links[w] = r
			delete(m.links, w)
		}
	}
	m.mu.Unlock()

	for w, r := range links {
		killLink(w, r)
	}
}

func killLink(w *ManagedWriter, r buf.Reader) {
	w.killed.Store(true)
	for _, p := range w.downlink {
		common.Interrupt(p)
		common.Close(p)
	}
	common.Close(w.writer)
	common.Interrupt(r)
	if w.conn != nil {
		// TLS close can wait on a stalled client to send its alert
		go w.conn.Close()
	}
}
